-- Repeatable CloudTMS function authority: protected-pay C1 publication staging.
-- This is an additive Weekly Source owner.  It cannot create a Draft, payment,
-- recovery, invoice line, provider instruction or settlement fact.

\set ON_ERROR_STOP on

begin;

create or replace function private.weekly_exceptional_hex_sha256_v1(
  p_value text
) returns bytea
language plpgsql immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
begin
  if p_value is null or p_value!~'^[0-9a-f]{64}$' then
    raise exception 'WEEKLY_PROTECTED_SHA256_INVALID' using errcode='22023';
  end if;
  return pg_catalog.decode(p_value,'hex');
end;
$function$;

create or replace function private.weekly_exceptional_json_keys_exact_v1(
  p_value jsonb,
  p_allowed_keys text[]
) returns boolean
language sql immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select pg_catalog.jsonb_typeof(p_value)='object'
    and not exists(
      select 1
      from pg_catalog.jsonb_object_keys(p_value) supplied(key)
      where not (supplied.key=any(p_allowed_keys))
    );
$function$;

create or replace function public.weekly_exceptional_pay_record_c1_unknown_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_keys constant text[]:=array[
    'schema_version','actor_user_id','publication_request_id','phase',
    'record_offset','next_record_offset','records_submitted','error_code',
    'recovery_call','idempotency_key'
  ];
  v_recovery_keys constant text[]:=array[
    'contract','method','rpc_name','parameters_json','parameters_sha256',
    'result_kind','stream_kind','stream_id','recovery_attempted'
  ];
  v_actor_user_id uuid;
  v_request_id uuid;
  v_phase text;
  v_record_offset integer;
  v_next_record_offset integer;
  v_records_submitted integer;
  v_error_code text;
  v_recovery jsonb;
  v_method text;
  v_rpc_name text;
  v_expected_rpc text;
  v_stream_kind text;
  v_stream_id uuid;
  v_idempotency_key text;
  v_recovery_hash bytea;
  v_parameters_hash bytea;
  v_publication public.weekly_exceptional_c1_publication_requests%rowtype;
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_existing public.weekly_exceptional_c1_unknown_outcomes%rowtype;
  v_unknown_sequence bigint;
  v_total_records integer;
begin
  if not private.weekly_exceptional_json_keys_exact_v1(p_request,v_keys)
     or not (p_request ?& v_keys)
     or p_request->>'schema_version'<>'WEEKLY_PROTECTED_C1_UNKNOWN_V1'
     or pg_catalog.jsonb_typeof(p_request->'recovery_call')<>'object' then
    raise exception 'WEEKLY_PROTECTED_C1_UNKNOWN_INVALID' using errcode='22023';
  end if;
  if pg_catalog.octet_length(pg_catalog.convert_to(p_request::text,'UTF8'))>524288 then
    raise exception 'WEEKLY_PROTECTED_C1_UNKNOWN_TOO_LARGE' using errcode='54000';
  end if;
  begin
    v_actor_user_id:=(p_request->>'actor_user_id')::uuid;
    v_request_id:=(p_request->>'publication_request_id')::uuid;
    v_record_offset:=(p_request->>'record_offset')::integer;
    v_next_record_offset:=(p_request->>'next_record_offset')::integer;
    v_records_submitted:=(p_request->>'records_submitted')::integer;
  exception when others then
    raise exception 'WEEKLY_PROTECTED_C1_UNKNOWN_INVALID' using errcode='22023';
  end;
  v_phase:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_request->>'phase','')));
  v_error_code:=pg_catalog.btrim(coalesce(p_request->>'error_code',''));
  v_recovery:=p_request->'recovery_call';
  v_idempotency_key:=pg_catalog.btrim(coalesce(p_request->>'idempotency_key',''));
  if v_phase not in ('START','STAGE','VALIDATE','CERTIFY','PUBLISH')
     or v_record_offset<0 or v_next_record_offset<v_record_offset
     or (v_phase='STAGE' and v_records_submitted<1)
     or (v_phase<>'STAGE' and v_records_submitted<>0)
     or pg_catalog.char_length(v_error_code) not between 1 and 120
     or pg_catalog.char_length(v_idempotency_key) not between 16 and 240
     or not private.weekly_exceptional_json_keys_exact_v1(v_recovery,v_recovery_keys)
     or not (v_recovery ?& v_recovery_keys)
     or v_recovery->>'contract'<>'WEEKLY_SOURCE_C1_UNKNOWN_CALL_V1'
     or pg_catalog.jsonb_typeof(v_recovery->'recovery_attempted')<>'boolean'
     or (v_recovery->>'recovery_attempted')::boolean then
    raise exception 'WEEKLY_PROTECTED_C1_UNKNOWN_INVALID' using errcode='22023';
  end if;
  v_method:=v_recovery->>'method';
  v_rpc_name:=v_recovery->>'rpc_name';
  v_stream_kind:=v_recovery->>'stream_kind';
  v_expected_rpc:=case v_method
    when 'start' then 'weekly_source_start_c1'
    when 'stage' then 'weekly_source_stage_c1'
    when 'continueValidation' then 'weekly_source_continue_c1'
    when 'certify' then 'weekly_source_certify_c1'
    when 'publish' then 'weekly_source_publish_c1'
    else null end;
  if v_expected_rpc is null or v_rpc_name<>v_expected_rpc
     or v_recovery->>'result_kind'<>'CONTROL'
     or (v_method='start') is distinct from (v_stream_kind='START')
     or (v_method<>'start' and v_stream_kind<>'OPERATION') then
    raise exception 'WEEKLY_PROTECTED_C1_UNKNOWN_ROUTE_INVALID' using errcode='22023';
  end if;
  begin
    v_parameters_hash:=private.weekly_exceptional_hex_sha256_v1(
      v_recovery->>'parameters_sha256'
    );
    if v_stream_kind='START' then
      v_stream_id:=(v_recovery->>'stream_id')::uuid;
      if v_stream_id is distinct from v_request_id then
        raise exception 'WEEKLY_PROTECTED_C1_UNKNOWN_STREAM_INVALID';
      end if;
      -- START is keyed by the immutable request id until C1 returns the real
      -- operation id.  Never persist that request id as an operation id.
      v_stream_id:=null;
    else
      v_stream_id:=(v_recovery->>'stream_id')::uuid;
    end if;
  exception when others then
    raise exception 'WEEKLY_PROTECTED_C1_UNKNOWN_STREAM_INVALID' using errcode='22023';
  end;
  if extensions.digest(
       pg_catalog.convert_to(v_recovery->>'parameters_json','UTF8'),'sha256'
     ) is distinct from v_parameters_hash then
    raise exception 'WEEKLY_PROTECTED_C1_UNKNOWN_PARAMETERS_INVALID' using errcode='22023';
  end if;
  v_recovery_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_C1_UNKNOWN_CALL_V1',v_recovery
  );

  select unknown_outcome.* into v_existing
  from public.weekly_exceptional_c1_unknown_outcomes unknown_outcome
  where unknown_outcome.idempotency_key=v_idempotency_key
  for share;
  if found then
    if v_existing.publication_request_id is distinct from v_request_id
       or v_existing.phase is distinct from v_phase
       or v_existing.record_offset is distinct from v_record_offset
       or v_existing.next_record_offset is distinct from v_next_record_offset
       or v_existing.records_submitted is distinct from v_records_submitted
       or v_existing.recovery_call_sha256 is distinct from v_recovery_hash then
      raise exception 'WEEKLY_PROTECTED_C1_UNKNOWN_KEY_COLLISION' using errcode='23505';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'outcome','RECOVERY_REQUIRED','idempotent_replay',true,
      'publication_request_id',v_request_id,'unknown_outcome_id',v_existing.id,
      'state',v_existing.state
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
     or not exists(
       select 1 from public.tms_users office_user
       where office_user.id=v_actor_user_id
         and office_user.is_active
         and (office_user.payment_authoriser or office_user.payment_golden_key)
     )
     or v_publication.state in ('PUBLISHED','REFUSED','FAILED','RETIRED')
     or exists(
       select 1 from public.weekly_exceptional_c1_unknown_outcomes pending
       where pending.publication_request_id=v_request_id
         and pending.state='RECOVERY_REQUIRED'
     )
     or (v_stream_id is not null
         and v_publication.c1_operation_id is not null
         and v_stream_id is distinct from v_publication.c1_operation_id) then
    raise exception 'WEEKLY_PROTECTED_C1_UNKNOWN_SCOPE_INVALID' using errcode='55000';
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
  if v_total_records<1 or v_next_record_offset>v_total_records
     or v_next_record_offset<>v_record_offset
     or v_records_submitted>v_total_records-v_record_offset then
    raise exception 'WEEKLY_PROTECTED_C1_UNKNOWN_OFFSET_INVALID' using errcode='55000';
  end if;
  select coalesce(max(unknown_sequence),0)+1 into v_unknown_sequence
  from public.weekly_exceptional_c1_unknown_outcomes
  where publication_request_id=v_request_id;
  insert into public.weekly_exceptional_c1_unknown_outcomes(
    publication_request_id,unknown_sequence,phase,record_offset,
    next_record_offset,records_submitted,error_code,recovery_call_json,
    recovery_call_sha256,state,idempotency_key
  ) values (
    v_request_id,v_unknown_sequence,v_phase,v_record_offset,
    v_next_record_offset,v_records_submitted,v_error_code,v_recovery,
    v_recovery_hash,'RECOVERY_REQUIRED',v_idempotency_key
  ) returning * into v_existing;
  update public.weekly_exceptional_c1_publication_requests
  set state='RECOVERY_REQUIRED',c1_operation_id=coalesce(c1_operation_id,v_stream_id)
  where id=v_request_id;
  update public.weekly_exceptional_pay_target_families
  set c1_publication_state='PENDING' where id=v_family.id;
  update public.weekly_exceptional_orchestration_runs
  set state='PENDING' where id=v_run.id;
  return pg_catalog.jsonb_build_object(
    'ok',true,'outcome','RECOVERY_REQUIRED','idempotent_replay',false,
    'publication_request_id',v_request_id,'unknown_outcome_id',v_existing.id,
    'unknown_sequence',v_unknown_sequence,'state','RECOVERY_REQUIRED',
    'recovery_call_sha256',pg_catalog.encode(v_recovery_hash,'hex')
  );
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_PROTECTED_C1_UNKNOWN_SCOPE_INVALID' using errcode='55000';
end;
$function$;

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
     or not exists(
       select 1 from public.tms_users office_user
       where office_user.id=v_actor_user_id
         and office_user.is_active
         and (office_user.payment_authoriser or office_user.payment_golden_key)
     )
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

create or replace function public.weekly_exceptional_pay_record_c1_recovery_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_keys constant text[]:=array[
    'schema_version','actor_user_id','unknown_outcome_id','recovery',
    'idempotency_key'
  ];
  v_recovery_keys constant text[]:=array[
    'contract','ok','decision','code','status','result','replay_attempted'
  ];
  v_actor_user_id uuid;
  v_unknown_id uuid;
  v_recovery jsonb;
  v_idempotency_key text;
  v_unknown public.weekly_exceptional_c1_unknown_outcomes%rowtype;
  v_publication public.weekly_exceptional_c1_publication_requests%rowtype;
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_result jsonb;
  v_decision text;
  v_code text;
  v_next_offset integer;
  v_consumed integer:=0;
  v_recovered_state text;
  v_checkpoint jsonb;
begin
  if not private.weekly_exceptional_json_keys_exact_v1(p_request,v_keys)
     or not (p_request ?& v_keys)
     or p_request->>'schema_version'<>'WEEKLY_PROTECTED_C1_RECOVERY_RESULT_V1'
     or pg_catalog.jsonb_typeof(p_request->'recovery')<>'object' then
    raise exception 'WEEKLY_PROTECTED_C1_RECOVERY_INVALID' using errcode='22023';
  end if;
  begin
    v_actor_user_id:=(p_request->>'actor_user_id')::uuid;
    v_unknown_id:=(p_request->>'unknown_outcome_id')::uuid;
  exception when others then
    raise exception 'WEEKLY_PROTECTED_C1_RECOVERY_INVALID' using errcode='22023';
  end;
  v_recovery:=p_request->'recovery';
  v_idempotency_key:=pg_catalog.btrim(coalesce(p_request->>'idempotency_key',''));
  if not private.weekly_exceptional_json_keys_exact_v1(v_recovery,v_recovery_keys)
     or not (v_recovery ?& v_recovery_keys)
     or v_recovery->>'contract'<>'WEEKLY_SOURCE_C1_RECOVERY_V1'
     or pg_catalog.jsonb_typeof(v_recovery->'ok')<>'boolean'
     or pg_catalog.jsonb_typeof(v_recovery->'replay_attempted')<>'boolean'
     or pg_catalog.char_length(v_idempotency_key) not between 16 and 240
     or pg_catalog.octet_length(pg_catalog.convert_to(v_recovery::text,'UTF8'))>131072 then
    raise exception 'WEEKLY_PROTECTED_C1_RECOVERY_INVALID' using errcode='22023';
  end if;
  v_decision:=pg_catalog.upper(pg_catalog.btrim(coalesce(v_recovery->>'decision','')));
  v_code:=pg_catalog.btrim(coalesce(v_recovery->>'code',''));
  if v_decision not in ('COMMITTED','REPLAYED','REPLAYED_READ','REFUSED')
     or pg_catalog.char_length(v_code) not between 1 and 120 then
    raise exception 'WEEKLY_PROTECTED_C1_RECOVERY_INVALID' using errcode='22023';
  end if;

  select unknown_outcome.* into strict v_unknown
  from public.weekly_exceptional_c1_unknown_outcomes unknown_outcome
  where unknown_outcome.id=v_unknown_id
  for update;
  select request.* into strict v_publication
  from public.weekly_exceptional_c1_publication_requests request
  where request.id=v_unknown.publication_request_id
  for update;
  select run.* into strict v_run
  from public.weekly_exceptional_orchestration_runs run
  where run.id=v_publication.orchestration_run_id
  for update;
  select family.* into strict v_family
  from public.weekly_exceptional_pay_target_families family
  where family.id=v_publication.family_id
  for update;

  if v_unknown.recovery_idempotency_key is not null then
    if v_unknown.recovery_idempotency_key is distinct from v_idempotency_key
       or v_unknown.recovery_result_json is distinct from v_recovery then
      raise exception 'WEEKLY_PROTECTED_C1_RECOVERY_ALREADY_RECORDED' using errcode='55000';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',v_unknown.state<>'REFUSED','outcome',v_unknown.state,
      'idempotent_replay',true,'publication_request_id',v_publication.id,
      'unknown_outcome_id',v_unknown.id,'state',v_publication.state
    );
  end if;

  if v_unknown.state<>'RECOVERY_REQUIRED'
     or v_publication.state<>'RECOVERY_REQUIRED'
     or v_run.requested_by_user_id is distinct from v_actor_user_id
     or not exists(
       select 1 from public.tms_users office_user
       where office_user.id=v_actor_user_id and office_user.is_active
         and (office_user.payment_authoriser or office_user.payment_golden_key)
     ) then
    raise exception 'WEEKLY_PROTECTED_C1_RECOVERY_SCOPE_INVALID' using errcode='55000';
  end if;

  if (v_recovery->>'ok')::boolean is not true then
    if v_decision<>'REFUSED' then
      raise exception 'WEEKLY_PROTECTED_C1_RECOVERY_INVALID' using errcode='22023';
    end if;
    update public.weekly_exceptional_c1_unknown_outcomes
    set state='REFUSED',recovery_result_json=v_recovery,
        recovered_at_utc=pg_catalog.statement_timestamp(),
        recovery_idempotency_key=v_idempotency_key
    where id=v_unknown.id;
    update public.weekly_exceptional_c1_publication_requests
    set state='REFUSED',typed_result_json=v_recovery,
        completed_at_utc=pg_catalog.statement_timestamp()
    where id=v_publication.id;
    update public.weekly_exceptional_pay_generations
    set lifecycle_state='FAILED'
    where id=v_publication.generation_id and lifecycle_state='PENDING_C1';
    update public.weekly_exceptional_pay_target_families
    set c1_publication_state=case when current_generation_id is null then 'FAILED' else 'LIVE' end,
        bound_version=bound_version+1
    where id=v_family.id;
    update public.weekly_exceptional_orchestration_runs
    set state='REFUSED',after_state_fingerprint=private.weekly_source_sha256_jsonb_v1(
          'WEEKLY_PROTECTED_C1_RECOVERY_REFUSED_V1',v_recovery
        ),completed_at_utc=pg_catalog.statement_timestamp()
    where id=v_run.id;
    return pg_catalog.jsonb_build_object(
      'ok',false,'outcome','REFUSED','idempotent_replay',false,
      'publication_request_id',v_publication.id,
      'unknown_outcome_id',v_unknown.id,'state','REFUSED','code',v_code
    );
  end if;

  if pg_catalog.jsonb_typeof(v_recovery->'result')<>'object' then
    raise exception 'WEEKLY_PROTECTED_C1_RECOVERY_RESULT_INVALID' using errcode='22023';
  end if;
  v_result:=v_recovery->'result';
  v_next_offset:=v_unknown.record_offset;
  if v_unknown.phase='STAGE' then
    if v_decision<>'REPLAYED'
       or v_code<>'C1_RECOVERY_STAGE_COMMITTED_EXACT_REPLAY'
       or (v_recovery->>'replay_attempted')::boolean is not true
       or nullif(v_result->>'input_records_consumed','') is null then
      raise exception 'WEEKLY_PROTECTED_C1_STAGE_RECOVERY_UNPROVED' using errcode='55000';
    end if;
    begin
      v_consumed:=(v_result->>'input_records_consumed')::integer;
    exception when others then
      raise exception 'WEEKLY_PROTECTED_C1_STAGE_RECOVERY_UNPROVED' using errcode='55000';
    end;
    if v_consumed<0 or v_consumed>v_unknown.records_submitted then
      raise exception 'WEEKLY_PROTECTED_C1_STAGE_RECOVERY_UNPROVED' using errcode='55000';
    end if;
    v_next_offset:=v_unknown.record_offset+v_consumed;
  end if;

  v_recovered_state:=case when v_decision='COMMITTED' then 'RECOVERED_COMMITTED'
    else 'RECOVERED_REPLAYED' end;
  update public.weekly_exceptional_c1_unknown_outcomes
  set state=v_recovered_state,recovery_result_json=v_recovery,
      recovered_at_utc=pg_catalog.statement_timestamp(),
      recovery_idempotency_key=v_idempotency_key
  where id=v_unknown.id;
  update public.weekly_exceptional_c1_publication_requests
  set state='SUBMITTED',typed_result_json=v_result
  where id=v_publication.id;

  v_checkpoint:=public.weekly_exceptional_pay_record_c1_checkpoint_v1(
    pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_PROTECTED_C1_CHECKPOINT_V1',
      'actor_user_id',v_actor_user_id,
      'publication_request_id',v_publication.id,
      'phase',v_unknown.phase,
      'record_offset',v_unknown.record_offset,
      'next_record_offset',v_next_offset,
      'result',v_result,
      'idempotency_key',v_idempotency_key||':checkpoint'
    )
  );
  return pg_catalog.jsonb_build_object(
    'ok',true,'outcome',v_recovered_state,'idempotent_replay',false,
    'publication_request_id',v_publication.id,
    'unknown_outcome_id',v_unknown.id,'next_record_offset',v_next_offset,
    'checkpoint',v_checkpoint
  );
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_PROTECTED_C1_RECOVERY_SCOPE_INVALID' using errcode='55000';
end;
$function$;

create or replace function public.weekly_exceptional_pay_read_c1_request_v1(
  p_request jsonb
) returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_keys constant text[]:=array[
    'schema_version','actor_user_id','publication_request_id',
    'expected_request_sha256'
  ];
  v_actor_user_id uuid;
  v_request_id uuid;
  v_expected_request_hash bytea;
  v_publication public.weekly_exceptional_c1_publication_requests%rowtype;
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_records jsonb;
  v_checkpoint jsonb;
  v_unknown jsonb;
begin
  if not private.weekly_exceptional_json_keys_exact_v1(p_request,v_keys)
     or not (p_request ?& v_keys)
     or p_request->>'schema_version'<>'WEEKLY_PROTECTED_C1_READ_V1' then
    raise exception 'WEEKLY_PROTECTED_C1_READ_INVALID' using errcode='22023';
  end if;
  begin
    v_actor_user_id:=(p_request->>'actor_user_id')::uuid;
    v_request_id:=(p_request->>'publication_request_id')::uuid;
    v_expected_request_hash:=private.weekly_exceptional_hex_sha256_v1(
      p_request->>'expected_request_sha256'
    );
  exception when others then
    raise exception 'WEEKLY_PROTECTED_C1_READ_INVALID' using errcode='22023';
  end;

  select request.* into strict v_publication
  from public.weekly_exceptional_c1_publication_requests request
  where request.id=v_request_id;
  select run.* into strict v_run
  from public.weekly_exceptional_orchestration_runs run
  where run.id=v_publication.orchestration_run_id;
  if v_publication.request_sha256 is distinct from v_expected_request_hash
     or v_run.requested_by_user_id is distinct from v_actor_user_id
     or not exists(
       select 1 from public.tms_users office_user
       where office_user.id=v_actor_user_id and office_user.is_active
         and (office_user.payment_authoriser or office_user.payment_golden_key)
     ) then
    raise exception 'WEEKLY_PROTECTED_C1_READ_SCOPE_INVALID' using errcode='55000';
  end if;

  with source_and_parts as (
    select source.source_ordinal::bigint as major_ordinal,0::bigint as minor_ordinal,
      pg_catalog.jsonb_build_object(
        'source_ordinal',source.source_ordinal::text,
        'source_id',source.id,
        'authority_kind',source.authority_kind,
        'source_system',source.source_system,
        'external_identity',source.external_identity,
        'external_revision',source.external_revision,
        'source_document_sha256',pg_catalog.encode(source.source_document_sha256,'hex'),
        'payload_bytes',source.payload_bytes::text,
        'part_count',source.part_count::text,
        'work_date',source.work_date,
        'root_timesheet_id',source.root_timesheet_id,
        'candidate_id',source.candidate_id,
        'contract_id',source.contract_id,
        'source_row_sha256',pg_catalog.encode(source.source_row_sha256,'hex'),
        'record_type','SOURCE'
      ) as record_json
    from public.weekly_exceptional_c1_source_records source
    where source.publication_request_id=v_request_id
    union all
    select part.source_ordinal::bigint,part.part_ordinal::bigint,
      pg_catalog.jsonb_build_object(
        'source_ordinal',part.source_ordinal::text,
        'part_ordinal',part.part_ordinal::text,
        'payload_utf8',pg_catalog.encode(part.payload_utf8,'hex'),
        'fragment_sha256',pg_catalog.encode(part.fragment_sha256,'hex'),
        'record_type','PART'
      )
    from public.weekly_exceptional_c1_source_parts part
    where part.publication_request_id=v_request_id
  ), ordered_records as (
    select 1::integer as kind_order,major_ordinal,minor_ordinal,record_json
    from source_and_parts
    union all
    select 2::integer,component.component_ordinal::bigint,0::bigint,
      pg_catalog.jsonb_build_object(
        'component_ordinal',component.component_ordinal::text,
        'component_id',component.component_id,
        'source_ordinal',component.source_ordinal::text,
        'source_key',component.source_key,
        'component_kind',component.component_kind,
        'economic_key_type',component.economic_key_type,
        'economic_key_value',component.economic_key_value,
        'component_member_identity',component.component_member_identity,
        'segment_id',component.segment_id,
        'segment_key',component.segment_key,
        'segment_stable_key',component.segment_stable_key,
        'work_date',component.work_date,
        'reference_number',component.reference_number,
        'hours_day',case when component.hours_day is null then null else pg_catalog.to_char(component.hours_day,'FM9999999999990.000000') end,
        'hours_night',case when component.hours_night is null then null else pg_catalog.to_char(component.hours_night,'FM9999999999990.000000') end,
        'hours_sat',case when component.hours_sat is null then null else pg_catalog.to_char(component.hours_sat,'FM9999999999990.000000') end,
        'hours_sun',case when component.hours_sun is null then null else pg_catalog.to_char(component.hours_sun,'FM9999999999990.000000') end,
        'hours_bh',case when component.hours_bh is null then null else pg_catalog.to_char(component.hours_bh,'FM9999999999990.000000') end,
        'additional_code_raw',component.additional_code_raw,
        'unit_count',case when component.unit_count is null then null else pg_catalog.to_char(component.unit_count,'FM9999999999990.000000') end,
        'unit_pay_rate',case when component.unit_pay_rate is null then null else pg_catalog.to_char(component.unit_pay_rate,'FM9999999999990.000000') end,
        'unit_charge_rate',case when component.unit_charge_rate is null then null else pg_catalog.to_char(component.unit_charge_rate,'FM9999999999990.000000') end,
        'expense_code',component.expense_code,
        'pay_ex_vat',pg_catalog.to_char(component.pay_ex_vat,'FM9999999999990.00'),
        'charge_ex_vat',case when component.charge_ex_vat is null then null else pg_catalog.to_char(component.charge_ex_vat,'FM9999999999990.00') end,
        'exclude_from_pay',component.exclude_from_pay,
        'origin',component.origin,
        'component_sha256',pg_catalog.encode(component.component_sha256,'hex'),
        'record_type','COMPONENT'
      )
    from public.weekly_exceptional_c1_component_records component
    where component.publication_request_id=v_request_id
  )
  select coalesce(pg_catalog.jsonb_agg(record_json order by kind_order,major_ordinal,minor_ordinal),'[]'::jsonb)
  into v_records from ordered_records;

  select pg_catalog.jsonb_build_object(
    'phase',checkpoint.phase,'result',checkpoint.result_json,
    'record_offset',checkpoint.record_offset,
    'next_record_offset',checkpoint.next_record_offset,
    'checkpoint_sha256',pg_catalog.encode(checkpoint.checkpoint_sha256,'hex')
  ) into v_checkpoint
  from public.weekly_exceptional_c1_publication_checkpoints checkpoint
  where checkpoint.publication_request_id=v_request_id
  order by checkpoint.checkpoint_sequence desc limit 1;

  select pg_catalog.jsonb_build_object(
    'unknown_outcome_id',unknown_outcome.id,
    'phase',unknown_outcome.phase,
    'record_offset',unknown_outcome.record_offset,
    'next_record_offset',unknown_outcome.next_record_offset,
    'records_submitted',unknown_outcome.records_submitted,
    'recovery_call',unknown_outcome.recovery_call_json,
    'recovery_call_sha256',pg_catalog.encode(unknown_outcome.recovery_call_sha256,'hex')
  ) into v_unknown
  from public.weekly_exceptional_c1_unknown_outcomes unknown_outcome
  where unknown_outcome.publication_request_id=v_request_id
    and unknown_outcome.state='RECOVERY_REQUIRED'
  order by unknown_outcome.unknown_sequence desc limit 1;

  return pg_catalog.jsonb_build_object(
    'ok',true,'contract','WEEKLY_PROTECTED_C1_READ_V1',
    'publication_request_id',v_publication.id,
    'state',v_publication.state,
    'publication',pg_catalog.jsonb_build_object(
      'request',pg_catalog.jsonb_build_object(
        'request_id',v_publication.id,
        'request_sequence',v_publication.request_sequence::text,
        'actor_user_id',v_actor_user_id,
        'candidate_id',v_publication.candidate_id,
        'contract_id',v_publication.contract_id,
        'root_timesheet_id',v_publication.root_timesheet_id,
        'week_ending_date',v_publication.week_ending_date,
        'source_mode',v_publication.source_mode,
        'expected_head_revision',v_publication.expected_head_revision::text,
        'expected_source_count',v_publication.expected_source_count::text,
        'expected_component_count',v_publication.expected_component_count::text,
        'expected_payload_bytes',v_publication.expected_payload_bytes::text,
        'source_manifest_sha256',pg_catalog.encode(v_publication.source_manifest_sha256,'hex'),
        'entitlement_sha256',pg_catalog.encode(v_publication.entitlement_sha256,'hex'),
        'approval_sha256',pg_catalog.encode(v_publication.approval_sha256,'hex'),
        'is_zero_entitlement',v_publication.is_zero_entitlement,
        'financial_row_id',v_publication.financial_row_id
      ),
      'request_sha256',pg_catalog.encode(v_publication.request_sha256,'hex'),
      'records',v_records
    ),
    'resume_checkpoint',v_checkpoint,
    'unknown_checkpoint',v_unknown
  );
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_PROTECTED_C1_READ_SCOPE_INVALID' using errcode='55000';
end;
$function$;

create or replace function public.weekly_exceptional_pay_stage_c1_request_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_top_keys constant text[]:=array[
    'schema_version','actor_user_id','family_id','orchestration_run_id',
    'source_cycle_id','client_id','work_event_id','evidence_timesheet_id',
    'expected_family_bound_version','protected_schedule','rate_classification',
    'source_proposal','target_snapshot','current_comparison_revision_id',
    'current_final_revision_id','c1_request','c1_sources','c1_components',
    'reason','idempotency_key'
  ];
  v_c1_keys constant text[]:=array[
    'request_id','request_sequence','actor_user_id','candidate_id','contract_id',
    'root_timesheet_id','week_ending_date','source_mode',
    'expected_head_revision','expected_source_count','expected_component_count',
    'expected_payload_bytes','source_manifest_sha256','entitlement_sha256',
    'approval_sha256','is_zero_entitlement','financial_row_id','request_sha256'
  ];
  v_source_keys constant text[]:=array[
    'source_ordinal','source_id','authority_kind','source_system',
    'external_identity','external_revision','source_document_sha256',
    'payload_bytes','part_count','work_date','root_timesheet_id','candidate_id',
    'contract_id','source_row_sha256','record_type','parts'
  ];
  v_part_keys constant text[]:=array[
    'source_ordinal','part_ordinal','payload_utf8','fragment_sha256','record_type'
  ];
  v_component_keys constant text[]:=array[
    'component_ordinal','component_id','source_ordinal','source_key',
    'component_kind','economic_key_type','economic_key_value',
    'component_member_identity','segment_id','segment_key','segment_stable_key',
    'work_date','reference_number','hours_day','hours_night','hours_sat',
    'hours_sun','hours_bh','additional_code_raw','unit_count','unit_pay_rate',
    -- S7 (WB-007, WB-013, 24 section 5): `adjustment_id` is deliberately absent
    -- from this exact-key allowlist and from the C1 component relation.  An
    -- adjustment is never copied into a head, because an adjustment created
    -- later would make the snapshot stale and create a second owner.  A caller
    -- that still sends the key now fails closed here.
    'unit_charge_rate','expense_code','pay_ex_vat',
    'charge_ex_vat','exclude_from_pay','origin','component_sha256','record_type'
  ];
  v_schedule_keys constant text[]:=array[
    'work_date','start_at_local','end_at_local','break_minutes'
  ];
  v_actor_user_id uuid;
  v_family_id uuid;
  v_run_id uuid;
  v_source_cycle_id uuid;
  v_client_id uuid;
  v_work_event_id uuid;
  v_evidence_timesheet_id uuid;
  v_expected_bound_version bigint;
  v_comparison_revision_id uuid;
  v_final_revision_id uuid;
  v_reason text;
  v_idempotency_key text;
  v_schedule jsonb;
  v_rate_classification jsonb;
  v_source_proposal jsonb;
  v_target_snapshot jsonb;
  v_start jsonb;
  v_sources jsonb;
  v_components jsonb;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_root public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_evidence public.timesheets%rowtype;
  v_signed_evidence jsonb;
  v_event public.weekly_work_events%rowtype;
  v_prior_generation public.weekly_exceptional_pay_generations%rowtype;
  v_approval public.weekly_exceptional_payment_approvals%rowtype;
  v_generation public.weekly_exceptional_pay_generations%rowtype;
  v_publication public.weekly_exceptional_c1_publication_requests%rowtype;
  v_policy jsonb;
  v_source_mode text;
  v_work_date date;
  v_start_at timestamp without time zone;
  v_end_at timestamp without time zone;
  v_break_minutes integer;
  v_expected_source_count integer;
  v_expected_component_count integer;
  v_expected_payload_bytes integer;
  v_request_sequence bigint;
  v_expected_head_revision bigint;
  v_financial_row_id uuid;
  v_request_id uuid;
  v_source_manifest_hash bytea;
  v_entitlement_hash bytea;
  v_outer_approval_hash bytea;
  v_c1_request_hash bytea;
  v_rate_policy_hash bytea;
  v_schedule_hash bytea;
  v_source_proposal_hash bytea;
  v_prior_vector jsonb;
  v_prior_vector_hash bytea;
  v_next_vector jsonb;
  v_next_vector_hash bytea;
  v_fixed_source_hash bytea;
  v_local_approval_hash bytea;
  v_family_event_hash bytea;
  v_target_event_hash bytea;
  v_prior_family_event_hash bytea;
  v_prior_target_event_hash bytea;
  v_issue_ids uuid[]:='{}'::uuid[];
  v_issue_ids_hash bytea;
  v_signed_revision integer;
  v_signed_hash bytea;
  v_signed_at timestamptz;
  v_generation_number integer;
  v_generation_reason text;
  v_family_event_state text;
  v_resulting_lifecycle_state text;
  v_pending_outcome text;
  v_payment_event_kind text;
  v_other_wait_count integer:=0;
  v_family_event_sequence bigint;
  v_target_event_sequence bigint;
  v_total_pay numeric(12,2):=0;
  v_payload_total bigint:=0;
  v_previous_source_key text:=null;
  v_source_entry jsonb;
  v_part_entry jsonb;
  v_component_entry jsonb;
  v_source_id uuid;
  v_source_ordinal integer;
  v_source_counter integer:=0;
  v_part_ordinal integer;
  v_component_ordinal integer;
  v_part_payload bytea;
  v_part_concat bytea;
  v_source_part_count integer;
  v_source_payload_bytes integer;
  v_source_kind text;
  v_source_document jsonb;
  v_count_client integer:=0;
  v_count_candidate integer:=0;
  v_count_root integer:=0;
  v_count_provider integer:=0;
  v_count_office integer:=0;
  v_step_sequence integer;
  v_request_body_hash bytea;
  v_before_hash bytea;
  v_after_hash bytea;
begin
  if not private.weekly_exceptional_json_keys_exact_v1(p_request,v_top_keys)
     or not (p_request ?& v_top_keys)
     or p_request->>'schema_version'<>'WEEKLY_PROTECTED_C1_STAGE_REQUEST_V1'
     or pg_catalog.jsonb_typeof(p_request->'protected_schedule')<>'object'
     or pg_catalog.jsonb_typeof(p_request->'rate_classification')<>'object'
     or pg_catalog.jsonb_typeof(p_request->'source_proposal')<>'object'
     or pg_catalog.jsonb_typeof(p_request->'target_snapshot')<>'object'
     or pg_catalog.jsonb_typeof(p_request->'c1_request')<>'object'
     or pg_catalog.jsonb_typeof(p_request->'c1_sources')<>'array'
     or pg_catalog.jsonb_typeof(p_request->'c1_components')<>'array' then
    raise exception 'WEEKLY_PROTECTED_C1_STAGE_REQUEST_INVALID' using errcode='22023';
  end if;
  if pg_catalog.octet_length(pg_catalog.convert_to(p_request::text,'UTF8'))>4194304 then
    raise exception 'WEEKLY_PROTECTED_C1_STAGE_REQUEST_TOO_LARGE' using errcode='54000';
  end if;
  begin
    v_actor_user_id:=(p_request->>'actor_user_id')::uuid;
    v_family_id:=(p_request->>'family_id')::uuid;
    v_run_id:=(p_request->>'orchestration_run_id')::uuid;
    v_source_cycle_id:=(p_request->>'source_cycle_id')::uuid;
    v_client_id:=(p_request->>'client_id')::uuid;
    v_work_event_id:=(p_request->>'work_event_id')::uuid;
    v_evidence_timesheet_id:=nullif(p_request->>'evidence_timesheet_id','')::uuid;
    v_expected_bound_version:=(p_request->>'expected_family_bound_version')::bigint;
    v_comparison_revision_id:=nullif(p_request->>'current_comparison_revision_id','')::uuid;
    v_final_revision_id:=nullif(p_request->>'current_final_revision_id','')::uuid;
  exception when others then
    raise exception 'WEEKLY_PROTECTED_C1_STAGE_REQUEST_INVALID' using errcode='22023';
  end;
  v_reason:=pg_catalog.btrim(coalesce(p_request->>'reason',''));
  v_idempotency_key:=pg_catalog.btrim(coalesce(p_request->>'idempotency_key',''));
  if pg_catalog.char_length(v_reason) not between 1 and 1000
     or pg_catalog.char_length(v_idempotency_key) not between 16 and 240 then
    raise exception 'WEEKLY_PROTECTED_C1_STAGE_REQUEST_INVALID' using errcode='22023';
  end if;

  v_schedule:=p_request->'protected_schedule';
  if not private.weekly_exceptional_json_keys_exact_v1(v_schedule,v_schedule_keys)
     or not (v_schedule ?& v_schedule_keys) then
    raise exception 'WEEKLY_PROTECTED_SCHEDULE_INVALID' using errcode='22023';
  end if;
  begin
    v_work_date:=(v_schedule->>'work_date')::date;
    v_start_at:=(v_schedule->>'start_at_local')::timestamp without time zone;
    v_end_at:=(v_schedule->>'end_at_local')::timestamp without time zone;
    v_break_minutes:=(v_schedule->>'break_minutes')::integer;
  exception when others then
    raise exception 'WEEKLY_PROTECTED_SCHEDULE_INVALID' using errcode='22023';
  end;
  if v_end_at<=v_start_at or v_start_at::date<>v_work_date
     or v_break_minutes<0
     or v_break_minutes>=extract(epoch from (v_end_at-v_start_at))/60 then
    raise exception 'WEEKLY_PROTECTED_SCHEDULE_INVALID' using errcode='22023';
  end if;
  v_rate_classification:=p_request->'rate_classification';
  v_source_proposal:=p_request->'source_proposal';
  v_target_snapshot:=p_request->'target_snapshot';
  if pg_catalog.octet_length(pg_catalog.convert_to(v_rate_classification::text,'UTF8'))>65536
     or pg_catalog.octet_length(pg_catalog.convert_to(v_source_proposal::text,'UTF8'))>65536
     or pg_catalog.octet_length(pg_catalog.convert_to(v_target_snapshot::text,'UTF8'))>1048576 then
    raise exception 'WEEKLY_PROTECTED_TARGET_TOO_LARGE' using errcode='54000';
  end if;

  v_start:=p_request->'c1_request';
  v_sources:=p_request->'c1_sources';
  v_components:=p_request->'c1_components';
  if not private.weekly_exceptional_json_keys_exact_v1(v_start,v_c1_keys)
     or not (v_start ?& v_c1_keys)
     or pg_catalog.jsonb_array_length(v_sources) not between 1 and 65536
     or pg_catalog.jsonb_array_length(v_components)>65536 then
    raise exception 'WEEKLY_PROTECTED_C1_STREAM_INVALID' using errcode='22023';
  end if;
  begin
    v_request_id:=(v_start->>'request_id')::uuid;
    v_request_sequence:=(v_start->>'request_sequence')::bigint;
    v_expected_head_revision:=(v_start->>'expected_head_revision')::bigint;
    v_expected_source_count:=(v_start->>'expected_source_count')::integer;
    v_expected_component_count:=(v_start->>'expected_component_count')::integer;
    v_expected_payload_bytes:=(v_start->>'expected_payload_bytes')::integer;
    v_financial_row_id:=(v_start->>'financial_row_id')::uuid;
    v_source_manifest_hash:=private.weekly_exceptional_hex_sha256_v1(v_start->>'source_manifest_sha256');
    v_entitlement_hash:=private.weekly_exceptional_hex_sha256_v1(v_start->>'entitlement_sha256');
    v_outer_approval_hash:=private.weekly_exceptional_hex_sha256_v1(v_start->>'approval_sha256');
    v_c1_request_hash:=private.weekly_exceptional_hex_sha256_v1(v_start->>'request_sha256');
  exception when others then
    raise exception 'WEEKLY_PROTECTED_C1_STREAM_INVALID' using errcode='22023';
  end;
  if v_request_sequence<1 or v_expected_head_revision<0
     or v_expected_source_count<>pg_catalog.jsonb_array_length(v_sources)
     or v_expected_component_count<>pg_catalog.jsonb_array_length(v_components)
     or v_expected_payload_bytes<1
     or ((v_start->>'is_zero_entitlement')::boolean is distinct from
         (v_expected_component_count=0)) then
    raise exception 'WEEKLY_PROTECTED_C1_STREAM_COUNT_INVALID' using errcode='22023';
  end if;

  -- An exact staged request is the idempotency authority even after the family
  -- version advances.  A UUID collision with different bytes always refuses.
  select request.* into v_publication
  from public.weekly_exceptional_c1_publication_requests request
  where request.id=v_request_id
  for update;
  if found then
    if v_publication.family_id is distinct from v_family_id
       or v_publication.orchestration_run_id is distinct from v_run_id
       or v_publication.request_sha256 is distinct from v_c1_request_hash then
      raise exception 'WEEKLY_PROTECTED_C1_REQUEST_ID_COLLISION' using errcode='23505';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'outcome','STAGED','idempotent_replay',true,
      'family_id',v_publication.family_id,'generation_id',v_publication.generation_id,
      'publication_request_id',v_publication.id,'request_sha256',
      pg_catalog.encode(v_publication.request_sha256,'hex'),
      'state',v_publication.state
    );
  end if;

  select family.* into strict v_family
  from public.weekly_exceptional_pay_target_families family
  where family.id=v_family_id
  for update;
  select run.* into strict v_run
  from public.weekly_exceptional_orchestration_runs run
  where run.id=v_run_id and run.family_id=v_family.id
  for update;
  select cycle.* into strict v_cycle
  from public.weekly_source_cycles cycle
  where cycle.id=v_source_cycle_id
  for share;
  select source_group.* into strict v_group
  from public.weekly_source_groups source_group
  where source_group.id=v_cycle.source_group_id and source_group.active
  for share;
  perform private.weekly_source_office_authority_v1(
    v_actor_user_id,'APPROVE_PROTECTED_PAY',v_group.id,v_client_id,v_work_date
  );

  if v_run.requested_by_user_id is distinct from v_actor_user_id
     or v_run.state<>'RUNNING'
     or v_run.request_kind not in (
       'APPROVE','AMEND','WITHDRAW','RECONCILE','RECORD_NOT_WORKED'
     )
     or v_family.bound_version<>v_expected_bound_version
     or v_family.c1_publication_state in ('PENDING','PUBLISHING')
     or v_family.agency_id is distinct from v_group.agency_id
     or v_family.root_timesheet_id is null
     or v_work_date not between v_family.week_start_date and v_family.week_ending_date
     or not exists(
       select 1 from public.weekly_source_group_clients membership
       where membership.source_group_id=v_group.id
         and membership.client_id=v_client_id
         and v_work_date between membership.valid_from
           and coalesce(membership.valid_to,'infinity'::date)
     ) then
    raise exception 'WEEKLY_PROTECTED_C1_STAGE_SCOPE_INVALID' using errcode='55000';
  end if;

  select timesheet.* into strict v_root
  from public.timesheets timesheet
  where timesheet.timesheet_id=v_family.root_timesheet_id
  for share;
  select financial.* into strict v_fin
  from public.timesheets_financials financial
  where financial.id=v_financial_row_id
    and financial.timesheet_id=v_root.timesheet_id
    and financial.is_current
  for share;
  select event.* into strict v_event
  from public.weekly_work_events event
  where event.id=v_work_event_id
    and event.candidate_id=v_family.candidate_id
    and event.client_id=v_client_id
    and event.work_date=v_work_date
  for share;
  if v_root.contract_id is distinct from v_family.contract_id
     or v_root.week_ending_date is distinct from v_family.week_ending_date
     or v_root.sheet_scope<>'WEEKLY'::public.timesheet_scope_enum
     or v_root.line_type<>'HOURS'::public.timesheet_line_type_enum
     or v_root.is_adjustment or not v_root.is_current
     or v_root.revoked_at is not null or v_root.archived_at_utc is not null then
    raise exception 'WEEKLY_PROTECTED_C1_ROOT_INVALID' using errcode='55000';
  end if;

  v_policy:=private._weekly_source_effective_policy_v1(
    v_client_id,v_family.contract_id,v_work_date
  );
  v_source_mode:=v_policy->>'c1_source_mode';
  if v_policy->>'authority_mode'<>'SOURCE_AUTHORITY'
     or coalesce((v_policy->>'self_bill_enabled')::boolean,false) is not true
     or v_source_mode not in ('NHSP_WEEKLY','HEALTHROSTER_WEEKLY')
     or (v_group.source_family='NHSP') is distinct from (v_source_mode='NHSP_WEEKLY') then
    raise exception 'WEEKLY_PROTECTED_C1_POLICY_INVALID' using errcode='55000';
  end if;

  if (v_start->>'actor_user_id')::uuid is distinct from v_actor_user_id
     or (v_start->>'candidate_id')::uuid is distinct from v_family.candidate_id
     or (v_start->>'contract_id')::uuid is distinct from v_family.contract_id
     or (v_start->>'root_timesheet_id')::uuid is distinct from v_root.timesheet_id
     or (v_start->>'week_ending_date')::date is distinct from v_family.week_ending_date
     or v_start->>'source_mode'<>v_source_mode
     or v_financial_row_id is distinct from v_fin.id
     or v_request_sequence<>v_family.current_generation_number+1 then
    raise exception 'WEEKLY_PROTECTED_C1_START_SCOPE_INVALID' using errcode='55000';
  end if;
  if v_family.current_generation_id is null then
    if v_expected_head_revision<>0 then
      raise exception 'WEEKLY_PROTECTED_C1_HEAD_INVALID' using errcode='55000';
    end if;
  else
    select generation.* into strict v_prior_generation
    from public.weekly_exceptional_pay_generations generation
    where generation.id=v_family.current_generation_id
      and generation.family_id=v_family.id
      and generation.lifecycle_state='PUBLISHED'
    for share;
    select request.* into strict v_publication
    from public.weekly_exceptional_c1_publication_requests request
    where request.generation_id=v_prior_generation.id
      and request.family_id=v_family.id
      and request.state='PUBLISHED';
    if v_publication.c1_head_revision is distinct from v_expected_head_revision then
      raise exception 'WEEKLY_PROTECTED_C1_HEAD_INVALID' using errcode='55000';
    end if;
    v_publication.id:=null;
  end if;

  if v_evidence_timesheet_id is not null then
    select evidence.* into strict v_evidence
    from public.timesheets evidence
    where evidence.timesheet_id=v_evidence_timesheet_id
      and evidence.contract_id=v_family.contract_id
      and evidence.week_ending_date=v_family.week_ending_date
      and evidence.sheet_scope='WEEKLY'::public.timesheet_scope_enum
      and evidence.is_current and evidence.revoked_at is null
      and evidence.archived_at_utc is null;
    v_signed_evidence:=private.weekly_exceptional_candidate_signed_evidence_v1(
      v_evidence_timesheet_id
    );
    v_signed_at:=(v_signed_evidence->>'signed_at_utc')::timestamptz;
    v_signed_revision:=(v_signed_evidence->>'timesheet_version')::integer;
    v_signed_hash:=private.weekly_exceptional_hex_sha256_v1(
      v_signed_evidence->>'signature_sha256'
    );
  end if;

  select coalesce(pg_catalog.array_agg(incident.id order by incident.id),'{}'::uuid[])
    into v_issue_ids
  from public.weekly_discrepancy_incidents incident
  where incident.source_group_id=v_group.id
    and incident.work_event_id=v_work_event_id
    and incident.candidate_id=v_family.candidate_id
    and incident.client_id=v_client_id;
  v_issue_ids_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_ISSUE_SET_V1',pg_catalog.to_jsonb(v_issue_ids)
  );
  v_schedule_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_SCHEDULE_V1',v_schedule
  );
  v_rate_policy_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_RATE_POLICY_V1',
    pg_catalog.jsonb_build_object(
      'contract_id',v_family.contract_id,
      'rates_json',(select contract.rates_json from public.contracts contract where contract.id=v_family.contract_id),
      'policy',v_policy,'classification',v_rate_classification
    )
  );
  v_source_proposal_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_SOURCE_PROPOSAL_V1',v_source_proposal
  );

  if v_comparison_revision_id is not null and not exists(
    select 1
    from public.weekly_issue_comparison_revisions comparison
    join public.weekly_discrepancy_incidents incident on incident.id=comparison.incident_id
    where comparison.id=v_comparison_revision_id
      and comparison.contract_id=v_family.contract_id
      and incident.source_group_id=v_group.id
      and incident.work_event_id=v_work_event_id
      and incident.candidate_id=v_family.candidate_id
      and incident.client_id=v_client_id
  ) then
    raise exception 'WEEKLY_PROTECTED_COMPARISON_SCOPE_INVALID' using errcode='55000';
  end if;
  if v_final_revision_id is not null and not exists(
    select 1 from public.weekly_source_final_revisions final_revision
    where final_revision.id=v_final_revision_id
      and final_revision.source_cycle_id=v_source_cycle_id
      and final_revision.state='CURRENT'
  ) then
    raise exception 'WEEKLY_PROTECTED_FINAL_SCOPE_INVALID' using errcode='55000';
  end if;

  -- Validate and persist the complete normalized C1 source stream.  The C1
  -- owner independently replays its TLV hashes before certification; these
  -- local checks prevent truncation, reordering or cross-root substitution.
  for v_source_entry in
    select value from pg_catalog.jsonb_array_elements(v_sources)
  loop
    v_source_counter:=v_source_counter+1;
    if not private.weekly_exceptional_json_keys_exact_v1(v_source_entry,v_source_keys)
       or not (v_source_entry ?& v_source_keys)
       or v_source_entry->>'record_type'<>'SOURCE'
       or pg_catalog.jsonb_typeof(v_source_entry->'parts')<>'array' then
      raise exception 'WEEKLY_PROTECTED_C1_SOURCE_INVALID' using errcode='22023';
    end if;
    begin
      v_source_ordinal:=(v_source_entry->>'source_ordinal')::integer;
      v_source_id:=(v_source_entry->>'source_id')::uuid;
      v_source_payload_bytes:=(v_source_entry->>'payload_bytes')::integer;
      v_source_part_count:=(v_source_entry->>'part_count')::integer;
    exception when others then
      raise exception 'WEEKLY_PROTECTED_C1_SOURCE_INVALID' using errcode='22023';
    end;
    if v_source_ordinal<>v_source_counter
       or v_source_ordinal not between 1 and v_expected_source_count
       or (v_source_entry->>'root_timesheet_id')::uuid is distinct from v_root.timesheet_id
       or (v_source_entry->>'candidate_id')::uuid is distinct from v_family.candidate_id
       or (v_source_entry->>'contract_id')::uuid is distinct from v_family.contract_id
       or v_source_payload_bytes not between 1 and 16384
       or v_source_part_count<>pg_catalog.jsonb_array_length(v_source_entry->'parts')
       or v_source_part_count not between 1 and 6 then
      raise exception 'WEEKLY_PROTECTED_C1_SOURCE_SCOPE_INVALID' using errcode='55000';
    end if;
    v_source_kind:=v_source_entry->>'authority_kind';
    if v_source_kind not in (
      'CANDIDATE_SUBMISSION','CLIENT_SOURCE','OFFICE_APPROVAL','ROOT_FINANCIAL',
      'PROVIDER','SOURCE_EXPENSE','ORDINARY_EXPENSE','APPROVED_COMPONENT'
    ) then
      raise exception 'WEEKLY_PROTECTED_C1_SOURCE_KIND_INVALID' using errcode='22023';
    end if;
    v_count_client:=v_count_client+(v_source_kind='CLIENT_SOURCE')::integer;
    v_count_candidate:=v_count_candidate+(v_source_kind='CANDIDATE_SUBMISSION')::integer;
    v_count_root:=v_count_root+(v_source_kind='ROOT_FINANCIAL')::integer;
    v_count_provider:=v_count_provider+(v_source_kind='PROVIDER')::integer;
    v_count_office:=v_count_office+(v_source_kind='OFFICE_APPROVAL')::integer;
    v_part_concat:=''::bytea;
    v_part_ordinal:=0;
    for v_part_entry in
      select value from pg_catalog.jsonb_array_elements(v_source_entry->'parts')
    loop
      v_part_ordinal:=v_part_ordinal+1;
      if not private.weekly_exceptional_json_keys_exact_v1(v_part_entry,v_part_keys)
         or not (v_part_entry ?& v_part_keys)
         or v_part_entry->>'record_type'<>'PART'
         or (v_part_entry->>'source_ordinal')::integer<>v_source_ordinal
         or (v_part_entry->>'part_ordinal')::integer<>v_part_ordinal
         or coalesce(v_part_entry->>'payload_utf8','')!~'^(?:[0-9a-f]{2})+$' then
        raise exception 'WEEKLY_PROTECTED_C1_PART_INVALID' using errcode='22023';
      end if;
      v_part_payload:=pg_catalog.decode(v_part_entry->>'payload_utf8','hex');
      if pg_catalog.octet_length(v_part_payload) not between 1 and 3072
         or extensions.digest(v_part_payload,'sha256') is distinct from
            private.weekly_exceptional_hex_sha256_v1(v_part_entry->>'fragment_sha256') then
        raise exception 'WEEKLY_PROTECTED_C1_PART_HASH_INVALID' using errcode='22023';
      end if;
      v_part_concat:=v_part_concat||v_part_payload;
    end loop;
    if v_part_ordinal<>v_source_part_count
       or pg_catalog.octet_length(v_part_concat)<>v_source_payload_bytes then
      raise exception 'WEEKLY_PROTECTED_C1_SOURCE_PAYLOAD_INVALID' using errcode='22023';
    end if;
    begin
      v_source_document:=pg_catalog.convert_from(v_part_concat,'UTF8')::jsonb;
    exception when others then
      raise exception 'WEEKLY_PROTECTED_C1_SOURCE_DOCUMENT_INVALID' using errcode='22023';
    end;
    if pg_catalog.jsonb_typeof(v_source_document)<>'object'
       or (v_source_document->>'root_timesheet_id')::uuid is distinct from v_root.timesheet_id
       or (v_source_document->>'candidate_id')::uuid is distinct from v_family.candidate_id
       or (v_source_document->>'contract_id')::uuid is distinct from v_family.contract_id
       or (v_source_document->>'week_ending_date')::date is distinct from v_family.week_ending_date
       or v_source_document->>'document_sha256' is distinct from
          v_source_entry->>'source_document_sha256' then
      raise exception 'WEEKLY_PROTECTED_C1_SOURCE_DOCUMENT_SCOPE_INVALID' using errcode='55000';
    end if;
    if v_source_kind='CLIENT_SOURCE'
       and coalesce((v_source_document->>'source_complete')::boolean,false) is not true then
      raise exception 'WEEKLY_PROTECTED_C1_CLIENT_SOURCE_INCOMPLETE' using errcode='55000';
    elsif v_source_kind='ROOT_FINANCIAL'
       and (v_source_document->>'financial_row_id')::uuid is distinct from v_fin.id then
      raise exception 'WEEKLY_PROTECTED_C1_ROOT_FINANCIAL_INVALID' using errcode='55000';
    elsif v_source_kind='OFFICE_APPROVAL'
       and (
         (v_source_document->>'actor_user_id')::uuid is distinct from v_actor_user_id
         or (v_source_document->>'request_id')::uuid is distinct from v_request_id
         or v_source_document->>'entitlement_sha256' is distinct from
            v_start->>'entitlement_sha256'
         or v_source_document->>'decision'<>'APPROVE_ENTITLEMENT'
       ) then
      raise exception 'WEEKLY_PROTECTED_C1_OFFICE_SOURCE_INVALID' using errcode='55000';
    end if;
    v_payload_total:=v_payload_total+v_source_payload_bytes;
  end loop;
  if v_source_counter<>v_expected_source_count
     or v_count_client<1 or v_count_root<>1 or v_count_provider<>1 or v_count_office<>1
     or (v_evidence_timesheet_id is null and v_count_candidate<>0)
     or (v_evidence_timesheet_id is not null and v_count_candidate<>1)
     or v_payload_total<>v_expected_payload_bytes then
    raise exception 'WEEKLY_PROTECTED_C1_SOURCE_CARDINALITY_INVALID' using errcode='55000';
  end if;

  v_component_ordinal:=0;
  for v_component_entry in
    select value from pg_catalog.jsonb_array_elements(v_components)
  loop
    v_component_ordinal:=v_component_ordinal+1;
    if not private.weekly_exceptional_json_keys_exact_v1(v_component_entry,v_component_keys)
       or not (v_component_entry ?& v_component_keys)
       or v_component_entry->>'record_type'<>'COMPONENT'
       or (v_component_entry->>'component_ordinal')::integer<>v_component_ordinal
       or (v_component_entry->>'source_ordinal')::integer not between 1 and v_expected_source_count
       or coalesce(v_component_entry->>'source_key','')=''
       or (v_previous_source_key is not null and v_component_entry->>'source_key'<=v_previous_source_key) then
      raise exception 'WEEKLY_PROTECTED_C1_COMPONENT_INVALID' using errcode='22023';
    end if;
    if nullif(v_component_entry->>'work_date','') is not null
       and (v_component_entry->>'work_date')::date not between
           v_family.week_start_date and v_family.week_ending_date then
      raise exception 'WEEKLY_PROTECTED_C1_COMPONENT_WEEK_INVALID' using errcode='55000';
    end if;
    v_total_pay:=v_total_pay+(v_component_entry->>'pay_ex_vat')::numeric;
    perform private.weekly_exceptional_hex_sha256_v1(v_component_entry->>'component_sha256');
    v_previous_source_key:=v_component_entry->>'source_key';
  end loop;
  if v_component_ordinal<>v_expected_component_count then
    raise exception 'WEEKLY_PROTECTED_C1_COMPONENT_COUNT_INVALID' using errcode='22023';
  end if;
  v_total_pay:=pg_catalog.round(v_total_pay,2);

  if v_family.current_generation_id is null then
    if v_run.request_kind<>'APPROVE' then
      raise exception 'WEEKLY_PROTECTED_C1_INITIAL_ACTION_INVALID' using errcode='55000';
    end if;
    v_prior_vector:=pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_PROTECTED_TARGET_VECTOR_V1',
      'components',pg_catalog.jsonb_build_array(),
      'component_count',0,'is_zero_entitlement',true
    );
    v_generation_number:=1;
    v_generation_reason:='INITIAL_APPROVAL';
  else
    v_prior_vector:=v_prior_generation.complete_next_vector_json;
    v_generation_number:=v_prior_generation.generation_number+1;
    v_generation_reason:=case v_run.request_kind
      when 'AMEND' then 'OFFICE_AMENDMENT'
      when 'WITHDRAW' then 'WITHDRAW'
      when 'RECONCILE' then 'RECONCILE'
      when 'RECORD_NOT_WORKED' then 'RECORD_NOT_WORKED'
      else null end;
    if v_generation_reason is null then
      raise exception 'WEEKLY_PROTECTED_C1_ACTION_INVALID' using errcode='55000';
    end if;
  end if;
  v_family_event_state:=case
    when v_run.request_kind in ('APPROVE','AMEND') then 'WAIT'
    when v_run.request_kind in ('WITHDRAW','RECONCILE') then 'ACCEPTED_SOURCE'
    else 'NOT_WORKED' end;
  if v_run.request_kind='RECORD_NOT_WORKED'
     and coalesce((v_source_proposal->>'source_present')::boolean,true) then
    raise exception 'WEEKLY_PROTECTED_NOT_WORKED_SOURCE_PRESENT' using errcode='55000';
  end if;
  with latest as (
    select family_event.durable_work_event_id,family_event.state,
      pg_catalog.row_number() over (
        partition by family_event.durable_work_event_id
        order by family_event.event_sequence desc
      ) as event_rank
    from public.weekly_exceptional_pay_family_events family_event
    where family_event.family_id=v_family.id
      and family_event.durable_work_event_id<>v_work_event_id
  )
  select pg_catalog.count(*)::integer into v_other_wait_count
  from latest where event_rank=1 and state='WAIT';
  v_resulting_lifecycle_state:=case
    when v_family_event_state='WAIT' or v_other_wait_count>0 then 'WAITING_SOURCE'
    when v_run.request_kind='RECORD_NOT_WORKED' then 'NOT_WORKED'
    else 'RECONCILED' end;
  v_pending_outcome:=case v_family_event_state
    when 'WAIT' then 'WAIT'
    when 'ACCEPTED_SOURCE' then 'ACCEPT_SOURCE'
    else 'RECORD_NOT_WORKED' end;
  v_payment_event_kind:=case v_run.request_kind
    when 'APPROVE' then 'APPROVED'
    when 'AMEND' then 'APPROVED'
    when 'RECORD_NOT_WORKED' then 'RECORDED_NOT_WORKED'
    else 'MATCH_ACCEPTED' end;
  v_prior_vector_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_COMPLETE_VECTOR_V1',v_prior_vector
  );
  v_next_vector:=pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_PROTECTED_TARGET_VECTOR_V1',
    'target_snapshot',v_target_snapshot,
    'components',v_components,
    'component_count',v_expected_component_count,
    'is_zero_entitlement',(v_expected_component_count=0),
    'entitlement_sha256',v_start->>'entitlement_sha256',
    'approved_pay_ex_vat',pg_catalog.to_char(v_total_pay,'FM9999999990.00')
  );
  v_next_vector_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_COMPLETE_VECTOR_V1',v_next_vector
  );
  v_fixed_source_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_FIXED_SOURCE_STATE_V1',
    pg_catalog.jsonb_build_object(
      'source_cycle_id',v_source_cycle_id,
      'comparison_revision_id',v_comparison_revision_id,
      'final_revision_id',v_final_revision_id,
      'source_proposal_hash',pg_catalog.encode(v_source_proposal_hash,'hex'),
      'financial_row_id',v_fin.id,'financial_timesheet_version',v_fin.timesheet_version
    )
  );
  v_local_approval_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_APPROVAL_V1',
    pg_catalog.jsonb_build_object(
      'family_id',v_family.id,'work_event_id',v_work_event_id,
      'candidate_id',v_family.candidate_id,'client_id',v_client_id,
      'contract_id',v_family.contract_id,'week_ending_date',v_family.week_ending_date,
      'schedule_hash',pg_catalog.encode(v_schedule_hash,'hex'),
      'evidence_timesheet_id',v_evidence_timesheet_id,
      'signed_hash',case when v_signed_hash is null then null else pg_catalog.encode(v_signed_hash,'hex') end,
      'issue_ids_hash',pg_catalog.encode(v_issue_ids_hash,'hex'),
      'rate_policy_hash',pg_catalog.encode(v_rate_policy_hash,'hex'),
      'target_vector_hash',pg_catalog.encode(v_next_vector_hash,'hex'),
      'actor_user_id',v_actor_user_id,'reason',v_reason
    )
  );

  insert into public.weekly_exceptional_payment_approvals(
    pay_target_family_id,work_event_id,evidence_timesheet_id,candidate_id,
    client_id,contract_id,week_ending,protected_work_date,
    protected_start_at_local,protected_end_at_local,protected_break_minutes,
    signed_submission_timesheet_id,signed_submission_revision,
    signed_submission_hash,signed_at_utc,contributing_issue_episode_ids,
    contributing_issue_episode_ids_hash,signed_schedule_fact_hash,
    contract_rate_policy_source_fingerprint,approved_by_user_id,
    approval_reason,source_cycle_id,comparison_revision_id,final_revision_id,
    approved_target_pay_components_json,approved_target_gross,
    creation_orchestration_run_id,approval_hash,creation_idempotency_key
  ) values (
    v_family.id,v_work_event_id,v_evidence_timesheet_id,v_family.candidate_id,
    v_client_id,v_family.contract_id,v_family.week_ending_date,v_work_date,
    v_start_at,v_end_at,v_break_minutes,
    v_evidence_timesheet_id,v_signed_revision,v_signed_hash,v_signed_at,v_issue_ids,
    v_issue_ids_hash,v_schedule_hash,v_rate_policy_hash,v_actor_user_id,
    v_reason,v_source_cycle_id,v_comparison_revision_id,v_final_revision_id,
    v_target_snapshot,v_total_pay,v_run.id,v_local_approval_hash,
    v_idempotency_key||':approval'
  ) returning * into v_approval;

  insert into public.weekly_exceptional_pay_generations(
    family_id,generation_number,prior_generation_id,prior_generation_hash,
    request_idempotency_key,reason,complete_prior_vector_json,
    complete_prior_vector_hash,complete_next_vector_json,complete_next_vector_hash,
    fixed_target_source_state_fingerprint,lifecycle_state
  ) values (
    v_family.id,v_generation_number,v_family.current_generation_id,
    case when v_family.current_generation_id is null then null else v_prior_vector_hash end,
    v_idempotency_key||':generation',v_generation_reason,v_prior_vector,
    v_prior_vector_hash,v_next_vector,v_next_vector_hash,v_fixed_source_hash,'PENDING_C1'
  ) returning * into v_generation;

  select coalesce(max(event_sequence),0)+1,
         (array_agg(event_hash order by event_sequence desc))[1]
    into v_family_event_sequence,v_prior_family_event_hash
  from public.weekly_exceptional_pay_family_events
  where family_id=v_family.id;
  v_family_event_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_FAMILY_EVENT_V1',
    pg_catalog.jsonb_build_object(
      'family_id',v_family.id,'event_sequence',v_family_event_sequence,
      'work_event_id',v_work_event_id,'approval_id',v_approval.id,
      'schedule',v_schedule,'source_proposal_hash',pg_catalog.encode(v_source_proposal_hash,'hex'),
      'target_vector_hash',pg_catalog.encode(v_next_vector_hash,'hex'),
      'prior_event_hash',case when v_prior_family_event_hash is null then null
        else pg_catalog.encode(v_prior_family_event_hash,'hex') end
    )
  );
  insert into public.weekly_exceptional_pay_family_events(
    family_id,event_sequence,durable_work_event_id,evidence_approval_id,
    work_date,start_at_local,end_at_local,break_minutes,rate_classification_json,
    source_proposal_snapshot_json,source_proposal_hash,
    fixed_office_target_snapshot_json,fixed_office_target_hash,state,
    current_comparison_revision_id,current_final_revision_id,office_actor_user_id,
    office_reason,prior_event_hash,event_hash
  ) values (
    v_family.id,v_family_event_sequence,v_work_event_id,v_approval.id,
    v_work_date,v_start_at,v_end_at,v_break_minutes,v_rate_classification,
    v_source_proposal,v_source_proposal_hash,v_schedule,v_schedule_hash,v_family_event_state,
    v_comparison_revision_id,v_final_revision_id,v_actor_user_id,v_reason,
    v_prior_family_event_hash,v_family_event_hash
  );

  select coalesce(max(event_sequence),0)+1,
         (array_agg(event_hash order by event_sequence desc))[1]
    into v_target_event_sequence,v_prior_target_event_hash
  from public.weekly_exceptional_pay_target_events
  where family_id=v_family.id;
  v_target_event_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_TARGET_EVENT_V1',
    pg_catalog.jsonb_build_object(
      'family_id',v_family.id,'event_sequence',v_target_event_sequence,
      'approval_id',v_approval.id,'generation_id',v_generation.id,
      'prior_vector_hash',pg_catalog.encode(v_prior_vector_hash,'hex'),
      'next_vector_hash',pg_catalog.encode(v_next_vector_hash,'hex'),
      'reason',v_generation_reason,'actor_user_id',v_actor_user_id
    )
  );
  insert into public.weekly_exceptional_pay_target_events(
    family_id,approval_id,event_sequence,triggering_comparison_revision_id,
    triggering_final_revision_id,prior_event_fingerprint,
    fixed_target_component_snapshot,current_source_proposal_snapshot,
    complete_prior_family_vector_fingerprint,
    complete_next_family_vector_fingerprint,reason,resulting_lifecycle_state,
    financial_generation_id,actor_user_id,event_hash,idempotency_key
  ) values (
    v_family.id,v_approval.id,v_target_event_sequence,v_comparison_revision_id,
    v_final_revision_id,v_prior_target_event_hash,v_target_snapshot,v_source_proposal,
    v_prior_vector_hash,v_next_vector_hash,v_generation_reason,v_resulting_lifecycle_state,
    v_generation.id,v_actor_user_id,v_target_event_hash,v_idempotency_key||':target-event'
  );
  update public.weekly_exceptional_pending_reconciliation_targets target
  set state=case when v_run.request_kind='WITHDRAW' then 'WITHDRAWN' else 'CONSUMED' end,
      completed_at_utc=pg_catalog.statement_timestamp()
  where target.family_id=v_family.id
    and target.durable_work_event_id=v_work_event_id
    and target.state='ACTIVE';
  if v_family_event_state='WAIT' then
    insert into public.weekly_exceptional_pending_reconciliation_targets(
      family_id,approval_id,durable_work_event_id,incident_id,current_final_revision_id,
      intended_outcome,source_action_policy_target_fingerprint,state
    ) values (
      v_family.id,v_approval.id,v_work_event_id,
      (select incident.id from public.weekly_discrepancy_incidents incident
       where incident.source_group_id=v_group.id and incident.work_event_id=v_work_event_id
       order by incident.episode_number desc limit 1),
      v_final_revision_id,v_pending_outcome,v_fixed_source_hash,'ACTIVE'
    );
  end if;
  insert into public.weekly_exceptional_payment_events(
    family_id,approval_id,event_kind,lifecycle_view,bounded_payload_json,
    idempotency_key
  ) values (
    v_family.id,v_approval.id,v_payment_event_kind,'PENDING_C1',
    pg_catalog.jsonb_build_object(
      'work_event_id',v_work_event_id,'generation_id',v_generation.id,
      'target_vector_hash',pg_catalog.encode(v_next_vector_hash,'hex'),
      'approved_pay_ex_vat',pg_catalog.to_char(v_total_pay,'FM9999999990.00')
    ),v_idempotency_key||':payment-event'
  );

  insert into public.weekly_exceptional_c1_publication_requests(
    id,family_id,generation_id,orchestration_run_id,agency_id,candidate_id,
    contract_id,root_timesheet_id,week_ending_date,source_mode,request_sequence,
    expected_head_revision,expected_source_count,expected_component_count,
    expected_payload_bytes,source_manifest_sha256,entitlement_sha256,
    approval_sha256,is_zero_entitlement,financial_row_id,request_sha256,state
  ) values (
    v_request_id,v_family.id,v_generation.id,v_run.id,v_family.agency_id,
    v_family.candidate_id,v_family.contract_id,v_root.timesheet_id,
    v_family.week_ending_date,v_source_mode,v_request_sequence,
    v_expected_head_revision,v_expected_source_count,v_expected_component_count,
    v_expected_payload_bytes,v_source_manifest_hash,v_entitlement_hash,
    v_outer_approval_hash,(v_expected_component_count=0),v_fin.id,
    v_c1_request_hash,'READY'
  ) returning * into v_publication;

  for v_source_entry in select value from pg_catalog.jsonb_array_elements(v_sources)
  loop
    v_source_ordinal:=(v_source_entry->>'source_ordinal')::integer;
    v_source_id:=(v_source_entry->>'source_id')::uuid;
    insert into public.weekly_exceptional_c1_source_records(
      id,publication_request_id,source_ordinal,authority_kind,source_system,
      external_identity,external_revision,source_document_sha256,payload_bytes,
      part_count,work_date,root_timesheet_id,candidate_id,contract_id,
      source_row_sha256
    ) values (
      v_source_id,v_publication.id,v_source_ordinal,v_source_entry->>'authority_kind',
      v_source_entry->>'source_system',v_source_entry->>'external_identity',
      v_source_entry->>'external_revision',
      private.weekly_exceptional_hex_sha256_v1(v_source_entry->>'source_document_sha256'),
      (v_source_entry->>'payload_bytes')::integer,
      (v_source_entry->>'part_count')::integer,
      nullif(v_source_entry->>'work_date','')::date,v_root.timesheet_id,
      v_family.candidate_id,v_family.contract_id,
      private.weekly_exceptional_hex_sha256_v1(v_source_entry->>'source_row_sha256')
    );
    for v_part_entry in select value from pg_catalog.jsonb_array_elements(v_source_entry->'parts')
    loop
      insert into public.weekly_exceptional_c1_source_parts(
        publication_request_id,source_record_id,source_ordinal,part_ordinal,
        payload_utf8,fragment_sha256
      ) values (
        v_publication.id,v_source_id,v_source_ordinal,
        (v_part_entry->>'part_ordinal')::integer,
        pg_catalog.decode(v_part_entry->>'payload_utf8','hex'),
        private.weekly_exceptional_hex_sha256_v1(v_part_entry->>'fragment_sha256')
      );
    end loop;
  end loop;

  for v_component_entry in select value from pg_catalog.jsonb_array_elements(v_components)
  loop
    insert into public.weekly_exceptional_c1_component_records(
      publication_request_id,component_ordinal,component_id,source_ordinal,
      source_key,component_kind,economic_key_type,economic_key_value,
      component_member_identity,segment_id,segment_key,segment_stable_key,
      work_date,reference_number,hours_day,hours_night,hours_sat,hours_sun,
      hours_bh,additional_code_raw,unit_count,unit_pay_rate,unit_charge_rate,
      expense_code,pay_ex_vat,charge_ex_vat,exclude_from_pay,
      origin,component_sha256
    ) values (
      v_publication.id,(v_component_entry->>'component_ordinal')::integer,
      (v_component_entry->>'component_id')::uuid,
      (v_component_entry->>'source_ordinal')::integer,
      v_component_entry->>'source_key',v_component_entry->>'component_kind',
      v_component_entry->>'economic_key_type',v_component_entry->>'economic_key_value',
      v_component_entry->>'component_member_identity',nullif(v_component_entry->>'segment_id',''),
      nullif(v_component_entry->>'segment_key',''),nullif(v_component_entry->>'segment_stable_key',''),
      nullif(v_component_entry->>'work_date','')::date,nullif(v_component_entry->>'reference_number',''),
      nullif(v_component_entry->>'hours_day','')::numeric,
      nullif(v_component_entry->>'hours_night','')::numeric,
      nullif(v_component_entry->>'hours_sat','')::numeric,
      nullif(v_component_entry->>'hours_sun','')::numeric,
      nullif(v_component_entry->>'hours_bh','')::numeric,
      nullif(v_component_entry->>'additional_code_raw',''),
      nullif(v_component_entry->>'unit_count','')::numeric,
      nullif(v_component_entry->>'unit_pay_rate','')::numeric,
      nullif(v_component_entry->>'unit_charge_rate','')::numeric,
      nullif(v_component_entry->>'expense_code',''),
      (v_component_entry->>'pay_ex_vat')::numeric,
      nullif(v_component_entry->>'charge_ex_vat','')::numeric,
      (v_component_entry->>'exclude_from_pay')::boolean,
      v_component_entry->>'origin',
      private.weekly_exceptional_hex_sha256_v1(v_component_entry->>'component_sha256')
    );
  end loop;

  update public.weekly_exceptional_pay_target_families
  set c1_publication_state='PENDING',bound_version=bound_version+1
  where id=v_family.id;

  v_request_body_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_C1_STAGE_REQUEST_BODY_V1',p_request
  );
  v_before_hash:=v_run.before_state_fingerprint;
  v_after_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_C1_STAGED_STATE_V1',
    pg_catalog.jsonb_build_object(
      'family_id',v_family.id,'generation_id',v_generation.id,
      'publication_request_id',v_publication.id,'request_sha256',
      pg_catalog.encode(v_c1_request_hash,'hex'),'new_bound_version',v_family.bound_version+1
    )
  );
  select coalesce(max(sequence),0)+1 into v_step_sequence
  from public.weekly_exceptional_orchestration_steps where orchestration_run_id=v_run.id;
  insert into public.weekly_exceptional_orchestration_steps(
    orchestration_run_id,sequence,step_kind,idempotency_key,
    allowlisted_owner_name,allowlisted_owner_signature,bounded_request_hash,
    before_state_fingerprint,bounded_owner_response_json,owner_response_hash,
    after_state_fingerprint,outcome,completed_at_utc
  ) values (
    v_run.id,v_step_sequence,'STAGE_C1_REQUEST',v_idempotency_key,
    'public.weekly_exceptional_pay_stage_c1_request_v1','jsonb->jsonb',
    v_request_body_hash,v_before_hash,
    pg_catalog.jsonb_build_object(
      'publication_request_id',v_publication.id,'generation_id',v_generation.id,
      'request_sha256',pg_catalog.encode(v_c1_request_hash,'hex')
    ),v_after_hash,v_after_hash,'COMPLETE',pg_catalog.statement_timestamp()
  );

  return pg_catalog.jsonb_build_object(
    'ok',true,'outcome','STAGED','idempotent_replay',false,
    'family_id',v_family.id,'generation_id',v_generation.id,
    'approval_id',v_approval.id,'publication_request_id',v_publication.id,
    'request_sha256',pg_catalog.encode(v_publication.request_sha256,'hex'),
    'source_count',v_expected_source_count,
    'component_count',v_expected_component_count,
    'approved_pay_ex_vat',pg_catalog.to_char(v_total_pay,'FM9999999990.00'),
    'new_family_bound_version',v_family.bound_version+1,'state','READY'
  );
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_PROTECTED_C1_STAGE_SCOPE_INVALID' using errcode='55000';
end;
$function$;

create or replace function public.weekly_exceptional_pay_complete_c1_publication_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_keys constant text[]:=array[
    'schema_version','actor_user_id','publication_request_id',
    'expected_request_sha256','idempotency_key'
  ];
  v_actor_user_id uuid;
  v_request_id uuid;
  v_expected_request_hash bytea;
  v_idempotency_key text;
  v_publication public.weekly_exceptional_c1_publication_requests%rowtype;
  v_generation public.weekly_exceptional_pay_generations%rowtype;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_checkpoint public.weekly_exceptional_c1_publication_checkpoints%rowtype;
  v_result jsonb;
  v_publication_id uuid;
  v_head_revision bigint;
  v_receipt_hash bytea;
  v_request_hash bytea;
  v_target_state text;
  v_source_hash bytea;
  v_step_sequence integer;
  v_before_hash bytea;
  v_after_hash bytea;
begin
  if not private.weekly_exceptional_json_keys_exact_v1(p_request,v_keys)
     or not (p_request ?& v_keys)
     or p_request->>'schema_version'<>'WEEKLY_PROTECTED_C1_COMPLETE_V1' then
    raise exception 'WEEKLY_PROTECTED_C1_COMPLETE_INVALID' using errcode='22023';
  end if;
  begin
    v_actor_user_id:=(p_request->>'actor_user_id')::uuid;
    v_request_id:=(p_request->>'publication_request_id')::uuid;
    v_expected_request_hash:=private.weekly_exceptional_hex_sha256_v1(
      p_request->>'expected_request_sha256'
    );
  exception when others then
    raise exception 'WEEKLY_PROTECTED_C1_COMPLETE_INVALID' using errcode='22023';
  end;
  v_idempotency_key:=pg_catalog.btrim(coalesce(p_request->>'idempotency_key',''));
  if pg_catalog.char_length(v_idempotency_key) not between 16 and 240 then
    raise exception 'WEEKLY_PROTECTED_C1_COMPLETE_INVALID' using errcode='22023';
  end if;

  select request.* into strict v_publication
  from public.weekly_exceptional_c1_publication_requests request
  where request.id=v_request_id
  for update;
  select generation.* into strict v_generation
  from public.weekly_exceptional_pay_generations generation
  where generation.id=v_publication.generation_id
    and generation.family_id=v_publication.family_id
  for update;
  select family.* into strict v_family
  from public.weekly_exceptional_pay_target_families family
  where family.id=v_publication.family_id
  for update;
  select run.* into strict v_run
  from public.weekly_exceptional_orchestration_runs run
  where run.id=v_publication.orchestration_run_id
  for update;

  if v_publication.request_sha256 is distinct from v_expected_request_hash
     or v_run.requested_by_user_id is distinct from v_actor_user_id
     or not exists(
       select 1 from public.tms_users office_user
       where office_user.id=v_actor_user_id and office_user.is_active
         and (office_user.payment_authoriser or office_user.payment_golden_key)
     ) then
    raise exception 'WEEKLY_PROTECTED_C1_COMPLETE_SCOPE_INVALID' using errcode='55000';
  end if;
  if v_publication.state='PUBLISHED' then
    if v_generation.lifecycle_state<>'PUBLISHED'
       or v_family.current_generation_id is distinct from v_generation.id
       or v_publication.c1_publication_id is null
       or v_publication.c1_head_revision is null
       or v_publication.c1_receipt_sha256 is null then
      raise exception 'WEEKLY_PROTECTED_C1_COMPLETE_STATE_INVALID' using errcode='55000';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'outcome','PUBLISHED','idempotent_replay',true,
      'publication_request_id',v_publication.id,
      'family_id',v_family.id,'generation_id',v_generation.id,
      'c1_publication_id',v_publication.c1_publication_id,
      'c1_head_revision',v_publication.c1_head_revision::text,
      'state','LIVE'
    );
  end if;
  if v_publication.state<>'SUBMITTED'
     or v_generation.lifecycle_state<>'PENDING_C1'
     or v_family.c1_publication_state<>'PUBLISHING'
     or exists(
       select 1 from public.weekly_exceptional_c1_unknown_outcomes unknown_outcome
       where unknown_outcome.publication_request_id=v_publication.id
         and unknown_outcome.state='RECOVERY_REQUIRED'
     ) then
    raise exception 'WEEKLY_PROTECTED_C1_COMPLETE_STATE_INVALID' using errcode='55000';
  end if;

  select checkpoint.* into strict v_checkpoint
  from public.weekly_exceptional_c1_publication_checkpoints checkpoint
  where checkpoint.publication_request_id=v_publication.id
  order by checkpoint.checkpoint_sequence desc
  limit 1
  for share;
  v_result:=v_checkpoint.result_json;
  if v_checkpoint.phase<>'PUBLISH'
     or pg_catalog.upper(coalesce(v_result->>'status',''))<>'PUBLISHED'
     or pg_catalog.jsonb_typeof(v_result->'has_more')<>'boolean'
     or (v_result->>'has_more')::boolean
     or nullif(v_result->>'publication_id','') is null
     or nullif(v_result->>'head_revision','') is null
     or nullif(v_result->>'receipt_sha256','') is null
     or nullif(v_result->>'request_sha256','') is null then
    raise exception 'WEEKLY_PROTECTED_C1_COMPLETE_RESULT_INVALID' using errcode='55000';
  end if;
  begin
    v_publication_id:=(v_result->>'publication_id')::uuid;
    v_head_revision:=(v_result->>'head_revision')::bigint;
    v_receipt_hash:=private.weekly_exceptional_hex_sha256_v1(v_result->>'receipt_sha256');
    v_request_hash:=private.weekly_exceptional_hex_sha256_v1(v_result->>'request_sha256');
  exception when others then
    raise exception 'WEEKLY_PROTECTED_C1_COMPLETE_RESULT_INVALID' using errcode='55000';
  end;
  if v_head_revision<1
     or v_request_hash is distinct from v_publication.request_sha256
     or (v_publication.c1_operation_id is not null
         and (v_result->>'operation_id')::uuid is distinct from v_publication.c1_operation_id) then
    raise exception 'WEEKLY_PROTECTED_C1_COMPLETE_RESULT_INVALID' using errcode='55000';
  end if;

  select target_event.resulting_lifecycle_state into strict v_target_state
  from public.weekly_exceptional_pay_target_events target_event
  where target_event.family_id=v_family.id
    and target_event.financial_generation_id=v_generation.id
  order by target_event.event_sequence desc limit 1;
  if v_target_state not in (
    'PENDING_APPROVAL','PROTECTED','WAITING_SOURCE','READY_TO_RECONCILE',
    'RECONCILED','NOT_WORKED','ACTION_REQUIRED'
  ) then
    raise exception 'WEEKLY_PROTECTED_C1_COMPLETE_TARGET_INVALID' using errcode='55000';
  end if;
  select family_event.source_proposal_hash into strict v_source_hash
  from public.weekly_exceptional_pay_family_events family_event
  where family_event.family_id=v_family.id
    and family_event.evidence_approval_id in (
      select approval.id from public.weekly_exceptional_payment_approvals approval
      where approval.pay_target_family_id=v_family.id
    )
  order by family_event.event_sequence desc limit 1;

  v_before_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_C1_COMPLETE_BEFORE_V1',
    pg_catalog.jsonb_build_object(
      'family_id',v_family.id,'family_bound_version',v_family.bound_version,
      'prior_generation_id',v_family.current_generation_id,
      'new_generation_id',v_generation.id,
      'publication_request_id',v_publication.id
    )
  );
  if v_family.current_generation_id is not null
     and v_family.current_generation_id is distinct from v_generation.id then
    update public.weekly_exceptional_pay_generations
    set lifecycle_state='SUPERSEDED',superseded_by_generation_id=v_generation.id,
        superseded_at_utc=pg_catalog.statement_timestamp()
    where id=v_family.current_generation_id
      and family_id=v_family.id
      and lifecycle_state='PUBLISHED';
    if not found then
      raise exception 'WEEKLY_PROTECTED_C1_PRIOR_GENERATION_INVALID' using errcode='55000';
    end if;
  end if;

  update public.weekly_exceptional_pay_generations
  set lifecycle_state='PUBLISHED',published_at_utc=pg_catalog.statement_timestamp(),
      result_hash=v_receipt_hash
  where id=v_generation.id and lifecycle_state='PENDING_C1';
  if not found then
    raise exception 'WEEKLY_PROTECTED_C1_GENERATION_INVALID' using errcode='55000';
  end if;
  update public.weekly_exceptional_c1_publication_requests
  set state='PUBLISHED',c1_publication_id=v_publication_id,
      c1_head_revision=v_head_revision,c1_receipt_sha256=v_receipt_hash,
      typed_result_json=v_result,completed_at_utc=pg_catalog.statement_timestamp()
  where id=v_publication.id;
  update public.weekly_exceptional_pay_target_families
  set ownership_state='TARGET_MANAGED',current_generation_id=v_generation.id,
      current_generation_number=v_generation.generation_number,
      current_complete_target_vector_hash=v_generation.complete_next_vector_hash,
      current_source_proposal_hash=v_source_hash,
      current_lifecycle_state=v_target_state,
      c1_publication_state='LIVE',
      current_component_count=v_publication.expected_component_count,
      bound_version=bound_version+1
  where id=v_family.id;
  update public.weekly_exceptional_orchestration_runs
  set state='COMPLETE',after_state_fingerprint=v_receipt_hash,
      completed_at_utc=pg_catalog.statement_timestamp()
  where id=v_run.id;
  insert into public.weekly_exceptional_payment_events(
    family_id,event_kind,lifecycle_view,bounded_payload_json,idempotency_key
  ) values (
    v_family.id,'PROTECTED_STATE_OBSERVED','LIVE',
    pg_catalog.jsonb_build_object(
      'generation_id',v_generation.id,
      'publication_request_id',v_publication.id,
      'c1_head_revision',v_head_revision::text
    ),v_idempotency_key||':payment-event'
  );
  v_after_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_C1_COMPLETE_AFTER_V1',
    pg_catalog.jsonb_build_object(
      'family_id',v_family.id,'generation_id',v_generation.id,
      'target_vector_hash',pg_catalog.encode(v_generation.complete_next_vector_hash,'hex'),
      'c1_publication_id',v_publication_id,'c1_head_revision',v_head_revision::text,
      'c1_receipt_sha256',pg_catalog.encode(v_receipt_hash,'hex')
    )
  );
  select coalesce(max(step.sequence),0)+1 into v_step_sequence
  from public.weekly_exceptional_orchestration_steps step
  where step.orchestration_run_id=v_run.id;
  insert into public.weekly_exceptional_orchestration_steps(
    orchestration_run_id,sequence,step_kind,idempotency_key,
    allowlisted_owner_name,allowlisted_owner_signature,bounded_request_hash,
    before_state_fingerprint,bounded_owner_response_json,owner_response_hash,
    after_state_fingerprint,outcome,completed_at_utc
  ) values (
    v_run.id,v_step_sequence,'COMPLETE_C1_PUBLICATION',v_idempotency_key,
    'C1.weekly_source_publish_c1','sealed C1 V1',v_expected_request_hash,
    v_before_hash,v_result,v_receipt_hash,v_after_hash,'COMPLETE',
    pg_catalog.statement_timestamp()
  );
  insert into public.audit_events(
    ts_utc,actor_user_id,actor_display,actor_role_at_time,
    object_type,object_id_text,action,before_json,after_json,reason
  )
  select pg_catalog.statement_timestamp(),v_actor_user_id,actor.display_name,actor.role,
    'weekly_exceptional_pay_target_families',v_family.id::text,
    'WEEKLY_PROTECTED_C1_PUBLISHED',
    pg_catalog.jsonb_build_object('prior_generation_id',v_family.current_generation_id),
    pg_catalog.jsonb_build_object(
      'generation_id',v_generation.id,'publication_request_id',v_publication.id,
      'c1_publication_id',v_publication_id,'c1_head_revision',v_head_revision::text,
      'component_count',v_publication.expected_component_count
    ),'Approved hours published through the existing pay route.'
  from public.tms_users actor where actor.id=v_actor_user_id;
  return pg_catalog.jsonb_build_object(
    'ok',true,'outcome','PUBLISHED','idempotent_replay',false,
    'publication_request_id',v_publication.id,
    'family_id',v_family.id,'generation_id',v_generation.id,
    'c1_publication_id',v_publication_id,
    'c1_head_revision',v_head_revision::text,
    'state','LIVE','family_bound_version',v_family.bound_version+1
  );
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_PROTECTED_C1_COMPLETE_SCOPE_INVALID' using errcode='55000';
end;
$function$;

alter function private.weekly_exceptional_hex_sha256_v1(text) owner to postgres;
alter function private.weekly_exceptional_json_keys_exact_v1(jsonb,text[]) owner to postgres;
alter function public.weekly_exceptional_pay_record_c1_unknown_v1(jsonb) owner to postgres;
alter function public.weekly_exceptional_pay_record_c1_checkpoint_v1(jsonb) owner to postgres;
alter function public.weekly_exceptional_pay_record_c1_recovery_v1(jsonb) owner to postgres;
alter function public.weekly_exceptional_pay_read_c1_request_v1(jsonb) owner to postgres;
alter function public.weekly_exceptional_pay_stage_c1_request_v1(jsonb) owner to postgres;
alter function public.weekly_exceptional_pay_complete_c1_publication_v1(jsonb) owner to postgres;

revoke all on function private.weekly_exceptional_hex_sha256_v1(text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_exceptional_json_keys_exact_v1(jsonb,text[])
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_exceptional_pay_record_c1_unknown_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_exceptional_pay_record_c1_checkpoint_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_exceptional_pay_record_c1_recovery_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_exceptional_pay_read_c1_request_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_exceptional_pay_stage_c1_request_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_exceptional_pay_complete_c1_publication_v1(jsonb)
  from public,anon,authenticated,service_role;
grant execute on function public.weekly_exceptional_pay_record_c1_unknown_v1(jsonb)
  to service_role;
grant execute on function public.weekly_exceptional_pay_record_c1_checkpoint_v1(jsonb)
  to service_role;
grant execute on function public.weekly_exceptional_pay_record_c1_recovery_v1(jsonb)
  to service_role;
grant execute on function public.weekly_exceptional_pay_read_c1_request_v1(jsonb)
  to service_role;
grant execute on function public.weekly_exceptional_pay_stage_c1_request_v1(jsonb)
  to service_role;
grant execute on function public.weekly_exceptional_pay_complete_c1_publication_v1(jsonb)
  to service_role;

comment on function public.weekly_exceptional_pay_record_c1_unknown_v1(jsonb) is
  'Service-only durable stop for one unknown C1 transport outcome. Stores the exact non-retry recovery envelope and changes no entitlement, Draft, payment or invoice fact.';

comment on function public.weekly_exceptional_pay_record_c1_checkpoint_v1(jsonb) is
  'Service-only append of one definite C1 publication cursor/result. Enforces exact order and offsets before another C1 call may start.';

comment on function public.weekly_exceptional_pay_record_c1_recovery_v1(jsonb) is
  'Service-only one-time resolution of a previously recorded unknown C1 transport outcome. A partial STAGE may use only the sealed exact replay authorised by C1; all other uncertain outcomes stop safely.';

comment on function public.weekly_exceptional_pay_read_c1_request_v1(jsonb) is
  'Service-only reconstruction of the exact immutable C1 publication request and record stream, with durable checkpoints and unknown-outcome state. Does not create or change pay, Draft, invoice or Banking facts.';

comment on function public.weekly_exceptional_pay_stage_c1_request_v1(jsonb) is
  'Service-only atomic persistence of one complete Office-approved protected Weekly entitlement and its exact immutable C1 stream. Does not publish, calculate a residual, create a Draft, invoice or Banking Pay row.';

comment on function public.weekly_exceptional_pay_complete_c1_publication_v1(jsonb) is
  'Service-only atomic completion after a definite sealed C1 PUBLISHED receipt. Plan 6.2 (24 section 4.3): this is C1 TRANSPORT bookkeeping only and is NOT the Candidate entitlement authority. The single current complete entitlement for a root, for both PROTECTED and LOCKED_FINAL_SOURCE, is public.weekly_source_entitlement_heads, published only by the Gate 5 coordinator; the Workbench reads that relation and never reads C1 staging or checkpoint tables. This owner calculates no residual, creates no Draft, does not invoice and changes no Banking Pay state. The disposition of the external C1 publication itself once the CloudTMS-side head exists is an OPEN QUESTION recorded in IMPL\reports\WP-06_DESIGN.md section 4.2 and is deliberately unchanged here.';

notify pgrst, 'reload schema';

commit;
