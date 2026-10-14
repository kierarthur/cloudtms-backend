-- WP-PP: one pay actor predicate; approved R-195/R-201/R-216 scope only.
-- Banking W1-W12/S11 consumers remain separately owned. No payment economics.
-- Current-source successors, not NEXT-only definitions; re-integrate current
-- final Banking owners before publication if the canonical TEST head advances.
\set ON_ERROR_STOP on
begin;

create or replace function private.weekly_source_pay_actor_allowed_v1(p_actor_user_id uuid)
returns boolean language sql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
  select exists(select 1 from public.tms_users actor
    where actor.id=p_actor_user_id and actor.is_active is true
      and (lower(btrim(coalesce(actor.role,'')))='admin'
        or coalesce(actor.payment_authoriser,false)
        or coalesce(actor.payment_golden_key,false)));
$function$;

-- Current TEST source: supabase/repeatable/15092026_1534_00_weekly_source_private_classifiers_v1.sql
-- Scope: WP-PP actor/read-view changes only.
create or replace function private.weekly_source_office_authority_v1(
  p_actor_user_id uuid,
  p_operation text,
  p_source_group_id uuid default null,
  p_client_id uuid default null,
  p_scope_date date default null
) returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_actor public.tms_users%rowtype;
  v_operation text:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_operation,'')));
  v_payment_operation boolean;
  v_scope_date date:=coalesce(p_scope_date,current_date);
  v_payload jsonb;
begin
  if p_actor_user_id is null or v_operation not in (
    'OPEN_QUERY_NOTICE','VIEW_SOURCE_PROGRESS','VIEW_PAY_WORK','OPEN_PAY_QUERY','UPLOAD_SOURCE','SEAL_SOURCE',
    'SUPERSEDE_SOURCE','RECHECK_SOURCE','ASK_CANDIDATES','SEND_MANAGER',
    'RESEND_MANAGER','ACCEPT_SYSTEM_HOURS','FINALISE_WEEK',
    'ADMIT_SOURCE_INVOICE','MOVE_SOURCE_INVOICE',
    'APPROVE_PROTECTED_PAY','AMEND_PROTECTED_PAY','WITHDRAW_PROTECTED_PAY',
    'RECORD_NOT_WORKED','RECONCILE_PROTECTED_PAY','CORRECT_FINAL_SOURCE'
  ) then
    raise exception 'WEEKLY_SOURCE_OFFICE_OPERATION_INVALID' using errcode='22023';
  end if;

  select * into v_actor
  from public.tms_users
  where id=p_actor_user_id;
  if not found or not v_actor.is_active then
    raise exception 'WEEKLY_SOURCE_OFFICE_ACTOR_INACTIVE' using errcode='42501';
  end if;

  v_payment_operation:=v_operation in (
    'APPROVE_PROTECTED_PAY','AMEND_PROTECTED_PAY','WITHDRAW_PROTECTED_PAY',
    'RECORD_NOT_WORKED','RECONCILE_PROTECTED_PAY','ACCEPT_SYSTEM_HOURS',
    'OPEN_PAY_QUERY','VIEW_PAY_WORK'
  );
  if v_payment_operation then
    if not private.weekly_source_pay_actor_allowed_v1(p_actor_user_id) then
      raise exception 'WEEKLY_SOURCE_PAYMENT_AUTHORISER_REQUIRED' using errcode='42501';
    end if;
  elsif v_operation<>'OPEN_QUERY_NOTICE'
     and pg_catalog.lower(pg_catalog.btrim(coalesce(v_actor.role,'')))<>'admin' then
    raise exception 'WEEKLY_SOURCE_OFFICE_ADMIN_REQUIRED' using errcode='42501';
  end if;

  if p_source_group_id is not null and not exists(
    select 1 from public.weekly_source_groups source_group
    where source_group.id=p_source_group_id and source_group.active
  ) then
    raise exception 'WEEKLY_SOURCE_GROUP_NOT_ACTIVE' using errcode='22023';
  end if;

  if p_client_id is not null and (
    p_source_group_id is null or not exists(
      select 1
      from public.weekly_source_group_clients membership
      where membership.source_group_id=p_source_group_id
        and membership.client_id=p_client_id
        and v_scope_date between membership.valid_from
          and coalesce(membership.valid_to,'infinity'::date)
    )
  ) then
    raise exception 'WEEKLY_SOURCE_CLIENT_NOT_IN_GROUP' using errcode='22023';
  end if;

  v_payload:=pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor.id,
    'actor_active',v_actor.is_active,
    'actor_role',pg_catalog.lower(pg_catalog.btrim(coalesce(v_actor.role,''))),
    'payment_authoriser',coalesce(v_actor.payment_authoriser,false),
    'payment_golden_key',coalesce(v_actor.payment_golden_key,false),
    'operation',v_operation,
    'source_group_id',p_source_group_id,
    'client_id',p_client_id,
    'scope_date',v_scope_date
  );
  return v_payload || pg_catalog.jsonb_build_object(
    'allowed',true,
    'decision_fingerprint',pg_catalog.encode(
      private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_OFFICE_AUTHORITY_V1',v_payload),
      'hex'
    )
  );
end;
$function$;


-- Current TEST source: supabase/repeatable/15092026_1534_weekly_source_protected_pay_c1_publication_v1.sql
-- Scope: WP-PP actor/read-view changes only.
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
     or not private.weekly_source_pay_actor_allowed_v1(v_actor_user_id)
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


-- Current TEST source: supabase/repeatable/15092026_1534_weekly_source_protected_pay_c1_publication_v1.sql
-- Scope: WP-PP actor/read-view changes only.
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


-- Current TEST source: supabase/repeatable/15092026_1534_weekly_source_protected_pay_c1_publication_v1.sql
-- Scope: WP-PP actor/read-view changes only.
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
     or not private.weekly_source_pay_actor_allowed_v1(v_actor_user_id) then
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


-- Current TEST source: supabase/repeatable/15092026_1534_weekly_source_protected_pay_c1_publication_v1.sql
-- Scope: WP-PP actor/read-view changes only.
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
     or not private.weekly_source_pay_actor_allowed_v1(v_actor_user_id) then
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


-- Current TEST source: supabase/repeatable/15092026_1534_weekly_source_protected_pay_c1_publication_v1.sql
-- Scope: WP-PP actor/read-view changes only.
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
     or not private.weekly_source_pay_actor_allowed_v1(v_actor_user_id) then
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


-- Current TEST source: supabase/repeatable/15092026_1534_weekly_source_protected_action_orchestration_v1.sql
-- Scope: WP-PP actor/read-view changes only.
create or replace function public.weekly_exceptional_pay_action_publication_status_v1(
  p_request jsonb
) returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_keys constant text[]:=array[
    'schema_version','actor_user_id','family_id','orchestration_run_id'
  ];
  v_actor uuid;
  v_family_id uuid;
  v_run_id uuid;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_publication public.weekly_exceptional_c1_publication_requests%rowtype;
  v_approval public.weekly_exceptional_payment_approvals%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_publication_count integer;
begin
  if pg_catalog.jsonb_typeof(p_request)<>'object'
     or not private.weekly_exceptional_json_keys_exact_v1(p_request,v_keys)
     or not (p_request ?& v_keys)
     or p_request->>'schema_version'<>'WEEKLY_PROTECTED_ACTION_PUBLICATION_STATUS_V1' then
    raise exception 'WEEKLY_PROTECTED_PUBLICATION_STATUS_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_family_id:=(p_request->>'family_id')::uuid;
    v_run_id:=(p_request->>'orchestration_run_id')::uuid;
  exception when others then
    raise exception 'WEEKLY_PROTECTED_PUBLICATION_STATUS_INVALID' using errcode='22023';
  end;
  select family.* into strict v_family
  from public.weekly_exceptional_pay_target_families family
  where family.id=v_family_id;
  select run.* into strict v_run
  from public.weekly_exceptional_orchestration_runs run
  where run.id=v_run_id and run.family_id=v_family.id;
  if v_run.requested_by_user_id is distinct from v_actor then
    raise exception 'WEEKLY_PROTECTED_PUBLICATION_STATUS_SCOPE_INVALID' using errcode='55000';
  end if;

  select count(*) into v_publication_count
  from public.weekly_exceptional_c1_publication_requests request
  where request.family_id=v_family.id
    and request.orchestration_run_id=v_run.id;
  if v_publication_count>1 then
    raise exception 'WEEKLY_PROTECTED_PUBLICATION_STATUS_SCOPE_INVALID' using errcode='55000';
  end if;
  if v_publication_count=0 then
    if not private.weekly_source_pay_actor_allowed_v1(v_actor) then
      raise exception 'WEEKLY_PROTECTED_PUBLICATION_STATUS_SCOPE_INVALID' using errcode='55000';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'staged',false,'family_id',v_family.id,
      'orchestration_run_id',v_run.id,'run_state',v_run.state
    );
  end if;
  select request.* into strict v_publication
  from public.weekly_exceptional_c1_publication_requests request
  where request.family_id=v_family.id and request.orchestration_run_id=v_run.id;

  select approval.* into strict v_approval
  from public.weekly_exceptional_payment_approvals approval
  where approval.creation_orchestration_run_id=v_run.id
    and approval.pay_target_family_id=v_family.id;
  select cycle.* into strict v_cycle
  from public.weekly_source_cycles cycle where cycle.id=v_approval.source_cycle_id;
  select source_group.* into strict v_group
  from public.weekly_source_groups source_group
  where source_group.id=v_cycle.source_group_id;
  perform private.weekly_source_office_authority_v1(
    v_actor,'APPROVE_PROTECTED_PAY',v_group.id,v_approval.client_id,
    v_cycle.finalisation_week_ending
  );
  return pg_catalog.jsonb_build_object(
    'ok',true,'staged',true,'family_id',v_family.id,
    'orchestration_run_id',v_run.id,'run_state',v_run.state,
    'publication_request_id',v_publication.id,
    'request_sha256',pg_catalog.encode(v_publication.request_sha256,'hex'),
    'publication_state',v_publication.state
  );
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_PROTECTED_PUBLICATION_STATUS_SCOPE_INVALID' using errcode='55000';
end;
$function$;


-- Current TEST source: supabase/repeatable/04102026_1257_weekly_source_local_protected_decision_v1.sql
-- Scope: WP-PP actor/read-view changes only.
create or replace function public.weekly_exceptional_pay_complete_local_v1(p_request jsonb)
returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
set lock_timeout to '2500ms'
set statement_timeout to '30s'
as $function$
declare
  v_keys constant text[]:=array['schema_version','actor_user_id','publication_request_id',
    'expected_request_sha256','idempotency_key'];
  v_actor uuid;
  v_request_id uuid;
  v_hash bytea;
  v_key text;
  v_request public.weekly_exceptional_c1_publication_requests%rowtype;
  v_generation public.weekly_exceptional_pay_generations%rowtype;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_approval public.weekly_exceptional_payment_approvals%rowtype;
  v_root public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_receipt private.weekly_source_local_protected_decision_receipts%rowtype;
  v_locks jsonb;
  v_snapshot jsonb;
  v_result jsonb;
  v_write jsonb;
  v_proposal jsonb;
  v_components jsonb;
  v_expenses jsonb;
  v_head_request jsonb;
  v_target_state text;
  v_source_hash bytea;
  v_result_hash bytea;
  v_live_authorisations integer;
  v_bundle_id uuid;
  v_local_context jsonb;
  v_local_origin jsonb;
  v_pending boolean:=false;
  v_manual_review_id uuid;
  v_rate_policy_hash bytea;
  v_scope_date date; v_scope_group uuid; v_scope_client uuid;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if not private.weekly_exceptional_json_keys_exact_v1(p_request,v_keys)
     or not (p_request ?& v_keys)
     or p_request->>'schema_version'<>'WEEKLY_PROTECTED_LOCAL_COMPLETE_V1' then
    raise exception 'WEEKLY_PROTECTED_LOCAL_COMPLETE_INVALID' using errcode='22023';
  end if;
  v_actor:=(p_request->>'actor_user_id')::uuid;
  v_request_id:=(p_request->>'publication_request_id')::uuid;
  v_hash:=private.weekly_exceptional_hex_sha256_v1(p_request->>'expected_request_sha256');
  v_key:=pg_catalog.btrim(coalesce(p_request->>'idempotency_key',''));
  if v_actor is null or v_request_id is null or pg_catalog.char_length(v_key) not between 16 and 240
     or not private.weekly_source_pay_actor_allowed_v1(v_actor) then
    raise exception 'WEEKLY_PROTECTED_LOCAL_COMPLETE_SCOPE_INVALID' using errcode='42501';
  end if;

  -- Exact committed receipt precedes freshness checks, locks and publication.
  select * into v_receipt from private.weekly_source_local_protected_decision_receipts
    where publication_request_id=v_request_id;
  -- The retained action's own report cutoff remains the Office authority,
  -- including exact replay. Work date and a later unrelated upload are not it.
  select c.finalisation_week_ending,c.source_group_id,a.client_id
    into strict v_scope_date,v_scope_group,v_scope_client
    from public.weekly_exceptional_c1_publication_requests r
    join public.weekly_exceptional_payment_approvals a
      on a.creation_orchestration_run_id=r.orchestration_run_id and a.pay_target_family_id=r.family_id
    join public.weekly_source_cycles c on c.id=a.source_cycle_id
    where r.id=v_request_id;
  perform private.weekly_source_office_authority_v1(v_actor,'APPROVE_PROTECTED_PAY',
    v_scope_group,v_scope_client,v_scope_date);
  -- SELECT INTO above changes FOUND; test the retained receipt identity itself.
  if v_receipt.publication_request_id is not null then
    if v_receipt.actor_user_id is distinct from v_actor or v_receipt.request_sha256 is distinct from v_hash
       or v_receipt.idempotency_key is distinct from v_key or v_receipt.state='PREPARING' then
      raise exception 'WEEKLY_PROTECTED_LOCAL_REPLAY_CONFLICT' using errcode='55000';
    end if;
    return v_receipt.result_json||jsonb_build_object('idempotent_replay',true);
  end if;
  perform private.weekly_source_pay_query_admit_v2();
  select * into strict v_request from public.weekly_exceptional_c1_publication_requests where id=v_request_id;
  -- I-1: Candidate serial gate, then all canonical family members, then rows.
  v_locks:=private.weekly_source_lock_and_resolve_families_v1(v_request.candidate_id,
    array[v_request.root_timesheet_id],'WORKBENCH_CANDIDATE_ENTITLEMENT_PUBLICATION',
    v_request.id,'WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION');
  if coalesce((v_locks->>'ok')::boolean,false) is not true then
    return v_locks||jsonb_build_object('ok',false);
  end if;
  select * into v_receipt from private.weekly_source_local_protected_decision_receipts
    where publication_request_id=v_request_id;
  if found then
    if v_receipt.actor_user_id is distinct from v_actor or v_receipt.request_sha256 is distinct from v_hash
       or v_receipt.idempotency_key is distinct from v_key or v_receipt.state='PREPARING' then
      raise exception 'WEEKLY_PROTECTED_LOCAL_REPLAY_CONFLICT' using errcode='55000';
    end if;
    return v_receipt.result_json||jsonb_build_object('idempotent_replay',true);
  end if;
  select * into strict v_family from public.weekly_exceptional_pay_target_families
    where id=v_request.family_id for update;
  select * into strict v_request from public.weekly_exceptional_c1_publication_requests
    where id=v_request_id for update;
  select * into strict v_generation from public.weekly_exceptional_pay_generations
    where id=v_request.generation_id and family_id=v_family.id for update;
  select * into strict v_run from public.weekly_exceptional_orchestration_runs
    where id=v_request.orchestration_run_id and family_id=v_family.id for update;
  select * into strict v_approval from public.weekly_exceptional_payment_approvals
    where creation_orchestration_run_id=v_run.id and pay_target_family_id=v_family.id for share;
  select * into strict v_root from public.timesheets where timesheet_id=v_family.root_timesheet_id for update;
  select * into strict v_fin from public.timesheets_financials
    where timesheet_id=v_root.timesheet_id and is_current for update;
  if v_hash is distinct from v_request.request_sha256 or v_run.requested_by_user_id is distinct from v_actor
     or v_request.state<>'READY' or v_run.state<>'RUNNING' or v_generation.lifecycle_state<>'PENDING_C1'
     or v_request.c1_operation_id is not null or v_request.c1_publication_id is not null
     or v_request.c1_head_revision is not null or v_request.c1_receipt_sha256 is not null
     or exists(select 1 from public.weekly_exceptional_c1_publication_checkpoints c where c.publication_request_id=v_request.id)
     or exists(select 1 from public.weekly_exceptional_c1_unknown_outcomes u
                where u.publication_request_id=v_request.id and u.state='RECOVERY_REQUIRED')
     or v_family.current_generation_id is distinct from v_generation.prior_generation_id
     or v_family.current_generation_number+1 is distinct from v_generation.generation_number
     or v_fin.id is distinct from v_request.financial_row_id
     or not v_root.is_current or v_root.revoked_at is not null or v_root.archived_at_utc is not null
     or v_root.contract_id is distinct from v_family.contract_id
     or v_root.week_ending_date is distinct from v_family.week_ending_date
     or v_root.version is distinct from (v_generation.complete_next_vector_json#>>'{target_snapshot,tsfin_snapshot_json,timesheet_version}')::integer
     or v_approval.withdrawn_at_utc is not null
     or exists(select 1 from private.weekly_source_local_protected_decision_receipts r
               where r.family_id=v_family.id and r.state='PENDING_FREEZE') then
    raise exception 'WEEKLY_PROTECTED_LOCAL_STATE_CHANGED' using errcode='55000';
  end if;
  select target.resulting_lifecycle_state into strict v_target_state
    from public.weekly_exceptional_pay_target_events target
    where target.financial_generation_id=v_generation.id and target.family_id=v_family.id;
  select event.source_proposal_hash into strict v_source_hash
    from public.weekly_exceptional_pay_family_events event
    where event.evidence_approval_id=v_approval.id and event.family_id=v_family.id;
  select private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_RATE_POLICY_V1',
    jsonb_build_object('contract_id',v_family.contract_id,'rates_json',contract.rates_json,
      'policy',private._weekly_source_effective_policy_v1(v_approval.client_id,v_family.contract_id,
        v_approval.protected_work_date),'classification',event.rate_classification_json))
    into strict v_rate_policy_hash
    from public.weekly_exceptional_pay_family_events event
    join public.contracts contract on contract.id=v_family.contract_id
    where event.evidence_approval_id=v_approval.id and event.family_id=v_family.id;
  if v_rate_policy_hash is distinct from v_approval.contract_rate_policy_source_fingerprint then
    raise exception 'WEEKLY_PROTECTED_LOCAL_RATE_POLICY_CHANGED' using errcode='40001';
  end if;
  v_snapshot:=v_generation.complete_next_vector_json#>'{target_snapshot,tsfin_snapshot_json}';
  if jsonb_typeof(v_snapshot) is distinct from 'object'
     or private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_COMPLETE_VECTOR_V1',
        v_generation.complete_next_vector_json) is distinct from v_generation.complete_next_vector_hash
     or v_snapshot is distinct from v_approval.approved_target_pay_components_json->'tsfin_snapshot_json' then
    raise exception 'WEEKLY_PROTECTED_LOCAL_SNAPSHOT_INVALID' using errcode='55000';
  end if;
  v_snapshot:=v_snapshot||jsonb_build_object('timesheet_id',v_root.timesheet_id::text,
    'timesheet_version',v_root.version,'actual_schedule_json',
    v_generation.complete_next_vector_json#>'{target_snapshot,actual_schedule_json}');
  -- Reconfirm the staged identity proof before either mutable first-approval
  -- TSFIN or the common HEAD writer. Immutable old requests without this new
  -- metadata retain their existing route; they are not silently remapped.
  if v_generation.complete_next_vector_json ? 'protected_component_identity_basis'
     or exists(select 1 from jsonb_array_elements(v_snapshot#>'{invoice_breakdown_json,segments}') r
       where r ? 'weekly_protected_component_identity') then
    if v_generation.complete_next_vector_json#>>'{protected_component_identity_basis,schema_version}'
          is distinct from 'WEEKLY_PROTECTED_COMPONENT_MANIFEST_V1'
       or v_generation.complete_next_vector_json#>>'{protected_component_identity_basis,family_id}'
          is distinct from v_family.id::text
       or v_generation.complete_next_vector_json#>>'{protected_component_identity_basis,root_timesheet_id}'
          is distinct from v_root.timesheet_id::text
       or v_generation.complete_next_vector_json#>>'{protected_component_identity_basis,contract_id}'
          is distinct from v_family.contract_id::text
       or v_generation.complete_next_vector_json#>>'{protected_component_identity_basis,candidate_id}'
          is distinct from v_family.candidate_id::text
       or v_generation.complete_next_vector_json#>>'{protected_component_identity_basis,accepted_schedule_sha256}'
          is distinct from encode(private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_PROTECTED_COMPONENT_ACCEPTED_SCHEDULE_V1',v_snapshot->'actual_schedule_json'),'hex')
       or v_generation.complete_next_vector_json#>>'{protected_component_identity_basis,calculated_segments_sha256}'
          is distinct from encode(private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_PROTECTED_COMPONENT_CALCULATED_SEGMENTS_V1',v_snapshot#>'{invoice_breakdown_json,segments}'),'hex')
       or v_generation.complete_next_vector_json#>>'{protected_component_identity_basis,approved_components_sha256}'
          is distinct from encode(private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_PROTECTED_COMPONENT_APPROVED_COMPONENTS_V1',
            case when v_generation.complete_next_vector_json ? 'prior_effective_inventory'
              then v_generation.complete_next_vector_json#>'{prior_effective_inventory,components}'
              else '[]'::jsonb end),'hex') then
      raise exception 'WEEKLY_PROTECTED_COMPONENT_MANIFEST_UNAVAILABLE' using errcode='55000'; end if;
  end if;
  select count(*) into v_live_authorisations from public.weekly_source_root_authorisations authority
    where authority.root_timesheet_id=v_root.timesheet_id and authority.withdrawn_at_utc is null;
  -- Exact accepted replay has already returned. Before admitting a NEW
  -- authorised decision, hold the existing owner share lock and require the
  -- same actual NEXT Local writer capabilities as the winning common core.
  -- Otherwise an old freeze could park a decision that cannot later publish.
  -- First Save remains the unauthorised TSFIN path and needs no Bank owner.
  if v_live_authorisations=1 and v_root.authorised_at_server is not null then
    if pg_catalog.to_regclass('private.bpay_next_module_control') is null
       or pg_catalog.to_regprocedure('private.bpay_next_legacy_callback_enabled_v1()') is null then
      raise exception 'WEEKLY_PROTECTED_LOCAL_PUBLICATION_OWNER_NOT_READY' using errcode='55000';
    end if;
    perform private.bpay_next_legacy_callback_enabled_v1();
    if (select active_owner from private.bpay_next_module_control where id=1) is distinct from 'NEXT'
       or pg_catalog.to_regprocedure(
         'private.weekly_source_local_publication_context_v2(uuid,uuid,uuid)') is null
       or pg_catalog.to_regprocedure(
         'private.weekly_source_entitlement_proposal_request_v2(uuid,jsonb,text,uuid,bigint,uuid,uuid,jsonb,text)') is null
       or pg_catalog.to_regprocedure(
         'private.bpay_next_capture_local_source_detail_v1(uuid,uuid,bigint,uuid,uuid,uuid,jsonb)') is null then
      raise exception 'WEEKLY_PROTECTED_LOCAL_PUBLICATION_OWNER_NOT_READY' using errcode='55000';
    end if;
  end if;
  insert into private.weekly_source_local_protected_decision_receipts(
    publication_request_id,generation_id,family_id,root_timesheet_id,actor_user_id,
    request_sha256,idempotency_key,preparing_transaction_id,prior_financial_id,approved_snapshot_json,state
  ) values(v_request.id,v_generation.id,v_family.id,v_root.timesheet_id,v_actor,
    v_hash,v_key,pg_current_xact_id(),v_fin.id,v_snapshot,'PREPARING');
  if v_live_authorisations=0 and v_root.authorised_at_server is null then
    -- 24 section 4.1: mutable unauthorised TSFIN, not a fabricated Authorise.
    if v_fin.authorised_at_utc is not null or v_fin.processing_status<>'PENDING_AUTH'
       or v_snapshot->>'processing_status' is distinct from 'PENDING_AUTH'
       or nullif(v_snapshot->>'authorised_at_utc','') is not null then
      raise exception 'WEEKLY_PROTECTED_LOCAL_AUTHORISATION_CONTRADICTION' using errcode='55000';
    end if;
    v_write:=public.tsfin_write_current_snapshot_single_bounded(v_root.timesheet_id,
      v_root.version,v_snapshot,v_actor,statement_timestamp());
    if coalesce((v_write->>'ok')::boolean,false) is not true then
      raise exception 'WEEKLY_PROTECTED_LOCAL_SNAPSHOT_WRITE_REFUSED' using errcode='55000';
    end if;
    -- V4 permits display of this confirmed decision before first Authorise,
    -- but never admission to Banking. Seal the actual written row, not the
    -- proposed input or an hours-only total. Old receipts are not backfilled.
    select * into strict v_fin from public.timesheets_financials financial
      where financial.id=(v_write->>'timesheet_financials_id')::uuid
        and financial.timesheet_id=v_root.timesheet_id and financial.is_current;
    if v_fin.authorised_at_utc is not null or v_fin.processing_status<>'PENDING_AUTH'
       or v_fin.paid_at_utc is not null or v_fin.locked_by_invoice_id is not null then
      raise exception 'WEEKLY_PROTECTED_LOCAL_SAVED_BASIS_INVALID' using errcode='55000';
    end if;
    update private.weekly_source_local_protected_decision_receipts
      set approved_snapshot_json=v_snapshot||jsonb_build_object('saved_unauthorised_basis',
        jsonb_build_object('schema_version','SAVED_UNAUTHORISED_LOCAL_V1',
          'financial_snapshot_id',v_fin.id,
          'financial_snapshot_sha256',encode(private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_SOURCE_SAVED_UNAUTHORISED_FINANCIAL_V1',to_jsonb(v_fin)),'hex'),
          'detail_sha256',encode(private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_SOURCE_SAVED_UNAUTHORISED_DETAIL_V1',jsonb_build_object(
              'segments',v_fin.invoice_breakdown_json->'segments',
              'actual_schedule',v_fin.actual_schedule_json,
              'additional_units',v_fin.additional_units_json)),'hex'),
          'inventory_sha256',encode(v_generation.complete_next_vector_hash,'hex'),
          'root_version',v_root.version,'family_booking_id',v_root.booking_id))
      where publication_request_id=v_request.id and state='PREPARING'
        and preparing_transaction_id=pg_current_xact_id();
    if not found then
      raise exception 'WEEKLY_PROTECTED_LOCAL_SAVED_BASIS_INVALID' using errcode='55000';
    end if;
    v_result:=jsonb_build_object('authority','UNAUTHORISED_TSFIN',
      'timesheet_financials_id',v_write->'timesheet_financials_id','requires_first_authorisation',true);
  elsif v_live_authorisations=1 and v_root.authorised_at_server is not null then
    -- Already authorised: the only later entitlement writer is the existing
    -- common-head publisher. Never rotate TSFIN, change Drafts or invoke C1.
    -- The real stage advances its own family bound version after sealing the
    -- old entitlement. The factual context proves that exact stage and the
    -- unchanged whole inventory, rather than treating the own increment as
    -- an unrelated intervening financial change.
    v_local_context:=private.weekly_source_local_publication_context_v2(v_request.id,v_generation.id,v_root.timesheet_id);
    if v_local_context is null then
      raise exception 'WEEKLY_PROTECTED_LOCAL_SOURCE_QUALIFICATION_UNAVAILABLE' using errcode='55000'; end if;
    select coalesce(jsonb_agg(jsonb_build_object('work_event_id',expense.work_event_id,
      'source_observation_kind',expense.source_observation_kind,
      'candidate_reimbursement_ex_vat',expense.candidate_reimbursement_ex_vat,
      'client_charge_ex_vat',expense.client_charge_ex_vat) order by expense.work_event_id),'[]'::jsonb)
      into v_expenses from (
        select authority.*,row_number() over(partition by authority.work_event_id
          order by cycle.finalisation_week_ending desc,revision.finalised_at_utc desc,
            authority.generation desc,authority.id desc) event_rank
        from public.weekly_expense_authority_generations authority
        join public.weekly_source_final_revisions revision on revision.id=authority.final_revision_id and revision.state='CURRENT'
        join public.weekly_source_cycles cycle on cycle.id=revision.source_cycle_id
        join public.weekly_source_cycles action_cycle on action_cycle.id=v_approval.source_cycle_id
        where authority.contract_id=v_family.contract_id and authority.state='CURRENT'
          and cycle.source_group_id=action_cycle.source_group_id
          and exists(select 1 from public.weekly_source_row_timesheet_lineages lineage
            where lineage.work_event_id=authority.work_event_id and lineage.contract_id=authority.contract_id
              and lineage.timesheet_id=any(private.weekly_source_invoice_family_timesheet_ids_v1(v_root.timesheet_id)))
      ) expense where expense.event_rank=1 and expense.source_expense_pence>0;
    v_components:=private.weekly_source_entitlement_components_v1(
      v_snapshot#>'{invoice_breakdown_json,segments}',v_expenses);
    -- Reuse the existing pure approved-additional mapper on the sealed
    -- calculator snapshot. It validates exact saved units/rates/totals and
    -- booking-family identity; it does not read or reprice live financial data.
    v_components:=private.weekly_source_initial_additional_components_v2(v_components,
      v_snapshot->'additional_units_json',
      (v_snapshot->>'additional_pay_ex_vat')::numeric,
      (v_snapshot->>'additional_charge_ex_vat')::numeric,v_root.booking_id);
    -- Source expenses must be the same approved observations and money, not
    -- a newer expense silently composed into an older protected decision.
    if (select coalesce(sum((item.value->>'pay_ex_vat')::numeric)
        filter(where not (item.value->>'exclude_from_pay')::boolean),0)
        from jsonb_array_elements(v_components) item(value)) is distinct from v_approval.approved_target_gross
       or (select coalesce(jsonb_agg(jsonb_build_object('code',upper('SOURCE_SUPPLIED:'||(item.value->>'work_event_id')),
          'pay',(item.value->>'candidate_reimbursement_ex_vat')::numeric,
          'charge',(item.value->>'client_charge_ex_vat')::numeric) order by item.value->>'work_event_id'),'[]'::jsonb)
          from jsonb_array_elements(v_expenses) item(value)) is distinct from
          (select coalesce(jsonb_agg(jsonb_build_object('code',upper(component.expense_code),
            'pay',component.pay_ex_vat,'charge',component.charge_ex_vat)
            order by component.expense_code),'[]'::jsonb)
            from public.weekly_exceptional_c1_component_records component
            where component.publication_request_id=v_request.id and component.component_kind='EXPENSE') then
      raise exception 'WEEKLY_PROTECTED_LOCAL_COMPLETE_ENTITLEMENT_CHANGED' using errcode='40001';
    end if;
    v_local_origin:=private.weekly_source_local_origin_canonical_v2(jsonb_build_object(
      'origin_kind','PROTECTED_LOCAL_DECISION_V1','publication_request_id',v_request.id,
      'generation_id',v_generation.id,'request_sha256',encode(v_request.request_sha256,'hex'),
      'source_qualification_digest',v_local_context->'source_qualification_digest',
      'policy_fingerprint',v_local_context->'policy_fingerprint','before_origin',v_local_context->'before_origin',
      'before_inventory_digest',v_local_context->'before_inventory_digest'));
    v_bundle_id:=pg_catalog.gen_random_uuid();
    update private.weekly_source_local_protected_decision_receipts set approved_snapshot_json=v_snapshot||jsonb_build_object(
      'common_decision_bundle_id',v_bundle_id,
      'local_source_qualification',v_local_context->'qualification','local_publication_origin',v_local_origin,
      'common_components_digest',encode(private.weekly_source_publication_request_digest_v1(
        (select coalesce(jsonb_agg(private.weekly_source_publication_component_canonical_v1(item.value,'protected.component')
          order by item.ordinality),'[]'::jsonb) from jsonb_array_elements(v_components) with ordinality item(value,ordinality))),'hex')),
      common_decision_bundle_id=v_bundle_id,common_bundle_revision=1,
      publication_origin_kind='PROTECTED_LOCAL_DECISION_V1',
      publication_origin_digest=private.weekly_source_publication_request_digest_v1(v_local_origin),
      source_qualification_digest=decode(v_local_context->>'source_qualification_digest','hex')
      where publication_request_id=v_request.id and state='PREPARING' and preparing_transaction_id=pg_current_xact_id();
    if not found then raise exception 'WEEKLY_PROTECTED_LOCAL_ORIGIN_SEAL_REFUSED' using errcode='55000'; end if;
    v_head_request:=private.weekly_source_entitlement_proposal_request_v2(v_root.timesheet_id,
      v_local_origin,'PROTECTED',v_bundle_id,1,pg_catalog.gen_random_uuid(),
      pg_catalog.gen_random_uuid(),v_components);
    v_proposal:=private.weekly_source_entitlement_proposal_record_v1(v_head_request,v_family.agency_id,
      v_family.contract_id,v_family.week_ending_date,v_actor);
    if coalesce((v_proposal->>'ok')::boolean,false) is not true then
      raise exception 'WEEKLY_PROTECTED_LOCAL_PROPOSAL_REFUSED' using errcode='55000';
    end if;
    v_write:=private.weekly_source_entitlement_publish_immediate_v1(v_head_request);
    if coalesce((v_write->>'ok')::boolean,false) is not true then
      raise exception '%',coalesce(v_write->>'code','WEEKLY_PROTECTED_LOCAL_HEAD_REFUSED') using errcode='55000';
    end if;
    v_pending:=coalesce((v_write->>'published')::boolean,false) is not true;
    if v_pending and v_write->>'code'<>'WEEKLY_SOURCE_PUBLICATION_DEFERRED_PENDING_FREEZE' then
      raise exception 'WEEKLY_PROTECTED_LOCAL_HEAD_RESULT_INVALID' using errcode='55000';
    end if;
    v_result:=jsonb_build_object('authority','COMMON_ENTITLEMENT_HEAD',
      'decision_bundle_id',v_bundle_id,'publication',v_write,'requires_first_authorisation',false);
  else
    raise exception 'WEEKLY_PROTECTED_LOCAL_AUTHORISATION_CONTRADICTION' using errcode='55000';
  end if;

  v_result:=v_result||jsonb_build_object('ok',true,'outcome',case when v_pending
    then 'SAVED_PENDING_FREEZE' else 'PUBLISHED' end,'idempotent_replay',false,
    'family_id',v_family.id,'generation_id',v_generation.id,'publication_request_id',v_request.id,
    'state',case when v_pending then 'PENDING' else 'LIVE' end,
    'family_bound_version',v_family.bound_version+1);
  v_result_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_LOCAL_RESULT_V1',v_result);
  if not v_pending then
    if v_family.current_generation_id is not null then
      update public.weekly_exceptional_pay_generations set lifecycle_state='SUPERSEDED',
        superseded_by_generation_id=v_generation.id,superseded_at_utc=statement_timestamp()
        where id=v_family.current_generation_id and lifecycle_state='PUBLISHED';
      if not found then raise exception 'WEEKLY_PROTECTED_LOCAL_PRIOR_GENERATION_INVALID' using errcode='55000'; end if;
    end if;
    update public.weekly_exceptional_pay_generations set lifecycle_state='PUBLISHED',
      published_at_utc=statement_timestamp(),result_hash=v_result_hash where id=v_generation.id;
    update public.weekly_exceptional_pay_target_families set current_generation_id=v_generation.id,
      current_generation_number=v_generation.generation_number,
      current_complete_target_vector_hash=v_generation.complete_next_vector_hash,
      current_source_proposal_hash=v_source_hash,current_lifecycle_state=v_target_state,
      current_component_count=v_request.expected_component_count,c1_publication_state='NONE',
      bound_version=bound_version+1 where id=v_family.id;
  else
    -- Previous effective family/head remains current while the existing frozen
    -- bundle owner holds the newly approved decision. No pretend live receipt.
    update public.weekly_exceptional_pay_target_families set c1_publication_state='NONE',
      bound_version=bound_version+1 where id=v_family.id;
  end if;
  update public.weekly_exceptional_c1_publication_requests set state='RETIRED',
    typed_result_json=v_result,completed_at_utc=statement_timestamp() where id=v_request.id;
  update public.weekly_exceptional_orchestration_runs set state='COMPLETE',
    after_state_fingerprint=v_result_hash,completed_at_utc=statement_timestamp() where id=v_run.id;
  update private.weekly_source_local_protected_decision_receipts set state=case when v_pending
    then 'PENDING_FREEZE' else 'COMPLETE' end,result_json=v_result,completed_at_utc=statement_timestamp()
    where publication_request_id=v_request.id;
  -- Finish the existing manual pay query atomically with its approved decision.
  -- The UI needs no second query command: only this accepted publication can
  -- clear the hold, using its exact retained approval and generation evidence.
  select review.id into v_manual_review_id from private.weekly_source_manual_reviews review
    join public.weekly_source_cycles cycle on cycle.id=v_approval.source_cycle_id
    where review.source_group_id=cycle.source_group_id
      and review.work_event_id=v_approval.work_event_id and review.state='OPEN'
      and review.contract_id=v_family.contract_id and review.candidate_id=v_family.candidate_id;
  insert into public.weekly_exceptional_payment_events(family_id,approval_id,event_kind,
    lifecycle_view,bounded_payload_json,idempotency_key) values(v_family.id,v_approval.id,
    'PROTECTED_STATE_OBSERVED',case when v_pending then 'PENDING' else 'LIVE' end,v_result,v_key||':payment-event');
  insert into public.weekly_exceptional_orchestration_steps(orchestration_run_id,sequence,
    step_kind,idempotency_key,allowlisted_owner_name,allowlisted_owner_signature,bounded_request_hash,
    before_state_fingerprint,bounded_owner_response_json,owner_response_hash,after_state_fingerprint,outcome,completed_at_utc)
    select v_run.id,coalesce(max(step.sequence),0)+1,'COMPLETE_LOCAL_PROTECTED_DECISION',v_key,
      'CloudTMS.weekly_exceptional_pay_complete_local_v1','jsonb',v_hash,v_run.before_state_fingerprint,
      v_result,v_result_hash,v_result_hash,case when v_pending then 'PENDING' else 'COMPLETE' end,statement_timestamp()
    from public.weekly_exceptional_orchestration_steps step where step.orchestration_run_id=v_run.id;
  -- Finish the actual completion evidence before any query resolution or
  -- accepted-status read, in this same outer transaction; a refusal rolls back
  -- the decision, publication, completion step and query together.
  if v_manual_review_id is not null and v_run.request_kind in ('APPROVE','AMEND') then
    perform public.weekly_source_manual_review_resolve_v1(jsonb_build_object('actor_user_id',v_actor,
      'review_id',v_manual_review_id,'resolution_kind','PROTECTED_PAY',
      'command_id',v_request.id,'accepted_approval_id',v_approval.id,
      'accepted_generation_id',v_generation.id));
  elsif v_manual_review_id is not null and v_run.request_kind in ('WITHDRAW','RECONCILE') then
    perform public.weekly_source_manual_review_resolve_v1(jsonb_build_object('actor_user_id',v_actor,
      'review_id',v_manual_review_id,'resolution_kind','OFFICE_ACCEPTED_SOURCE',
      'command_id',v_request.id,'accepted_approval_id',v_approval.id,
      'accepted_generation_id',v_generation.id));
  end if;
  -- Accepted pending approval is not publication. The existing release
  -- trigger retires its contacts only after the deferred receipt is proved.
  if not v_pending then
    perform private.weekly_source_protected_contact_retire_v1(v_family.id,v_approval.work_event_id);
  end if;
  return v_result;
exception when no_data_found or too_many_rows then
  raise exception 'WEEKLY_PROTECTED_LOCAL_COMPLETE_SCOPE_INVALID' using errcode='55000';
end;
$function$;


-- Current TEST source: supabase/repeatable/15092026_1534_weekly_source_read_projections_v1.sql
-- Scope: WP-PP actor/read-view changes only.
create or replace function public.weekly_source_office_workspace_v1(
  p_request jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_allowed_keys constant text[]:=array[
    'actor_user_id','tab','source_group_id','source_cycle_id','client_id',
    'report_scope_id','projection_publication_id','cursor','limit','sort_key',
    'sort_direction','status','candidate','issue','cycle_filter'
  ]::text[];
  v_unknown text;
  v_actor uuid;
  v_pay_view boolean;
  v_pay_row jsonb;
  v_pay_rows jsonb;
  v_pay_child jsonb;
  v_pay_children jsonb;
  v_cursor_view_version text;
  v_result jsonb;
  v_tab text;
  v_group_id uuid;
  v_cycle_id uuid;
  v_client_id uuid;
  v_report_scope_id uuid;
  v_publication_id uuid;
  v_cursor text;
  v_limit integer;
  v_offset integer:=0;
  v_sort_key text;
  v_sort_direction text;
  v_cycle_filter text;
  v_filters jsonb;
  v_group public.weekly_source_groups%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_publication public.weekly_source_projection_publications%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_profile public.weekly_source_format_profiles%rowtype;
  v_guard jsonb;
  v_workspace_version text;
  v_recheck_action jsonb:='{}'::jsonb;
  v_imports jsonb;
  v_queries jsonb;
  v_ready jsonb;
  v_blocked jsonb;
  v_complete jsonb;
  v_history jsonb;
  v_tracker jsonb;
  v_controls jsonb;
  v_query_count integer:=0;
  v_ask_eligible_count integer:=0;
  v_manager_eligible_count integer:=0;
  v_blocker_count integer:=0;
  v_paid_unresolved_count integer:=0;
  v_total integer:=0;
  v_rows jsonb:='[]'::jsonb;
  v_next_cursor text:='';
  v_cycle_label text;
  v_cycle_state_label text;
  v_cycle_state_tone text;
  v_finalise_payload jsonb:='{}'::jsonb;
  v_finalise_pay_follow_up jsonb;
  v_finalise_enabled boolean:=false;
  v_bulk_actions jsonb:='{}'::jsonb;
  v_all_group_keys jsonb:='[]'::jsonb;
  v_ask_selection_proof text;
  v_manager_selection_proof text;
  v_rate_warnings jsonb:='{}'::jsonb;
  v_rate_warning_rows jsonb:='[]'::jsonb;
  v_rate_warning_keys jsonb:='[]'::jsonb;
  v_rate_warning_selection_proof text;
  v_rate_warning_count integer:=0;
  v_rate_warning_accepted_count integer:=0;
  v_rate_warning_unaccepted_count integer:=0;
  v_rate_warning_zero_count integer:=0;
  v_rate_warning_disparity_count integer:=0;
  v_authority_mode text:='SOURCE_AUTHORITY';
  v_document_mode text:='CHECK_ONLY';
  v_import_journey jsonb:='{}'::jsonb;
  v_import_attention_rows jsonb:='[]'::jsonb;
  v_import_waiting_rows jsonb:='[]'::jsonb;
  v_import_ready_rows jsonb:='[]'::jsonb;
  v_mode_a_import_id uuid;
  v_context_client_name text;
  v_context_cutoff timestamptz;
  v_nhsp_report_number text;
  v_import_prepared boolean:=false;
  v_office_checks jsonb:='{}'::jsonb;
  v_prepare_action jsonb;
  v_unfinalised_count integer:=0;
  v_excluded_rows jsonb:='[]'::jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_WORKSPACE_REQUEST_INVALID' using errcode='22023';
  end if;
  select key into v_unknown from pg_catalog.jsonb_object_keys(p_request) key
  where not (key=any(v_allowed_keys)) order by key limit 1;
  if v_unknown is not null then
    raise exception 'WEEKLY_SOURCE_WORKSPACE_REQUEST_UNKNOWN_FIELD'
      using errcode='22023',detail=v_unknown;
  end if;
  if pg_catalog.jsonb_typeof(p_request->'actor_user_id')<>'string'
     or exists(
       select 1 from pg_catalog.jsonb_each(p_request) item
       where item.key in ('tab','source_group_id','source_cycle_id','client_id',
         'report_scope_id','projection_publication_id','cursor','sort_key',
         'sort_direction','status','candidate','issue','cycle_filter')
         and pg_catalog.jsonb_typeof(item.value) not in ('string','null')
     )
     or (p_request ? 'limit' and pg_catalog.jsonb_typeof(p_request->'limit') not in ('number','null')) then
    raise exception 'WEEKLY_SOURCE_WORKSPACE_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_pay_view:=not exists(select 1 from public.tms_users actor where actor.id=v_actor
      and lower(btrim(coalesce(actor.role,'')))='admin');
    v_tab:=pg_catalog.lower(coalesce(nullif(pg_catalog.btrim(p_request->>'tab'),''),case when v_pay_view then 'queries' else 'imports' end));
    v_group_id:=nullif(p_request->>'source_group_id','')::uuid;
    v_cycle_id:=nullif(p_request->>'source_cycle_id','')::uuid;
    v_client_id:=nullif(p_request->>'client_id','')::uuid;
    v_report_scope_id:=nullif(p_request->>'report_scope_id','')::uuid;
    v_publication_id:=nullif(p_request->>'projection_publication_id','')::uuid;
    v_limit:=coalesce(nullif(p_request->>'limit','')::integer,50);
  exception when others then
    raise exception 'WEEKLY_SOURCE_WORKSPACE_REQUEST_INVALID' using errcode='22023';
  end;
  if v_actor is null or v_tab not in ('imports','queries','finalise','history')
     or v_limit not between 1 and 100 then
    raise exception 'WEEKLY_SOURCE_WORKSPACE_REQUEST_INVALID' using errcode='22023';
  end if;
  v_sort_direction:=pg_catalog.lower(coalesce(nullif(pg_catalog.btrim(p_request->>'sort_direction'),''),
    case when v_tab='history' then 'desc' else 'asc' end));
  if v_sort_direction not in ('asc','desc') then
    raise exception 'WEEKLY_SOURCE_WORKSPACE_SORT_INVALID' using errcode='22023';
  end if;
  v_sort_key:=coalesce(nullif(pg_catalog.lower(pg_catalog.btrim(p_request->>'sort_key')),''),
    case v_tab when 'imports' then 'uploaded' when 'history' then 'when' else 'candidate' end);
  if (v_tab='imports' and v_sort_key not in ('file','uploaded','rows','coverage','report','cutoff','status','final_source'))
     or (v_tab='queries' and v_sort_key not in ('candidate','client','issues','candidate_asked','manager_informed','status','age'))
     or (v_tab='finalise' and v_sort_key not in (
       'candidate','day_date','client','system_hours','actual_hours','movement',
       'commission','total_cost','invoice_charge','status','problem','job_role',
       'contract','outcome'
     ))
     or (v_tab='history' and v_sort_key not in ('when','source','event','by','detail')) then
    raise exception 'WEEKLY_SOURCE_WORKSPACE_SORT_INVALID' using errcode='22023';
  end if;
  v_cycle_filter:=pg_catalog.upper(coalesce(nullif(pg_catalog.btrim(p_request->>'cycle_filter'),''),'CURRENT_PAY_CYCLE'));
  if v_cycle_filter not in ('CURRENT_PAY_CYCLE','LAST_4_PAY_CYCLES','LAST_13_PAY_CYCLES') then
    raise exception 'WEEKLY_SOURCE_WORKSPACE_HISTORY_FILTER_INVALID' using errcode='22023';
  end if;
  v_filters:=pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
    'status',nullif(p_request->>'status',''),
    'candidate',nullif(p_request->>'candidate',''),
    'issue',nullif(p_request->>'issue','')
  ));

  if v_group_id is null and v_cycle_id is not null then
    select source_group_id into v_group_id from public.weekly_source_cycles where id=v_cycle_id;
  end if;
  if v_group_id is null then
    select source_group.id into v_group_id
    from public.weekly_source_groups source_group
    where source_group.active
    order by source_group.updated_at_utc desc,source_group.id limit 1;
  end if;
  select * into v_group from public.weekly_source_groups where id=v_group_id and active;
  if not found then raise exception 'WEEKLY_SOURCE_GROUP_NOT_ACTIVE' using errcode='22023'; end if;
  if v_cycle_id is null then
    select cycle.id into v_cycle_id from public.weekly_source_cycles cycle
    where cycle.source_group_id=v_group.id
      and (cycle.scope_client_id is null or cycle.scope_client_id=v_client_id)
    order by cycle.finalisation_week_ending desc,cycle.id desc limit 1;
  end if;
  select * into v_cycle from public.weekly_source_cycles
  where id=v_cycle_id and source_group_id=v_group.id;
  if not found then raise exception 'WEEKLY_SOURCE_CYCLE_NOT_FOUND' using errcode='22023'; end if;

  if v_cycle.scope_client_id is not null then
    if v_client_id is not null and v_client_id<>v_cycle.scope_client_id then
      raise exception 'WEEKLY_SOURCE_CLIENT_CYCLE_SCOPE_MISMATCH' using errcode='22023';
    end if;
    v_client_id:=v_cycle.scope_client_id;
  end if;

  perform private.weekly_source_office_authority_v1(
    v_actor,case when v_pay_view then 'VIEW_PAY_WORK' else 'VIEW_SOURCE_PROGRESS' end,
    v_group.id,v_client_id,v_cycle.finalisation_week_ending
  );
  if v_pay_view and v_tab<>'queries' then
    raise exception 'WEEKLY_SOURCE_OFFICE_ADMIN_REQUIRED' using errcode='42501';
  end if;
  if v_report_scope_id is not null then
    select scope.client_id into strict v_client_id
    from public.weekly_source_report_scopes scope
    where scope.id=v_report_scope_id and scope.source_cycle_id=v_cycle.id
      and (v_client_id is null or scope.client_id=v_client_id);
  end if;
  if v_client_id is not null and not exists(
    select 1 from public.weekly_source_group_clients membership
    where membership.source_group_id=v_group.id and membership.client_id=v_client_id
      and v_cycle.finalisation_week_ending between membership.valid_from
        and coalesce(membership.valid_to,'infinity'::date)
  ) then
    raise exception 'WEEKLY_SOURCE_CLIENT_NOT_IN_GROUP' using errcode='22023';
  end if;

  if v_client_id is not null then
    select policy.authority_mode,policy.document_mode
    into v_authority_mode,v_document_mode
    from public.weekly_source_client_policies policy
    where policy.source_group_id=v_group.id and policy.client_id=v_client_id
      and v_cycle.finalisation_week_ending between policy.effective_from
        and coalesce(policy.effective_to,'infinity'::date)
    order by policy.effective_from desc,policy.id desc limit 1;
    if not found then
      v_authority_mode:='SOURCE_AUTHORITY';
      v_document_mode:='CHECK_ONLY';
    end if;
  end if;

  -- Queries are a cycle-wide pre-finalisation journey. An NHSP report scope
  -- can retain an older final backing publication while a newer complete
  -- previously-released upload is current for the cycle. Never let the
  -- selected Trust's report scope hide that current candidate work.
  if v_tab='queries' and v_cycle.current_projection_publication_id is not null then
    if v_publication_id is not null
       and v_publication_id is distinct from v_cycle.current_projection_publication_id then
      raise exception 'WEEKLY_SOURCE_WORKSPACE_PUBLICATION_STALE' using errcode='40001';
    end if;
    v_publication_id:=v_cycle.current_projection_publication_id;
  else
    if v_publication_id is null and v_report_scope_id is not null then
      select current_projection_publication_id into v_publication_id
      from public.weekly_source_report_scopes where id=v_report_scope_id;
    end if;
    if v_publication_id is null and v_client_id is not null and v_group.source_family='NHSP' then
      select scope.current_projection_publication_id,scope.id
      into v_publication_id,v_report_scope_id
      from public.weekly_source_report_scopes scope
      where scope.source_cycle_id=v_cycle.id and scope.client_id=v_client_id
      order by scope.cutoff_at_utc desc,scope.id desc limit 1;
    end if;
  end if;
  if v_publication_id is null then
    v_publication_id:=v_cycle.current_projection_publication_id;
  end if;
  if v_publication_id is not null then
    v_guard:=private.weekly_source_query_current_publication_v1(v_cycle.id,v_publication_id);
    select * into strict v_publication from public.weekly_source_projection_publications
    where id=v_publication_id;
    select * into strict v_upload from public.weekly_source_uploads where id=v_publication.upload_id;
    select profile.* into strict v_profile
    from public.weekly_source_format_profiles profile where profile.id=v_upload.source_format_profile_id;
    v_workspace_version:=private.weekly_source_office_workspace_version_v1(v_cycle.id,v_publication.id);
  else
    v_workspace_version:=pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_OFFICE_WORKSPACE_VERSION_V1',pg_catalog.jsonb_build_object(
        'source_cycle_id',v_cycle.id,'cycle_version',v_cycle.version,
        'cycle_state',v_cycle.state,'projection_state',v_cycle.projection_state,
        'completions',coalesce((select pg_catalog.jsonb_agg(pg_catalog.encode(completion.completion_hash,'hex')
          order by completion.client_id) from public.weekly_source_client_cycle_completions completion
          where completion.source_cycle_id=v_cycle.id and completion.state='CURRENT'),'[]'::jsonb)
      )),'hex');
  end if;

  v_cursor_view_version:=case when v_pay_view then encode(private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_PAYMENT_CURSOR_V1',jsonb_build_object('actor',v_actor,'view','PAY','source',v_workspace_version)),
    'hex') else v_workspace_version end;
  v_cursor:=coalesce(nullif(p_request->>'cursor',''),'');
  if v_cursor<>'' then
    if split_part(v_cursor,':',1)<>v_cursor_view_version
       or split_part(v_cursor,':',2)!~ '^[0-9]+$' then
      raise exception 'WEEKLY_SOURCE_WORKSPACE_CURSOR_STALE' using errcode='40001';
    end if;
    v_offset:=split_part(v_cursor,':',2)::integer;
  end if;

  if v_publication_id is not null then
    select pg_catalog.count(*)::integer,
      pg_catalog.count(*) filter (where query.outreach_eligible)::integer,
      pg_catalog.count(*) filter (where query.manager_eligible)::integer,
      coalesce(pg_catalog.jsonb_agg(query.group_key order by query.group_key),'[]'::jsonb)
    into v_query_count,v_ask_eligible_count,v_manager_eligible_count,v_all_group_keys
    from private.weekly_source_office_query_groups_v1(v_cycle.id,v_publication.id,v_filters) query
    where (not v_pay_view or exists(select 1 from jsonb_array_elements(query.children) child
      where child ? 'manual_review_id' or jsonb_typeof(child->'protected_pay_seed')='object'
        or coalesce((child->>'accept_eligible')::boolean,false)));
    v_ask_selection_proof:=pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_BULK_FILTER_SELECTION_V1',pg_catalog.jsonb_build_object(
        'action','ASK_CANDIDATES','source_cycle_id',v_cycle.id,
        'projection_publication_id',v_publication.id,
        'workspace_version',v_workspace_version,'filters',v_filters,
        'group_keys',v_all_group_keys
      )),'hex');
    v_manager_selection_proof:=pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_BULK_FILTER_SELECTION_V1',pg_catalog.jsonb_build_object(
        'action','SEND_MANAGER_NOW','source_cycle_id',v_cycle.id,
        'projection_publication_id',v_publication.id,
        'workspace_version',v_workspace_version,'filters',v_filters,
        'group_keys',v_all_group_keys
      )),'hex');
    v_bulk_actions:=pg_catalog.jsonb_build_object(
      'contract','WEEKLY_SOURCE_BULK_FILTER_SELECTION_V1',
      'filtered_group_count',v_query_count,'selection_complete',true,
      'ask_candidates',pg_catalog.jsonb_build_object(
        'action','ASK_CANDIDATES','enabled',v_ask_eligible_count>0,
        'eligible_group_count',v_ask_eligible_count,
        'selection_proof',v_ask_selection_proof,
        'request',pg_catalog.jsonb_build_object(
          'actor_user_id',v_actor,'source_cycle_id',v_cycle.id,
          'projection_publication_id',v_publication.id,
          'expected_workspace_version',v_workspace_version,'action','ASK_CANDIDATES',
          'selection',pg_catalog.jsonb_build_object(
            'mode','ALL_FILTERED','group_keys','[]'::jsonb,
            'excluded_group_keys','[]'::jsonb,'incident_ids','[]'::jsonb,
            'filters',v_filters,'sort_key',v_sort_key,
            'sort_direction',v_sort_direction,'selection_proof',v_ask_selection_proof
          )
        )
      ),
      'send_manager_now',pg_catalog.jsonb_build_object(
        'action','SEND_MANAGER_NOW','enabled',v_manager_eligible_count>0,
        'eligible_group_count',v_manager_eligible_count,
        'selection_proof',v_manager_selection_proof,
        'request',pg_catalog.jsonb_build_object(
          'actor_user_id',v_actor,'source_cycle_id',v_cycle.id,
          'projection_publication_id',v_publication.id,
          'expected_workspace_version',v_workspace_version,'action','SEND_MANAGER_NOW',
          'selection',pg_catalog.jsonb_build_object(
            'mode','ALL_FILTERED','group_keys','[]'::jsonb,
            'excluded_group_keys','[]'::jsonb,'incident_ids','[]'::jsonb,
            'filters',v_filters,'sort_key',v_sort_key,
            'sort_direction',v_sort_direction,'selection_proof',v_manager_selection_proof
          )
        )
      )
    );
  end if;
  select pg_catalog.count(distinct family.id)::integer into v_paid_unresolved_count
  from public.weekly_exceptional_pay_target_families family
  join public.weekly_exceptional_payment_approvals approval on approval.pay_target_family_id=family.id
  where approval.source_cycle_id=v_cycle.id
    and family.current_lifecycle_state in ('PROTECTED','WAITING_SOURCE','READY_TO_RECONCILE','ACTION_REQUIRED');

  select pg_catalog.count(*)::integer into v_blocker_count
  from public.weekly_source_upload_rows source_row
  left join lateral (
    select resolution.* from public.weekly_source_row_resolutions resolution
    where resolution.upload_row_id=source_row.id
    order by resolution.generation desc,resolution.id desc limit 1
  ) resolution on true
  left join lateral (
    select charge.* from public.weekly_source_charge_checks charge
    where charge.upload_row_id=source_row.id
    order by charge.generation desc,charge.id desc limit 1
  ) charge on true
  left join public.weekly_source_charge_acceptances charge_acceptance
    on charge_acceptance.charge_check_id=charge.id
  where v_publication_id is not null and source_row.upload_id=v_upload.id
    and (resolution.mapping_state is distinct from 'RESOLVED'
      or source_row.row_finalisation_state in ('BLOCK_FINALISATION_DISAGREEMENT','BLOCK_ACTUAL_TUPLE')
      or charge.phase_severity='FINALISATION_BLOCKER'
      or (charge.phase_severity='PROVISIONAL_WARNING' and charge_acceptance.id is null));

  -- PHD-014..019 / PRC-043..050.  The final NHSP source value remains
  -- authoritative, but a known disparity or structurally valid zero charge is
  -- shown as one concise, server-owned warning decision.  The Trust-wide zero
  -- pattern has one notice, with a decision for each shift. No source money
  -- from this projection is ever an input to Candidate pay.
  if v_publication_id is not null
     and v_profile.profile_code in ('NHSP_PREFINAL_RELEASED_V1','NHSP_FINAL_BACKING_V1') then
    select pg_catalog.count(*)::integer,
      pg_catalog.count(acceptance.id)::integer,
      pg_catalog.count(*) filter (where acceptance.id is null)::integer,
      pg_catalog.count(*) filter (where charge.comparison_result='ZERO_SOURCE_CHARGE')::integer,
      pg_catalog.count(*) filter (where charge.comparison_result='MISMATCH')::integer
    into v_rate_warning_count,v_rate_warning_accepted_count,
      v_rate_warning_unaccepted_count,v_rate_warning_zero_count,
      v_rate_warning_disparity_count
    from public.weekly_source_charge_checks charge
    join public.weekly_source_upload_rows source_row on source_row.id=charge.upload_row_id
    left join public.weekly_source_charge_acceptances acceptance
      on acceptance.charge_check_id=charge.id
    where source_row.upload_id=v_upload.id
      and charge.generation=coalesce(v_publication.projection_generation,v_publication.authority_scope_version)
      and charge.comparison_result in ('MISMATCH','ZERO_SOURCE_CHARGE')
      and charge.phase_severity='PROVISIONAL_WARNING'
      and charge.blocker_code is null;

    if v_rate_warning_count>0 then
      select coalesce(pg_catalog.jsonb_agg(warning.row_json order by warning.work_date,warning.candidate_name,warning.warning_key),'[]'::jsonb)
      into v_rate_warning_rows
      from (
        select source_row.work_date,
          coalesce(candidate.display_name,candidate.tms_ref,source_row.source_candidate_identity,'Candidate') candidate_name,
          'charge-check:'||charge.id::text warning_key,
          pg_catalog.jsonb_build_object(
            'warning_key','charge-check:'||charge.id::text,
            'source_row_id',source_row.id,
            'candidate',coalesce(candidate.display_name,candidate.tms_ref,source_row.source_candidate_identity,'Candidate'),
            'day_date',to_char(source_row.work_date,'Dy FMDD Mon YYYY'),
            'source_charge',case when charge.source_shift_charge_pence<0 then '-£' else '£' end
              ||to_char(pg_catalog.abs(charge.source_shift_charge_pence)::numeric/100,'FM9999999990.00'),
            'commission',case when charge.source_commission_pence<0 then '-£' else '£' end
              ||to_char(pg_catalog.abs(charge.source_commission_pence)::numeric/100,'FM9999999990.00'),
            'total_cost',case when charge.source_total_cost_pence<0 then '-£' else '£' end
              ||to_char(pg_catalog.abs(charge.source_total_cost_pence)::numeric/100,'FM9999999990.00'),
            'calculated_charge',case when charge.calculated_segment_charge_pence<0 then '-£' else '£' end
              ||to_char(pg_catalog.abs(charge.calculated_segment_charge_pence)::numeric/100,'FM9999999990.00'),
            'difference',case when charge.source_charge_difference_pence<0 then '-£' else '£' end
              ||to_char(pg_catalog.abs(charge.source_charge_difference_pence)::numeric/100,'FM9999999990.00'),
            'contract',pg_catalog.concat_ws(' · ',contract.role,contract.band,contract.display_site),
            'warning',case when charge.comparison_result='ZERO_SOURCE_CHARGE'
              then 'Possible NHSP rate card issue' else 'Rate card expired or wrong Contract rate' end,
            'warning_tone','warning',
            'action_label',case when acceptance.id is null then 'Review charge' else 'Accepted' end,
            'accept_eligible',v_profile.profile_code='NHSP_FINAL_BACKING_V1'
              and resolution.mapping_state='RESOLVED' and resolution.contract_id is not null
              and acceptance.id is null,
            'detail_rows','[]'::jsonb
          ) row_json
        from public.weekly_source_charge_checks charge
        join public.weekly_source_upload_rows source_row on source_row.id=charge.upload_row_id
        left join public.weekly_source_row_resolutions resolution on resolution.id=charge.row_resolution_id
        left join public.candidates candidate on candidate.id=resolution.candidate_id
        left join public.contracts contract on contract.id=resolution.contract_id
        left join public.weekly_source_charge_acceptances acceptance on acceptance.charge_check_id=charge.id
        where source_row.upload_id=v_upload.id
          and charge.generation=coalesce(v_publication.projection_generation,v_publication.authority_scope_version)
          and charge.comparison_result in ('MISMATCH','ZERO_SOURCE_CHARGE')
          and charge.phase_severity='PROVISIONAL_WARNING'
          and charge.blocker_code is null
      ) warning;

      if v_profile.profile_code='NHSP_FINAL_BACKING_V1' and v_rate_warning_unaccepted_count>0 then
        select coalesce(pg_catalog.jsonb_agg(key_value order by key_value),'[]'::jsonb)
        into v_rate_warning_keys
        from (
          select 'charge-check:'||charge.id::text key_value
          from public.weekly_source_charge_checks charge
          join public.weekly_source_upload_rows source_row on source_row.id=charge.upload_row_id
          left join public.weekly_source_charge_acceptances acceptance on acceptance.charge_check_id=charge.id
          where source_row.upload_id=v_upload.id
            and charge.generation=coalesce(v_publication.projection_generation,v_publication.authority_scope_version)
            and charge.comparison_result in ('MISMATCH','ZERO_SOURCE_CHARGE')
            and charge.phase_severity='PROVISIONAL_WARNING'
            and charge.blocker_code is null and acceptance.id is null
        ) eligible;
        v_rate_warning_selection_proof:=pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
          'NHSP_RATE_WARNING_SELECTION_V1',pg_catalog.jsonb_build_object(
            'source_cycle_id',v_cycle.id,'projection_publication_id',v_publication.id,
            'projection_generation',coalesce(v_publication.projection_generation,v_publication.authority_scope_version),
            'workspace_version',v_workspace_version,
            'eligible_warning_keys',v_rate_warning_keys
          )),'hex');
      end if;

      v_rate_warnings:=pg_catalog.jsonb_build_object(
        'contract','NHSP_RATE_WARNING_WORKSPACE_V1',
        'phase',case when v_profile.profile_code='NHSP_PREFINAL_RELEASED_V1' then 'PREFINAL'
          when v_rate_warning_unaccepted_count>0 then 'FINAL_AWAITING_ACCEPTANCE' else 'READY' end,
        'rows',v_rate_warning_rows,'total_count',v_rate_warning_count,
        'accepted_count',v_rate_warning_accepted_count,
        'ready_count',v_rate_warning_accepted_count,'hard_blocker_count',0,
        'notice',case when v_rate_warning_zero_count>1 then pg_catalog.jsonb_build_object(
          'title','Possible Trust rate card issue',
          'body',v_rate_warning_zero_count::text||' shifts have a £0 source charge. Check the Trust rate card in NHSP'
            ||case when v_profile.profile_code='NHSP_FINAL_BACKING_V1' then ' before accepting.' else '.' end
            ||case when v_rate_warning_disparity_count=1 then ' One other shift has a different source charge.'
              when v_rate_warning_disparity_count>1 then ' '||v_rate_warning_disparity_count::text||' other shifts have different source charges.' else '' end,
          'tone','warning') else null end,
        'acceptance',case when v_profile.profile_code='NHSP_FINAL_BACKING_V1'
            and v_rate_warning_unaccepted_count>0 then pg_catalog.jsonb_build_object(
          'enabled',true,'action','ACCEPT_NHSP_SOURCE_CHARGES',
          'payload',pg_catalog.jsonb_build_object(
            'source_cycle_id',v_cycle.id,'projection_publication_id',v_publication.id),
          'selection',pg_catalog.jsonb_build_object(
            'key','warning_keys','proof_key','selection_proof','proof',v_rate_warning_selection_proof)
        ) else null end
      );
    end if;
  end if;

  if v_tab='imports' then
    select pg_catalog.count(*)::integer into v_total
    from public.weekly_source_uploads upload where upload.source_cycle_id=v_cycle.id;
    select coalesce(pg_catalog.jsonb_agg(page.row_json order by page.position),'[]'::jsonb)
    into v_rows
    from (
      select pg_catalog.jsonb_build_object(
        'row_key',upload.id,'file',upload.original_filename,
        'client_id',coalesce(scope.client_id,v_cycle.scope_client_id),
        'uploaded_at',upload.uploaded_at_utc,'state',upload.state,
        'prepared',private.weekly_source_import_is_prepared_v1(upload.id),
        'purpose_label',case when exists(select 1 from public.weekly_source_format_profiles file_profile
          where file_profile.id=upload.source_format_profile_id and file_profile.profile_code='NHSP_FINAL_BACKING_V1')
          or upload.file_metadata_json->>'import_use'='PREPARE_FINALISATION'
          then 'Finalisation report' else 'Checking hours' end,
        'uploaded',to_char(upload.uploaded_at_utc at time zone 'Europe/London','DD Mon YYYY HH24:MI'),
        'rows',upload.accepted_count,
        'coverage',case when upload.confirmed_coverage_start_local_date is null then 'Not confirmed'
          else to_char(upload.confirmed_coverage_start_local_date,'DD Mon YYYY')||' to '
            ||to_char(upload.confirmed_coverage_end_local_date,'DD Mon YYYY') end,
        'report',case when v_group.source_family='NHSP' then coalesce(
          nullif(pg_catalog.btrim(upload.file_metadata_json->>'nhsp_report_number'),''),'Not confirmed') else null end,
        'cutoff',case when v_group.source_family='NHSP' then coalesce(
          to_char(scope.cutoff_at_utc at time zone 'Europe/London','FMDD Mon YYYY · HH24:MI'),'Not confirmed') else null end,
        'status',pg_catalog.jsonb_build_object('text',case upload.state
          when 'CURRENT' then 'Current' when 'SUPERSEDED' then 'Superseded'
          when 'SEALED' then 'Ready' else initcap(pg_catalog.replace(upload.state,'_',' ')) end,
          'tone',case when upload.state='CURRENT' then 'positive'
            when upload.state in ('REJECTED') then 'danger' else 'neutral' end),
        'final_source',case when exists(select 1 from public.weekly_source_final_revisions revision
          where revision.upload_id=upload.id and revision.state='CURRENT') then 'Finalised' else pg_catalog.chr(8212) end,
        'actions',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
          'label','View','enabled',true,'payload',pg_catalog.jsonb_build_object('upload_id',upload.id)
        ))
      ) row_json,
      pg_catalog.row_number() over(order by
        case when v_sort_key='file' and v_sort_direction='asc' then upload.original_filename end asc,
        case when v_sort_key='file' and v_sort_direction='desc' then upload.original_filename end desc,
        case when v_sort_key='rows' and v_sort_direction='asc' then upload.accepted_count end asc,
        case when v_sort_key='rows' and v_sort_direction='desc' then upload.accepted_count end desc,
        case when v_sort_key='report' and v_sort_direction='asc' then upload.file_metadata_json->>'nhsp_report_number' end asc nulls last,
        case when v_sort_key='report' and v_sort_direction='desc' then upload.file_metadata_json->>'nhsp_report_number' end desc nulls last,
        case when v_sort_key='cutoff' and v_sort_direction='asc' then scope.cutoff_at_utc end asc nulls last,
        case when v_sort_key='cutoff' and v_sort_direction='desc' then scope.cutoff_at_utc end desc nulls last,
        case when v_sort_direction='asc' then upload.uploaded_at_utc end asc,
        case when v_sort_direction='desc' then upload.uploaded_at_utc end desc,
        upload.id
      ) position
      from public.weekly_source_uploads upload
      left join public.weekly_source_report_scopes scope on scope.id=upload.report_scope_id
      where upload.source_cycle_id=v_cycle.id
      order by
        case when v_sort_key='file' and v_sort_direction='asc' then upload.original_filename end asc,
        case when v_sort_key='file' and v_sort_direction='desc' then upload.original_filename end desc,
        case when v_sort_key='rows' and v_sort_direction='asc' then upload.accepted_count end asc,
        case when v_sort_key='rows' and v_sort_direction='desc' then upload.accepted_count end desc,
        case when v_sort_key='report' and v_sort_direction='asc' then upload.file_metadata_json->>'nhsp_report_number' end asc nulls last,
        case when v_sort_key='report' and v_sort_direction='desc' then upload.file_metadata_json->>'nhsp_report_number' end desc nulls last,
        case when v_sort_key='cutoff' and v_sort_direction='asc' then scope.cutoff_at_utc end asc nulls last,
        case when v_sort_key='cutoff' and v_sort_direction='desc' then scope.cutoff_at_utc end desc nulls last,
        case when v_sort_direction='asc' then upload.uploaded_at_utc end asc,
        case when v_sort_direction='desc' then upload.uploaded_at_utc end desc,
        upload.id
      offset v_offset limit v_limit
    ) page;
    v_next_cursor:=case when v_offset+v_limit<v_total
      then v_cursor_view_version||':'||(v_offset+v_limit)::text else '' end;
    v_imports:=pg_catalog.jsonb_build_object('rows',v_rows,'total_count',v_total,
      'next_cursor',v_next_cursor,'has_more',v_next_cursor<>'','record_version',v_workspace_version,'stale',false);
  else
    v_imports:=pg_catalog.jsonb_build_object('rows','[]'::jsonb,'total_count',
      (select pg_catalog.count(*) from public.weekly_source_uploads upload where upload.source_cycle_id=v_cycle.id),
      'next_cursor','','has_more',false,'record_version',v_workspace_version,'stale',false);
  end if;

  if v_authority_mode='TIMESHEET_AUTHORITY' and v_publication_id is not null then
    -- The established import-review owns the manager correction email.  Give
    -- Office its exact per-client review rather than reopening the upload hub.
    select mode_a_import.id into v_mode_a_import_id
    from public.hr_imports mode_a_import
    where mode_a_import.coverage_operation_key=
      private.weekly_source_mode_a_operation_key_v1(v_publication_id,v_client_id);

    -- Resolved source work may arrive before the candidate and manager have
    -- completed their Timesheet. Keep that work visible without treating it
    -- as a comparison, an Office authorisation, or source finalisation.
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'row_key','authority-waiting-'||waiting.contract_id::text,
      'candidate',waiting.candidate_name,
      'day_date','Week ending '||to_char(v_cycle.finalisation_week_ending,'FMDD Mon YYYY'),
      'attention','Timesheet not yet completed',
      'reference','Not added',
      'timesheet_id',waiting.timesheet_id,
      'status',pg_catalog.jsonb_build_object('text','Waiting for completed Timesheet','tone','neutral'),
      'actions',case when waiting.timesheet_id is null then '[]'::jsonb
        else pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
          'label','View Timesheet','enabled',true,
          'payload',pg_catalog.jsonb_build_object('timesheet_id',waiting.timesheet_id))) end
    ) order by private.weekly_source_query_ascii_fold_v1(waiting.candidate_name) collate "C",
      waiting.contract_id),'[]'::jsonb) into v_import_waiting_rows
    from (
      select distinct on (resolution.contract_id) resolution.contract_id,
        coalesce(candidate.display_name,candidate.tms_ref,'Candidate') candidate_name,
        current_timesheet.timesheet_id
      from public.weekly_source_row_resolutions resolution
      join public.weekly_source_upload_rows source_row on source_row.id=resolution.upload_row_id
        and source_row.upload_id=v_upload.id
      join public.candidates candidate on candidate.id=resolution.candidate_id
      left join lateral (
        select timesheet_row.timesheet_id
        from public.timesheets timesheet_row
        where timesheet_row.contract_id=resolution.contract_id
          and timesheet_row.week_ending_date=v_cycle.finalisation_week_ending
          and timesheet_row.is_current
          and timesheet_row.revoked_at is null
        order by timesheet_row.version desc,timesheet_row.timesheet_id limit 1
      ) current_timesheet on true
      left join lateral (
        select workflow.id,workflow.state,workflow.candidate_signed_at_utc,
          workflow.manager_approved_at_utc,workflow.candidate_signature_component_id,
          workflow.manager_signature_component_id
        from public.candidate_submission_workflows workflow
        where workflow.contract_id=resolution.contract_id
          and workflow.week_ending_date=v_cycle.finalisation_week_ending
          and workflow.scope='WEEKLY'
          and workflow.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED')
        order by workflow.updated_at_utc desc,workflow.id desc limit 1
      ) current_workflow on true
      where resolution.generation=coalesce(
          v_publication.projection_generation,v_publication.authority_scope_version::integer)
        and resolution.client_id=v_client_id
        and resolution.mapping_state='RESOLVED'
        and resolution.work_event_id is not null
        and exists(select 1 from public.weekly_timesheet_authority_resolutions authority
          where authority.source_cycle_id=v_cycle.id
            and authority.contract_id=resolution.contract_id
            and authority.work_date=source_row.work_date)
        and (current_timesheet.timesheet_id is null
          or (current_workflow.id is not null and not (
            current_workflow.state='FINALISED'
            and current_workflow.candidate_signed_at_utc is not null
            and current_workflow.manager_approved_at_utc is not null
            and current_workflow.candidate_signature_component_id is not null
            and current_workflow.manager_signature_component_id is not null)))
      order by resolution.contract_id,source_row.work_date,resolution.id
    ) waiting;
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'row_key','authority-'||comparison.id::text,
      'candidate',coalesce(candidate.display_name,candidate.tms_ref,'Candidate'),
      'day_date',to_char(comparison.work_date,'Dy FMDD Mon YYYY'),
      'attention',case comparison.comparison_state
        when 'HOURS_MISMATCH' then 'Hours are different'
        when 'SOURCE_SHIFT_MISSING' then 'Shift is missing or not yet authorised'
        when 'REFERENCE_MISSING' then 'Reference is missing'
        when 'AMBIGUOUS_SOURCE_ROW' then 'More than one source shift matches'
        else 'Check this Timesheet' end,
      'reference',case when nullif(pg_catalog.btrim(comparison.source_reference_number),'') is null
        then 'Not added' else comparison.source_reference_number end,
      'status',pg_catalog.jsonb_build_object(
        'text',case when comparison.comparison_state='REFERENCE_MISSING'
          then 'Reference needed' else 'Manager correction needed' end,
        'tone','warning'),
      'actions',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('label','View Timesheet','enabled',true,
          'payload',pg_catalog.jsonb_build_object('timesheet_id',comparison.timesheet_id)),
        pg_catalog.jsonb_build_object('label','Email manager','enabled',v_mode_a_import_id is not null,
          'payload',pg_catalog.jsonb_build_object('timesheet_id',comparison.timesheet_id,
            'comparison_id',comparison.id,'import_id',v_mode_a_import_id)))
    ) order by private.weekly_source_query_ascii_fold_v1(coalesce(candidate.display_name,candidate.tms_ref,'')) collate "C",
      comparison.work_date,comparison.id),'[]'::jsonb)
    into v_import_attention_rows
    from public.weekly_timesheet_source_comparisons comparison
    join public.timesheets timesheet_row on timesheet_row.timesheet_id=comparison.timesheet_id
      and timesheet_row.is_current
    join public.contracts contract on contract.id=timesheet_row.contract_id
    left join public.candidates candidate on candidate.id=contract.candidate_id
    where comparison.projection_publication_id=v_publication_id
      and comparison.comparison_state<>'EXACT_MATCH'
      and not exists(select 1 from pg_catalog.jsonb_array_elements(v_import_waiting_rows) waiting
        where waiting->>'timesheet_id'=comparison.timesheet_id::text);

    -- Ready is a Timesheet-authority handoff, never a source-finalisation
    -- queue.  The established reference owner must already have applied the
    -- complete matching Timesheet, and an Office-authorised head no longer
    -- needs this action list.
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'row_key','authority-ready-'||ready.timesheet_id::text,
      'candidate',ready.candidate_name,
      'day_date',to_char(ready.work_date,'Dy FMDD Mon YYYY'),
      'attention','Source reference added',
      'reference',ready.source_reference_number,
      'status',pg_catalog.jsonb_build_object('text','Ready for Office authorisation','tone','positive'),
      'actions',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'label','View Timesheet','enabled',true,
        'payload',pg_catalog.jsonb_build_object('timesheet_id',ready.timesheet_id)))
    ) order by private.weekly_source_query_ascii_fold_v1(
        ready.candidate_name) collate "C",ready.work_date,ready.timesheet_id),'[]'::jsonb)
      into v_import_ready_rows
    from (
      select timesheet_row.timesheet_id,
        coalesce(candidate.display_name,candidate.tms_ref,'Candidate') as candidate_name,
        pg_catalog.min(comparison.work_date) as work_date,
        pg_catalog.min(comparison.source_reference_number) as source_reference_number
      from public.weekly_timesheet_source_comparisons comparison
      join public.timesheets timesheet_row on timesheet_row.timesheet_id=comparison.timesheet_id
        and timesheet_row.is_current and timesheet_row.authorised_at_server is null
      join public.contracts contract on contract.id=timesheet_row.contract_id
      left join public.candidates candidate on candidate.id=contract.candidate_id
      where comparison.projection_publication_id=v_publication_id
        and comparison.comparison_state='EXACT_MATCH'
        and not exists(select 1 from pg_catalog.jsonb_array_elements(v_import_waiting_rows) waiting
          where waiting->>'timesheet_id'=comparison.timesheet_id::text)
        and not exists (
          select 1 from public.weekly_timesheet_source_comparisons other
          where other.projection_publication_id=v_publication_id
            and other.timesheet_id=comparison.timesheet_id
            and other.comparison_state<>'EXACT_MATCH'
        )
        and exists (
          select 1 from public.weekly_timesheet_reference_apply_operations operation
          where operation.projection_publication_id=v_publication_id
            and operation.timesheet_id=comparison.timesheet_id
            and operation.state='APPLIED'
        )
      group by timesheet_row.timesheet_id,candidate.display_name,candidate.tms_ref
    ) ready;
  end if;

  v_import_journey:=pg_catalog.jsonb_build_object(
    'authority_mode',v_authority_mode,
    'document_mode',v_document_mode,
    'title',case when v_authority_mode='TIMESHEET_AUTHORITY'
      then 'Signed Timesheet decides hours' else 'Client system decides hours' end,
    'body',case when v_authority_mode='TIMESHEET_AUTHORITY'
      then 'Timesheet hours are used. The client system is checked so matching references can be added.'
      else 'Client system hours are used after the source is finalised.' end,
    'import_id',v_mode_a_import_id,
    'waiting_rows',v_import_waiting_rows,
    'waiting_count',pg_catalog.jsonb_array_length(v_import_waiting_rows),
    'attention_rows',v_import_attention_rows,
    'attention_count',pg_catalog.jsonb_array_length(v_import_attention_rows),
    'ready_rows',v_import_ready_rows,
    'ready_count',pg_catalog.jsonb_array_length(v_import_ready_rows)
  );
  v_imports:=v_imports||pg_catalog.jsonb_build_object('journey',v_import_journey);

  if v_tab='queries' and v_publication_id is not null then
    v_total:=v_query_count;
    select coalesce(pg_catalog.jsonb_agg(page.row_json order by page.position),'[]'::jsonb)
    into v_rows
    from (
      select pg_catalog.jsonb_build_object(
        'row_key',query.group_key,'group_key',query.group_key,
        'client_id',query.client_id,'candidate_id',query.candidate_id,
        'candidate_sort',coalesce((select nullif(person.last_name,'') from public.candidates person
          where person.id=query.candidate_id),query.candidate_name),
        'candidate',query.candidate_name,'client',query.client_name,'issues',query.issue_count,
        'candidate_asked',query.candidate_asked,'manager_informed',query.manager_informed,
        'status',pg_catalog.jsonb_build_object('text',query.status_text,'tone',query.status_tone),
        'age',case when query.first_seen_at_utc is null then pg_catalog.chr(8212)
          when pg_catalog.transaction_timestamp()-query.first_seen_at_utc<interval '1 hour' then 'Less than 1 hour'
          when pg_catalog.transaction_timestamp()-query.first_seen_at_utc<interval '24 hours'
            then floor(extract(epoch from (pg_catalog.transaction_timestamp()-query.first_seen_at_utc))/3600)::integer||' hours'
          else floor(extract(epoch from (pg_catalog.transaction_timestamp()-query.first_seen_at_utc))/86400)::integer||' days' end,
        'outreach_eligible',query.outreach_eligible,
        'manager_eligible',query.manager_eligible,
        'candidate_app_available',query.candidate_app_available,
        'contact_policy',private.weekly_source_office_contact_policy_v1(v_cycle.id,
          to_jsonb(query)||jsonb_build_object('projection_publication_id',v_publication.id)),
        'children',coalesce((
          select pg_catalog.jsonb_agg(
            (child.value-'actions')||pg_catalog.jsonb_build_object(
              'actions',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
                'label','View details','enabled',true,
                'payload',pg_catalog.jsonb_build_object('detail',pg_catalog.jsonb_build_object(
                  'candidate',query.candidate_name,'client',query.client_name,
                  'status',child.value#>>'{status,text}',
                  'age',case when query.first_seen_at_utc is null then pg_catalog.chr(8212)
                    when pg_catalog.transaction_timestamp()-query.first_seen_at_utc<interval '1 hour' then 'Less than 1 hour'
                    when pg_catalog.transaction_timestamp()-query.first_seen_at_utc<interval '24 hours'
                      then floor(extract(epoch from (pg_catalog.transaction_timestamp()-query.first_seen_at_utc))/3600)::integer||' hours'
                    else floor(extract(epoch from (pg_catalog.transaction_timestamp()-query.first_seen_at_utc))/86400)::integer||' days' end,
                  'shifts',pg_catalog.jsonb_build_array(child.value-'actions')
                ))
              ))||case when pg_catalog.jsonb_typeof(child.value->'protected_pay_seed')='object'
                and private.weekly_source_pay_actor_allowed_v1(v_actor)
                then pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
                  'label','Protect pay','enabled',true,'payload',child.value->'protected_pay_seed'))
                else '[]'::jsonb end
              ||case when child.value ? 'manual_review_id' then
                pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
                  'label','Accept current source hours','kind','COMMAND',
                  'command','RESOLVE_MANUAL_REVIEW',
                  'payload',pg_catalog.jsonb_build_object(
                    'review_id',child.value->>'manual_review_id',
                    'resolution_kind','OFFICE_ACCEPTED_SOURCE',
                    'expected_current_row_hash',child.value->>'current_source_row_hash'),
                  'enabled',child.value->>'current_source_row_hash' is not null,
                  'context',pg_catalog.jsonb_build_object(
                    'candidate',query.candidate_name,'client',query.client_name,
                    'status',child.value->>'day_date',
                    'system_hours',child.value->>'system_hours')))
                else '[]'::jsonb end
            ) order by child.ordinality
          )
          from pg_catalog.jsonb_array_elements(query.children) with ordinality child(value,ordinality)
        ),'[]'::jsonb),
        'actions',pg_catalog.jsonb_build_array(
          pg_catalog.jsonb_build_object(
            'label','Open','enabled',true,
            'payload',pg_catalog.jsonb_build_object(
              'group_key',query.group_key,
              'detail',pg_catalog.jsonb_build_object(
                'candidate',query.candidate_name,'client',query.client_name,
                'status',query.status_text,
                'age',case when query.first_seen_at_utc is null then pg_catalog.chr(8212)
                  when pg_catalog.transaction_timestamp()-query.first_seen_at_utc<interval '1 hour' then 'Less than 1 hour'
                  when pg_catalog.transaction_timestamp()-query.first_seen_at_utc<interval '24 hours'
                    then floor(extract(epoch from (pg_catalog.transaction_timestamp()-query.first_seen_at_utc))/3600)::integer||' hours'
                  else floor(extract(epoch from (pg_catalog.transaction_timestamp()-query.first_seen_at_utc))/86400)::integer||' days' end,
                'shifts',query.children
              )
            )
          ),
          pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
            'label','Remind candidate','kind','COMMAND','command','REMIND_CANDIDATE',
            'enabled',candidate_reminder.candidate_generation_id is not null
              and (candidate_reminder.manual_reminder_available_at_utc is null
                or pg_catalog.transaction_timestamp()>=candidate_reminder.manual_reminder_available_at_utc),
            'reason',case
              when candidate_reminder.candidate_generation_id is null then 'Ask the candidate before sending a reminder.'
              when candidate_reminder.manual_reminder_available_at_utc is not null
               and pg_catalog.transaction_timestamp()<candidate_reminder.manual_reminder_available_at_utc
                then 'A reminder can be sent after '
                  ||to_char(candidate_reminder.manual_reminder_available_at_utc at time zone 'Europe/London','Dy FMDD Mon YYYY HH24:MI')||'.'
              else null end,
            'available_at_utc',candidate_reminder.manual_reminder_available_at_utc,
            'context',pg_catalog.jsonb_build_object(
              'candidate',query.candidate_name,'client',query.client_name,'status',query.status_text
            ),
            'payload',case when candidate_reminder.candidate_generation_id is not null
              and (candidate_reminder.manual_reminder_available_at_utc is null
                or pg_catalog.transaction_timestamp()>=candidate_reminder.manual_reminder_available_at_utc)
              then pg_catalog.jsonb_build_object(
                'candidate_generation_id',candidate_reminder.candidate_generation_id,
                'projection_publication_id',v_publication.id
              ) else '{}'::jsonb end
          ))
        )||case when submission_reminder.generation_id is not null then pg_catalog.jsonb_build_array(
          pg_catalog.jsonb_build_object('label','Remind missing timesheet','kind','COMMAND','command','REMIND_CANDIDATE',
            'enabled',pg_catalog.transaction_timestamp()>=submission_reminder.available_at,
            'reason','Reminder for missing week(s): '||submission_reminder.weeks,
            'context',pg_catalog.jsonb_build_object('candidate',query.candidate_name,
              'client',query.client_name,'weeks',submission_reminder.weeks),
            'payload',pg_catalog.jsonb_build_object('candidate_generation_id',submission_reminder.generation_id,
              'projection_publication_id',v_publication.id))) else '[]'::jsonb end,
        'accept_system_hours_action',pg_catalog.jsonb_build_object(
          'label','Accept system hours','kind','COMMAND','command','ACCEPT_SYSTEM_HOURS',
          'enabled',coalesce(pg_catalog.array_length(query.accept_incident_ids,1),0)>0,
          'context',pg_catalog.jsonb_build_object(
            'candidate',query.candidate_name,'client',query.client_name,
            'issue',query.issue_summary
          ),
          'payload',pg_catalog.jsonb_build_object(
            'actor_user_id',v_actor,'source_cycle_id',v_cycle.id,
            'projection_publication_id',v_publication.id,
            'expected_workspace_version',v_workspace_version,'action','ACCEPT_SYSTEM_HOURS',
            'selection',pg_catalog.jsonb_build_object(
              'mode','EXPLICIT','group_keys',pg_catalog.jsonb_build_array(query.group_key),
              'excluded_group_keys','[]'::jsonb,
              'incident_ids',to_jsonb(query.accept_incident_ids),
              'filters',v_filters,'sort_key',v_sort_key,'sort_direction',v_sort_direction,
              'selection_proof',null,
              'group_selection_proofs',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
                'group_key',query.group_key,
                'selection_proof',pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
                  'WEEKLY_SOURCE_ACCEPT_GROUP_SELECTION_V1',pg_catalog.jsonb_build_object(
                    'workspace_version',v_workspace_version,'group_key',query.group_key,
                    'incident_ids',to_jsonb(query.accept_incident_ids)
                  )),'hex')
              ))
            )
          )
        )
      ) row_json,
      pg_catalog.row_number() over(order by
        case when v_sort_key='candidate' and v_sort_direction='asc' then private.weekly_source_query_ascii_fold_v1(query.candidate_name) end asc,
        case when v_sort_key='candidate' and v_sort_direction='desc' then private.weekly_source_query_ascii_fold_v1(query.candidate_name) end desc,
        case when v_sort_key='client' and v_sort_direction='asc' then private.weekly_source_query_ascii_fold_v1(query.client_name) end asc,
        case when v_sort_key='client' and v_sort_direction='desc' then private.weekly_source_query_ascii_fold_v1(query.client_name) end desc,
        case when v_sort_key='issues' and v_sort_direction='asc' then query.issue_count end asc,
        case when v_sort_key='issues' and v_sort_direction='desc' then query.issue_count end desc,
        case when v_sort_key='age' and v_sort_direction='asc' then query.first_seen_at_utc end asc,
        case when v_sort_key='age' and v_sort_direction='desc' then query.first_seen_at_utc end desc,
        query.group_key
      ) position
      from private.weekly_source_office_query_groups_v1(v_cycle.id,v_publication.id,v_filters) query
      left join lateral (
        select generation.id candidate_generation_id,
          generation.manual_reminder_available_at_utc
        from public.weekly_candidate_cohorts cohort
        join public.weekly_candidate_outreach_generations generation
          on generation.id=cohort.current_generation_id and generation.state='ACTIVE'
        where cohort.source_cycle_id=v_cycle.id
          and cohort.candidate_id=query.candidate_id
          and cohort.client_id=query.client_id
          and cohort.manager_recipient_route_key=query.manager_recipient_route_key
          and generation.request_kind='CHECK_HOURS'
          and exists(select 1 from public.weekly_candidate_outreach_memberships membership
            join public.weekly_discrepancy_incidents incident on incident.id=membership.incident_id
            where membership.candidate_generation_id=generation.id and membership.state='ACTIONABLE'
              and incident.state='OPEN' and incident.candidate_action_state='ASKED'
              and membership.comparison_revision_id=incident.current_comparison_revision_id
              and exists(select 1 from pg_catalog.jsonb_array_elements(query.children) child
                where child->>'incident_id'=incident.id::text))
        limit 1
      ) candidate_reminder on true
      left join lateral (
        select generation.id generation_id,generation.manual_reminder_available_at_utc available_at,
          pg_catalog.string_agg(distinct to_char(membership.week_ending,'FMDD Mon YYYY'),', ') weeks
        from public.weekly_candidate_cohorts cohort
        join public.weekly_candidate_outreach_generations generation
          on generation.id=cohort.current_submission_generation_id and generation.state='ACTIVE'
          and generation.request_kind='SUBMIT_TIMESHEET'
        join public.weekly_timesheet_submission_requests request
          on request.candidate_cohort_id=cohort.id and request.request_generation=generation.generation_number
          and request.state in ('ACTIVE','OVERDUE','PARTLY_SUBMITTED')
        join public.weekly_timesheet_submission_request_memberships membership
          on membership.submission_request_id=request.id and membership.state='WAITING'
        where cohort.source_cycle_id=v_cycle.id and cohort.candidate_id=query.candidate_id
          and exists(select 1 from pg_catalog.jsonb_array_elements(query.missing_scopes) scope
            where scope->>'client_id'=membership.client_id::text
              and scope->>'contract_id'=membership.contract_id::text
              and (scope->>'week_ending')::date=membership.week_ending)
        group by generation.id,generation.manual_reminder_available_at_utc
        limit 1
      ) submission_reminder on true
      where (not v_pay_view or exists(select 1 from jsonb_array_elements(query.children) child
      where child ? 'manual_review_id' or jsonb_typeof(child->'protected_pay_seed')='object'
        or coalesce((child->>'accept_eligible')::boolean,false)))
      order by
        case when v_sort_key='candidate' and v_sort_direction='asc' then private.weekly_source_query_ascii_fold_v1(query.candidate_name) end asc,
        case when v_sort_key='candidate' and v_sort_direction='desc' then private.weekly_source_query_ascii_fold_v1(query.candidate_name) end desc,
        case when v_sort_key='client' and v_sort_direction='asc' then private.weekly_source_query_ascii_fold_v1(query.client_name) end asc,
        case when v_sort_key='client' and v_sort_direction='desc' then private.weekly_source_query_ascii_fold_v1(query.client_name) end desc,
        case when v_sort_key='issues' and v_sort_direction='asc' then query.issue_count end asc,
        case when v_sort_key='issues' and v_sort_direction='desc' then query.issue_count end desc,
        case when v_sort_key='age' and v_sort_direction='asc' then query.first_seen_at_utc end asc,
        case when v_sort_key='age' and v_sort_direction='desc' then query.first_seen_at_utc end desc,
        query.group_key
      offset v_offset limit v_limit
    ) page;
    v_next_cursor:=case when v_offset+v_limit<v_total
      then v_cursor_view_version||':'||(v_offset+v_limit)::text else '' end;
    v_queries:=pg_catalog.jsonb_build_object('rows',v_rows,'total_count',v_total,
      'next_cursor',v_next_cursor,'has_more',v_next_cursor<>'','record_version',v_workspace_version,'stale',false);
  else
    v_queries:=pg_catalog.jsonb_build_object('rows','[]'::jsonb,'total_count',v_query_count,
      'next_cursor','','has_more',false,'record_version',v_workspace_version,'stale',false);
  end if;
  v_queries:=v_queries||pg_catalog.jsonb_build_object('bulk_actions',v_bulk_actions);

  if v_publication_id is not null then
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'row_key',source_row.id,'source_row_id',source_row.id,
      'candidate',coalesce(candidate.display_name,candidate.tms_ref,'Candidate'),
      'candidate_sort',coalesce(nullif(candidate.last_name,''),candidate.display_name,candidate.tms_ref,''),
      'work_date',source_row.work_date,
      'day_date',to_char(source_row.work_date,'Dy FMDD Mon YYYY'),
      'client',coalesce(client.name,source_row.source_client_identity),
      'system_hours',case when source_row.start_at_local is null then pg_catalog.chr(8212)
        else to_char(source_row.start_at_local,'HH24:MI')||'-'||to_char(source_row.end_at_local,'HH24:MI')
          ||' ('||coalesce(source_row.break_minutes,0)||' min break)' end,
      'actual_hours',case when source_row.start_at_local is null then pg_catalog.chr(8212)
        else to_char(source_row.start_at_local,'HH24:MI')||'-'||to_char(source_row.end_at_local,'HH24:MI')
          ||' ('||coalesce(source_row.break_minutes,0)||' min break)' end,
      'movement',case charge.row_sign_kind
        when 'FULL_NEGATIVE' then 'Reversal'
        when 'POSITIVE' then 'Positive'
        else case when coalesce(source_row.actual_net_minutes,0)<0 then 'Reversal' else 'Positive' end end,
      'commission',case when source_row.source_commission_pence is null then pg_catalog.chr(8212)
        else case when source_row.source_commission_pence<0 then '-£' else '£' end
          ||to_char(pg_catalog.abs(source_row.source_commission_pence)::numeric/100,'FM9999999990.00') end,
      'total_cost',case when source_row.source_total_cost_pence is null then pg_catalog.chr(8212)
        else case when source_row.source_total_cost_pence<0 then '-£' else '£' end
          ||to_char(pg_catalog.abs(source_row.source_total_cost_pence)::numeric/100,'FM9999999990.00') end,
      'invoice_charge',case when source_row.source_shift_charge_pence is null then pg_catalog.chr(8212)
        else case when source_row.source_shift_charge_pence<0 then '-£' else '£' end
          ||to_char(pg_catalog.abs(source_row.source_shift_charge_pence)::numeric/100,'FM9999999990.00') end,
      'status',pg_catalog.jsonb_build_object('text','Ready','tone','positive'),
      'actions',case when source_row.row_finalisation_state in ('NOT_APPLICABLE','SOURCE_WORKED')
        and not exists(select 1 from private.weekly_source_manual_reviews review
          where review.source_group_id=v_cycle.source_group_id
            and review.work_event_id=resolution.work_event_id and review.state='OPEN')
        and not exists(select 1 from public.weekly_discrepancy_incidents incident
          join public.weekly_source_cycles incident_cycle on incident_cycle.id=incident.source_cycle_id
          where incident_cycle.source_group_id=v_cycle.source_group_id
            and incident.work_event_id=resolution.work_event_id and incident.state='OPEN')
        then pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
          'label','Send to Pay Queries','kind','COMMAND','command','OPEN_MANUAL_REVIEW',
          'enabled',true,'payload',pg_catalog.jsonb_build_object('source_row_id',source_row.id),
          'context',pg_catalog.jsonb_build_object('candidate',coalesce(candidate.display_name,candidate.tms_ref),
            'client',coalesce(client.name,source_row.source_client_identity),
            'status',to_char(source_row.work_date,'Dy FMDD Mon YYYY')))) else '[]'::jsonb end
    ) order by
      case when v_sort_key='candidate' and v_sort_direction='asc'
        then private.weekly_source_query_ascii_fold_v1(coalesce(candidate.display_name,candidate.tms_ref,'')) end collate "C" asc,
      case when v_sort_key='candidate' and v_sort_direction='desc'
        then private.weekly_source_query_ascii_fold_v1(coalesce(candidate.display_name,candidate.tms_ref,'')) end collate "C" desc,
      case when v_sort_key='day_date' and v_sort_direction='asc' then source_row.work_date end asc,
      case when v_sort_key='day_date' and v_sort_direction='desc' then source_row.work_date end desc,
      case when v_sort_key='client' and v_sort_direction='asc'
        then private.weekly_source_query_ascii_fold_v1(coalesce(client.name,source_row.source_client_identity,'')) end collate "C" asc,
      case when v_sort_key='client' and v_sort_direction='desc'
        then private.weekly_source_query_ascii_fold_v1(coalesce(client.name,source_row.source_client_identity,'')) end collate "C" desc,
      case when v_sort_key in ('system_hours','actual_hours') and v_sort_direction='asc' then source_row.start_at_local end asc nulls last,
      case when v_sort_key in ('system_hours','actual_hours') and v_sort_direction='desc' then source_row.start_at_local end desc nulls last,
      case when v_sort_key in ('system_hours','actual_hours') and v_sort_direction='asc' then source_row.end_at_local end asc nulls last,
      case when v_sort_key in ('system_hours','actual_hours') and v_sort_direction='desc' then source_row.end_at_local end desc nulls last,
      case when v_sort_key in ('system_hours','actual_hours') and v_sort_direction='asc' then source_row.break_minutes end asc nulls last,
      case when v_sort_key in ('system_hours','actual_hours') and v_sort_direction='desc' then source_row.break_minutes end desc nulls last,
      case when v_sort_key='movement' and v_sort_direction='asc' then
        case charge.row_sign_kind when 'FULL_NEGATIVE' then 'Reversal' when 'POSITIVE' then 'Positive'
          else case when coalesce(source_row.actual_net_minutes,0)<0 then 'Reversal' else 'Positive' end end end asc,
      case when v_sort_key='movement' and v_sort_direction='desc' then
        case charge.row_sign_kind when 'FULL_NEGATIVE' then 'Reversal' when 'POSITIVE' then 'Positive'
          else case when coalesce(source_row.actual_net_minutes,0)<0 then 'Reversal' else 'Positive' end end end desc,
      case when v_sort_key='commission' and v_sort_direction='asc' then source_row.source_commission_pence end asc nulls last,
      case when v_sort_key='commission' and v_sort_direction='desc' then source_row.source_commission_pence end desc nulls last,
      case when v_sort_key='total_cost' and v_sort_direction='asc' then source_row.source_total_cost_pence end asc nulls last,
      case when v_sort_key='total_cost' and v_sort_direction='desc' then source_row.source_total_cost_pence end desc nulls last,
      case when v_sort_key='invoice_charge' and v_sort_direction='asc' then source_row.source_shift_charge_pence end asc nulls last,
      case when v_sort_key='invoice_charge' and v_sort_direction='desc' then source_row.source_shift_charge_pence end desc nulls last,
      case when v_sort_key='status' and v_sort_direction='asc' then 'Ready' end asc,
      case when v_sort_key='status' and v_sort_direction='desc' then 'Ready' end desc,
      case when v_sort_key='job_role' and v_sort_direction='asc'
        then private.weekly_source_query_ascii_fold_v1(coalesce(source_row.role_band_source,'')) end collate "C" asc,
      case when v_sort_key='job_role' and v_sort_direction='desc'
        then private.weekly_source_query_ascii_fold_v1(coalesce(source_row.role_band_source,'')) end collate "C" desc,
      case when v_sort_key='contract' and v_sort_direction='asc' then resolution.contract_id end asc nulls last,
      case when v_sort_key='contract' and v_sort_direction='desc' then resolution.contract_id end desc nulls last,
      case when v_sort_key='outcome' and v_sort_direction='asc' then source_row.row_finalisation_state end asc,
      case when v_sort_key='outcome' and v_sort_direction='desc' then source_row.row_finalisation_state end desc,
      private.weekly_source_query_ascii_fold_v1(coalesce(candidate.display_name,candidate.tms_ref,'')) collate "C",
      source_row.work_date,source_row.start_at_local,source_row.id),'[]'::jsonb)
    into v_rows
    from public.weekly_source_upload_rows source_row
    join lateral (select resolution.* from public.weekly_source_row_resolutions resolution
      where resolution.upload_row_id=source_row.id order by resolution.generation desc,resolution.id desc limit 1) resolution
      on resolution.mapping_state='RESOLVED'
    left join public.candidates candidate on candidate.id=resolution.candidate_id
    left join public.clients client on client.id=resolution.client_id
    left join lateral (select charge.* from public.weekly_source_charge_checks charge
      where charge.upload_row_id=source_row.id order by charge.generation desc,charge.id desc limit 1) charge on true
    left join public.weekly_source_charge_acceptances charge_acceptance
      on charge_acceptance.charge_check_id=charge.id
    where source_row.upload_id=v_upload.id
      and source_row.row_finalisation_state in ('NOT_APPLICABLE','SOURCE_WORKED','SOURCE_ABSENT_ZERO')
      and coalesce(charge.phase_severity,'NONE')<>'FINALISATION_BLOCKER'
      and (charge.phase_severity is distinct from 'PROVISIONAL_WARNING' or charge_acceptance.id is not null);
    v_ready:=pg_catalog.jsonb_build_object('rows',v_rows,'total_count',pg_catalog.jsonb_array_length(v_rows),
      'next_cursor','','has_more',false,'record_version',v_workspace_version,'stale',false);

    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'row_key',source_row.id,'candidate',coalesce(candidate.display_name,
        nullif(source_row.bounded_raw_columns_json->>'worker_name',''),
        nullif(source_row.bounded_raw_columns_json->>'staff_name',''),
        nullif(source_row.bounded_raw_columns_json->>'candidate',''),source_row.source_candidate_identity),
      'source_reference',source_row.source_candidate_identity,
      'candidate_sort',coalesce(nullif(candidate.last_name,''),candidate.display_name,
        nullif(source_row.bounded_raw_columns_json->>'worker_name',''),source_row.source_candidate_identity),
      'work_date',source_row.work_date,
      'booking_reference',source_row.external_source_key,
      'client_id',resolution.client_id,
      'client',source_row.source_client_identity,
      'mapping_state',resolution.mapping_state,
      'pay_blocking',resolution.mapping_state is distinct from 'RESOLVED'
        or source_row.row_finalisation_state in
          ('SOURCE_UNFINALISED','BLOCK_FINALISATION_DISAGREEMENT','BLOCK_ACTUAL_TUPLE'),
      'day_date',to_char(source_row.work_date,'Dy FMDD Mon YYYY'),
      'system_hours',case
        when source_row.start_at_local is not null and source_row.end_at_local is not null
          then to_char(source_row.start_at_local,'HH24:MI')||pg_catalog.chr(8211)
            ||to_char(source_row.end_at_local,'HH24:MI')
            ||case when source_row.break_minutes is not null
              then ' ('||source_row.break_minutes::text||' min break)' else '' end
        when source_row.row_finalisation_state='SOURCE_UNFINALISED' then 'No confirmed hours'
        else 'Start or finish missing' end,
      'status',pg_catalog.jsonb_build_object(
        'text',case
          when resolution.mapping_state is distinct from 'RESOLVED' then 'Needs correction'
          when source_row.row_finalisation_state='SOURCE_UNFINALISED' then 'Not finalised'
          when source_row.row_finalisation_state in ('BLOCK_FINALISATION_DISAGREEMENT','BLOCK_ACTUAL_TUPLE')
            then 'Source shift needs correction'
          when charge.phase_severity='FINALISATION_BLOCKER' then 'Charge needs checking'
          when charge.phase_severity='PROVISIONAL_WARNING' and charge_acceptance.id is null then 'Charge decision needed'
          else 'Needs correction' end,
        'tone',case
          when source_row.row_finalisation_state='SOURCE_UNFINALISED' then 'warning'
          when resolution.mapping_state='RESOLVED' and charge.phase_severity='PROVISIONAL_WARNING'
            and charge_acceptance.id is null then 'warning'
          else 'danger' end),
      'problem',case
        when resolution.blocker_code='PROTECTED_SHIFT_MATCH_REQUIRED' then 'Confirm whether this is the protected shift or a separate shift.'
        when resolution.blocker_code='CANDIDATE_MAPPING_AMBIGUOUS' then 'More than one candidate matches. Choose the correct candidate.'
        when resolution.mapping_state='CANDIDATE_NOT_FOUND' then 'No active candidate matches this source row'
        when resolution.mapping_state='CLIENT_NOT_FOUND' then 'No eligible client matches this source row'
        when resolution.mapping_state in ('NO_ELIGIBLE_CONTRACT','AMBIGUOUS_CONTRACT','CONTRACT_SELECTION_REQUIRED') then 'Review the contract for this shift'
        when resolution.mapping_state is distinct from 'RESOLVED' then 'Review the source row matching'
        when source_row.row_finalisation_state='SOURCE_UNFINALISED' then 'This shift is not finalised'
        when source_row.row_finalisation_state in ('BLOCK_FINALISATION_DISAGREEMENT','BLOCK_ACTUAL_TUPLE')
          then 'Check the source shift status and hours'
        when charge.phase_severity='FINALISATION_BLOCKER' then 'Check the charge for this shift'
        when charge.phase_severity='PROVISIONAL_WARNING' and charge_acceptance.id is null then 'Accept the final NHSP source charge'
        else 'Check the hours for this shift' end,
      'issue_count',(
        (case when resolution.mapping_state is distinct from 'RESOLVED' then 1 else 0 end)
        +(case when source_row.row_finalisation_state in ('SOURCE_UNFINALISED','BLOCK_FINALISATION_DISAGREEMENT','BLOCK_ACTUAL_TUPLE') then 1 else 0 end)
        +(case when charge.phase_severity='FINALISATION_BLOCKER'
          or (charge.phase_severity='PROVISIONAL_WARNING' and charge_acceptance.id is null) then 1 else 0 end)),
      'charge_warning_key',case when charge.phase_severity='PROVISIONAL_WARNING'
        and charge_acceptance.id is null then 'charge-check:'||charge.id::text end,
      'actions',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'label',case when resolution.blocker_code='PROTECTED_SHIFT_MATCH_REQUIRED' then 'Confirm shift match'
          when resolution.mapping_state='CANDIDATE_NOT_FOUND'
          or resolution.blocker_code='CANDIDATE_MAPPING_AMBIGUOUS' then 'Link candidate'
          when resolution.mapping_state='CLIENT_NOT_FOUND' then 'Link client'
          when resolution.mapping_state in ('NO_ELIGIBLE_CONTRACT','AMBIGUOUS_CONTRACT','CONTRACT_SELECTION_REQUIRED') then 'Choose contract'
          when resolution.mapping_state='SOURCE_ROW_BLOCKED' and resolution.candidate_id is not null and resolution.client_id is not null then 'Choose contract'
          when source_row.row_finalisation_state in ('BLOCK_FINALISATION_DISAGREEMENT','BLOCK_ACTUAL_TUPLE') then 'View details'
          when charge.phase_severity in ('FINALISATION_BLOCKER','PROVISIONAL_WARNING') then 'Open charge details'
          else 'View details' end,
        'enabled',true,'payload',pg_catalog.jsonb_build_object(
          'upload_row_id',source_row.id,
          'match_choices',private.weekly_source_protected_match_candidates_v1(source_row.id,resolution.candidate_id,resolution.client_id),
          'contract_id',resolution.contract_id,
          'system_hours',to_char(source_row.start_at_local,'HH24:MI')||'–'||to_char(source_row.end_at_local,'HH24:MI')||' · '||source_row.break_minutes::text||' min break',
          'candidate',coalesce(candidate.display_name,source_row.bounded_raw_columns_json->>'worker_name',
            source_row.bounded_raw_columns_json->>'candidate',source_row.source_candidate_identity),
          'client',source_row.source_client_identity,
          'shift',to_char(source_row.work_date,'FMDD Mon YYYY'),
          'source_row_ordinal',source_row.source_row_ordinal,
          'source_role_band',source_row.role_band_source,
          'contract_seed',case when resolution.candidate_id is not null and resolution.client_id is not null
            then pg_catalog.jsonb_build_object('candidate_id',resolution.candidate_id,
              'client_id',resolution.client_id,'start_date',source_row.work_date) else '{}'::jsonb end,
          'recheck_payload',pg_catalog.jsonb_build_object('request_id',pg_catalog.gen_random_uuid(),
            'upload_id',v_upload.id,'upload_row_id',source_row.id,
            'projection_publication_id',v_publication.id,
            'expected_authority_scope_version',v_publication.authority_scope_version,
            'expected_row_manifest_hash',pg_catalog.encode(v_upload.row_manifest_hash,'hex')),
          'choices',coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
            'contract_id',contract.id,'role_band',pg_catalog.concat_ws(' / ',contract.role,contract.band),
            'site',contract.display_site,'dates',to_char(contract.start_date,'FMDD Mon YYYY')||' onwards',
            'pay_type',contract.pay_method_snapshot) order by contract.start_date,contract.id)
            from public.contracts contract where contract.candidate_id=resolution.candidate_id
              and contract.client_id=resolution.client_id
              and source_row.work_date between contract.start_date and coalesce(contract.end_date,'infinity'::date)),'[]'::jsonb),
          'detail',pg_catalog.jsonb_build_object('candidate',coalesce(candidate.display_name,
              source_row.bounded_raw_columns_json->>'worker_name',source_row.bounded_raw_columns_json->>'candidate',source_row.source_candidate_identity),
            'source_reference',source_row.source_candidate_identity,'booking_reference',source_row.external_source_key,
            'client',source_row.source_client_identity,'day_date',to_char(source_row.work_date,'FMDD Mon YYYY'),
            'contract_id',resolution.contract_id,
            'commission',case when charge.source_commission_pence is not null then '£'||to_char(charge.source_commission_pence::numeric/100,'FM9999999990.00') end,
            'total_cost',case when charge.source_total_cost_pence is not null then '£'||to_char(charge.source_total_cost_pence::numeric/100,'FM9999999990.00') end,
            'source_charge',case when charge.source_shift_charge_pence is not null then '£'||to_char(charge.source_shift_charge_pence::numeric/100,'FM9999999990.00') end,
            'calculated_charge',case when charge.calculated_segment_charge_pence is not null then '£'||to_char(charge.calculated_segment_charge_pence::numeric/100,'FM9999999990.00') end,
            'difference',case when charge.source_charge_difference_pence is not null then '£'||to_char(charge.source_charge_difference_pence::numeric/100,'FM9999999990.00') end,
            'problem',case
              when resolution.blocker_code='PROTECTED_SHIFT_MATCH_REQUIRED' then 'Confirm whether the imported shift is one of the protected shifts shown. Different hours are allowed. Choose a separate shift only if it is different work.'
              when resolution.blocker_code='CANDIDATE_MAPPING_AMBIGUOUS' then 'More than one candidate matches. Choose the correct candidate.'
              when resolution.mapping_state='CANDIDATE_NOT_FOUND' then 'No active candidate matches. Link an active candidate, or review the candidate record before rechecking.'
              when resolution.mapping_state='CLIENT_NOT_FOUND' then 'Choose the client this shift belongs to.'
              when resolution.mapping_state in ('NO_ELIGIBLE_CONTRACT','AMBIGUOUS_CONTRACT','CONTRACT_SELECTION_REQUIRED') then 'Review the candidate contract, dates, role and band, then recheck.'
              when resolution.mapping_state='SOURCE_ROW_BLOCKED' then 'Review the source details and contract rates, then recheck. The remaining hours and pricing checks have not passed yet.'
              when charge.phase_severity in ('FINALISATION_BLOCKER','PROVISIONAL_WARNING') then 'The source charge needs review. Compare it with the contract rates before proceeding.'
              when source_row.row_finalisation_state='SOURCE_UNFINALISED' then 'The source has not finalised this shift.'
              else 'Review the source hours and finalisation details.' end)
        )
      ))
    ) order by
      case when v_sort_key='candidate' and v_sort_direction='asc'
        then private.weekly_source_query_ascii_fold_v1(coalesce(candidate.display_name,source_row.source_candidate_identity,'')) end collate "C" asc,
      case when v_sort_key='candidate' and v_sort_direction='desc'
        then private.weekly_source_query_ascii_fold_v1(coalesce(candidate.display_name,source_row.source_candidate_identity,'')) end collate "C" desc,
      case when v_sort_key='day_date' and v_sort_direction='asc' then source_row.work_date end asc,
      case when v_sort_key='day_date' and v_sort_direction='desc' then source_row.work_date end desc,
      case when v_sort_key='client' and v_sort_direction='asc'
        then private.weekly_source_query_ascii_fold_v1(coalesce(source_row.source_client_identity,'')) end collate "C" asc,
      case when v_sort_key='client' and v_sort_direction='desc'
        then private.weekly_source_query_ascii_fold_v1(coalesce(source_row.source_client_identity,'')) end collate "C" desc,
      case when v_sort_key in ('system_hours','actual_hours') and v_sort_direction='asc' then source_row.start_at_local end asc nulls last,
      case when v_sort_key in ('system_hours','actual_hours') and v_sort_direction='desc' then source_row.start_at_local end desc nulls last,
      case when v_sort_key in ('system_hours','actual_hours') and v_sort_direction='asc' then source_row.end_at_local end asc nulls last,
      case when v_sort_key in ('system_hours','actual_hours') and v_sort_direction='desc' then source_row.end_at_local end desc nulls last,
      case when v_sort_key in ('system_hours','actual_hours') and v_sort_direction='asc' then source_row.break_minutes end asc nulls last,
      case when v_sort_key in ('system_hours','actual_hours') and v_sort_direction='desc' then source_row.break_minutes end desc nulls last,
      case when v_sort_key='movement' and v_sort_direction='asc'
        then case when coalesce(source_row.actual_net_minutes,0)<0 then 'Reversal' else 'Positive' end end asc,
      case when v_sort_key='movement' and v_sort_direction='desc'
        then case when coalesce(source_row.actual_net_minutes,0)<0 then 'Reversal' else 'Positive' end end desc,
      case when v_sort_key='commission' and v_sort_direction='asc' then source_row.source_commission_pence end asc nulls last,
      case when v_sort_key='commission' and v_sort_direction='desc' then source_row.source_commission_pence end desc nulls last,
      case when v_sort_key='total_cost' and v_sort_direction='asc' then source_row.source_total_cost_pence end asc nulls last,
      case when v_sort_key='total_cost' and v_sort_direction='desc' then source_row.source_total_cost_pence end desc nulls last,
      case when v_sort_key='invoice_charge' and v_sort_direction='asc' then source_row.source_shift_charge_pence end asc nulls last,
      case when v_sort_key='invoice_charge' and v_sort_direction='desc' then source_row.source_shift_charge_pence end desc nulls last,
      case when v_sort_key='status' and v_sort_direction='asc' then case
        when resolution.mapping_state is distinct from 'RESOLVED' then 'Needs correction'
        when source_row.row_finalisation_state='SOURCE_UNFINALISED' then 'Not finalised'
        when charge.phase_severity='FINALISATION_BLOCKER' then 'Charge needs checking'
        else 'Needs correction' end end asc,
      case when v_sort_key='status' and v_sort_direction='desc' then case
        when resolution.mapping_state is distinct from 'RESOLVED' then 'Needs correction'
        when source_row.row_finalisation_state='SOURCE_UNFINALISED' then 'Not finalised'
        when charge.phase_severity='FINALISATION_BLOCKER' then 'Charge needs checking'
        else 'Needs correction' end end desc,
      case when v_sort_key='problem' and v_sort_direction='asc' then case
        when resolution.mapping_state is distinct from 'RESOLVED' then 'Link this row before finalising'
        when source_row.row_finalisation_state='SOURCE_UNFINALISED' then 'This shift is not finalised'
        when charge.phase_severity='FINALISATION_BLOCKER' then 'Check the charge for this shift'
        else 'Check the hours for this shift' end end asc,
      case when v_sort_key='problem' and v_sort_direction='desc' then case
        when resolution.mapping_state is distinct from 'RESOLVED' then 'Link this row before finalising'
        when source_row.row_finalisation_state='SOURCE_UNFINALISED' then 'This shift is not finalised'
        when charge.phase_severity='FINALISATION_BLOCKER' then 'Check the charge for this shift'
        else 'Check the hours for this shift' end end desc,
      case when v_sort_key='job_role' and v_sort_direction='asc'
        then private.weekly_source_query_ascii_fold_v1(coalesce(source_row.role_band_source,'')) end collate "C" asc,
      case when v_sort_key='job_role' and v_sort_direction='desc'
        then private.weekly_source_query_ascii_fold_v1(coalesce(source_row.role_band_source,'')) end collate "C" desc,
      case when v_sort_key='contract' and v_sort_direction='asc' then resolution.contract_id end asc nulls last,
      case when v_sort_key='contract' and v_sort_direction='desc' then resolution.contract_id end desc nulls last,
      case when v_sort_key='outcome' and v_sort_direction='asc' then source_row.row_finalisation_state end asc,
      case when v_sort_key='outcome' and v_sort_direction='desc' then source_row.row_finalisation_state end desc,
      source_row.work_date,private.weekly_source_query_ascii_fold_v1(source_row.source_candidate_identity) collate "C",source_row.id),'[]'::jsonb)
    into v_rows
    from public.weekly_source_upload_rows source_row
    left join lateral (select latest.* from public.weekly_source_row_resolutions latest
      where latest.upload_row_id=source_row.id order by latest.generation desc,latest.id desc limit 1) resolution on true
    left join public.candidates candidate on candidate.id=resolution.candidate_id
    left join lateral (select latest.* from public.weekly_source_charge_checks latest
      where latest.upload_row_id=source_row.id order by latest.generation desc,latest.id desc limit 1) charge on true
    left join public.weekly_source_charge_acceptances charge_acceptance
      on charge_acceptance.charge_check_id=charge.id
    where source_row.upload_id=v_upload.id
      and (resolution.mapping_state is distinct from 'RESOLVED'
        or source_row.row_finalisation_state in ('SOURCE_UNFINALISED','BLOCK_FINALISATION_DISAGREEMENT','BLOCK_ACTUAL_TUPLE')
        or charge.phase_severity='FINALISATION_BLOCKER'
        or (charge.phase_severity='PROVISIONAL_WARNING' and charge_acceptance.id is null));
    v_blocked:=pg_catalog.jsonb_build_object('rows',v_rows,'total_count',pg_catalog.jsonb_array_length(v_rows),
      'next_cursor','','has_more',false,'record_version',v_workspace_version,'stale',false);
    v_import_prepared:=private.weekly_source_import_is_prepared_v1(v_upload.id);
    v_office_checks:=v_blocked;
    select coalesce(pg_catalog.jsonb_agg(row_data),'[]'::jsonb) into v_excluded_rows
    from pg_catalog.jsonb_array_elements(v_rows) row_data where row_data->'status'->>'text'='Not finalised';
    select coalesce(pg_catalog.jsonb_agg(row_data),'[]'::jsonb) into v_rows
    from pg_catalog.jsonb_array_elements(v_rows) row_data
    where row_data->'status'->>'text'<>'Not finalised';
    v_blocked:=v_blocked||pg_catalog.jsonb_build_object('rows',v_rows,'total_count',pg_catalog.jsonb_array_length(v_rows));
    select pg_catalog.count(*)::integer into v_unfinalised_count
    from public.weekly_source_upload_rows where upload_id=v_upload.id and row_finalisation_state='SOURCE_UNFINALISED';
    v_finalise_enabled:=v_import_prepared and v_blocker_count=0
      and (v_profile.profile_code<>'NHSP_FINAL_BACKING_V1' or v_rate_warning_unaccepted_count=0)
      and (v_profile.profile_code in ('HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1','HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1')
        or pg_catalog.statement_timestamp()>=coalesce(
        (select scope.cutoff_at_utc from public.weekly_source_report_scopes scope
          where scope.id=v_report_scope_id),
        v_cycle.cutoff_at_utc
      ))
      and v_cycle.state not in ('FINALISING','FINALISED');
    v_finalise_payload:=pg_catalog.jsonb_build_object(
      'source_cycle_id',v_cycle.id,'authority_scope_kind',v_publication.authority_scope_kind,
      'report_scope_id',v_publication.report_scope_id,'upload_id',v_upload.id,
      'projection_publication_id',v_publication.id,
      'expected_authority_scope_version',v_publication.authority_scope_version,
      'expected_row_manifest_hash',pg_catalog.encode(v_upload.row_manifest_hash,'hex'),
      'expected_comparison_manifest_hash',pg_catalog.encode(v_publication.comparison_manifest_hash,'hex'),
      'expected_issue_set_hash',pg_catalog.encode(v_publication.issue_set_hash,'hex')
    );

    select pg_catalog.jsonb_build_object(
      'state',run_row.state,
      'title',case run_row.state
        when 'ACTION_REQUIRED' then 'Approved hours need attention'
        when 'RECOVERY_REQUIRED' then 'Approved hours update needs checking'
        else 'Approved hours update is not complete' end,
      'body',case run_row.state
        when 'ACTION_REQUIRED' then run_row.action_required_task_count::text
          ||case when run_row.action_required_task_count=1 then ' timesheet needs' else ' timesheets need' end
          ||' review. The source is finalised and invoicing is not delayed.'
        when 'RECOVERY_REQUIRED' then 'The source is finalised. Check the approved-hours update before trying it again.'
        else 'The source is finalised. Continue the approved-hours update.' end,
      'action',case
        when task.state='READY' then pg_catalog.jsonb_build_object(
          'label','Continue approved hours update','command','FINALISE_WEEK',
          'payload',v_finalise_payload
        )
        when task.state in ('SUBMISSION_STARTED','RECOVERY_REQUIRED') then pg_catalog.jsonb_build_object(
          'label','Check approved hours update','command','RECOVER_FINALISED_PAY',
          'payload',pg_catalog.jsonb_build_object(
            'final_revision_id',run_row.final_revision_id,'run_id',run_row.id,
            'task_id',task.id,'expected_task_version',task.version,'confirm_retry',false
          )
        )
        else null end
    ) into v_finalise_pay_follow_up
    from public.weekly_source_final_revisions revision
    join public.weekly_source_finalisation_pay_runs run_row
      on run_row.final_revision_id=revision.id and run_row.state<>'COMPLETE'
    left join lateral (
      select pending.*
      from public.weekly_source_finalisation_pay_tasks pending
      where pending.run_id=run_row.id
        and pending.state in ('READY','SUBMISSION_STARTED','RECOVERY_REQUIRED')
      order by case pending.state
        when 'SUBMISSION_STARTED' then 1 when 'RECOVERY_REQUIRED' then 2 else 3 end,
        pending.task_ordinal,pending.id
      limit 1
    ) task on true
    where revision.source_cycle_id=v_cycle.id and revision.state='CURRENT'
      and revision.authority_scope_kind=v_publication.authority_scope_kind
      and revision.report_scope_id is not distinct from v_publication.report_scope_id
    order by revision.revision_number desc,revision.id desc
    limit 1;
  else
    v_ready:=pg_catalog.jsonb_build_object('rows','[]'::jsonb,'total_count',0,'next_cursor','','has_more',false,'record_version',v_workspace_version,'stale',false);
    v_blocked:=v_ready;
  end if;

  v_complete:=case when v_authority_mode='SOURCE_AUTHORITY'
    then private.weekly_source_completed_rows_v1(v_cycle.id,v_client_id,v_report_scope_id)
    else jsonb_build_object('rows','[]'::jsonb,'total_count',0,'next_cursor','','has_more',false,'stale',false) end;
  if v_authority_mode='SOURCE_AUTHORITY' and exists(select 1 from public.weekly_source_final_revisions completed
    where completed.source_cycle_id=v_cycle.id and completed.state='CURRENT'
      and completed.upload_id=v_upload.id
      and completed.report_scope_id is not distinct from v_report_scope_id) then
    v_ready:=pg_catalog.jsonb_build_object('rows','[]'::jsonb,'total_count',0,
      'next_cursor','','has_more',false,'record_version',v_workspace_version,'stale',false);
    v_blocked:=v_ready;
    v_blocker_count:=0;
    v_finalise_enabled:=false;
    v_finalise_payload:='{}'::jsonb;
  end if;
  v_queries:=v_queries||pg_catalog.jsonb_build_object('office_checks',v_office_checks);
  if not v_import_prepared then
    if v_publication_id is not null and v_upload.state='CURRENT' and v_upload.purpose='ORDINARY'
      and v_cycle.state not in ('FINALISING','FINALISED') and v_upload.coverage_state='COMPLETE'
      and v_profile.profile_code in ('HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',
        'HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1','ROSTER_WEEKLY_SUMMARY_ACTUAL_V1') then
      v_prepare_action:=pg_catalog.jsonb_build_object('label','Prepare for finalisation',
        'command','PREPARE_FINALISATION','payload',pg_catalog.jsonb_build_object(
          'upload_id',v_upload.id,'projection_publication_id',v_publication.id,
          'expected_authority_scope_version',v_publication.authority_scope_version,
          'expected_row_manifest_hash',pg_catalog.encode(v_upload.row_manifest_hash,'hex')));
    end if;
    v_ready:=pg_catalog.jsonb_build_object('rows','[]'::jsonb,'total_count',0,
      'next_cursor','','has_more',false,'record_version',v_workspace_version,'stale',false);
    v_blocked:=v_ready;
    v_finalise_enabled:=false;
    v_finalise_payload:='{}'::jsonb;
  end if;

  select pg_catalog.jsonb_build_object(
    'title','Finalisation progress','cycle_label','Week ending '||to_char(v_cycle.finalisation_week_ending,'FMDD Mon YYYY'),
    'cycle_id',v_cycle.id,
    'cycle_options',coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'value',item.id,'label','Week ending '||to_char(item.finalisation_week_ending,'FMDD Mon YYYY'))
      order by item.finalisation_week_ending desc,item.id)
      from public.weekly_source_cycles item where item.source_group_id=v_group.id),'[]'::jsonb),
    'complete',not exists(
      select 1 from public.weekly_source_group_clients membership
      where membership.source_group_id=v_group.id
        and (v_cycle.scope_client_id is null or membership.client_id=v_cycle.scope_client_id)
        and v_cycle.finalisation_week_ending between membership.valid_from and coalesce(membership.valid_to,'infinity'::date)
        and not exists(select 1 from public.weekly_source_client_cycle_completions completion
          where completion.source_cycle_id=v_cycle.id and completion.client_id=membership.client_id and completion.state='CURRENT')
    ),
    'rows',coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'row_key','tracker-'||membership.client_id::text,'source',v_group.display_name,'client',client.name,
      'status',pg_catalog.jsonb_build_object(
        'text',case when completion.id is not null then case completion.completion_kind
          when 'FINAL_SOURCE' then 'Finalised' else 'No shifts this week' end else 'Not finalised' end,
        'tone',case when completion.id is not null then 'positive' else 'warning' end),
      'detail',case when completion.id is not null then to_char(completion.attested_at_utc at time zone 'Europe/London','DD Mon YYYY HH24:MI') else '' end,
      'actions',case
        when completion.id is null
         and v_cycle.state not in ('FINALISING','FINALISED')
         and not exists(select 1 from public.weekly_source_final_revisions revision
           join public.weekly_source_final_snapshot_lines snapshot on snapshot.final_revision_id=revision.id
           where revision.source_cycle_id=v_cycle.id and revision.state in ('PREPARED','CURRENT')
             and snapshot.client_id=membership.client_id)
         and not exists(select 1 from public.weekly_source_uploads upload
           join public.weekly_source_upload_rows source_row on source_row.upload_id=upload.id
           left join lateral (select resolution.* from public.weekly_source_row_resolutions resolution
             where resolution.upload_row_id=source_row.id order by resolution.generation desc,resolution.id desc limit 1) resolution on true
           where upload.source_cycle_id=v_cycle.id
             and (upload.id=v_cycle.current_complete_upload_id or upload.state='CURRENT')
             and resolution.mapping_state is distinct from 'RESOLVED')
         and not exists(select 1 from public.weekly_source_report_scopes scope
           where scope.source_cycle_id=v_cycle.id and scope.client_id=membership.client_id
             and (scope.current_complete_upload_id is not null or scope.current_projection_publication_id is not null or scope.current_final_revision_id is not null))
         and not exists(select 1 from public.weekly_source_upload_rows source_row
           join public.weekly_source_uploads upload on upload.id=source_row.upload_id and upload.source_cycle_id=v_cycle.id and upload.state='CURRENT'
           join lateral (select resolution.* from public.weekly_source_row_resolutions resolution
             where resolution.upload_row_id=source_row.id order by resolution.generation desc,resolution.id desc limit 1) resolution
             on resolution.mapping_state='RESOLVED' and resolution.client_id=membership.client_id)
         and not exists(select 1 from public.weekly_discrepancy_incidents incident
           where incident.source_cycle_id=v_cycle.id and incident.client_id=membership.client_id and incident.state='OPEN')
        then pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
          'label','No shifts to import','enabled',true,'kind','COMMAND','command','NO_SHIFTS_TO_IMPORT',
          'context',pg_catalog.jsonb_build_object(
            'trust',client.name,
            'cutoff',to_char(v_cycle.cutoff_at_utc at time zone 'Europe/London','Dy FMDD Mon YYYY HH24:MI')
          ),
          'payload',pg_catalog.jsonb_build_object(
            'actor_user_id',v_actor,
            'source_cycle_id',v_cycle.id,'source_group_id',v_group.id,
            'client_id',membership.client_id,'expected_cycle_version',v_cycle.version,
            'attestation_text','No shifts to import'
          )))
        else pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
          'label','View','enabled',true,'payload',pg_catalog.jsonb_build_object('client_id',membership.client_id)
        )) end
    ) order by private.weekly_source_query_ascii_fold_v1(client.name) collate "C",client.id)
    from public.weekly_source_group_clients membership
    join public.clients client on client.id=membership.client_id
    left join public.weekly_source_client_cycle_completions completion
      on completion.source_cycle_id=v_cycle.id and completion.client_id=membership.client_id and completion.state='CURRENT'
    where membership.source_group_id=v_group.id
      and (v_cycle.scope_client_id is null or membership.client_id=v_cycle.scope_client_id)
      and v_cycle.finalisation_week_ending between membership.valid_from and coalesce(membership.valid_to,'infinity'::date)),'[]'::jsonb)
  ) into v_tracker;

  if v_tab='history' then
    with ranked_cycles as (
      select item.id,pg_catalog.row_number() over(
        order by item.finalisation_week_ending desc,item.id desc
      ) cycle_ordinal
      from public.weekly_source_cycles item
      where item.source_group_id=v_group.id
        and item.finalisation_week_ending<=v_cycle.finalisation_week_ending
    ), selected_cycles as (
      select id from ranked_cycles
      where (v_cycle_filter='CURRENT_PAY_CYCLE' and id=v_cycle.id)
         or (v_cycle_filter='LAST_4_PAY_CYCLES' and cycle_ordinal<=4)
         or (v_cycle_filter='LAST_13_PAY_CYCLES' and cycle_ordinal<=13)
    ), events as (
      select completion.attested_at_utc when_utc,completion.id::text row_key,
        v_group.display_name source_label,
        case completion.completion_kind when 'FINAL_SOURCE' then 'Finalised' else 'No shifts to import' end event_label,
        coalesce(office_user.display_name,office_user.email,'Office') by_label,
        client.name detail_label
      from public.weekly_source_client_cycle_completions completion
      join selected_cycles selected on selected.id=completion.source_cycle_id
      join public.clients client on client.id=completion.client_id
      left join public.tms_users office_user on office_user.id=completion.attested_by_user_id
      union all
      select upload.uploaded_at_utc,upload.id::text,v_group.display_name,'Source uploaded',
        coalesce(office_user.display_name,office_user.email,'Office'),upload.original_filename
      from public.weekly_source_uploads upload
      join selected_cycles selected on selected.id=upload.source_cycle_id
      left join public.tms_users office_user on office_user.id=upload.uploaded_by_user_id
    )
    select pg_catalog.count(*)::integer into v_total from events;

    with ranked_cycles as (
      select item.id,pg_catalog.row_number() over(
        order by item.finalisation_week_ending desc,item.id desc
      ) cycle_ordinal
      from public.weekly_source_cycles item
      where item.source_group_id=v_group.id
        and item.finalisation_week_ending<=v_cycle.finalisation_week_ending
    ), selected_cycles as (
      select id from ranked_cycles
      where (v_cycle_filter='CURRENT_PAY_CYCLE' and id=v_cycle.id)
         or (v_cycle_filter='LAST_4_PAY_CYCLES' and cycle_ordinal<=4)
         or (v_cycle_filter='LAST_13_PAY_CYCLES' and cycle_ordinal<=13)
    ), events as (
      select completion.attested_at_utc when_utc,completion.id::text row_key,
        v_group.display_name source_label,
        case completion.completion_kind when 'FINAL_SOURCE' then 'Finalised' else 'No shifts to import' end event_label,
        coalesce(office_user.display_name,office_user.email,'Office') by_label,
        client.name detail_label
      from public.weekly_source_client_cycle_completions completion
      join selected_cycles selected on selected.id=completion.source_cycle_id
      join public.clients client on client.id=completion.client_id
      left join public.tms_users office_user on office_user.id=completion.attested_by_user_id
      union all
      select upload.uploaded_at_utc,upload.id::text,v_group.display_name,'Source uploaded',
        coalesce(office_user.display_name,office_user.email,'Office'),upload.original_filename
      from public.weekly_source_uploads upload
      join selected_cycles selected on selected.id=upload.source_cycle_id
      left join public.tms_users office_user on office_user.id=upload.uploaded_by_user_id
    ), page as (
      select pg_catalog.jsonb_build_object(
          'row_key',event.row_key,'when',to_char(event.when_utc at time zone 'Europe/London','DD Mon YYYY HH24:MI'),
          'source',event.source_label,'event',event.event_label,'by',event.by_label,'detail',event.detail_label
        ) row_json,
        pg_catalog.row_number() over(order by
          case when v_sort_key='when' and v_sort_direction='asc' then event.when_utc end asc,
          case when v_sort_key='when' and v_sort_direction='desc' then event.when_utc end desc,
          case when v_sort_key='source' and v_sort_direction='asc' then private.weekly_source_query_ascii_fold_v1(event.source_label) end asc,
          case when v_sort_key='source' and v_sort_direction='desc' then private.weekly_source_query_ascii_fold_v1(event.source_label) end desc,
          case when v_sort_key='event' and v_sort_direction='asc' then private.weekly_source_query_ascii_fold_v1(event.event_label) end asc,
          case when v_sort_key='event' and v_sort_direction='desc' then private.weekly_source_query_ascii_fold_v1(event.event_label) end desc,
          case when v_sort_key='by' and v_sort_direction='asc' then private.weekly_source_query_ascii_fold_v1(event.by_label) end asc,
          case when v_sort_key='by' and v_sort_direction='desc' then private.weekly_source_query_ascii_fold_v1(event.by_label) end desc,
          case when v_sort_key='detail' and v_sort_direction='asc' then private.weekly_source_query_ascii_fold_v1(event.detail_label) end asc,
          case when v_sort_key='detail' and v_sort_direction='desc' then private.weekly_source_query_ascii_fold_v1(event.detail_label) end desc,
          case when v_sort_direction='asc' then event.when_utc end asc,
          case when v_sort_direction='desc' then event.when_utc end desc,
          event.row_key
        ) position
      from events event
      order by
        case when v_sort_key='when' and v_sort_direction='asc' then event.when_utc end asc,
        case when v_sort_key='when' and v_sort_direction='desc' then event.when_utc end desc,
        case when v_sort_key='source' and v_sort_direction='asc' then private.weekly_source_query_ascii_fold_v1(event.source_label) end asc,
        case when v_sort_key='source' and v_sort_direction='desc' then private.weekly_source_query_ascii_fold_v1(event.source_label) end desc,
        case when v_sort_key='event' and v_sort_direction='asc' then private.weekly_source_query_ascii_fold_v1(event.event_label) end asc,
        case when v_sort_key='event' and v_sort_direction='desc' then private.weekly_source_query_ascii_fold_v1(event.event_label) end desc,
        case when v_sort_key='by' and v_sort_direction='asc' then private.weekly_source_query_ascii_fold_v1(event.by_label) end asc,
        case when v_sort_key='by' and v_sort_direction='desc' then private.weekly_source_query_ascii_fold_v1(event.by_label) end desc,
        case when v_sort_key='detail' and v_sort_direction='asc' then private.weekly_source_query_ascii_fold_v1(event.detail_label) end asc,
        case when v_sort_key='detail' and v_sort_direction='desc' then private.weekly_source_query_ascii_fold_v1(event.detail_label) end desc,
        case when v_sort_direction='asc' then event.when_utc end asc,
        case when v_sort_direction='desc' then event.when_utc end desc,
        event.row_key
      offset v_offset limit v_limit
    )
    select coalesce(pg_catalog.jsonb_agg(page.row_json order by page.position),'[]'::jsonb)
    into v_rows from page;
    v_next_cursor:=case when v_offset+v_limit<v_total
      then v_cursor_view_version||':'||(v_offset+v_limit)::text else '' end;
  else
    v_rows:='[]'::jsonb;
    v_total:=0;
    v_next_cursor:='';
  end if;
  v_history:=pg_catalog.jsonb_build_object('rows',v_rows,
    'total_count',v_total,'next_cursor',v_next_cursor,'has_more',v_next_cursor<>'',
    'record_version',v_workspace_version,'stale',false,'cycle_filter',v_cycle_filter,
    'cycle_options',pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object('value','CURRENT_PAY_CYCLE','label','Current pay cycle'),
      pg_catalog.jsonb_build_object('value','LAST_4_PAY_CYCLES','label','Last 4 pay cycles'),
      pg_catalog.jsonb_build_object('value','LAST_13_PAY_CYCLES','label','Last 13 pay cycles')
    ));

  if v_client_id is not null then
    select client.name into v_context_client_name from public.clients client where client.id=v_client_id;
  end if;
  v_context_cutoff:=v_cycle.cutoff_at_utc;
  if v_report_scope_id is not null then
    select scope.cutoff_at_utc into v_context_cutoff
    from public.weekly_source_report_scopes scope where scope.id=v_report_scope_id;
  end if;
  if v_group.source_family='NHSP' then
    v_nhsp_report_number:=nullif(pg_catalog.btrim(v_upload.file_metadata_json->>'nhsp_report_number'),'');
    select pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object('key','source_group','label','Source','value',v_group.id,
        'options',coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
          'value',item.id,'label',case when item.source_family='NHSP' then 'NHSP' else item.display_name end)
          order by private.weekly_source_query_ascii_fold_v1(item.display_name) collate "C",item.id)
          from public.weekly_source_groups item where item.active),'[]'::jsonb)),
      pg_catalog.jsonb_build_object('key','client','label','Trust','value',coalesce(v_client_id::text,''),
        'options',coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('value',client.id,'label',client.name)
          order by private.weekly_source_query_ascii_fold_v1(client.name) collate "C",client.id)
          from public.weekly_source_group_clients membership join public.clients client on client.id=membership.client_id
          where membership.source_group_id=v_group.id and v_cycle.finalisation_week_ending between membership.valid_from and coalesce(membership.valid_to,'infinity'::date)),'[]'::jsonb)),
      pg_catalog.jsonb_build_object('key','report','label','Report number','value',coalesce(v_nhsp_report_number,'Not confirmed'),'options','[]'::jsonb),
      pg_catalog.jsonb_build_object('key','cutoff','label','Cutoff','value',to_char(v_context_cutoff at time zone 'Europe/London','DD/MM/YYYY HH24:MI'),'options','[]'::jsonb)
    ) into v_controls;
  else
    select pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object('key','journey','label','Journey',
        'value',case when v_authority_mode='TIMESHEET_AUTHORITY'
          then 'Signed Timesheet decides hours' else 'Client system decides hours' end,
        'options','[]'::jsonb),
      pg_catalog.jsonb_build_object('key','source_group','label','Source','value',v_group.id,
        'options',coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('value',item.id,'label',item.display_name)
          order by private.weekly_source_query_ascii_fold_v1(item.display_name) collate "C",item.id)
          from public.weekly_source_groups item where item.active),'[]'::jsonb)),
      pg_catalog.jsonb_build_object('key','cycle','label','Week','value',v_cycle.id,
        'options',coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('value',item.id,'label','Week ending '||to_char(item.finalisation_week_ending,'FMDD Mon YYYY'),
          'scope_client_id',item.scope_client_id,'finalisation_week_ending',item.finalisation_week_ending)
          order by item.finalisation_week_ending desc,item.id)
          from public.weekly_source_cycles item where item.source_group_id=v_group.id),'[]'::jsonb)),
      pg_catalog.jsonb_build_object('key','client','label','Client','value',coalesce(v_client_id::text,''),
        'options',coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('value',client.id,'label',client.name)
          order by private.weekly_source_query_ascii_fold_v1(client.name) collate "C",client.id)
          from public.weekly_source_group_clients membership join public.clients client on client.id=membership.client_id
          where membership.source_group_id=v_group.id and v_cycle.finalisation_week_ending between membership.valid_from and coalesce(membership.valid_to,'infinity'::date)),'[]'::jsonb))
    ) into v_controls;
  end if;

  v_cycle_label:='Week ending '||to_char(v_cycle.finalisation_week_ending,'FMDD Mon YYYY');
  v_cycle_state_label:=case v_cycle.state when 'OPEN' then 'Before cutoff'
    when 'FINALISABLE' then 'Ready for finalisation' when 'FINALISED' then 'Finalised'
    when 'CORRECTION_IN_PROGRESS' then 'Correction in progress' else 'Ready for finalisation' end;
  v_cycle_state_tone:=case when v_cycle.state='FINALISED' then 'positive'
    when v_cycle.state='CORRECTION_IN_PROGRESS' then 'danger' else 'warning' end;

  if v_publication.state='CURRENT' and v_upload.purpose='ORDINARY'
    and v_cycle.state not in ('FINALISING','FINALISED') then
    v_recheck_action:=pg_catalog.jsonb_build_object('request_id',pg_catalog.gen_random_uuid(),
      'upload_id',v_upload.id,'projection_publication_id',v_publication.id,
      'expected_authority_scope_version',v_publication.authority_scope_version,
      'expected_row_manifest_hash',pg_catalog.encode(v_upload.row_manifest_hash,'hex'));
  else
    select saved.request_json-'actor_user_id' into v_recheck_action
      from private.weekly_source_office_rechecks saved
      join public.weekly_source_projection_publications pending on pending.id=saved.publication_id
      join public.weekly_source_uploads upload on upload.id=saved.upload_id
      where saved.actor_user_id=v_actor and pending.source_cycle_id=v_cycle.id
        and pending.state='BUILDING' and upload.state='CURRENT'
        and pending.report_scope_id is not distinct from v_report_scope_id
      order by saved.created_at_utc desc,saved.request_id desc limit 1;
  end if;
  -- Reduce the existing Hours-question projection, preserving each owner's
  -- exact pay proofs. No contact, upload, settings or invoice command survives.
  if v_pay_view then
    v_pay_rows:='[]'::jsonb;
    for v_pay_row in select value from jsonb_array_elements(coalesce(v_queries->'rows','[]'::jsonb))
    loop
      v_pay_children:='[]'::jsonb;
      for v_pay_child in select value from jsonb_array_elements(coalesce(v_pay_row->'children','[]'::jsonb))
      loop
        if v_pay_child ? 'manual_review_id' or jsonb_typeof(v_pay_child->'protected_pay_seed')='object'
          or coalesce((v_pay_child->>'accept_eligible')::boolean,false) then
          v_pay_children:=v_pay_children||jsonb_build_array(v_pay_child||jsonb_build_object('actions',
            coalesce((select jsonb_agg(action.value order by action.ordinality)
              from jsonb_array_elements(coalesce(v_pay_child->'actions','[]'::jsonb))
                with ordinality action(value,ordinality)
              where action.value->>'command'='RESOLVE_MANUAL_REVIEW'
                or (not action.value ? 'command' and (
                  action.value->'payload' ? 'detail'
                  or action.value->'payload' ?& array['source_group_id','candidate_id','work_date']))),'[]'::jsonb)));
        end if;
      end loop;
      if jsonb_array_length(v_pay_children)>0 then
        v_pay_rows:=v_pay_rows||jsonb_build_array(
          (v_pay_row-array['actions','contact_policy','outreach_eligible','manager_eligible'])
          ||jsonb_build_object('children',v_pay_children));
      end if;
    end loop;
    v_queries:=(v_queries-array['office_checks','bulk_actions','rows'])||jsonb_build_object(
      'rows',v_pay_rows,'bulk_actions','{}'::jsonb);
  end if;
  v_result:=pg_catalog.jsonb_build_object(
    'contract','WEEKLY_SOURCE_IMPORT_WORKSPACE_V1','workspace_version',v_workspace_version,
    'combined_source_workspace',v_authority_mode='SOURCE_AUTHORITY' and exists(
      select 1 from public.weekly_source_client_policies policy where policy.source_group_id=v_group.id
        and policy.authority_mode='SOURCE_AUTHORITY'
        and (v_client_id is null or policy.client_id=v_client_id)
        and v_cycle.finalisation_week_ending between policy.effective_from and coalesce(policy.effective_to,'infinity'::date)),
    'recheck_payload',coalesce(v_recheck_action,'{}'::jsonb),
    'profile',pg_catalog.jsonb_build_object(
      'id',coalesce(v_profile.profile_code,case when v_group.source_family='NHSP' then 'NHSP_FINAL_BACKING_V1' else 'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1' end),
      'label',case when v_group.source_family='NHSP' then 'NHSP' else v_group.display_name end,
      'finalise_label',case when v_group.source_family='NHSP' then 'Finalise report' else 'Finalise source' end),
    'context',pg_catalog.jsonb_build_object('subtitle',case when v_group.source_family='NHSP' then
        'NHSP'||case when v_context_client_name is null then '' else ' · '||v_context_client_name end
      else v_cycle_label end,'cycle_state',v_cycle_state_label,
      'cycle_tone',v_cycle_state_tone,'controls',v_controls),
    'selected',pg_catalog.jsonb_build_object('source_group_id',v_group.id,'source_cycle_id',v_cycle.id,
      'client_id',v_client_id,'report_scope_id',v_report_scope_id,'projection_publication_id',v_publication_id),
    'counts',pg_catalog.jsonb_build_object('queries',v_query_count,'blockers',v_blocker_count,
      'paid_unresolved',v_paid_unresolved_count),
    'imports',v_imports,'queries',v_queries||pg_catalog.jsonb_build_object(
      'protected_shifts',private.weekly_source_protected_query_rows_v1(v_group.id,v_client_id,
        private.weekly_source_pay_actor_allowed_v1(v_actor)),
      'protected_pay_enabled',private.weekly_source_pay_actor_allowed_v1(v_actor)
        and exists(select 1 from public.weekly_source_client_policies policy
          where policy.source_group_id=v_group.id and policy.authority_mode='SOURCE_AUTHORITY'
            and (v_client_id is null or policy.client_id=v_client_id)
            and v_cycle.finalisation_week_ending between policy.effective_from and coalesce(policy.effective_to,'infinity'::date))),
    'finalise',pg_catalog.jsonb_build_object(
      'prepared',v_import_prepared,'prepare_action',v_prepare_action,
      'unfinalised_count',v_unfinalised_count,
      'excluded_rows',v_excluded_rows,
      'exclusion_confirmation',case when v_unfinalised_count>0 and v_profile.profile_code in
        ('HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1','HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1')
        then 'I confirm that non-finalised shifts are excluded. Previously finalised shifts now missing or non-finalised within this file period may be reversed.' else null end,
      'ready',v_ready,'blocked',v_blocked,'complete',v_complete,
      'active_list',case when v_blocker_count>0 then 'blocked'
        when (v_ready->>'total_count')::integer=0 and (v_complete->>'total_count')::integer>0 then 'complete' else 'ready' end,
      'confirmation_text','I confirm this is the final source for this week.',
      'confirmation_required',true,'finalise_enabled',v_finalise_enabled,
      'finalise_payload',v_finalise_payload,
      'rate_warnings',v_rate_warnings,
      'approved_hours_follow_up',v_finalise_pay_follow_up,
      'source_summary',case when v_publication_id is null then 'Upload a source file to continue.'
        else v_upload.original_filename end,'tracker',v_tracker),
    'history',v_history,'notices','[]'::jsonb,
    -- Gate 9 G9-1, workspace half.  The workspace serves the complete
    -- server-owned lifecycle vocabulary -- all 22 rows, with their exact
    -- headings, statuses, permitted new actions and forbidden inferences -- so
    -- that no Office surface ever hard-codes a lifecycle heading and no browser
    -- has to map a state to one.  The PER-TIMESHEET phase is not computed here:
    -- it is one read per Timesheet from the named owner below, which is what
    -- the Simple Timesheet and the Bulk right pane already call.
    'lifecycle_policy',private.weekly_source_office_lifecycle_policy_v1()
      ||pg_catalog.jsonb_build_object(
        'per_timesheet_owner','public.weekly_source_office_timesheet_presentation_v1',
        'per_timesheet_member','lifecycle',
        'browser_may_infer_phase',false)
  );
  if v_pay_view then
    v_result:=v_result-array['imports','finalise','history','recheck_payload','lifecycle_policy'];
    v_result:=jsonb_set(v_result,'{context,controls}','{}'::jsonb,false)
      ||jsonb_build_object('permissions',jsonb_build_object(
        'contract','WEEKLY_SOURCE_PAYMENT_VIEW_V1','view','PAY',
        'allowed_tabs',jsonb_build_array('queries'),
        'allowed_sections',jsonb_build_array('questions','checks','protected'),
        'landing_tab','queries','can_upload',false,'can_finalise',false,'can_manage_settings',false,
        'checklist',jsonb_build_object('available',false,'read_only',true,'pay_only',true,'action_links',false)));
  end if;
  return v_result;
exception
  when invalid_text_representation or numeric_value_out_of_range or datetime_field_overflow then
    raise exception 'WEEKLY_SOURCE_WORKSPACE_REQUEST_INVALID' using errcode='22023';
end;
$function$;


-- Current TEST source: supabase/repeatable/05102026_0428_weekly_source_manual_review_commands_v2.sql
-- Scope: WP-PP actor/read-view changes only.
create or replace function public.weekly_source_manual_review_open_v1(p_request jsonb)
returns jsonb language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid; v_row_id uuid; v_command_id uuid; v_reason text; v_hash bytea;
  v_prior private.weekly_source_manual_review_commands%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_source jsonb; v_route jsonb; v_result jsonb; v_review uuid; v_existing boolean;
  v_family_id uuid; v_sequence bigint; v_count integer;
begin
  perform private.weekly_source_query_require_service_v1();
  if jsonb_typeof(p_request) is distinct from 'object' or exists(select 1 from jsonb_object_keys(p_request) k
    where k not in ('actor_user_id','source_row_id','reason','command_id')) then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_REQUEST_INVALID' using errcode='22023'; end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid; v_row_id:=(p_request->>'source_row_id')::uuid;
    v_command_id:=(p_request->>'command_id')::uuid;
  exception when others then raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_REQUEST_INVALID' using errcode='22023'; end;
  v_reason:=btrim(coalesce(p_request->>'reason',''));
  if v_actor is null or v_row_id is null or v_command_id is null or char_length(v_reason) not between 1 and 1000 then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_REQUEST_INVALID' using errcode='22023'; end if;
  v_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_MANUAL_REVIEW_OPEN_V2',p_request);
  select c.* into v_prior from private.weekly_source_manual_review_commands c where c.command_id=v_command_id;
  if found then
    if v_prior.command_kind is distinct from 'OPEN' or v_prior.actor_user_id is distinct from v_actor
       or v_prior.request_sha256 is distinct from v_hash then
      raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_COMMAND_COLLISION' using errcode='23505'; end if;
    select c.* into strict v_cycle from public.weekly_source_cycles c where c.id=v_prior.source_cycle_id;
    perform private.weekly_source_office_authority_v1(v_actor,'OPEN_PAY_QUERY',v_cycle.source_group_id,
      v_prior.client_id,v_cycle.finalisation_week_ending);
    return v_prior.result_json||jsonb_build_object('idempotent_replay',true);
  end if;
  perform private.weekly_source_pay_query_admit_v2();
  perform pg_advisory_xact_lock(hashtextextended('WEEKLY_SOURCE_MANUAL_REVIEW_COMMAND:'||v_command_id::text,0));
  select c.* into v_prior from private.weekly_source_manual_review_commands c where c.command_id=v_command_id;
  if found then
    if v_prior.command_kind is distinct from 'OPEN' or v_prior.actor_user_id is distinct from v_actor
       or v_prior.request_sha256 is distinct from v_hash then
      raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_COMMAND_COLLISION' using errcode='23505'; end if;
    select c.* into strict v_cycle from public.weekly_source_cycles c where c.id=v_prior.source_cycle_id;
    perform private.weekly_source_office_authority_v1(v_actor,'OPEN_PAY_QUERY',v_cycle.source_group_id,
      v_prior.client_id,v_cycle.finalisation_week_ending);
    return v_prior.result_json||jsonb_build_object('idempotent_replay',true);
  end if;
  -- The global try-key serialises participating source writers. Re-read the
  -- actual source and current authority after admission; never use a stale UI tuple.
  v_source:=private.weekly_source_manual_review_source_v2(v_row_id);
  if v_source is null then raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_SOURCE_STALE' using errcode='40001'; end if;
  select c.* into strict v_cycle from public.weekly_source_cycles c where c.id=(v_source->>'source_cycle_id')::uuid;
  perform private.weekly_source_office_authority_v1(v_actor,'OPEN_PAY_QUERY',v_cycle.source_group_id,
    (v_source->>'client_id')::uuid,v_cycle.finalisation_week_ending);
  v_route:=private.weekly_source_office_route_key_v1(v_cycle.id,(v_source->>'candidate_id')::uuid,
    (v_source->>'client_id')::uuid,(v_source->>'contract_id')::uuid,(v_source->>'work_date')::date);
  if v_route->>'authority_mode' is distinct from 'SOURCE_AUTHORITY' then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_AUTHORITY_INVALID' using errcode='22023'; end if;
  perform pg_advisory_xact_lock(hashtextextended('WEEKLY_SOURCE_QUERY_FAMILY:'||
    (v_source->>'contract_id')||':'||(v_source->>'week_ending_date'),0));
  select r.id into v_review from private.weekly_source_manual_reviews r
    where r.source_group_id=v_cycle.source_group_id and r.work_event_id=(v_source->>'work_event_id')::uuid
      and r.state='OPEN' for update;
  v_existing:=v_review is not null;
  if v_existing and not exists(select 1 from private.weekly_source_manual_reviews r where r.id=v_review
    and r.candidate_id=(v_source->>'candidate_id')::uuid and r.client_id=(v_source->>'client_id')::uuid
    and r.contract_id=(v_source->>'contract_id')::uuid and r.work_date=(v_source->>'work_date')::date) then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_AUTHORITY_INVALID' using errcode='55000'; end if;
  if not v_existing then
    select count(*),(array_agg(f.id))[1] into v_count,v_family_id
      from public.weekly_exceptional_pay_target_families f
      where f.contract_id=(v_source->>'contract_id')::uuid and f.candidate_id=(v_source->>'candidate_id')::uuid
        and f.week_ending_date=(v_source->>'week_ending_date')::date;
    if v_count>1 then raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_AUTHORITY_AMBIGUOUS' using errcode='55000'; end if;
    select coalesce(max(e.event_sequence),0) into v_sequence from public.weekly_exceptional_pay_family_events e
      where e.family_id=v_family_id and e.durable_work_event_id=(v_source->>'work_event_id')::uuid;
    insert into private.weekly_source_manual_reviews(source_group_id,source_cycle_id,upload_row_id,
      row_resolution_id,source_row_hash,work_event_id,candidate_id,client_id,contract_id,work_date,
      state,office_reason,opened_by_user_id)
    values(v_cycle.source_group_id,v_cycle.id,v_row_id,(v_source->>'row_resolution_id')::uuid,
      decode(v_source->>'source_row_hash','hex'),(v_source->>'work_event_id')::uuid,
      (v_source->>'candidate_id')::uuid,(v_source->>'client_id')::uuid,(v_source->>'contract_id')::uuid,
      (v_source->>'work_date')::date,'OPEN',v_reason,v_actor) returning id into v_review;
    perform public._audit_insert('weekly_source_manual_review',v_review::text,'WEEKLY_SOURCE_MANUAL_REVIEW_OPENED',null,
      jsonb_build_object('source_row_id',v_row_id,'work_event_id',v_source->'work_event_id','reason',v_reason,
        'opening_final_revision_id',v_source->'final_revision_id',
        'opening_projection_publication_id',v_source->'projection_publication_id'),
      'Office sent imported shift to Pay Queries',v_actor);
  end if;
  v_result:=jsonb_build_object('ok',true,'review_id',v_review,'already_open',v_existing,
    'problem','Manually queried','idempotent_replay',false);
  insert into private.weekly_source_manual_review_commands(command_id,command_kind,actor_user_id,request_sha256,
    review_id,source_cycle_id,client_id,week_ending_date,opening_final_revision_id,opening_projection_publication_id,
    opening_target_family_id,opening_event_sequence,result_json)
  values(v_command_id,'OPEN',v_actor,v_hash,v_review,v_cycle.id,(v_source->>'client_id')::uuid,
    (v_source->>'week_ending_date')::date,case when not v_existing then (v_source->>'final_revision_id')::uuid end,
    case when not v_existing then (v_source->>'projection_publication_id')::uuid end,
    case when not v_existing then v_family_id end,case when not v_existing then v_sequence end,v_result);
  return v_result;
end;
$function$;


-- Current TEST source: supabase/repeatable/05102026_1210_weekly_source_office_authorise_scope_v1.sql
-- Scope: WP-PP actor/read-view changes only.
create or replace function public.weekly_source_office_authorise_scope_v1(p_request jsonb)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid; v_id uuid; v_group uuid; v_count integer;
  v_root public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
begin
  perform private.weekly_source_query_require_service_v1();
  if jsonb_typeof(p_request) is distinct from 'object'
     or not p_request ?& array['actor_user_id','timesheet_id']
     or p_request-array['actor_user_id','timesheet_id']<>'{}'::jsonb
     or jsonb_typeof(p_request->'actor_user_id') is distinct from 'string'
     or jsonb_typeof(p_request->'timesheet_id') is distinct from 'string' then
    raise exception 'WEEKLY_SOURCE_AUTHORISE_SCOPE_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_id:=(p_request->>'timesheet_id')::uuid;
  exception when invalid_text_representation then
    raise exception 'WEEKLY_SOURCE_AUTHORISE_SCOPE_REQUEST_INVALID' using errcode='22023';
  end;
  if v_actor is null or v_id is null then
    raise exception 'WEEKLY_SOURCE_AUTHORISE_SCOPE_REQUEST_INVALID' using errcode='22023';
  end if;
  perform private.weekly_source_office_authority_v1(v_actor,'VIEW_PAY_WORK',null,null,current_date);
  select t.* into v_root from public.timesheets t where t.timesheet_id=v_id
    and t.is_current and t.revoked_at is null and t.archived_at_utc is null;
  if not found then
    raise exception 'WEEKLY_SOURCE_TIMESHEET_NOT_FOUND' using errcode='22023';
  end if;
  if v_root.sheet_scope is distinct from 'WEEKLY'
     or v_root.line_type is distinct from 'HOURS' or v_root.contract_id is null then
    return jsonb_build_object('contract','WEEKLY_SOURCE_AUTHORISE_SCOPE_V1','applicable',false);
  end if;
  select c.* into strict v_contract from public.contracts c where c.id=v_root.contract_id;
  select count(*),(array_agg(g.id))[1] into v_count,v_group
    from public.weekly_source_group_clients membership
    join public.weekly_source_groups g on g.id=membership.source_group_id
    where membership.client_id=v_contract.client_id and g.active
      and v_root.week_ending_date between membership.valid_from and coalesce(membership.valid_to,'infinity'::date);
  if v_count=0 then
    return jsonb_build_object('contract','WEEKLY_SOURCE_AUTHORISE_SCOPE_V1','applicable',false);
  elsif v_count<>1 then
    raise exception 'WEEKLY_SOURCE_AUTHORISE_SCOPE_AMBIGUOUS' using errcode='55000';
  end if;
  perform private.weekly_source_office_authority_v1(v_actor,'VIEW_PAY_WORK',
    v_group,v_contract.client_id,v_root.week_ending_date);
  return jsonb_build_object('contract','WEEKLY_SOURCE_AUTHORISE_SCOPE_V1','applicable',true);
exception when no_data_found or too_many_rows then
  raise exception 'WEEKLY_SOURCE_AUTHORISE_SCOPE_UNAVAILABLE' using errcode='55000';
end;
$function$;


-- Current TEST source: supabase/repeatable/01102026_1201_weekly_source_combined_workspace.sql
-- Scope: WP-PP actor/read-view changes only.
create or replace function public.weekly_source_workspace_scopes_v1(p_request jsonb)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_group uuid;
  v_client uuid;
  v_week date;
  v_limit integer:=100;
  v_offset integer:=0;
  v_cursor jsonb;
  v_rows jsonb;
  v_version text;
  v_total integer;
  v_missing integer;
  v_pending integer;
  v_cutoffs jsonb;
  v_monday date:=(date_trunc('week',statement_timestamp() at time zone 'Europe/London'))::date;
begin
  perform private.weekly_source_query_require_service_v1();
  perform private._weekly_source_settings_assert_request_v1(p_request,
    array['actor_user_id','source_group_id','client_id','week_ending','cursor','limit'],
    'WEEKLY_SOURCE_SCOPE_REQUEST_INVALID');
  v_actor:=(p_request->>'actor_user_id')::uuid;
  v_group:=nullif(p_request->>'source_group_id','')::uuid;
  v_client:=nullif(p_request->>'client_id','')::uuid;
  v_week:=nullif(p_request->>'week_ending','')::date;
  v_limit:=coalesce((p_request->>'limit')::integer,100);
  if v_limit not between 1 and 100 then
    raise exception 'WEEKLY_SOURCE_SCOPE_REQUEST_INVALID' using errcode='22023';
  end if;
  perform private.weekly_source_office_authority_v1(v_actor,case when exists(select 1 from public.tms_users actor where actor.id=v_actor
    and lower(btrim(coalesce(actor.role,'')))='admin') then 'VIEW_SOURCE_PROGRESS' else 'VIEW_PAY_WORK' end,v_group);
  if nullif(p_request->>'cursor','') is not null then
    begin
      v_cursor:=convert_from(decode(p_request->>'cursor','base64'),'UTF8')::jsonb;
      v_offset:=(v_cursor->>'offset')::integer;
      if v_offset is null or v_offset<0 then raise exception 'invalid offset'; end if;
    exception when others then
      raise exception 'WEEKLY_SOURCE_WORKSPACE_CURSOR_STALE' using errcode='40001';
    end;
  end if;
  with candidates as (
    select distinct on (source_group.id,membership.client_id,cycle.finalisation_week_ending)
      source_group.id group_id,source_group.display_name source_name,source_group.source_family,
      membership.client_id,client.name client_name,cycle.id cycle_id,
      cycle.scope_client_id,
      cycle.finalisation_week_ending,cycle.cutoff_at_utc,cycle.version cycle_version,
      cycle.current_complete_upload_id,cycle.current_projection_publication_id
    from public.weekly_source_groups source_group
    join public.weekly_source_cycles cycle on cycle.source_group_id=source_group.id
    join public.weekly_source_group_clients membership on membership.source_group_id=source_group.id
      and cycle.finalisation_week_ending between membership.valid_from and coalesce(membership.valid_to,'infinity'::date)
      and (cycle.scope_client_id is null or cycle.scope_client_id=membership.client_id)
    join public.clients client on client.id=membership.client_id
    where source_group.active and (v_group is null or source_group.id=v_group)
      and (v_client is null or membership.client_id=v_client)
      and (v_week is null or cycle.finalisation_week_ending=v_week)
      and exists(select 1 from public.weekly_source_client_policies policy
        where policy.source_group_id=source_group.id and policy.client_id=membership.client_id
          and policy.authority_mode='SOURCE_AUTHORITY'
          and cycle.finalisation_week_ending between policy.effective_from and coalesce(policy.effective_to,'infinity'::date))
    order by source_group.id,membership.client_id,cycle.finalisation_week_ending,
      (cycle.scope_client_id is not null) desc,cycle.id
  ), scopes as (
    select candidate.*,report.id report_scope_id,report.version report_version,
      case when candidate.source_family='NHSP' then report.current_complete_upload_id
        when candidate.scope_client_id=candidate.client_id then candidate.current_complete_upload_id end upload_id,
      case when candidate.source_family='NHSP' then report.current_projection_publication_id
        when candidate.scope_client_id=candidate.client_id then candidate.current_projection_publication_id end publication_id,
      coalesce(final_report.id,completion.id) completion_id,
      case when final_report.id is not null then 'FINAL_SOURCE' else completion.completion_kind end completion_kind,
      coalesce(final_report.finalised_at_utc,completion.attested_at_utc) completed_at,
      encode(coalesce(final_report.manifest_hash,completion.completion_hash),'hex') completion_hash,
      coalesce(report.cutoff_at_utc,candidate.cutoff_at_utc) actual_cutoff
    from candidates candidate
    left join public.weekly_source_report_scopes report on report.source_cycle_id=candidate.cycle_id
      and report.client_id=candidate.client_id and candidate.source_family='NHSP'
    left join public.weekly_source_client_cycle_completions completion
      on completion.source_cycle_id=candidate.cycle_id and completion.client_id=candidate.client_id
      and completion.state='CURRENT' and completion.completion_kind='NO_SHIFTS_TO_IMPORT'
    -- Completion belongs to this exact client/report. A completed sibling
    -- report for the same client/week must not hide another pending report.
    left join lateral (
      select revision.id,revision.finalised_at_utc,revision.manifest_hash
      from public.weekly_source_final_revisions revision
      join public.weekly_source_client_manifests manifest on manifest.final_revision_id=revision.id
        and manifest.client_id=candidate.client_id
      where revision.source_cycle_id=candidate.cycle_id and revision.state='CURRENT'
        and revision.report_scope_id is not distinct from report.id
      order by revision.revision_number desc,revision.id limit 1
    ) final_report on true
  ), facts as (
    select scopes.*,private.weekly_source_import_is_prepared_v1(scopes.upload_id) prepared
    from scopes
  ), ordered as (
    select facts.*,bool_or(prepared) over(partition by cycle_id,client_id) client_has_prepared,
      row_number() over(order by private.weekly_source_query_ascii_fold_v1(client_name) collate "C",
      client_id,finalisation_week_ending desc,group_id,report_scope_id nulls first) ordinal
    from facts
  )
  select count(*)::integer,
    count(distinct (cycle_id,client_id)) filter(where completion_id is null and not client_has_prepared
      and actual_cutoff<(v_monday::timestamp at time zone 'Europe/London'))::integer,
    count(distinct (cycle_id,client_id)) filter(where completion_id is null and prepared)::integer,
    encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_WORKSPACE_SCOPES_V1',
      jsonb_build_object('filter',p_request-'cursor'-'limit','rows',coalesce(jsonb_agg(jsonb_build_array(
        cycle_id,client_id,cycle_version,report_scope_id,report_version,upload_id,publication_id,completion_hash,prepared)
        order by ordinal),'[]'::jsonb))),'hex'),
    coalesce(jsonb_agg(jsonb_build_object(
      'key',cycle_id::text||':'||client_id::text||':'||coalesce(report_scope_id::text,'CYCLE'),
      'source_group_id',group_id,'source_cycle_id',cycle_id,'report_scope_id',report_scope_id,
      'client_id',client_id,'client',client_name,'source',source_name,'source_family',source_family,
      'week_ending',finalisation_week_ending,'period',to_char(finalisation_week_ending,'FMDD Mon YYYY'),
      'cutoff',actual_cutoff,'upload_id',upload_id,'projection_publication_id',publication_id,
      'prepared',prepared,'completed',completion_id is not null,'completion_kind',completion_kind,
      'completed_at',completed_at,
      'missing_previous_report',completion_id is null and not client_has_prepared
        and actual_cutoff<(v_monday::timestamp at time zone 'Europe/London')
      ) order by ordinal) filter(where ordinal>v_offset and ordinal<=v_offset+v_limit),'[]'::jsonb)
    into v_total,v_missing,v_pending,v_version,v_rows from ordered;
  if v_cursor is not null and v_cursor->>'version' is distinct from v_version then
    raise exception 'WEEKLY_SOURCE_WORKSPACE_CURSOR_STALE' using errcode='40001';
  end if;
  select coalesce(jsonb_agg(jsonb_build_object('source_group_id',source_group.id,
    'source',source_group.display_name,'cutoff',deadline.value,
    'passed',statement_timestamp()>=deadline.value) order by source_group.display_name,source_group.id),'[]'::jsonb)
  into v_cutoffs from public.weekly_source_groups source_group
  cross join lateral(select ((v_monday+((source_group.cutoff_weekday+6)%7))
    +source_group.cutoff_local_time) at time zone source_group.timezone value) deadline
  where source_group.active and (v_group is null or source_group.id=v_group)
    and exists(select 1 from public.weekly_source_group_clients membership
      join public.weekly_source_client_policies policy on policy.source_group_id=source_group.id
        and policy.client_id=membership.client_id and policy.authority_mode='SOURCE_AUTHORITY'
      where membership.source_group_id=source_group.id and (v_client is null or membership.client_id=v_client)
        and (statement_timestamp() at time zone source_group.timezone)::date between membership.valid_from and coalesce(membership.valid_to,'infinity'::date)
        and (statement_timestamp() at time zone source_group.timezone)::date between policy.effective_from and coalesce(policy.effective_to,'infinity'::date));
  return jsonb_build_object('ok',true,'contract','WEEKLY_SOURCE_WORKSPACE_SCOPES_V1',
    'rows',v_rows,'total_count',v_total,'version',v_version,
    'missing_previous_reports',v_missing,'reports_awaiting_finalisation',v_pending,
    'this_week_cutoffs',v_cutoffs,'has_more',v_offset+v_limit<v_total,
    'next_cursor',case when v_offset+v_limit<v_total then
      encode(convert_to(jsonb_build_object('offset',v_offset+v_limit,'version',v_version)::text,'UTF8'),'base64') else '' end);
end;
$function$;


-- Current TEST source: supabase/repeatable/09102026_1130_weekly_source_released_review_visibility_v1.sql
-- Scope: WP-PP actor/read-view changes only.
CREATE OR REPLACE FUNCTION public.weekly_source_combined_review_workspace_v1(p_request jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'pg_temp'
AS $function$
declare
  v_pay_view boolean;
  v_result jsonb;
  v_request jsonb; v_scope_page jsonb; v_scopes jsonb:='[]'; v_scope jsonb;
  v_workspace jsonb; v_owner_request jsonb; v_rows jsonb:='[]'; v_item jsonb;
  v_manual_children jsonb; v_question_children jsonb; v_child jsonb;
  v_follow_workspace jsonb; v_follow_up jsonb;
  v_versions jsonb:='[]'; v_owners jsonb:='[]'; v_summary jsonb;
  v_tab text:=coalesce(nullif(p_request->>'tab',''),'queries');
  v_section text:=coalesce(nullif(p_request->>'section',''),'questions');
  v_sort text:=coalesce(nullif(p_request->>'sort_key',''),case when nullif(p_request->>'client_id','') is null then 'client' else 'candidate' end);
  v_direction text:=coalesce(nullif(p_request->>'sort_direction',''),'asc');
  v_seek text:=private.weekly_source_query_ascii_fold_v1(coalesce(p_request->>'seek',''));
  v_limit integer:=coalesce((p_request->>'limit')::integer,50);
  v_offset integer:=0; v_base integer:=0; v_total integer; v_page jsonb; v_counts jsonb;
  v_version text; v_cursor jsonb; v_client uuid; v_seen text[]:='{}'; v_key text; v_owner_key text;
  v_attention jsonb; v_attention_first boolean:=coalesce((p_request->>'attention_first')::boolean,false);
  v_attention_kind text:=coalesce(p_request->>'attention_kind','');
  v_released_scope record;
  v_pending record; v_pending_checks jsonb:='[]'::jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  perform private._weekly_source_settings_assert_request_v1(p_request,
    array['actor_user_id','source_group_id','client_id','week_ending','tab','section',
      'sort_key','sort_direction','seek','cursor','limit','report_key','attention_first','attention_kind'],'WEEKLY_SOURCE_COMBINED_REQUEST_INVALID');
  v_pay_view:=not exists(select 1 from public.tms_users actor
    where actor.id=(p_request->>'actor_user_id')::uuid and lower(btrim(coalesce(actor.role,'')))='admin');
  if v_pay_view then
    perform private.weekly_source_office_authority_v1((p_request->>'actor_user_id')::uuid,'VIEW_PAY_WORK');
    if v_tab<>'queries' or v_section not in ('questions','checks','protected') then
      raise exception 'WEEKLY_SOURCE_OFFICE_ADMIN_REQUIRED' using errcode='42501';
    end if;
  end if;
  if v_tab='history' then
    return public.weekly_source_report_history_v1(p_request-'tab'-'section');
  end if;
  if p_request ? 'report_key' then
    raise exception 'WEEKLY_SOURCE_COMBINED_REQUEST_INVALID' using errcode='22023';
  end if;
  if v_tab not in ('imports','queries') or v_section not in ('questions','checks','protected','current','archive')
    or v_sort not in ('client','candidate','day_date','status','file','uploaded')
    or v_direction not in ('asc','desc') or v_limit not between 1 and 100 or length(v_seek)>100
    or v_attention_kind not in ('','missing_source','questions','checks','protected')
    or (p_request ? 'attention_first' and jsonb_typeof(p_request->'attention_first')<>'boolean') then
    raise exception 'WEEKLY_SOURCE_COMBINED_REQUEST_INVALID' using errcode='22023';
  end if;
  v_client:=nullif(p_request->>'client_id','')::uuid;
  v_request:=p_request-'tab'-'section'-'sort_key'-'sort_direction'-'seek'-'cursor'-'limit'-'attention_first'-'attention_kind';
  loop
    v_scope_page:=public.weekly_source_workspace_scopes_v1(v_request);
    v_summary:=v_scope_page-'rows'-'next_cursor'-'has_more';
    v_scopes:=v_scopes||(v_scope_page->'rows');
    exit when not (v_scope_page->>'has_more')::boolean;
    v_request:=v_request||jsonb_build_object('cursor',v_scope_page->>'next_cursor');
  end loop;

  -- Checking-only released NHSP files have row-wise client authority. These
  -- review scopes are not report obligations and must never feed Finalise.
  -- Keep contract, charge and Hours questions visible for eligible row clients
  -- even when they are outside the final backing report's source membership.
  if v_tab='queries' then
    for v_released_scope in
      select distinct cycle.id cycle_id,cycle.source_group_id,
        cycle.finalisation_week_ending,cycle.cutoff_at_utc,
        source_group.display_name source_name,resolution.client_id,client.name client_name,
        upload.id upload_id,publication.id publication_id
      from public.weekly_source_cycles cycle
      join public.weekly_source_groups source_group on source_group.id=cycle.source_group_id
      join public.weekly_source_uploads upload on upload.id=cycle.current_complete_upload_id
        and upload.source_cycle_id=cycle.id and upload.state='CURRENT' and upload.report_scope_id is null
      join public.weekly_source_format_profiles profile on profile.id=upload.source_format_profile_id
      join public.weekly_source_projection_publications publication on publication.upload_id=upload.id
        and publication.source_cycle_id=cycle.id and publication.authority_scope_kind='CYCLE'
        and publication.report_scope_id is null and publication.authority_scope_version=cycle.version
      left join private.weekly_source_office_rechecks recheck on recheck.publication_id=publication.id
      left join public.weekly_source_projection_publications prior_publication on prior_publication.id=recheck.prior_publication_id
      join public.weekly_source_upload_rows source_row on source_row.upload_id=upload.id
      join public.weekly_source_row_resolutions resolution on resolution.upload_row_id=source_row.id
        and resolution.generation=case when publication.state='CURRENT'
          then coalesce(publication.projection_generation,publication.authority_scope_version)
          else coalesce(prior_publication.projection_generation,prior_publication.authority_scope_version) end
      join public.clients client on client.id=resolution.client_id
      where source_group.active and source_group.source_family='NHSP'
        and profile.profile_code='NHSP_PREFINAL_RELEASED_V1'
        and profile.row_finalisation_capability='CHECKING_ONLY' and not profile.single_client_required
        and ((publication.state='CURRENT' and cycle.current_projection_publication_id=publication.id)
          or (publication.state='BUILDING' and recheck.upload_id=upload.id))
        and (nullif(p_request->>'source_group_id','') is null or cycle.source_group_id=(p_request->>'source_group_id')::uuid)
        and (v_client is null or resolution.client_id=v_client)
        and (nullif(p_request->>'week_ending','') is null or cycle.finalisation_week_ending=(p_request->>'week_ending')::date)
        and private.weekly_source_upload_client_eligible_v1(upload.id,resolution.client_id,source_row.work_date)
        and not exists(select 1 from jsonb_array_elements(v_scopes) existing
          where existing->>'source_cycle_id'=cycle.id::text and existing->>'client_id'=resolution.client_id::text)
      order by cycle.id,resolution.client_id,publication.id
    loop
      perform private.weekly_source_office_authority_v1((p_request->>'actor_user_id')::uuid,
        case when v_pay_view then 'VIEW_PAY_WORK' else 'VIEW_SOURCE_PROGRESS' end,v_released_scope.source_group_id);
      v_scopes:=v_scopes||jsonb_build_array(jsonb_build_object(
        'key',v_released_scope.cycle_id::text||':'||v_released_scope.client_id::text||':RELEASED_REVIEW',
        'source_group_id',v_released_scope.source_group_id,'source_cycle_id',v_released_scope.cycle_id,
        'report_scope_id',null,'client_id',v_released_scope.client_id,'client',v_released_scope.client_name,
        'source',v_released_scope.source_name,'source_family','NHSP',
        'week_ending',v_released_scope.finalisation_week_ending,
        'period',to_char(v_released_scope.finalisation_week_ending,'FMDD Mon YYYY'),
        'cutoff',v_released_scope.cutoff_at_utc,'upload_id',v_released_scope.upload_id,
        'projection_publication_id',v_released_scope.publication_id,
        'review_only',true,'prepared',false,'completed',false,'completion_kind',null,
        'missing_previous_report',false));
    end loop;
  end if;

  -- A cycle publication is shared across its client scopes. If it has no
  -- cycle publication, each NHSP client's current report scope can instead
  -- own distinct query work. Visit each such scope exactly once.
  for v_scope in select distinct on (item->>'source_cycle_id',
      case when v_tab='queries' and cycle.current_projection_publication_id is null then item->>'client_id' else '' end)
      item||jsonb_build_object('cycle_publication_id',cycle.current_projection_publication_id)
    from jsonb_array_elements(v_scopes) item
    join public.weekly_source_cycles cycle on cycle.id=(item->>'source_cycle_id')::uuid
    order by item->>'source_cycle_id',
      case when v_tab='queries' and cycle.current_projection_publication_id is null then item->>'client_id' else '' end,
      case when item->>'report_scope_id' is null then 1 else 0 end,
      item->>'cutoff' desc nulls last,item->>'key'
  loop
    v_owner_request:=jsonb_build_object('actor_user_id',p_request->>'actor_user_id',
      'tab',v_tab,'source_group_id',v_scope->>'source_group_id',
      'source_cycle_id',v_scope->>'source_cycle_id','limit',100);
    if v_scope->>'source_family'='ROSTER' or
      (v_tab='queries' and v_scope->>'source_family'='NHSP' and v_scope->>'cycle_publication_id' is null
        and v_scope->>'report_scope_id' is not null) then
      v_owner_request:=v_owner_request||jsonb_build_object('client_id',v_scope->>'client_id');
    end if;
    if v_tab='queries' and v_scope->>'source_family'='NHSP' and v_scope->>'cycle_publication_id' is null
      and v_scope->>'report_scope_id' is not null then
      v_owner_request:=v_owner_request||jsonb_build_object('report_scope_id',v_scope->>'report_scope_id');
    end if;
    loop
      v_workspace:=public.weekly_source_office_workspace_v1(v_owner_request);
      v_owner_key:=case when v_tab='imports' then v_scope->>'source_cycle_id'
        else (v_scope->>'source_cycle_id')||':'||
          coalesce(v_workspace#>>'{selected,projection_publication_id}','none') end;
      v_versions:=v_versions||jsonb_build_array(jsonb_build_array(v_scope->>'source_cycle_id',v_workspace->>'workspace_version'));
      if not (v_owner_request ? 'cursor') then
        v_owners:=v_owners||jsonb_build_array(jsonb_build_object('key',v_owner_key,
          'source',v_scope->>'source','period',v_scope->>'period','scope',v_workspace->'selected',
          'bulk_actions',v_workspace#>'{queries,bulk_actions}',
          'protected_pay_enabled',v_workspace#>'{queries,protected_pay_enabled}'));
      end if;
      for v_item in select item from jsonb_array_elements(coalesce(v_workspace#>array[v_tab,'rows'],'[]')) item
      loop
        v_key:=v_tab||':'||v_owner_key;
        v_key:=v_key||':'||(v_item->>'row_key');
        if v_key=any(v_seen) then continue; end if;
        v_seen:=array_append(v_seen,v_key);
        if nullif(v_item->>'client_id','') is not null and not exists(select 1
          from jsonb_array_elements(v_scopes) permitted where permitted->>'source_cycle_id'=v_scope->>'source_cycle_id'
            and permitted->>'client_id'=v_item->>'client_id') then continue; end if;
        if v_client is not null and nullif(v_item->>'client_id','') is not null
          and (v_item->>'client_id')::uuid<>v_client then continue; end if;
        if v_tab='imports' then
          -- Upload errors remain in their immediate review receipt, not an
          -- ever-growing archive. Retain only accepted current work here.
          if v_item->>'state' is distinct from 'CURRENT' then continue; end if;
          if exists(select 1 from public.weekly_source_final_revisions revision
            join public.weekly_source_client_manifests manifest on manifest.final_revision_id=revision.id
            where revision.upload_id=(v_item->>'row_key')::uuid
              and revision.state in ('CURRENT','SUPERSEDED')
              and (nullif(v_item->>'client_id','') is null
                or manifest.client_id=(v_item->>'client_id')::uuid))
            and not exists(select 1 from jsonb_array_elements(v_scopes) sibling
              where sibling->>'upload_id'=v_item->>'row_key'
                and not coalesce((sibling->>'completed')::boolean,false)) then continue; end if;
        end if;
        if v_tab='queries' then
          -- An Office-created pay review is an Office check, not a request for
          -- candidate/manager hours evidence. Split only those children so a
          -- mixed group can retain its genuine Hours questions independently.
          select coalesce(jsonb_agg(child.value order by child.ordinality)
              filter(where child.value ? 'manual_review_id'),'[]'::jsonb),
            coalesce(jsonb_agg(child.value order by child.ordinality)
              filter(where not (child.value ? 'manual_review_id')),'[]'::jsonb)
            into v_manual_children,v_question_children
          from jsonb_array_elements(coalesce(v_item->'children','[]'::jsonb))
            with ordinality child(value,ordinality);
          for v_child in select value from jsonb_array_elements(v_manual_children)
          loop
            v_rows:=v_rows||jsonb_build_array(jsonb_build_object(
              'combined_key','manual-check:'||(v_child->>'manual_review_id'),
              'row_key',v_child->>'row_key','section','checks',
              'scope_key',v_owner_key,
              'source',v_scope->>'source','source_family',v_scope->>'source_family',
              'period',v_scope->>'period',
              'client',v_item->>'client','client_id',v_item->>'client_id',
              'candidate',v_item->>'candidate','candidate_id',v_item->>'candidate_id',
              'day_date',v_child->>'day_date','system_hours',v_child->>'system_hours',
              'status',v_child->'status','manual_query',v_child->'manual_query',
              'problem','Accept current source hours or protect pay.',
              'pay_blocking',true,'actions',v_child->'actions'));
          end loop;
          if jsonb_array_length(v_question_children)=0 then continue; end if;
          if jsonb_array_length(v_manual_children)>0 then
            v_item:=jsonb_set(v_item,'{actions,0,payload,detail,shifts}',v_question_children,false)
              ||jsonb_build_object('children',v_question_children,
                'issues',greatest(0,coalesce((v_item->>'issues')::integer,0)
                  -jsonb_array_length(v_manual_children)));
          end if;
        end if;
        v_rows:=v_rows||jsonb_build_array(v_item||jsonb_build_object(
          'combined_key',v_key,'scope_key',v_owner_key,'source',v_scope->>'source',
          'source_family',v_scope->>'source_family',
          'period',v_scope->>'period','section',case when v_tab='queries' then 'questions' else 'current' end,
          'client',coalesce(nullif(v_item->>'client',''),(select name from public.clients
            where id=nullif(v_item->>'client_id','')::uuid),'Source-wide file')));
      end loop;
      if v_tab='queries' and not (v_owner_request ? 'cursor') then
        for v_item in
          select item||jsonb_build_object('section','checks') from jsonb_array_elements(coalesce(v_workspace#>'{queries,office_checks,rows}','[]')) item
          union all
          select item||jsonb_build_object('section','protected') from jsonb_array_elements(coalesce(v_workspace#>'{queries,protected_shifts,rows}','[]')) item
        loop
          v_key:=case when v_item->>'section'='protected' then
            'protected:'||(v_item->>'family_id')||':'||(v_item->>'work_event_id')
            else (v_item->>'section')||':'||v_owner_key||':'||(v_item->>'row_key') end;
          if v_key=any(v_seen) then continue; end if;
          v_seen:=array_append(v_seen,v_key);
          if nullif(v_item->>'client_id','') is not null and not exists(select 1
            from jsonb_array_elements(v_scopes) permitted where permitted->>'source_group_id'=v_scope->>'source_group_id'
              and permitted->>'client_id'=v_item->>'client_id') then continue; end if;
          if v_client is not null and nullif(v_item->>'client_id','') is not null
            and (v_item->>'client_id')::uuid<>v_client then continue; end if;
          v_rows:=v_rows||jsonb_build_array(v_item||jsonb_build_object('combined_key',v_key,
            'scope_key',v_owner_key,'source',v_scope->>'source','period',
              case when v_item->>'section'='protected' then 'Awaiting source period' else v_scope->>'period' end));
        end loop;
      end if;
      exit when not coalesce((v_workspace#>>array[v_tab,'has_more'])::boolean,false);
      v_owner_request:=v_owner_request||jsonb_build_object('cursor',v_workspace#>>array[v_tab,'next_cursor']);
    end loop;
  end loop;
  -- Finalisation does not complete its separately tracked approved-hours work.
  -- Keep that exact owner/action reachable from outstanding Office checks.
  if v_tab='queries' and not v_pay_view then
    for v_scope in select item from jsonb_array_elements(v_scopes) item
      where item->>'completion_kind'='FINAL_SOURCE'
    loop
      v_follow_workspace:=public.weekly_source_office_workspace_v1(jsonb_build_object(
        'actor_user_id',p_request->>'actor_user_id','tab','finalise',
        'source_group_id',v_scope->>'source_group_id','source_cycle_id',v_scope->>'source_cycle_id',
        'client_id',v_scope->>'client_id','report_scope_id',v_scope->>'report_scope_id'));
      v_follow_up:=v_follow_workspace#>'{finalise,approved_hours_follow_up}';
      if nullif(v_follow_up->>'title','') is null then continue; end if;
      v_key:='approved-hours:'||(v_scope->>'key');
      v_versions:=v_versions||jsonb_build_array(jsonb_build_array(v_key,v_follow_workspace->>'workspace_version',v_follow_up));
      v_rows:=v_rows||jsonb_build_array(jsonb_build_object(
        'combined_key',v_key,'row_key',v_key,'section','checks',
        'client',v_scope->>'client','client_id',v_scope->>'client_id',
        'source',v_scope->>'source','period',v_scope->>'period',
        'candidate','Report follow-up','requires_attention',
          v_follow_up->>'state' in ('ACTION_REQUIRED','RECOVERY_REQUIRED')
          or jsonb_typeof(v_follow_up->'action')='object',
        'status',jsonb_build_object('text',v_follow_up->>'title'),
        'problem',v_follow_up->>'body','follow_up_scope',v_follow_workspace->'selected','actions','[]'::jsonb));
    end loop;
  end if;
  -- Rechecks invalidate the old authority before the replacement is ready.
  -- Retain old unresolved rows as explicitly non-actionable history, with only
  -- the exact saved recheck available. Never treat a missing CURRENT pointer
  -- as proof that Office has no work remaining.
  if v_tab='queries' and not v_pay_view then
    for v_pending in
      select recheck.request_id,recheck.actor_user_id,recheck.request_json,
        recheck.upload_id,recheck.publication_id,source_row.id as upload_row_id,
        source_row.work_date,source_row.source_client_identity,source_row.source_candidate_identity,
        source_row.external_source_key,source_row.start_at_local,source_row.end_at_local,source_row.break_minutes,
        coalesce(candidate.display_name,source_row.bounded_raw_columns_json->>'worker_name',
          source_row.bounded_raw_columns_json->>'candidate',source_row.source_candidate_identity) as candidate_name,
        resolution.mapping_state,charge.phase_severity,cycle.source_group_id,cycle.finalisation_week_ending,
        (select item->>'source' from jsonb_array_elements(v_scopes) item
          where item->>'source_cycle_id'=cycle.id::text limit 1) as source_name
      from private.weekly_source_office_rechecks recheck
      join public.weekly_source_projection_publications publication on publication.id=recheck.publication_id
      join public.weekly_source_projection_publications prior_publication on prior_publication.id=recheck.prior_publication_id
      join public.weekly_source_uploads upload on upload.id=recheck.upload_id
      join public.weekly_source_cycles cycle on cycle.id=upload.source_cycle_id
      left join public.weekly_source_report_scopes scope on scope.id=upload.report_scope_id
      join public.weekly_source_upload_rows source_row on source_row.upload_id=upload.id
      join public.weekly_source_row_resolutions resolution on resolution.upload_row_id=source_row.id
        and resolution.generation=coalesce(prior_publication.projection_generation,prior_publication.authority_scope_version)
      left join public.weekly_source_charge_checks charge on charge.upload_row_id=source_row.id
        and charge.generation=resolution.generation
      left join public.candidates candidate on candidate.id=coalesce(
        (select choice.candidate_id from private.weekly_source_office_row_choices choice
          where choice.upload_row_id=source_row.id order by choice.id desc limit 1),resolution.candidate_id)
      where publication.state='BUILDING' and upload.state='CURRENT'
        and publication.authority_scope_version=case when upload.report_scope_id is null then cycle.version else scope.version end
        and upload.id=case when upload.report_scope_id is null then cycle.current_complete_upload_id else scope.current_complete_upload_id end
        and exists(select 1 from jsonb_array_elements(v_scopes) item where item->>'source_cycle_id'=cycle.id::text)
        and (v_client is null or resolution.client_id=v_client or scope.client_id=v_client)
      order by recheck.request_id,source_row.source_row_ordinal
    loop
      v_key:='recheck:'||v_pending.request_id::text||':'||v_pending.upload_row_id::text;
      v_pending_checks:=v_pending_checks||jsonb_build_array(jsonb_build_object(
        'combined_key',v_key,'row_key',v_key,'section','checks','recheck_pending',true,
        'client',v_pending.source_client_identity,'candidate',v_pending.candidate_name,
        'source_reference',v_pending.source_candidate_identity,'booking_reference',v_pending.external_source_key,
        'source',v_pending.source_name,'period',to_char(v_pending.finalisation_week_ending,'FMDD Mon YYYY'),
        'work_date',v_pending.work_date,'day_date',to_char(v_pending.work_date,'FMDD Mon YYYY'),
        'system_hours',to_char(v_pending.start_at_local,'HH24:MI')||'–'||to_char(v_pending.end_at_local,'HH24:MI')
          ||' · '||v_pending.break_minutes::text||' min break',
        'pay_blocking',v_pending.mapping_state<>'RESOLVED',
        'status',jsonb_build_object('text','Recheck incomplete'),
        'problem',case when v_pending.mapping_state<>'RESOLVED' then 'Previous linking check — selection saved; replacement check incomplete.'
          when v_pending.phase_severity in ('PROVISIONAL_WARNING','FINALISATION_BLOCKER') then 'Previous contract charge warning — replacement check incomplete.'
          else 'Previous check — replacement check incomplete.' end,
        'actions',jsonb_build_array(jsonb_build_object('label','Retry recheck',
          'enabled',v_pending.actor_user_id=(p_request->>'actor_user_id')::uuid,
          'command','RECHECK_SOURCE','payload',v_pending.request_json-'actor_user_id'))));
    end loop;
    v_rows:=v_rows||v_pending_checks;
    v_summary:=v_summary||jsonb_build_object('recheck_pending_count',jsonb_array_length(v_pending_checks));
  end if;
  -- Classify the complete permitted collection before counting or paging.
  -- A waiting signature alone is informational; red unresolved work and green
  -- charge/reconciliation decisions remain visible and count as Office work.
  select coalesce(jsonb_agg(item||jsonb_build_object(
    'attention_missing_source_count',case when item->>'section'='questions' then
      (select count(*) from jsonb_array_elements(coalesce(item->'children','[]')) child
        where child->'candidate_shift_absent_from_import'='true'::jsonb) else 0 end,
    'attention_question_count',case when item->>'section'='questions' then
      (select count(*) from jsonb_array_elements(coalesce(item->'children','[]')) child
        where child->>'issue' is distinct from 'Timesheet missing'
          and child->'candidate_shift_absent_from_import' is distinct from 'true'::jsonb) else 0 end,
    'requires_attention',case
    when item->>'section'='questions' then exists(select 1
      from jsonb_array_elements(coalesce(item->'children','[]')) child
      where child->>'issue' is distinct from 'Timesheet missing')
    when item->>'section'='checks' then case when item ? 'follow_up_scope'
      then coalesce((item->>'requires_attention')::boolean,false) else true end
    when item->>'section'='protected' then coalesce((item->>'requires_attention')::boolean,false)
    else false end)),'[]') into v_rows from jsonb_array_elements(v_rows) item;
  select jsonb_build_object('missing_source',coalesce(sum((item->>'attention_missing_source_count')::integer),0),
    'questions',coalesce(sum((item->>'attention_question_count')::integer),0),
    'checks',count(*) filter(where item->>'section'='checks'),
    'protected',count(*) filter(where item->>'section'='protected'),
    'total',coalesce(sum((item->>'attention_missing_source_count')::integer
      +(item->>'attention_question_count')::integer),0)
      +count(*) filter(where item->>'section' in ('checks','protected')),'complete',true) into v_attention
  from jsonb_array_elements(v_rows) item where item->'requires_attention'='true'::jsonb;
  select jsonb_build_object('questions',count(*) filter(where item->>'section'='questions'),
    'checks',count(*) filter(where item->>'section'='checks'),'protected',count(*) filter(where item->>'section'='protected'),
    'current',count(*) filter(where item->>'section'='current'),'archive',count(*) filter(where item->>'section'='archive'))
    into v_counts from jsonb_array_elements(v_rows) item;
  v_version:=encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_COMBINED_REVIEW_V1',
    jsonb_build_object('scopes',v_summary->>'version','owners',v_versions,'attention_rows',v_rows,
      'filters',p_request-'cursor'-'limit')),'hex');
  -- Attention is an exact view filter, not a tab-total alias. The full census
  -- above stays intact; only this page collection is narrowed. Never rewrite
  -- server-owned action selections or comparison/financial proof payloads.
  if v_attention_kind<>'' then
    select coalesce(jsonb_agg(item),'[]') into v_rows from jsonb_array_elements(v_rows) item
    where item->'requires_attention'='true'::jsonb and case v_attention_kind
      when 'missing_source' then item->>'section'='questions' and (item->>'attention_missing_source_count')::integer>0
      when 'questions' then item->>'section'='questions' and (item->>'attention_question_count')::integer>0
      when 'checks' then item->>'section'='checks'
      when 'protected' then item->>'section'='protected'
      else false end;
    if v_attention_kind in ('missing_source','questions') then
      for v_item in select item from jsonb_array_elements(v_rows) item
      loop
        select coalesce(jsonb_agg(child.value order by child.ordinality),'[]') into v_question_children
          from jsonb_array_elements(v_item->'children') with ordinality child(value,ordinality)
          where child.value->>'issue' is distinct from 'Timesheet missing' and case v_attention_kind
            when 'missing_source' then child.value->'candidate_shift_absent_from_import'='true'::jsonb
            else child.value->'candidate_shift_absent_from_import' is distinct from 'true'::jsonb end;
        v_item:=v_item||jsonb_build_object('children',v_question_children,'issues',jsonb_array_length(v_question_children),
          'actions',(select coalesce(jsonb_agg(case when action->>'label'='Open'
            then jsonb_set(action,'{payload,detail,shifts}',v_question_children,false) else action end),'[]')
            from jsonb_array_elements(coalesce(v_item->'actions','[]')) action));
        select coalesce(jsonb_agg(case when item->>'combined_key'=v_item->>'combined_key' then v_item else item end),'[]')
          into v_rows from jsonb_array_elements(v_rows) item;
      end loop;
    end if;
  end if;
  if nullif(p_request->>'cursor','') is not null then
    begin
      v_cursor:=convert_from(decode(p_request->>'cursor','base64'),'UTF8')::jsonb;
      v_offset:=(v_cursor->>'offset')::integer;
      if v_offset is null or v_offset<0 or v_cursor->>'version' is distinct from v_version then raise exception 'stale'; end if;
    exception when others then raise exception 'WEEKLY_SOURCE_WORKSPACE_CURSOR_STALE' using errcode='40001'; end;
  end if;
  with keyed as (
    select item,private.weekly_source_query_ascii_fold_v1(coalesce(case v_sort
      when 'candidate' then coalesce(item->>'candidate_sort',item->>'candidate') when 'client' then item->>'client'
      when 'day_date' then item->>'work_date' when 'file' then item->>'file' when 'uploaded' then item->>'uploaded_at'
      when 'status' then item#>>'{status,text}' end,'')) sort_value
    from jsonb_array_elements(v_rows) item where item->>'section'=v_section
  ), ordered as (
    select *,row_number() over(order by case when v_attention_first and v_seek='' then
      case v_attention_kind when 'missing_source' then ((item->>'attention_missing_source_count')::integer>0)::integer
        when 'questions' then ((item->>'attention_question_count')::integer>0)::integer
        else 0 end else 0 end desc,
      case when v_attention_first and v_seek=''
      then coalesce((item->>'requires_attention')::boolean,false)::integer else 0 end desc,
      case when v_direction='asc' then sort_value end collate "C" asc,
      case when v_direction='desc' then sort_value end collate "C" desc,
      private.weekly_source_query_ascii_fold_v1(item->>'client') collate "C",
      private.weekly_source_query_ascii_fold_v1(coalesce(item->>'candidate_sort',item->>'candidate')) collate "C",
      item->>'work_date',item->>'combined_key') ordinal from keyed
  ), origin as (select coalesce(min(ordinal) filter(where v_seek<>'' and starts_with(sort_value,v_seek)),1)-1 base from ordered)
  select count(*)::integer,coalesce(max(origin.base),0)::integer,coalesce(jsonb_agg(item order by ordinal)
    filter(where ordinal>origin.base+v_offset and ordinal<=origin.base+v_offset+v_limit),'[]')
    into v_total,v_base,v_page from ordered cross join origin;
  v_result:=jsonb_build_object('ok',true,'contract','WEEKLY_SOURCE_COMBINED_REVIEW_V1','tab',v_tab,'section',v_section,
    'version',v_version,'rows',v_page,'total_count',v_total,'owners',v_owners,'scope_options',v_scopes,
    'summary',v_summary,'counts',v_counts,'attention',v_attention,
    'attention_kind',v_attention_kind,
    'sort_key',v_sort,'sort_direction',v_direction,
    'has_more',v_base+v_offset+v_limit<v_total,'next_cursor',case when v_base+v_offset+v_limit<v_total
      then encode(convert_to(jsonb_build_object('version',v_version,'offset',v_offset+v_limit)::text,'UTF8'),'base64') else '' end);
  if v_pay_view then
    v_result:=v_result-array['attention','summary'];
    v_result:=v_result||jsonb_build_object('permissions',jsonb_build_object(
      'contract','WEEKLY_SOURCE_PAYMENT_VIEW_V1','view','PAY',
      'allowed_tabs',jsonb_build_array('queries'),
      'allowed_sections',jsonb_build_array('questions','checks','protected'),
      'landing_tab','queries','can_upload',false,'can_finalise',false,'can_manage_settings',false,
      'checklist',jsonb_build_object('available',false,'read_only',true,'pay_only',true,'action_links',false)));
  end if;
  return v_result;
end;
$function$;


-- Current TEST source: supabase/repeatable/15092026_1534_weekly_source_read_projections_v1.sql
-- Scope: WP-PP actor/read-view changes only.
create or replace function public.weekly_source_office_timesheet_presentation_v1(
  p_request jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_allowed_keys constant text[]:=array['actor_user_id','timesheet_id']::text[];
  v_unknown text;
  v_actor uuid;
  v_timesheet_id uuid;
  v_timesheet public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_client public.clients%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_policy jsonb;
  v_authority text;
  v_route text;
  v_publication public.weekly_source_projection_publications%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_tsfin public.timesheets_financials%rowtype;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_source_rows jsonb:='[]'::jsonb;
  v_submitted_rows jsonb:='[]'::jsonb;
  v_approved_rows jsonb:='[]'::jsonb;
  v_manage_items jsonb:='[]'::jsonb;
  v_comparison_state text;
  v_submitted_available boolean;
  v_submitted_complete boolean;
  v_open_issue_count integer:=0;
  v_unprotected_issue_count integer:=0;
  v_reference_blocker_count integer:=0;
  v_require_reference_to_pay boolean:=false;
  v_authorise_allowed boolean:=false;
  v_blocked_reason text:='';
  v_source_expense_policy text;
  v_manage_allowed boolean:=false;
  v_add_expense_allowed boolean:=false;
  v_record_version text;
  v_operational_category jsonb;
  v_pay_ex numeric:=0;
  v_pay_inc numeric:=0;
  v_charge_ex numeric:=0;
  v_charge_inc numeric:=0;
  v_charge_vat_base numeric:=0;
  v_vat_rate numeric:=0;
  v_totals_complete boolean:=false;
  -- Gate 9 items G9-1, G9-3 and G9-5.
  v_lifecycle jsonb;
  v_candidate_lifecycle jsonb;
  v_inventory jsonb;
  v_settlement jsonb;
  v_schedules jsonb;
  v_currently_approved jsonb;
  v_paid jsonb;
  v_phase text;
  v_first_authorisation boolean:=false;
  v_next boolean:=false;
  v_next_evidence jsonb;
  -- WP-30 (WP-27 sweep finding N5), standing rule 3.  The current source
  -- publication for a week is found through the source-row lineage binding and
  -- the source comparison, both of which are facts about the Timesheet FAMILY.
  -- WP-27 named the lineage limb; the comparison limb beside it is the same
  -- class and was not on its list, so both move together.  EXECUTED on a
  -- rotated family: no publication was found for the current root while the
  -- demoted sibling still found one.
  v_root_family uuid[];
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_PRESENTATION_REQUEST_INVALID' using errcode='22023';
  end if;
  select key into v_unknown from pg_catalog.jsonb_object_keys(p_request) key
  where not (key=any(v_allowed_keys)) order by key limit 1;
  if v_unknown is not null then
    raise exception 'WEEKLY_SOURCE_PRESENTATION_REQUEST_UNKNOWN_FIELD'
      using errcode='22023',detail=v_unknown;
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_timesheet_id:=(p_request->>'timesheet_id')::uuid;
  exception when others then
    raise exception 'WEEKLY_SOURCE_PRESENTATION_REQUEST_INVALID' using errcode='22023';
  end;
  if v_actor is null or v_timesheet_id is null then
    raise exception 'WEEKLY_SOURCE_PRESENTATION_REQUEST_INVALID' using errcode='22023';
  end if;
  perform private.weekly_source_office_authority_v1(
    v_actor,'VIEW_PAY_WORK',null,null,current_date
  );
  select * into v_timesheet from public.timesheets
  where timesheet_id=v_timesheet_id and is_current and revoked_at is null
    and archived_at_utc is null;
  if not found then raise exception 'WEEKLY_SOURCE_TIMESHEET_NOT_FOUND' using errcode='22023'; end if;
  if v_timesheet.sheet_scope='DAILY' then
    -- Matrix `UI-017`.  The phase is still SERVED, so the browser never has to
    -- decide that a Daily record is a Daily record; the heading stays the
    -- existing Daily owner's and this owner supplies none.
    return pg_catalog.jsonb_build_object('applicable',false,'scope','DAILY')
      ||pg_catalog.jsonb_build_object('lifecycle',
        private.weekly_source_office_lifecycle_phase_v1(
          v_timesheet.timesheet_id,
          pg_catalog.jsonb_build_object('applicable',false,'scope','DAILY')));
  end if;
  if v_timesheet.sheet_scope<>'WEEKLY' or v_timesheet.line_type<>'HOURS'
     or v_timesheet.contract_id is null then
    return pg_catalog.jsonb_build_object('applicable',false,'scope',v_timesheet.sheet_scope)
      ||pg_catalog.jsonb_build_object('lifecycle',
        private.weekly_source_office_lifecycle_phase_v1(
          v_timesheet.timesheet_id,
          pg_catalog.jsonb_build_object('applicable',false,'scope',v_timesheet.sheet_scope)));
  end if;
  select * into strict v_contract from public.contracts where id=v_timesheet.contract_id;
  select * into strict v_client from public.clients where id=v_contract.client_id;
  select case
    when coalesce(v_contract.overrideclientsettings,false)
      then coalesce(v_contract.require_reference_to_pay,false)
    else coalesce((
      select settings.pay_reference_required
      from public.client_settings settings
      where settings.client_id=v_contract.client_id
        and (settings.effective_from is null
          or settings.effective_from<=v_timesheet.week_ending_date)
      order by settings.effective_from desc nulls last,settings.updated_at desc,settings.id desc
      limit 1
    ),false)
  end into v_require_reference_to_pay;
  select source_group.* into v_group
  from public.weekly_source_group_clients membership
  join public.weekly_source_groups source_group on source_group.id=membership.source_group_id
  where membership.client_id=v_contract.client_id and source_group.active
    and v_timesheet.week_ending_date between membership.valid_from and coalesce(membership.valid_to,'infinity'::date);
  -- Matrix `UI-016`: an ordinary Weekly record is not in a Weekly Source group,
  -- the Weekly Source component must not mount, and the legacy Weekly owner
  -- keeps its own heading.  The phase is nevertheless served.
  if not found then return pg_catalog.jsonb_build_object('applicable',false,'scope','WEEKLY')
    ||pg_catalog.jsonb_build_object('lifecycle',
      private.weekly_source_office_lifecycle_phase_v1(
        v_timesheet.timesheet_id,
        pg_catalog.jsonb_build_object('applicable',false,'scope','WEEKLY'))); end if;
  v_policy:=private._weekly_source_effective_policy_v1(
    v_contract.client_id,v_contract.id,v_timesheet.week_ending_date
  );
  perform private.weekly_source_office_authority_v1(
    v_actor,'VIEW_PAY_WORK',v_group.id,v_contract.client_id,v_timesheet.week_ending_date
  );
  v_authority:=case when v_policy->>'authority_mode'='SOURCE_AUTHORITY'
    then 'CLIENT_SYSTEM' else 'SIGNED_TIMESHEET' end;
  v_route:=case when v_policy->>'authority_mode'='TIMESHEET_AUTHORITY'
    then 'TIMESHEETS_CHECKED_WITH_CLIENT'
    when v_group.source_family='NHSP' then 'NHSP' else 'CLIENT_PROVIDED_HOURS' end;
  v_source_expense_policy:=case
    when coalesce((v_policy->>'source_fixed_expenses_enabled')::boolean,false)
      then 'SOURCE_SUPPLIED' else 'SEPARATE_ADDITIONAL_TIMESHEET' end;
  v_add_expense_allowed:=v_source_expense_policy='SEPARATE_ADDITIONAL_TIMESHEET';
  -- Categorisation is a read-only, current-owner fact. It does not authorise
  -- payment, archive the record, or change any invoice movement.
  v_operational_category:=private.weekly_source_timesheet_category_v2(v_timesheet.timesheet_id);

  -- One family resolution, through the one installed adapter, read by both
  -- limbs.  Standing rule 3's fail-closed branch is the adapter's own fallback
  -- plus this explicit cardinality test; it is never a `limit`.  The
  -- `order by … limit 1` below is PRE-EXISTING and is a display choice between
  -- several current publications, not a safety property: nothing is refused on
  -- it, and WP-30 deliberately did not change it.
  v_root_family:=private.weekly_source_invoice_family_timesheet_ids_v1(
    v_timesheet.timesheet_id
  );
  if v_root_family is null or pg_catalog.cardinality(v_root_family)=0 then
    raise exception 'WEEKLY_SOURCE_OFFICE_PRESENTATION_FAMILY_UNRESOLVED'
      using errcode='55000';
  end if;
  select publication.* into v_publication
  from public.weekly_source_projection_publications publication
  where publication.state='CURRENT'
    and (
      exists(select 1 from public.weekly_source_row_timesheet_lineages lineage
        join public.weekly_source_row_resolutions resolution on resolution.id=lineage.row_resolution_id
        join public.weekly_source_upload_rows source_row on source_row.id=resolution.upload_row_id
        where lineage.timesheet_id=any(v_root_family) and source_row.upload_id=publication.upload_id)
      or exists(select 1 from public.weekly_timesheet_source_comparisons comparison
        where comparison.timesheet_id=any(v_root_family)
          and comparison.projection_publication_id=publication.id)
    )
  order by publication.published_at_utc desc,publication.id desc limit 1;
  if found then
    select * into strict v_cycle from public.weekly_source_cycles where id=v_publication.source_cycle_id;
    perform private.weekly_source_query_current_publication_v1(v_cycle.id,v_publication.id);
  elsif v_policy->>'authority_mode'='SOURCE_AUTHORITY'
      and v_timesheet.authorised_at_server is not null
      and not coalesce((
        v_operational_category->>'presentation_category' in ('PROCESSING_DELAYED','WITHDRAWN')
        and v_operational_category->'is_archived'='false'::jsonb
        and v_operational_category#>>'{category_basis,facts,root_timesheet_id}'=v_timesheet.timesheet_id::text
        and v_operational_category#>>'{category_basis,facts,root_version}'=v_timesheet.version::text
      ),false) then
    -- A formerly authorised source week with no current publication is
    -- inconsistent and retains the previous fail-closed behaviour.
    raise exception 'SOURCE_CHECK_IN_PROGRESS' using errcode='55000';
  end if;

  v_submitted_available:=v_timesheet.r2_nurse_key is not null
    and v_timesheet.img_sha256_nurse is not null;
  v_submitted_complete:=v_submitted_available and (
    v_policy->>'authority_mode'='SOURCE_AUTHORITY'
    or (v_timesheet.r2_auth_key is not null and v_timesheet.img_sha256_auth is not null)
  );

  if v_publication.id is not null then
    select pg_catalog.count(*)::integer into v_open_issue_count
    from public.weekly_discrepancy_incidents incident
    join public.weekly_issue_comparison_revisions comparison
      on comparison.id=incident.current_comparison_revision_id
    where incident.source_cycle_id=v_publication.source_cycle_id
      and incident.candidate_id=v_contract.candidate_id
      and incident.client_id=v_contract.client_id and incident.state='OPEN'
      and comparison.projection_publication_id=v_publication.id
      and (comparison.candidate_timesheet_id is null
        or comparison.candidate_timesheet_id=v_timesheet.timesheet_id);

    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'row_key','source-'||source_row.id::text,
      'work_event_id',resolution.work_event_id,
      'day_date',to_char(source_row.work_date,'Dy FMDD Mon YYYY'),
      'hours',to_char(source_row.start_at_local,'HH24:MI')||'-'||to_char(source_row.end_at_local,'HH24:MI'),
      'break_text',coalesce(source_row.break_minutes,0)||' min',
      'state',case when incident.id is null then case when v_submitted_available then 'MATCH' else 'NO_TIMESHEET' end else 'MISMATCH' end,
      'issue_text',case comparison.issue_family
        when 'SOURCE_HOURS_DIFFER' then 'Hours differ'
        when 'SOURCE_MISSING_OR_NOT_AUTHORISED' then case when v_group.source_family='NHSP'
          then 'Missing or not yet authorised' else 'Not included in system hours' end
        when 'HEALTHROSTER_NOT_FINALISED' then 'Not finalised'
        when 'REFERENCE_MISSING' then 'Reference missing' else null end,
      'additional_units',coalesce((select pg_catalog.jsonb_agg(unit.value order by unit.ordinality)
        from pg_catalog.jsonb_array_elements(pg_catalog.jsonb_build_array(
          case when coalesce(economic.hours_day,0)<>0 then pg_catalog.jsonb_build_object('key','day','label','Day','value',economic.hours_day||' hours','state',case when incident.id is null then 'MATCH' else 'MISMATCH' end) end,
          case when coalesce(economic.hours_night,0)<>0 then pg_catalog.jsonb_build_object('key','night','label','Night','value',economic.hours_night||' hours','state',case when incident.id is null then 'MATCH' else 'MISMATCH' end) end,
          case when coalesce(economic.hours_sat,0)<>0 then pg_catalog.jsonb_build_object('key','sat','label','Saturday','value',economic.hours_sat||' hours','state',case when incident.id is null then 'MATCH' else 'MISMATCH' end) end,
          case when coalesce(economic.hours_sun,0)<>0 then pg_catalog.jsonb_build_object('key','sun','label','Sunday','value',economic.hours_sun||' hours','state',case when incident.id is null then 'MATCH' else 'MISMATCH' end) end,
          case when coalesce(economic.hours_bh,0)<>0 then pg_catalog.jsonb_build_object('key','bh','label','Bank holiday','value',economic.hours_bh||' hours','state',case when incident.id is null then 'MATCH' else 'MISMATCH' end) end
        )) with ordinality unit(value,ordinality) where unit.value<>'null'::jsonb),'[]'::jsonb)
    ) order by source_row.work_date,source_row.start_at_local,source_row.id),'[]'::jsonb)
    into v_source_rows
    from public.weekly_source_upload_rows source_row
    join lateral (select current_resolution.* from public.weekly_source_row_resolutions current_resolution
      where current_resolution.upload_row_id=source_row.id order by current_resolution.generation desc,current_resolution.id desc limit 1) resolution
      on resolution.mapping_state='RESOLVED'
    left join public.weekly_source_row_economic_snapshots economic on economic.row_resolution_id=resolution.id
    left join public.weekly_discrepancy_incidents incident
      on incident.source_cycle_id=v_publication.source_cycle_id
     and incident.work_event_id=resolution.work_event_id and incident.state='OPEN'
    left join public.weekly_issue_comparison_revisions comparison
      on comparison.id=incident.current_comparison_revision_id
     and comparison.projection_publication_id=v_publication.id
    where source_row.upload_id=v_publication.upload_id
      and resolution.candidate_id=v_contract.candidate_id
      and resolution.client_id=v_contract.client_id
      and resolution.contract_id=v_contract.id
      and (date_trunc('week',source_row.work_date)::date+6)=v_timesheet.week_ending_date
      and source_row.row_finalisation_state in ('NOT_APPLICABLE','SOURCE_WORKED');
  end if;

  if pg_catalog.jsonb_typeof(v_timesheet.actual_schedule_json)='array' then
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'row_key','submitted-'||entry.ordinality,
      'day_date',to_char((entry.value->>'date')::date,'Dy FMDD Mon YYYY'),
      'hours',coalesce(entry.value->>'start',pg_catalog.chr(8212))||'-'||coalesce(entry.value->>'end',pg_catalog.chr(8212)),
      'break_text',coalesce(entry.value#>>'{break_entry,calculated_break_minutes}',entry.value#>>'{break_entry,break_minutes}',entry.value->>'break_minutes','0')||' min',
      'state',case when exists(
        select 1 from public.weekly_discrepancy_incidents incident
        join public.weekly_issue_comparison_revisions comparison on comparison.id=incident.current_comparison_revision_id
        join public.weekly_work_events work_event on work_event.id=incident.work_event_id
        where incident.state='OPEN' and incident.candidate_id=v_contract.candidate_id
          and incident.client_id=v_contract.client_id
          and work_event.work_date=(entry.value->>'date')::date
          and (comparison.candidate_timesheet_id is null or comparison.candidate_timesheet_id=v_timesheet.timesheet_id)
      ) then 'MISMATCH' else 'MATCH' end,
      'affected',exists(
        select 1 from public.weekly_discrepancy_incidents incident
        join public.weekly_issue_comparison_revisions comparison on comparison.id=incident.current_comparison_revision_id
        join public.weekly_work_events work_event on work_event.id=incident.work_event_id
        where incident.state='OPEN' and incident.candidate_id=v_contract.candidate_id
          and incident.client_id=v_contract.client_id
          and work_event.work_date=(entry.value->>'date')::date
          and (comparison.candidate_timesheet_id is null or comparison.candidate_timesheet_id=v_timesheet.timesheet_id)
      ),
      'issue_text',case when exists(
        select 1 from public.weekly_discrepancy_incidents incident
        join public.weekly_work_events work_event on work_event.id=incident.work_event_id
        where incident.state='OPEN' and incident.candidate_id=v_contract.candidate_id
          and incident.client_id=v_contract.client_id and work_event.work_date=(entry.value->>'date')::date
      ) then 'Hours differ' else null end
    ) order by (entry.value->>'date')::date,entry.ordinality),'[]'::jsonb)
    into v_submitted_rows
    from pg_catalog.jsonb_array_elements(v_timesheet.actual_schedule_json) with ordinality entry(value,ordinality)
    where v_policy->>'authority_mode'='TIMESHEET_AUTHORITY'
       or v_open_issue_count>0;
  end if;

  -- Schema change S8 removed `unique (root_timesheet_id)` from
  -- `public.weekly_exceptional_pay_target_families`, so a bare `select … into`
  -- on that column now returns an ARBITRARY row instead of raising, and this
  -- read decides which protected-hours actions the Office screen offers.
  -- The deterministic fail-closed reader resolves the family through the root
  -- identity and raises on an ambiguous or unresolvable family (WP-06 handoff
  -- N2).  A function call does not set FOUND, so the row itself is tested.
  v_family:=private.weekly_source_target_family_for_root_v1(v_timesheet.timesheet_id);
  if v_family.id is not null then
    v_manage_allowed:=v_family.ownership_state='TARGET_MANAGED'
      or v_family.current_lifecycle_state in ('PROTECTED','WAITING_SOURCE','READY_TO_RECONCILE','ACTION_REQUIRED');
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'item_id',event.id,'work_event_id',event.durable_work_event_id,'state',event.state,
      'primary_action','AMEND_PROTECTED_HOURS',
      'available_actions',pg_catalog.jsonb_build_array('AMEND_PROTECTED_HOURS','WITHDRAW_PROTECTED_HOURS','WAIT_FOR_SOURCE','ACCEPT_SOURCE_AND_RECONCILE','RECORD_NOT_WORKED'),
      'command_payload',pg_catalog.jsonb_build_object(
        'source_cycle_id',approval.source_cycle_id,'candidate_id',approval.candidate_id,
        'client_id',approval.client_id,'contract_id',approval.contract_id,
        'week_ending_date',approval.week_ending,'family_id',v_family.id,
        'work_event_id',event.durable_work_event_id,'evidence_timesheet_id',approval.evidence_timesheet_id,
        'work_date',event.work_date,'start_at_local',to_char(event.start_at_local,'HH24:MI'),
        'end_at_local',to_char(event.end_at_local,'HH24:MI'),'break_minutes',event.break_minutes),
      'schedule',pg_catalog.jsonb_build_object('work_date',event.work_date,
        'start_at_local',to_char(event.start_at_local,'HH24:MI'),
        'end_at_local',to_char(event.end_at_local,'HH24:MI'),'break_minutes',event.break_minutes)
    ) order by event.work_date,event.start_at_local,event.id),'[]'::jsonb)
    into v_manage_items
    from public.weekly_exceptional_pay_family_events event
    join lateral (select max(latest.event_sequence) sequence from public.weekly_exceptional_pay_family_events latest
      where latest.family_id=event.family_id and latest.durable_work_event_id=event.durable_work_event_id) latest
      on latest.sequence=event.event_sequence
    join public.weekly_exceptional_payment_approvals approval on approval.id=event.evidence_approval_id
    where event.family_id=v_family.id and event.state='WAIT';
  end if;

  if v_policy->>'authority_mode'='SOURCE_AUTHORITY' and (
    v_publication.id is not null or exists (
      select 1 from private.weekly_source_manual_reviews manual_review
      where manual_review.contract_id=v_timesheet.contract_id
        and manual_review.work_date between v_timesheet.week_ending_date-6
          and v_timesheet.week_ending_date
        and manual_review.state='OPEN'
    )) then
    v_manage_allowed:=v_open_issue_count>0 or v_family.id is not null or exists (
      select 1 from private.weekly_source_manual_reviews manual_review
      where manual_review.contract_id=v_timesheet.contract_id
        and manual_review.work_date between v_timesheet.week_ending_date-6
          and v_timesheet.week_ending_date
        and manual_review.state='OPEN'
    );
    select pg_catalog.count(*)::integer into v_unprotected_issue_count
    from public.weekly_discrepancy_incidents incident
    join public.weekly_issue_comparison_revisions comparison
      on comparison.id=incident.current_comparison_revision_id
    join public.weekly_work_events issue_event on issue_event.id=incident.work_event_id
    where incident.candidate_id=v_contract.candidate_id
      and incident.client_id=v_contract.client_id
      and incident.state='OPEN'
      and comparison.contract_id=v_timesheet.contract_id
      and issue_event.work_date between v_timesheet.week_ending_date-6
        and v_timesheet.week_ending_date
      and not exists(
        select 1
        from public.weekly_exceptional_pay_family_events protected_event
        where protected_event.family_id=v_family.id
          and protected_event.durable_work_event_id=incident.work_event_id
          and protected_event.state='WAIT'
          and protected_event.event_sequence=(
            select pg_catalog.max(latest.event_sequence)
            from public.weekly_exceptional_pay_family_events latest
            where latest.family_id=protected_event.family_id
              and latest.durable_work_event_id=protected_event.durable_work_event_id
          )
      );
    -- An Office-created review holds this same first-authorisation decision;
    -- it is not an automatically detected comparison incident.
    v_unprotected_issue_count:=v_unprotected_issue_count+(
      select pg_catalog.count(*)::integer
      from private.weekly_source_manual_reviews manual_review
      where manual_review.contract_id=v_timesheet.contract_id
        and manual_review.candidate_id=v_contract.candidate_id
        and manual_review.client_id=v_contract.client_id
        and manual_review.work_date between v_timesheet.week_ending_date-6
          and v_timesheet.week_ending_date
        and manual_review.state='OPEN');
  end if;

  if v_policy->>'authority_mode'='SOURCE_AUTHORITY' then
    select coalesce(pg_catalog.jsonb_agg(row_value order by work_date,start_at_local,row_key),'[]'::jsonb)
    into v_approved_rows
    from (
      select source_row.work_date,source_row.start_at_local,'approved-'||source_row.id::text row_key,
        pg_catalog.jsonb_build_object('row_key','approved-'||source_row.id::text,
          'day_date',to_char(source_row.work_date,'Dy FMDD Mon YYYY'),
          'hours',to_char(source_row.start_at_local,'HH24:MI')||'-'||to_char(source_row.end_at_local,'HH24:MI'),
          'reference_number',source_row.bounded_raw_columns_json->>'reference_number',
          'break_text',coalesce(source_row.break_minutes,0)||' min','state',case when incident.id is null then 'READY' else 'MISMATCH' end,
          'status_text',case when incident.id is null then 'Ready' else 'Needs attention' end) row_value
      from public.weekly_source_upload_rows source_row
      join public.weekly_source_final_revisions final_revision
        on final_revision.upload_id=source_row.upload_id
       and final_revision.source_cycle_id=v_cycle.id
       and final_revision.state='CURRENT'
      join public.weekly_source_billing_movements movement
        on movement.nhsp_upload_row_id=source_row.id
       and movement.final_revision_id=final_revision.id
       and movement.movement_role='POSITIVE'
       and movement.invoice_timesheet_id=any(v_root_family)
      left join public.weekly_discrepancy_incidents incident
        on incident.source_cycle_id=final_revision.source_cycle_id
       and incident.work_event_id=movement.work_event_id and incident.state='OPEN'
      where movement.candidate_id=v_contract.candidate_id and movement.actual_client_id=v_contract.client_id
        and movement.contract_id=v_contract.id
        and (date_trunc('week',source_row.work_date)::date+6)=v_timesheet.week_ending_date
        and source_row.row_finalisation_state in ('NOT_APPLICABLE','SOURCE_WORKED')
        and not exists(select 1 from public.weekly_exceptional_pay_family_events event
          where event.family_id=v_family.id and event.durable_work_event_id=movement.work_event_id and event.state='WAIT'
            and event.event_sequence=(select max(latest.event_sequence) from public.weekly_exceptional_pay_family_events latest
              where latest.family_id=event.family_id and latest.durable_work_event_id=event.durable_work_event_id))
      union all
      select snapshot.work_date,snapshot.start_at_local,'approved-'||snapshot.id::text,
        pg_catalog.jsonb_build_object('row_key','approved-'||snapshot.id::text,
          'day_date',to_char(snapshot.work_date,'Dy FMDD Mon YYYY'),
          'hours',to_char(snapshot.start_at_local,'HH24:MI')||'-'||to_char(snapshot.end_at_local,'HH24:MI'),
          'reference_number',coalesce(source_row.bounded_raw_columns_json->>'reference_number',
            source_row.bounded_raw_columns_json->>'booking_reference'),
          'break_text',snapshot.break_minutes||' min',
          'state',case when incident.id is null then 'READY' else 'MISMATCH' end,
          'status_text',case when incident.id is null then 'Ready' else 'Needs attention' end)
      from public.weekly_source_final_snapshot_lines snapshot
      join public.weekly_source_final_revisions final_revision
        on final_revision.id=snapshot.final_revision_id
       and final_revision.source_cycle_id=v_cycle.id
       and final_revision.state='CURRENT'
      join public.weekly_source_upload_rows source_row on source_row.id=snapshot.upload_row_id
      left join public.weekly_discrepancy_incidents incident
        on incident.source_cycle_id=final_revision.source_cycle_id
       and incident.work_event_id=snapshot.work_event_id and incident.state='OPEN'
      where snapshot.candidate_id=v_contract.candidate_id and snapshot.client_id=v_contract.client_id
        and snapshot.contract_id=v_contract.id
        and (date_trunc('week',snapshot.work_date)::date+6)=v_timesheet.week_ending_date
        and exists(select 1 from public.weekly_source_row_timesheet_lineages lineage
          where lineage.row_resolution_id=snapshot.row_resolution_id
            and lineage.timesheet_id=any(v_root_family))
        and not exists(select 1 from public.weekly_exceptional_pay_family_events event
          where event.family_id=v_family.id and event.durable_work_event_id=snapshot.work_event_id and event.state='WAIT'
            and event.event_sequence=(select max(latest.event_sequence) from public.weekly_exceptional_pay_family_events latest
              where latest.family_id=event.family_id and latest.durable_work_event_id=event.durable_work_event_id))
      union all
      select event.work_date,event.start_at_local,'protected-'||event.id::text,
        pg_catalog.jsonb_build_object('row_key','protected-'||event.id::text,
          'day_date',to_char(event.work_date,'Dy FMDD Mon YYYY'),
          'hours',to_char(event.start_at_local,'HH24:MI')||'-'||to_char(event.end_at_local,'HH24:MI'),
          'break_text',event.break_minutes||' min','state','PROTECTED','status_text','Office-approved hours',
          'context_text','Client system hours are still being checked.')
      from public.weekly_exceptional_pay_family_events event
      where event.family_id=v_family.id and event.state='WAIT'
        and event.event_sequence=(select max(latest.event_sequence) from public.weekly_exceptional_pay_family_events latest
          where latest.family_id=event.family_id and latest.durable_work_event_id=event.durable_work_event_id)
    ) approved;
  else
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'row_key','approved-'||entry.ordinality,'day_date',to_char((entry.value->>'date')::date,'Dy FMDD Mon YYYY'),
      -- `->>` and `||` share a precedence class and associate left to right, so
      -- the unparenthesised form parsed as `entry.value ->> ('start'||'-'||…)`
      -- and raised 42883 for every Timesheet-authority week that had a
      -- submitted schedule, which is exactly matrix rows UI-014 and UI-015.
      'hours',(entry.value->>'start')||'-'||(entry.value->>'end'),
      'break_text',coalesce(entry.value#>>'{break_entry,calculated_break_minutes}',entry.value#>>'{break_entry,break_minutes}',entry.value->>'break_minutes','0')||' min',
      'state','READY','status_text','Ready'
    ) order by (entry.value->>'date')::date,entry.ordinality),'[]'::jsonb)
    into v_approved_rows
    from pg_catalog.jsonb_array_elements(case when pg_catalog.jsonb_typeof(v_timesheet.actual_schedule_json)='array'
      then v_timesheet.actual_schedule_json else '[]'::jsonb end) with ordinality entry(value,ordinality);
  end if;

  select * into v_tsfin from public.timesheets_financials financial
  where financial.timesheet_id=v_timesheet.timesheet_id and financial.is_current and not financial.is_stale
  order by financial.timesheet_version desc,financial.computed_at_utc desc,financial.id desc limit 1;
  if found then
    v_pay_ex:=pg_catalog.round(coalesce(v_tsfin.total_pay_ex_vat,0)+coalesce(v_tsfin.additional_pay_ex_vat,0)
      +coalesce(v_tsfin.expenses_pay_ex_vat,0)+coalesce(v_tsfin.mileage_pay_ex_vat,0),2);
    v_charge_ex:=pg_catalog.round(coalesce(v_tsfin.total_charge_ex_vat,0)+coalesce(v_tsfin.additional_charge_ex_vat,0)
      +coalesce(v_tsfin.expenses_charge_ex_vat,0)+coalesce(v_tsfin.mileage_charge_ex_vat,0),2);
    select finance.vat_rate_pct into v_vat_rate from public.settings_finance_windows finance
    where v_timesheet.week_ending_date>=finance.date_from
      and (finance.date_to is null or v_timesheet.week_ending_date<=finance.date_to)
    order by finance.date_from desc,finance.id desc limit 1;
    v_vat_rate:=coalesce(v_vat_rate,0);
    v_pay_inc:=case when v_tsfin.pay_total_inc_vat_snapshot is not null and v_tsfin.pay_total_inc_vat_snapshot<>0
      then v_tsfin.pay_total_inc_vat_snapshot else v_pay_ex+coalesce(v_tsfin.pay_vat_amount_snapshot,0) end;
    v_charge_vat_base:=case when v_client.vat_chargeable then
      coalesce(v_tsfin.total_charge_ex_vat,0)+coalesce(v_tsfin.additional_charge_ex_vat,0)
      +coalesce(v_tsfin.mileage_charge_ex_vat,0)
      +case when v_source_expense_policy<>'SOURCE_SUPPLIED'
          or coalesce((v_policy->>'source_expense_vat_enabled')::boolean,false)
        then coalesce(v_tsfin.expenses_charge_ex_vat,0) else 0 end
      else 0 end;
    v_charge_inc:=pg_catalog.round(v_charge_ex+(v_charge_vat_base*v_vat_rate/100),2);
    v_totals_complete:=true;
  end if;

  if v_policy->>'authority_mode'='TIMESHEET_AUTHORITY' then
    if not v_submitted_complete then
      v_comparison_state:='WAITING_FOR_COMPLETE_TIMESHEET';
      v_authorise_allowed:=false;
      v_blocked_reason:='Waiting for the worker and manager to complete the Timesheet.';
    else
      if v_publication.id is not null then
        select pg_catalog.count(*)::integer into v_reference_blocker_count
        from public.weekly_timesheet_source_comparisons comparison
        where comparison.timesheet_id=v_timesheet.timesheet_id
          and comparison.projection_publication_id=v_publication.id
          and comparison.comparison_state<>'EXACT_MATCH';
      elsif v_require_reference_to_pay then
        v_reference_blocker_count:=1;
      end if;
      v_comparison_state:=case when v_reference_blocker_count>0 then 'MISMATCH' else 'MATCH' end;
      v_authorise_allowed:=not v_require_reference_to_pay or v_reference_blocker_count=0;
      if not v_authorise_allowed then v_blocked_reason:='The required client reference is not ready yet.'; end if;
    end if;
  else
    v_comparison_state:=case when v_publication.id is null then 'SOURCE_PENDING'
      when not v_submitted_available then 'NO_TIMESHEET'
      when v_open_issue_count>0 then 'MISMATCH' else 'MATCH' end;
    v_authorise_allowed:=v_publication.id is not null and v_unprotected_issue_count=0;
    if not v_authorise_allowed then
      v_blocked_reason:=case when v_publication.id is null
        then 'Waiting for final source hours before authorising.'
        else 'Resolve the hours needing attention before authorising.' end;
    end if;
  end if;

  -- =======================================================================
  -- Gate 9 items G9-1, G9-3 and G9-5.
  --
  -- The phase, the heading and the permitted new actions are decided HERE and
  -- served; the browser performs no financial inference (`24 section 15`,
  -- contract section 13).  Every schedule below is either an explicit
  -- `available:true` with the server's own rows, or an explicit
  -- `available:false` with a machine reason.  There is no third case, so a
  -- missing figure can never be read as a zero.
  -- =======================================================================
  v_lifecycle:=private.weekly_source_office_lifecycle_phase_v1(
    v_timesheet.timesheet_id,
    pg_catalog.jsonb_build_object(
      'applicable',true,'scope','WEEKLY',
      'authority_mode',v_policy->>'authority_mode',
      'comparison_state',v_comparison_state,
      'protected_wait_items',pg_catalog.jsonb_array_length(v_manage_items),
      'source_fixed_expenses',v_source_expense_policy='SOURCE_SUPPLIED',
      'submitted_available',v_submitted_available));
  v_candidate_lifecycle:=private.weekly_source_office_candidate_phase_v1(
    v_timesheet.timesheet_id,pg_catalog.transaction_timestamp());
  v_phase:=v_lifecycle->>'server_phase';
  v_settlement:=v_lifecycle->'settlement';
  v_first_authorisation:=v_phase in (
    'FIRST_AUTHORISATION_PENDING','FIRST_AUTHORISATION_PENDING_NO_SUBMISSION',
    'FIRST_AUTHORISATION_PENDING_MISMATCH','FIRST_ALTERNATE_HOURS_PENDING',
    'TIMESHEET_AUTHORITY_MATCH','TIMESHEET_AUTHORITY_MISMATCH');

  -- The current effective entitlement, read once from interface I-7 and never
  -- recomputed here.  `Approved hours` and `Currently approved hours` are the
  -- SAME server fact under two policy names, so they are built from this one
  -- read and cannot disagree.
  v_inventory:=private.weekly_source_effective_inventory_v1(v_timesheet.timesheet_id);
  v_currently_approved:=case
    when coalesce((v_inventory->>'ok')::boolean,false) is not true
      then private.weekly_source_office_schedule_absent_v1(
        coalesce(v_inventory->>'code','EFFECTIVE_INVENTORY_UNAVAILABLE'))
    when coalesce(v_lifecycle->>'authorisation_state','UNKNOWN')<>'AUTHORISED'
      then private.weekly_source_office_schedule_absent_v1('NO_CURRENT_AUTHORISED_ENTITLEMENT')
    else private.weekly_source_office_schedule_from_components_v1(
      coalesce(v_inventory->'components','[]'::jsonb),
      'EFFECTIVE_INVENTORY_'||coalesce(v_inventory->>'authority','UNKNOWN')) end;

  -- `Hours paid`, `Hours paid to date` and `current paid hours` come only from
  -- the immutable per-root, per-shift settlement allocation (Gate 9 G9-2).
  -- Never from a currency-to-hours calculation, never from the last-settled
  -- cache, and never guessed when the evidence is unreadable.
  v_paid:=private.weekly_source_office_schedule_from_allocation_v1(
    v_settlement,'WEEKLY_SOURCE_SETTLEMENT_ALLOCATION');
  v_next:=private.weekly_source_office_next_owner_v1();
  if v_next then
    v_next_evidence:=jsonb_build_object('contract','WEEKLY_SOURCE_OFFICE_NEXT_EVIDENCE_V1',
      'components',private.weekly_source_office_next_paid_evidence_page_v1(
        v_actor,v_timesheet.timesheet_id,'COMPONENTS'),
      'active_holds',private.weekly_source_office_next_paid_evidence_page_v1(
        v_actor,v_timesheet.timesheet_id,'ACTIVE_HOLDS'));
    -- The strict Worker adapter replaces only these informational schedules
    -- after validating the complete contextual pages. No legacy fallback.
    v_paid:=private.weekly_source_office_schedule_absent_v1('ROOT_ACTIVITY_NOT_INDEXED');
  end if;

  v_schedules:=pg_catalog.jsonb_build_object(
    'submitted',private.weekly_source_office_schedule_from_actual_v1(
      v_timesheet.actual_schedule_json),
    'latest_source',case when v_publication.id is null
      then private.weekly_source_office_schedule_absent_v1('NO_CURRENT_SOURCE_PUBLICATION')
      else private.weekly_source_office_schedule_from_rows_v1(
        v_source_rows,'WEEKLY_SOURCE_LATEST_SOURCE') end,
    'hours_to_authorise',case when v_first_authorisation and
      (v_authority<>'CLIENT_SYSTEM' or v_publication.id is not null)
      then private.weekly_source_office_schedule_from_rows_v1(
        v_approved_rows,'WEEKLY_SOURCE_PROPOSED_FIRST_ENTITLEMENT')
      else private.weekly_source_office_schedule_absent_v1(
        'NOT_IN_A_FIRST_AUTHORISATION_PHASE') end,
    'currently_approved',v_currently_approved,
    'approved',v_currently_approved,
    'processing',case when not v_next and v_phase='PAYMENT_PROCESSING' then v_currently_approved
      when v_next then private.weekly_source_office_schedule_absent_v1('ROOT_ACTIVITY_NOT_INDEXED')
      else private.weekly_source_office_schedule_absent_v1('NO_PAYMENT_IN_FLIGHT') end,
    'paid_to_date',v_paid,
    'current_paid',v_paid);

  v_record_version:=pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_OFFICE_PRESENTATION_VERSION_V1',pg_catalog.jsonb_build_object(
      'timesheet_id',v_timesheet.timesheet_id,'timesheet_version',v_timesheet.version,
      'publication_id',v_publication.id,'publication_issue_hash',case when v_publication.id is null then null else pg_catalog.encode(v_publication.issue_set_hash,'hex') end,
      'timesheet_financial_id',v_tsfin.id,
      'timesheet_financial_evidence_hash',case when v_tsfin.id is null then null
        else pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
          'WEEKLY_SOURCE_OFFICE_TIMESHEET_FINANCIAL_VERSION_V1',to_jsonb(v_tsfin)
        ),'hex') end,
      'protected_family_id',v_family.id,'protected_family_version',v_family.bound_version,
      'comparison_state',v_comparison_state,
      -- Gate 9: the record version must move when the PHASE moves, or a stale
      -- screen could post an action the new phase no longer permits.
      'lifecycle_ok',v_lifecycle->'ok',
      'server_phase',v_lifecycle->'server_phase',
      'lifecycle_errors',v_lifecycle->'errors',
      'proposal_state',v_lifecycle#>'{proposal,state}',
      'proposal_bundle_revision',v_lifecycle#>'{proposal,bundle_revision}',
      'settlement_state',v_settlement->'state',
      'settlement_last_settled_at_utc',v_settlement->'last_settled_at_utc',
      'operational_category_basis',v_operational_category->'category_basis'
    )),'hex');

  return pg_catalog.jsonb_build_object(
    'applicable',true,'contract','WEEKLY_SOURCE_OFFICE_PRESENTATION_V1','scope','WEEKLY',
    'root_timesheet_id',v_timesheet.timesheet_id,
    'record_version',v_record_version,'freshness','CURRENT','route',v_route,'authority',v_authority,
    'operational_category',v_operational_category,
    'source_expense_policy',v_source_expense_policy,
    'submitted_timesheet_available',v_submitted_available,'submitted_timesheet_complete',v_submitted_complete,
    'comparison',pg_catalog.jsonb_build_object('state',v_comparison_state,
      'source_rows',v_source_rows,'submitted_rows',v_submitted_rows,'rows','[]'::jsonb),
    'approved_rows',v_approved_rows,
    'totals',pg_catalog.jsonb_build_object('complete',v_totals_complete,
      'charge_excluding_vat',private.weekly_source_office_money_text_v1(v_charge_ex),
      'charge_including_vat',private.weekly_source_office_money_text_v1(v_charge_inc),
      'pay_excluding_vat',private.weekly_source_office_money_text_v1(v_pay_ex),
      'pay_including_vat',private.weekly_source_office_money_text_v1(v_pay_inc)),
    'action_state',pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
      'authorise_allowed',v_authorise_allowed and private.weekly_source_pay_actor_allowed_v1(v_actor),'blocked_reason',v_blocked_reason,
      'manage_approved_hours_allowed',v_manage_allowed,
      'manage_approved_hours',case when v_manage_allowed then pg_catalog.jsonb_build_object(
        'endpoint','/api/weekly-source/v1/commands','expected_record_version',v_record_version,
        'new_item',pg_catalog.jsonb_build_object('allowed',true,'action','APPROVE_PROTECTED_HOURS',
          'command_payload',pg_catalog.jsonb_build_object('source_cycle_id',v_cycle.id,
            'candidate_id',v_contract.candidate_id,'client_id',v_contract.client_id,
            'contract_id',v_contract.id,'week_ending_date',v_timesheet.week_ending_date,
            'evidence_timesheet_id',v_timesheet.timesheet_id)),
        'items',v_manage_items) else null end,
      'add_additional_expense_timesheet_allowed',v_add_expense_allowed and exists(select 1 from public.tms_users actor
        where actor.id=v_actor and lower(btrim(coalesce(actor.role,'')))='admin')
    ))
    -- Gate 9 item G9-4.  `unauthorise_allowed`, the W1 to W9 refusal kind and
    -- its permanence, and the withdrawn state.  None of those checks is
    -- computed here: the bridge calls the withdrawal owner's own read-only
    -- availability function and reports its verdict (`proof/36 section 3` and
    -- section 4).  Merged AFTER `jsonb_strip_nulls` so that every member
    -- survives and so the existing members are byte-identical to before.
    ||private.weekly_source_office_unauthorise_action_state_v1(v_timesheet.timesheet_id)
  )
  -- Gate 9 G9-1: the server-owned phase, its heading, its immutable schedules
  -- and its permitted new actions.  Gate 9 G9-3: the proposal, carried on the
  -- lifecycle object because a proposal IS part of the phase, and repeated at
  -- the top level for the decision surface.  Gate 9 G9-5: `UI-022` arrives as a
  -- phase, and the invoice movement history is returned SEPARATELY so it can
  -- never be mistaken for Candidate paid hours.
  ||pg_catalog.jsonb_build_object(
    'lifecycle',v_lifecycle||pg_catalog.jsonb_build_object('schedules',v_schedules),
    'candidate_lifecycle',v_candidate_lifecycle,
    'proposal',v_lifecycle->'proposal',
    'invoice_movement_history',v_lifecycle->'invoice_movements')
  ||case when v_next then jsonb_build_object('next_paid_evidence',v_next_evidence) else '{}'::jsonb end;
exception
  when invalid_text_representation or numeric_value_out_of_range or datetime_field_overflow then
    raise exception 'WEEKLY_SOURCE_PRESENTATION_REQUEST_INVALID' using errcode='22023';
end;
$function$;


-- Current TEST source: supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql
-- Scope: WP-PP actor/read-view changes only.
create or replace function public.weekly_source_office_notifications_list_v1(
  p_request jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_limit integer;
  v_open_only boolean;
  v_rows jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in ('actor_user_id','limit','open_only')) then
    raise exception 'WEEKLY_SOURCE_OFFICE_NOTICE_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_limit:=coalesce(nullif(p_request->>'limit','')::integer,100);
    v_open_only:=coalesce((p_request->>'open_only')::boolean,true);
  exception when others then
    raise exception 'WEEKLY_SOURCE_OFFICE_NOTICE_REQUEST_INVALID' using errcode='22023';
  end;
  if v_limit<1 or v_limit>500 or not exists(
    select 1 from public.tms_users u where u.id=v_actor and u.is_active
      and (pg_catalog.lower(pg_catalog.btrim(u.role)) in ('admin','user')
        or private.weekly_source_pay_actor_allowed_v1(v_actor))
  ) then
    raise exception 'WEEKLY_SOURCE_OFFICE_NOTICE_FORBIDDEN' using errcode='42501';
  end if;
  select coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(row_data) order by row_data.created_at_utc desc,row_data.id),'[]'::jsonb)
  into v_rows
  from (
    select notice.id,notice.event_kind,notice.issue_id,notice.issue_generation,
      notice.payload_json,notice.operational_state,notice.read_at_utc,notice.created_at_utc
    from public.office_action_notifications notice
    where notice.recipient_user_id=v_actor
      and (not v_open_only or notice.operational_state='OPEN')
    order by notice.created_at_utc desc,notice.id
    limit v_limit
  ) row_data;
  return pg_catalog.jsonb_build_object('ok',true,'notifications',v_rows);
end;
$function$;


alter function private.weekly_source_pay_actor_allowed_v1(uuid) owner to postgres;
revoke all on function private.weekly_source_pay_actor_allowed_v1(uuid) from public,anon,authenticated;
grant execute on function private.weekly_source_pay_actor_allowed_v1(uuid) to service_role;

alter function private.weekly_source_office_authority_v1(uuid,text,uuid,uuid,date) owner to postgres;
revoke all on function private.weekly_source_office_authority_v1(uuid,text,uuid,uuid,date) from public,anon,authenticated;
grant execute on function private.weekly_source_office_authority_v1(uuid,text,uuid,uuid,date) to service_role;

alter function public.weekly_exceptional_pay_record_c1_unknown_v1(jsonb) owner to postgres;
revoke all on function public.weekly_exceptional_pay_record_c1_unknown_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_exceptional_pay_record_c1_unknown_v1(jsonb) to service_role;

alter function public.weekly_exceptional_pay_record_c1_checkpoint_v1(jsonb) owner to postgres;
revoke all on function public.weekly_exceptional_pay_record_c1_checkpoint_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_exceptional_pay_record_c1_checkpoint_v1(jsonb) to service_role;

alter function public.weekly_exceptional_pay_record_c1_recovery_v1(jsonb) owner to postgres;
revoke all on function public.weekly_exceptional_pay_record_c1_recovery_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_exceptional_pay_record_c1_recovery_v1(jsonb) to service_role;

alter function public.weekly_exceptional_pay_read_c1_request_v1(jsonb) owner to postgres;
revoke all on function public.weekly_exceptional_pay_read_c1_request_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_exceptional_pay_read_c1_request_v1(jsonb) to service_role;

alter function public.weekly_exceptional_pay_complete_c1_publication_v1(jsonb) owner to postgres;
revoke all on function public.weekly_exceptional_pay_complete_c1_publication_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_exceptional_pay_complete_c1_publication_v1(jsonb) to service_role;

alter function public.weekly_exceptional_pay_action_publication_status_v1(jsonb) owner to postgres;
revoke all on function public.weekly_exceptional_pay_action_publication_status_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_exceptional_pay_action_publication_status_v1(jsonb) to service_role;

alter function public.weekly_exceptional_pay_complete_local_v1(jsonb) owner to postgres;
revoke all on function public.weekly_exceptional_pay_complete_local_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_exceptional_pay_complete_local_v1(jsonb) to service_role;

alter function public.weekly_source_office_workspace_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_office_workspace_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_office_workspace_v1(jsonb) to service_role;

alter function public.weekly_source_manual_review_open_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_manual_review_open_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_manual_review_open_v1(jsonb) to service_role;

alter function public.weekly_source_office_authorise_scope_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_office_authorise_scope_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_office_authorise_scope_v1(jsonb) to service_role;

alter function public.weekly_source_workspace_scopes_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_workspace_scopes_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_workspace_scopes_v1(jsonb) to service_role;

alter function public.weekly_source_combined_review_workspace_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_combined_review_workspace_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_combined_review_workspace_v1(jsonb) to service_role;

alter function public.weekly_source_office_timesheet_presentation_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_office_timesheet_presentation_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_office_timesheet_presentation_v1(jsonb) to service_role;

alter function public.weekly_source_office_notifications_list_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_office_notifications_list_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_office_notifications_list_v1(jsonb) to service_role;

notify pgrst,'reload schema';
commit;
