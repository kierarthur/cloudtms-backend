-- Final stable-operation authority for the import-review hard cutover.
-- The request envelope contains only database-issued action/correction identity.
-- Every financial policy envelope is rebuilt and frozen here from DB evidence.

create or replace function public._import_apply_operation_claim_core_v2(
  p_operation_id uuid,p_import_id uuid,p_source_system public.hr_source_enum,p_import_revision text,
  p_request_hash text,p_actor_user_id uuid,p_request_envelope jsonb
)
returns jsonb language plpgsql security definer set search_path to 'public','extensions','pg_temp' as $function$
declare
  v_op public.import_apply_operations%rowtype;
  v_same public.import_apply_operations%rowtype;
  v_hash text;
  v_envelope jsonb:=coalesce(p_request_envelope,'{}');
  v_units jsonb;
  v_unit jsonb;
  v_policy jsonb;
  v_canonical_unit jsonb;
  v_canonical_units jsonb:='[]'::jsonb;
  v_preview_consequences jsonb:='[]'::jsonb;
  v_contract_payload jsonb;
  v_contract jsonb;
  v_contract_fingerprint text;
  v_response jsonb;
  v_now timestamptz:=statement_timestamp();
  v_action text;
  v_shape text;
  v_expected_roles jsonb;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if p_operation_id is null or p_import_id is null or p_source_system is null
     or length(btrim(coalesce(p_import_revision,''))) not between 1 and 512
     or jsonb_typeof(v_envelope)<>'object' or pg_column_size(v_envelope)>1048576 then
    raise exception 'IMPORT_OPERATION_V2_INPUT_INVALID' using errcode='22023';
  end if;
  if v_envelope->>'schema_version' is distinct from 'IMPORT_REVIEW_APPLY_V1'
     or nullif(v_envelope->>'import_id','')::uuid is distinct from p_import_id
     or jsonb_typeof(v_envelope->'selected_action_ids')<>'array'
     or jsonb_typeof(v_envelope->'reference_invalidation_action_ids')<>'array' then
    raise exception 'IMPORT_OPERATION_V2_REVIEW_ENVELOPE_INVALID' using errcode='22023';
  end if;
  v_units:=coalesce(v_envelope->'correction_units','[]'::jsonb);
  if jsonb_typeof(v_units)<>'array' or jsonb_array_length(v_units)>500 then
    raise exception 'IMPORT_OPERATION_V2_CORRECTION_SCOPE_LIMIT' using errcode='22023';
  end if;
  if exists(
    select 1 from jsonb_array_elements(v_units) u
    where jsonb_typeof(u)<>'object'
       or nullif(u->>'root_timesheet_id','') is null
       or (u->>'root_timesheet_id') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       or nullif(btrim(u->>'source_row_key'),'') is null
       or upper(btrim(coalesce(u->>'correction_action',''))) not in ('CHANGED_HOURS','CANCELLATION')
       or upper(btrim(coalesce(u->>'correction_shape',''))) not in ('REVERSAL_ONLY','REVERSAL_REPLACEMENT')
       or (upper(btrim(u->>'correction_action'))='CANCELLATION' and upper(btrim(u->>'correction_shape'))<>'REVERSAL_ONLY')
       or (upper(btrim(u->>'correction_action'))='CHANGED_HOURS' and upper(btrim(u->>'correction_shape'))<>'REVERSAL_REPLACEMENT')
  ) then raise exception 'IMPORT_OPERATION_V2_CORRECTION_UNIT_INVALID' using errcode='22023'; end if;
  if (select count(*) from jsonb_array_elements(v_units))<>(
    select count(distinct concat_ws('|',u->>'root_timesheet_id',u->>'source_row_key',upper(u->>'correction_action'),upper(u->>'correction_shape')))
    from jsonb_array_elements(v_units) u
  ) then raise exception 'IMPORT_OPERATION_V2_CORRECTION_UNIT_DUPLICATE' using errcode='22023'; end if;

  v_hash:=public._import_review_hash_v1(v_envelope::text);
  if lower(btrim(coalesce(p_request_hash,'')))<>v_hash then
    raise exception 'IMPORT_OPERATION_V2_REQUEST_HASH_MISMATCH' using errcode='22023',
      detail=jsonb_build_object('server_request_hash',v_hash)::text;
  end if;
  if not exists(select 1 from public.hr_imports i where i.id=p_import_id and i.source_system=p_source_system) then
    raise exception 'IMPORT_OPERATION_V2_IMPORT_OR_SOURCE_MISMATCH' using errcode='P0002';
  end if;

  perform pg_advisory_xact_lock(hashtextextended('IMPORT_OPERATION_V2|'||p_operation_id::text,21072026));
  select * into v_op from public.import_apply_operations where id=p_operation_id for update;
  if found then
    if v_op.import_id<>p_import_id or v_op.source_system<>p_source_system
       or v_op.import_revision<>btrim(p_import_revision) or v_op.request_hash<>v_hash
       or v_op.actor_user_id<>p_actor_user_id
       or v_op.response_json->'request_envelope' is distinct from v_envelope then
      raise exception 'IMPORT_OPERATION_V2_IDEMPOTENCY_CONFLICT' using errcode='23505';
    end if;
    return jsonb_build_object('ok',true,'replay',true,'operation_id',v_op.id,'state',v_op.state,
      'request_hash',v_op.request_hash,'source_committed',v_op.committed_at_utc is not null,'response_json',v_op.response_json);
  end if;
  select * into v_same from public.import_apply_operations
  where import_id=p_import_id and import_revision=btrim(p_import_revision) and request_hash=v_hash for update;
  if found then
    raise exception 'IMPORT_OPERATION_V2_OPERATION_ID_MISMATCH_FOR_EXISTING_REQUEST' using errcode='23505',
      detail=jsonb_build_object('existing_operation_id',v_same.id)::text;
  end if;

  for v_unit in select value from jsonb_array_elements(v_units) loop
    v_action:=upper(btrim(v_unit->>'correction_action'));
    v_shape:=upper(btrim(v_unit->>'correction_shape'));
    v_expected_roles:=case when v_shape='REVERSAL_ONLY' then jsonb_build_array('REVERSAL')
      else jsonb_build_array('REVERSAL','REPLACEMENT') end;
    v_policy:=public._ctms_correction_financials_policy_build_v2(
      (v_unit->>'root_timesheet_id')::uuid,p_import_id,btrim(v_unit->>'source_row_key'),v_action,v_shape,
      p_operation_id,v_hash,v_now,true,32);
    v_canonical_unit:=jsonb_build_object(
      'action_id',v_unit->>'action_id','root_timesheet_id',v_policy->>'root_timesheet_id',
      'correction_chain_id',v_policy->>'correction_chain_id','source_shift_id',v_policy#>>'{classification,source_shift_id}',
      'source_row_key',btrim(v_unit->>'source_row_key'),'correction_action',v_action,'correction_shape',v_shape,
      'expected_member_roles',v_expected_roles,'expected_member_count',jsonb_array_length(v_expected_roles),
      'settings_snapshot',v_policy->'settings_snapshot','policy_envelope_fingerprint',v_policy->>'envelope_fingerprint',
      'policy_envelope',v_policy);
    v_canonical_units:=v_canonical_units||jsonb_build_array(v_canonical_unit);
  end loop;

  if jsonb_array_length(v_canonical_units)>0 then
    select coalesce(jsonb_agg(jsonb_build_object(
      'action_id',u->>'action_id','root_timesheet_id',u->>'root_timesheet_id','source_row_key',u->>'source_row_key',
      'correction_action',u->>'correction_action','correction_shape',u->>'correction_shape',
      'expected_member_roles',u->'expected_member_roles','expected_member_count',u->'expected_member_count',
      'reversal',jsonb_build_object('applicable',coalesce((u#>>'{policy_envelope,reversal,applicable}')::boolean,false),
        'setting',u#>>'{policy_envelope,reversal,setting}','setting_source',u#>>'{policy_envelope,reversal,setting_source}',
        'tsfin_policy',u#>'{policy_envelope,reversal,tsfin_policy}','invoice_policy',u#>'{policy_envelope,reversal,invoice_policy}',
        'leg_fingerprint',u#>>'{policy_envelope,reversal,leg_fingerprint}'),
      'replacement',jsonb_build_object('applicable',coalesce((u#>>'{policy_envelope,replacement,applicable}')::boolean,false),
        'setting',u#>>'{policy_envelope,replacement,setting}','setting_source',u#>>'{policy_envelope,replacement,setting_source}',
        'tsfin_policy',u#>'{policy_envelope,replacement,tsfin_policy}','invoice_policy',u#>'{policy_envelope,replacement,invoice_policy}',
        'leg_fingerprint',u#>>'{policy_envelope,replacement,leg_fingerprint}'),
      'policy_envelope_fingerprint',u->>'policy_envelope_fingerprint') order by u->>'action_id'),'[]'::jsonb)
    into v_preview_consequences from jsonb_array_elements(v_canonical_units) u;
    v_contract_payload:=jsonb_build_object(
      'schema_version','IMPORT_CORRECTION_OPERATION_V2','route_family','IMPORT_AUTHORITATIVE',
      'operation_id',p_operation_id,'import_id',p_import_id,'source_system',p_source_system,
      'import_revision',btrim(p_import_revision),'request_hash',v_hash,'request_seed_fingerprint',v_hash,
      'operation_at_utc',v_now,'operation_date_london',(v_now at time zone 'Europe/London')::date,
      'requested_timesheet_ids',(select coalesce(jsonb_agg(distinct u->>'root_timesheet_id'),'[]'::jsonb) from jsonb_array_elements(v_canonical_units) u),
      'expanded_timesheet_ids',(select coalesce(jsonb_agg(distinct u->>'root_timesheet_id'),'[]'::jsonb) from jsonb_array_elements(v_canonical_units) u),
      'correction_units',v_canonical_units);
    v_contract_fingerprint:=encode(extensions.digest(convert_to(v_contract_payload::text,'UTF8'),'sha256'),'hex');
    v_contract:=v_contract_payload||jsonb_build_object('operation_contract_fingerprint',v_contract_fingerprint);
  end if;

  v_response:=jsonb_build_object('schema_version','IMPORT_APPLY_OPERATION_V2','request_envelope',v_envelope,'server_request_hash',v_hash);
  if v_contract is not null then
    v_response:=v_response||jsonb_build_object('correction_operation_at_utc',v_now,
      'correction_operation_date_london',(v_now at time zone 'Europe/London')::date,
      'preview_consequences',v_preview_consequences,'correction_operation_contract',v_contract);
  end if;
  if pg_column_size(v_response)>4194304 then raise exception 'IMPORT_OPERATION_V2_RESPONSE_TOO_LARGE' using errcode='54000'; end if;
  insert into public.import_apply_operations(id,import_id,source_system,import_revision,request_hash,actor_user_id,state,response_json)
  values(p_operation_id,p_import_id,p_source_system,btrim(p_import_revision),v_hash,p_actor_user_id,'PREPARED',v_response)
  returning * into v_op;
  return jsonb_build_object('ok',true,'replay',false,'operation_id',v_op.id,'state',v_op.state,'request_hash',v_op.request_hash,
    'source_committed',false,'response_json',v_op.response_json);
end $function$;

create or replace function public.import_apply_operation_claim_v2(
  p_operation_id uuid,p_import_id uuid,p_source_system public.hr_source_enum,p_import_revision text,
  p_request_hash text,p_actor_user_id uuid,p_request_envelope jsonb default '{}'::jsonb
)
returns jsonb language plpgsql security definer set search_path to 'public','pg_temp' as $function$
begin
  return public._import_apply_operation_claim_core_v2(p_operation_id,p_import_id,p_source_system,p_import_revision,
    p_request_hash,p_actor_user_id,p_request_envelope);
end $function$;

revoke all on function public._import_apply_operation_claim_core_v2(uuid,uuid,public.hr_source_enum,text,text,uuid,jsonb) from public,anon,authenticated,service_role;
revoke all on function public.import_apply_operation_claim_v2(uuid,uuid,public.hr_source_enum,text,text,uuid,jsonb) from public,anon,authenticated;
grant execute on function public.import_apply_operation_claim_v2(uuid,uuid,public.hr_source_enum,text,text,uuid,jsonb) to service_role;
