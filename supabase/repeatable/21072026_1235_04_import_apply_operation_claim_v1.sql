-- Durable import apply operation claim.
--
-- p_response_seed_json must contain the exact requested/expanded timesheet
-- scope and a correction_units array. Each correction unit supplies only its
-- source identity (root timesheet, source row key, action and shape). The
-- database validates that identity and builds the complete policy envelope;
-- callers cannot seed settings, evidence, roles, counts or fingerprints. The
-- supplied request hash is audit metadata only; durable operation identity is
-- the database-computed fingerprint of the validated canonical seed.

create or replace function public.import_apply_operation_claim_v1(
  p_import_id uuid,
  p_source_system public.hr_source_enum,
  p_import_revision text,
  p_request_hash text,
  p_actor_user_id uuid,
  p_response_seed_json jsonb default '{}'::jsonb,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
declare
  -- p_now_utc is retained only for signature compatibility. Financial policy
  -- time is always server-owned and cannot be selected by an RPC caller.
  v_now timestamptz := statement_timestamp();
  v_import_revision text := btrim(coalesce(p_import_revision, ''));
  -- The supplied hash is retained only as bounded caller audit metadata.
  -- Database identity is computed below from the validated canonical scope.
  v_caller_request_hash text := btrim(coalesce(p_request_hash, ''));
  v_canonical_request_hash text;
  v_seed jsonb := coalesce(p_response_seed_json, '{}'::jsonb);
  v_import public.hr_imports%rowtype;
  v_operation public.import_apply_operations%rowtype;
  v_operation_id uuid;
  v_requested_ids uuid[] := array[]::uuid[];
  v_expanded_ids uuid[] := array[]::uuid[];
  v_units_input jsonb;
  v_unit_input jsonb;
  v_canonical_seed jsonb;
  v_seed_fingerprint text;
  v_envelope jsonb;
  v_canonical_unit jsonb;
  v_canonical_units jsonb := '[]'::jsonb;
  v_preview_consequences jsonb := '[]'::jsonb;
  v_contract_payload jsonb;
  v_contract jsonb;
  v_contract_fingerprint text;
  v_existing_contract jsonb;
  v_existing_contract_fingerprint text;
  v_inserted boolean := false;
  v_reset_for_retry boolean := false;
  v_lock_acquired boolean;
  v_action text;
  v_root_id uuid;
  v_source_row_key text;
  v_correction_action text;
  v_shape text;
  v_expected_roles jsonb;
begin
  if p_import_id is null or p_source_system is null or p_actor_user_id is null then
    raise exception 'IMPORT_OPERATION_REQUIRED_IDENTITY_MISSING'
      using errcode = '22023';
  end if;
  if char_length(v_import_revision) < 1 or char_length(v_import_revision) > 512 then
    raise exception 'IMPORT_OPERATION_REVISION_LENGTH_INVALID' using errcode = '22023';
  end if;
  if char_length(v_caller_request_hash) < 16 or char_length(v_caller_request_hash) > 256 then
    raise exception 'IMPORT_OPERATION_REQUEST_HASH_LENGTH_INVALID' using errcode = '22023';
  end if;
  if jsonb_typeof(v_seed) <> 'object' then
    raise exception 'IMPORT_OPERATION_RESPONSE_SEED_MUST_BE_OBJECT' using errcode = '22023';
  end if;
  if octet_length(v_seed::text) > 1048576 then
    raise exception 'IMPORT_OPERATION_RESPONSE_SEED_TOO_LARGE'
      using errcode = '22023', detail = jsonb_build_object('max_bytes',1048576)::text;
  end if;

  perform 1 from public.tms_users actor
  where actor.id = p_actor_user_id and coalesce(actor.is_active,false);
  if not found then
    raise exception 'IMPORT_OPERATION_ACTOR_INVALID' using errcode = '42501';
  end if;

  select hi.* into v_import from public.hr_imports hi where hi.id = p_import_id;
  if not found then
    raise exception 'IMPORT_OPERATION_IMPORT_NOT_FOUND' using errcode = 'P0002';
  end if;
  if v_import.source_system is distinct from p_source_system then
    raise exception 'IMPORT_OPERATION_SOURCE_SYSTEM_MISMATCH' using errcode = '22023';
  end if;

  if jsonb_typeof(v_seed -> 'requested_timesheet_ids') <> 'array'
     or jsonb_typeof(v_seed -> 'expanded_timesheet_ids') <> 'array'
     or jsonb_typeof(v_seed -> 'correction_units') <> 'array' then
    raise exception 'IMPORT_OPERATION_CONTRACT_ARRAYS_REQUIRED'
      using errcode = '22023';
  end if;
  if jsonb_array_length(v_seed -> 'requested_timesheet_ids') > 100
     or jsonb_array_length(v_seed -> 'expanded_timesheet_ids') > 100
     or jsonb_array_length(v_seed -> 'correction_units') > 100 then
    raise exception 'IMPORT_OPERATION_SCOPE_LIMIT_EXCEEDED' using errcode = '22023';
  end if;
  if exists (
    select 1 from jsonb_array_elements(v_seed -> 'requested_timesheet_ids') e
    where jsonb_typeof(e) <> 'string'
       or trim(both '"' from e::text) !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ) or exists (
    select 1 from jsonb_array_elements(v_seed -> 'expanded_timesheet_ids') e
    where jsonb_typeof(e) <> 'string'
       or trim(both '"' from e::text) !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ) then
    raise exception 'IMPORT_OPERATION_SCOPE_UUID_INVALID' using errcode = '22023';
  end if;

  select coalesce(array_agg(distinct value::uuid order by value::uuid),array[]::uuid[])
  into v_requested_ids
  from jsonb_array_elements_text(v_seed -> 'requested_timesheet_ids') value;
  select coalesce(array_agg(distinct value::uuid order by value::uuid),array[]::uuid[])
  into v_expanded_ids
  from jsonb_array_elements_text(v_seed -> 'expanded_timesheet_ids') value;

  if cardinality(v_requested_ids) <> jsonb_array_length(v_seed -> 'requested_timesheet_ids')
     or cardinality(v_expanded_ids) <> jsonb_array_length(v_seed -> 'expanded_timesheet_ids') then
    raise exception 'IMPORT_OPERATION_SCOPE_DUPLICATE' using errcode = '22023';
  end if;
  if not (v_requested_ids <@ v_expanded_ids) then
    raise exception 'IMPORT_OPERATION_REQUESTED_SCOPE_NOT_EXPANDED'
      using errcode = '22023';
  end if;

  v_units_input := v_seed -> 'correction_units';
  if exists (
    select 1 from jsonb_array_elements(v_units_input) unit
    where jsonb_typeof(unit) <> 'object'
       or nullif(btrim(unit ->> 'root_timesheet_id'),'') is null
       or nullif(btrim(unit ->> 'source_row_key'),'') is null
       or upper(btrim(coalesce(unit ->> 'correction_action',''))) not in ('CHANGED_HOURS','CANCELLATION')
       or upper(btrim(coalesce(unit ->> 'correction_shape',''))) not in ('REVERSAL_ONLY','REVERSAL_REPLACEMENT')
       or nullif(btrim(unit ->> 'root_timesheet_id'),'') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       or (upper(btrim(unit ->> 'correction_action')) = 'CANCELLATION'
           and upper(btrim(unit ->> 'correction_shape')) <> 'REVERSAL_ONLY')
       or (upper(btrim(unit ->> 'correction_action')) = 'CHANGED_HOURS'
           and upper(btrim(unit ->> 'correction_shape')) <> 'REVERSAL_REPLACEMENT')
  ) then
    raise exception 'IMPORT_OPERATION_CORRECTION_UNIT_INVALID' using errcode = '22023';
  end if;
  if (
    select count(*) from jsonb_array_elements(v_units_input)
  ) <> (
    select count(distinct concat_ws('|',
      unit ->> 'root_timesheet_id',
      unit ->> 'source_row_key',
      upper(unit ->> 'correction_action'),
      upper(unit ->> 'correction_shape')
    )) from jsonb_array_elements(v_units_input) unit
  ) then
    raise exception 'IMPORT_OPERATION_CORRECTION_UNIT_DUPLICATE' using errcode = '22023';
  end if;
  if exists (
    select 1 from jsonb_array_elements(v_units_input) unit
    where not ((unit ->> 'root_timesheet_id')::uuid = any(v_expanded_ids))
  ) then
    raise exception 'IMPORT_OPERATION_CORRECTION_ROOT_OUTSIDE_EXPANDED_SCOPE'
      using errcode = '22023';
  end if;

  v_canonical_seed := jsonb_build_object(
    'schema_version','IMPORT_CORRECTION_OPERATION_SEED_V2',
    'import_id',p_import_id,
    'source_system',p_source_system,
    'import_revision',v_import_revision,
    'requested_timesheet_ids',to_jsonb(v_requested_ids),
    'expanded_timesheet_ids',to_jsonb(v_expanded_ids),
    'correction_units',(
      select coalesce(jsonb_agg(jsonb_build_object(
        'root_timesheet_id',unit ->> 'root_timesheet_id',
        'source_row_key',unit ->> 'source_row_key',
        'correction_action',upper(unit ->> 'correction_action'),
        'correction_shape',upper(unit ->> 'correction_shape')
      ) order by unit ->> 'root_timesheet_id',unit ->> 'source_row_key'),'[]'::jsonb)
      from jsonb_array_elements(v_units_input) unit
    )
  );
  v_seed_fingerprint := encode(
    extensions.digest(convert_to(v_canonical_seed::text,'UTF8'),'sha256'::text),'hex'
  );
  v_canonical_request_hash := v_seed_fingerprint;

  v_lock_acquired := pg_try_advisory_xact_lock(hashtextextended(
    'IMPORT_APPLY_OPERATION|'||p_import_id::text||'|'||v_import_revision||'|'||v_canonical_request_hash,
    24062026
  ));
  if not v_lock_acquired then
    raise exception 'IMPORT_OPERATION_LOCK_BUSY' using errcode = '55P03';
  end if;

  select o.* into v_operation
  from public.import_apply_operations o
  where o.import_id = p_import_id
    and o.import_revision = v_import_revision
    and o.request_hash = v_canonical_request_hash
  for update;

  if found then
    if v_operation.source_system is distinct from p_source_system
       or v_operation.actor_user_id is distinct from p_actor_user_id then
      raise exception 'IMPORT_OPERATION_IDEMPOTENCY_IDENTITY_CONFLICT'
        using errcode = '23505';
    end if;
    v_existing_contract := v_operation.response_json -> 'correction_operation_contract';
    if jsonb_typeof(v_existing_contract) <> 'object'
       or v_existing_contract ->> 'request_seed_fingerprint' is distinct from v_seed_fingerprint then
      raise exception 'IMPORT_OPERATION_IDEMPOTENCY_CONTRACT_CONFLICT'
        using errcode = '23505';
    end if;
    v_existing_contract_fingerprint := encode(
      extensions.digest(
        convert_to((v_existing_contract - 'operation_contract_fingerprint')::text,'UTF8'),
        'sha256'::text
      ),'hex'
    );
    if v_existing_contract ->> 'operation_contract_fingerprint'
       is distinct from v_existing_contract_fingerprint then
      raise exception 'IMPORT_OPERATION_STORED_CONTRACT_FINGERPRINT_INVALID'
        using errcode = 'P0001';
    end if;

    if v_operation.state in ('FAILED_BEFORE_COMMIT','BLOCKED')
       and v_operation.committed_at_utc is null then
      update public.import_apply_operations o
      set state = 'PREPARED', updated_at_utc = v_now
      where o.id = v_operation.id
      returning * into v_operation;
      v_reset_for_retry := true;
    end if;
  else
    v_operation_id := gen_random_uuid();
    for v_unit_input in select value from jsonb_array_elements(v_units_input) loop
      v_root_id := (v_unit_input ->> 'root_timesheet_id')::uuid;
      v_source_row_key := btrim(v_unit_input ->> 'source_row_key');
      v_correction_action := upper(btrim(v_unit_input ->> 'correction_action'));
      v_shape := upper(btrim(v_unit_input ->> 'correction_shape'));
      v_expected_roles := case when v_shape = 'REVERSAL_ONLY'
        then jsonb_build_array('REVERSAL')
        else jsonb_build_array('REVERSAL','REPLACEMENT') end;

      v_envelope := public._ctms_correction_financials_policy_build_v2(
        v_root_id,
        p_import_id,
        v_source_row_key,
        v_correction_action,
        v_shape,
        v_operation_id,
        v_canonical_request_hash,
        v_now,
        true,
        32
      );
      v_canonical_unit := jsonb_build_object(
        'root_timesheet_id',v_envelope ->> 'root_timesheet_id',
        'correction_chain_id',v_envelope ->> 'correction_chain_id',
        'source_shift_id',v_envelope #>> '{classification,source_shift_id}',
        'source_row_key',v_source_row_key,
        'correction_action',v_correction_action,
        'correction_shape',v_shape,
        'expected_member_roles',v_expected_roles,
        'expected_member_count',jsonb_array_length(v_expected_roles),
        'settings_snapshot',v_envelope -> 'settings_snapshot',
        'policy_envelope_fingerprint',v_envelope ->> 'envelope_fingerprint',
        'policy_envelope',v_envelope
      );
      v_canonical_units := v_canonical_units || jsonb_build_array(v_canonical_unit);
    end loop;

    select coalesce(jsonb_agg(jsonb_build_object(
      'root_timesheet_id', unit ->> 'root_timesheet_id',
      'source_row_key', unit ->> 'source_row_key',
      'correction_action', unit ->> 'correction_action',
      'correction_shape', unit ->> 'correction_shape',
      'expected_member_roles', unit -> 'expected_member_roles',
      'expected_member_count', unit -> 'expected_member_count',
      'reversal', jsonb_build_object(
        'applicable', coalesce((unit #>> '{policy_envelope,reversal,applicable}')::boolean, false),
        'setting', unit #>> '{policy_envelope,reversal,setting}',
        'setting_source', unit #>> '{policy_envelope,reversal,setting_source}',
        'tsfin_policy', unit #> '{policy_envelope,reversal,tsfin_policy}',
        'invoice_policy', unit #> '{policy_envelope,reversal,invoice_policy}',
        'leg_fingerprint', unit #>> '{policy_envelope,reversal,leg_fingerprint}'
      ),
      'replacement', jsonb_build_object(
        'applicable', coalesce((unit #>> '{policy_envelope,replacement,applicable}')::boolean, false),
        'setting', unit #>> '{policy_envelope,replacement,setting}',
        'setting_source', unit #>> '{policy_envelope,replacement,setting_source}',
        'tsfin_policy', unit #> '{policy_envelope,replacement,tsfin_policy}',
        'invoice_policy', unit #> '{policy_envelope,replacement,invoice_policy}',
        'leg_fingerprint', unit #>> '{policy_envelope,replacement,leg_fingerprint}'
      ),
      'policy_envelope_fingerprint', unit ->> 'policy_envelope_fingerprint'
    ) order by unit ->> 'root_timesheet_id', unit ->> 'source_row_key'), '[]'::jsonb)
    into v_preview_consequences
    from jsonb_array_elements(v_canonical_units) unit;

    v_contract_payload := jsonb_build_object(
      'schema_version','IMPORT_CORRECTION_OPERATION_V2',
      'route_family','IMPORT_AUTHORITATIVE',
      'operation_id',v_operation_id,
      'import_id',p_import_id,
      'source_system',p_source_system,
      'import_revision',v_import_revision,
      'request_hash',v_canonical_request_hash,
      'caller_request_hash_audit',v_caller_request_hash,
      'request_seed_fingerprint',v_seed_fingerprint,
      'operation_at_utc',v_now,
      'operation_date_london',(v_now at time zone 'Europe/London')::date,
      'requested_timesheet_ids',to_jsonb(v_requested_ids),
      'expanded_timesheet_ids',to_jsonb(v_expanded_ids),
      'correction_units',v_canonical_units
    );
    v_contract_fingerprint := encode(
      extensions.digest(convert_to(v_contract_payload::text,'UTF8'),'sha256'::text),'hex'
    );
    v_contract := v_contract_payload || jsonb_build_object(
      'operation_contract_fingerprint',v_contract_fingerprint
    );

    insert into public.import_apply_operations(
      id,import_id,source_system,import_revision,request_hash,actor_user_id,
      state,response_json,created_at_utc,updated_at_utc
    ) values (
      v_operation_id,p_import_id,p_source_system,v_import_revision,v_canonical_request_hash,p_actor_user_id,
      'PREPARED',
      (v_seed - 'correction_operation_contract') || jsonb_build_object(
        'correction_operation_at_utc',v_now,
        'correction_operation_date_london',(v_now at time zone 'Europe/London')::date,
        'preview_consequences',v_preview_consequences,
        'correction_operation_contract',v_contract
      ),
      v_now,v_now
    ) returning * into v_operation;
    v_inserted := true;
  end if;

  v_action := case when v_inserted then 'IMPORT_OPERATION_CLAIMED'
                   when v_reset_for_retry then 'IMPORT_OPERATION_RETRY_CLAIMED'
                   else 'IMPORT_OPERATION_REPLAYED' end;
  if v_inserted or v_reset_for_retry then
    perform public._inv_write_audit(
      p_actor_user_id,
      v_action,
      jsonb_build_object(
        'operation_id',v_operation.id,
        'import_id',p_import_id,
        'source_system',p_source_system,
        'import_revision',v_import_revision,
        'request_hash',v_canonical_request_hash,
        'caller_request_hash_audit',v_caller_request_hash,
        'state',v_operation.state,
        'correction_unit_count',jsonb_array_length(
          coalesce(v_operation.response_json #> '{correction_operation_contract,correction_units}','[]'::jsonb)
        ),
        'operation_contract_fingerprint',v_operation.response_json #>> '{correction_operation_contract,operation_contract_fingerprint}'
      ),
      'import_apply_operation',v_operation.id::text,null::jsonb,
      'Durable import apply operation claim',null::text,null::text,
      'import-apply-operation:'||v_operation.id::text
    );
  end if;

  return jsonb_build_object(
    'ok',true,
    'operation_id',v_operation.id,
    'import_id',v_operation.import_id,
    'source_system',v_operation.source_system,
    'import_revision',v_operation.import_revision,
    'request_hash',v_operation.request_hash,
    'caller_request_hash_audit',v_operation.response_json #>> '{correction_operation_contract,caller_request_hash_audit}',
    'caller_request_hash_received',v_caller_request_hash,
    'state',v_operation.state,
    'correction_operation_contract',v_operation.response_json -> 'correction_operation_contract',
    'preview_consequences',coalesce(v_operation.response_json -> 'preview_consequences','[]'::jsonb),
    'response_json',v_operation.response_json,
    'inserted',v_inserted,
    'reset_for_retry',v_reset_for_retry,
    'replay',not v_inserted and not v_reset_for_retry,
    'source_committed',v_operation.committed_at_utc is not null,
    'financialised',v_operation.financialised_at_utc is not null,
    'finalised',v_operation.finalised_at_utc is not null,
    'terminal',v_operation.state = 'COMPLETE',
    'continuation_required',v_operation.state in (
      'SOURCE_COMMITTED_TSFIN_PENDING','FINANCIALISED_PENDING_FINALISATION'
    ),
    'created_at_utc',v_operation.created_at_utc,
    'updated_at_utc',v_operation.updated_at_utc
  );
end;
$function$;

comment on function public.import_apply_operation_claim_v1(
  uuid,public.hr_source_enum,text,text,uuid,jsonb,timestamptz
) is
  'Claims or exactly replays one import operation and database-builds every frozen correction policy envelope from canonical source evidence.';

revoke all on function public.import_apply_operation_claim_v1(
  uuid,public.hr_source_enum,text,text,uuid,jsonb,timestamptz
) from public,anon,authenticated;
grant execute on function public.import_apply_operation_claim_v1(
  uuid,public.hr_source_enum,text,text,uuid,jsonb,timestamptz
) to service_role;
