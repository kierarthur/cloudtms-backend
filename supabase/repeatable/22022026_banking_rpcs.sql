CREATE OR REPLACE FUNCTION public.banking_get_capabilities()
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
declare
  v_provider text;
  v_env text;

  v_supports_scheduling boolean;
  v_supports_name_check boolean;
  v_supports_auto_execute boolean;

  v_supports_csv_confirm boolean;
begin
  -- settings_defaults is expected to have a single row; do not assume an id column.
  select
    sd.rail_provider_default,
    sd.rail_env_default,
    sd.rail_supports_scheduling,
    sd.rail_supports_name_check,
    sd.rail_supports_auto_execute
  into
    v_provider,
    v_env,
    v_supports_scheduling,
    v_supports_name_check,
    v_supports_auto_execute
  from public.settings_defaults sd
  limit 1;

  v_provider := upper(btrim(coalesce(v_provider, 'CSV')));
  v_env := upper(btrim(coalesce(v_env, 'PROD')));

  v_supports_scheduling := coalesce(v_supports_scheduling, false);
  v_supports_name_check := coalesce(v_supports_name_check, false);
  v_supports_auto_execute := coalesce(v_supports_auto_execute, false);

  -- CSV rail implies manual bank confirmation (upload + confirm).
  v_supports_csv_confirm := (v_provider = 'CSV');

  return jsonb_build_object(
    'rail_provider', v_provider,
    'rail_env', v_env,
    'supports_scheduling', v_supports_scheduling,
    'supports_name_check', v_supports_name_check,
    'supports_auto_execute', v_supports_auto_execute,
    'supports_csv_confirm', v_supports_csv_confirm,
    'requires_manual_bank_confirm', v_supports_csv_confirm
  );
end;
$function$;



CREATE OR REPLACE FUNCTION public.bank_name_check_record_result(
  p_provider text,
  p_env text,
  p_entity_kind text,
  p_entity_id uuid,
  p_bank_details_hash text,
  p_status text,
  p_result_json jsonb,
  p_checked_at_utc timestamptz,
  p_actor_user_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
declare
  v_provider text := upper(btrim(coalesce(p_provider,'')));
  v_env text := upper(btrim(coalesce(p_env,'')));
  v_kind text := upper(btrim(coalesce(p_entity_kind,'')));
  v_status text := upper(btrim(coalesce(p_status,'')));

  v_current_hash text;
  v_now timestamptz := now();

  v_row public.bank_name_checks%rowtype;
begin
  if v_provider = '' then
    raise exception '%', jsonb_build_object('error','PROVIDER_REQUIRED')::text;
  end if;
  if v_env = '' then
    raise exception '%', jsonb_build_object('error','ENV_REQUIRED')::text;
  end if;
  if v_kind not in ('CANDIDATE','UMBRELLA') then
    raise exception '%', jsonb_build_object('error','INVALID_ENTITY_KIND','expected','CANDIDATE|UMBRELLA')::text;
  end if;
  if p_entity_id is null then
    raise exception '%', jsonb_build_object('error','ENTITY_ID_REQUIRED')::text;
  end if;
  if v_status not in ('UNVERIFIED','PASS','NEAR_MATCH','FAIL','UNAVAILABLE') then
    raise exception '%', jsonb_build_object('error','INVALID_STATUS')::text;
  end if;
  if p_bank_details_hash is null or btrim(p_bank_details_hash) = '' then
    raise exception '%', jsonb_build_object('error','BANK_DETAILS_HASH_REQUIRED')::text;
  end if;

  -- Resolve current bank_details_hash from the entity table (must match to accept the result)
  if v_kind = 'CANDIDATE' then
    select c.bank_details_hash
    into v_current_hash
    from public.candidates c
    where c.id = p_entity_id
    limit 1;
  else
    select u.bank_details_hash
    into v_current_hash
    from public.umbrellas u
    where u.id = p_entity_id
    limit 1;
  end if;

  if v_current_hash is null then
    raise exception '%', jsonb_build_object('error','ENTITY_NOT_FOUND_OR_NO_HASH','entity_kind',v_kind)::text;
  end if;

  -- Stale-result guard (bank details changed while check in-flight)
  if v_current_hash is distinct from btrim(p_bank_details_hash) then
    return jsonb_build_object(
      'ignored', true,
      'reason', 'STALE_HASH',
      'entity_kind', v_kind,
      'entity_id', p_entity_id::text,
      'current_bank_details_hash', v_current_hash,
      'provided_bank_details_hash', btrim(p_bank_details_hash)
    );
  end if;

  -- Upsert the check row. If status becomes PASS, clear override fields.
  insert into public.bank_name_checks (
    rail_provider,
    rail_env,
    entity_kind,
    entity_id,
    bank_details_hash,
    status,
    checked_at_utc,
    result_json,
    created_at_utc,
    updated_at_utc,
    override_reason,
    override_by_user_id,
    override_at_utc,
    override_hash
  )
  values (
    v_provider,
    v_env,
    v_kind,
    p_entity_id,
    v_current_hash,
    v_status,
    coalesce(p_checked_at_utc, v_now),
    p_result_json,
    v_now,
    v_now,
    case when v_status = 'PASS' then null else null end,
    case when v_status = 'PASS' then null else null end,
    case when v_status = 'PASS' then null else null end,
    case when v_status = 'PASS' then null else null end
  )
  on conflict (rail_provider, rail_env, entity_kind, entity_id, bank_details_hash)
  do update set
    status = excluded.status,
    checked_at_utc = excluded.checked_at_utc,
    result_json = excluded.result_json,
    updated_at_utc = v_now,

    override_reason = case
      when excluded.status = 'PASS' then null
      else public.bank_name_checks.override_reason
    end,
    override_by_user_id = case
      when excluded.status = 'PASS' then null
      else public.bank_name_checks.override_by_user_id
    end,
    override_at_utc = case
      when excluded.status = 'PASS' then null
      else public.bank_name_checks.override_at_utc
    end,
    override_hash = case
      when excluded.status = 'PASS' then null
      else public.bank_name_checks.override_hash
    end;

  select bnc.*
  into v_row
  from public.bank_name_checks bnc
  where bnc.rail_provider = v_provider
    and bnc.rail_env = v_env
    and bnc.entity_kind = v_kind
    and bnc.entity_id = p_entity_id
    and bnc.bank_details_hash = v_current_hash
  limit 1;

  return jsonb_build_object(
    'ignored', false,
    'row', jsonb_build_object(
      'rail_provider', v_row.rail_provider,
      'rail_env', v_row.rail_env,
      'entity_kind', v_row.entity_kind,
      'entity_id', v_row.entity_id::text,
      'bank_details_hash', v_row.bank_details_hash,
      'status', v_row.status,
      'checked_at_utc', v_row.checked_at_utc,
      'result_json', v_row.result_json,
      'override_reason', v_row.override_reason,
      'override_by_user_id', case when v_row.override_by_user_id is null then null else v_row.override_by_user_id::text end,
      'override_at_utc', v_row.override_at_utc,
      'override_hash', v_row.override_hash,
      'created_at_utc', v_row.created_at_utc,
      'updated_at_utc', v_row.updated_at_utc
    )
  );
end;
$function$;


CREATE OR REPLACE FUNCTION public.bank_name_check_set_override(
  p_provider text,
  p_env text,
  p_entity_kind text,
  p_entity_id uuid,
  p_reason text,
  p_actor_user_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
declare
  v_provider text := upper(btrim(coalesce(p_provider,'')));
  v_env text := upper(btrim(coalesce(p_env,'')));
  v_kind text := upper(btrim(coalesce(p_entity_kind,'')));

  v_reason text := nullif(btrim(coalesce(p_reason,'')), '');
  v_now timestamptz := now();

  v_current_hash text;
  v_row public.bank_name_checks%rowtype;
begin
  if v_provider = '' then
    raise exception '%', jsonb_build_object('error','PROVIDER_REQUIRED')::text;
  end if;
  if v_env = '' then
    raise exception '%', jsonb_build_object('error','ENV_REQUIRED')::text;
  end if;
  if v_kind not in ('CANDIDATE','UMBRELLA') then
    raise exception '%', jsonb_build_object('error','INVALID_ENTITY_KIND','expected','CANDIDATE|UMBRELLA')::text;
  end if;
  if p_entity_id is null then
    raise exception '%', jsonb_build_object('error','ENTITY_ID_REQUIRED')::text;
  end if;
  if v_reason is null then
    raise exception '%', jsonb_build_object('error','REASON_REQUIRED')::text;
  end if;

  -- Resolve current bank_details_hash (override must apply to the current hash)
  if v_kind = 'CANDIDATE' then
    select c.bank_details_hash
    into v_current_hash
    from public.candidates c
    where c.id = p_entity_id
    limit 1;
  else
    select u.bank_details_hash
    into v_current_hash
    from public.umbrellas u
    where u.id = p_entity_id
    limit 1;
  end if;

  if v_current_hash is null then
    raise exception '%', jsonb_build_object('error','ENTITY_NOT_FOUND_OR_NO_HASH','entity_kind',v_kind)::text;
  end if;

  -- Ensure row exists (insert UNVERIFIED if not), then set override fields.
  insert into public.bank_name_checks (
    rail_provider,
    rail_env,
    entity_kind,
    entity_id,
    bank_details_hash,
    status,
    checked_at_utc,
    result_json,
    override_reason,
    override_by_user_id,
    override_at_utc,
    override_hash,
    created_at_utc,
    updated_at_utc
  )
  values (
    v_provider,
    v_env,
    v_kind,
    p_entity_id,
    v_current_hash,
    'UNVERIFIED',
    null,
    null,
    v_reason,
    p_actor_user_id,
    v_now,
    v_current_hash,
    v_now,
    v_now
  )
  on conflict (rail_provider, rail_env, entity_kind, entity_id, bank_details_hash)
  do update set
    override_reason = excluded.override_reason,
    override_by_user_id = excluded.override_by_user_id,
    override_at_utc = excluded.override_at_utc,
    override_hash = excluded.override_hash,
    updated_at_utc = v_now;

  select bnc.*
  into v_row
  from public.bank_name_checks bnc
  where bnc.rail_provider = v_provider
    and bnc.rail_env = v_env
    and bnc.entity_kind = v_kind
    and bnc.entity_id = p_entity_id
    and bnc.bank_details_hash = v_current_hash
  limit 1;

  return jsonb_build_object(
    'ok', true,
    'row', jsonb_build_object(
      'rail_provider', v_row.rail_provider,
      'rail_env', v_row.rail_env,
      'entity_kind', v_row.entity_kind,
      'entity_id', v_row.entity_id::text,
      'bank_details_hash', v_row.bank_details_hash,
      'status', v_row.status,
      'checked_at_utc', v_row.checked_at_utc,
      'result_json', v_row.result_json,
      'override_reason', v_row.override_reason,
      'override_by_user_id', case when v_row.override_by_user_id is null then null else v_row.override_by_user_id::text end,
      'override_at_utc', v_row.override_at_utc,
      'override_hash', v_row.override_hash,
      'created_at_utc', v_row.created_at_utc,
      'updated_at_utc', v_row.updated_at_utc
    )
  );
end;
$function$;


CREATE OR REPLACE FUNCTION public.bank_name_check_clear_override(
  p_provider text,
  p_env text,
  p_entity_kind text,
  p_entity_id uuid,
  p_actor_user_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
declare
  v_provider text := upper(btrim(coalesce(p_provider,'')));
  v_env text := upper(btrim(coalesce(p_env,'')));
  v_kind text := upper(btrim(coalesce(p_entity_kind,'')));

  v_now timestamptz := now();
  v_current_hash text;

  v_updated int := 0;
  v_row public.bank_name_checks%rowtype;
begin
  if v_provider = '' then
    raise exception '%', jsonb_build_object('error','PROVIDER_REQUIRED')::text;
  end if;
  if v_env = '' then
    raise exception '%', jsonb_build_object('error','ENV_REQUIRED')::text;
  end if;
  if v_kind not in ('CANDIDATE','UMBRELLA') then
    raise exception '%', jsonb_build_object('error','INVALID_ENTITY_KIND','expected','CANDIDATE|UMBRELLA')::text;
  end if;
  if p_entity_id is null then
    raise exception '%', jsonb_build_object('error','ENTITY_ID_REQUIRED')::text;
  end if;

  -- Resolve current hash
  if v_kind = 'CANDIDATE' then
    select c.bank_details_hash
    into v_current_hash
    from public.candidates c
    where c.id = p_entity_id
    limit 1;
  else
    select u.bank_details_hash
    into v_current_hash
    from public.umbrellas u
    where u.id = p_entity_id
    limit 1;
  end if;

  if v_current_hash is null then
    raise exception '%', jsonb_build_object('error','ENTITY_NOT_FOUND_OR_NO_HASH','entity_kind',v_kind)::text;
  end if;

  update public.bank_name_checks bnc
  set
    override_reason = null,
    override_by_user_id = null,
    override_at_utc = null,
    override_hash = null,
    updated_at_utc = v_now
  where bnc.rail_provider = v_provider
    and bnc.rail_env = v_env
    and bnc.entity_kind = v_kind
    and bnc.entity_id = p_entity_id
    and bnc.bank_details_hash = v_current_hash;

  get diagnostics v_updated = row_count;

  if v_updated = 0 then
    return jsonb_build_object(
      'ok', true,
      'did_update', false,
      'row', null
    );
  end if;

  select bnc2.*
  into v_row
  from public.bank_name_checks bnc2
  where bnc2.rail_provider = v_provider
    and bnc2.rail_env = v_env
    and bnc2.entity_kind = v_kind
    and bnc2.entity_id = p_entity_id
    and bnc2.bank_details_hash = v_current_hash
  limit 1;

  return jsonb_build_object(
    'ok', true,
    'did_update', true,
    'row', jsonb_build_object(
      'rail_provider', v_row.rail_provider,
      'rail_env', v_row.rail_env,
      'entity_kind', v_row.entity_kind,
      'entity_id', v_row.entity_id::text,
      'bank_details_hash', v_row.bank_details_hash,
      'status', v_row.status,
      'checked_at_utc', v_row.checked_at_utc,
      'result_json', v_row.result_json,
      'override_reason', v_row.override_reason,
      'override_by_user_id', case when v_row.override_by_user_id is null then null else v_row.override_by_user_id::text end,
      'override_at_utc', v_row.override_at_utc,
      'override_hash', v_row.override_hash,
      'created_at_utc', v_row.created_at_utc,
      'updated_at_utc', v_row.updated_at_utc
    )
  );
end;
$function$;


CREATE OR REPLACE FUNCTION public.bank_payee_map_upsert(
  p_provider text,
  p_env text,
  p_entity_kind text,
  p_entity_id uuid,
  p_bank_details_hash text,
  p_payee_id text,
  p_payee_account_id text,
  p_meta_json jsonb,
  p_actor_user_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
declare
  v_provider text := upper(btrim(coalesce(p_provider,'')));
  v_env text := upper(btrim(coalesce(p_env,'')));
  v_kind text := upper(btrim(coalesce(p_entity_kind,'')));

  v_hash text := nullif(btrim(coalesce(p_bank_details_hash,'')), '');
  v_payee_id text := nullif(btrim(coalesce(p_payee_id,'')), '');
  v_payee_account_id text := nullif(btrim(coalesce(p_payee_account_id,'')), '');

  v_now timestamptz := now();

  v_row public.bank_payee_map%rowtype;
begin
  if v_provider = '' then
    raise exception '%', jsonb_build_object('error','PROVIDER_REQUIRED')::text;
  end if;

  if v_env = '' then
    raise exception '%', jsonb_build_object('error','ENV_REQUIRED')::text;
  end if;

  if v_kind not in ('CANDIDATE','UMBRELLA') then
    raise exception '%', jsonb_build_object('error','INVALID_ENTITY_KIND','expected','CANDIDATE|UMBRELLA')::text;
  end if;

  if p_entity_id is null then
    raise exception '%', jsonb_build_object('error','ENTITY_ID_REQUIRED')::text;
  end if;

  if v_hash is null then
    raise exception '%', jsonb_build_object('error','BANK_DETAILS_HASH_REQUIRED')::text;
  end if;

  if v_payee_id is null then
    raise exception '%', jsonb_build_object('error','PAYEE_ID_REQUIRED')::text;
  end if;

  insert into public.bank_payee_map (
    rail_provider,
    rail_env,
    entity_kind,
    entity_id,
    bank_details_hash,
    payee_id,
    payee_account_id,
    meta_json,
    created_at_utc,
    updated_at_utc
  )
  values (
    v_provider,
    v_env,
    v_kind,
    p_entity_id,
    v_hash,
    v_payee_id,
    v_payee_account_id,
    p_meta_json,
    v_now,
    v_now
  )
  on conflict (rail_provider, rail_env, entity_kind, entity_id, bank_details_hash)
  do update set
    payee_id = excluded.payee_id,
    payee_account_id = excluded.payee_account_id,
    meta_json = excluded.meta_json,
    updated_at_utc = v_now;

  select bpm.*
  into v_row
  from public.bank_payee_map bpm
  where bpm.rail_provider = v_provider
    and bpm.rail_env = v_env
    and bpm.entity_kind = v_kind
    and bpm.entity_id = p_entity_id
    and bpm.bank_details_hash = v_hash
  limit 1;

  return jsonb_build_object(
    'ok', true,
    'row', jsonb_build_object(
      'rail_provider', v_row.rail_provider,
      'rail_env', v_row.rail_env,
      'entity_kind', v_row.entity_kind,
      'entity_id', v_row.entity_id::text,
      'bank_details_hash', v_row.bank_details_hash,
      'payee_id', v_row.payee_id,
      'payee_account_id', v_row.payee_account_id,
      'meta_json', v_row.meta_json,
      'created_at_utc', v_row.created_at_utc,
      'updated_at_utc', v_row.updated_at_utc
    )
  );
end;
$function$;
