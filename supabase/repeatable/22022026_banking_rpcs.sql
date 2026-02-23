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


create or replace function public.pay_batch_schedule(
  p_pay_batch_id uuid,
  p_schedule_kind text,
  p_scheduled_at_utc timestamptz,
  p_funding_account_ref text,
  p_warning_hours_json jsonb,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_kind text := upper(btrim(coalesce(p_schedule_kind,'')));
  v_batch record;
  v_cfg record;

  v_provider text := upper(btrim(coalesce(nullif(btrim(coalesce(p_schedule_kind,'')),''),'')));

  v_sched_at timestamptz;
  v_warn jsonb;

  v_need_name_check boolean := false;
  v_requires_payee_map boolean := false;
  v_funding text := nullif(btrim(coalesce(p_funding_account_ref,'')), '');

  v_missing_bank int := 0;
  v_blocked_name int := 0;
  v_missing_map int := 0;
  v_pending_transfers int := 0;
begin
  if p_pay_batch_id is null then
    raise exception 'pay_batch_schedule: pay_batch_id is required';
  end if;
  if p_actor_user_id is null then
    raise exception 'pay_batch_schedule: actor_user_id is required';
  end if;
  if v_kind not in ('IMMEDIATE','SCHEDULED') then
    raise exception 'pay_batch_schedule: invalid schedule_kind (IMMEDIATE|SCHEDULED)';
  end if;

  select
    pb.id,
    pb.status,
    pb.pay_date,
    pb.rail_provider_snapshot,
    pb.rail_env_snapshot
  into v_batch
  from public.pay_batches pb
  where pb.id = p_pay_batch_id
  for update;

  if v_batch.id is null then
    raise exception 'pay_batch_schedule: pay_batch not found';
  end if;

  v_provider := upper(btrim(coalesce(v_batch.rail_provider_snapshot,'')));
  if v_provider = '' then
    raise exception 'pay_batch_schedule: rail_provider_snapshot missing on batch';
  end if;

  select
    sd.funds_warning_hours_json,
    sd.rail_supports_name_check,
    sd.rail_supports_scheduling,
    sd.rail_default_funding_account_ref
  into v_cfg
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;

  if v_cfg.rail_supports_scheduling is null then
    raise exception 'pay_batch_schedule: settings_defaults missing (id=1)';
  end if;

  -- Manual rails (e.g. CSV) must not use scheduling
  if v_provider = 'CSV' then
    raise exception 'pay_batch_schedule: scheduling is not supported for manual rail_provider_snapshot=%', v_batch.rail_provider_snapshot;
  end if;

  -- Global capability gate (current defaults)
  if coalesce(v_cfg.rail_supports_scheduling,false) = false then
    raise exception 'pay_batch_schedule: scheduling is not enabled in settings_defaults (rail_supports_scheduling=false)';
  end if;

  v_need_name_check := (coalesce(v_cfg.rail_supports_name_check,false) = true);
  v_requires_payee_map := true; -- scheduling rails are API rails and require payee mapping

  v_warn := coalesce(p_warning_hours_json, v_cfg.funds_warning_hours_json);
  if v_warn is not null and jsonb_typeof(v_warn) <> 'array' then
    raise exception 'pay_batch_schedule: warning_hours_json must be a JSON array';
  end if;

  if v_kind = 'IMMEDIATE' then
    v_sched_at := now();
  else
    if p_scheduled_at_utc is null then
      raise exception 'pay_batch_schedule: scheduled_at_utc is required when schedule_kind=SCHEDULED';
    end if;
    v_sched_at := p_scheduled_at_utc;
  end if;

  -- Funding account must be present for scheduling rails; allow fallback to settings default
  if v_funding is null then
    v_funding := nullif(btrim(coalesce(v_cfg.rail_default_funding_account_ref,'')), '');
  end if;
  if v_funding is null then
    raise exception 'pay_batch_schedule: funding_account_ref is required for this rail (provider=%)', v_batch.rail_provider_snapshot;
  end if;

  select count(*)::int
  into v_pending_transfers
  from public.pay_bank_transfers pbt
  where pbt.pay_batch_id = p_pay_batch_id
    and upper(coalesce(pbt.status,'')) = 'PENDING';

  if v_pending_transfers = 0 then
    raise exception 'pay_batch_schedule: no PENDING transfers exist for this batch (execute-bank required first)';
  end if;

  with t as (
    select
      upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) as payee_kind,
      coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then pbt.umbrella_id else pbt.candidate_id end
      ) as payee_id,
      coalesce(nullif(btrim(coalesce(pbt.bank_details_hash_snapshot,'')),''), c.bank_details_hash, u.bank_details_hash) as bank_hash
    from public.pay_bank_transfers pbt
    left join public.candidates c
      on c.id = coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then null else pbt.candidate_id end
      )
     and upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) = 'CANDIDATE'
    left join public.umbrellas u
      on u.id = coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then pbt.umbrella_id else null end
      )
     and upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) = 'UMBRELLA'
    where pbt.pay_batch_id = p_pay_batch_id
      and upper(coalesce(pbt.status,'')) = 'PENDING'
    group by 1,2,3
  )
  select
    sum(case when t.bank_hash is null or btrim(t.bank_hash) = '' then 1 else 0 end)::int
  into v_missing_bank
  from t;

  if v_missing_bank > 0 then
    raise exception 'pay_batch_schedule: BLOCKED_BANK_DETAILS for % payee(s)', v_missing_bank;
  end if;

  with t as (
    select
      upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) as payee_kind,
      coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then pbt.umbrella_id else pbt.candidate_id end
      ) as payee_id,
      coalesce(nullif(btrim(coalesce(pbt.bank_details_hash_snapshot,'')),''), c.bank_details_hash, u.bank_details_hash) as bank_hash
    from public.pay_bank_transfers pbt
    left join public.candidates c
      on c.id = coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then null else pbt.candidate_id end
      )
     and upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) = 'CANDIDATE'
    left join public.umbrellas u
      on u.id = coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then pbt.umbrella_id else null end
      )
     and upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) = 'UMBRELLA'
    where pbt.pay_batch_id = p_pay_batch_id
      and upper(coalesce(pbt.status,'')) = 'PENDING'
    group by 1,2,3
  )
  select
    sum(
      case
        when v_need_name_check = true then
          case
            when coalesce(bnc.status,'UNVERIFIED') = 'PASS' then 0
            when (bnc.override_reason is not null and bnc.override_hash = t.bank_hash) then 0
            else 1
          end
        else 0
      end
    )::int
  into v_blocked_name
  from t
  left join public.bank_name_checks bnc
    on bnc.rail_provider = v_batch.rail_provider_snapshot
   and bnc.rail_env = v_batch.rail_env_snapshot
   and bnc.entity_kind = t.payee_kind
   and bnc.entity_id = t.payee_id
   and bnc.bank_details_hash = t.bank_hash;

  if v_blocked_name > 0 then
    raise exception 'pay_batch_schedule: BLOCKED_NAME_CHECK for % payee(s)', v_blocked_name;
  end if;

  with t as (
    select
      upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) as payee_kind,
      coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then pbt.umbrella_id else pbt.candidate_id end
      ) as payee_id,
      coalesce(nullif(btrim(coalesce(pbt.bank_details_hash_snapshot,'')),''), c.bank_details_hash, u.bank_details_hash) as bank_hash
    from public.pay_bank_transfers pbt
    left join public.candidates c
      on c.id = coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then null else pbt.candidate_id end
      )
     and upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) = 'CANDIDATE'
    left join public.umbrellas u
      on u.id = coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then pbt.umbrella_id else null end
      )
     and upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) = 'UMBRELLA'
    where pbt.pay_batch_id = p_pay_batch_id
      and upper(coalesce(pbt.status,'')) = 'PENDING'
    group by 1,2,3
  )
  select
    sum(
      case
        when v_requires_payee_map = true then
          case when bpm.payee_id is null then 1 else 0 end
        else 0
      end
    )::int
  into v_missing_map
  from t
  left join public.bank_payee_map bpm
    on bpm.rail_provider = v_batch.rail_provider_snapshot
   and bpm.rail_env = v_batch.rail_env_snapshot
   and bpm.entity_kind = t.payee_kind
   and bpm.entity_id = t.payee_id
   and bpm.bank_details_hash = t.bank_hash;

  if v_missing_map > 0 then
    raise exception 'pay_batch_schedule: BLOCKED_NO_PAYEE_MAP for % payee(s)', v_missing_map;
  end if;

  update public.pay_batches pb
  set
    schedule_kind = v_kind,
    scheduled_at_utc = v_sched_at,
    scheduled_by_user_id = p_actor_user_id,
    funding_account_ref = v_funding,
    funds_warning_hours_json = v_warn,
    status = 'SCHEDULED'
  where pb.id = p_pay_batch_id;

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'status', (select pb2.status from public.pay_batches pb2 where pb2.id = p_pay_batch_id),
    'schedule_kind', v_kind,
    'scheduled_at_utc', v_sched_at::text,
    'funding_account_ref', v_funding,
    'funds_warning_hours_json', v_warn,
    'rail_provider_snapshot', v_batch.rail_provider_snapshot,
    'rail_env_snapshot', v_batch.rail_env_snapshot
  );
end;
$$;



create or replace function public.pay_batch_prepare(
  p_pay_batch_id uuid,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_batch record;
  v_cfg record;

  v_provider text := null;

  v_need_name_check boolean := false;
  v_requires_payee_map boolean := false;

  v_payees jsonb := '[]'::jsonb;
  v_summary jsonb := '{}'::jsonb;
  v_has_hard_blockers boolean := false;
begin
  if p_pay_batch_id is null then
    raise exception 'pay_batch_prepare: pay_batch_id is required';
  end if;
  if p_actor_user_id is null then
    raise exception 'pay_batch_prepare: actor_user_id is required';
  end if;

  select
    pb.id,
    pb.status,
    pb.pay_date,
    pb.rail_provider_snapshot,
    pb.rail_env_snapshot,
    pb.schedule_kind,
    pb.scheduled_at_utc,
    pb.funding_account_ref,
    pb.funds_warning_hours_json
  into v_batch
  from public.pay_batches pb
  where pb.id = p_pay_batch_id;

  if v_batch.id is null then
    raise exception 'pay_batch_prepare: pay_batch not found';
  end if;

  v_provider := upper(btrim(coalesce(v_batch.rail_provider_snapshot,'')));

  select
    sd.rail_provider_default,
    sd.rail_env_default,
    sd.rail_supports_scheduling,
    sd.rail_supports_name_check,
    sd.rail_supports_auto_execute,
    sd.default_schedule_umbrella_local,
    sd.default_schedule_paye_local,
    sd.funds_warning_hours_json,
    sd.rail_default_funding_account_ref
  into v_cfg
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;

  if v_cfg.rail_provider_default is null then
    raise exception 'pay_batch_prepare: settings_defaults missing (id=1)';
  end if;

  -- Generic readiness semantics:
  -- - name-check only when the rail supports it (and never for manual CSV)
  -- - payee-map required for API rails (false for manual CSV)
  v_need_name_check := (coalesce(v_cfg.rail_supports_name_check,false) = true)
                       and (v_provider <> 'CSV');

  v_requires_payee_map := (v_provider <> 'CSV');

  with t as (
    select
      pbt.id as transfer_id,
      pbt.pay_batch_id,
      upper(coalesce(pbt.pay_channel,'')) as pay_channel,
      upper(coalesce(pbt.status,'')) as status,
      pbt.amount,
      pbt.currency,
      pbt.payment_reference,
      pbt.payee_name,
      pbt.sort_code,
      pbt.account_number,
      pbt.account_type,
      pbt.rail_provider,
      pbt.rail_env,
      pbt.request_id,
      pbt.rail_tx_id,
      pbt.rail_state,
      pbt.rail_meta_json,
      pbt.bank_details_hash_snapshot,
      pbt.payee_entity_kind,
      pbt.payee_entity_id,
      pbt.transfer_group_key,
      pbt.grouping_mode_used,
      pbt.week_ending_bucket,
      pbt.candidate_id,
      pbt.umbrella_id,

      upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) as derived_payee_kind,

      coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then pbt.umbrella_id else pbt.candidate_id end
      ) as derived_payee_id
    from public.pay_bank_transfers pbt
    where pbt.pay_batch_id = p_pay_batch_id
  ),
  t2 as (
    select
      t.*,
      c.bank_details_hash as cand_bank_hash,
      u.bank_details_hash as umb_bank_hash
    from t
    left join public.candidates c
      on c.id = t.derived_payee_id
     and t.derived_payee_kind = 'CANDIDATE'
    left join public.umbrellas u
      on u.id = t.derived_payee_id
     and t.derived_payee_kind = 'UMBRELLA'
  ),
  t3 as (
    select
      t2.*,
      coalesce(nullif(btrim(coalesce(t2.bank_details_hash_snapshot,'')),''), t2.cand_bank_hash, t2.umb_bank_hash) as payee_bank_hash
    from t2
  ),
  payees as (
    select
      t3.derived_payee_kind as payee_entity_kind,
      t3.derived_payee_id as payee_entity_id,
      t3.payee_bank_hash as bank_details_hash
    from t3
    group by t3.derived_payee_kind, t3.derived_payee_id, t3.payee_bank_hash
  ),
  payees_enriched as (
    select
      p.payee_entity_kind,
      p.payee_entity_id,
      p.bank_details_hash,

      coalesce(bnc.status, 'UNVERIFIED') as name_check_status,
      (bnc.override_reason is not null and bnc.override_hash = p.bank_details_hash) as name_check_has_override,

      (bpm.payee_id is not null) as payee_map_present,

      (p.bank_details_hash is null or btrim(p.bank_details_hash) = '') as is_missing_bank_details,

      (
        v_need_name_check = true
        and coalesce(bnc.status, 'UNVERIFIED') <> 'PASS'
        and not (bnc.override_reason is not null and bnc.override_hash = p.bank_details_hash)
      ) as is_name_check_blocked,

      (
        v_requires_payee_map = true
        and (bpm.payee_id is null)
      ) as is_payee_map_blocked
    from payees p
    left join public.bank_name_checks bnc
      on bnc.rail_provider = v_batch.rail_provider_snapshot
     and bnc.rail_env = v_batch.rail_env_snapshot
     and bnc.entity_kind = p.payee_entity_kind
     and bnc.entity_id = p.payee_entity_id
     and bnc.bank_details_hash = p.bank_details_hash
    left join public.bank_payee_map bpm
      on bpm.rail_provider = v_batch.rail_provider_snapshot
     and bpm.rail_env = v_batch.rail_env_snapshot
     and bpm.entity_kind = p.payee_entity_kind
     and bpm.entity_id = p.payee_entity_id
     and bpm.bank_details_hash = p.bank_details_hash
  ),
  payees_json as (
    select
      jsonb_agg(
        jsonb_build_object(
          'payee_entity_kind', pe.payee_entity_kind,
          'payee_entity_id', pe.payee_entity_id::text,
          'bank_details_hash', pe.bank_details_hash,
          'name_check', jsonb_build_object(
            'status', pe.name_check_status,
            'has_override', pe.name_check_has_override
          ),
          'payee_map', jsonb_build_object(
            'present', pe.payee_map_present
          ),
          'blockers',
            (
              (case when pe.is_missing_bank_details then jsonb_build_array('BLOCKED_BANK_DETAILS') else '[]'::jsonb end)
              ||
              (case when pe.is_name_check_blocked then jsonb_build_array('BLOCKED_NAME_CHECK') else '[]'::jsonb end)
              ||
              (case when pe.is_payee_map_blocked then jsonb_build_array('BLOCKED_NO_PAYEE_MAP') else '[]'::jsonb end)
            ),
          'transfers',
            coalesce(
              (
                select jsonb_agg(
                  jsonb_build_object(
                    'id', t3.transfer_id::text,
                    'pay_channel', t3.pay_channel,
                    'status', t3.status,
                    'amount', t3.amount,
                    'currency', t3.currency,
                    'payment_reference', t3.payment_reference,
                    'payee_name', t3.payee_name,
                    'sort_code', t3.sort_code,
                    'account_number', t3.account_number,
                    'account_type', t3.account_type,
                    'rail_provider', t3.rail_provider,
                    'rail_env', t3.rail_env,
                    'request_id', t3.request_id,
                    'rail_tx_id', t3.rail_tx_id,
                    'rail_state', t3.rail_state,
                    'rail_meta_json', t3.rail_meta_json,
                    'bank_details_hash_snapshot', t3.bank_details_hash_snapshot,
                    'transfer_group_key', t3.transfer_group_key,
                    'grouping_mode_used', t3.grouping_mode_used,
                    'week_ending_bucket', case when t3.week_ending_bucket is null then null else t3.week_ending_bucket::text end
                  )
                  order by t3.pay_channel, t3.amount desc, t3.transfer_id
                )
                from t3
                where t3.derived_payee_kind = pe.payee_entity_kind
                  and t3.derived_payee_id = pe.payee_entity_id
                  and t3.payee_bank_hash is not distinct from pe.bank_details_hash
              ),
              '[]'::jsonb
            )
        )
        order by pe.payee_entity_kind, pe.payee_entity_id
      ) as j
    from payees_enriched pe
  ),
  summary as (
    select
      jsonb_build_object(
        'total_transfers', count(*)::int,
        'pending', sum(case when upper(coalesce(t3.status,'')) = 'PENDING' then 1 else 0 end)::int,
        'blocked', sum(case when upper(coalesce(t3.status,'')) = 'BLOCKED' then 1 else 0 end)::int,
        'completed', sum(case when upper(coalesce(t3.status,'')) = 'COMPLETED' then 1 else 0 end)::int,
        'failed', sum(case when upper(coalesce(t3.status,'')) = 'FAILED' then 1 else 0 end)::int
      ) as j
    from t3
  ),
  hard_blockers as (
    select
      exists(
        select 1
        from payees_enriched pe
        where pe.is_missing_bank_details = true
           or pe.is_name_check_blocked = true
           or pe.is_payee_map_blocked = true
      ) as has_any
  )
  select
    coalesce((select pj.j from payees_json pj), '[]'::jsonb),
    coalesce((select s.j from summary s), '{}'::jsonb),
    coalesce((select hb.has_any from hard_blockers hb), false)
  into v_payees, v_summary, v_has_hard_blockers;

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', v_batch.id::text,
    'status', v_batch.status,
    'pay_date', case when v_batch.pay_date is null then null else v_batch.pay_date::text end,
    'rail', jsonb_build_object(
      'provider_snapshot', v_batch.rail_provider_snapshot,
      'env_snapshot', v_batch.rail_env_snapshot,
      'need_name_check', v_need_name_check,
      'requires_payee_map', v_requires_payee_map
    ),
    'schedule_recommendations', jsonb_build_object(
      'default_schedule_umbrella_local', v_cfg.default_schedule_umbrella_local,
      'default_schedule_paye_local', v_cfg.default_schedule_paye_local,
      'funds_warning_hours_json', v_cfg.funds_warning_hours_json,
      'rail_default_funding_account_ref', v_cfg.rail_default_funding_account_ref
    ),
    'batch_schedule', jsonb_build_object(
      'schedule_kind', v_batch.schedule_kind,
      'scheduled_at_utc', case when v_batch.scheduled_at_utc is null then null else v_batch.scheduled_at_utc::text end,
      'funding_account_ref', v_batch.funding_account_ref,
      'funds_warning_hours_json', v_batch.funds_warning_hours_json
    ),
    'payees', v_payees,
    'summary', v_summary,
    'has_hard_blockers', v_has_hard_blockers
  );
end;
$$;





create or replace function public.pay_batch_cancel(
  p_pay_batch_id uuid,
  p_actor_user_id uuid,
  p_reason text
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_batch record;
begin
  if p_pay_batch_id is null then
    raise exception 'pay_batch_cancel: pay_batch_id is required';
  end if;
  if p_actor_user_id is null then
    raise exception 'pay_batch_cancel: actor_user_id is required';
  end if;

  select
    pb.id,
    pb.status
  into v_batch
  from public.pay_batches pb
  where pb.id = p_pay_batch_id
  for update;

  if v_batch.id is null then
    raise exception 'pay_batch_cancel: pay_batch not found';
  end if;

  if v_batch.status not in ('READY','SCHEDULED') then
    raise exception 'pay_batch_cancel: batch status must be READY or SCHEDULED (current=%)', v_batch.status;
  end if;

  update public.pay_batch_items pbi
  set pay_bank_transfer_id = null
  from public.pay_batch_candidates pbc
  where pbc.id = pbi.pay_batch_candidate_id
    and pbc.pay_batch_id = p_pay_batch_id
    and pbi.pay_bank_transfer_id is not null;

  delete from public.pay_bank_transfers pbt
  where pbt.pay_batch_id = p_pay_batch_id;

  update public.pay_batches pb
  set
    status = 'CANCELLED',
    cancelled_at_utc = now(),
    cancelled_by_user_id = p_actor_user_id,
    cancel_reason = p_reason,
    schedule_kind = null,
    scheduled_at_utc = null,
    scheduled_by_user_id = null,
    funding_account_ref = null
  where pb.id = p_pay_batch_id;

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'status', (select pb2.status from public.pay_batches pb2 where pb2.id = p_pay_batch_id),
    'cancelled_at_utc', (select pb3.cancelled_at_utc::text from public.pay_batches pb3 where pb3.id = p_pay_batch_id),
    'cancelled_by_user_id', p_actor_user_id::text,
    'cancel_reason', p_reason
  );
end;
$$;

create or replace function public.pay_batch_mark_blocked_funds(
  p_pay_batch_id uuid,
  p_actor_user_id uuid,
  p_funds_check_json jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_batch record;
begin
  if p_pay_batch_id is null then
    raise exception 'pay_batch_mark_blocked_funds: pay_batch_id is required';
  end if;
  if p_actor_user_id is null then
    raise exception 'pay_batch_mark_blocked_funds: actor_user_id is required';
  end if;

  select
    pb.id,
    pb.status
  into v_batch
  from public.pay_batches pb
  where pb.id = p_pay_batch_id
  for update;

  if v_batch.id is null then
    raise exception 'pay_batch_mark_blocked_funds: pay_batch not found';
  end if;

  if v_batch.status not in ('READY','SCHEDULED','EXECUTING') then
    raise exception 'pay_batch_mark_blocked_funds: batch status must be READY, SCHEDULED or EXECUTING (current=%)', v_batch.status;
  end if;

  update public.pay_batch_items pbi
  set pay_bank_transfer_id = null
  from public.pay_batch_candidates pbc
  where pbc.id = pbi.pay_batch_candidate_id
    and pbc.pay_batch_id = p_pay_batch_id
    and pbi.pay_bank_transfer_id is not null;

  delete from public.pay_bank_transfers pbt
  where pbt.pay_batch_id = p_pay_batch_id;

  update public.pay_batches pb
  set
    status = 'BLOCKED_FUNDS',
    last_funds_check_at_utc = now(),
    last_funds_check_json = p_funds_check_json,
    schedule_kind = null,
    scheduled_at_utc = null,
    scheduled_by_user_id = null
  where pb.id = p_pay_batch_id;

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'status', (select pb2.status from public.pay_batches pb2 where pb2.id = p_pay_batch_id),
    'last_funds_check_at_utc', (select pb3.last_funds_check_at_utc::text from public.pay_batches pb3 where pb3.id = p_pay_batch_id),
    'last_funds_check_json', (select pb4.last_funds_check_json from public.pay_batches pb4 where pb4.id = p_pay_batch_id)
  );
end;
$$;

create or replace function public.pay_settle_rail(
  p_pay_batch_id uuid,
  p_settlement_json jsonb,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_batch record;

  v_now timestamptz := now();

  v_newly_settled_candidates jsonb := '[]'::jsonb;

  v_pending_transfers jsonb := '[]'::jsonb;
  v_failed_transfers  jsonb := '[]'::jsonb;
  v_blocked_transfers jsonb := '[]'::jsonb;

  v_batch_status text;

  v_missing_timesheets jsonb := '[]'::jsonb;
  v_ambig_timesheets jsonb := '[]'::jsonb;

  v_adv_id uuid;
  v_old_sched jsonb;
  v_new_sched jsonb;
  v_old_out numeric;
  v_new_out numeric;
  v_old_next date;
  v_new_next date;
  v_total_taken numeric;

  v_linked_transfer_ct int := 0;
  v_has_pos_net boolean := false;
begin
  if p_pay_batch_id is null then
    raise exception 'pay_settle_rail: pay_batch_id is required';
  end if;
  if p_actor_user_id is null then
    raise exception 'pay_settle_rail: actor_user_id is required';
  end if;

  if p_settlement_json is null or jsonb_typeof(p_settlement_json) <> 'array' then
    raise exception 'pay_settle_rail: settlement_json must be a JSON array';
  end if;

  select
    pb.id,
    pb.status,
    pb.pay_date,
    pb.rail_provider_snapshot,
    pb.rail_env_snapshot
  into v_batch
  from public.pay_batches pb
  where pb.id = p_pay_batch_id
  for update;

  if v_batch.id is null then
    raise exception 'pay_settle_rail: pay_batch not found';
  end if;

  create temp table if not exists _tmp_settle_in (
    transfer_id uuid not null,
    status text not null,
    rail_tx_id text null,
    rail_state text null,
    rail_meta_json jsonb null
  ) on commit drop;

  delete from _tmp_settle_in;

  insert into _tmp_settle_in(transfer_id, status, rail_tx_id, rail_state, rail_meta_json)
  select
    nullif(btrim(coalesce(e->>'transfer_id','')),'')::uuid as transfer_id,
    upper(btrim(coalesce(e->>'status',''))) as status,
    nullif(btrim(coalesce(e->>'rail_tx_id','')),'') as rail_tx_id,
    nullif(btrim(coalesce(e->>'rail_state','')),'') as rail_state,
    case
      when (e ? 'rail_meta_json') and jsonb_typeof(e->'rail_meta_json') in ('object','array','string','number','boolean','null')
        then e->'rail_meta_json'
      else null
    end as rail_meta_json
  from jsonb_array_elements(p_settlement_json) e
  where e is not null and jsonb_typeof(e) = 'object';

  if exists (select 1 from _tmp_settle_in t where t.transfer_id is null limit 1) then
    raise exception 'pay_settle_rail: settlement_json contains an invalid or missing transfer_id';
  end if;

  if exists (
    select 1
    from _tmp_settle_in t
    where t.status not in ('PENDING','COMPLETED','FAILED')
    limit 1
  ) then
    raise exception 'pay_settle_rail: invalid status in settlement_json (allowed: PENDING|COMPLETED|FAILED)';
  end if;

  if exists (
    select 1
    from _tmp_settle_in t
    left join public.pay_bank_transfers pbt
      on pbt.id = t.transfer_id
     and pbt.pay_batch_id = p_pay_batch_id
    where pbt.id is null
    limit 1
  ) then
    raise exception 'pay_settle_rail: one or more transfer_id values do not belong to the specified pay batch';
  end if;

  update public.pay_bank_transfers pbt
  set
    status = t.status,
    rail_tx_id = coalesce(t.rail_tx_id, pbt.rail_tx_id),
    rail_state = coalesce(t.rail_state, pbt.rail_state),
    rail_meta_json = case
      when t.rail_meta_json is null then pbt.rail_meta_json
      when pbt.rail_meta_json is null then t.rail_meta_json
      else (pbt.rail_meta_json || t.rail_meta_json)
    end,
    completed_at_utc = case
      when t.status = 'COMPLETED' then coalesce(pbt.completed_at_utc, v_now)
      else pbt.completed_at_utc
    end,
    failed_reason = case
      when t.status = 'FAILED' then coalesce(pbt.failed_reason, nullif(btrim(coalesce(t.rail_state,'')),''))
      else pbt.failed_reason
    end
  from _tmp_settle_in t
  where pbt.id = t.transfer_id
    and pbt.pay_batch_id = p_pay_batch_id;

  -- =========================================================
  -- A5 HARDENING (batch consistency guard):
  -- If any candidate has positive frozen net_bank_amount, there must be at least one linked payable transfer.
  -- (Covers "deleted transfers" and "cleared pay_bank_transfer_id links" bad states.)
  -- =========================================================
  select count(distinct pbi_chk.pay_bank_transfer_id)::int
  into v_linked_transfer_ct
  from public.pay_batch_items pbi_chk
  join public.pay_batch_candidates pbc_chk
    on pbc_chk.id = pbi_chk.pay_batch_candidate_id
  where pbc_chk.pay_batch_id = p_pay_batch_id
    and pbi_chk.item_type <> 'DEBT_CREATED'
    and pbi_chk.pay_bank_transfer_id is not null;

  select exists (
    select 1
    from public.pay_batch_candidates pbc_pos
    where pbc_pos.pay_batch_id = p_pay_batch_id
      and coalesce(pbc_pos.net_bank_amount,0) > 0
    limit 1
  )
  into v_has_pos_net;

  if v_linked_transfer_ct = 0 and v_has_pos_net then
    raise exception 'STATE_INCONSISTENT: positive net_bank_amount but no transfers';
  end if;

  create temp table if not exists _tmp_newly_settled_candidates (
    candidate_id uuid primary key
  ) on commit drop;

  delete from _tmp_newly_settled_candidates;

  with cand_transfers as (
    select
      pbc.candidate_id as candidate_id,
      count(distinct pbi.pay_bank_transfer_id) filter (
        where pbi.item_type <> 'DEBT_CREATED'
          and pbi.pay_bank_transfer_id is not null
      ) as total_transfers,
      count(distinct pbi.pay_bank_transfer_id) filter (
        where pbi.item_type <> 'DEBT_CREATED'
          and pbi.pay_bank_transfer_id is not null
          and upper(coalesce(pbt.status,'')) = 'COMPLETED'
      ) as completed_transfers
    from public.pay_batch_candidates pbc
    left join public.pay_batch_items pbi
      on pbi.pay_batch_candidate_id = pbc.id
    left join public.pay_bank_transfers pbt
      on pbt.id = pbi.pay_bank_transfer_id
     and pbt.pay_batch_id = p_pay_batch_id
    where pbc.pay_batch_id = p_pay_batch_id
    group by pbc.candidate_id
  ),
  eligible as (
    select
      ct.candidate_id
    from cand_transfers ct
    join public.pay_batch_candidates pbc2
      on pbc2.pay_batch_id = p_pay_batch_id
     and pbc2.candidate_id = ct.candidate_id
    where (coalesce(pbc2.settled_at_utc, null) is null)
      and (
        (ct.total_transfers = 0 and coalesce(pbc2.net_bank_amount,0) <= 0)
        or (ct.total_transfers > 0 and ct.total_transfers = ct.completed_transfers)
      )
  )
  insert into _tmp_newly_settled_candidates(candidate_id)
  select e.candidate_id
  from eligible e;

  update public.pay_batch_candidates pbc
  set
    settlement_status = 'SETTLED',
    settled_at_utc = v_now,
    settled_via = upper(coalesce(v_batch.rail_provider_snapshot,'RAIL')),
    settled_note = null
  where pbc.pay_batch_id = p_pay_batch_id
    and pbc.candidate_id in (select t.candidate_id from _tmp_newly_settled_candidates t);

  select coalesce(jsonb_agg(t.candidate_id::text order by t.candidate_id), '[]'::jsonb)
  into v_newly_settled_candidates
  from _tmp_newly_settled_candidates t;

  with needed_timesheets as (
    select distinct
      pbi.timesheet_id as timesheet_id
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = p_pay_batch_id
      and pbc.candidate_id in (select t.candidate_id from _tmp_newly_settled_candidates t)
      and pbi.item_type <> 'DEBT_CREATED'
      and pbi.timesheet_id is not null
  ),
  have_snap as (
    select distinct
      pbs.timesheet_id as timesheet_id
    from public.pay_batch_timesheet_snapshots pbs
    where pbs.pay_batch_id = p_pay_batch_id
      and pbs.candidate_id in (select t.candidate_id from _tmp_newly_settled_candidates t)
  ),
  missing as (
    select n.timesheet_id
    from needed_timesheets n
    left join have_snap h
      on h.timesheet_id = n.timesheet_id
    where h.timesheet_id is null
  )
  select coalesce(jsonb_agg(m.timesheet_id::text order by m.timesheet_id), '[]'::jsonb)
  into v_missing_timesheets
  from missing m;

  if jsonb_array_length(v_missing_timesheets) > 0 then
    raise exception 'pay_settle_rail: MISSING_FROZEN_SNAPSHOTS for timesheets %', v_missing_timesheets::text;
  end if;

  with snap as (
    select
      pbs.timesheet_id,
      pbs.target_snapshot_json
    from public.pay_batch_timesheet_snapshots pbs
    where pbs.pay_batch_id = p_pay_batch_id
      and pbs.candidate_id in (select t.candidate_id from _tmp_newly_settled_candidates t)
  ),
  ambig as (
    select
      s.timesheet_id
    from snap s
    group by s.timesheet_id
    having count(distinct s.target_snapshot_json) > 1
  )
  select coalesce(jsonb_agg(a.timesheet_id::text order by a.timesheet_id), '[]'::jsonb)
  into v_ambig_timesheets
  from ambig a;

  if jsonb_array_length(v_ambig_timesheets) > 0 then
    raise exception 'pay_settle_rail: AMBIGUOUS_TARGET_SNAPSHOT for timesheets %', v_ambig_timesheets::text;
  end if;

  with chosen as (
    select distinct on (pbs.timesheet_id)
      pbs.timesheet_id,
      pbs.target_snapshot_json,
      pbs.signature
    from public.pay_batch_timesheet_snapshots pbs
    where pbs.pay_batch_id = p_pay_batch_id
      and pbs.candidate_id in (select t.candidate_id from _tmp_newly_settled_candidates t)
    order by pbs.timesheet_id, pbs.created_at_utc desc, pbs.id
  )
  insert into public.timesheet_pay_state_history(
    timesheet_id,
    pay_batch_id,
    settled_at_utc,
    snapshot_json,
    signature
  )
  select
    c.timesheet_id,
    p_pay_batch_id,
    v_now,
    c.target_snapshot_json,
    c.signature
  from chosen c;

  with chosen as (
    select distinct on (pbs.timesheet_id)
      pbs.timesheet_id,
      pbs.target_snapshot_json,
      pbs.signature
    from public.pay_batch_timesheet_snapshots pbs
    where pbs.pay_batch_id = p_pay_batch_id
      and pbs.candidate_id in (select t.candidate_id from _tmp_newly_settled_candidates t)
    order by pbs.timesheet_id, pbs.created_at_utc desc, pbs.id
  )
  insert into public.timesheet_pay_state(
    timesheet_id,
    last_settled_snapshot_json,
    last_settled_signature,
    last_settled_pay_batch_id,
    last_settled_at_utc
  )
  select
    c.timesheet_id,
    c.target_snapshot_json,
    c.signature,
    p_pay_batch_id,
    v_now
  from chosen c
  on conflict (timesheet_id) do update
  set
    last_settled_snapshot_json = excluded.last_settled_snapshot_json,
    last_settled_signature = excluded.last_settled_signature,
    last_settled_pay_batch_id = excluded.last_settled_pay_batch_id,
    last_settled_at_utc = excluded.last_settled_at_utc;

  create temp table if not exists _tmp_loan_taken (
    advance_id uuid not null,
    week_start date not null,
    taken_amount numeric not null,
    primary key (advance_id, week_start)
  ) on commit drop;

  delete from _tmp_loan_taken;

  insert into _tmp_loan_taken(advance_id, week_start, taken_amount)
  select
    nullif(btrim(split_part(coalesce(pbi.source_ref,''), ':', 2)),'')::uuid as advance_id,
    pbi.repayment_week_start as week_start,
    round(sum(abs(coalesce(pbi.amount_ex_vat, pbi.amount_inc_vat, 0))),2) as taken_amount
  from public.pay_batch_items pbi
  join public.pay_batch_candidates pbc
    on pbc.id = pbi.pay_batch_candidate_id
  where pbc.pay_batch_id = p_pay_batch_id
    and pbc.candidate_id in (select t.candidate_id from _tmp_newly_settled_candidates t)
    and pbi.item_type = 'LOAN_REPAYMENT'
    and pbi.repayment_week_start is not null
    and pbi.source_ref is not null
    and btrim(coalesce(pbi.source_ref,'')) like 'advance:%'
  group by
    nullif(btrim(split_part(coalesce(pbi.source_ref,''), ':', 2)),'')::uuid,
    pbi.repayment_week_start
  having round(sum(abs(coalesce(pbi.amount_ex_vat, pbi.amount_inc_vat, 0))),2) > 0;

  for v_adv_id in
    select distinct lt.advance_id
    from _tmp_loan_taken lt
    where lt.advance_id is not null
  loop
    select
      pa.schedule_json,
      pa.outstanding_amount,
      pa.next_due_week_start
    into
      v_old_sched,
      v_old_out,
      v_old_next
    from public.pay_advances pa
    where pa.id = v_adv_id
    for update;

    if v_old_sched is null then
      v_old_sched := '[]'::jsonb;
    end if;

    select round(coalesce(sum(lt.taken_amount),0),2)
    into v_total_taken
    from _tmp_loan_taken lt
    where lt.advance_id = v_adv_id;

    v_new_out := round(greatest(coalesce(v_old_out,0) - coalesce(v_total_taken,0), 0), 2);

    with taken_map as (
      select
        lt.week_start,
        lt.taken_amount
      from _tmp_loan_taken lt
      where lt.advance_id = v_adv_id
    ),
    expanded as (
      select
        e.elem as elem,
        nullif(e.elem->>'week_start','')::date as wk,
        coalesce(nullif(e.elem->>'amount','')::numeric,0) as amt
      from jsonb_array_elements(coalesce(v_old_sched,'[]'::jsonb)) e(elem)
    ),
    rewritten as (
      select
        case
          when em.wk is not null
           and tm.week_start is not null
           and em.wk = tm.week_start
           and em.amt < 0
          then
            jsonb_set(
              em.elem,
              '{amount}',
              to_jsonb(round(em.amt + tm.taken_amount, 2)),
              true
            )
          else em.elem
        end as elem
      from expanded em
      left join taken_map tm
        on tm.week_start = em.wk
    )
    select coalesce(jsonb_agg(r.elem), '[]'::jsonb)
    into v_new_sched
    from rewritten r;

    with expanded2 as (
      select
        nullif(e2.elem->>'week_start','')::date as wk,
        coalesce(nullif(e2.elem->>'amount','')::numeric,0) as amt
      from jsonb_array_elements(coalesce(v_new_sched,'[]'::jsonb)) e2(elem)
    )
    select min(ex2.wk)
    into v_new_next
    from expanded2 ex2
    where ex2.wk is not null
      and ex2.amt < 0;

    update public.pay_advances pa2
    set
      schedule_json = coalesce(v_new_sched,'[]'::jsonb),
      outstanding_amount = v_new_out,
      next_due_week_start = v_new_next,
      status = case
        when v_new_out <= 0 or v_new_next is null then 'PAID_OFF'::pay_advance_status_enum
        else pa2.status
      end,
      updated_at = v_now
    where pa2.id = v_adv_id;

    insert into public.pay_advance_patches(
      advance_id,
      pay_batch_id,
      old_outstanding_amount,
      new_outstanding_amount,
      old_schedule_json,
      new_schedule_json,
      old_next_due_week_start,
      new_next_due_week_start
    )
    values (
      v_adv_id,
      p_pay_batch_id,
      v_old_out,
      v_new_out,
      v_old_sched,
      v_new_sched,
      v_old_next,
      v_new_next
    );
  end loop;

  with payable_transfer_ids as (
    select distinct
      pbi.pay_bank_transfer_id as transfer_id
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = p_pay_batch_id
      and pbi.item_type <> 'DEBT_CREATED'
      and pbi.pay_bank_transfer_id is not null
  ),
  stats as (
    select
      sum(case when upper(coalesce(pbt.status,'')) = 'PENDING' then 1 else 0 end)::int as pending_ct,
      sum(case when upper(coalesce(pbt.status,'')) = 'COMPLETED' then 1 else 0 end)::int as completed_ct,
      sum(case when upper(coalesce(pbt.status,'')) = 'FAILED' then 1 else 0 end)::int as failed_ct,
      sum(case when upper(coalesce(pbt.status,'')) = 'BLOCKED' then 1 else 0 end)::int as blocked_ct
    from payable_transfer_ids pti
    join public.pay_bank_transfers pbt
      on pbt.id = pti.transfer_id
     and pbt.pay_batch_id = p_pay_batch_id
  )
  select
    case
      when coalesce(s.pending_ct,0) = 0 and coalesce(s.failed_ct,0) = 0 and coalesce(s.blocked_ct,0) = 0 then 'SETTLED'
      when coalesce(s.pending_ct,0) = 0 and coalesce(s.failed_ct,0) > 0 then 'FAILED'
      else 'PARTIAL'
    end
  into v_batch_status
  from stats s;

  update public.pay_batches pb2
  set
    status = v_batch_status,
    completed_at_utc = case when v_batch_status = 'SETTLED' then coalesce(pb2.completed_at_utc, v_now) else pb2.completed_at_utc end
  where pb2.id = p_pay_batch_id;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'id', pbt.id::text,
        'pay_channel', pbt.pay_channel,
        'status', pbt.status,
        'amount', pbt.amount,
        'rail_tx_id', pbt.rail_tx_id,
        'rail_state', pbt.rail_state,
        'rail_meta_json', pbt.rail_meta_json
      )
      order by pbt.pay_channel, pbt.amount desc, pbt.id
    ),
    '[]'::jsonb
  )
  into v_pending_transfers
  from public.pay_bank_transfers pbt
  where pbt.pay_batch_id = p_pay_batch_id
    and upper(coalesce(pbt.status,'')) = 'PENDING';

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'id', pbt.id::text,
        'pay_channel', pbt.pay_channel,
        'status', pbt.status,
        'amount', pbt.amount,
        'rail_tx_id', pbt.rail_tx_id,
        'rail_state', pbt.rail_state,
        'rail_meta_json', pbt.rail_meta_json,
        'failed_reason', pbt.failed_reason
      )
      order by pbt.pay_channel, pbt.amount desc, pbt.id
    ),
    '[]'::jsonb
  )
  into v_failed_transfers
  from public.pay_bank_transfers pbt
  where pbt.pay_batch_id = p_pay_batch_id
    and upper(coalesce(pbt.status,'')) = 'FAILED';

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'id', pbt.id::text,
        'pay_channel', pbt.pay_channel,
        'status', pbt.status,
        'amount', pbt.amount,
        'rail_state', pbt.rail_state,
        'rail_meta_json', pbt.rail_meta_json
      )
      order by pbt.pay_channel, pbt.amount desc, pbt.id
    ),
    '[]'::jsonb
  )
  into v_blocked_transfers
  from public.pay_bank_transfers pbt
  where pbt.pay_batch_id = p_pay_batch_id
    and upper(coalesce(pbt.status,'')) = 'BLOCKED';

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'batch_status', (select pb3.status from public.pay_batches pb3 where pb3.id = p_pay_batch_id),
    'newly_settled_candidates', v_newly_settled_candidates,
    'still_pending_transfers', v_pending_transfers,
    'failed_transfers', v_failed_transfers,
    'blocked_transfers', v_blocked_transfers
  );
end;
$$;
create or replace function public.pay_settle_manual_confirm(
  p_pay_batch_id uuid,
  p_scope text,
  p_bank_confirm_ref text,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_scope text := upper(btrim(coalesce(p_scope,'ALL')));
  v_batch record;

  v_settlement_json jsonb := '[]'::jsonb;
  v_now timestamptz := now();

  v_pending_count int := 0;
begin
  if p_pay_batch_id is null then
    raise exception 'pay_settle_manual_confirm: pay_batch_id is required';
  end if;
  if p_actor_user_id is null then
    raise exception 'pay_settle_manual_confirm: actor_user_id is required';
  end if;
  if v_scope not in ('ALL','PAYE','UMBRELLA') then
    raise exception 'pay_settle_manual_confirm: invalid scope (ALL|PAYE|UMBRELLA)';
  end if;
  if nullif(btrim(coalesce(p_bank_confirm_ref,'')),'') is null then
    raise exception 'pay_settle_manual_confirm: bank_confirm_ref is required';
  end if;

  select
    pb.id,
    pb.status,
    pb.rail_provider_snapshot,
    pb.rail_env_snapshot
  into v_batch
  from public.pay_batches pb
  where pb.id = p_pay_batch_id
  for update;

  if v_batch.id is null then
    raise exception 'pay_settle_manual_confirm: pay_batch not found';
  end if;

  if upper(coalesce(v_batch.rail_provider_snapshot,'')) <> 'CSV' then
    raise exception 'pay_settle_manual_confirm: CSV-only (rail_provider_snapshot must be CSV; current=%)', v_batch.rail_provider_snapshot;
  end if;

  select count(*)::int
  into v_pending_count
  from public.pay_bank_transfers pbt
  where pbt.pay_batch_id = p_pay_batch_id
    and (v_scope = 'ALL' or upper(coalesce(pbt.pay_channel,'')) = v_scope)
    and upper(coalesce(pbt.status,'')) = 'PENDING';

  if v_pending_count = 0 then
    raise exception 'pay_settle_manual_confirm: no PENDING transfers found for batch % (scope=%)', p_pay_batch_id, v_scope;
  end if;

  update public.pay_batches pb2
  set
    monzo_confirmed_at_utc = v_now,
    monzo_confirmed_by_user_id = p_actor_user_id
  where pb2.id = p_pay_batch_id;

  update public.pay_batch_items pbi
  set bank_reference = p_bank_confirm_ref
  from public.pay_bank_transfers pbt
  join public.pay_batch_candidates pbc
    on pbc.id = pbi.pay_batch_candidate_id
   and pbc.pay_batch_id = p_pay_batch_id
  where pbt.pay_batch_id = p_pay_batch_id
    and (v_scope = 'ALL' or upper(coalesce(pbt.pay_channel,'')) = v_scope)
    and upper(coalesce(pbt.status,'')) = 'PENDING'
    and pbi.pay_bank_transfer_id = pbt.id;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'transfer_id', pbt.id::text,
        'status', 'COMPLETED',
        'rail_tx_id', null,
        'rail_state', 'MANUAL_CONFIRM',
        'rail_meta_json', jsonb_build_object(
          'bank_confirm_ref', p_bank_confirm_ref,
          'confirmed_at_utc', v_now::text,
          'confirmed_by_user_id', p_actor_user_id::text
        )
      )
      order by pbt.pay_channel, pbt.amount desc, pbt.id
    ),
    '[]'::jsonb
  )
  into v_settlement_json
  from public.pay_bank_transfers pbt
  where pbt.pay_batch_id = p_pay_batch_id
    and (v_scope = 'ALL' or upper(coalesce(pbt.pay_channel,'')) = v_scope)
    and upper(coalesce(pbt.status,'')) = 'PENDING';

  return public.pay_settle_rail(p_pay_batch_id, v_settlement_json, p_actor_user_id);
end;
$$;

create or replace function public.pay_remittance_build(
  p_pay_batch_id uuid,
  p_scope text default 'ALL'
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_scope text := upper(btrim(coalesce(p_scope, 'ALL')));
  v_batch record;

  v_jobs_umb jsonb := '[]'::jsonb;
  v_jobs_paye jsonb := '[]'::jsonb;
begin
  if p_pay_batch_id is null then
    raise exception 'pay_remittance_build: pay_batch_id is required';
  end if;

  if v_scope not in ('ALL','PAYE','UMBRELLA') then
    raise exception 'pay_remittance_build: invalid scope "%". Expected ALL|PAYE|UMBRELLA.', v_scope;
  end if;

  select
    pb.id,
    pb.pay_date,
    pb.status,
    pb.bulk_reference,
    pb.banking_system_snapshot,
    pb.external_paye_system_snapshot
  into v_batch
  from public.pay_batches pb
  where pb.id = p_pay_batch_id;

  if v_batch.id is null then
    raise exception 'pay_remittance_build: pay batch % not found.', p_pay_batch_id;
  end if;

  -- ============================================================
  -- Umbrella remittance jobs (recipient = umbrella; per-candidate breakdown)
  -- Uses ONLY batch artifacts:
  --  - pay_bank_transfers (COMPLETED in-scope)
  --  - pay_batch_items (in-scope)
  --  - pay_batch_candidates (display metadata)
  -- Plus read-only enrichment from:
  --  - candidates, umbrellas, timesheets
  -- ============================================================
  if v_scope in ('ALL','UMBRELLA') then
    with umb_transfers as (
      select
        pbt.id as transfer_id,
        pbt.pay_batch_id,
        pbt.candidate_id,
        pbt.umbrella_id,
        pbt.pay_channel,
        pbt.amount,
        pbt.currency,
        pbt.status,
        pbt.payment_reference,
        pbt.completed_at_utc
      from public.pay_bank_transfers pbt
      where pbt.pay_batch_id = p_pay_batch_id
        and pbt.pay_channel = 'UMBRELLA'
        and pbt.status = 'COMPLETED'
        and pbt.umbrella_id is not null
        and pbt.candidate_id is not null
    ),
    umb_groups as (
      select
        ut.umbrella_id,
        max(u.name) as umbrella_name,
        max(u.remittance_email) as remittance_email,
        jsonb_agg(
          jsonb_build_object(
            'transfer_id', ut.transfer_id::text,
            'completed_at_utc', case when ut.completed_at_utc is null then null else ut.completed_at_utc::text end,
            'candidate', jsonb_build_object(
              'candidate_id', c.id::text,
              'tms_ref', c.tms_ref,
              'display_name', c.display_name,
              'email', c.email
            ),
            'transfer', jsonb_build_object(
              'pay_channel', ut.pay_channel,
              'amount', ut.amount,
              'currency', ut.currency,
              'status', ut.status,
              'payment_reference', ut.payment_reference
            ),
            'items', coalesce((
              select jsonb_agg(
                jsonb_build_object(
                  'item_id', pbi.id::text,
                  'item_type', pbi.item_type,
                  'description', pbi.description,
                  'timesheet_id', case when pbi.timesheet_id is null then null else pbi.timesheet_id::text end,
                  'segment_key', pbi.segment_key,
                  'source_ref', pbi.source_ref,
                  'amount_ex_vat', pbi.amount_ex_vat,
                  'amount_vat', pbi.amount_vat,
                  'amount_inc_vat', pbi.amount_inc_vat,
                  'pay_channel', pbi.pay_channel,
                  'timesheet', case
                    when pbi.timesheet_id is null then null
                    else jsonb_build_object(
                      'week_ending_date', case when ts.week_ending_date is null then null else ts.week_ending_date::text end,
                      'reference_number', ts.reference_number,
                      'worked_start_iso', ts.worked_start_iso,
                      'worked_end_iso', ts.worked_end_iso,
                      'break_minutes', ts.break_minutes,
                      'worked_minutes', ts.worked_minutes
                    )
                  end
                )
                order by pbi.timesheet_id nulls last, pbi.id
              )
              from public.pay_batch_candidates pbc
              join public.pay_batch_items pbi
                on pbi.pay_batch_candidate_id = pbc.id
              left join public.timesheets ts
                on ts.timesheet_id = pbi.timesheet_id
               and ts.is_current = true
              where pbc.pay_batch_id = p_pay_batch_id
                and pbc.candidate_id = c.id
                and pbi.pay_channel = 'UMBRELLA'
            ), '[]'::jsonb),
            'totals', jsonb_build_object(
              'items_total_inc_vat', coalesce((
                select round(coalesce(sum(coalesce(pbi2.amount_inc_vat,0)),0),2)
                from public.pay_batch_candidates pbc2
                join public.pay_batch_items pbi2
                  on pbi2.pay_batch_candidate_id = pbc2.id
                where pbc2.pay_batch_id = p_pay_batch_id
                  and pbc2.candidate_id = c.id
                  and pbi2.pay_channel = 'UMBRELLA'
                  and pbi2.item_type <> 'DEBT_CREATED'
              ), 0),
              'items_total_ex_vat', coalesce((
                select round(coalesce(sum(coalesce(pbi3.amount_ex_vat,0)),0),2)
                from public.pay_batch_candidates pbc3
                join public.pay_batch_items pbi3
                  on pbi3.pay_batch_candidate_id = pbc3.id
                where pbc3.pay_batch_id = p_pay_batch_id
                  and pbc3.candidate_id = c.id
                  and pbi3.pay_channel = 'UMBRELLA'
                  and pbi3.item_type <> 'DEBT_CREATED'
              ), 0),
              'debt_created_total', coalesce((
                select round(coalesce(sum(coalesce(pbi4.amount_inc_vat,0)),0),2)
                from public.pay_batch_candidates pbc4
                join public.pay_batch_items pbi4
                  on pbi4.pay_batch_candidate_id = pbc4.id
                where pbc4.pay_batch_id = p_pay_batch_id
                  and pbc4.candidate_id = c.id
                  and pbi4.pay_channel = 'UMBRELLA'
                  and pbi4.item_type = 'DEBT_CREATED'
              ), 0)
            )
          )
          order by c.display_name nulls last, c.tms_ref nulls last, c.id
        ) as candidate_transfers_json,
        round(coalesce(sum(ut.amount),0),2) as umbrella_total_amount
      from umb_transfers ut
      join public.umbrellas u
        on u.id = ut.umbrella_id
      join public.candidates c
        on c.id = ut.candidate_id
      group by ut.umbrella_id
    )
    select coalesce(
      jsonb_agg(
        jsonb_build_object(
          'job_kind', 'UMBRELLA_REMITTANCE',
          'pay_batch_id', v_batch.id::text,
          'scope', 'UMBRELLA',
          'pay_date', case when v_batch.pay_date is null then null else v_batch.pay_date::text end,
          'bulk_reference', v_batch.bulk_reference,
          'recipient', jsonb_build_object(
            'entity_kind', 'UMBRELLA',
            'umbrella_id', ug.umbrella_id::text,
            'name', ug.umbrella_name,
            'remittance_email', ug.remittance_email
          ),
          'summary', jsonb_build_object(
            'total_amount', ug.umbrella_total_amount,
            'currency', 'GBP',
            'transfer_count', jsonb_array_length(coalesce(ug.candidate_transfers_json,'[]'::jsonb))
          ),
          'candidates', coalesce(ug.candidate_transfers_json,'[]'::jsonb)
        )
        order by ug.umbrella_name nulls last, ug.umbrella_id
      ),
      '[]'::jsonb
    )
    into v_jobs_umb
    from umb_groups ug;
  end if;

  -- ============================================================
  -- PAYE remittance jobs (recipient = candidate)
  -- Uses ONLY batch artifacts + read-only enrichment
  -- ============================================================
  if v_scope in ('ALL','PAYE') then
    with paye_transfers as (
      select
        pbt.id as transfer_id,
        pbt.pay_batch_id,
        pbt.candidate_id,
        pbt.pay_channel,
        pbt.amount,
        pbt.currency,
        pbt.status,
        pbt.payment_reference,
        pbt.completed_at_utc
      from public.pay_bank_transfers pbt
      where pbt.pay_batch_id = p_pay_batch_id
        and pbt.pay_channel = 'PAYE'
        and pbt.status = 'COMPLETED'
        and pbt.candidate_id is not null
    ),
    paye_groups as (
      select
        pt.candidate_id,
        max(c.tms_ref) as tms_ref,
        max(c.display_name) as display_name,
        max(c.email) as email,
        round(coalesce(sum(pt.amount),0),2) as total_amount,
        jsonb_agg(
          jsonb_build_object(
            'transfer_id', pt.transfer_id::text,
            'completed_at_utc', case when pt.completed_at_utc is null then null else pt.completed_at_utc::text end,
            'transfer', jsonb_build_object(
              'pay_channel', pt.pay_channel,
              'amount', pt.amount,
              'currency', pt.currency,
              'status', pt.status,
              'payment_reference', pt.payment_reference
            )
          )
          order by pt.transfer_id
        ) as transfers_json
      from paye_transfers pt
      join public.candidates c
        on c.id = pt.candidate_id
      group by pt.candidate_id
    )
    select coalesce(
      jsonb_agg(
        jsonb_build_object(
          'job_kind', 'PAYE_REMITTANCE',
          'pay_batch_id', v_batch.id::text,
          'scope', 'PAYE',
          'pay_date', case when v_batch.pay_date is null then null else v_batch.pay_date::text end,
          'bulk_reference', v_batch.bulk_reference,
          'recipient', jsonb_build_object(
            'entity_kind', 'CANDIDATE',
            'candidate_id', pg.candidate_id::text,
            'tms_ref', pg.tms_ref,
            'display_name', pg.display_name,
            'email', pg.email
          ),
          'summary', jsonb_build_object(
            'total_amount', pg.total_amount,
            'currency', 'GBP'
          ),
          'net_input', coalesce((
            select jsonb_build_object(
              'source', pni.source,
              'net_amount', pni.net_amount,
              'imported_at_utc', case when pni.imported_at_utc is null then null else pni.imported_at_utc::text end,
              'file_name', pni.file_name
            )
            from public.pay_batch_candidates pbc
            join public.pay_batch_paye_net_inputs pni
              on pni.pay_batch_candidate_id = pbc.id
            where pbc.pay_batch_id = p_pay_batch_id
              and pbc.candidate_id = pg.candidate_id
            order by pni.imported_at_utc desc
            limit 1
          ), null),
          'items', coalesce((
            select jsonb_agg(
              jsonb_build_object(
                'item_id', pbi.id::text,
                'item_type', pbi.item_type,
                'description', pbi.description,
                'timesheet_id', case when pbi.timesheet_id is null then null else pbi.timesheet_id::text end,
                'segment_key', pbi.segment_key,
                'source_ref', pbi.source_ref,
                'amount_ex_vat', pbi.amount_ex_vat,
                'amount_vat', pbi.amount_vat,
                'amount_inc_vat', pbi.amount_inc_vat,
                'pay_channel', pbi.pay_channel,
                'timesheet', case
                  when pbi.timesheet_id is null then null
                  else jsonb_build_object(
                    'week_ending_date', case when ts.week_ending_date is null then null else ts.week_ending_date::text end,
                    'reference_number', ts.reference_number,
                    'worked_start_iso', ts.worked_start_iso,
                    'worked_end_iso', ts.worked_end_iso,
                    'break_minutes', ts.break_minutes,
                    'worked_minutes', ts.worked_minutes
                  )
                end
              )
              order by pbi.timesheet_id nulls last, pbi.id
            )
            from public.pay_batch_candidates pbcx
            join public.pay_batch_items pbix
              on pbix.pay_batch_candidate_id = pbcx.id
            left join public.timesheets ts
              on ts.timesheet_id = pbix.timesheet_id
             and ts.is_current = true
            where pbcx.pay_batch_id = p_pay_batch_id
              and pbcx.candidate_id = pg.candidate_id
              and pbix.pay_channel = 'PAYE'
          ), '[]'::jsonb),
          'transfers', coalesce(pg.transfers_json,'[]'::jsonb)
        )
        order by pg.display_name nulls last, pg.tms_ref nulls last, pg.candidate_id
      ),
      '[]'::jsonb
    )
    into v_jobs_paye
    from paye_groups pg;
  end if;

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', v_batch.id::text,
    'scope', v_scope,
    'pay_date', case when v_batch.pay_date is null then null else v_batch.pay_date::text end,
    'bulk_reference', v_batch.bulk_reference,
    'jobs', (coalesce(v_jobs_umb,'[]'::jsonb) || coalesce(v_jobs_paye,'[]'::jsonb))
  );
end;
$$;


create or replace function public.pay_remittance_mark_sent(
  p_pay_batch_id uuid,
  p_scope text,
  p_results_json jsonb,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_scope text := upper(btrim(coalesce(p_scope, 'ALL')));
  v_exists boolean := false;
  v_audit_id uuid;
begin
  if p_pay_batch_id is null then
    raise exception 'pay_remittance_mark_sent: pay_batch_id is required';
  end if;

  if v_scope not in ('ALL','PAYE','UMBRELLA') then
    raise exception 'pay_remittance_mark_sent: invalid scope "%". Expected ALL|PAYE|UMBRELLA.', v_scope;
  end if;

  select exists(
    select 1
    from public.pay_batches pb
    where pb.id = p_pay_batch_id
  )
  into v_exists;

  if v_exists is false then
    raise exception 'pay_remittance_mark_sent: pay batch % not found.', p_pay_batch_id;
  end if;

  insert into public.audit_events(
    actor_user_id,
    object_type,
    object_id_text,
    action,
    before_json,
    after_json,
    reason
  )
  values (
    p_actor_user_id,
    'pay_batch',
    p_pay_batch_id::text,
    'PAY_REMITTANCE_SENT',
    null,
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id::text,
      'scope', v_scope,
      'results', coalesce(p_results_json, 'null'::jsonb)
    ),
    'remittance_mark_sent'
  )
  returning id into v_audit_id;

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'scope', v_scope,
    'audit_event_id', v_audit_id::text
  );
end;
$$;

create or replace function public.pay_reconcile_external_payment(
  p_actor_user_id uuid,
  p_payload_json jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_pay_date date;
  v_note text;
  v_payment_reference text;

  v_settings record;
  v_batch_id uuid;

  v_timesheet_ids uuid[] := array[]::uuid[];
  v_dup jsonb := '[]'::jsonb;
  v_missing jsonb := '[]'::jsonb;

  v_now timestamptz := now();

  v_ts record;
  v_snapshot jsonb;
  v_signature text;

  v_timesheet_count int := 0;
  v_candidate_count int := 0;
begin
  if p_payload_json is null or jsonb_typeof(p_payload_json) <> 'object' then
    raise exception 'pay_reconcile_external_payment: payload_json must be an object';
  end if;

  if nullif(btrim(coalesce(p_payload_json->>'pay_date','')), '') is null then
    raise exception 'pay_reconcile_external_payment: payload.pay_date is required (YYYY-MM-DD)';
  end if;

  begin
    v_pay_date := (p_payload_json->>'pay_date')::date;
  exception when others then
    raise exception 'pay_reconcile_external_payment: payload.pay_date is not a valid date (YYYY-MM-DD)';
  end;

  v_note := nullif(btrim(coalesce(p_payload_json->>'note','')), '');

  v_payment_reference := nullif(
    btrim(
      coalesce(
        p_payload_json->>'payment_reference',
        p_payload_json->>'bulk_reference',
        p_payload_json->>'bank_reference',
        ''
      )
    ),
    ''
  );

  if v_payment_reference is null then
    raise exception 'pay_reconcile_external_payment: payload.payment_reference is required';
  end if;

  if jsonb_typeof(p_payload_json->'timesheet_ids') <> 'array' then
    raise exception 'pay_reconcile_external_payment: payload.timesheet_ids must be an array of UUID strings';
  end if;

  select coalesce(array_agg((x::text)::uuid), array[]::uuid[])
  into v_timesheet_ids
  from jsonb_array_elements_text(p_payload_json->'timesheet_ids') x;

  if array_length(v_timesheet_ids, 1) is null or array_length(v_timesheet_ids, 1) = 0 then
    raise exception 'pay_reconcile_external_payment: payload.timesheet_ids must not be empty';
  end if;

  -- Duplicates guard
  select coalesce(jsonb_agg(d.ts_id::text), '[]'::jsonb)
  into v_dup
  from (
    select t.ts_id
    from (
      select unnest(v_timesheet_ids) as ts_id
    ) t
    group by t.ts_id
    having count(*) > 1
  ) d;

  if jsonb_array_length(v_dup) > 0 then
    raise exception 'pay_reconcile_external_payment: duplicate timesheet_ids %', v_dup::text;
  end if;

  -- Validate settings exist (for snapshot fields required by pay_batches checks)
  select
    sd.banking_system,
    sd.external_paye_system
  into v_settings
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;

  if v_settings.banking_system is null or v_settings.external_paye_system is null then
    raise exception 'pay_reconcile_external_payment: settings_defaults missing banking_system/external_paye_system';
  end if;

  -- Validate timesheets exist and are current
  select coalesce(jsonb_agg(m.ts_id::text), '[]'::jsonb)
  into v_missing
  from (
    select u.ts_id
    from (select unnest(v_timesheet_ids) as ts_id) u
    left join public.timesheets ts
      on ts.timesheet_id = u.ts_id
     and ts.is_current = true
    where ts.timesheet_id is null
  ) m;

  if jsonb_array_length(v_missing) > 0 then
    raise exception 'pay_reconcile_external_payment: timesheet(s) not found or not current %', v_missing::text;
  end if;

  -- Create a settled batch for audit/history linkage
  insert into public.pay_batches(
    pay_date,
    created_at_utc,
    created_by_user_id,
    status,
    banking_system_snapshot,
    external_paye_system_snapshot,
    rail_provider_snapshot,
    rail_env_snapshot,
    bulk_reference
  )
  values (
    v_pay_date,
    v_now,
    p_actor_user_id,
    'SETTLED',
    v_settings.banking_system,
    v_settings.external_paye_system,
    'EXTERNAL',
    'EXTERNAL',
    v_payment_reference
  )
  returning id into v_batch_id;

  -- Create pay_batch_candidates (for history drill-down)
  insert into public.pay_batch_candidates(
    pay_batch_id,
    candidate_id,
    candidate_tms_ref,
    candidate_display_name,
    settlement_status,
    settled_at_utc,
    settled_via,
    settled_note
  )
  select
    v_batch_id,
    c.id,
    c.tms_ref,
    c.display_name,
    'SETTLED',
    v_now,
    'EXTERNAL_RECONCILE',
    v_note
  from public.timesheets_financials tf
  join public.timesheets ts
    on ts.timesheet_id = tf.timesheet_id
   and ts.is_current = true
  join public.candidates c
    on c.id = tf.candidate_id
  where tf.is_current = true
    and tf.timesheet_id = any(v_timesheet_ids)
  group by c.id, c.tms_ref, c.display_name;

  get diagnostics v_candidate_count = row_count;

  -- For each timesheet: build CURRENT snapshot and write to pay_state + history (+ pay_batch_timesheet_snapshots)
  for v_ts in
    select
      tf.timesheet_id,
      tf.candidate_id,
      upper(coalesce(tf.pay_method,'')) as pay_method,
      tf.total_pay_ex_vat,
      tf.expenses_pay_ex_vat,
      tf.travel_pay_ex_vat,
      tf.accommodation_pay_ex_vat,
      tf.other_pay_ex_vat,
      tf.mileage_pay_ex_vat,
      tf.invoice_breakdown_json,
      ts.reference_number
    from public.timesheets_financials tf
    join public.timesheets ts
      on ts.timesheet_id = tf.timesheet_id
     and ts.is_current = true
    where tf.is_current = true
      and tf.timesheet_id = any(v_timesheet_ids)
  loop
    -- Build segments snapshot (mirrors pay_preview "current" shape)
    v_snapshot := jsonb_build_object(
      'segments',
        case
          when v_ts.invoice_breakdown_json is not null
           and jsonb_typeof(v_ts.invoice_breakdown_json) = 'object'
           and upper(coalesce(v_ts.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
           and jsonb_typeof(v_ts.invoice_breakdown_json->'segments') = 'array'
          then (
            select coalesce(jsonb_agg(
              jsonb_build_object(
                'segment_id', nullif(btrim(coalesce(seg->>'segment_id','')), ''),
                'pay_amount', round(coalesce(nullif(seg->>'pay_amount','')::numeric,0),2),
                'exclude_from_pay', coalesce(nullif(seg->>'exclude_from_pay','')::boolean,false),
                'ref_num', nullif(btrim(coalesce(seg->>'ref_num','')), '')
              )
            ), '[]'::jsonb)
            from jsonb_array_elements(v_ts.invoice_breakdown_json->'segments') seg
            where seg is not null and jsonb_typeof(seg)='object'
          )
          else jsonb_build_array(
            jsonb_build_object(
              'segment_id', ('ts:' || v_ts.timesheet_id::text),
              'pay_amount', round(coalesce(v_ts.total_pay_ex_vat,0),2),
              'exclude_from_pay', false,
              'ref_num', nullif(btrim(coalesce(v_ts.reference_number,'')), '')
            )
          )
        end,
      'adjustments', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'id', a.id::text,
            'delta_pay_ex_vat', round(coalesce(a.delta_pay_ex_vat,0),2)
          )
          order by a.created_at nulls last, a.id
        )
        from public.ts_pay_adjustments a
        where a.as_advance = false
          and a.timesheet_id = v_ts.timesheet_id
      ), '[]'::jsonb),
      'additional_pay_ex_vat',
        case
          when v_ts.invoice_breakdown_json is not null
           and jsonb_typeof(v_ts.invoice_breakdown_json)='object'
           and upper(coalesce(v_ts.invoice_breakdown_json->>'mode',''))='SEGMENTS'
          then round(coalesce(nullif(v_ts.invoice_breakdown_json #>> '{additional,pay_ex_vat}','')::numeric,0),2)
          else 0::numeric
        end,
      'expenses', jsonb_build_object(
        'expenses_pay_ex_vat', round(coalesce(v_ts.expenses_pay_ex_vat,0),2),
        'travel_pay_ex_vat', round(coalesce(v_ts.travel_pay_ex_vat,0),2),
        'accommodation_pay_ex_vat', round(coalesce(v_ts.accommodation_pay_ex_vat,0),2),
        'other_pay_ex_vat', round(coalesce(v_ts.other_pay_ex_vat,0),2),
        'mileage_pay_ex_vat', round(coalesce(v_ts.mileage_pay_ex_vat,0),2)
      ),
      'reconciled_external', true,
      'reconciled_pay_batch_id', v_batch_id::text,
      'reconciled_at_utc', v_now::text,
      'external_payment_reference', v_payment_reference
    );

    v_signature := md5(v_snapshot::text);

    -- Optional but strong: write pay_batch_timesheet_snapshots for full batch artefact auditability
    insert into public.pay_batch_timesheet_snapshots(
      pay_batch_id,
      timesheet_id,
      candidate_id,
      pay_channel,
      base_snapshot_json,
      target_snapshot_json,
      signature
    )
    values (
      v_batch_id,
      v_ts.timesheet_id,
      v_ts.candidate_id,
      case when v_ts.pay_method = 'PAYE' then 'PAYE' else 'UMBRELLA' end,
      v_snapshot,
      v_snapshot,
      v_signature
    );

    insert into public.timesheet_pay_state(
      timesheet_id,
      last_settled_snapshot_json,
      last_settled_signature,
      last_settled_pay_batch_id,
      last_settled_at_utc
    )
    values (
      v_ts.timesheet_id,
      v_snapshot,
      v_signature,
      v_batch_id,
      v_now
    )
    on conflict (timesheet_id)
    do update set
      last_settled_snapshot_json = excluded.last_settled_snapshot_json,
      last_settled_signature = excluded.last_settled_signature,
      last_settled_pay_batch_id = excluded.last_settled_pay_batch_id,
      last_settled_at_utc = excluded.last_settled_at_utc;

    insert into public.timesheet_pay_state_history(
      timesheet_id,
      pay_batch_id,
      settled_at_utc,
      snapshot_json,
      signature
    )
    values (
      v_ts.timesheet_id,
      v_batch_id,
      v_now,
      v_snapshot,
      v_signature
    );

    insert into public.audit_events(
      actor_user_id,
      object_type,
      object_id_text,
      action,
      before_json,
      after_json,
      reason
    )
    values (
      p_actor_user_id,
      'timesheet',
      v_ts.timesheet_id::text,
      'PAY_EXTERNAL_RECONCILE',
      null,
      jsonb_build_object(
        'pay_batch_id', v_batch_id::text,
        'pay_date', v_pay_date::text,
        'timesheet_id', v_ts.timesheet_id::text,
        'candidate_id', v_ts.candidate_id::text,
        'pay_method', v_ts.pay_method,
        'payment_reference', v_payment_reference
      ),
      v_note
    );

    v_timesheet_count := v_timesheet_count + 1;
  end loop;

  -- Batch-level audit
  insert into public.audit_events(
    actor_user_id,
    object_type,
    object_id_text,
    action,
    before_json,
    after_json,
    reason
  )
  values (
    p_actor_user_id,
    'pay_batch',
    v_batch_id::text,
    'PAY_EXTERNAL_RECONCILE_BATCH',
    null,
    jsonb_build_object(
      'pay_batch_id', v_batch_id::text,
      'pay_date', v_pay_date::text,
      'payment_reference', v_payment_reference,
      'timesheet_ids', (select jsonb_agg(t::text) from unnest(v_timesheet_ids) t),
      'note', v_note
    ),
    v_note
  );

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', v_batch_id::text,
    'pay_date', v_pay_date::text,
    'payment_reference', v_payment_reference,
    'settled_timesheet_count', v_timesheet_count,
    'candidate_count', v_candidate_count
  );
end;
$$;

create or replace function public.pay_batches_claim_due_scheduled(
  p_limit int,
  p_now_utc timestamptz
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_limit int := greatest(1, least(coalesce(p_limit,50), 500));
  v_now timestamptz := now();
  v_cutoff timestamptz := coalesce(p_now_utc, v_now);

  v_claimed jsonb := '[]'::jsonb;
  v_claimed_count int := 0;
begin
  with claim as (
    select
      pb.id,
      pb.rail_provider_snapshot,
      pb.rail_env_snapshot,
      pb.schedule_kind,
      pb.scheduled_at_utc,
      pb.funding_account_ref
    from public.pay_batches pb
    where upper(coalesce(pb.status,'')) = 'SCHEDULED'
      and pb.scheduled_at_utc is not null
      and pb.scheduled_at_utc <= v_cutoff
    order by pb.scheduled_at_utc asc, pb.id asc
    for update skip locked
    limit v_limit
  ),
  upd as (
    update public.pay_batches pb2
    set
      status = 'EXECUTING',
      executing_started_at_utc = coalesce(pb2.executing_started_at_utc, v_now)
    from claim c
    where pb2.id = c.id
    returning
      pb2.id,
      pb2.rail_provider_snapshot,
      pb2.rail_env_snapshot,
      pb2.schedule_kind,
      pb2.scheduled_at_utc,
      pb2.funding_account_ref,
      pb2.executing_started_at_utc
  )
  select
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'pay_batch_id', u.id::text,
          'rail_provider_snapshot', u.rail_provider_snapshot,
          'rail_env_snapshot', u.rail_env_snapshot,
          'schedule_kind', u.schedule_kind,
          'scheduled_at_utc', u.scheduled_at_utc,
          'funding_account_ref', u.funding_account_ref,
          'executing_started_at_utc', u.executing_started_at_utc
        )
        order by u.scheduled_at_utc asc nulls last, u.id
      ),
      '[]'::jsonb
    ),
    count(*)::int
  into v_claimed, v_claimed_count
  from upd u;

  return jsonb_build_object(
    'ok', true,
    'server_utc', v_now,
    'cutoff_utc', v_cutoff,
    'limit', v_limit,
    'claimed_count', v_claimed_count,
    'claimed', v_claimed
  );
end;
$$;


create or replace function public.pay_bank_transfers_apply_rail_updates(
  p_pay_batch_id uuid,
  p_updates jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();

  v_updated_count int := 0;
  v_input_count int := 0;

  v_missing jsonb := '[]'::jsonb;
  v_duplicates jsonb := '[]'::jsonb;
begin
  if p_pay_batch_id is null then
    raise exception 'pay_bank_transfers_apply_rail_updates: pay_batch_id is required';
  end if;

  if p_updates is null or jsonb_typeof(p_updates) <> 'array' then
    raise exception 'pay_bank_transfers_apply_rail_updates: updates must be a JSON array';
  end if;

  create temp table if not exists _tmp_pbt_updates (
    transfer_id uuid not null,
    status text not null,
    rail_tx_id text null,
    rail_state text null,
    rail_meta_json jsonb null,
    failed_reason text null,
    completed_at_utc timestamptz null
  ) on commit drop;

  delete from _tmp_pbt_updates;

  insert into _tmp_pbt_updates(
    transfer_id,
    status,
    rail_tx_id,
    rail_state,
    rail_meta_json,
    failed_reason,
    completed_at_utc
  )
  select
    nullif(btrim(coalesce(e->>'transfer_id','')),'')::uuid as transfer_id,
    upper(btrim(coalesce(e->>'status',''))) as status,
    nullif(btrim(coalesce(e->>'rail_tx_id','')),'') as rail_tx_id,
    nullif(btrim(coalesce(e->>'rail_state','')),'') as rail_state,
    case
      when (e ? 'rail_meta_json') and jsonb_typeof(e->'rail_meta_json') in ('object','array','string','number','boolean','null')
        then e->'rail_meta_json'
      else null
    end as rail_meta_json,
    nullif(btrim(coalesce(e->>'failed_reason','')),'') as failed_reason,
    nullif(btrim(coalesce(e->>'completed_at_utc','')),'')::timestamptz as completed_at_utc
  from jsonb_array_elements(p_updates) e
  where e is not null and jsonb_typeof(e) = 'object';

  select count(*)::int
  into v_input_count
  from _tmp_pbt_updates t;

  if v_input_count = 0 then
    return jsonb_build_object(
      'ok', true,
      'pay_batch_id', p_pay_batch_id::text,
      'server_utc', v_now,
      'input_count', 0,
      'updated_count', 0,
      'missing_transfer_ids', '[]'::jsonb
    );
  end if;

  if exists (select 1 from _tmp_pbt_updates t where t.transfer_id is null limit 1) then
    raise exception 'pay_bank_transfers_apply_rail_updates: updates contains an invalid or missing transfer_id';
  end if;

  if exists (
    select 1
    from _tmp_pbt_updates t
    where t.status not in ('PENDING','COMPLETED','FAILED','BLOCKED')
    limit 1
  ) then
    raise exception 'pay_bank_transfers_apply_rail_updates: invalid status in updates (allowed: PENDING|COMPLETED|FAILED|BLOCKED)';
  end if;

  select coalesce(jsonb_agg(d.transfer_id::text order by d.transfer_id), '[]'::jsonb)
  into v_duplicates
  from (
    select t.transfer_id
    from _tmp_pbt_updates t
    group by t.transfer_id
    having count(*) > 1
  ) d;

  if jsonb_array_length(v_duplicates) > 0 then
    raise exception 'pay_bank_transfers_apply_rail_updates: duplicate transfer_id values %', v_duplicates::text;
  end if;

  select coalesce(
    jsonb_agg(t.transfer_id::text order by t.transfer_id),
    '[]'::jsonb
  )
  into v_missing
  from _tmp_pbt_updates t
  left join public.pay_bank_transfers pbt_chk
    on pbt_chk.id = t.transfer_id
   and pbt_chk.pay_batch_id = p_pay_batch_id
  where pbt_chk.id is null;

  update public.pay_bank_transfers pbt
  set
    status = t.status,
    rail_tx_id = coalesce(t.rail_tx_id, pbt.rail_tx_id),
    rail_state = coalesce(t.rail_state, pbt.rail_state),
    rail_meta_json = case
      when t.rail_meta_json is null then pbt.rail_meta_json
      when pbt.rail_meta_json is null then t.rail_meta_json
      else (pbt.rail_meta_json || t.rail_meta_json)
    end,
    completed_at_utc = case
      when t.status = 'COMPLETED' then coalesce(t.completed_at_utc, pbt.completed_at_utc, v_now)
      when t.status = 'FAILED' then coalesce(t.completed_at_utc, pbt.completed_at_utc)
      else pbt.completed_at_utc
    end,
    failed_reason = case
      when t.status = 'FAILED' then coalesce(nullif(btrim(coalesce(t.failed_reason,'')), ''), pbt.failed_reason, nullif(btrim(coalesce(t.rail_state,'')), ''))
      else pbt.failed_reason
    end
  from _tmp_pbt_updates t
  where pbt.id = t.transfer_id
    and pbt.pay_batch_id = p_pay_batch_id;

  get diagnostics v_updated_count = row_count;

  update public.pay_batches pb
  set last_status_checked_at_utc = v_now
  where pb.id = p_pay_batch_id;

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'server_utc', v_now,
    'input_count', v_input_count,
    'updated_count', v_updated_count,
    'missing_transfer_ids', v_missing
  );
end;
$$;






