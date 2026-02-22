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

  select
    sd.rail_provider_default,
    sd.rail_env_default,
    sd.rail_supports_scheduling,
    sd.rail_supports_name_check,
    sd.rail_supports_auto_execute,
    sd.default_schedule_umbrella_local,
    sd.default_schedule_paye_local,
    sd.funds_warning_hours_json
  into v_cfg
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;

  if v_cfg.rail_provider_default is null then
    raise exception 'pay_batch_prepare: settings_defaults missing (id=1)';
  end if;

  v_need_name_check := (upper(coalesce(v_batch.rail_provider_snapshot,'')) = 'REVOLUT') and coalesce(v_cfg.rail_supports_name_check,false) = true;
  v_requires_payee_map := (upper(coalesce(v_batch.rail_provider_snapshot,'')) = 'REVOLUT');

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
      'funds_warning_hours_json', v_cfg.funds_warning_hours_json
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

  v_sched_at timestamptz;
  v_warn jsonb;

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

  if upper(coalesce(v_batch.rail_provider_snapshot,'')) <> 'REVOLUT' then
    raise exception 'pay_batch_schedule: scheduling is only supported for rail_provider_snapshot=REVOLUT (current=%)', v_batch.rail_provider_snapshot;
  end if;

  select
    sd.funds_warning_hours_json,
    sd.rail_supports_name_check
  into v_cfg
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;

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
        when coalesce(v_cfg.rail_supports_name_check,false) = true then
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
    sum(case when bpm.payee_id is null then 1 else 0 end)::int
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
    funding_account_ref = p_funding_account_ref,
    funds_warning_hours_json = v_warn,
    status = 'SCHEDULED'
  where pb.id = p_pay_batch_id;

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'status', (select pb2.status from public.pay_batches pb2 where pb2.id = p_pay_batch_id),
    'schedule_kind', v_kind,
    'scheduled_at_utc', v_sched_at::text,
    'funding_account_ref', p_funding_account_ref,
    'funds_warning_hours_json', v_warn,
    'rail_provider_snapshot', v_batch.rail_provider_snapshot,
    'rail_env_snapshot', v_batch.rail_env_snapshot
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










