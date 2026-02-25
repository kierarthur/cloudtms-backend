begin;

-- =========================================================
-- Helpers (private)
-- =========================================================

-- Normalize percent: supports 20 or 0.2 -> returns 0.2
create or replace function public._pay_pct_to_frac(p_pct numeric)
returns numeric
language plpgsql
immutable
as $$
begin
  if p_pct is null then return 0; end if;
  if p_pct > 1 then return p_pct / 100; end if;
  return p_pct;
end;
$$;

-- 1 + pct (supports 15 or 0.15 -> 1.15)
create or replace function public._pay_pct_to_mult(p_pct numeric)
returns numeric
language plpgsql
immutable
as $$
declare
  v_frac numeric;
begin
  v_frac := public._pay_pct_to_frac(p_pct);
  return 1 + coalesce(v_frac, 0);
end;
$$;

-- Monday week-start for a given date
create or replace function public._pay_week_start_monday(p_date date)
returns date
language plpgsql
immutable
as $$
declare
  v_dow int;
  v_offset int;
begin
  if p_date is null then return null; end if;
  v_dow := extract(dow from p_date)::int;      -- 0=Sun..6=Sat
  v_offset := (v_dow + 6) % 7;                 -- days since Monday
  return (p_date - v_offset);
end;
$$;

-- Umbrella VAT calc: returns {ex, vat, inc}
create or replace function public._pay_umbrella_vat_calc(
  p_ex numeric,
  p_vat_rate_pct numeric,
  p_vat_chargeable boolean
)
returns jsonb
language plpgsql
immutable
as $$
declare
  v_ex numeric := coalesce(p_ex,0);
  v_vat numeric := 0;
  v_inc numeric := 0;
  v_frac numeric := public._pay_pct_to_frac(p_vat_rate_pct);
begin
  if coalesce(p_vat_chargeable,false) then
    v_vat := round(v_ex * v_frac, 2);
    v_inc := round(v_ex + v_vat, 2);
  else
    v_vat := 0;
    v_inc := round(v_ex, 2);
  end if;

  return jsonb_build_object(
    'ex', round(v_ex,2),
    'vat', round(v_vat,2),
    'inc', round(v_inc,2)
  );
end;
$$;

-- Conversion: PAYE(ex) -> UMBRELLA (ex after ERNI, then VAT if chargeable)
create or replace function public._pay_convert_paye_to_umbrella(
  p_paye_ex numeric,
  p_erni_pct numeric,
  p_vat_rate_pct numeric,
  p_vat_chargeable boolean
)
returns jsonb
language plpgsql
immutable
as $$
declare
  v_erni_mult numeric := public._pay_pct_to_mult(p_erni_pct);
  v_ex numeric := round(coalesce(p_paye_ex,0) * v_erni_mult, 2);
begin
  return public._pay_umbrella_vat_calc(v_ex, p_vat_rate_pct, p_vat_chargeable);
end;
$$;

-- Conversion: UMBRELLA(ex) -> PAYE(ex) (reverse ERNI; VAT not applicable on PAYE)
create or replace function public._pay_convert_umbrella_to_paye_ex(
  p_umbrella_ex numeric,
  p_erni_pct numeric
)
returns numeric
language plpgsql
immutable
as $$
declare
  v_erni_mult numeric := public._pay_pct_to_mult(p_erni_pct);
  v_ex numeric := coalesce(p_umbrella_ex,0);
begin
  if v_erni_mult <= 0 then v_erni_mult := 1; end if;
  return round(v_ex / v_erni_mult, 2);
end;
$$;

-- Robust CSV line parser (handles quoted fields, commas inside quotes)
create or replace function public._pay_csv_parse_line(p_line text)
returns text[]
language plpgsql
immutable
as $$
declare
  v text := coalesce(p_line,'');
  i int := 1;
  ch text;
  in_quotes boolean := false;
  cur text := '';
  out_arr text[] := array[]::text[];
begin
  while i <= char_length(v) loop
    ch := substr(v, i, 1);

    if ch = '"' then
      -- Double quote escape inside quoted field
      if in_quotes and i < char_length(v) and substr(v, i+1, 1) = '"' then
        cur := cur || '"';
        i := i + 1;
      else
        in_quotes := not in_quotes;
      end if;

    elsif ch = ',' and not in_quotes then
      out_arr := out_arr || array[cur];
      cur := '';

    else
      cur := cur || ch;
    end if;

    i := i + 1;
  end loop;

  out_arr := out_arr || array[cur];
  return out_arr;
end;
$$;

create or replace function public._pay_csv_trim_field(p_field text)
returns text
language plpgsql
immutable
as $$
begin
  return nullif(btrim(coalesce(p_field,'')), '');
end;
$$;


create or replace function public.pay_batch_get(p_pay_batch_id uuid)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_batch record;
  v_candidates jsonb := '[]'::jsonb;
  v_transfers jsonb := '[]'::jsonb;
  v_items jsonb := '[]'::jsonb;

  -- ✅ C: schedule recommendations / UI defaults
  v_default_schedule_umbrella_local text := null;
  v_default_schedule_paye_local text := null;
  v_funds_warning_hours_json jsonb := null;
  v_rail_default_funding_account_ref text := null;

  -- ✅ D: authorisation summary (lightweight, for Banking UI)
  v_ar record;
  v_auth_approved_count int := 0;
  v_auth_approved_by jsonb := '[]'::jsonb;
  v_auth_invited_to jsonb := '[]'::jsonb;
  v_auth jsonb := null;
begin
  if p_pay_batch_id is null then
    raise exception 'pay_batch_id is required';
  end if;

  select
    pb.*
  into v_batch
  from public.pay_batches pb
  where pb.id = p_pay_batch_id
  limit 1;

  if not found then
    raise exception 'pay_batch not found';
  end if;

  -- ✅ C: load schedule defaults for UI preselect (do not change batch state)
  select
    sd.default_schedule_umbrella_local,
    sd.default_schedule_paye_local,
    sd.funds_warning_hours_json,
    sd.rail_default_funding_account_ref
  into
    v_default_schedule_umbrella_local,
    v_default_schedule_paye_local,
    v_funds_warning_hours_json,
    v_rail_default_funding_account_ref
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;

  -- ✅ D: authorisation summary (latest active request if present)
  v_auth := jsonb_build_object(
    'auth_request_id', null,
    'auth_state', null,
    'required_quantity', null,
    'approved_count', 0,
    'approved_by', '[]'::jsonb,
    'invited_to', '[]'::jsonb,
    'golden_key_used', false,
    'golden_key_user_id', null
  );

  select
    pbar0.id as auth_request_id,
    pbar0.state as auth_state,
    pbar0.required_quantity as required_quantity,
    pbar0.golden_key_used as golden_key_used,
    pbar0.golden_key_user_id as golden_key_user_id
  into v_ar
  from public.pay_batch_auth_requests pbar0
  where pbar0.pay_batch_id = p_pay_batch_id
    and pbar0.state in ('AWAITING','AUTHORISED')
  order by pbar0.created_at_utc desc, pbar0.id desc
  limit 1;

  if v_ar.auth_request_id is not null then
    select count(*)::int
    into v_auth_approved_count
    from public.pay_batch_auth_actions pbaa0
    where pbaa0.auth_request_id = v_ar.auth_request_id
      and pbaa0.action in ('AUTHORISE','USE_GOLDEN_KEY');

    select coalesce(
      jsonb_agg(
        jsonb_build_object(
          'actor_user_id', pbaa1.actor_user_id::text,
          'actor_display_name', tu1.display_name,
          'action', pbaa1.action,
          'action_at_utc', pbaa1.action_at_utc,
          'note', pbaa1.note
        )
        order by pbaa1.action_at_utc asc nulls last, pbaa1.id asc
      ),
      '[]'::jsonb
    )
    into v_auth_approved_by
    from public.pay_batch_auth_actions pbaa1
    left join public.tms_users tu1
      on tu1.id = pbaa1.actor_user_id
    where pbaa1.auth_request_id = v_ar.auth_request_id;

    select coalesce(
      jsonb_agg(
        jsonb_build_object(
          'target_user_id', pbat0.target_user_id::text,
          'target_display_name', tu2.display_name,
          'invited_at_utc', pbat0.created_at_utc,
          'expires_at_utc', pbat0.expires_at_utc,
          'used_at_utc', pbat0.used_at_utc
        )
        order by pbat0.created_at_utc asc nulls last, pbat0.token asc
      ),
      '[]'::jsonb
    )
    into v_auth_invited_to
    from public.pay_batch_auth_tokens pbat0
    left join public.tms_users tu2
      on tu2.id = pbat0.target_user_id
    where pbat0.auth_request_id = v_ar.auth_request_id;

    v_auth := jsonb_build_object(
      'auth_request_id', v_ar.auth_request_id::text,
      'auth_state', v_ar.auth_state,
      'required_quantity', v_ar.required_quantity,
      'approved_count', v_auth_approved_count,
      'approved_by', v_auth_approved_by,
      'invited_to', v_auth_invited_to,
      'golden_key_used', coalesce(v_ar.golden_key_used,false),
      'golden_key_user_id', case when v_ar.golden_key_user_id is null then null else v_ar.golden_key_user_id::text end
    );
  end if;

  -- Candidates + PAYE net input summary (latest per candidate)
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'id', pbc.id::text,
        'candidate_id', pbc.candidate_id::text,
        'candidate_tms_ref', pbc.candidate_tms_ref,
        'candidate_display_name', pbc.candidate_display_name,
        'paye_state', pbc.paye_state,
        'mismatch_settlement_choice', pbc.mismatch_settlement_choice,
        'gross_preview', pbc.gross_preview,
        'net_bank_amount', pbc.net_bank_amount,
        'debt_created', pbc.debt_created,
        'loan_repayment_taken', pbc.loan_repayment_taken,
        'settlement_status', pbc.settlement_status,
        'settled_at_utc', pbc.settled_at_utc,
        'settled_via', pbc.settled_via,
        'settled_note', pbc.settled_note,

        -- Latest PAYE net input summary
        'paye_net_amount', ni.net_amount,
        'paye_net_source', ni.source,
        'paye_net_imported_at_utc', ni.imported_at_utc,
        'paye_net_file_name', ni.file_name
      )
      order by pbc.candidate_display_name nulls last, pbc.candidate_tms_ref nulls last, pbc.candidate_id
    ),
    '[]'::jsonb
  )
  into v_candidates
  from public.pay_batch_candidates pbc
  left join lateral (
    select
      pni.net_amount,
      pni.source,
      pni.imported_at_utc,
      pni.file_name
    from public.pay_batch_paye_net_inputs pni
    where pni.pay_batch_candidate_id = pbc.id
    order by pni.imported_at_utc desc
    limit 1
  ) ni on true
  where pbc.pay_batch_id = p_pay_batch_id;

  -- Transfers + snapshot fields + rail-generic fields
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'id', pbt.id::text,
        'candidate_id', case when pbt.candidate_id is null then null else pbt.candidate_id::text end,
        'umbrella_id', case when pbt.umbrella_id is null then null else pbt.umbrella_id::text end,
        'pay_channel', pbt.pay_channel,
        'amount', pbt.amount,
        'currency', pbt.currency,
        'status', pbt.status,

        -- Rail-generic equivalents
        'rail_provider', pbt.rail_provider,
        'rail_env', pbt.rail_env,
        'request_id', pbt.request_id,
        'rail_tx_id', pbt.rail_tx_id,
        'rail_state', pbt.rail_state,
        'rail_meta_json', pbt.rail_meta_json,

        -- Snapshot bank fields
        'payment_reference', pbt.payment_reference,
        'payee_name', pbt.payee_name,
        'sort_code', pbt.sort_code,
        'account_number', pbt.account_number,
        'account_type', pbt.account_type,
        'bank_details_hash_snapshot', pbt.bank_details_hash_snapshot,

        -- Drilldown/grouping fields
        'transfer_group_key', pbt.transfer_group_key,
        'grouping_mode_used', pbt.grouping_mode_used,
        'week_ending_bucket', case when pbt.week_ending_bucket is null then null else pbt.week_ending_bucket::text end,

        'created_at_utc', pbt.created_at_utc,
        'completed_at_utc', pbt.completed_at_utc,
        'failed_reason', pbt.failed_reason
      )
      order by pbt.pay_channel, pbt.amount desc, pbt.id
    ),
    '[]'::jsonb
  )
  into v_transfers
  from public.pay_bank_transfers pbt
  where pbt.pay_batch_id = p_pay_batch_id;

  -- Items unchanged
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'id', pbi.id::text,
        'pay_batch_candidate_id', pbi.pay_batch_candidate_id::text,
        'item_type', pbi.item_type,
        'timesheet_id', case when pbi.timesheet_id is null then null else pbi.timesheet_id::text end,
        'segment_key', pbi.segment_key,
        'source_ref', pbi.source_ref,
        'description', pbi.description,
        'amount_ex_vat', pbi.amount_ex_vat,
        'amount_vat', pbi.amount_vat,
        'amount_inc_vat', pbi.amount_inc_vat,
        'pay_channel', pbi.pay_channel,
        'umbrella_id', case when pbi.umbrella_id is null then null else pbi.umbrella_id::text end
      )
      order by pbi.pay_batch_candidate_id, pbi.id
    ),
    '[]'::jsonb
  )
  into v_items
  from public.pay_batch_items pbi
  join public.pay_batch_candidates pbc2
    on pbc2.id = pbi.pay_batch_candidate_id
  where pbc2.pay_batch_id = p_pay_batch_id;

  return jsonb_build_object(
    'ok', true,

    -- ✅ C: schedule recommendations for UI preselect (includes default funding account)
    'schedule_recommendations', jsonb_build_object(
      'default_schedule_umbrella_local', v_default_schedule_umbrella_local,
      'default_schedule_paye_local', v_default_schedule_paye_local,
      'funds_warning_hours_json', v_funds_warning_hours_json,
      'rail_default_funding_account_ref', v_rail_default_funding_account_ref
    ),

    -- ✅ D: authorisation summary for Banking UI
    'auth', v_auth,

    'batch', jsonb_build_object(
      'id', v_batch.id::text,
      'pay_date', v_batch.pay_date::text,
      'created_at_utc', v_batch.created_at_utc,
      'created_by_user_id', case when v_batch.created_by_user_id is null then null else v_batch.created_by_user_id::text end,
      'status', v_batch.status,
      'banking_system_snapshot', v_batch.banking_system_snapshot,
      'external_paye_system_snapshot', v_batch.external_paye_system_snapshot,

      -- Neutral (rail-generic) manual confirm aliases (keep legacy keys too)
      'manual_confirmed_at_utc', v_batch.monzo_confirmed_at_utc,
      'manual_confirmed_by_user_id', case when v_batch.monzo_confirmed_by_user_id is null then null else v_batch.monzo_confirmed_by_user_id::text end,

      'monzo_confirmed_at_utc', v_batch.monzo_confirmed_at_utc,
      'monzo_confirmed_by_user_id', case when v_batch.monzo_confirmed_by_user_id is null then null else v_batch.monzo_confirmed_by_user_id::text end,

      'last_status_checked_at_utc', v_batch.last_status_checked_at_utc,

      -- Rail-generic scheduling/execution fields
      'rail_provider_snapshot', v_batch.rail_provider_snapshot,
      'rail_env_snapshot', v_batch.rail_env_snapshot,
      'schedule_kind', v_batch.schedule_kind,
      'scheduled_at_utc', v_batch.scheduled_at_utc,
      'executing_started_at_utc', v_batch.executing_started_at_utc,

      -- Bulk payment reference fields
      'bulk_ref_num', v_batch.bulk_ref_num,
      'bulk_ref_date', case when v_batch.bulk_ref_date is null then null else v_batch.bulk_ref_date::text end,
      'bulk_reference', v_batch.bulk_reference
    ),
    'candidates', v_candidates,
    'transfers', v_transfers,
    'items', v_items
  );
end;
$$;


-- =========================================================
-- A4.3 pay_set_paye_net_from_sage(p_pay_batch_id, p_csv_raw, p_actor_user_id, p_source_filename)
-- =========================================================

create or replace function public.pay_set_paye_net_from_sage(
  p_pay_batch_id uuid,
  p_csv_raw text,
  p_actor_user_id uuid,
  p_source_filename text
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_lines text[];
  v_line text;
  v_fields text[];

  header_idx int := 0;
  i int := 0;

  works_col int := 0;
  net_col int := 0;

  works text;
  works_norm text;
  net_raw text;
  net_amt numeric;

  dup_check jsonb := '[]'::jsonb;
  unknown_check jsonb := '[]'::jsonb;
  ambig_check jsonb := '[]'::jsonb;

begin
  if p_pay_batch_id is null then
    raise exception 'pay_batch_id is required';
  end if;

  -- Ensure batch exists
  if not exists (select 1 from public.pay_batches pb where pb.id = p_pay_batch_id limit 1) then
    raise exception 'pay_batch not found';
  end if;

  v_lines := regexp_split_to_array(coalesce(p_csv_raw,''), E'\\r?\\n');

  -- Find header row containing Works Number and Net Pay
  for i in 1..coalesce(array_length(v_lines,1),0) loop
    v_line := coalesce(v_lines[i],'');
    if btrim(v_line) = '' then continue; end if;

    v_fields := public._pay_csv_parse_line(v_line);

    works_col := 0;
    net_col := 0;

    for header_idx in 1..coalesce(array_length(v_fields,1),0) loop
      if lower(btrim(coalesce(v_fields[header_idx],''))) = lower('Works Number') then
        works_col := header_idx;
      end if;
      if lower(btrim(coalesce(v_fields[header_idx],''))) = lower('Net Pay') then
        net_col := header_idx;
      end if;
    end loop;

    if works_col > 0 and net_col > 0 then
      header_idx := i;
      exit;
    end if;
  end loop;

  if header_idx = 0 then
    raise exception 'SAGE_IMPORT_INVALID: header row with Works Number and Net Pay not found';
  end if;

  -- Parse rows after header
  create temp table if not exists _tmp_sage_net (
    works_number text,
    works_norm text,
    net_amount numeric
  ) on commit drop;

  delete from _tmp_sage_net;

  for i in (header_idx+1)..coalesce(array_length(v_lines,1),0) loop
    v_line := coalesce(v_lines[i],'');
    if btrim(v_line) = '' then continue; end if;

    v_fields := public._pay_csv_parse_line(v_line);

    works := public._pay_csv_trim_field(case when works_col <= array_length(v_fields,1) then v_fields[works_col] else null end);
    if works is null then
      continue; -- ignore blank works number
    end if;

    if lower(works) = 'totals' then
      continue; -- ignore Totals row
    end if;

    works_norm := upper(regexp_replace(btrim(coalesce(works,'')), '\s+', '', 'g'));
    if works_norm = '' then
      continue;
    end if;

    net_raw := public._pay_csv_trim_field(case when net_col <= array_length(v_fields,1) then v_fields[net_col] else null end);
    if net_raw is null then
      raise exception 'SAGE_IMPORT_INVALID: Net Pay missing for Works Number %', works;
    end if;

    -- Normalize numeric: remove commas and currency symbols
    net_raw := replace(net_raw, ',', '');
    net_raw := regexp_replace(net_raw, '[^0-9\\.-]', '', 'g');

    begin
      net_amt := net_raw::numeric;
    exception when others then
      raise exception 'SAGE_IMPORT_INVALID: Net Pay not numeric for Works Number %', works;
    end;

    if net_amt < 0 then
      raise exception 'SAGE_IMPORT_INVALID: Net Pay negative for Works Number %', works;
    end if;

    insert into _tmp_sage_net(works_number, works_norm, net_amount)
    values (works, works_norm, round(net_amt,2));
  end loop;

  -- Validate duplicates (normalized)
  select coalesce(jsonb_agg(t.works_norm), '[]'::jsonb)
  into dup_check
  from (
    select works_norm
    from _tmp_sage_net
    group by works_norm
    having count(*) > 1
  ) t;

  if jsonb_array_length(dup_check) > 0 then
    raise exception 'SAGE_IMPORT_INVALID: duplicate Works Number(s) %', dup_check::text;
  end if;

  -- Build PAYE candidate key map for this batch:
  -- Works Number matches candidate_tms_ref/tms_ref OR candidate.ni_number (normalized: uppercase, remove spaces)
  create temp table if not exists _tmp_batch_paye (
    candidate_id uuid,
    pay_batch_candidate_id uuid,
    tms_ref_norm text,
    ni_norm text
  ) on commit drop;

  delete from _tmp_batch_paye;

  insert into _tmp_batch_paye(candidate_id, pay_batch_candidate_id, tms_ref_norm, ni_norm)
  select
    pbc.candidate_id,
    pbc.id,
    upper(regexp_replace(btrim(coalesce(pbc.candidate_tms_ref, c.tms_ref, '')), '\s+', '', 'g')) as tms_ref_norm,
    upper(regexp_replace(btrim(coalesce(c.ni_number, '')), '\s+', '', 'g')) as ni_norm
  from public.pay_batch_candidates pbc
  join public.candidates c on c.id = pbc.candidate_id
  where pbc.pay_batch_id = p_pay_batch_id
    and pbc.paye_state is not null;

  -- Unknown works numbers (no match by TMS ref or NI)
  select coalesce(jsonb_agg(s.works_number), '[]'::jsonb)
  into unknown_check
  from _tmp_sage_net s
  left join _tmp_batch_paye bp
    on (bp.tms_ref_norm is not null and bp.tms_ref_norm <> '' and bp.tms_ref_norm = s.works_norm)
    or (bp.ni_norm is not null and bp.ni_norm <> '' and bp.ni_norm = s.works_norm)
  where bp.candidate_id is null;

  if jsonb_array_length(unknown_check) > 0 then
    raise exception 'SAGE_IMPORT_INVALID: Works Number(s) not found in batch candidates %', unknown_check::text;
  end if;

  -- Ambiguous works numbers (one works_norm matches multiple batch candidates)
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'works_number', a.works_norm,
        'candidate_ids', a.candidate_ids
      )
    ),
    '[]'::jsonb
  )
  into ambig_check
  from (
    select
      s.works_norm,
      jsonb_agg(distinct bp.candidate_id::text order by bp.candidate_id::text) as candidate_ids,
      count(distinct bp.candidate_id) as cnt
    from _tmp_sage_net s
    join _tmp_batch_paye bp
      on (bp.tms_ref_norm is not null and bp.tms_ref_norm <> '' and bp.tms_ref_norm = s.works_norm)
      or (bp.ni_norm is not null and bp.ni_norm <> '' and bp.ni_norm = s.works_norm)
    group by s.works_norm
    having count(distinct bp.candidate_id) > 1
  ) a;

  if jsonb_array_length(ambig_check) > 0 then
    raise exception 'SAGE_IMPORT_INVALID: ambiguous Works Number(s) %', ambig_check::text;
  end if;

  -- Apply (no partial deletes outside this batch): replace prior SAGE_IMPORT rows for this batch’s PAYE candidates
  delete from public.pay_batch_paye_net_inputs pni
  using public.pay_batch_candidates pbc
  where pni.pay_batch_candidate_id = pbc.id
    and pbc.pay_batch_id = p_pay_batch_id
    and pni.source = 'SAGE_IMPORT';

  with matched as (
    select
      bp.pay_batch_candidate_id,
      s.net_amount
    from _tmp_sage_net s
    join _tmp_batch_paye bp
      on (bp.tms_ref_norm is not null and bp.tms_ref_norm <> '' and bp.tms_ref_norm = s.works_norm)
      or (bp.ni_norm is not null and bp.ni_norm <> '' and bp.ni_norm = s.works_norm)
  )
  insert into public.pay_batch_paye_net_inputs(
    pay_batch_candidate_id, source, net_amount, imported_at_utc, file_name, file_hash
  )
  select distinct
    m.pay_batch_candidate_id,
    'SAGE_IMPORT',
    m.net_amount,
    now(),
    p_source_filename,
    null
  from matched m;

  -- Set PAYE state READY only for PAYE candidates that now have at least one net input row
  update public.pay_batch_candidates pbc
  set paye_state = 'READY'
  where pbc.pay_batch_id = p_pay_batch_id
    and pbc.paye_state is not null
    and exists (
      select 1
      from public.pay_batch_paye_net_inputs pni
      where pni.pay_batch_candidate_id = pbc.id
      limit 1
    );

  return jsonb_build_object('ok', true, 'pay_batch_id', p_pay_batch_id::text, 'source', 'SAGE_IMPORT');
end;
$$;

-- =========================================================
-- A4.4 pay_set_paye_net_manual(p_pay_batch_id, p_entries_json, p_actor_user_id)
-- =========================================================

create or replace function public.pay_set_paye_net_manual(
  p_pay_batch_id uuid,
  p_entries_json jsonb,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_entries jsonb := coalesce(p_entries_json, '[]'::jsonb);
  v_dup jsonb;
  v_ambig jsonb;
begin
  if p_pay_batch_id is null then
    raise exception 'pay_batch_id is required';
  end if;

  if jsonb_typeof(v_entries) <> 'array' then
    raise exception 'entries must be an array';
  end if;

  if not exists (select 1 from public.pay_batches pb where pb.id = p_pay_batch_id limit 1) then
    raise exception 'pay_batch not found';
  end if;

  create temp table if not exists _tmp_manual_net (
    candidate_id uuid,
    works_raw text,
    works_norm text,
    net_amount numeric
  ) on commit drop;

  delete from _tmp_manual_net;

  insert into _tmp_manual_net(candidate_id, works_raw, works_norm, net_amount)
  select
    nullif(e->>'candidate_id','')::uuid as candidate_id,
    nullif(btrim(e->>'tms_ref'), '') as works_raw,
    case
      when nullif(btrim(e->>'tms_ref'), '') is null then null
      else upper(regexp_replace(btrim(e->>'tms_ref'), '\s+', '', 'g'))
    end as works_norm,
    round(coalesce(nullif(e->>'net_amount','')::numeric, 0),2) as net_amount
  from jsonb_array_elements(v_entries) e
  where e is not null and jsonb_typeof(e)='object';

  -- Validate non-negative
  if exists (select 1 from _tmp_manual_net t where t.net_amount < 0 limit 1) then
    raise exception 'MANUAL_NET_INVALID: net_amount must be non-negative';
  end if;

  -- Validate duplicates (by candidate_id or normalized works key)
  select coalesce(jsonb_agg(x), '[]'::jsonb)
  into v_dup
  from (
    select to_jsonb(coalesce(candidate_id::text, works_norm)) as x
    from _tmp_manual_net
    group by coalesce(candidate_id::text, works_norm)
    having count(*) > 1
  ) d;

  if jsonb_array_length(v_dup) > 0 then
    raise exception 'MANUAL_NET_INVALID: duplicate entries %', v_dup::text;
  end if;

  -- Resolve batch PAYE candidates (tms_ref OR ni_number)
  create temp table if not exists _tmp_batch_paye2 (
    pay_batch_candidate_id uuid,
    candidate_id uuid,
    tms_ref_norm text,
    ni_norm text
  ) on commit drop;

  delete from _tmp_batch_paye2;

  insert into _tmp_batch_paye2(pay_batch_candidate_id, candidate_id, tms_ref_norm, ni_norm)
  select
    pbc.id,
    pbc.candidate_id,
    upper(regexp_replace(btrim(coalesce(pbc.candidate_tms_ref, c.tms_ref, '')), '\s+', '', 'g')) as tms_ref_norm,
    upper(regexp_replace(btrim(coalesce(c.ni_number, '')), '\s+', '', 'g')) as ni_norm
  from public.pay_batch_candidates pbc
  join public.candidates c on c.id = pbc.candidate_id
  where pbc.pay_batch_id = p_pay_batch_id
    and pbc.paye_state is not null;

  -- Hard validation: every entry must identify a candidate either by candidate_id OR works_norm
  if exists (
    select 1
    from _tmp_manual_net mn
    where mn.candidate_id is null
      and (mn.works_norm is null or mn.works_norm = '')
    limit 1
  ) then
    raise exception 'MANUAL_NET_INVALID: each entry must include candidate_id or tms_ref/NI (Works Number)';
  end if;

  -- Ambiguity: works_norm matches multiple candidates in the batch
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'works_number', a.works_norm,
        'candidate_ids', a.candidate_ids
      )
    ),
    '[]'::jsonb
  )
  into v_ambig
  from (
    select
      mn.works_norm,
      jsonb_agg(distinct bp.candidate_id::text order by bp.candidate_id::text) as candidate_ids,
      count(distinct bp.candidate_id) as cnt
    from _tmp_manual_net mn
    join _tmp_batch_paye2 bp
      on mn.candidate_id is null
     and mn.works_norm is not null
     and mn.works_norm <> ''
     and (
       (bp.tms_ref_norm is not null and bp.tms_ref_norm <> '' and bp.tms_ref_norm = mn.works_norm)
       or (bp.ni_norm is not null and bp.ni_norm <> '' and bp.ni_norm = mn.works_norm)
     )
    group by mn.works_norm
    having count(distinct bp.candidate_id) > 1
  ) a;

  if jsonb_array_length(v_ambig) > 0 then
    raise exception 'MANUAL_NET_INVALID: ambiguous Works Number(s) %', v_ambig::text;
  end if;

  -- Validate that each entry matches a batch PAYE candidate
  if exists (
    select 1
    from _tmp_manual_net mn
    left join _tmp_batch_paye2 bp
      on (mn.candidate_id is not null and bp.candidate_id = mn.candidate_id)
      or (
        mn.candidate_id is null
        and mn.works_norm is not null
        and mn.works_norm <> ''
        and (
          (bp.tms_ref_norm is not null and bp.tms_ref_norm <> '' and bp.tms_ref_norm = mn.works_norm)
          or (bp.ni_norm is not null and bp.ni_norm <> '' and bp.ni_norm = mn.works_norm)
        )
      )
    where bp.pay_batch_candidate_id is null
    limit 1
  ) then
    raise exception 'MANUAL_NET_INVALID: entry candidate not found in pay batch PAYE candidates';
  end if;

  create temp table if not exists _tmp_manual_match (
    pay_batch_candidate_id uuid,
    net_amount numeric
  ) on commit drop;

  delete from _tmp_manual_match;

  -- candidate_id path
  insert into _tmp_manual_match(pay_batch_candidate_id, net_amount)
  select
    bp.pay_batch_candidate_id,
    mn.net_amount
  from _tmp_manual_net mn
  join _tmp_batch_paye2 bp
    on mn.candidate_id is not null
   and bp.candidate_id = mn.candidate_id;

  -- works_norm path (tms_ref OR NI)
  insert into _tmp_manual_match(pay_batch_candidate_id, net_amount)
  select
    bp.pay_batch_candidate_id,
    mn.net_amount
  from _tmp_manual_net mn
  join _tmp_batch_paye2 bp
    on mn.candidate_id is null
   and mn.works_norm is not null
   and mn.works_norm <> ''
   and (
     (bp.tms_ref_norm is not null and bp.tms_ref_norm <> '' and bp.tms_ref_norm = mn.works_norm)
     or (bp.ni_norm is not null and bp.ni_norm <> '' and bp.ni_norm = mn.works_norm)
   );

  -- Apply: replace prior MANUAL_ENTRY rows for those candidates
  delete from public.pay_batch_paye_net_inputs pni
  using _tmp_batch_paye2 bp
  where pni.pay_batch_candidate_id = bp.pay_batch_candidate_id
    and pni.source = 'MANUAL_ENTRY';

  insert into public.pay_batch_paye_net_inputs(
    pay_batch_candidate_id, source, net_amount, imported_at_utc, file_name, file_hash
  )
  select
    mm.pay_batch_candidate_id,
    'MANUAL_ENTRY',
    mm.net_amount,
    now(),
    null,
    null
  from _tmp_manual_match mm;

  -- Set PAYE state READY only for PAYE candidates that now have at least one net input row
  update public.pay_batch_candidates pbc
  set paye_state = 'READY'
  where pbc.pay_batch_id = p_pay_batch_id
    and pbc.paye_state is not null
    and exists (
      select 1
      from public.pay_batch_paye_net_inputs pni
      where pni.pay_batch_candidate_id = pbc.id
      limit 1
    );

  return jsonb_build_object('ok', true, 'pay_batch_id', p_pay_batch_id::text, 'source', 'MANUAL_ENTRY');
end;
$$;

commit;




begin;

-- =========================================================
-- pay_snooze_upsert
-- Creates a new active snooze, or updates the snooze_until_date/note for an existing ACTIVE snooze
-- matching the identity key (candidate_id + timesheet_id + segment_id + source_ref + snooze_kind).
--
-- Identity rules:
--   - Segment snooze: timesheet_id + segment_id required
--   - Non-segment snooze: source_ref required (timesheet_id optional but recommended)
--
-- snooze_until_date:
--   - NULL => snooze forever
--   - date => snoozed until that date (inclusive)
-- =========================================================
create or replace function public.pay_snooze_upsert(
  p_candidate_id uuid,
  p_timesheet_id uuid,
  p_segment_id text,
  p_source_ref text,
  p_snooze_kind text default 'DO_NOT_PAY',
  p_snooze_until_date date default null,
  p_actor_user_id uuid default null,
  p_note text default null
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_kind text := upper(btrim(coalesce(p_snooze_kind, 'DO_NOT_PAY')));
  v_segment_id text := nullif(btrim(coalesce(p_segment_id, '')), '');
  v_source_ref text := nullif(btrim(coalesce(p_source_ref, '')), '');
  v_note text := nullif(btrim(coalesce(p_note, '')), '');

  v_existing_id uuid := null;
  v_updated_count int := 0;
  v_action text := null;
begin
  if p_candidate_id is null then
    raise exception 'candidate_id is required';
  end if;

  if v_kind not in ('DO_NOT_PAY','BLOCKED') then
    raise exception 'invalid snooze_kind (expected DO_NOT_PAY or BLOCKED)';
  end if;

  -- Enforce identity rules (match table constraint and intended usage)
  if v_segment_id is not null then
    if p_timesheet_id is null then
      raise exception 'timesheet_id is required when segment_id is provided';
    end if;
  end if;

  if v_segment_id is null and v_source_ref is null then
    raise exception 'either (timesheet_id+segment_id) or source_ref must be provided';
  end if;

  -- Disallow nonsensical "snooze until" in the past (keeps UX clean).
  -- If you want "unsnooze" use pay_snooze_clear.
  if p_snooze_until_date is not null and p_snooze_until_date < current_date then
    raise exception 'snooze_until_date must be today or later (or NULL for forever)';
  end if;

  -- Update any ACTIVE snooze rows matching identity (IS NOT DISTINCT FROM makes NULL match NULL)
  update public.pay_item_snoozes s
  set
    snooze_until_date = p_snooze_until_date,
    note = v_note
  where s.candidate_id = p_candidate_id
    and s.cleared_at_utc is null
    and s.snooze_kind = v_kind
    and s.timesheet_id is not distinct from p_timesheet_id
    and s.segment_id is not distinct from v_segment_id
    and s.source_ref is not distinct from v_source_ref;

  get diagnostics v_updated_count = row_count;

  if v_updated_count > 0 then
    -- Return the (first) active id after update
    select s.id
    into v_existing_id
    from public.pay_item_snoozes s
    where s.candidate_id = p_candidate_id
      and s.cleared_at_utc is null
      and s.snooze_kind = v_kind
      and s.timesheet_id is not distinct from p_timesheet_id
      and s.segment_id is not distinct from v_segment_id
      and s.source_ref is not distinct from v_source_ref
    order by s.created_at_utc desc, s.id desc
    limit 1;

    v_action := 'UPDATED';
  else
    insert into public.pay_item_snoozes (
      candidate_id,
      timesheet_id,
      segment_id,
      source_ref,
      snooze_kind,
      snooze_until_date,
      created_at_utc,
      created_by_user_id,
      note,
      cleared_at_utc,
      cleared_by_user_id
    )
    values (
      p_candidate_id,
      p_timesheet_id,
      v_segment_id,
      v_source_ref,
      v_kind,
      p_snooze_until_date,
      now(),
      p_actor_user_id,
      v_note,
      null,
      null
    )
    returning id into v_existing_id;

    v_action := 'CREATED';
  end if;

  return jsonb_build_object(
    'ok', true,
    'action', v_action,
    'id', v_existing_id::text,
    'candidate_id', p_candidate_id::text,
    'timesheet_id', case when p_timesheet_id is null then null else p_timesheet_id::text end,
    'segment_id', v_segment_id,
    'source_ref', v_source_ref,
    'snooze_kind', v_kind,
    'snooze_until_date', case when p_snooze_until_date is null then null else p_snooze_until_date::text end,
    'note', v_note
  );
end;
$$;

-- =========================================================
-- pay_snooze_clear
-- Clears (deactivates) a snooze by id (audit-safe; does not delete).
-- =========================================================
create or replace function public.pay_snooze_clear(
  p_snooze_id uuid,
  p_actor_user_id uuid default null
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_row record;
begin
  if p_snooze_id is null then
    raise exception 'snooze_id is required';
  end if;

  select
    s.id,
    s.candidate_id,
    s.timesheet_id,
    s.segment_id,
    s.source_ref,
    s.snooze_kind,
    s.snooze_until_date,
    s.created_at_utc,
    s.cleared_at_utc
  into v_row
  from public.pay_item_snoozes s
  where s.id = p_snooze_id
  limit 1;

  if not found then
    raise exception 'SNOOZE_NOT_FOUND';
  end if;

  if v_row.cleared_at_utc is not null then
    return jsonb_build_object(
      'ok', true,
      'action', 'NOOP_ALREADY_CLEARED',
      'id', v_row.id::text
    );
  end if;

  update public.pay_item_snoozes s
  set
    cleared_at_utc = now(),
    cleared_by_user_id = p_actor_user_id
  where s.id = p_snooze_id;

  return jsonb_build_object(
    'ok', true,
    'action', 'CLEARED',
    'id', v_row.id::text,
    'candidate_id', v_row.candidate_id::text,
    'timesheet_id', case when v_row.timesheet_id is null then null else v_row.timesheet_id::text end,
    'segment_id', v_row.segment_id,
    'source_ref', v_row.source_ref,
    'snooze_kind', v_row.snooze_kind
  );
end;
$$;

-- =========================================================
-- pay_snoozes_list (optional)
-- Lists snoozes with paging. By default returns only ACTIVE snoozes.
-- =========================================================
create or replace function public.pay_snoozes_list(
  p_candidate_id uuid default null,
  p_active_only boolean default true,
  p_limit int default 200,
  p_offset int default 0
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_limit int := greatest(1, least(coalesce(p_limit,200), 500));
  v_offset int := greatest(coalesce(p_offset,0), 0);
  v_total int := 0;
  v_rows jsonb := '[]'::jsonb;
begin
  select count(*)::int
  into v_total
  from public.pay_item_snoozes s
  where (p_candidate_id is null or s.candidate_id = p_candidate_id)
    and (coalesce(p_active_only,true) = false or s.cleared_at_utc is null);

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'id', s.id::text,
        'candidate_id', s.candidate_id::text,
        'timesheet_id', case when s.timesheet_id is null then null else s.timesheet_id::text end,
        'segment_id', s.segment_id,
        'source_ref', s.source_ref,
        'snooze_kind', s.snooze_kind,
        'snooze_until_date', case when s.snooze_until_date is null then null else s.snooze_until_date::text end,
        'note', s.note,
        'created_at_utc', s.created_at_utc,
        'created_by_user_id', case when s.created_by_user_id is null then null else s.created_by_user_id::text end,
        'cleared_at_utc', s.cleared_at_utc,
        'cleared_by_user_id', case when s.cleared_by_user_id is null then null else s.cleared_by_user_id::text end
      )
      order by s.created_at_utc desc, s.id desc
    ),
    '[]'::jsonb
  )
  into v_rows
  from (
    select s.*
    from public.pay_item_snoozes s
    where (p_candidate_id is null or s.candidate_id = p_candidate_id)
      and (coalesce(p_active_only,true) = false or s.cleared_at_utc is null)
    order by s.created_at_utc desc, s.id desc
    limit v_limit offset v_offset
  ) s;

  return jsonb_build_object(
    'ok', true,
    'total_count', v_total,
    'limit', v_limit,
    'offset', v_offset,
    'rows', v_rows
  );
end;
$$;

commit;



begin;

-- =========================================================
-- A4.1 pay_preview(p_pay_date date, p_actor_user_id uuid)
-- UPDATED LOGIC FULLY APPLIED:
--  - Uses effective require_reference_to_pay = COALESCE(CASE WHEN ct.overrideclientsettings THEN ct.require_reference_to_pay END, cs.pay_reference_required, false)
--  - Segment-level reference gating using invoice_breakdown_json.segments[].ref_num (fallback uses timesheets.reference_number for non-SEGMENTS mode)
--  - Returns BLOCKED items (missing ref_num where required AND delta > 0) instead of filtering them out
--  - Returns DO_NOT_PAY items (exclude_from_pay=true), including those with no delta (informational)
--  - Applies snoozing via public.pay_item_snoozes:
--      * snooze_kind='BLOCKED' for blocked items
--      * snooze_kind='DO_NOT_PAY' for do-not-pay items
--    Active snooze: cleared_at_utc IS NULL AND (snooze_until_date IS NULL OR snooze_until_date >= p_pay_date)
--    Suppression rule:
--      * BLOCKED: suppressed if snoozed (always)
--      * DO_NOT_PAY: suppressed if snoozed AND raw_delta = 0 (informational-only)
--  - Payable deltas exclude blocked-positive segments (delta forced to 0) but still show them in blocked_items list
-- =========================================================


create or replace function public.pay_preview(p_pay_date date, p_actor_user_id uuid)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_week_start date := public._pay_week_start_monday(p_pay_date);

  -- ✅ UK “today” anchor for eligibility window (Option A)
  v_today_uk date := (now() at time zone 'Europe/London')::date;

  -- ✅ Eligibility window knobs (from settings_defaults; fallback to defaults if column absent)
  v_pay_eligibility_months_back int := 6;
  v_pay_eligibility_weeks_ahead int := 2;

  -- ✅ Computed eligibility window
  v_eligibility_from_date date;
  v_eligibility_to_date date;

  v_vat_rate_pct numeric;
  v_erni_pct numeric;

  -- ✅ Settings (rail defaults + scheduling defaults)
  v_rail_provider_default text;
  v_rail_env_default text;
  v_rail_supports_scheduling boolean;
  v_rail_supports_name_check boolean;
  v_rail_supports_auto_execute boolean;
  v_default_schedule_umbrella_local text;
  v_default_schedule_paye_local text;
  v_funds_warning_hours_json jsonb;

  v_paye jsonb := '[]'::jsonb;
  v_nonpaye jsonb := '[]'::jsonb;

  v_blocked jsonb := '[]'::jsonb;
  v_do_not_pay jsonb := '[]'::jsonb;
  v_snoozed jsonb := '[]'::jsonb;
begin
  if p_pay_date is null then
    raise exception 'pay_date is required';
  end if;

  if to_regclass('public.settings_finance_windows') is null then
    raise exception 'settings_finance_windows missing';
  end if;

  select
    sfw.vat_rate_pct,
    sfw.erni_pct
  into
    v_vat_rate_pct,
    v_erni_pct
  from public.settings_finance_windows sfw
  where p_pay_date >= sfw.date_from
    and p_pay_date <= coalesce(sfw.date_to, 'infinity'::date)
  order by sfw.date_from desc
  limit 1;

  if v_vat_rate_pct is null or v_erni_pct is null then
    raise exception 'No finance window found for pay_date %', p_pay_date;
  end if;

  -- ✅ Load rail/scheduling defaults for UI (single-row settings table)
  select
    sd.rail_provider_default,
    sd.rail_env_default,
    sd.rail_supports_scheduling,
    sd.rail_supports_name_check,
    sd.rail_supports_auto_execute,
    sd.default_schedule_umbrella_local,
    sd.default_schedule_paye_local,
    sd.funds_warning_hours_json
  into
    v_rail_provider_default,
    v_rail_env_default,
    v_rail_supports_scheduling,
    v_rail_supports_name_check,
    v_rail_supports_auto_execute,
    v_default_schedule_umbrella_local,
    v_default_schedule_paye_local,
    v_funds_warning_hours_json
  from public.settings_defaults sd
  order by sd.id asc
  limit 1;

  if v_rail_provider_default is null or v_rail_env_default is null then
    raise exception 'settings_defaults missing or not populated';
  end if;

  -- ✅ Load eligibility window knobs from settings_defaults (Option A)
  -- NOTE: use exception guard so the function is robust even if the column is absent at runtime.
  begin
    select
      sd.pay_eligibility_months_back,
      sd.pay_eligibility_weeks_ahead
    into
      v_pay_eligibility_months_back,
      v_pay_eligibility_weeks_ahead
    from public.settings_defaults sd
    order by sd.id asc
    limit 1;
  exception when undefined_column then
    v_pay_eligibility_months_back := 6;
    v_pay_eligibility_weeks_ahead := 2;
  end;

  v_pay_eligibility_months_back := greatest(0, least(120, coalesce(v_pay_eligibility_months_back, 6)));
  v_pay_eligibility_weeks_ahead := greatest(0, least(52, coalesce(v_pay_eligibility_weeks_ahead, 2)));

  v_eligibility_from_date := (v_today_uk - (v_pay_eligibility_months_back::text || ' months')::interval)::date;
  v_eligibility_to_date   := (v_today_uk + (v_pay_eligibility_weeks_ahead::text || ' weeks')::interval)::date;

  with active_snoozes as (
    select
      s.id as snooze_id,
      s.candidate_id,
      s.timesheet_id,
      s.segment_id,
      s.source_ref,
      upper(coalesce(s.snooze_kind,'')) as snooze_kind,
      s.snooze_until_date,
      s.note
    from public.pay_item_snoozes s
    where s.cleared_at_utc is null
      and (
        s.snooze_until_date is null
        or s.snooze_until_date >= p_pay_date
      )
  ),
  eligible_tsfin as (
    select
      tf.timesheet_id,
      tf.id as tsfin_id,
      tf.candidate_id,
      tf.client_id,
      upper(coalesce(tf.pay_method,'')) as ts_pay_method,
      upper(coalesce(c.pay_method,''))  as cand_pay_method,
      c.tms_ref as cand_tms_ref,
      c.display_name as cand_display_name,
      c.umbrella_id as cand_umbrella_id,

      -- ✅ Bank readiness (candidate)
      c.bank_details_hash as cand_bank_hash,

      ts.authorised_at_server,
      ts.contract_id,
      ts.reference_number,

      -- ✅ Effective require_reference_to_pay (overrideclientsettings-aware)
      coalesce(
        case when ct.overrideclientsettings then ct.require_reference_to_pay end,
        cs.pay_reference_required,
        false
      ) as require_reference_to_pay,

      coalesce(tf.total_pay_ex_vat,0) as total_pay_ex_vat,
      coalesce(tf.expenses_pay_ex_vat,0) as expenses_pay_ex_vat,
      coalesce(tf.travel_pay_ex_vat,0) as travel_pay_ex_vat,
      coalesce(tf.accommodation_pay_ex_vat,0) as accommodation_pay_ex_vat,
      coalesce(tf.other_pay_ex_vat,0) as other_pay_ex_vat,
      coalesce(tf.mileage_pay_ex_vat,0) as mileage_pay_ex_vat,

      tf.invoice_breakdown_json,

      tps.last_settled_snapshot_json
    from public.timesheets_financials tf
    join public.timesheets ts
      on ts.timesheet_id = tf.timesheet_id
     and ts.is_current = true
    join public.candidates c
      on c.id = tf.candidate_id
    left join public.contracts ct
      on ct.id = ts.contract_id
    left join public.client_settings cs
      on cs.client_id = tf.client_id
    left join public.timesheet_pay_state tps
      on tps.timesheet_id = tf.timesheet_id
    where tf.is_current = true
      and coalesce(tf.pay_on_hold,false) = false
      and ts.authorised_at_server is not null
      and coalesce(tf.has_rate_issue,false) = false
      and coalesce(tf.has_pay_channel_issue,false) = false
      and upper(coalesce(tf.processing_status,'')) not in ('UNASSIGNED','CLIENT_UNRESOLVED','RATE_MISSING','PAY_CHANNEL_MISSING')
      and upper(coalesce(c.pay_method,'')) in ('PAYE','UMBRELLA')

      -- ✅ Option A: eligibility window is controlled by Settings (relative to UK “today”),
      -- NOT scoped by pay_date week.
      and ts.week_ending_date::date >= v_eligibility_from_date
      and ts.week_ending_date::date <= v_eligibility_to_date
  ),
  umb_map as (
    select
      u.id as umbrella_id,
      coalesce(u.enabled,false) as umb_enabled,
      coalesce(u.vat_chargeable,false) as vat_chargeable,
      -- ✅ Bank readiness (umbrella)
      u.bank_details_hash as umb_bank_hash
    from public.umbrellas u
  ),
  adj as (
    select
      a.id as adj_id,
      a.timesheet_id,
      a.candidate_id,
      round(coalesce(a.delta_pay_ex_vat,0),2) as delta_pay_ex_vat
    from public.ts_pay_adjustments a
    where a.as_advance = false
      and a.timesheet_id is not null
  ),
  ts_current as (
    select
      e.candidate_id,
      e.timesheet_id,
      e.tsfin_id,
      e.client_id,
      e.ts_pay_method,
      e.cand_pay_method,
      e.cand_tms_ref,
      e.cand_display_name,
      e.cand_umbrella_id,
      e.cand_bank_hash,
      e.reference_number,
      e.require_reference_to_pay,

      coalesce(um.vat_chargeable,false) as umb_vat_chargeable,
      coalesce(um.umb_enabled,false) as umb_enabled,
      um.umb_bank_hash,

      -- ✅ segments include ref_num (key = ref_num)
      case
        when e.invoice_breakdown_json is not null
         and jsonb_typeof(e.invoice_breakdown_json) = 'object'
         and upper(coalesce(e.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
         and jsonb_typeof(e.invoice_breakdown_json->'segments') = 'array'
        then (
          select coalesce(jsonb_agg(
            jsonb_build_object(
              'segment_id', nullif(btrim(coalesce(seg->>'segment_id','')), ''),
              'pay_amount', round(coalesce(nullif(seg->>'pay_amount','')::numeric,0),2),
              'exclude_from_pay', coalesce(nullif(seg->>'exclude_from_pay','')::boolean, false),
              'ref_num', nullif(btrim(coalesce(seg->>'ref_num','')), '')
            )
          ), '[]'::jsonb)
          from jsonb_array_elements(e.invoice_breakdown_json->'segments') seg
          where seg is not null and jsonb_typeof(seg)='object'
        )
        else jsonb_build_array(
          jsonb_build_object(
            'segment_id', ('ts:' || e.timesheet_id::text),
            'pay_amount', round(coalesce(e.total_pay_ex_vat,0),2),
            'exclude_from_pay', false,
            'ref_num', nullif(btrim(coalesce(e.reference_number,'')), '')
          )
        )
      end as current_segments_json,

      case
        when e.invoice_breakdown_json is not null
         and jsonb_typeof(e.invoice_breakdown_json)='object'
         and upper(coalesce(e.invoice_breakdown_json->>'mode',''))='SEGMENTS'
        then round(coalesce(nullif(e.invoice_breakdown_json #>> '{additional,pay_ex_vat}','')::numeric,0),2)
        else 0::numeric
      end as current_additional_pay_ex_vat,

      round(coalesce(e.expenses_pay_ex_vat,0),2) as current_expenses_pay_ex_vat,
      round(coalesce(e.travel_pay_ex_vat,0),2) as current_travel_pay_ex_vat,
      round(coalesce(e.accommodation_pay_ex_vat,0),2) as current_accommodation_pay_ex_vat,
      round(coalesce(e.other_pay_ex_vat,0),2) as current_other_pay_ex_vat,
      round(coalesce(e.mileage_pay_ex_vat,0),2) as current_mileage_pay_ex_vat,

      coalesce(
        (
          select jsonb_agg(
            jsonb_build_object(
              'id', a.adj_id::text,
              'delta_pay_ex_vat', a.delta_pay_ex_vat
            )
          )
          from adj a
          where a.timesheet_id = e.timesheet_id
        ),
        '[]'::jsonb
      ) as current_adjustments_json,

      e.last_settled_snapshot_json
    from eligible_tsfin e
    left join umb_map um
      on um.umbrella_id = e.cand_umbrella_id
  ),
  ts_baseline as (
    select
      t.candidate_id,
      t.timesheet_id,
      t.ts_pay_method,
      t.cand_pay_method,
      t.cand_tms_ref,
      t.cand_display_name,
      t.cand_umbrella_id,
      t.umb_enabled,
      t.umb_vat_chargeable,
      t.require_reference_to_pay,

      -- ✅ bank readiness propagation
      t.cand_bank_hash,
      t.umb_bank_hash,

      coalesce(t.last_settled_snapshot_json, '{}'::jsonb) as base_json,

      coalesce(t.current_segments_json, '[]'::jsonb) as current_segments_json,
      coalesce(t.current_adjustments_json, '[]'::jsonb) as current_adjustments_json,

      t.current_additional_pay_ex_vat,
      t.current_expenses_pay_ex_vat,
      t.current_travel_pay_ex_vat,
      t.current_accommodation_pay_ex_vat,
      t.current_other_pay_ex_vat,
      t.current_mileage_pay_ex_vat
    from ts_current t
  ),
  segment_status as (
    select
      b.candidate_id,
      b.timesheet_id,
      b.require_reference_to_pay,
      ids.segment_id,

      coalesce(cur.pay_amount, 0)::numeric(12,2) as cur_pay_amount,
      coalesce(cur.exclude_from_pay, false) as cur_exclude_from_pay,
      cur.ref_num as cur_ref_num,

      coalesce(bas.pay_amount, 0)::numeric(12,2) as bas_pay_amount,
      coalesce(bas.exclude_from_pay, false) as bas_exclude_from_pay,

      (case when coalesce(cur.exclude_from_pay,false) then 0 else coalesce(cur.pay_amount,0) end)::numeric(12,2) as cur_payable,
      (case when coalesce(bas.exclude_from_pay,false) then 0 else coalesce(bas.pay_amount,0) end)::numeric(12,2) as bas_payable,

      round(
        (case when coalesce(cur.exclude_from_pay,false) then 0 else coalesce(cur.pay_amount,0) end)
        -
        (case when coalesce(bas.exclude_from_pay,false) then 0 else coalesce(bas.pay_amount,0) end),
        2
      ) as raw_delta_ex,

      (coalesce(cur.exclude_from_pay,false) = true) as is_do_not_pay,

      (b.require_reference_to_pay = true and coalesce(cur.exclude_from_pay,false) = false and nullif(btrim(coalesce(cur.ref_num,'')),'') is null) as is_ref_missing,

      (
        b.require_reference_to_pay = true
        and coalesce(cur.exclude_from_pay,false) = false
        and nullif(btrim(coalesce(cur.ref_num,'')),'') is null
        and round(
              (case when coalesce(cur.exclude_from_pay,false) then 0 else coalesce(cur.pay_amount,0) end)
              -
              (case when coalesce(bas.exclude_from_pay,false) then 0 else coalesce(bas.pay_amount,0) end),
              2
            ) > 0
      ) as is_blocked,

      case
        when (
          b.require_reference_to_pay = true
          and coalesce(cur.exclude_from_pay,false) = false
          and nullif(btrim(coalesce(cur.ref_num,'')),'') is null
          and round(
                (case when coalesce(cur.exclude_from_pay,false) then 0 else coalesce(cur.pay_amount,0) end)
                -
                (case when coalesce(bas.exclude_from_pay,false) then 0 else coalesce(bas.pay_amount,0) end),
                2
              ) > 0
        )
        then 0::numeric
        else round(
          (case when coalesce(cur.exclude_from_pay,false) then 0 else coalesce(cur.pay_amount,0) end)
          -
          (case when coalesce(bas.exclude_from_pay,false) then 0 else coalesce(bas.pay_amount,0) end),
          2
        )
      end as eff_delta_ex
    from ts_baseline b
    join lateral (
      select distinct segment_id
      from (
        select nullif(btrim(coalesce(s->>'segment_id','')),'') as segment_id
        from jsonb_array_elements(b.current_segments_json) s
        where s is not null and jsonb_typeof(s)='object'
        union
        select nullif(btrim(coalesce(s->>'segment_id','')),'') as segment_id
        from jsonb_array_elements(coalesce(b.base_json->'segments','[]'::jsonb)) s
        where s is not null and jsonb_typeof(s)='object'
      ) u
      where segment_id is not null
    ) ids on true
    left join lateral (
      select
        round(coalesce(nullif(s->>'pay_amount','')::numeric,0),2) as pay_amount,
        coalesce(nullif(s->>'exclude_from_pay','')::boolean,false) as exclude_from_pay,
        nullif(btrim(coalesce(s->>'ref_num','')),'') as ref_num
      from jsonb_array_elements(b.current_segments_json) s
      where s is not null
        and jsonb_typeof(s)='object'
        and nullif(btrim(coalesce(s->>'segment_id','')),'') = ids.segment_id
      limit 1
    ) cur on true
    left join lateral (
      select
        round(coalesce(nullif(s->>'pay_amount','')::numeric,0),2) as pay_amount,
        coalesce(nullif(s->>'exclude_from_pay','')::boolean,false) as exclude_from_pay
      from jsonb_array_elements(coalesce(b.base_json->'segments','[]'::jsonb)) s
      where s is not null
        and jsonb_typeof(s)='object'
        and nullif(btrim(coalesce(s->>'segment_id','')),'') = ids.segment_id
      limit 1
    ) bas on true
  ),
  blocked_items_all as (
    select
      ss.candidate_id,
      ss.timesheet_id,
      ss.segment_id,
      ss.cur_ref_num as ref_num,
      ss.require_reference_to_pay,
      ss.raw_delta_ex as blocked_delta_ex,
      sn.snooze_id,
      sn.snooze_until_date,
      sn.note
    from segment_status ss
    left join active_snoozes sn
      on sn.candidate_id = ss.candidate_id
     and sn.timesheet_id is not distinct from ss.timesheet_id
     and sn.segment_id is not distinct from ss.segment_id
     and sn.snooze_kind = 'BLOCKED'
    where ss.is_blocked = true
  ),
  blocked_items as (
    select
      b.candidate_id,
      b.timesheet_id,
      b.segment_id,
      b.ref_num,
      b.require_reference_to_pay,
      b.blocked_delta_ex,
      b.snooze_id
    from blocked_items_all b
    where b.snooze_id is null
  ),
  blocked_items_snoozed as (
    select
      b.candidate_id,
      b.timesheet_id,
      b.segment_id,
      b.ref_num,
      b.require_reference_to_pay,
      b.blocked_delta_ex,
      b.snooze_id,
      b.snooze_until_date,
      b.note
    from blocked_items_all b
    where b.snooze_id is not null
  ),
  do_not_pay_all as (
    select
      ss.candidate_id,
      ss.timesheet_id,
      ss.segment_id,
      ss.cur_ref_num as ref_num,
      ss.raw_delta_ex as raw_delta_ex,
      sn.snooze_id,
      sn.snooze_until_date,
      sn.note
    from segment_status ss
    left join active_snoozes sn
      on sn.candidate_id = ss.candidate_id
     and sn.timesheet_id is not distinct from ss.timesheet_id
     and sn.segment_id is not distinct from ss.segment_id
     and sn.snooze_kind = 'DO_NOT_PAY'
    where ss.is_do_not_pay = true
  ),
  do_not_pay_items as (
    select
      d.candidate_id,
      d.timesheet_id,
      d.segment_id,
      d.ref_num,
      d.raw_delta_ex,
      d.snooze_id
    from do_not_pay_all d
    where d.snooze_id is null
       or coalesce(d.raw_delta_ex,0) <> 0
  ),
  do_not_pay_items_snoozed as (
    select
      d.candidate_id,
      d.timesheet_id,
      d.segment_id,
      d.ref_num,
      d.raw_delta_ex,
      d.snooze_id,
      d.snooze_until_date,
      d.note
    from do_not_pay_all d
    where d.snooze_id is not null
      and coalesce(d.raw_delta_ex,0) = 0
  ),
  ts_deltas as (
    select
      b.candidate_id,
      b.timesheet_id,
      b.ts_pay_method,
      b.cand_pay_method,
      b.cand_tms_ref,
      b.cand_display_name,
      b.cand_umbrella_id,
      b.umb_enabled,
      b.umb_vat_chargeable,

      -- ✅ bank readiness propagation
      b.cand_bank_hash,
      b.umb_bank_hash,

      coalesce(
        (
          select jsonb_agg(
            jsonb_build_object(
              'segment_id', ss.segment_id,
              'delta_pay_ex_vat', ss.eff_delta_ex
            )
          )
          from segment_status ss
          where ss.timesheet_id = b.timesheet_id
            and ss.candidate_id = b.candidate_id
            and coalesce(ss.eff_delta_ex,0) <> 0
        ),
        '[]'::jsonb
      ) as segment_deltas_json,

      round(
        b.current_additional_pay_ex_vat
        -
        coalesce(nullif(b.base_json->>'additional_pay_ex_vat','')::numeric,0),
        2
      ) as delta_additional_pay_ex_vat,

      round(b.current_expenses_pay_ex_vat - coalesce(nullif(b.base_json #>> '{expenses,expenses_pay_ex_vat}','')::numeric,0),2) as delta_expenses_pay_ex_vat,
      round(b.current_travel_pay_ex_vat   - coalesce(nullif(b.base_json #>> '{expenses,travel_pay_ex_vat}','')::numeric,0),2) as delta_travel_pay_ex_vat,
      round(b.current_accommodation_pay_ex_vat - coalesce(nullif(b.base_json #>> '{expenses,accommodation_pay_ex_vat}','')::numeric,0),2) as delta_accommodation_pay_ex_vat,
      round(b.current_other_pay_ex_vat    - coalesce(nullif(b.base_json #>> '{expenses,other_pay_ex_vat}','')::numeric,0),2) as delta_other_pay_ex_vat,
      round(b.current_mileage_pay_ex_vat  - coalesce(nullif(b.base_json #>> '{expenses,mileage_pay_ex_vat}','')::numeric,0),2) as delta_mileage_pay_ex_vat,

      (
        select coalesce(jsonb_agg(d), '[]'::jsonb)
        from (
          with cur as (
            select
              nullif(btrim(coalesce(a->>'id','')),'') as adj_id,
              round(coalesce(nullif(a->>'delta_pay_ex_vat','')::numeric,0),2) as amt
            from jsonb_array_elements(b.current_adjustments_json) a
            where a is not null and jsonb_typeof(a)='object'
          ),
          bas as (
            select
              nullif(btrim(coalesce(a->>'id','')),'') as adj_id,
              round(coalesce(nullif(a->>'delta_pay_ex_vat','')::numeric,0),2) as amt
            from jsonb_array_elements(coalesce(b.base_json->'adjustments','[]'::jsonb)) a
            where a is not null and jsonb_typeof(a)='object'
          ),
          ids as (
            select adj_id from cur where adj_id is not null
            union
            select adj_id from bas where adj_id is not null
          )
          select jsonb_build_object(
            'adj_id', i.adj_id,
            'delta_pay_ex_vat',
              round(
                coalesce((select c.amt from cur c where c.adj_id=i.adj_id),0)
                -
                coalesce((select p.amt from bas p where p.adj_id=i.adj_id),0),
                2
              )
          ) as d
          from ids i
        ) q
        where coalesce(nullif(q.d->>'delta_pay_ex_vat','')::numeric,0) <> 0
      ) as adjustment_deltas_json
    from ts_baseline b
  ),
  candidate_rollup as (
    select
      d.candidate_id,
      max(d.cand_tms_ref) as cand_tms_ref,
      max(d.cand_display_name) as cand_display_name,
      max(d.cand_pay_method) as cand_pay_method,
      max(d.cand_umbrella_id) as cand_umbrella_id,
      bool_or(d.umb_enabled) as umb_enabled,
      bool_or(d.umb_vat_chargeable) as umb_vat_chargeable,

      -- ✅ bank readiness rollups
      bool_or(d.cand_bank_hash is not null and btrim(d.cand_bank_hash) <> '') as candidate_has_bank_details,
      max(d.cand_bank_hash) as candidate_bank_hash,
      bool_or(d.umb_bank_hash is not null and btrim(d.umb_bank_hash) <> '') as umbrella_has_bank_details,
      max(d.umb_bank_hash) as umbrella_bank_hash,

      bool_or(
        d.ts_pay_method <> d.cand_pay_method
        and (
          jsonb_array_length(d.segment_deltas_json) > 0
          or jsonb_array_length(d.adjustment_deltas_json) > 0
          or d.delta_additional_pay_ex_vat <> 0
          or d.delta_expenses_pay_ex_vat <> 0
          or d.delta_travel_pay_ex_vat <> 0
          or d.delta_accommodation_pay_ex_vat <> 0
          or d.delta_other_pay_ex_vat <> 0
          or d.delta_mileage_pay_ex_vat <> 0
        )
      ) as has_mismatch,

      round(
        sum(
          case when d.ts_pay_method = d.cand_pay_method then
            coalesce((select sum(coalesce(nullif(x->>'delta_pay_ex_vat','')::numeric,0)) from jsonb_array_elements(d.segment_deltas_json) x),0)
            + coalesce(d.delta_additional_pay_ex_vat,0)
            + coalesce(d.delta_expenses_pay_ex_vat,0)
            + coalesce(d.delta_travel_pay_ex_vat,0)
            + coalesce(d.delta_accommodation_pay_ex_vat,0)
            + coalesce(d.delta_other_pay_ex_vat,0)
            + coalesce(d.delta_mileage_pay_ex_vat,0)
            + coalesce((select sum(coalesce(nullif(x->>'delta_pay_ex_vat','')::numeric,0)) from jsonb_array_elements(d.adjustment_deltas_json) x),0)
          else 0 end
        ),
        2
      ) as non_mismatch_total_ex,

      round(
        sum(
          case when d.ts_pay_method <> d.cand_pay_method and d.ts_pay_method = 'PAYE' then
            coalesce((select sum(coalesce(nullif(x->>'delta_pay_ex_vat','')::numeric,0)) from jsonb_array_elements(d.segment_deltas_json) x),0)
            + coalesce(d.delta_additional_pay_ex_vat,0)
            + coalesce(d.delta_expenses_pay_ex_vat,0)
            + coalesce(d.delta_travel_pay_ex_vat,0)
            + coalesce(d.delta_accommodation_pay_ex_vat,0)
            + coalesce(d.delta_other_pay_ex_vat,0)
            + coalesce(d.delta_mileage_pay_ex_vat,0)
            + coalesce((select sum(coalesce(nullif(x->>'delta_pay_ex_vat','')::numeric,0)) from jsonb_array_elements(d.adjustment_deltas_json) x),0)
          else 0 end
        ),
        2
      ) as mismatch_source_paye_ex,

      round(
        sum(
          case when d.ts_pay_method <> d.cand_pay_method and d.ts_pay_method = 'UMBRELLA' then
            coalesce((select sum(coalesce(nullif(x->>'delta_pay_ex_vat','')::numeric,0)) from jsonb_array_elements(d.segment_deltas_json) x),0)
            + coalesce(d.delta_additional_pay_ex_vat,0)
            + coalesce(d.delta_expenses_pay_ex_vat,0)
            + coalesce(d.delta_travel_pay_ex_vat,0)
            + coalesce(d.delta_accommodation_pay_ex_vat,0)
            + coalesce(d.delta_other_pay_ex_vat,0)
            + coalesce(d.delta_mileage_pay_ex_vat,0)
            + coalesce((select sum(coalesce(nullif(x->>'delta_pay_ex_vat','')::numeric,0)) from jsonb_array_elements(d.adjustment_deltas_json) x),0)
          else 0 end
        ),
        2
      ) as mismatch_source_umbrella_ex,

      coalesce(
        jsonb_agg(
          jsonb_build_object(
            'timesheet_id', d.timesheet_id::text,
            'source_pay_method', d.ts_pay_method,
            'segment_deltas', d.segment_deltas_json,
            'adjustment_deltas', d.adjustment_deltas_json,
            'delta_additional_pay_ex_vat', d.delta_additional_pay_ex_vat,
            'delta_expenses_pay_ex_vat', d.delta_expenses_pay_ex_vat,
            'delta_travel_pay_ex_vat', d.delta_travel_pay_ex_vat,
            'delta_accommodation_pay_ex_vat', d.delta_accommodation_pay_ex_vat,
            'delta_other_pay_ex_vat', d.delta_other_pay_ex_vat,
            'delta_mileage_pay_ex_vat', d.delta_mileage_pay_ex_vat
          )
        ) filter (where
          jsonb_array_length(d.segment_deltas_json) > 0
          or jsonb_array_length(d.adjustment_deltas_json) > 0
          or d.delta_additional_pay_ex_vat <> 0
          or d.delta_expenses_pay_ex_vat <> 0
          or d.delta_travel_pay_ex_vat <> 0
          or d.delta_accommodation_pay_ex_vat <> 0
          or d.delta_other_pay_ex_vat <> 0
          or d.delta_mileage_pay_ex_vat <> 0
        ),
        '[]'::jsonb
      ) as timesheets_itemisation
    from ts_deltas d
    group by d.candidate_id
  ),
  blocked_counts as (
    select bi.candidate_id, count(*)::int as blocked_count
    from blocked_items bi
    group by bi.candidate_id
  ),
  do_not_pay_counts as (
    select di.candidate_id, count(*)::int as do_not_pay_count
    from do_not_pay_items di
    group by di.candidate_id
  ),
  -- ✅ Loan catch-up (Option A): include ALL schedule entries week_start <= v_week_start and amount < 0
  loan_due as (
    select
      pa.candidate_id,
      round(
        sum(
          abs(coalesce(nullif(e->>'amount','')::numeric,0))
        ),
        2
      ) as loan_due_total,
      jsonb_agg(
        jsonb_build_object(
          'advance_id', pa.id::text,
          'week_start', (nullif(e->>'week_start','')::date)::text,
          'due_amount', round(abs(coalesce(nullif(e->>'amount','')::numeric,0)),2),
          'reason', pa.reason::text,
          -- legacy alias (kept to avoid breaking any existing consumer)
          'scheduled_amount', round(abs(coalesce(nullif(e->>'amount','')::numeric,0)),2)
        )
        order by (nullif(e->>'week_start','')::date) asc nulls last, pa.created_at asc, pa.id
      ) as loan_due_entries
    from public.pay_advances pa
    join lateral jsonb_array_elements(coalesce(pa.schedule_json,'[]'::jsonb)) e on true
    where pa.status::text = 'ACTIVE'
      and nullif(e->>'week_start','') is not null
      and (nullif(e->>'week_start','')::date) <= v_week_start
      and coalesce(nullif(e->>'amount','')::numeric,0) < 0
    group by pa.candidate_id
  ),
  cand_enriched as (
    select
      cr.candidate_id,
      cr.cand_tms_ref,
      cr.cand_display_name,
      cr.cand_pay_method,
      cr.cand_umbrella_id,
      cr.umb_enabled,
      cr.umb_vat_chargeable,
      cr.has_mismatch,
      cr.non_mismatch_total_ex,
      cr.mismatch_source_paye_ex,
      cr.mismatch_source_umbrella_ex,
      cr.timesheets_itemisation,
      coalesce(bc.blocked_count,0) as blocked_count,
      coalesce(dpc.do_not_pay_count,0) as do_not_pay_count,

      -- ✅ bank readiness
      cr.candidate_has_bank_details,
      cr.candidate_bank_hash,
      cr.umbrella_has_bank_details,
      cr.umbrella_bank_hash,

      -- ✅ loan catch-up
      coalesce(ld.loan_due_total,0) as loan_due_total,
      coalesce(ld.loan_due_entries,'[]'::jsonb) as loan_due_entries
    from candidate_rollup cr
    left join blocked_counts bc on bc.candidate_id = cr.candidate_id
    left join do_not_pay_counts dpc on dpc.candidate_id = cr.candidate_id
    left join loan_due ld on ld.candidate_id = cr.candidate_id
  )
  select
    -- paye_candidates
    coalesce(
      (
        select jsonb_agg(
          jsonb_build_object(
            'candidate_id', ce.candidate_id::text,
            'tms_ref', ce.cand_tms_ref,
            'display_name', ce.cand_display_name,
            'current_pay_method', ce.cand_pay_method,
            'umbrella_id', case when ce.cand_umbrella_id is null then null else ce.cand_umbrella_id::text end,
            'umbrella_enabled', ce.umb_enabled,
            'umbrella_vat_chargeable', ce.umb_vat_chargeable,

            -- ✅ bank readiness summary
            'candidate_has_bank_details', ce.candidate_has_bank_details,
            'candidate_bank_hash', ce.candidate_bank_hash,
            'umbrella_has_bank_details', null,
            'umbrella_bank_hash', null,

            'blocked_count', ce.blocked_count,
            'do_not_pay_count', ce.do_not_pay_count,
            'has_any_delta',
              (coalesce(ce.non_mismatch_total_ex,0) <> 0
               or coalesce(ce.mismatch_source_paye_ex,0) <> 0
               or coalesce(ce.mismatch_source_umbrella_ex,0) <> 0),
            'gross_preview_ex_vat_non_mismatch', ce.non_mismatch_total_ex,
            'mismatch', jsonb_build_object(
              'has_mismatch', ce.has_mismatch,
              'source_paye_ex_vat', ce.mismatch_source_paye_ex,
              'source_umbrella_ex_vat', ce.mismatch_source_umbrella_ex,
              'if_settle_via_paye_ex_vat',
                round(
                  ce.mismatch_source_paye_ex
                  + public._pay_convert_umbrella_to_paye_ex(ce.mismatch_source_umbrella_ex, v_erni_pct),
                  2
                ),
              'if_settle_via_umbrella',
                public._pay_convert_paye_to_umbrella(ce.mismatch_source_paye_ex, v_erni_pct, v_vat_rate_pct, ce.umb_vat_chargeable)::jsonb
                ||
                public._pay_umbrella_vat_calc(ce.mismatch_source_umbrella_ex, v_vat_rate_pct, ce.umb_vat_chargeable)::jsonb
            ),
            'loan', jsonb_build_object(
              'pay_week_start', v_week_start::text,
              'loan_due_total', ce.loan_due_total,
              'loan_due_entries', ce.loan_due_entries,
              'cap_fields', jsonb_build_object('min_take_home', 0, 'max_deduction', null)
            ),
            'computed_net_bank_amount_non_mismatch', null,
            'itemisation', ce.timesheets_itemisation
          )
          order by ce.cand_display_name nulls last, ce.cand_tms_ref nulls last, ce.candidate_id
        )
        from cand_enriched ce
        where ce.cand_pay_method = 'PAYE'
      ),
      '[]'::jsonb
    ),
    -- non_paye_payees
    coalesce(
      (
        select jsonb_agg(
          jsonb_build_object(
            'candidate_id', ce.candidate_id::text,
            'tms_ref', ce.cand_tms_ref,
            'display_name', ce.cand_display_name,
            'current_pay_method', ce.cand_pay_method,
            'umbrella_id', case when ce.cand_umbrella_id is null then null else ce.cand_umbrella_id::text end,
            'umbrella_enabled', ce.umb_enabled,
            'umbrella_vat_chargeable', ce.umb_vat_chargeable,

            -- ✅ bank readiness summary
            'candidate_has_bank_details', ce.candidate_has_bank_details,
            'candidate_bank_hash', ce.candidate_bank_hash,
            'umbrella_has_bank_details', case when ce.cand_pay_method <> 'PAYE' then ce.umbrella_has_bank_details else null end,
            'umbrella_bank_hash', case when ce.cand_pay_method <> 'PAYE' then ce.umbrella_bank_hash else null end,

            'blocked_count', ce.blocked_count,
            'do_not_pay_count', ce.do_not_pay_count,
            'has_any_delta',
              (coalesce(ce.non_mismatch_total_ex,0) <> 0
               or coalesce(ce.mismatch_source_paye_ex,0) <> 0
               or coalesce(ce.mismatch_source_umbrella_ex,0) <> 0),
            'gross_preview_ex_vat_non_mismatch', ce.non_mismatch_total_ex,
            'mismatch', jsonb_build_object(
              'has_mismatch', ce.has_mismatch,
              'source_paye_ex_vat', ce.mismatch_source_paye_ex,
              'source_umbrella_ex_vat', ce.mismatch_source_umbrella_ex,
              'if_settle_via_paye_ex_vat',
                round(
                  ce.mismatch_source_paye_ex
                  + public._pay_convert_umbrella_to_paye_ex(ce.mismatch_source_umbrella_ex, v_erni_pct),
                  2
                ),
              'if_settle_via_umbrella',
                public._pay_convert_paye_to_umbrella(ce.mismatch_source_paye_ex, v_erni_pct, v_vat_rate_pct, ce.umb_vat_chargeable)::jsonb
                ||
                public._pay_umbrella_vat_calc(ce.mismatch_source_umbrella_ex, v_vat_rate_pct, ce.umb_vat_chargeable)::jsonb
            ),
            'loan', jsonb_build_object(
              'pay_week_start', v_week_start::text,
              'loan_due_total', ce.loan_due_total,
              'loan_due_entries', ce.loan_due_entries,
              'cap_fields', jsonb_build_object('min_take_home', 0, 'max_deduction', null)
            ),
            'computed_net_bank_amount_non_mismatch',
              (public._pay_umbrella_vat_calc(ce.non_mismatch_total_ex, v_vat_rate_pct, ce.umb_vat_chargeable)->>'inc')::numeric,
            'itemisation', ce.timesheets_itemisation
          )
          order by ce.cand_display_name nulls last, ce.cand_tms_ref nulls last, ce.candidate_id
        )
        from cand_enriched ce
        where ce.cand_pay_method <> 'PAYE'
      ),
      '[]'::jsonb
    ),
    -- blocked_items (unsnoozed)
    coalesce(
      (
        select jsonb_agg(
          jsonb_build_object(
            'candidate_id', bi.candidate_id::text,
            'timesheet_id', bi.timesheet_id::text,
            'segment_id', bi.segment_id,
            'ref_num', bi.ref_num,
            'reason', 'MISSING_REF_NUM',
            'blocked_delta_ex_vat', bi.blocked_delta_ex
          )
          order by bi.candidate_id, bi.timesheet_id, bi.segment_id
        )
        from blocked_items bi
      ),
      '[]'::jsonb
    ),
    -- do_not_pay_items (unsnoozed or impactful)
    coalesce(
      (
        select jsonb_agg(
          jsonb_build_object(
            'candidate_id', di.candidate_id::text,
            'timesheet_id', di.timesheet_id::text,
            'segment_id', di.segment_id,
            'ref_num', di.ref_num,
            'raw_delta_ex_vat', di.raw_delta_ex
          )
          order by di.candidate_id, di.timesheet_id, di.segment_id
        )
        from do_not_pay_items di
      ),
      '[]'::jsonb
    ),
    -- snoozed_items (both kinds)
    coalesce(
      (
        select jsonb_agg(x)
        from (
          select jsonb_build_object(
            'kind', 'BLOCKED',
            'candidate_id', bs.candidate_id::text,
            'timesheet_id', bs.timesheet_id::text,
            'segment_id', bs.segment_id,
            'ref_num', bs.ref_num,
            'blocked_delta_ex_vat', bs.blocked_delta_ex,
            'snooze_id', bs.snooze_id::text,
            'snooze_until_date', case when bs.snooze_until_date is null then null else bs.snooze_until_date::text end,
            'note', bs.note
          ) as x
          from blocked_items_snoozed bs
          union all
          select jsonb_build_object(
            'kind', 'DO_NOT_PAY',
            'candidate_id', ds.candidate_id::text,
            'timesheet_id', ds.timesheet_id::text,
            'segment_id', ds.segment_id,
            'ref_num', ds.ref_num,
            'raw_delta_ex_vat', ds.raw_delta_ex,
            'snooze_id', ds.snooze_id::text,
            'snooze_until_date', case when ds.snooze_until_date is null then null else ds.snooze_until_date::text end,
            'note', ds.note
          ) as x
          from do_not_pay_items_snoozed ds
        ) u
      ),
      '[]'::jsonb
    )
  into v_paye, v_nonpaye, v_blocked, v_do_not_pay, v_snoozed;

  return jsonb_build_object(
    'pay_date', p_pay_date::text,
    'pay_week_start', v_week_start::text,

    -- ✅ Option A: expose eligibility period for UI (“Eligible Timesheet period …”)
    'eligibility', jsonb_build_object(
      'today_uk', v_today_uk::text,
      'from_date', v_eligibility_from_date::text,
      'to_date', v_eligibility_to_date::text,
      'months_back', v_pay_eligibility_months_back,
      'weeks_ahead', v_pay_eligibility_weeks_ahead
    ),

    'finance', jsonb_build_object(
      'vat_rate_pct', v_vat_rate_pct,
      'erni_pct', v_erni_pct
    ),
    'settings', jsonb_build_object(
      'rail', jsonb_build_object(
        'provider_default', v_rail_provider_default,
        'env_default', v_rail_env_default,
        'supports_scheduling', v_rail_supports_scheduling,
        'supports_name_check', v_rail_supports_name_check,
        'supports_auto_execute', v_rail_supports_auto_execute
      ),
      'schedule_defaults', jsonb_build_object(
        'umbrella_local', v_default_schedule_umbrella_local,
        'paye_local', v_default_schedule_paye_local
      ),
      'funds_warning_hours_json', v_funds_warning_hours_json
    ),
    'paye_candidates', v_paye,
    'non_paye_payees', v_nonpaye,
    'blocked_items', v_blocked,
    'do_not_pay_items', v_do_not_pay,
    'snoozed_items', v_snoozed
  );
end;
$$;




-- =========================================================
-- A4.2 pay_create_draft_batch(p_pay_date, p_actor_user_id, p_preview_decisions_json)
-- UPDATED LOGIC FULLY APPLIED:
--  - Candidate selection uses pay_preview but only includes candidates with has_any_delta=true
--  - Segment-level ref gating identical to pay_preview:
--      * Missing ref_num where required blocks ONLY positive delta for that segment (excluded from payable items)
--  - Uses effective require_reference_to_pay (overrideclientsettings-aware, client_settings fallback)
--  - Does not filter out missing refs at timesheet level
-- =========================================================

begin;

create or replace function public.pay_create_draft_batch(
  p_pay_date date,
  p_actor_user_id uuid,
  p_preview_decisions_json jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_week_start date := public._pay_week_start_monday(p_pay_date);

  -- ✅ UK “today” anchor for eligibility window (Option A)
  v_today_uk date := (now() at time zone 'Europe/London')::date;

  -- ✅ Eligibility window knobs (from settings_defaults; fallback to defaults if column absent)
  v_pay_eligibility_months_back int := 6;
  v_pay_eligibility_weeks_ahead int := 2;

  -- ✅ Computed eligibility window
  v_eligibility_from_date date;
  v_eligibility_to_date date;

  v_vat_rate_pct numeric;
  v_erni_pct numeric;

  v_settings record;

  v_batch_id uuid;

  v_candidate_ids uuid[] := array[]::uuid[];
  v_candidate_filter uuid[] := null;

  v_mismatch_choices jsonb := coalesce(p_preview_decisions_json->'mismatch_choices','{}'::jsonb);
  v_loan_caps jsonb := coalesce(p_preview_decisions_json->'loan_caps','{}'::jsonb);

  v_reserved jsonb := '[]'::jsonb;

  -- loan/debt loop vars (kept at function scope; NO nested DO blocks)
  v_rec record;
  v_cap jsonb;
  v_min_take numeric;
  v_max_ded numeric;
  v_gross_main numeric;
  v_remaining numeric;
  v_adv record;
  v_sched_amt numeric;
  v_take_amt numeric;
  v_pbci uuid;
  v_cand_pm text;
  v_cand_umb uuid;

  v_sum_paye numeric;
  v_sum_umb numeric;
  v_debt_paye numeric;
  v_debt_umb numeric;
begin
  if p_pay_date is null then
    raise exception 'pay_date is required';
  end if;

  if to_regclass('public.settings_defaults') is null then
    raise exception 'settings_defaults missing';
  end if;
  if to_regclass('public.settings_finance_windows') is null then
    raise exception 'settings_finance_windows missing';
  end if;

  select
    sfw.vat_rate_pct,
    sfw.erni_pct
  into
    v_vat_rate_pct,
    v_erni_pct
  from public.settings_finance_windows sfw
  where p_pay_date >= sfw.date_from
    and p_pay_date <= coalesce(sfw.date_to, 'infinity'::date)
  order by sfw.date_from desc
  limit 1;

  if v_vat_rate_pct is null or v_erni_pct is null then
    raise exception 'No finance window found for pay_date %', p_pay_date;
  end if;

  select
    sd.banking_system,
    sd.external_paye_system,
    sd.rail_provider_default,
    sd.rail_env_default
  into v_settings
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;

  if v_settings.banking_system is null or v_settings.external_paye_system is null then
    raise exception 'settings_defaults missing banking_system/external_paye_system';
  end if;

  if v_settings.rail_provider_default is null or v_settings.rail_env_default is null then
    raise exception 'settings_defaults missing rail_provider_default/rail_env_default';
  end if;

  -- ✅ Load eligibility window knobs from settings_defaults (Option A)
  -- NOTE: use exception guard so function is robust even if column is absent at runtime.
  begin
    select
      sd.pay_eligibility_months_back,
      sd.pay_eligibility_weeks_ahead
    into
      v_pay_eligibility_months_back,
      v_pay_eligibility_weeks_ahead
    from public.settings_defaults sd
    where sd.id = 1
    limit 1;
  exception when undefined_column then
    v_pay_eligibility_months_back := 6;
    v_pay_eligibility_weeks_ahead := 2;
  end;

  v_pay_eligibility_months_back := greatest(0, least(120, coalesce(v_pay_eligibility_months_back, 6)));
  v_pay_eligibility_weeks_ahead := greatest(0, least(52,  coalesce(v_pay_eligibility_weeks_ahead, 2)));

  v_eligibility_from_date := (v_today_uk - (v_pay_eligibility_months_back::text || ' months')::interval)::date;
  v_eligibility_to_date   := (v_today_uk + (v_pay_eligibility_weeks_ahead::text || ' weeks')::interval)::date;

  if jsonb_typeof(p_preview_decisions_json->'candidate_ids') = 'array' then
    select coalesce(array_agg((x::text)::uuid), array[]::uuid[])
    into v_candidate_filter
    from jsonb_array_elements_text(p_preview_decisions_json->'candidate_ids') x;
  end if;

  -- Candidate set from pay_preview.
  -- Compatible with both preview shapes:
  --  - if pay_preview returns has_any_delta => use it
  --  - otherwise derive "has_any_delta" from totals
  with preview as (
    select public.pay_preview(p_pay_date, p_actor_user_id) as j
  ),
  all_cands as (
    select c as cand
    from preview, lateral jsonb_array_elements(preview.j->'paye_candidates') c
    union all
    select c as cand
    from preview, lateral jsonb_array_elements(preview.j->'non_paye_payees') c
  ),
  selected as (
    select
      (cand->>'candidate_id')::uuid as candidate_id
    from all_cands
    where (
      case
        when (cand ? 'has_any_delta') then coalesce(nullif(cand->>'has_any_delta','')::boolean,false)
        else (
          coalesce(nullif(cand->>'gross_preview_ex_vat_non_mismatch','')::numeric,0) <> 0
          or coalesce(nullif(cand#>>'{mismatch,source_paye_ex_vat}','')::numeric,0) <> 0
          or coalesce(nullif(cand#>>'{mismatch,source_umbrella_ex_vat}','')::numeric,0) <> 0
        )
      end
    )
    and (v_candidate_filter is null or (cand->>'candidate_id')::uuid = any(v_candidate_filter))
  )
  select coalesce(array_agg(s.candidate_id), array[]::uuid[])
  into v_candidate_ids
  from selected s;

  if array_length(v_candidate_ids,1) is null or array_length(v_candidate_ids,1) = 0 then
    raise exception 'Nothing to pay (no payable deltas after blockers)';
  end if;

  -- Validate mismatch decisions completeness for included candidates
  with preview as (
    select public.pay_preview(p_pay_date, p_actor_user_id) as j
  ),
  all_cands as (
    select c as cand
    from preview, lateral jsonb_array_elements(preview.j->'paye_candidates') c
    union all
    select c as cand
    from preview, lateral jsonb_array_elements(preview.j->'non_paye_payees') c
  ),
  need as (
    select (cand->>'candidate_id')::uuid as candidate_id
    from all_cands
    where (cand->>'candidate_id')::uuid = any(v_candidate_ids)
      and coalesce((cand#>>'{mismatch,has_mismatch}')::boolean,false) = true
  ),
  missing as (
    select n.candidate_id
    from need n
    where coalesce(nullif(v_mismatch_choices->>n.candidate_id::text,''), '') = ''
  )
  select coalesce(jsonb_agg(m.candidate_id::text), '[]'::jsonb)
  into v_reserved
  from missing m;

  if jsonb_array_length(v_reserved) > 0 then
    raise exception 'MISMATCH_DECISIONS_REQUIRED for candidates %', v_reserved::text;
  end if;

  insert into public.pay_batches(
    pay_date,
    created_at_utc,
    created_by_user_id,
    status,
    banking_system_snapshot,
    external_paye_system_snapshot,
    rail_provider_snapshot,
    rail_env_snapshot
  )
  values (
    p_pay_date,
    now(),
    p_actor_user_id,
    'DRAFT',
    v_settings.banking_system,
    v_settings.external_paye_system,
    v_settings.rail_provider_default,
    v_settings.rail_env_default
  )
  returning id into v_batch_id;

  insert into public.pay_batch_candidates(
    pay_batch_id,
    candidate_id,
    candidate_tms_ref,
    candidate_display_name,
    paye_state,
    mismatch_settlement_choice,
    gross_preview,
    net_bank_amount,
    debt_created,
    loan_repayment_taken
  )
  select
    v_batch_id,
    c.id,
    c.tms_ref,
    c.display_name,
    case when upper(coalesce(c.pay_method,'')) = 'PAYE' then 'PENDING_NET' else null end,
    nullif(v_mismatch_choices->>c.id::text,''),
    null, null, 0, 0
  from public.candidates c
  where c.id = any(v_candidate_ids);

  -- Build pay_batch_items with segment-level ref gating:
  -- blocked-positive segments (missing ref_num where required) are excluded (delta forced to 0).
  with finance as (
    select v_vat_rate_pct as vat_rate_pct, v_erni_pct as erni_pct
  ),
  eligible_tf as (
    select
      tf.timesheet_id,
      tf.candidate_id,
      tf.client_id,
      upper(coalesce(tf.pay_method,'')) as ts_pay_method,
      upper(coalesce(c.pay_method,'')) as cand_pay_method,
      c.umbrella_id as umbrella_id,

      ts.contract_id,
      ts.reference_number,

      ct.overrideclientsettings,

      coalesce(
        case when ct.overrideclientsettings then ct.require_reference_to_pay end,
        cs.pay_reference_required,
        false
      ) as require_reference_to_pay,

      tf.invoice_breakdown_json,
      round(coalesce(tf.total_pay_ex_vat,0),2) as total_pay_ex_vat,
      round(coalesce(tf.expenses_pay_ex_vat,0),2) as expenses_pay_ex_vat,
      round(coalesce(tf.travel_pay_ex_vat,0),2) as travel_pay_ex_vat,
      round(coalesce(tf.accommodation_pay_ex_vat,0),2) as accommodation_pay_ex_vat,
      round(coalesce(tf.other_pay_ex_vat,0),2) as other_pay_ex_vat,
      round(coalesce(tf.mileage_pay_ex_vat,0),2) as mileage_pay_ex_vat,

      tps.last_settled_snapshot_json as base_json
    from public.timesheets_financials tf
    join public.timesheets ts
      on ts.timesheet_id = tf.timesheet_id
     and ts.is_current = true
    join public.candidates c
      on c.id = tf.candidate_id
    left join public.contracts ct
      on ct.id = ts.contract_id
    left join public.client_settings cs
      on cs.client_id = tf.client_id
    left join public.timesheet_pay_state tps
      on tps.timesheet_id = tf.timesheet_id
    where tf.is_current = true
      and coalesce(tf.pay_on_hold,false) = false
      and ts.authorised_at_server is not null
      and coalesce(tf.has_rate_issue,false) = false
      and coalesce(tf.has_pay_channel_issue,false) = false
      and upper(coalesce(tf.processing_status,'')) not in ('UNASSIGNED','CLIENT_UNRESOLVED','RATE_MISSING','PAY_CHANNEL_MISSING')
      and upper(coalesce(c.pay_method,'')) in ('PAYE','UMBRELLA')
      and tf.candidate_id = any(v_candidate_ids)

      -- ✅ Option A: match pay_preview eligibility window (relative to UK “today”).
      and ts.week_ending_date::date >= v_eligibility_from_date
      and ts.week_ending_date::date <= v_eligibility_to_date
  ),
  umb as (
    select
      u.id as umbrella_id,
      coalesce(u.vat_chargeable,false) as vat_chargeable
    from public.umbrellas u
  ),
  adj as (
    select
      a.id as adj_id,
      a.timesheet_id,
      a.candidate_id,
      round(coalesce(a.delta_pay_ex_vat,0),2) as delta_pay_ex_vat
    from public.ts_pay_adjustments a
    where a.as_advance = false
      and a.timesheet_id is not null
      and a.candidate_id = any(v_candidate_ids)
  ),
  cur as (
    select
      e.*,
      coalesce(u.vat_chargeable,false) as umb_vat_chargeable,

      case
        when e.invoice_breakdown_json is not null
         and jsonb_typeof(e.invoice_breakdown_json)='object'
         and upper(coalesce(e.invoice_breakdown_json->>'mode',''))='SEGMENTS'
         and jsonb_typeof(e.invoice_breakdown_json->'segments')='array'
        then (
          select coalesce(jsonb_agg(
            jsonb_build_object(
              'segment_id', nullif(btrim(coalesce(seg->>'segment_id','')),''),
              'pay_amount', round(coalesce(nullif(seg->>'pay_amount','')::numeric,0),2),
              'exclude_from_pay', coalesce(nullif(seg->>'exclude_from_pay','')::boolean,false),
              'ref_num', nullif(btrim(coalesce(seg->>'ref_num','')),'')
            )
          ), '[]'::jsonb)
          from jsonb_array_elements(e.invoice_breakdown_json->'segments') seg
          where seg is not null and jsonb_typeof(seg)='object'
        )
        else jsonb_build_array(
          jsonb_build_object(
            'segment_id', ('ts:' || e.timesheet_id::text),
            'pay_amount', round(coalesce(e.total_pay_ex_vat,0),2),
            'exclude_from_pay', false,
            'ref_num', nullif(btrim(coalesce(e.reference_number,'')), '')
          )
        )
      end as cur_segments,

      case
        when e.invoice_breakdown_json is not null
         and jsonb_typeof(e.invoice_breakdown_json)='object'
         and upper(coalesce(e.invoice_breakdown_json->>'mode',''))='SEGMENTS'
        then round(coalesce(nullif(e.invoice_breakdown_json #>> '{additional,pay_ex_vat}','')::numeric,0),2)
        else 0::numeric
      end as cur_additional,

      coalesce(
        (
          select jsonb_agg(jsonb_build_object('id', a.adj_id::text, 'delta_pay_ex_vat', a.delta_pay_ex_vat))
          from adj a
          where a.timesheet_id = e.timesheet_id
        ),
        '[]'::jsonb
      ) as cur_adjs
    from eligible_tf e
    left join umb u on u.umbrella_id = e.umbrella_id
  ),
  deltas as (
    select
      c.candidate_id,
      c.timesheet_id,
      c.ts_pay_method,
      c.cand_pay_method,
      c.umbrella_id,
      c.umb_vat_chargeable,
      c.require_reference_to_pay,

      coalesce(c.base_json,'{}'::jsonb) as base_json,

      c.cur_segments,
      c.cur_adjs,

      c.cur_additional,
      c.expenses_pay_ex_vat,
      c.travel_pay_ex_vat,
      c.accommodation_pay_ex_vat,
      c.other_pay_ex_vat,
      c.mileage_pay_ex_vat
    from cur c
  ),
  segment_delta_rows as (
    select
      d.candidate_id,
      d.timesheet_id,
      d.ts_pay_method,
      d.cand_pay_method,
      d.umbrella_id,
      d.umb_vat_chargeable,
      ids.segment_id as segment_key,

      round(
        (case when coalesce(cur.exclude_from_pay,false) then 0 else coalesce(cur.pay_amount,0) end)
        -
        (case when coalesce(bas.exclude_from_pay,false) then 0 else coalesce(bas.pay_amount,0) end),
        2
      ) as raw_delta_ex,

      case
        when (
          d.require_reference_to_pay = true
          and coalesce(cur.exclude_from_pay,false) = false
          and nullif(btrim(coalesce(cur.ref_num,'')),'') is null
          and round(
                (case when coalesce(cur.exclude_from_pay,false) then 0 else coalesce(cur.pay_amount,0) end)
                -
                (case when coalesce(bas.exclude_from_pay,false) then 0 else coalesce(bas.pay_amount,0) end),
                2
              ) > 0
        ) then 0::numeric
        else round(
          (case when coalesce(cur.exclude_from_pay,false) then 0 else coalesce(cur.pay_amount,0) end)
          -
          (case when coalesce(bas.exclude_from_pay,false) then 0 else coalesce(bas.pay_amount,0) end),
          2
        )
      end as delta_ex
    from deltas d
    join lateral (
      select distinct segment_id
      from (
        select nullif(btrim(coalesce(s->>'segment_id','')),'') as segment_id
        from jsonb_array_elements(d.cur_segments) s
        where s is not null and jsonb_typeof(s)='object'
        union
        select nullif(btrim(coalesce(s->>'segment_id','')),'') as segment_id
        from jsonb_array_elements(coalesce(d.base_json->'segments','[]'::jsonb)) s
        where s is not null and jsonb_typeof(s)='object'
      ) u
      where segment_id is not null
    ) ids on true
    left join lateral (
      select
        round(coalesce(nullif(s->>'pay_amount','')::numeric,0),2) as pay_amount,
        coalesce(nullif(s->>'exclude_from_pay','')::boolean,false) as exclude_from_pay,
        nullif(btrim(coalesce(s->>'ref_num','')),'') as ref_num
      from jsonb_array_elements(d.cur_segments) s
      where s is not null and jsonb_typeof(s)='object'
        and nullif(btrim(coalesce(s->>'segment_id','')),'') = ids.segment_id
      limit 1
    ) cur on true
    left join lateral (
      select
        round(coalesce(nullif(s->>'pay_amount','')::numeric,0),2) as pay_amount,
        coalesce(nullif(s->>'exclude_from_pay','')::boolean,false) as exclude_from_pay
      from jsonb_array_elements(coalesce(d.base_json->'segments','[]'::jsonb)) s
      where s is not null and jsonb_typeof(s)='object'
        and nullif(btrim(coalesce(s->>'segment_id','')),'') = ids.segment_id
      limit 1
    ) bas on true
    where round(
      case
        when (
          d.require_reference_to_pay = true
          and coalesce(cur.exclude_from_pay,false) = false
          and nullif(btrim(coalesce(cur.ref_num,'')),'') is null
          and round(
                (case when coalesce(cur.exclude_from_pay,false) then 0 else coalesce(cur.pay_amount,0) end)
                -
                (case when coalesce(bas.exclude_from_pay,false) then 0 else coalesce(bas.pay_amount,0) end),
                2
              ) > 0
        ) then 0::numeric
        else round(
          (case when coalesce(cur.exclude_from_pay,false) then 0 else coalesce(cur.pay_amount,0) end)
          -
          (case when coalesce(bas.exclude_from_pay,false) then 0 else coalesce(bas.pay_amount,0) end),
          2
        )
      end,
      2
    ) <> 0
  ),
  adj_delta_rows as (
    select
      d.candidate_id,
      d.timesheet_id,
      d.ts_pay_method,
      d.cand_pay_method,
      d.umbrella_id,
      d.umb_vat_chargeable,
      ('adj:' || ids.adj_id) as source_ref,
      round(
        coalesce((select coalesce(nullif(a->>'delta_pay_ex_vat','')::numeric,0)
                  from jsonb_array_elements(d.cur_adjs) a
                  where nullif(btrim(coalesce(a->>'id','')),'') = ids.adj_id
                  limit 1), 0)
        -
        coalesce((select coalesce(nullif(a->>'delta_pay_ex_vat','')::numeric,0)
                  from jsonb_array_elements(coalesce(d.base_json->'adjustments','[]'::jsonb)) a
                  where nullif(btrim(coalesce(a->>'id','')),'') = ids.adj_id
                  limit 1), 0),
        2
      ) as delta_ex
    from deltas d
    join lateral (
      select adj_id
      from (
        select nullif(btrim(coalesce(a->>'id','')),'') as adj_id
        from jsonb_array_elements(d.cur_adjs) a
        where a is not null and jsonb_typeof(a)='object'
        union
        select nullif(btrim(coalesce(a->>'id','')),'') as adj_id
        from jsonb_array_elements(coalesce(d.base_json->'adjustments','[]'::jsonb)) a
        where a is not null and jsonb_typeof(a)='object'
      ) u
      where adj_id is not null
    ) ids on true
    where round(
      round(
        coalesce((select coalesce(nullif(a->>'delta_pay_ex_vat','')::numeric,0)
                  from jsonb_array_elements(d.cur_adjs) a
                  where nullif(btrim(coalesce(a->>'id','')),'') = ids.adj_id
                  limit 1), 0)
        -
        coalesce((select coalesce(nullif(a->>'delta_pay_ex_vat','')::numeric,0)
                  from jsonb_array_elements(coalesce(d.base_json->'adjustments','[]'::jsonb)) a
                  where nullif(btrim(coalesce(a->>'id','')),'') = ids.adj_id
                  limit 1), 0),
        2
      ),
      2
    ) <> 0
  ),
  other_delta_rows as (
    select
      d.candidate_id,
      d.timesheet_id,
      d.ts_pay_method,
      d.cand_pay_method,
      d.umbrella_id,
      d.umb_vat_chargeable,
      'ADDITIONAL'::text as kind,
      null::text as segment_key,
      'additional'::text as source_ref,
      round(d.cur_additional - coalesce(nullif(d.base_json->>'additional_pay_ex_vat','')::numeric,0), 2) as delta_ex
    from deltas d
    union all
    select d.candidate_id, d.timesheet_id, d.ts_pay_method, d.cand_pay_method, d.umbrella_id, d.umb_vat_chargeable,
      'EXPENSES', null, 'expenses', round(d.expenses_pay_ex_vat - coalesce(nullif(d.base_json #>> '{expenses,expenses_pay_ex_vat}','')::numeric,0),2)
    from deltas d
    union all
    select d.candidate_id, d.timesheet_id, d.ts_pay_method, d.cand_pay_method, d.umbrella_id, d.umb_vat_chargeable,
      'TRAVEL', null, 'travel', round(d.travel_pay_ex_vat - coalesce(nullif(d.base_json #>> '{expenses,travel_pay_ex_vat}','')::numeric,0),2)
    from deltas d
    union all
    select d.candidate_id, d.timesheet_id, d.ts_pay_method, d.cand_pay_method, d.umbrella_id, d.umb_vat_chargeable,
      'ACCOMMODATION', null, 'accommodation', round(d.accommodation_pay_ex_vat - coalesce(nullif(d.base_json #>> '{expenses,accommodation_pay_ex_vat}','')::numeric,0),2)
    from deltas d
    union all
    select d.candidate_id, d.timesheet_id, d.ts_pay_method, d.cand_pay_method, d.umbrella_id, d.umb_vat_chargeable,
      'OTHER', null, 'other', round(d.other_pay_ex_vat - coalesce(nullif(d.base_json #>> '{expenses,other_pay_ex_vat}','')::numeric,0),2)
    from deltas d
    union all
    select d.candidate_id, d.timesheet_id, d.ts_pay_method, d.cand_pay_method, d.umbrella_id, d.umb_vat_chargeable,
      'MILEAGE', null, 'mileage', round(d.mileage_pay_ex_vat - coalesce(nullif(d.base_json #>> '{expenses,mileage_pay_ex_vat}','')::numeric,0),2)
    from deltas d
  ),
  all_delta_items as (
    select
      s.candidate_id,
      s.timesheet_id,
      s.ts_pay_method,
      s.cand_pay_method,
      s.umbrella_id,
      s.umb_vat_chargeable,
      'SEGMENT'::text as kind,
      s.segment_key,
      ('seg:' || s.segment_key)::text as source_ref,
      s.delta_ex
    from segment_delta_rows s
    union all
    select
      a.candidate_id, a.timesheet_id, a.ts_pay_method, a.cand_pay_method, a.umbrella_id, a.umb_vat_chargeable,
      'ADJUSTMENT'::text, null::text, a.source_ref, a.delta_ex
    from adj_delta_rows a
    union all
    select
      o.candidate_id, o.timesheet_id, o.ts_pay_method, o.cand_pay_method, o.umbrella_id, o.umb_vat_chargeable,
      o.kind, o.segment_key, o.source_ref, o.delta_ex
    from other_delta_rows o
    where round(coalesce(o.delta_ex,0),2) <> 0
  ),
  routed as (
    select
      i.*,
      case
        when i.ts_pay_method = i.cand_pay_method then i.cand_pay_method
        else upper(coalesce(nullif(v_mismatch_choices->>i.candidate_id::text,''), ''))
      end as pay_channel,
      (i.ts_pay_method <> i.cand_pay_method) as is_mismatch
    from all_delta_items i
    where round(coalesce(i.delta_ex,0),2) <> 0
      and (
        i.ts_pay_method = i.cand_pay_method
        or upper(coalesce(nullif(v_mismatch_choices->>i.candidate_id::text,''), '')) in ('PAYE','UMBRELLA')
      )
  ),
  amounts as (
    select
      r.candidate_id,
      r.timesheet_id,
      r.segment_key,
      r.source_ref,
      r.kind,
      r.ts_pay_method,
      r.cand_pay_method,
      r.pay_channel,
      r.is_mismatch,
      r.umb_vat_chargeable,

      case
        when r.is_mismatch and r.ts_pay_method = 'PAYE' and r.pay_channel = 'UMBRELLA'
          then (public._pay_convert_paye_to_umbrella(r.delta_ex, (select erni_pct from finance), (select vat_rate_pct from finance), r.umb_vat_chargeable)->>'ex')::numeric
        when r.is_mismatch and r.ts_pay_method = 'UMBRELLA' and r.pay_channel = 'PAYE'
          then public._pay_convert_umbrella_to_paye_ex(r.delta_ex, (select erni_pct from finance))
        else r.delta_ex
      end as ex_amt_for_channel
    from routed r
    where r.pay_channel in ('PAYE','UMBRELLA')
  ),
  final_items as (
    select
      a.candidate_id,
      a.timesheet_id,
      a.segment_key,
      a.source_ref,
      a.pay_channel,

      case
        when a.is_mismatch and a.ts_pay_method <> a.pay_channel then 'CONVERSION_ADJ'
        when a.kind = 'SEGMENT' then 'SEGMENT_DELTA'
        when a.kind = 'MILEAGE' then 'MILEAGE_DELTA'
        when a.kind = 'ADJUSTMENT' then 'ADJUSTMENT_DELTA'
        else 'EXPENSE_DELTA'
      end as item_type,

      case
        when a.pay_channel = 'UMBRELLA'
          then (public._pay_umbrella_vat_calc(a.ex_amt_for_channel, (select vat_rate_pct from finance), a.umb_vat_chargeable)->>'ex')::numeric
        else round(a.ex_amt_for_channel,2)
      end as amount_ex_vat,

      case
        when a.pay_channel = 'UMBRELLA'
          then (public._pay_umbrella_vat_calc(a.ex_amt_for_channel, (select vat_rate_pct from finance), a.umb_vat_chargeable)->>'vat')::numeric
        else 0::numeric
      end as amount_vat,

      case
        when a.pay_channel = 'UMBRELLA'
          then (public._pay_umbrella_vat_calc(a.ex_amt_for_channel, (select vat_rate_pct from finance), a.umb_vat_chargeable)->>'inc')::numeric
        else round(a.ex_amt_for_channel,2)
      end as amount_inc_vat
    from amounts a
  )
  insert into public.pay_batch_items(
    pay_batch_candidate_id,
    item_type,
    timesheet_id,
    segment_key,
    source_ref,
    description,
    amount_ex_vat,
    amount_vat,
    amount_inc_vat,
    pay_channel,
    umbrella_id
  )
  select
    pbc.id,
    fi.item_type,
    fi.timesheet_id,
    fi.segment_key,
    fi.source_ref,
    case
      when fi.item_type = 'SEGMENT_DELTA' then 'Segment delta'
      when fi.item_type = 'MILEAGE_DELTA' then 'Mileage delta'
      when fi.item_type = 'ADJUSTMENT_DELTA' then 'Pay adjustment delta'
      when fi.item_type = 'CONVERSION_ADJ' then 'Mismatch conversion adjustment'
      else 'Expense delta'
    end,
    round(fi.amount_ex_vat,2),
    round(fi.amount_vat,2),
    round(fi.amount_inc_vat,2),
    fi.pay_channel,
    case when fi.pay_channel = 'UMBRELLA' then c.umbrella_id else null end
  from final_items fi
  join public.pay_batch_candidates pbc
    on pbc.pay_batch_id = v_batch_id
   and pbc.candidate_id = fi.candidate_id
  join public.candidates c
    on c.id = fi.candidate_id;

  -- Double-pay prevention
  with my_items as (
    select
      pbi.timesheet_id,
      pbi.segment_key,
      pbi.source_ref
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = v_batch_id
  ),
  conflicts as (
    select distinct
      mi.timesheet_id,
      mi.segment_key,
      mi.source_ref,
      pb2.id as existing_pay_batch_id
    from my_items mi
    join public.pay_batch_items p2
      on p2.timesheet_id is not distinct from mi.timesheet_id
     and (
       (mi.segment_key is not null and p2.segment_key = mi.segment_key)
       or (mi.source_ref is not null and p2.source_ref = mi.source_ref)
     )
    join public.pay_batch_candidates pbc2 on pbc2.id = p2.pay_batch_candidate_id
    join public.pay_batches pb2 on pb2.id = pbc2.pay_batch_id
    where pb2.status in ('DRAFT','WAITING_BANK_CONFIRM','DRAFT_CREATED','PARTIAL')
      and pb2.id <> v_batch_id
  )
  select
    coalesce(jsonb_agg(
      jsonb_build_object(
        'timesheet_id', coalesce(conflicts.timesheet_id::text, null),
        'segment_key', conflicts.segment_key,
        'source_ref', conflicts.source_ref,
        'existing_pay_batch_id', conflicts.existing_pay_batch_id::text
      )
    ), '[]'::jsonb)
  into v_reserved
  from conflicts;

  if jsonb_array_length(v_reserved) > 0 then
    raise exception 'DOUBLE_PAY_BLOCK: items already reserved in batches %', v_reserved::text;
  end if;

  -- Apply loan repayments (catch-up: include overdue weeks <= v_week_start; oldest-first)
  for v_rec in
    select
      pbc.id as pay_batch_candidate_id,
      pbc.candidate_id,
      upper(coalesce(c.pay_method,'')) as cand_pay_method,
      c.umbrella_id as umbrella_id
    from public.pay_batch_candidates pbc
    join public.candidates c on c.id = pbc.candidate_id
    where pbc.pay_batch_id = v_batch_id
  loop
    v_pbci := v_rec.pay_batch_candidate_id;
    v_cand_pm := v_rec.cand_pay_method;
    v_cand_umb := v_rec.umbrella_id;

    if v_cand_pm = 'UMBRELLA' then
      select round(coalesce(sum(pbi.amount_inc_vat),0),2)
      into v_gross_main
      from public.pay_batch_items pbi
      where pbi.pay_batch_candidate_id = v_pbci
        and pbi.pay_channel = 'UMBRELLA'
        and pbi.item_type <> 'DEBT_CREATED';
    else
      select round(coalesce(sum(pbi.amount_ex_vat),0),2)
      into v_gross_main
      from public.pay_batch_items pbi
      where pbi.pay_batch_candidate_id = v_pbci
        and pbi.pay_channel = 'PAYE'
        and pbi.item_type <> 'DEBT_CREATED';
    end if;

    v_cap := coalesce(v_loan_caps->v_rec.candidate_id::text, '{}'::jsonb);
    v_min_take := nullif(v_cap->>'min_take_home','')::numeric;
    v_max_ded  := nullif(v_cap->>'max_deduction','')::numeric;

    if v_min_take is not null and v_min_take < 0 then
      raise exception 'Invalid min_take_home for candidate %', v_rec.candidate_id::text;
    end if;
    if v_max_ded is not null and v_max_ded < 0 then
      raise exception 'Invalid max_deduction for candidate %', v_rec.candidate_id::text;
    end if;

    v_remaining := v_gross_main;

    if v_min_take is not null then
      v_remaining := greatest(round(v_gross_main - v_min_take,2), 0);
    end if;
    if v_max_ded is not null then
      v_remaining := least(v_remaining, round(v_max_ded,2));
    end if;

    for v_adv in
      select
        pa.id as advance_id,
        nullif(x->>'week_start','')::date as due_week_start,
        abs(coalesce(nullif(x->>'amount','')::numeric,0)) as due_amt
      from public.pay_advances pa
      join lateral jsonb_array_elements(coalesce(pa.schedule_json,'[]'::jsonb)) x on true
      where pa.candidate_id = v_rec.candidate_id
        and pa.status::text = 'ACTIVE'
        and nullif(x->>'week_start','')::date is not null
        and nullif(x->>'week_start','')::date <= v_week_start
        and coalesce(nullif(x->>'amount','')::numeric,0) < 0
      order by nullif(x->>'week_start','')::date asc, pa.created_at asc, pa.id
    loop
      v_sched_amt := round(coalesce(v_adv.due_amt,0),2);
      if v_sched_amt <= 0 then
        continue;
      end if;

      v_take_amt := least(v_sched_amt, v_remaining);
      v_take_amt := round(greatest(v_take_amt,0),2);

      if v_take_amt > 0 then
        insert into public.pay_batch_items(
          pay_batch_candidate_id,
          item_type,
          timesheet_id,
          segment_key,
          source_ref,
          description,
          amount_ex_vat,
          amount_vat,
          amount_inc_vat,
          pay_channel,
          umbrella_id,
          repayment_week_start
        )
        values (
          v_pbci,
          'LOAN_REPAYMENT',
          null,
          null,
          ('advance:' || v_adv.advance_id::text),
          'Loan repayment',
          -v_take_amt,
          0,
          -v_take_amt,
          v_cand_pm,
          case when v_cand_pm = 'UMBRELLA' then v_cand_umb else null end,
          v_adv.due_week_start
        );

        v_remaining := round(v_remaining - v_take_amt,2);
      end if;

      if v_remaining <= 0 then
        exit;
      end if;
    end loop;

    update public.pay_batch_candidates pbc
    set loan_repayment_taken = coalesce((
      select round(sum(abs(coalesce(pbi.amount_inc_vat, pbi.amount_ex_vat, 0))),2)
      from public.pay_batch_items pbi
      where pbi.pay_batch_candidate_id = v_pbci
        and pbi.item_type = 'LOAN_REPAYMENT'
    ),0)
    where pbc.id = v_pbci;
  end loop;

  -- Clip negatives into DEBT_CREATED (in-function loop; no DO)
  for v_rec in
    select
      pbc.id as pay_batch_candidate_id,
      pbc.candidate_id,
      c.umbrella_id as umbrella_id
    from public.pay_batch_candidates pbc
    join public.candidates c on c.id = pbc.candidate_id
    where pbc.pay_batch_id = v_batch_id
  loop
    v_pbci := v_rec.pay_batch_candidate_id;

    select round(coalesce(sum(pbi.amount_ex_vat),0),2)
    into v_sum_paye
    from public.pay_batch_items pbi
    where pbi.pay_batch_candidate_id = v_pbci
      and pbi.pay_channel = 'PAYE'
      and pbi.item_type <> 'DEBT_CREATED';

    select round(coalesce(sum(pbi.amount_inc_vat),0),2)
    into v_sum_umb
    from public.pay_batch_items pbi
    where pbi.pay_batch_candidate_id = v_pbci
      and pbi.pay_channel = 'UMBRELLA'
      and pbi.item_type <> 'DEBT_CREATED';

    v_debt_paye := round(greatest(-coalesce(v_sum_paye,0),0),2);
    v_debt_umb  := round(greatest(-coalesce(v_sum_umb,0),0),2);

    if v_debt_paye > 0 then
      insert into public.pay_batch_items(
        pay_batch_candidate_id,item_type,timesheet_id,segment_key,source_ref,description,
        amount_ex_vat,amount_vat,amount_inc_vat,pay_channel,umbrella_id
      )
      values (
        v_pbci,'DEBT_CREATED',null,null,'debt:paye','Debt created (clipped negative)',
        v_debt_paye,0,v_debt_paye,'PAYE',null
      );
    end if;

    if v_debt_umb > 0 then
      insert into public.pay_batch_items(
        pay_batch_candidate_id,item_type,timesheet_id,segment_key,source_ref,description,
        amount_ex_vat,amount_vat,amount_inc_vat,pay_channel,umbrella_id
      )
      values (
        v_pbci,'DEBT_CREATED',null,null,'debt:umbrella','Debt created (clipped negative)',
        v_debt_umb,0,v_debt_umb,'UMBRELLA',v_rec.umbrella_id
      );
    end if;

    update public.pay_batch_candidates pbc
    set debt_created = round(coalesce(v_debt_paye,0) + coalesce(v_debt_umb,0),2)
    where pbc.id = v_pbci;
  end loop;

  -- Populate gross_preview/net_bank_amount summaries
  update public.pay_batch_candidates pbc
  set
    gross_preview = case
      when upper(coalesce(c.pay_method,'')) = 'PAYE'
        then round((
          select coalesce(sum(pbi.amount_ex_vat),0)
          from public.pay_batch_items pbi
          where pbi.pay_batch_candidate_id = pbc.id
            and pbi.pay_channel = 'PAYE'
            and pbi.item_type <> 'DEBT_CREATED'
        ),2)
      else round((
          select coalesce(sum(pbi.amount_inc_vat),0)
          from public.pay_batch_items pbi
          where pbi.pay_batch_candidate_id = pbc.id
            and pbi.pay_channel = 'UMBRELLA'
            and pbi.item_type <> 'DEBT_CREATED'
        ),2)
    end,
    net_bank_amount = case
      when upper(coalesce(c.pay_method,'')) = 'PAYE' then null
      else round((
        select greatest(coalesce(sum(pbi.amount_inc_vat),0),0)
        from public.pay_batch_items pbi
        where pbi.pay_batch_candidate_id = pbc.id
          and pbi.pay_channel = 'UMBRELLA'
          and pbi.item_type <> 'DEBT_CREATED'
      ),2)
    end,
    mismatch_settlement_choice = nullif(v_mismatch_choices->>pbc.candidate_id::text,'')
  from public.candidates c
  where pbc.pay_batch_id = v_batch_id
    and c.id = pbc.candidate_id;

  -- Create frozen timesheet snapshots for settlement baselines (do NOT advance blocked segments)
  with touched_ts as (
    select distinct pbi.timesheet_id
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = v_batch_id
      and pbi.timesheet_id is not null
  ),
  ts_channel as (
    select
      pbi.timesheet_id,
      max(pbi.pay_channel) as pay_channel_used
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = v_batch_id
      and pbi.timesheet_id is not null
      and pbi.pay_channel in ('PAYE','UMBRELLA')
    group by pbi.timesheet_id
  ),
  tf0 as (
    select
      tf.timesheet_id,
      tf.candidate_id,
      tf.client_id,
      ts.contract_id,
      ts.reference_number,
      tf.invoice_breakdown_json,
      round(coalesce(tf.total_pay_ex_vat,0),2) as total_pay_ex_vat,
      round(coalesce(tf.expenses_pay_ex_vat,0),2) as expenses_pay_ex_vat,
      round(coalesce(tf.travel_pay_ex_vat,0),2) as travel_pay_ex_vat,
      round(coalesce(tf.accommodation_pay_ex_vat,0),2) as accommodation_pay_ex_vat,
      round(coalesce(tf.other_pay_ex_vat,0),2) as other_pay_ex_vat,
      round(coalesce(tf.mileage_pay_ex_vat,0),2) as mileage_pay_ex_vat,
      coalesce(
        case when ct.overrideclientsettings then ct.require_reference_to_pay end,
        cs.pay_reference_required,
        false
      ) as require_reference_to_pay,
      coalesce(tps.last_settled_snapshot_json, '{}'::jsonb) as base_json
    from public.timesheets_financials tf
    join touched_ts t on t.timesheet_id = tf.timesheet_id
    join public.timesheets ts
      on ts.timesheet_id = tf.timesheet_id
     and ts.is_current = true
    left join public.contracts ct
      on ct.id = ts.contract_id
    left join public.client_settings cs
      on cs.client_id = tf.client_id
    left join public.timesheet_pay_state tps
      on tps.timesheet_id = tf.timesheet_id
    where tf.is_current = true
  ),
  cur0 as (
    select
      t.*,
      case
        when t.invoice_breakdown_json is not null
         and jsonb_typeof(t.invoice_breakdown_json)='object'
         and upper(coalesce(t.invoice_breakdown_json->>'mode',''))='SEGMENTS'
         and jsonb_typeof(t.invoice_breakdown_json->'segments')='array'
        then (
          select coalesce(jsonb_agg(
            jsonb_build_object(
              'segment_id', nullif(btrim(coalesce(seg->>'segment_id','')),''),
              'pay_amount', round(coalesce(nullif(seg->>'pay_amount','')::numeric,0),2),
              'exclude_from_pay', coalesce(nullif(seg->>'exclude_from_pay','')::boolean,false),
              'ref_num', nullif(btrim(coalesce(seg->>'ref_num','')),'')
            )
          ), '[]'::jsonb)
          from jsonb_array_elements(t.invoice_breakdown_json->'segments') seg
          where seg is not null and jsonb_typeof(seg)='object'
        )
        else jsonb_build_array(
          jsonb_build_object(
            'segment_id', ('ts:' || t.timesheet_id::text),
            'pay_amount', round(coalesce(t.total_pay_ex_vat,0),2),
            'exclude_from_pay', false,
            'ref_num', nullif(btrim(coalesce(t.reference_number,'')), '')
          )
        )
      end as cur_segments,
      case
        when t.invoice_breakdown_json is not null
         and jsonb_typeof(t.invoice_breakdown_json)='object'
         and upper(coalesce(t.invoice_breakdown_json->>'mode',''))='SEGMENTS'
        then round(coalesce(nullif(t.invoice_breakdown_json #>> '{additional,pay_ex_vat}','')::numeric,0),2)
        else 0::numeric
      end as cur_additional
    from tf0 t
  ),
  seg_ids as (
    select
      c.timesheet_id,
      nullif(btrim(coalesce(s->>'segment_id','')),'') as segment_id
    from cur0 c
    join lateral jsonb_array_elements(coalesce(c.cur_segments,'[]'::jsonb)) s on true
    where s is not null and jsonb_typeof(s)='object'
    union
    select
      c.timesheet_id,
      nullif(btrim(coalesce(s->>'segment_id','')),'') as segment_id
    from cur0 c
    join lateral jsonb_array_elements(coalesce(c.base_json->'segments','[]'::jsonb)) s on true
    where s is not null and jsonb_typeof(s)='object'
  ),
  seg_calc as (
    select
      c.timesheet_id,
      c.candidate_id,
      c.require_reference_to_pay,
      i.segment_id,

      coalesce(cur.pay_amount, 0)::numeric(12,2) as cur_pay_amount,
      coalesce(cur.exclude_from_pay, false) as cur_exclude_from_pay,
      cur.ref_num as cur_ref_num,

      coalesce(bas.pay_amount, 0)::numeric(12,2) as bas_pay_amount,
      coalesce(bas.exclude_from_pay, false) as bas_exclude_from_pay,

      (case when coalesce(cur.exclude_from_pay,false) then 0 else coalesce(cur.pay_amount,0) end)::numeric(12,2) as cur_payable,
      (case when coalesce(bas.exclude_from_pay,false) then 0 else coalesce(bas.pay_amount,0) end)::numeric(12,2) as bas_payable,

      round(
        (case when coalesce(cur.exclude_from_pay,false) then 0 else coalesce(cur.pay_amount,0) end)
        -
        (case when coalesce(bas.exclude_from_pay,false) then 0 else coalesce(bas.pay_amount,0) end),
        2
      ) as raw_delta_ex,

      (
        c.require_reference_to_pay = true
        and coalesce(cur.exclude_from_pay,false) = false
        and nullif(btrim(coalesce(cur.ref_num,'')),'') is null
        and round(
              (case when coalesce(cur.exclude_from_pay,false) then 0 else coalesce(cur.pay_amount,0) end)
              -
              (case when coalesce(bas.exclude_from_pay,false) then 0 else coalesce(bas.pay_amount,0) end),
              2
            ) > 0
      ) as is_blocked
    from cur0 c
    join (select distinct timesheet_id, segment_id from seg_ids where segment_id is not null) i
      on i.timesheet_id = c.timesheet_id
    left join lateral (
      select
        round(coalesce(nullif(s->>'pay_amount','')::numeric,0),2) as pay_amount,
        coalesce(nullif(s->>'exclude_from_pay','')::boolean,false) as exclude_from_pay,
        nullif(btrim(coalesce(s->>'ref_num','')),'') as ref_num
      from jsonb_array_elements(coalesce(c.cur_segments,'[]'::jsonb)) s
      where s is not null and jsonb_typeof(s)='object'
        and nullif(btrim(coalesce(s->>'segment_id','')),'') = i.segment_id
      limit 1
    ) cur on true
    left join lateral (
      select
        round(coalesce(nullif(s->>'pay_amount','')::numeric,0),2) as pay_amount,
        coalesce(nullif(s->>'exclude_from_pay','')::boolean,false) as exclude_from_pay
      from jsonb_array_elements(coalesce(c.base_json->'segments','[]'::jsonb)) s
      where s is not null and jsonb_typeof(s)='object'
        and nullif(btrim(coalesce(s->>'segment_id','')),'') = i.segment_id
      limit 1
    ) bas on true
  ),
  new_segments as (
    select
      sc.timesheet_id,
      jsonb_agg(
        jsonb_build_object(
          'segment_id', sc.segment_id,
          'pay_amount', case when sc.is_blocked then sc.bas_pay_amount else sc.cur_pay_amount end,
          'exclude_from_pay', case when sc.is_blocked then sc.bas_exclude_from_pay else sc.cur_exclude_from_pay end,
          'ref_num', sc.cur_ref_num
        )
        order by sc.segment_id
      ) as segments_json
    from seg_calc sc
    group by sc.timesheet_id
  ),
  new_adjustments as (
    select
      c.timesheet_id,
      coalesce(
        jsonb_agg(
          jsonb_build_object(
            'id', a.id::text,
            'delta_pay_ex_vat', round(coalesce(a.delta_pay_ex_vat,0),2)
          )
          order by a.id
        ),
        '[]'::jsonb
      ) as adjustments_json
    from cur0 c
    left join public.ts_pay_adjustments a
      on a.timesheet_id = c.timesheet_id
     and a.as_advance = false
    group by c.timesheet_id
  ),
  snap as (
    select
      c.timesheet_id,
      c.candidate_id,
      coalesce(c.base_json, '{}'::jsonb) as base_snapshot_json,
      jsonb_build_object(
        'segments', coalesce(ns.segments_json, '[]'::jsonb),
        'additional_pay_ex_vat', round(coalesce(c.cur_additional,0),2),
        'expenses', jsonb_build_object(
          'expenses_pay_ex_vat', round(coalesce(c.expenses_pay_ex_vat,0),2),
          'travel_pay_ex_vat', round(coalesce(c.travel_pay_ex_vat,0),2),
          'accommodation_pay_ex_vat', round(coalesce(c.accommodation_pay_ex_vat,0),2),
          'other_pay_ex_vat', round(coalesce(c.other_pay_ex_vat,0),2),
          'mileage_pay_ex_vat', round(coalesce(c.mileage_pay_ex_vat,0),2)
        ),
        'adjustments', coalesce(na.adjustments_json, '[]'::jsonb)
      ) as target_snapshot_json
    from cur0 c
    left join new_segments ns on ns.timesheet_id = c.timesheet_id
    left join new_adjustments na on na.timesheet_id = c.timesheet_id
  )
  insert into public.pay_batch_timesheet_snapshots(
    pay_batch_id,
    timesheet_id,
    candidate_id,
    pay_channel,
    base_snapshot_json,
    target_snapshot_json,
    signature,
    created_at_utc
  )
  select
    v_batch_id,
    s.timesheet_id,
    s.candidate_id,
    tc.pay_channel_used,
    s.base_snapshot_json,
    s.target_snapshot_json,
    encode(digest(convert_to(s.target_snapshot_json::text,'utf8'), 'sha256'), 'hex'),
    now()
  from snap s
  join ts_channel tc
    on tc.timesheet_id = s.timesheet_id;

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', v_batch_id::text,
    'pay_date', p_pay_date::text,
    'pay_week_start', v_week_start::text,
    'banking_system_snapshot', v_settings.banking_system,
    'external_paye_system_snapshot', v_settings.external_paye_system,
    'rail_provider_snapshot', v_settings.rail_provider_default,
    'rail_env_snapshot', v_settings.rail_env_default
  );
end;
$$;



commit;

begin;

alter table public.pay_batch_candidates
  add column if not exists settlement_status text null,
  add column if not exists settled_at_utc timestamptz null,
  add column if not exists settled_via text null,
  add column if not exists settled_note text null;

-- settlement_status allowed values
do $$
begin
  if not exists (
    select 1
    from pg_constraint c
    join pg_class t on t.oid = c.conrelid
    join pg_namespace n on n.oid = t.relnamespace
    where n.nspname='public'
      and t.relname='pay_batch_candidates'
      and c.conname='pay_batch_candidates_settlement_status_check'
  ) then
    alter table public.pay_batch_candidates
      add constraint pay_batch_candidates_settlement_status_check
      check (settlement_status is null or settlement_status in ('PENDING','SETTLED','PARTIAL','FAILED','UNPAID'));
  end if;
end$$;

commit;

begin;



-- =========================================================
-- A4.7 pay_settle_revolut(p_pay_batch_id, p_settlement_json)
-- settlement_json: array of {id|transfer_id, status, revolut_transaction_id?, revolut_state?}
-- =========================================================


-- =========================================================
-- A4.8 pay_unpay_batch(p_pay_batch_id, p_actor_user_id, p_reason, p_force boolean)
-- =========================================================

-- =========================================================
-- A4.9 pay_batches_list / pay_batch_get
-- =========================================================



-- =========================================================
-- A4.10 pay_timesheet_impact_preview(p_timesheet_id)
-- Returns baseline totals, current totals, and estimated deltas (raw + payable under ref-blocking rules)
-- =========================================================
create or replace function public.pay_timesheet_impact_preview(p_timesheet_id uuid)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_tf record;
  v_ts record;

  v_require_ref boolean;

  v_base jsonb;
  v_base_segments jsonb;
  v_cur_segments jsonb;

  v_segment_ids text[];
  v_seg_id text;

  v_cur_pay numeric;
  v_cur_excl boolean;
  v_cur_ref text;

  v_base_pay numeric;
  v_base_excl boolean;

  v_cur_payable numeric;
  v_base_payable numeric;

  v_seg_delta_raw numeric;
  v_seg_delta_payable numeric;

  v_seg_sum_raw numeric := 0;
  v_seg_sum_payable numeric := 0;

  v_base_add numeric := 0;
  v_cur_add numeric := 0;

  v_base_exp numeric := 0;
  v_cur_exp numeric := 0;

  v_base_adj numeric := 0;
  v_cur_adj numeric := 0;

  v_delta_raw numeric := 0;
  v_delta_payable numeric := 0;
begin
  if p_timesheet_id is null then
    raise exception 'timesheet_id is required';
  end if;

  select
    tf.timesheet_id,
    tf.client_id,
    tf.invoice_breakdown_json,
    tf.total_pay_ex_vat,
    tf.additional_pay_ex_vat,
    tf.expenses_pay_ex_vat,
    tf.travel_pay_ex_vat,
    tf.accommodation_pay_ex_vat,
    tf.other_pay_ex_vat,
    tf.mileage_pay_ex_vat
  into v_tf
  from public.timesheets_financials tf
  where tf.timesheet_id = p_timesheet_id
    and tf.is_current = true
  limit 1;

  if v_tf.timesheet_id is null then
    raise exception 'timesheet_financials not found for timesheet';
  end if;

  select
    ts.timesheet_id,
    ts.contract_id,
    ts.reference_number
  into v_ts
  from public.timesheets ts
  where ts.timesheet_id = p_timesheet_id
    and ts.is_current = true
  limit 1;

  select
    coalesce(
      case when ct.overrideclientsettings then ct.require_reference_to_pay end,
      cs.pay_reference_required,
      false
    )
  into v_require_ref
  from public.contracts ct
  left join public.client_settings cs on cs.client_id = v_tf.client_id
  where ct.id = v_ts.contract_id
  limit 1;

  select coalesce(tps.last_settled_snapshot_json, '{}'::jsonb)
  into v_base
  from public.timesheet_pay_state tps
  where tps.timesheet_id = p_timesheet_id
  limit 1;

  v_base_segments := coalesce(v_base->'segments','[]'::jsonb);

  -- current segments
  if v_tf.invoice_breakdown_json is not null
     and jsonb_typeof(v_tf.invoice_breakdown_json) = 'object'
     and upper(coalesce(v_tf.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
     and jsonb_typeof(v_tf.invoice_breakdown_json->'segments') = 'array' then
    select coalesce(
      jsonb_agg(
        jsonb_build_object(
          'segment_id', nullif(btrim(coalesce(seg->>'segment_id','')), ''),
          'pay_amount', round(coalesce(nullif(seg->>'pay_amount','')::numeric,0),2),
          'exclude_from_pay', coalesce(nullif(seg->>'exclude_from_pay','')::boolean,false),
          'ref_num', nullif(btrim(coalesce(seg->>'ref_num','')), '')
        )
      ),
      '[]'::jsonb
    )
    into v_cur_segments
    from jsonb_array_elements(v_tf.invoice_breakdown_json->'segments') seg
    where seg is not null and jsonb_typeof(seg)='object';
  else
    v_cur_segments := jsonb_build_array(
      jsonb_build_object(
        'segment_id', ('ts:' || p_timesheet_id::text),
        'pay_amount', round(coalesce(v_tf.total_pay_ex_vat,0),2),
        'exclude_from_pay', false,
        'ref_num', nullif(btrim(coalesce(v_ts.reference_number,'')), '')
      )
    );
  end if;

  select coalesce(array_agg(distinct sid), array[]::text[])
  into v_segment_ids
  from (
    select nullif(btrim(coalesce(s->>'segment_id','')),'') as sid
    from jsonb_array_elements(v_cur_segments) s
    where s is not null and jsonb_typeof(s)='object'
    union
    select nullif(btrim(coalesce(s->>'segment_id','')),'') as sid
    from jsonb_array_elements(v_base_segments) s
    where s is not null and jsonb_typeof(s)='object'
  ) u
  where u.sid is not null;

  foreach v_seg_id in array v_segment_ids
  loop
    select
      round(coalesce(nullif(s->>'pay_amount','')::numeric,0),2) as pay_amount,
      coalesce(nullif(s->>'exclude_from_pay','')::boolean,false) as exclude_from_pay,
      nullif(btrim(coalesce(s->>'ref_num','')), '') as ref_num
    into v_cur_pay, v_cur_excl, v_cur_ref
    from jsonb_array_elements(v_cur_segments) s
    where nullif(btrim(coalesce(s->>'segment_id','')),'') = v_seg_id
    limit 1;

    if v_cur_pay is null then v_cur_pay := 0; end if;
    if v_cur_excl is null then v_cur_excl := false; end if;

    select
      round(coalesce(nullif(s->>'pay_amount','')::numeric,0),2) as pay_amount,
      coalesce(nullif(s->>'exclude_from_pay','')::boolean,false) as exclude_from_pay
    into v_base_pay, v_base_excl
    from jsonb_array_elements(v_base_segments) s
    where nullif(btrim(coalesce(s->>'segment_id','')),'') = v_seg_id
    limit 1;

    if v_base_pay is null then v_base_pay := 0; end if;
    if v_base_excl is null then v_base_excl := false; end if;

    v_cur_payable := case when v_cur_excl then 0 else v_cur_pay end;
    v_base_payable := case when v_base_excl then 0 else v_base_pay end;

    v_seg_delta_raw := round(v_cur_payable - v_base_payable, 2);

    v_seg_delta_payable := v_seg_delta_raw;
    if v_require_ref = true
       and v_cur_excl = false
       and nullif(btrim(coalesce(v_cur_ref,'')),'') is null
       and v_seg_delta_raw > 0 then
      v_seg_delta_payable := 0;
    end if;

    v_seg_sum_raw := round(v_seg_sum_raw + v_seg_delta_raw, 2);
    v_seg_sum_payable := round(v_seg_sum_payable + v_seg_delta_payable, 2);
  end loop;

  v_base_add := round(coalesce(nullif(v_base->>'additional_pay_ex_vat','')::numeric,0),2);
  v_cur_add := round(coalesce(v_tf.additional_pay_ex_vat,0),2);

  v_base_exp :=
    round(coalesce(nullif(v_base #>> '{expenses,expenses_pay_ex_vat}','')::numeric,0),2)
    + round(coalesce(nullif(v_base #>> '{expenses,travel_pay_ex_vat}','')::numeric,0),2)
    + round(coalesce(nullif(v_base #>> '{expenses,accommodation_pay_ex_vat}','')::numeric,0),2)
    + round(coalesce(nullif(v_base #>> '{expenses,other_pay_ex_vat}','')::numeric,0),2)
    + round(coalesce(nullif(v_base #>> '{expenses,mileage_pay_ex_vat}','')::numeric,0),2);

  v_cur_exp :=
    round(coalesce(v_tf.expenses_pay_ex_vat,0),2)
    + round(coalesce(v_tf.travel_pay_ex_vat,0),2)
    + round(coalesce(v_tf.accommodation_pay_ex_vat,0),2)
    + round(coalesce(v_tf.other_pay_ex_vat,0),2)
    + round(coalesce(v_tf.mileage_pay_ex_vat,0),2);

  -- baseline adjustments sum
  select round(coalesce(sum(coalesce(nullif(a->>'delta_pay_ex_vat','')::numeric,0)),0),2)
  into v_base_adj
  from jsonb_array_elements(coalesce(v_base->'adjustments','[]'::jsonb)) a;

  -- current adjustments sum
  select round(coalesce(sum(coalesce(a.delta_pay_ex_vat,0)),0),2)
  into v_cur_adj
  from public.ts_pay_adjustments a
  where a.timesheet_id = p_timesheet_id
    and a.as_advance = false;

  v_delta_raw := round((v_seg_sum_raw + (v_cur_add - v_base_add) + (v_cur_exp - v_base_exp) + (v_cur_adj - v_base_adj)), 2);
  v_delta_payable := round((v_seg_sum_payable + (v_cur_add - v_base_add) + (v_cur_exp - v_base_exp) + (v_cur_adj - v_base_adj)), 2);

  return jsonb_build_object(
    'ok', true,
    'timesheet_id', p_timesheet_id::text,
    'require_reference_to_pay', v_require_ref,
    'baseline', jsonb_build_object(
      'segments_payable_ex_vat', round(coalesce(v_seg_sum_raw,0) - coalesce(v_seg_sum_raw - v_seg_sum_raw,0),2),
      'additional_pay_ex_vat', v_base_add,
      'expenses_total_pay_ex_vat', v_base_exp,
      'adjustments_total_ex_vat', v_base_adj
    ),
    'current', jsonb_build_object(
      'additional_pay_ex_vat', v_cur_add,
      'expenses_total_pay_ex_vat', v_cur_exp,
      'adjustments_total_ex_vat', v_cur_adj
    ),
    'delta', jsonb_build_object(
      'raw_delta_ex_vat', v_delta_raw,
      'payable_delta_ex_vat', v_delta_payable
    )
  );
end;
$$;

commit;


begin;

-- =========================================================
-- A4.6 pay_settle_monzo(p_pay_batch_id, p_actor_user_id, p_confirmed bool)
-- =========================================================
create or replace function public.pay_settle_monzo(
  p_pay_batch_id uuid,
  p_actor_user_id uuid,
  p_confirmed boolean
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_batch record;
  v_week_start date;

  v_timesheet_id uuid;
  v_tf record;
  v_ts record;

  v_require_ref boolean;

  v_base jsonb;
  v_base_segments jsonb;
  v_cur_segments jsonb;
  v_new_segments jsonb;

  v_cur_seg record;
  v_base_seg record;

  v_segment_ids text[];
  v_seg_id text;

  v_cur_pay numeric;
  v_cur_excl boolean;
  v_cur_ref text;

  v_base_pay numeric;
  v_base_excl boolean;

  v_cur_payable numeric;
  v_base_payable numeric;

  v_blocked boolean;

  v_adj jsonb;
  v_snapshot jsonb;
  v_sig text;

  v_adv record;
  v_adv_id uuid;
  v_take_total numeric;
  v_old_outstanding numeric;
  v_new_outstanding numeric;
  v_old_schedule jsonb;
  v_new_schedule jsonb;
  v_old_next_due date;
  v_new_next_due date;

  v_transfer_completed_count int := 0;
  v_candidate_count int := 0;
  v_timesheet_count int := 0;
  v_advance_count int := 0;
begin
  if p_pay_batch_id is null then
    raise exception 'pay_batch_id is required';
  end if;

  if p_confirmed is distinct from true then
    raise exception 'CONFIRM_REQUIRED';
  end if;

  select
    pb.id,
    pb.pay_date,
    pb.status,
    pb.banking_system_snapshot,
    pb.external_paye_system_snapshot
  into v_batch
  from public.pay_batches pb
  where pb.id = p_pay_batch_id
  for update;

  if v_batch.id is null then
    raise exception 'pay_batch not found';
  end if;

  if v_batch.status <> 'WAITING_BANK_CONFIRM' then
    raise exception 'pay_batch must be WAITING_BANK_CONFIRM to settle (current=%)', v_batch.status;
  end if;

  if v_batch.banking_system_snapshot not in ('MONZO_CSV','REVOLUT_CSV') then
    raise exception 'pay_settle_monzo only valid for MONZO_CSV/REVOLUT_CSV batches';
  end if;

  v_week_start := public._pay_week_start_monday(v_batch.pay_date);

  update public.pay_bank_transfers pbt
  set status = 'COMPLETED'
  where pbt.pay_batch_id = p_pay_batch_id
    and pbt.status = 'PENDING';

  get diagnostics v_transfer_completed_count = row_count;

  update public.pay_batch_candidates pbc
  set
    settlement_status = 'SETTLED',
    settled_at_utc = v_now,
    settled_via = v_batch.banking_system_snapshot,
    settled_note = null,
    paye_state = case when pbc.paye_state is null then null else 'SETTLED' end
  where pbc.pay_batch_id = p_pay_batch_id;

  get diagnostics v_candidate_count = row_count;

  update public.pay_batches pb
  set
    status = 'SETTLED',
    monzo_confirmed_at_utc = v_now,
    monzo_confirmed_by_user_id = p_actor_user_id
  where pb.id = p_pay_batch_id;

  -- === Baseline updates for all timesheets referenced by pay_batch_items in this batch ===
  for v_timesheet_id in
    select distinct pbi.timesheet_id
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = p_pay_batch_id
      and pbi.timesheet_id is not null
  loop
    v_timesheet_count := v_timesheet_count + 1;

    select
      tf.timesheet_id,
      tf.candidate_id,
      tf.client_id,
      tf.invoice_breakdown_json,
      tf.total_pay_ex_vat,
      tf.additional_pay_ex_vat,
      tf.expenses_pay_ex_vat,
      tf.travel_pay_ex_vat,
      tf.accommodation_pay_ex_vat,
      tf.other_pay_ex_vat,
      tf.mileage_pay_ex_vat
    into v_tf
    from public.timesheets_financials tf
    where tf.timesheet_id = v_timesheet_id
      and tf.is_current = true
    limit 1;

    if v_tf.timesheet_id is null then
      continue;
    end if;

    select
      ts.timesheet_id,
      ts.contract_id,
      ts.reference_number
    into v_ts
    from public.timesheets ts
    where ts.timesheet_id = v_timesheet_id
      and ts.is_current = true
    limit 1;

    select
      coalesce(
        case when ct.overrideclientsettings then ct.require_reference_to_pay end,
        cs.pay_reference_required,
        false
      )
    into v_require_ref
    from public.contracts ct
    left join public.client_settings cs on cs.client_id = v_tf.client_id
    where ct.id = v_ts.contract_id
    limit 1;

    -- baseline json (may be empty)
    select coalesce(tps.last_settled_snapshot_json, '{}'::jsonb)
    into v_base
    from public.timesheet_pay_state tps
    where tps.timesheet_id = v_timesheet_id
    limit 1;

    v_base_segments := coalesce(v_base->'segments','[]'::jsonb);

    -- current segments
    if v_tf.invoice_breakdown_json is not null
       and jsonb_typeof(v_tf.invoice_breakdown_json) = 'object'
       and upper(coalesce(v_tf.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
       and jsonb_typeof(v_tf.invoice_breakdown_json->'segments') = 'array' then
      select coalesce(
        jsonb_agg(
          jsonb_build_object(
            'segment_id', nullif(btrim(coalesce(seg->>'segment_id','')), ''),
            'pay_amount', round(coalesce(nullif(seg->>'pay_amount','')::numeric,0),2),
            'exclude_from_pay', coalesce(nullif(seg->>'exclude_from_pay','')::boolean,false),
            'ref_num', nullif(btrim(coalesce(seg->>'ref_num','')), '')
          )
        ),
        '[]'::jsonb
      )
      into v_cur_segments
      from jsonb_array_elements(v_tf.invoice_breakdown_json->'segments') seg
      where seg is not null and jsonb_typeof(seg)='object';
    else
      v_cur_segments := jsonb_build_array(
        jsonb_build_object(
          'segment_id', ('ts:' || v_timesheet_id::text),
          'pay_amount', round(coalesce(v_tf.total_pay_ex_vat,0),2),
          'exclude_from_pay', false,
          'ref_num', nullif(btrim(coalesce(v_ts.reference_number,'')), '')
        )
      );
    end if;

    -- union of segment ids
    select coalesce(array_agg(distinct sid), array[]::text[])
    into v_segment_ids
    from (
      select nullif(btrim(coalesce(s->>'segment_id','')),'') as sid
      from jsonb_array_elements(v_cur_segments) s
      where s is not null and jsonb_typeof(s)='object'
      union
      select nullif(btrim(coalesce(s->>'segment_id','')),'') as sid
      from jsonb_array_elements(v_base_segments) s
      where s is not null and jsonb_typeof(s)='object'
    ) u
    where u.sid is not null;

    v_new_segments := '[]'::jsonb;

    foreach v_seg_id in array v_segment_ids
    loop
      -- current segment fields
      select
        round(coalesce(nullif(s->>'pay_amount','')::numeric,0),2) as pay_amount,
        coalesce(nullif(s->>'exclude_from_pay','')::boolean,false) as exclude_from_pay,
        nullif(btrim(coalesce(s->>'ref_num','')), '') as ref_num
      into v_cur_pay, v_cur_excl, v_cur_ref
      from jsonb_array_elements(v_cur_segments) s
      where nullif(btrim(coalesce(s->>'segment_id','')),'') = v_seg_id
      limit 1;

      if v_cur_pay is null then v_cur_pay := 0; end if;
      if v_cur_excl is null then v_cur_excl := false; end if;

      -- base segment fields
      select
        round(coalesce(nullif(s->>'pay_amount','')::numeric,0),2) as pay_amount,
        coalesce(nullif(s->>'exclude_from_pay','')::boolean,false) as exclude_from_pay
      into v_base_pay, v_base_excl
      from jsonb_array_elements(v_base_segments) s
      where nullif(btrim(coalesce(s->>'segment_id','')),'') = v_seg_id
      limit 1;

      if v_base_pay is null then v_base_pay := 0; end if;
      if v_base_excl is null then v_base_excl := false; end if;

      v_cur_payable := case when v_cur_excl then 0 else v_cur_pay end;
      v_base_payable := case when v_base_excl then 0 else v_base_pay end;

      v_blocked :=
        (v_require_ref = true)
        and (v_cur_excl = false)
        and (nullif(btrim(coalesce(v_cur_ref,'')),'') is null)
        and round(v_cur_payable - v_base_payable, 2) > 0;

      if v_blocked then
        -- preserve base values for blocked-positive segments
        v_new_segments := v_new_segments || jsonb_build_array(
          jsonb_build_object(
            'segment_id', v_seg_id,
            'pay_amount', round(v_base_pay,2),
            'exclude_from_pay', v_base_excl,
            'ref_num', v_cur_ref
          )
        );
      else
        v_new_segments := v_new_segments || jsonb_build_array(
          jsonb_build_object(
            'segment_id', v_seg_id,
            'pay_amount', round(v_cur_pay,2),
            'exclude_from_pay', v_cur_excl,
            'ref_num', v_cur_ref
          )
        );
      end if;
    end loop;

    -- current adjustments for this timesheet
    select coalesce(
      jsonb_agg(
        jsonb_build_object(
          'id', a.id::text,
          'delta_pay_ex_vat', round(coalesce(a.delta_pay_ex_vat,0),2)
        )
        order by a.created_at asc, a.id
      ),
      '[]'::jsonb
    )
    into v_adj
    from public.ts_pay_adjustments a
    where a.timesheet_id = v_timesheet_id
      and a.as_advance = false;

    v_snapshot := jsonb_build_object(
      'segments', v_new_segments,
      'additional_pay_ex_vat', round(coalesce(v_tf.additional_pay_ex_vat,0),2),
      'expenses', jsonb_build_object(
        'expenses_pay_ex_vat', round(coalesce(v_tf.expenses_pay_ex_vat,0),2),
        'travel_pay_ex_vat', round(coalesce(v_tf.travel_pay_ex_vat,0),2),
        'accommodation_pay_ex_vat', round(coalesce(v_tf.accommodation_pay_ex_vat,0),2),
        'other_pay_ex_vat', round(coalesce(v_tf.other_pay_ex_vat,0),2),
        'mileage_pay_ex_vat', round(coalesce(v_tf.mileage_pay_ex_vat,0),2)
      ),
      'adjustments', v_adj
    );

v_sig := encode(extensions.digest(convert_to(v_snapshot::text, 'utf8'), 'sha256'::text), 'hex');

    insert into public.timesheet_pay_state_history(
      timesheet_id,
      pay_batch_id,
      settled_at_utc,
      snapshot_json,
      signature
    )
    values (
      v_timesheet_id,
      p_pay_batch_id,
      v_now,
      v_snapshot,
      v_sig
    );

    insert into public.timesheet_pay_state(
      timesheet_id,
      last_settled_snapshot_json,
      last_settled_signature,
      last_settled_pay_batch_id,
      last_settled_at_utc
    )
    values (
      v_timesheet_id,
      v_snapshot,
      v_sig,
      p_pay_batch_id,
      v_now
    )
    on conflict (timesheet_id) do update
    set
      last_settled_snapshot_json = excluded.last_settled_snapshot_json,
      last_settled_signature = excluded.last_settled_signature,
      last_settled_pay_batch_id = excluded.last_settled_pay_batch_id,
      last_settled_at_utc = excluded.last_settled_at_utc;
  end loop;

  -- === Apply loan/advance changes (record patches) ===
  for v_adv in
    select
      (regexp_replace(pbi.source_ref, '^advance:', ''))::uuid as advance_id,
      round(sum(abs(coalesce(pbi.amount_inc_vat, pbi.amount_ex_vat, 0))),2) as taken_total
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = p_pay_batch_id
      and pbi.item_type = 'LOAN_REPAYMENT'
      and pbi.source_ref like 'advance:%'
    group by (regexp_replace(pbi.source_ref, '^advance:', ''))::uuid
  loop
    v_adv_id := v_adv.advance_id;
    v_take_total := round(coalesce(v_adv.taken_total,0),2);
    if v_take_total <= 0 then
      continue;
    end if;

    -- prevent duplicate patching for same batch+advance
    if exists (
      select 1 from public.pay_advance_patches pap
      where pap.pay_batch_id = p_pay_batch_id
        and pap.advance_id = v_adv_id
      limit 1
    ) then
      continue;
    end if;

    select
      pa.outstanding_amount,
      pa.schedule_json,
      pa.next_due_week_start
    into v_old_outstanding, v_old_schedule, v_old_next_due
    from public.pay_advances pa
    where pa.id = v_adv_id
      and pa.status::text = 'ACTIVE'
    for update;

    if v_old_schedule is null then
      continue;
    end if;

    -- Update schedule entry for this pay week: amount := amount + taken_total (e.g. -100 + 60 = -40)
    select coalesce(
      jsonb_agg(
        case
          when nullif(e->>'week_start','')::date = v_week_start
          then jsonb_set(e, '{amount}', to_jsonb(round(coalesce(nullif(e->>'amount','')::numeric,0) + v_take_total, 2)), true)
          else e
        end
        order by ord
      ),
      '[]'::jsonb
    )
    into v_new_schedule
    from jsonb_array_elements(v_old_schedule) with ordinality as t(e, ord);

    v_new_outstanding := round(greatest(coalesce(v_old_outstanding,0) - v_take_total, 0), 2);

    select min(nullif(x->>'week_start','')::date)
    into v_new_next_due
    from jsonb_array_elements(v_new_schedule) x
    where coalesce(nullif(x->>'amount','')::numeric,0) < 0;

    update public.pay_advances pa
    set
      schedule_json = v_new_schedule,
      outstanding_amount = v_new_outstanding,
      next_due_week_start = v_new_next_due,
      status = case when v_new_outstanding <= 0 then 'PAID_OFF'::pay_advance_status_enum else pa.status end,
      updated_at = now()
    where pa.id = v_adv_id;

    insert into public.pay_advance_patches(
      advance_id,
      pay_batch_id,
      old_outstanding_amount,
      new_outstanding_amount,
      old_schedule_json,
      new_schedule_json,
      old_next_due_week_start,
      new_next_due_week_start,
      patched_at_utc
    )
    values (
      v_adv_id,
      p_pay_batch_id,
      v_old_outstanding,
      v_new_outstanding,
      v_old_schedule,
      v_new_schedule,
      v_old_next_due,
      v_new_next_due,
      v_now
    );

    v_advance_count := v_advance_count + 1;
  end loop;

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'status', 'SETTLED',
    'banking_system_snapshot', v_batch.banking_system_snapshot,
    'completed_transfers_updated', v_transfer_completed_count,
    'candidates_marked_settled', v_candidate_count,
    'timesheets_baselined', v_timesheet_count,
    'advances_patched', v_advance_count
  );
end;
$$;

-- =========================================================
-- A4.7 pay_settle_revolut(p_pay_batch_id, p_settlement_json)
-- settlement_json: array of {id|transfer_id, status, revolut_transaction_id?, revolut_state?}
-- =========================================================

-- =========================================================
-- A4.8 pay_unpay_batch(p_pay_batch_id, p_actor_user_id, p_reason, p_force boolean)
-- =========================================================
create or replace function public.pay_unpay_batch(
  p_pay_batch_id uuid,
  p_actor_user_id uuid,
  p_reason text,
  p_force boolean
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_batch record;
  v_timesheet_id uuid;

  v_aff_timesheets uuid[] := array[]::uuid[];

  v_hist record;
  v_latest record;

  v_patch record;
  v_old_outstanding numeric;
  v_old_schedule jsonb;
  v_old_next_due date;

  v_reverted_adv int := 0;
  v_removed_hist int := 0;
  v_rebuilt_states int := 0;
begin
  if p_pay_batch_id is null then
    raise exception 'pay_batch_id is required';
  end if;

  select
    pb.id,
    pb.status
  into v_batch
  from public.pay_batches pb
  where pb.id = p_pay_batch_id
  for update;

  if v_batch.id is null then
    raise exception 'pay_batch not found';
  end if;

  if v_batch.status in ('SETTLED','PARTIAL') and p_force is distinct from true then
    raise exception 'UNPAY_REQUIRES_FORCE_FOR_SETTLED_BATCH';
  end if;

  -- Revert advances using patches
  for v_patch in
    select
      pap.advance_id,
      pap.old_outstanding_amount,
      pap.old_schedule_json,
      pap.old_next_due_week_start
    from public.pay_advance_patches pap
    where pap.pay_batch_id = p_pay_batch_id
  loop
    v_old_outstanding := v_patch.old_outstanding_amount;
    v_old_schedule := v_patch.old_schedule_json;
    v_old_next_due := v_patch.old_next_due_week_start;

    update public.pay_advances pa
    set
      outstanding_amount = v_old_outstanding,
      schedule_json = v_old_schedule,
      next_due_week_start = v_old_next_due,
      status = case when coalesce(v_old_outstanding,0) <= 0 then 'PAID_OFF'::pay_advance_status_enum else 'ACTIVE'::pay_advance_status_enum end,
      updated_at = now()
    where pa.id = v_patch.advance_id;

    v_reverted_adv := v_reverted_adv + 1;
  end loop;

  -- Gather affected timesheets from history rows for this batch
  select coalesce(array_agg(distinct h.timesheet_id), array[]::uuid[])
  into v_aff_timesheets
  from public.timesheet_pay_state_history h
  where h.pay_batch_id = p_pay_batch_id;

  -- Remove history rows for this batch
  delete from public.timesheet_pay_state_history h
  where h.pay_batch_id = p_pay_batch_id;

  get diagnostics v_removed_hist = row_count;

  -- Rebuild timesheet_pay_state for affected timesheets
  foreach v_timesheet_id in array v_aff_timesheets
  loop
    select
      h.timesheet_id,
      h.pay_batch_id,
      h.settled_at_utc,
      h.snapshot_json,
      h.signature
    into v_latest
    from public.timesheet_pay_state_history h
    where h.timesheet_id = v_timesheet_id
    order by h.settled_at_utc desc, h.id desc
    limit 1;

    if v_latest.timesheet_id is null then
      delete from public.timesheet_pay_state tps
      where tps.timesheet_id = v_timesheet_id;
    else
      insert into public.timesheet_pay_state(
        timesheet_id,
        last_settled_snapshot_json,
        last_settled_signature,
        last_settled_pay_batch_id,
        last_settled_at_utc
      )
      values (
        v_timesheet_id,
        v_latest.snapshot_json,
        v_latest.signature,
        v_latest.pay_batch_id,
        v_latest.settled_at_utc
      )
      on conflict (timesheet_id) do update
      set
        last_settled_snapshot_json = excluded.last_settled_snapshot_json,
        last_settled_signature = excluded.last_settled_signature,
        last_settled_pay_batch_id = excluded.last_settled_pay_batch_id,
        last_settled_at_utc = excluded.last_settled_at_utc;
    end if;

    v_rebuilt_states := v_rebuilt_states + 1;
  end loop;

  -- Mark candidate rows as UNPAID
  update public.pay_batch_candidates pbc
  set
    settlement_status = 'UNPAID',
    settled_at_utc = null,
    settled_via = null,
    settled_note = coalesce(nullif(btrim(coalesce(p_reason,'')),''), 'UNPAID'),
    paye_state = case when pbc.paye_state is null then null else 'READY' end
  where pbc.pay_batch_id = p_pay_batch_id;

  -- Mark batch as UNPAID (preserve audit; do not delete)
  update public.pay_batches pb
  set status = 'UNPAID'
  where pb.id = p_pay_batch_id;

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'action', 'UNPAID',
    'reverted_advances', v_reverted_adv,
    'deleted_history_rows', v_removed_hist,
    'rebuilt_timesheet_states', v_rebuilt_states
  );
end;
$$;

-- =========================================================
-- A4.9 pay_batches_list / pay_batch_get
-- =========================================================

create or replace function public.pay_execute_bank(
  p_pay_batch_id uuid,
  p_pay_channel_scope text,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_scope text := upper(coalesce(p_pay_channel_scope,''));
  v_batch record;

  v_bulk_ref_num int;
  v_bulk_ref_date date;
  v_bulk_reference text;

  v_provider text;
  v_env text;

  v_tax_year_start date;
  v_tax_week int;

  v_pay_week_start date;
  v_pay_week_end date;

  v_pending_count int := 0;
  v_blocked_count int := 0;

  v_auto_execute boolean := false;

  v_transfers jsonb := '[]'::jsonb;
  v_blocked_reasons jsonb := '[]'::jsonb;
begin
  if p_pay_batch_id is null then
    raise exception 'pay_batch_id is required';
  end if;
  if v_scope not in ('PAYE','UMBRELLA','ALL') then
    raise exception 'Invalid pay_channel_scope (PAYE|UMBRELLA|ALL)';
  end if;

  -- Lock the batch row to prevent concurrent execution / bulk ref allocation races
  select
    pb.id,
    pb.status,
    pb.pay_date,
    pb.banking_system_snapshot,
    pb.external_paye_system_snapshot,
    pb.bulk_ref_num,
    pb.bulk_ref_date,
    pb.bulk_reference,
    pb.rail_provider_snapshot,
    pb.rail_env_snapshot
  into v_batch
  from public.pay_batches pb
  where pb.id = p_pay_batch_id
  for update;

  if v_batch.id is null then
    raise exception 'pay_batch not found';
  end if;

  if v_batch.status not in ('DRAFT','READY','PARTIAL','WAITING_BANK_CONFIRM','DRAFT_CREATED') then
    raise exception 'pay_batch status not valid for execute (current=%)', v_batch.status;
  end if;

  if v_batch.pay_date is null then
    raise exception 'pay_batch pay_date is required';
  end if;

  v_provider := upper(coalesce(v_batch.rail_provider_snapshot, 'CSV'));
  v_env := upper(coalesce(v_batch.rail_env_snapshot, 'PROD'));

  -- Allocate bulk reference ONCE per batch (digits-only canonical)
  if v_batch.bulk_reference is null then
    v_bulk_ref_num := v_batch.bulk_ref_num;
    if v_bulk_ref_num is null then
      v_bulk_ref_num := nextval('public.pay_bulk_ref_seq')::int;
    end if;

    v_bulk_ref_date := coalesce(v_batch.bulk_ref_date, v_batch.pay_date);
    if v_bulk_ref_date is null then
      raise exception 'pay_batch pay_date is required to allocate bulk_reference';
    end if;

    v_bulk_reference := to_char(v_bulk_ref_date, 'DDMMYYYY') || lpad(v_bulk_ref_num::text, 7, '0');

    update public.pay_batches pb2
    set bulk_ref_num = v_bulk_ref_num,
        bulk_ref_date = v_bulk_ref_date,
        bulk_reference = v_bulk_reference
    where pb2.id = p_pay_batch_id;

    v_batch.bulk_ref_num := v_bulk_ref_num;
    v_batch.bulk_ref_date := v_bulk_ref_date;
    v_batch.bulk_reference := v_bulk_reference;
  else
    v_bulk_ref_num := v_batch.bulk_ref_num;
    v_bulk_ref_date := v_batch.bulk_ref_date;
    v_bulk_reference := v_batch.bulk_reference;
  end if;

  -- PAYE tax-week number (week 1 starts 6 April)
  v_tax_year_start := make_date(extract(year from v_batch.pay_date)::int, 4, 6);
  if v_batch.pay_date < v_tax_year_start then
    v_tax_year_start := make_date((extract(year from v_batch.pay_date)::int) - 1, 4, 6);
  end if;
  v_tax_week := ((v_batch.pay_date - v_tax_year_start) / 7) + 1;
  if v_tax_week < 1 then v_tax_week := 1; end if;

  -- Bucket for non-timesheet items (loan repayments etc.)
  v_pay_week_start := public._pay_week_start_monday(v_batch.pay_date);
  v_pay_week_end := (v_pay_week_start + 6);

  -- Validate PAYE net inputs if PAYE is being executed
  if v_scope in ('PAYE','ALL') then
    if exists (
      select 1
      from public.pay_batch_candidates pbc_chk
      where pbc_chk.pay_batch_id = p_pay_batch_id
        and pbc_chk.paye_state is not null
        and pbc_chk.paye_state <> 'READY'
      limit 1
    ) then
      raise exception 'PAYE_NOT_READY: some PAYE candidates are not READY';
    end if;

    if exists (
      select 1
      from public.pay_batch_candidates pbc_chk2
      left join lateral (
        select pni_chk.net_amount
        from public.pay_batch_paye_net_inputs pni_chk
        where pni_chk.pay_batch_candidate_id = pbc_chk2.id
        order by pni_chk.imported_at_utc desc
        limit 1
      ) ni_chk on true
      where pbc_chk2.pay_batch_id = p_pay_batch_id
        and pbc_chk2.paye_state is not null
        and ni_chk.net_amount is null
      limit 1
    ) then
      raise exception 'PAYE_NET_MISSING: missing net pay for one or more PAYE candidates';
    end if;
  end if;

  -- Temp staging for transfer groups (idempotent within transaction)
  create temp table if not exists _tmp_pay_transfer_groups (
    pay_channel text not null,
    candidate_id uuid not null,
    umbrella_id uuid null,
    week_ending_bucket date null,
    amount numeric not null,
    currency text not null,
    status text not null,
    rail_state text null,
    rail_meta_json jsonb null,
    payment_reference text null,
    payee_name text null,
    sort_code text null,
    account_number text null,
    account_type text null,
    bank_details_hash_snapshot text null,
    payee_entity_kind text not null,
    payee_entity_id uuid null,
    transfer_group_key text not null,
    grouping_mode_used text null
  ) on commit drop;

  delete from _tmp_pay_transfer_groups;

  -- =========================================================
  -- PAYE groups: candidate_id only (one transfer per candidate)
  -- =========================================================
  if v_scope in ('PAYE','ALL') then
    insert into _tmp_pay_transfer_groups(
      pay_channel,
      candidate_id,
      umbrella_id,
      week_ending_bucket,
      amount,
      currency,
      status,
      rail_state,
      rail_meta_json,
      payment_reference,
      payee_name,
      sort_code,
      account_number,
      account_type,
      bank_details_hash_snapshot,
      payee_entity_kind,
      payee_entity_id,
      transfer_group_key,
      grouping_mode_used
    )
    select
      'PAYE'::text as pay_channel,
      pbc.candidate_id,
      null::uuid as umbrella_id,
      null::date as week_ending_bucket,
      round(ni.net_amount, 2) as amount,
      'GBP'::text as currency,

      case
        when (sc_norm is null or acct_norm is null or payee_nm is null) then 'BLOCKED'
        else 'PENDING'
      end as status,

      case
        when (sc_norm is null or acct_norm is null or payee_nm is null) then 'BLOCKED_BANK_DETAILS'
        else null
      end as rail_state,

      case
        when (sc_norm is null or acct_norm is null or payee_nm is null) then
          jsonb_build_object(
            'reason_code', 'BANK_DETAILS_MISSING',
            'missing', (
              select jsonb_agg(x.m)
              from (
                select 'sort_code'::text as m where sc_norm is null
                union all
                select 'account_number'::text where acct_norm is null
                union all
                select 'payee_name'::text where payee_nm is null
              ) x
            )
          )
        else null
      end as rail_meta_json,

      ('Pay - week ' || v_tax_week::text) as payment_reference,

      payee_nm as payee_name,
      sc_norm as sort_code,
      acct_norm as account_number,
      'Personal'::text as account_type,

      c.bank_details_hash as bank_details_hash_snapshot,

      'CANDIDATE'::text as payee_entity_kind,
      pbc.candidate_id as payee_entity_id,

      (pbc.candidate_id::text) as transfer_group_key,
      'CANDIDATE'::text as grouping_mode_used
    from public.pay_batch_candidates pbc
    join public.candidates c
      on c.id = pbc.candidate_id
    join lateral (
      select pni.net_amount
      from public.pay_batch_paye_net_inputs pni
      where pni.pay_batch_candidate_id = pbc.id
      order by pni.imported_at_utc desc
      limit 1
    ) ni on true
    join lateral (
      select
        nullif(btrim(coalesce(c.account_holder, c.display_name, concat_ws(' ', c.first_name, c.last_name))), '') as payee_nm,
        case
          when length(regexp_replace(coalesce(c.sort_code,''), '[^0-9]', '', 'g')) = 6 then
            substr(regexp_replace(coalesce(c.sort_code,''), '[^0-9]', '', 'g'), 1, 2) || '-' ||
            substr(regexp_replace(coalesce(c.sort_code,''), '[^0-9]', '', 'g'), 3, 2) || '-' ||
            substr(regexp_replace(coalesce(c.sort_code,''), '[^0-9]', '', 'g'), 5, 2)
          else null
        end as sc_norm,
        nullif(regexp_replace(coalesce(c.account_number,''), '[^0-9]', '', 'g'), '') as acct_norm
    ) b on true
    where pbc.pay_batch_id = p_pay_batch_id
      and pbc.paye_state is not null
      and round(coalesce(ni.net_amount,0),2) > 0;
  end if;

  -- =========================================================
  -- UMBRELLA groups: candidate_id + week_ending_bucket (default)
  -- Payee is umbrella (funds go to umbrella)
  -- =========================================================
  if v_scope in ('UMBRELLA','ALL') then
    insert into _tmp_pay_transfer_groups(
      pay_channel,
      candidate_id,
      umbrella_id,
      week_ending_bucket,
      amount,
      currency,
      status,
      rail_state,
      rail_meta_json,
      payment_reference,
      payee_name,
      sort_code,
      account_number,
      account_type,
      bank_details_hash_snapshot,
      payee_entity_kind,
      payee_entity_id,
      transfer_group_key,
      grouping_mode_used
    )
    select
      'UMBRELLA'::text as pay_channel,
      pbc.candidate_id,
      g.umb_id as umbrella_id,
      g.wk_end as week_ending_bucket,
      round(greatest(g.sum_amt,0),2) as amount,
      'GBP'::text as currency,

      case
        when (g.umb_id is null) then 'BLOCKED'
        when (sc_norm is null or acct_norm is null or payee_nm is null) then 'BLOCKED'
        else 'PENDING'
      end as status,

      case
        when (g.umb_id is null) then 'BLOCKED_UMBRELLA_MISSING'
        when (sc_norm is null or acct_norm is null or payee_nm is null) then 'BLOCKED_BANK_DETAILS'
        else null
      end as rail_state,

      case
        when (g.umb_id is null) then
          jsonb_build_object('reason_code','UMBRELLA_MISSING')
        when (sc_norm is null or acct_norm is null or payee_nm is null) then
          jsonb_build_object(
            'reason_code', 'BANK_DETAILS_MISSING',
            'missing', (
              select jsonb_agg(x.m)
              from (
                select 'sort_code'::text as m where sc_norm is null
                union all
                select 'account_number'::text where acct_norm is null
                union all
                select 'payee_name'::text where payee_nm is null
              ) x
            )
          )
        else null
      end as rail_meta_json,

      left(
        btrim(concat_ws(' ', nullif(btrim(c.last_name),''), nullif(btrim(c.first_name),''))),
        18
      ) as payment_reference,

      payee_nm as payee_name,
      sc_norm as sort_code,
      acct_norm as account_number,

      -- ✅ FIX (A3): umbrellas table has no account_type column; snapshot is always Business
      'Business'::text as account_type,

      u.bank_details_hash as bank_details_hash_snapshot,

      case when g.umb_id is null then 'CANDIDATE' else 'UMBRELLA' end as payee_entity_kind,
      case when g.umb_id is null then pbc.candidate_id else g.umb_id end as payee_entity_id,

      (pbc.candidate_id::text || '|' || g.wk_end::text) as transfer_group_key,
      'CANDIDATE_WEEK'::text as grouping_mode_used
    from (
      select
        pbc0.id as pay_batch_candidate_id,
        pbc0.candidate_id,
        coalesce(vts.week_ending_date, v_pay_week_end) as wk_end,
        max(pbi0.umbrella_id) as umb_id,
        sum(coalesce(pbi0.amount_inc_vat,0)) as sum_amt
      from public.pay_batch_candidates pbc0
      join public.pay_batch_items pbi0
        on pbi0.pay_batch_candidate_id = pbc0.id
       and pbi0.pay_channel = 'UMBRELLA'
       and pbi0.item_type <> 'DEBT_CREATED'
      left join public.v_timesheets_summary_base vts
        on vts.timesheet_id = pbi0.timesheet_id
      where pbc0.pay_batch_id = p_pay_batch_id
      group by pbc0.id, pbc0.candidate_id, coalesce(vts.week_ending_date, v_pay_week_end)
      having round(greatest(sum(coalesce(pbi0.amount_inc_vat,0)),0),2) > 0
    ) g
    join public.pay_batch_candidates pbc
      on pbc.id = g.pay_batch_candidate_id
    join public.candidates c
      on c.id = pbc.candidate_id
    left join public.umbrellas u
      on u.id = g.umb_id
    join lateral (
      select
        nullif(btrim(coalesce(u.name,'')), '') as payee_nm,
        case
          when length(regexp_replace(coalesce(u.sort_code,''), '[^0-9]', '', 'g')) = 6 then
            substr(regexp_replace(coalesce(u.sort_code,''), '[^0-9]', '', 'g'), 1, 2) || '-' ||
            substr(regexp_replace(coalesce(u.sort_code,''), '[^0-9]', '', 'g'), 3, 2) || '-' ||
            substr(regexp_replace(coalesce(u.sort_code,''), '[^0-9]', '', 'g'), 5, 2)
          else null
        end as sc_norm,
        nullif(regexp_replace(coalesce(u.account_number,''), '[^0-9]', '', 'g'), '') as acct_norm
    ) b on true;
  end if;

  -- Clear old item→transfer links for this scope (rebuild coherently)
  if v_scope in ('PAYE','ALL') then
    update public.pay_batch_items pbi_clr
    set pay_bank_transfer_id = null
    from public.pay_batch_candidates pbc_clr
    where pbi_clr.pay_batch_candidate_id = pbc_clr.id
      and pbc_clr.pay_batch_id = p_pay_batch_id
      and pbi_clr.pay_channel = 'PAYE'
      and pbi_clr.item_type <> 'DEBT_CREATED';
  end if;

  if v_scope in ('UMBRELLA','ALL') then
    update public.pay_batch_items pbi_clr2
    set pay_bank_transfer_id = null
    from public.pay_batch_candidates pbc_clr2
    where pbi_clr2.pay_batch_candidate_id = pbc_clr2.id
      and pbc_clr2.pay_batch_id = p_pay_batch_id
      and pbi_clr2.pay_channel = 'UMBRELLA'
      and pbi_clr2.item_type <> 'DEBT_CREATED';
  end if;

  -- Upsert transfers (idempotent by (pay_batch_id, pay_channel, transfer_group_key))
  insert into public.pay_bank_transfers(
    pay_batch_id,
    candidate_id,
    umbrella_id,
    pay_channel,
    amount,
    currency,
    status,
    payment_reference,
    payee_name,
    sort_code,
    account_number,
    account_type,
    rail_provider,
    rail_env,
    request_id,
    rail_tx_id,
    rail_state,
    rail_meta_json,
    bank_details_hash_snapshot,
    payee_entity_kind,
    payee_entity_id,
    transfer_group_key,
    grouping_mode_used,
    week_ending_bucket
  )
  select
    p_pay_batch_id,
    g.candidate_id,
    g.umbrella_id,
    g.pay_channel,
    g.amount,
    g.currency,
    g.status,
    g.payment_reference,
    g.payee_name,
    g.sort_code,
    g.account_number,
    g.account_type,
    v_provider,
    v_env,
    null::text as request_id,
    null::text as rail_tx_id,
    g.rail_state,
    g.rail_meta_json,
    g.bank_details_hash_snapshot,
    g.payee_entity_kind,
    g.payee_entity_id,
    g.transfer_group_key,
    g.grouping_mode_used,
    g.week_ending_bucket
  from _tmp_pay_transfer_groups g
  where (v_scope = 'ALL' or g.pay_channel = v_scope)
  on conflict (pay_batch_id, pay_channel, transfer_group_key)
  do update set
    candidate_id = excluded.candidate_id,
    umbrella_id = excluded.umbrella_id,
    amount = excluded.amount,
    currency = excluded.currency,
    status = excluded.status,
    payment_reference = excluded.payment_reference,
    payee_name = excluded.payee_name,
    sort_code = excluded.sort_code,
    account_number = excluded.account_number,
    account_type = excluded.account_type,
    rail_provider = excluded.rail_provider,
    rail_env = excluded.rail_env,
    rail_state = excluded.rail_state,
    rail_meta_json = excluded.rail_meta_json,
    bank_details_hash_snapshot = excluded.bank_details_hash_snapshot,
    payee_entity_kind = excluded.payee_entity_kind,
    payee_entity_id = excluded.payee_entity_id,
    grouping_mode_used = excluded.grouping_mode_used,
    week_ending_bucket = excluded.week_ending_bucket
  where public.pay_bank_transfers.status in ('PENDING','BLOCKED','FAILED');

  -- Ensure request_id is populated and stable (default to id::text)
  update public.pay_bank_transfers pbt_req
  set request_id = coalesce(nullif(pbt_req.request_id,''), pbt_req.id::text)
  where pbt_req.pay_batch_id = p_pay_batch_id
    and (v_scope = 'ALL' or pbt_req.pay_channel = v_scope)
    and (pbt_req.request_id is null or pbt_req.request_id = '');

  -- Link items → transfers
  if v_scope in ('PAYE','ALL') then
    update public.pay_batch_items pbi_l
    set pay_bank_transfer_id = pbt_l.id
    from public.pay_batch_candidates pbc_l
    join public.pay_bank_transfers pbt_l
      on pbt_l.pay_batch_id = p_pay_batch_id
     and pbt_l.pay_channel = 'PAYE'
     and pbt_l.candidate_id = pbc_l.candidate_id
     and pbt_l.transfer_group_key = (pbc_l.candidate_id::text)
    where pbi_l.pay_batch_candidate_id = pbc_l.id
      and pbc_l.pay_batch_id = p_pay_batch_id
      and pbi_l.pay_channel = 'PAYE'
      and pbi_l.item_type <> 'DEBT_CREATED';
  end if;

  if v_scope in ('UMBRELLA','ALL') then
    update public.pay_batch_items pbi_u
    set pay_bank_transfer_id = pbt_u.id
    from public.pay_batch_candidates pbc_u
    left join public.v_timesheets_summary_base vts_u
      on vts_u.timesheet_id = pbi_u.timesheet_id
    join public.pay_bank_transfers pbt_u
      on pbt_u.pay_batch_id = p_pay_batch_id
     and pbt_u.pay_channel = 'UMBRELLA'
     and pbt_u.candidate_id = pbc_u.candidate_id
     and pbt_u.week_ending_bucket = coalesce(vts_u.week_ending_date, v_pay_week_end)
    where pbi_u.pay_batch_candidate_id = pbc_u.id
      and pbc_u.pay_batch_id = p_pay_batch_id
      and pbi_u.pay_channel = 'UMBRELLA'
      and pbi_u.item_type <> 'DEBT_CREATED';
  end if;

  -- Counts for UI
  select count(*)::int
  into v_pending_count
  from public.pay_bank_transfers pbt_cnt
  where pbt_cnt.pay_batch_id = p_pay_batch_id
    and (v_scope = 'ALL' or pbt_cnt.pay_channel = v_scope)
    and pbt_cnt.status = 'PENDING';

  select count(*)::int
  into v_blocked_count
  from public.pay_bank_transfers pbt_cnt2
  where pbt_cnt2.pay_batch_id = p_pay_batch_id
    and (v_scope = 'ALL' or pbt_cnt2.pay_channel = v_scope)
    and pbt_cnt2.status = 'BLOCKED';

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'id', pbt_blk.id::text,
        'pay_channel', pbt_blk.pay_channel,
        'candidate_id', pbt_blk.candidate_id::text,
        'umbrella_id', case when pbt_blk.umbrella_id is null then null else pbt_blk.umbrella_id::text end,
        'transfer_group_key', pbt_blk.transfer_group_key,
        'week_ending_bucket', case when pbt_blk.week_ending_bucket is null then null else pbt_blk.week_ending_bucket::text end,
        'rail_state', pbt_blk.rail_state,
        'rail_meta_json', pbt_blk.rail_meta_json
      )
      order by pbt_blk.pay_channel, pbt_blk.candidate_id, pbt_blk.id
    ),
    '[]'::jsonb
  )
  into v_blocked_reasons
  from public.pay_bank_transfers pbt_blk
  where pbt_blk.pay_batch_id = p_pay_batch_id
    and (v_scope = 'ALL' or pbt_blk.pay_channel = v_scope)
    and pbt_blk.status = 'BLOCKED';

  -- ✅ FIX (B-safe): determine READY vs WAITING_BANK_CONFIRM from rail capability, not provider string
  select coalesce(sd.rail_supports_auto_execute, false)
  into v_auto_execute
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;

  -- Batch status update (rail-aware via capability)
  update public.pay_batches pb3
  set status = case
    when v_pending_count > 0 then
      case when coalesce(v_auto_execute,false) = true then 'READY' else 'WAITING_BANK_CONFIRM' end
    else
      'PARTIAL'
  end
  where pb3.id = p_pay_batch_id;

  -- Return transfers (including snapshot fields)
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'id', pbt.id::text,
        'pay_batch_id', pbt.pay_batch_id::text,
        'candidate_id', pbt.candidate_id::text,
        'umbrella_id', case when pbt.umbrella_id is null then null else pbt.umbrella_id::text end,
        'pay_channel', pbt.pay_channel,
        'amount', pbt.amount,
        'currency', pbt.currency,
        'status', pbt.status,
        'payment_reference', pbt.payment_reference,
        'payee_name', pbt.payee_name,
        'sort_code', pbt.sort_code,
        'account_number', pbt.account_number,
        'account_type', pbt.account_type,
        'rail_provider', pbt.rail_provider,
        'rail_env', pbt.rail_env,
        'request_id', pbt.request_id,
        'rail_tx_id', pbt.rail_tx_id,
        'rail_state', pbt.rail_state,
        'rail_meta_json', pbt.rail_meta_json,
        'bank_details_hash_snapshot', pbt.bank_details_hash_snapshot,
        'payee_entity_kind', pbt.payee_entity_kind,
        'payee_entity_id', case when pbt.payee_entity_id is null then null else pbt.payee_entity_id::text end,
        'transfer_group_key', pbt.transfer_group_key,
        'grouping_mode_used', pbt.grouping_mode_used,
        'week_ending_bucket', case when pbt.week_ending_bucket is null then null else pbt.week_ending_bucket::text end
      )
      order by pbt.pay_channel, pbt.week_ending_bucket nulls last, pbt.amount desc, pbt.id
    ),
    '[]'::jsonb
  )
  into v_transfers
  from public.pay_bank_transfers pbt
  where pbt.pay_batch_id = p_pay_batch_id
    and (v_scope = 'ALL' or pbt.pay_channel = v_scope);

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'status', (select pb4.status from public.pay_batches pb4 where pb4.id = p_pay_batch_id),
    'rail_provider_snapshot', v_provider,
    'rail_env_snapshot', v_env,
    'banking_system_snapshot', v_batch.banking_system_snapshot,
    'external_paye_system_snapshot', v_batch.external_paye_system_snapshot,
    'bulk_ref_num', v_bulk_ref_num,
    'bulk_ref_date', case when v_bulk_ref_date is null then null else v_bulk_ref_date::text end,
    'bulk_reference', v_bulk_reference,
    'pending_count', v_pending_count,
    'blocked_count', v_blocked_count,
    'blocked', v_blocked_reasons,
    'transfers', v_transfers
  );
end;
$$;


create or replace function public.pay_batches_list(
  p_limit int default 50,
  p_offset int default 0,
  p_status text default null
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_limit int := greatest(1, least(coalesce(p_limit,50), 500));
  v_offset int := greatest(coalesce(p_offset,0), 0);
  v_status text := upper(nullif(btrim(coalesce(p_status,'')), ''));
  v_total int := 0;
  v_rows jsonb := '[]'::jsonb;
begin
  select count(*)::int
  into v_total
  from public.pay_batches pb
  where v_status is null or upper(coalesce(pb.status,'')) = v_status;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'id', pb.id::text,
        'pay_date', pb.pay_date::text,
        'created_at_utc', pb.created_at_utc,
        'created_by_user_id', case when pb.created_by_user_id is null then null else pb.created_by_user_id::text end,
        'status', pb.status,
        'banking_system_snapshot', pb.banking_system_snapshot,
        'external_paye_system_snapshot', pb.external_paye_system_snapshot,

        -- Rail-generic scheduling/execution fields
        'rail_provider_snapshot', pb.rail_provider_snapshot,
        'rail_env_snapshot', pb.rail_env_snapshot,
        'schedule_kind', pb.schedule_kind,
        'scheduled_at_utc', pb.scheduled_at_utc,
        'executing_started_at_utc', pb.executing_started_at_utc,
        'last_status_checked_at_utc', pb.last_status_checked_at_utc,

        -- ✅ NEW: funding account reference used for this batch (UI audit/history)
        'funding_account_ref', pb.funding_account_ref,

        -- Neutral (rail-generic) manual confirm aliases (keep legacy keys too)
        'manual_confirmed_at_utc', pb.monzo_confirmed_at_utc,
        'manual_confirmed_by_user_id', case when pb.monzo_confirmed_by_user_id is null then null else pb.monzo_confirmed_by_user_id::text end,

        'monzo_confirmed_at_utc', pb.monzo_confirmed_at_utc,
        'monzo_confirmed_by_user_id', case when pb.monzo_confirmed_by_user_id is null then null else pb.monzo_confirmed_by_user_id::text end,

        'total_bank_out', pb.total_bank_out,
        'total_debt_created', pb.total_debt_created,

        -- Bulk payment reference fields
        'bulk_ref_num', pb.bulk_ref_num,
        'bulk_ref_date', case when pb.bulk_ref_date is null then null else pb.bulk_ref_date::text end,
        'bulk_reference', pb.bulk_reference,

        -- ✅ NEW: lightweight authorisation summary for list visibility
        'auth_required_quantity', pb.auth_required_quantity,
        'auth_approved_count', pb.auth_approved_count,
        'auth_label', pb.auth_label,
        'auth_state', pb.auth_state
      )
      order by pb.created_at_utc desc, pb.id desc
    ),
    '[]'::jsonb
  )
  into v_rows
  from (
    select
      pb0.*,
      pbar.required_quantity as auth_required_quantity,
      pbar.state as auth_state,
      case
        when pbar.id is null then null
        else pbaa.approved_count
      end as auth_approved_count,
      case
        when pbar.id is null then null
        else (coalesce(pbaa.approved_count, 0)::text || '/' || pbar.required_quantity::text)
      end as auth_label
    from public.pay_batches pb0
    left join lateral (
      select
        pbar0.id,
        pbar0.state,
        pbar0.required_quantity
      from public.pay_batch_auth_requests pbar0
      where pbar0.pay_batch_id = pb0.id
        and pbar0.state in ('AWAITING','AUTHORISED')
      order by pbar0.created_at_utc desc, pbar0.id desc
      limit 1
    ) pbar on true
    left join lateral (
      select
        count(*)::int as approved_count
      from public.pay_batch_auth_actions pbaa0
      where pbar.id is not null
        and pbaa0.auth_request_id = pbar.id
        and pbaa0.action in ('AUTHORISE','USE_GOLDEN_KEY')
    ) pbaa on true
    where v_status is null or upper(coalesce(pb0.status,'')) = v_status
    order by pb0.created_at_utc desc, pb0.id desc
    limit v_limit offset v_offset
  ) pb;

  return jsonb_build_object(
    'ok', true,
    'total_count', v_total,
    'limit', v_limit,
    'offset', v_offset,
    'rows', v_rows
  );
end;
$$;
