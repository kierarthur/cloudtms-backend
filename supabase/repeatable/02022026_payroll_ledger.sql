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

  -- ✅ Updated: allow deleting/cancelling DRAFT and other pre-execution / pre-final statuses
  if upper(coalesce(v_batch.status,'')) not in (
    'DRAFT',
    'DRAFT_CREATED',
    'READY',
    'WAITING_BANK_CONFIRM',
    'PARTIAL',
    'SCHEDULED',
    'AWAITING_AUTHORISATION',
    'AUTHORISED_FOR_PAYMENT'
  ) then
    raise exception 'pay_batch_cancel: batch status must be DRAFT, DRAFT_CREATED, READY, WAITING_BANK_CONFIRM, PARTIAL, SCHEDULED, AWAITING_AUTHORISATION or AUTHORISED_FOR_PAYMENT (current=%)', v_batch.status;
  end if;

  update public.pay_batch_items pbi
  set pay_bank_transfer_id = null
  from public.pay_batch_candidates pbc
  where pbc.id = pbi.pay_batch_candidate_id
    and pbc.pay_batch_id = p_pay_batch_id
    and pbi.pay_bank_transfer_id is not null;

  delete from public.pay_bank_transfers pbt
  where pbt.pay_batch_id = p_pay_batch_id;

  -- If auth tables exist, cancel any active auth request and invalidate tokens.
  if to_regclass('public.pay_batch_auth_requests') is not null then
    execute
      'update public.pay_batch_auth_requests pbar
       set state = ''CANCELLED''
       where pbar.pay_batch_id = $1
         and pbar.state = ''AWAITING'''
      using p_pay_batch_id;

    if to_regclass('public.pay_batch_auth_tokens') is not null then
      execute
        'update public.pay_batch_auth_tokens pbat
         set used_at_utc = now()
         where pbat.used_at_utc is null
           and pbat.auth_request_id in (
             select pbar2.id
             from public.pay_batch_auth_requests pbar2
             where pbar2.pay_batch_id = $1
               and pbar2.state in (''AWAITING'',''CANCELLED'')
           )'
        using p_pay_batch_id;
    end if;
  end if;

  update public.pay_batches pb
  set
    status = 'CANCELLED',
    cancelled_at_utc = now(),
    cancelled_by_user_id = p_actor_user_id,
    cancel_reason = p_reason,
    schedule_kind = null,
    scheduled_at_utc = null,
    scheduled_by_user_id = null,
    funding_account_ref = null,
    funds_warning_hours_json = null
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

        -- ✅ NEW: batch kind marker (PAYE / UMBRELLA / MIXED / null)
        'batch_kind', pb.batch_kind,
        'pay_channels_present', pb.pay_channels_present,

        -- Rail-generic scheduling/execution fields
        'rail_provider_snapshot', pb.rail_provider_snapshot,
        'rail_env_snapshot', pb.rail_env_snapshot,
        'schedule_kind', pb.schedule_kind,
        'scheduled_at_utc', pb.scheduled_at_utc,
        'executing_started_at_utc', pb.executing_started_at_utc,
        'last_status_checked_at_utc', pb.last_status_checked_at_utc,

        -- funding account reference used for this batch (UI audit/history)
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

        -- lightweight authorisation summary for list visibility
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

      -- ✅ NEW: channels present + derived batch kind
      case
        when ch.channels is null then null
        when array_position(ch.channels,'PAYE') is not null and array_position(ch.channels,'UMBRELLA') is not null then 'MIXED'
        when array_position(ch.channels,'PAYE') is not null then 'PAYE'
        when array_position(ch.channels,'UMBRELLA') is not null then 'UMBRELLA'
        else null
      end as batch_kind,
      coalesce(to_jsonb(ch.channels), '[]'::jsonb) as pay_channels_present,

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
        array_agg(distinct upper(coalesce(pbi.pay_channel,'')) order by upper(coalesce(pbi.pay_channel,'')))
          filter (where upper(coalesce(pbi.pay_channel,'')) in ('PAYE','UMBRELLA')) as channels
      from public.pay_batch_items pbi
      join public.pay_batch_candidates pbc
        on pbc.id = pbi.pay_batch_candidate_id
      where pbc.pay_batch_id = pb0.id
        and pbi.item_type <> 'DEBT_CREATED'
    ) ch on true
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

  v_matched jsonb := '[]'::jsonb;
  v_unknown jsonb := '[]'::jsonb;
  v_ambig jsonb := '[]'::jsonb;
  v_missing_net jsonb := '[]'::jsonb;

  v_applied_count int := 0;
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

  truncate table _tmp_sage_net;

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

    if net_raw is null or btrim(net_raw) = '' then
      insert into _tmp_sage_net(works_number, works_norm, net_amount)
      values (works, works_norm, null);
      continue;
    end if;

    -- Normalize numeric: remove commas and currency symbols
    net_raw := replace(net_raw, ',', '');
    net_raw := regexp_replace(net_raw, '[^0-9\\.-]', '', 'g');

    if net_raw is null or btrim(net_raw) = '' then
      insert into _tmp_sage_net(works_number, works_norm, net_amount)
      values (works, works_norm, null);
      continue;
    end if;

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
    select sn.works_norm
    from _tmp_sage_net sn
    group by sn.works_norm
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

  truncate table _tmp_batch_paye;

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
  into v_ambig
  from (
    select
      sn.works_norm,
      jsonb_agg(distinct bp.candidate_id::text order by bp.candidate_id::text) as candidate_ids
    from _tmp_sage_net sn
    join _tmp_batch_paye bp
      on (bp.tms_ref_norm is not null and bp.tms_ref_norm <> '' and bp.tms_ref_norm = sn.works_norm)
      or (bp.ni_norm is not null and bp.ni_norm <> '' and bp.ni_norm = sn.works_norm)
    group by sn.works_norm
    having count(distinct bp.candidate_id) > 1
  ) a;

  -- Unknown works numbers (no match by TMS ref or NI)
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'works_number', sn.works_number,
        'works_norm', sn.works_norm
      )
    ),
    '[]'::jsonb
  )
  into v_unknown
  from _tmp_sage_net sn
  left join _tmp_batch_paye bp
    on (bp.tms_ref_norm is not null and bp.tms_ref_norm <> '' and bp.tms_ref_norm = sn.works_norm)
    or (bp.ni_norm is not null and bp.ni_norm <> '' and bp.ni_norm = sn.works_norm)
  where bp.candidate_id is null;

  -- Missing net rows (blank net in CSV) for matched-unambiguous works numbers
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'works_number', sn.works_number,
        'works_norm', sn.works_norm,
        'candidate_id', bp.candidate_id::text,
        'pay_batch_candidate_id', bp.pay_batch_candidate_id::text
      )
    ),
    '[]'::jsonb
  )
  into v_missing_net
  from _tmp_sage_net sn
  join _tmp_batch_paye bp
    on (
      (bp.tms_ref_norm is not null and bp.tms_ref_norm <> '' and bp.tms_ref_norm = sn.works_norm)
      or (bp.ni_norm is not null and bp.ni_norm <> '' and bp.ni_norm = sn.works_norm)
    )
  where sn.net_amount is null
    and not exists (
      select 1
      from jsonb_array_elements(coalesce(v_ambig,'[]'::jsonb)) a
      where (a->>'works_number') = sn.works_norm
      limit 1
    );

  -- Matched rows (unambiguous, known, net_amount present)
  create temp table if not exists _tmp_sage_match (
    pay_batch_candidate_id uuid,
    net_amount numeric
  ) on commit drop;

  truncate table _tmp_sage_match;

  insert into _tmp_sage_match(pay_batch_candidate_id, net_amount)
  select
    bp.pay_batch_candidate_id,
    sn.net_amount
  from _tmp_sage_net sn
  join _tmp_batch_paye bp
    on (
      (bp.tms_ref_norm is not null and bp.tms_ref_norm <> '' and bp.tms_ref_norm = sn.works_norm)
      or (bp.ni_norm is not null and bp.ni_norm <> '' and bp.ni_norm = sn.works_norm)
    )
  where sn.net_amount is not null
    and not exists (
      select 1
      from jsonb_array_elements(coalesce(v_ambig,'[]'::jsonb)) a
      where (a->>'works_number') = sn.works_norm
      limit 1
    )
    and not exists (
      select 1
      from jsonb_array_elements(coalesce(v_unknown,'[]'::jsonb)) u
      where (u->>'works_norm') = sn.works_norm
      limit 1
    );

  select count(*)::int
  into v_applied_count
  from _tmp_sage_match sm;

  -- PATCH semantics for SAGE_IMPORT: replace prior SAGE_IMPORT rows only for matched candidates
  delete from public.pay_batch_paye_net_inputs pni
  using _tmp_sage_match sm
  where pni.pay_batch_candidate_id = sm.pay_batch_candidate_id
    and pni.source = 'SAGE_IMPORT';

  insert into public.pay_batch_paye_net_inputs(
    pay_batch_candidate_id, source, net_amount, imported_at_utc, file_name, file_hash
  )
  select
    sm.pay_batch_candidate_id,
    'SAGE_IMPORT',
    sm.net_amount,
    now(),
    p_source_filename,
    null
  from _tmp_sage_match sm;

  -- Recompute PAYE state: READY iff any net input exists (SAGE_IMPORT or MANUAL_ENTRY), else PENDING_NET
  update public.pay_batch_candidates pbc
  set paye_state = case
    when exists (
      select 1
      from public.pay_batch_paye_net_inputs pni
      where pni.pay_batch_candidate_id = pbc.id
      limit 1
    ) then 'READY'
    else 'PENDING_NET'
  end
  where pbc.pay_batch_id = p_pay_batch_id
    and pbc.paye_state is not null;

  -- Build matched list for response (per-candidate)
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'pay_batch_candidate_id', bp.pay_batch_candidate_id::text,
        'candidate_id', bp.candidate_id::text,
        'net_amount', sm.net_amount
      )
      order by bp.candidate_id::text
    ),
    '[]'::jsonb
  )
  into v_matched
  from _tmp_sage_match sm
  join _tmp_batch_paye bp
    on bp.pay_batch_candidate_id = sm.pay_batch_candidate_id;

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'source', 'SAGE_IMPORT',
    'applied_count', v_applied_count,
    'matched', v_matched,
    'missing_net', v_missing_net,
    'unknown', v_unknown,
    'ambiguous', v_ambig
  );
end;
$$;


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

  v_dup jsonb := '[]'::jsonb;
  v_ambig jsonb := '[]'::jsonb;
  v_unmatched jsonb := '[]'::jsonb;

  v_updated_count int := 0;
  v_cleared_count int := 0;
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
    pay_batch_candidate_id uuid,
    candidate_id uuid,
    works_raw text,
    works_norm text,
    net_amount numeric
  ) on commit drop;

  truncate table _tmp_manual_net;

  insert into _tmp_manual_net(pay_batch_candidate_id, candidate_id, works_raw, works_norm, net_amount)
  select
    nullif(e->>'pay_batch_candidate_id','')::uuid as pay_batch_candidate_id,
    nullif(e->>'candidate_id','')::uuid as candidate_id,
    nullif(btrim(e->>'tms_ref'), '') as works_raw,
    case
      when nullif(btrim(e->>'tms_ref'), '') is null then null
      else upper(regexp_replace(btrim(e->>'tms_ref'), '\s+', '', 'g'))
    end as works_norm,
    case
      when nullif(btrim(coalesce(e->>'net_amount','')), '') is null then null
      else round((nullif(btrim(coalesce(e->>'net_amount','')), '')::numeric), 2)
    end as net_amount
  from jsonb_array_elements(v_entries) e
  where e is not null and jsonb_typeof(e)='object';

  -- Hard validation: each entry must include pay_batch_candidate_id OR candidate_id OR works_norm
  if exists (
    select 1
    from _tmp_manual_net mn
    where mn.pay_batch_candidate_id is null
      and mn.candidate_id is null
      and (mn.works_norm is null or mn.works_norm = '')
    limit 1
  ) then
    raise exception 'MANUAL_NET_INVALID: each entry must include pay_batch_candidate_id, candidate_id or tms_ref/NI (Works Number)';
  end if;

  -- Validate numeric + non-negative (NULL is allowed = clear)
  if exists (
    select 1
    from _tmp_manual_net t
    where t.net_amount is not null
      and t.net_amount < 0
    limit 1
  ) then
    raise exception 'MANUAL_NET_INVALID: net_amount must be non-negative';
  end if;

  -- Validate duplicates (by explicit key if present; else candidate_id; else works_norm)
  select coalesce(jsonb_agg(x), '[]'::jsonb)
  into v_dup
  from (
    select to_jsonb(
      case
        when mn.pay_batch_candidate_id is not null then ('PBC:' || mn.pay_batch_candidate_id::text)
        when mn.candidate_id is not null then ('CAND:' || mn.candidate_id::text)
        else ('WORKS:' || mn.works_norm)
      end
    ) as x
    from _tmp_manual_net mn
    group by
      case
        when mn.pay_batch_candidate_id is not null then ('PBC:' || mn.pay_batch_candidate_id::text)
        when mn.candidate_id is not null then ('CAND:' || mn.candidate_id::text)
        else ('WORKS:' || mn.works_norm)
      end
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

  truncate table _tmp_batch_paye2;

  insert into _tmp_batch_paye2(pay_batch_candidate_id, candidate_id, tms_ref_norm, ni_norm)
  select
    pbc.id,
    pbc.candidate_id,
    upper(regexp_replace(btrim(coalesce(pbc.candidate_tms_ref, c.tms_ref, '')), '\s+', '', 'g')) as tms_ref_norm,
    upper(regexp_replace(btrim(coalesce(c.ni_number, '')), '\s+', '', 'g')) as ni_norm
  from public.pay_batch_candidates pbc
  join public.candidates c
    on c.id = pbc.candidate_id
  where pbc.pay_batch_id = p_pay_batch_id
    and pbc.paye_state is not null;

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
      jsonb_agg(distinct bp.candidate_id::text order by bp.candidate_id::text) as candidate_ids
    from _tmp_manual_net mn
    join _tmp_batch_paye2 bp
      on mn.pay_batch_candidate_id is null
     and mn.candidate_id is null
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

  -- Validate that each entry matches a batch PAYE candidate (unmatched => error)
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'pay_batch_candidate_id', case when mn.pay_batch_candidate_id is null then null else mn.pay_batch_candidate_id::text end,
        'candidate_id', case when mn.candidate_id is null then null else mn.candidate_id::text end,
        'works_norm', mn.works_norm
      )
    ),
    '[]'::jsonb
  )
  into v_unmatched
  from _tmp_manual_net mn
  left join _tmp_batch_paye2 bp
    on (mn.pay_batch_candidate_id is not null and bp.pay_batch_candidate_id = mn.pay_batch_candidate_id)
    or (mn.pay_batch_candidate_id is null and mn.candidate_id is not null and bp.candidate_id = mn.candidate_id)
    or (
      mn.pay_batch_candidate_id is null
      and mn.candidate_id is null
      and mn.works_norm is not null
      and mn.works_norm <> ''
      and (
        (bp.tms_ref_norm is not null and bp.tms_ref_norm <> '' and bp.tms_ref_norm = mn.works_norm)
        or (bp.ni_norm is not null and bp.ni_norm <> '' and bp.ni_norm = mn.works_norm)
      )
    )
  where bp.pay_batch_candidate_id is null;

  if jsonb_array_length(v_unmatched) > 0 then
    raise exception 'MANUAL_NET_INVALID: entry candidate not found in pay batch PAYE candidates %', v_unmatched::text;
  end if;

  create temp table if not exists _tmp_manual_match (
    pay_batch_candidate_id uuid,
    net_amount numeric
  ) on commit drop;

  truncate table _tmp_manual_match;

  -- pay_batch_candidate_id path (preferred)
  insert into _tmp_manual_match(pay_batch_candidate_id, net_amount)
  select
    bp.pay_batch_candidate_id,
    mn.net_amount
  from _tmp_manual_net mn
  join _tmp_batch_paye2 bp
    on mn.pay_batch_candidate_id is not null
   and bp.pay_batch_candidate_id = mn.pay_batch_candidate_id;

  -- candidate_id path
  insert into _tmp_manual_match(pay_batch_candidate_id, net_amount)
  select
    bp.pay_batch_candidate_id,
    mn.net_amount
  from _tmp_manual_net mn
  join _tmp_batch_paye2 bp
    on mn.pay_batch_candidate_id is null
   and mn.candidate_id is not null
   and bp.candidate_id = mn.candidate_id;

  -- works_norm path (tms_ref OR NI)
  insert into _tmp_manual_match(pay_batch_candidate_id, net_amount)
  select
    bp.pay_batch_candidate_id,
    mn.net_amount
  from _tmp_manual_net mn
  join _tmp_batch_paye2 bp
    on mn.pay_batch_candidate_id is null
   and mn.candidate_id is null
   and mn.works_norm is not null
   and mn.works_norm <> ''
   and (
     (bp.tms_ref_norm is not null and bp.tms_ref_norm <> '' and bp.tms_ref_norm = mn.works_norm)
     or (bp.ni_norm is not null and bp.ni_norm <> '' and bp.ni_norm = mn.works_norm)
   );

  -- Deduplicate any accidental multiple matches (defensive; should not occur due to earlier checks)
  if exists (
    select 1
    from _tmp_manual_match mm
    group by mm.pay_batch_candidate_id
    having count(*) > 1
    limit 1
  ) then
    raise exception 'MANUAL_NET_INVALID: duplicate resolved candidates in request (internal match collision)';
  end if;

  select count(*)::int
  into v_updated_count
  from _tmp_manual_match mm
  where mm.net_amount is not null;

  select count(*)::int
  into v_cleared_count
  from _tmp_manual_match mm
  where mm.net_amount is null;

  -- PATCH semantics:
  -- For referenced candidates only:
  --  - clear prior MANUAL_ENTRY rows
  --  - insert MANUAL_ENTRY row only when net_amount is non-null
  delete from public.pay_batch_paye_net_inputs pni
  using _tmp_manual_match mm
  where pni.pay_batch_candidate_id = mm.pay_batch_candidate_id
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
  from _tmp_manual_match mm
  where mm.net_amount is not null;

  -- Recompute PAYE state: READY iff any net input exists (SAGE_IMPORT or MANUAL_ENTRY), else PENDING_NET
  update public.pay_batch_candidates pbc
  set paye_state = case
    when exists (
      select 1
      from public.pay_batch_paye_net_inputs pni
      where pni.pay_batch_candidate_id = pbc.id
      limit 1
    ) then 'READY'
    else 'PENDING_NET'
  end
  where pbc.pay_batch_id = p_pay_batch_id
    and pbc.paye_state is not null;

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'source', 'MANUAL_ENTRY',
    'updated_count', v_updated_count,
    'cleared_count', v_cleared_count,
    'unmatched', '[]'::jsonb
  );
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


CREATE OR REPLACE FUNCTION public.pay_preview(
  p_pay_date date,
  p_week_ending_cutoff date,
  p_actor_user_id uuid,
  p_candidate_id uuid DEFAULT NULL::uuid,
  p_client_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
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

  -- ✅ Derived readiness semantics (match pay_batch_prepare)
  v_need_name_check boolean := false;
  v_requires_payee_map boolean := false;

  v_paye jsonb := '[]'::jsonb;
  v_nonpaye jsonb := '[]'::jsonb;

  v_blocked jsonb := '[]'::jsonb;
  v_do_not_pay jsonb := '[]'::jsonb;
  v_snoozed jsonb := '[]'::jsonb;

  -- ✅ NEW: payees section (ready/blocked status per payee)
  v_payees jsonb := '[]'::jsonb;

  -- ✅ NEW: summary section (readiness + candidate counts)
  v_summary jsonb := '{}'::jsonb;
begin
  if p_pay_date is null then
    raise exception 'pay_date is required';
  end if;

  if p_week_ending_cutoff is null then
    raise exception 'week_ending_cutoff is required';
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

  -- ✅ Derived semantics (match pay_batch_prepare):
  -- - name-check only when the rail supports it (and never for manual CSV)
  -- - payee-map required for API rails (false for manual CSV)
  v_need_name_check := (coalesce(v_rail_supports_name_check,false) = true)
                       and (upper(btrim(coalesce(v_rail_provider_default,''))) <> 'CSV');

  v_requires_payee_map := (upper(btrim(coalesce(v_rail_provider_default,''))) <> 'CSV');

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
  reserved_batch_items as (
    -- Items are considered "drafted/reserved" if they belong to a batch that is not CANCELLED/SETTLED.
    -- We intentionally DO NOT exclude SETTLED batches so new deltas on the same segment/source_ref can appear later.
    select distinct
      pbi.timesheet_id,
      pbi.segment_key,
      pbi.source_ref
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc_r
      on pbc_r.id = pbi.pay_batch_candidate_id
    join public.pay_batches pb_r
      on pb_r.id = pbc_r.pay_batch_id
    where pbi.timesheet_id is not null
      and pbi.pay_channel in ('PAYE','UMBRELLA')
      and pbi.item_type <> 'DEBT_CREATED'
      and upper(coalesce(pb_r.status,'')) in (
        'DRAFT',
        'DRAFT_CREATED',
        'READY',
        'WAITING_BANK_CONFIRM',
        'PARTIAL',
        'FAILED',
        'BLOCKED_FUNDS',
        'SCHEDULED',
        'EXECUTING',
        'AWAITING_AUTHORISATION',
        'AUTHORISED_FOR_PAYMENT'
      )
  ),
  eligible_tsfin as (
    select
      tf.timesheet_id,
      tf.id as tsfin_id,
      tf.candidate_id,
      tf.client_id,
      ts.week_ending_date as ts_week_ending_date,
      cl.name as ts_client_name,
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
    join public.clients cl
      on cl.id = tf.client_id
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
      and upper(coalesce(tf.processing_status::text,'')) not in ('UNASSIGNED','CLIENT_UNRESOLVED','RATE_MISSING','PAY_CHANNEL_MISSING')
      and upper(coalesce(c.pay_method,'')) in ('PAYE','UMBRELLA')

      -- ✅ Option A: eligibility window is controlled by Settings (relative to UK “today”),
      -- NOT scoped by pay_date week.
      and ts.week_ending_date::date >= v_eligibility_from_date
      and ts.week_ending_date::date <= v_eligibility_to_date

      -- ✅ New: week-ending cutoff (user-controlled)
      and ts.week_ending_date::date <= p_week_ending_cutoff

      -- ✅ Optional filters (default ALL/ALL when NULL)
      and (p_candidate_id is null or tf.candidate_id = p_candidate_id)
      and (p_client_id is null or tf.client_id = p_client_id)
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
      e.ts_week_ending_date,
      e.ts_client_name,
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
      t.client_id,
      t.ts_week_ending_date,
      t.ts_client_name,
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

      exists (
        select 1
        from reserved_batch_items rbi
        where rbi.timesheet_id = b.timesheet_id
          and (
            (rbi.segment_key is not null and rbi.segment_key = ids.segment_id)
            or (rbi.source_ref is not null and rbi.source_ref = ('seg:' || ids.segment_id))
          )
        limit 1
      ) as is_reserved,

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

      (coalesce(cur.exclude_from_pay,false) = true and not exists (
        select 1
        from reserved_batch_items rbi2
        where rbi2.timesheet_id = b.timesheet_id
          and (
            (rbi2.segment_key is not null and rbi2.segment_key = ids.segment_id)
            or (rbi2.source_ref is not null and rbi2.source_ref = ('seg:' || ids.segment_id))
          )
        limit 1
      )) as is_do_not_pay,

      (b.require_reference_to_pay = true and coalesce(cur.exclude_from_pay,false) = false and nullif(btrim(coalesce(cur.ref_num,'')),'') is null and not exists (
        select 1
        from reserved_batch_items rbi3
        where rbi3.timesheet_id = b.timesheet_id
          and (
            (rbi3.segment_key is not null and rbi3.segment_key = ids.segment_id)
            or (rbi3.source_ref is not null and rbi3.source_ref = ('seg:' || ids.segment_id))
          )
        limit 1
      )) as is_ref_missing,

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
        and not exists (
          select 1
          from reserved_batch_items rbi4
          where rbi4.timesheet_id = b.timesheet_id
            and (
              (rbi4.segment_key is not null and rbi4.segment_key = ids.segment_id)
              or (rbi4.source_ref is not null and rbi4.source_ref = ('seg:' || ids.segment_id))
            )
          limit 1
        )
      ) as is_blocked,

      case
        when exists (
          select 1
          from reserved_batch_items rbi5
          where rbi5.timesheet_id = b.timesheet_id
            and (
              (rbi5.segment_key is not null and rbi5.segment_key = ids.segment_id)
              or (rbi5.source_ref is not null and rbi5.source_ref = ('seg:' || ids.segment_id))
            )
          limit 1
        )
        then 0::numeric
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
      b.client_id,
      b.ts_week_ending_date,
      b.ts_client_name,
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

      case
        when exists (
          select 1
          from reserved_batch_items rbi
          where rbi.timesheet_id = b.timesheet_id
            and rbi.source_ref = 'additional'
          limit 1
        )
        then 0::numeric
        else round(
          b.current_additional_pay_ex_vat
          -
          coalesce(nullif(b.base_json->>'additional_pay_ex_vat','')::numeric,0),
          2
        )
      end as delta_additional_pay_ex_vat,

      case
        when exists (
          select 1
          from reserved_batch_items rbi
          where rbi.timesheet_id = b.timesheet_id
            and rbi.source_ref = 'expenses'
          limit 1
        )
        then 0::numeric
        else round(b.current_expenses_pay_ex_vat - coalesce(nullif(b.base_json #>> '{expenses,expenses_pay_ex_vat}','')::numeric,0),2)
      end as delta_expenses_pay_ex_vat,

      case
        when exists (
          select 1
          from reserved_batch_items rbi
          where rbi.timesheet_id = b.timesheet_id
            and rbi.source_ref = 'travel'
          limit 1
        )
        then 0::numeric
        else round(b.current_travel_pay_ex_vat   - coalesce(nullif(b.base_json #>> '{expenses,travel_pay_ex_vat}','')::numeric,0),2)
      end as delta_travel_pay_ex_vat,

      case
        when exists (
          select 1
          from reserved_batch_items rbi
          where rbi.timesheet_id = b.timesheet_id
            and rbi.source_ref = 'accommodation'
          limit 1
        )
        then 0::numeric
        else round(b.current_accommodation_pay_ex_vat - coalesce(nullif(b.base_json #>> '{expenses,accommodation_pay_ex_vat}','')::numeric,0),2)
      end as delta_accommodation_pay_ex_vat,

      case
        when exists (
          select 1
          from reserved_batch_items rbi
          where rbi.timesheet_id = b.timesheet_id
            and rbi.source_ref = 'other'
          limit 1
        )
        then 0::numeric
        else round(b.current_other_pay_ex_vat    - coalesce(nullif(b.base_json #>> '{expenses,other_pay_ex_vat}','')::numeric,0),2)
      end as delta_other_pay_ex_vat,

      case
        when exists (
          select 1
          from reserved_batch_items rbi
          where rbi.timesheet_id = b.timesheet_id
            and rbi.source_ref = 'mileage'
          limit 1
        )
        then 0::numeric
        else round(b.current_mileage_pay_ex_vat  - coalesce(nullif(b.base_json #>> '{expenses,mileage_pay_ex_vat}','')::numeric,0),2)
      end as delta_mileage_pay_ex_vat,

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
          where not exists (
            select 1
            from reserved_batch_items rbi
            where rbi.timesheet_id = b.timesheet_id
              and rbi.source_ref = ('adj:' || i.adj_id)
            limit 1
          )
        ) q
        where coalesce(nullif(q.d->>'delta_pay_ex_vat','')::numeric,0) <> 0
      ) as adjustment_deltas_json
    from ts_baseline b
  ),
  ts_itemised as (
    select
      d2.*,
      d2.total_ex as payment_amount_ex_vat,
      case
        when d2.ts_pay_method = 'UMBRELLA' then (public._pay_umbrella_vat_calc(d2.total_ex, v_vat_rate_pct, d2.umb_vat_chargeable)->>'inc')::numeric
        else d2.total_ex
      end as payment_amount_inc_vat,
      case
        when d2.ts_pay_method = 'UMBRELLA' then (public._pay_umbrella_vat_calc(d2.total_ex, v_vat_rate_pct, d2.umb_vat_chargeable)->>'inc')::numeric
        else d2.total_ex
      end as payment_amount
    from (
      select
        d1.*,
        round(
          coalesce((select sum(coalesce(nullif(x->>'delta_pay_ex_vat','')::numeric,0)) from jsonb_array_elements(d1.segment_deltas_json) x),0)
          + coalesce(d1.delta_additional_pay_ex_vat,0)
          + coalesce(d1.delta_expenses_pay_ex_vat,0)
          + coalesce(d1.delta_travel_pay_ex_vat,0)
          + coalesce(d1.delta_accommodation_pay_ex_vat,0)
          + coalesce(d1.delta_other_pay_ex_vat,0)
          + coalesce(d1.delta_mileage_pay_ex_vat,0)
          + coalesce((select sum(coalesce(nullif(x->>'delta_pay_ex_vat','')::numeric,0)) from jsonb_array_elements(d1.adjustment_deltas_json) x),0),
          2
        ) as total_ex
      from ts_deltas d1
    ) d2
  ),
  candidate_rollup as (
    select
      d.candidate_id,
      max(d.cand_tms_ref) as cand_tms_ref,
      max(d.cand_display_name) as cand_display_name,
      max(d.cand_pay_method) as cand_pay_method,
      max(d.cand_umbrella_id::text)::uuid as cand_umbrella_id,
      bool_or(d.umb_enabled) as umb_enabled,
      bool_or(d.umb_vat_chargeable) as umb_vat_chargeable,

      -- ✅ bank readiness rollups
      bool_or(d.cand_bank_hash is not null and btrim(d.cand_bank_hash) <> '') as candidate_has_bank_details,
      max(d.cand_bank_hash) as candidate_bank_hash,
      bool_or(d.umb_bank_hash is not null and btrim(d.umb_bank_hash) <> '') as umbrella_has_bank_details,
      max(d.umb_bank_hash) as umbrella_bank_hash,

      bool_or(d.ts_pay_method <> d.cand_pay_method
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
            'week_ending_date', case when d.ts_week_ending_date is null then null else d.ts_week_ending_date::text end,
            'client_id', case when d.client_id is null then null else d.client_id::text end,
            'client_name', d.ts_client_name,
            'payment_amount_ex_vat', d.payment_amount_ex_vat,
            'payment_amount_inc_vat', d.payment_amount_inc_vat,
            'payment_amount', d.payment_amount,
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
    from ts_itemised d
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
  ),

  -- ✅ NEW: Payees section (candidate + umbrella) + readiness derived from bank_name_checks + bank_payee_map
  payees_src as (
    select
      'CANDIDATE'::text as payee_entity_kind,
      ce.candidate_id as payee_entity_id,
      nullif(btrim(coalesce(ce.candidate_bank_hash,'')), '') as bank_details_hash
    from cand_enriched ce
    union all
    select
      'UMBRELLA'::text as payee_entity_kind,
      ce.cand_umbrella_id as payee_entity_id,
      nullif(btrim(coalesce(ce.umbrella_bank_hash,'')), '') as bank_details_hash
    from cand_enriched ce
    where ce.cand_umbrella_id is not null
  ),
  payees as (
    select
      upper(btrim(coalesce(ps.payee_entity_kind,''))) as payee_entity_kind,
      ps.payee_entity_id as payee_entity_id,
      ps.bank_details_hash as bank_details_hash
    from payees_src ps
    where ps.payee_entity_id is not null
    group by 1,2,3
  ),
  payees_enriched as (
    select
      p.payee_entity_kind,
      p.payee_entity_id,
      p.bank_details_hash,

      b.payee_name,
      b.account_holder,
      b.bank_name,
      b.sort_code,
      b.account_number,
      b.account_type,

      coalesce(bnc.status, 'UNVERIFIED') as name_check_status,
      bnc.checked_at_utc as name_check_checked_at_utc,
      bnc.override_reason as name_check_override_reason,
      bnc.override_by_user_id as name_check_override_by_user_id,
      bnc.override_at_utc as name_check_override_at_utc,
      bnc.override_hash as name_check_override_hash,
      (bnc.override_reason is not null and bnc.override_hash = p.bank_details_hash) as name_check_has_override,

      bpm.payee_id as payee_map_payee_id,
      bpm.payee_account_id as payee_map_payee_account_id,
      bpm.meta_json as payee_map_meta_json,
      (bpm.payee_id is not null) as payee_map_present,

      b.is_missing_bank_details as is_missing_bank_details,

      (
        v_need_name_check = true
        and b.is_missing_bank_details = false
        and coalesce(bnc.status, 'UNVERIFIED') <> 'PASS'
        and not (bnc.override_reason is not null and bnc.override_hash = p.bank_details_hash)
      ) as is_name_check_blocked,

      (
        v_requires_payee_map = true
        and b.is_missing_bank_details = false
        and (bpm.payee_id is null)
      ) as is_payee_map_blocked
    from payees p
    left join public.candidates c_pay
      on p.payee_entity_kind = 'CANDIDATE'
     and c_pay.id = p.payee_entity_id
    left join public.umbrellas u_pay
      on p.payee_entity_kind = 'UMBRELLA'
     and u_pay.id = p.payee_entity_id

    -- ✅ FIX: CROSS JOIN LATERAL cannot have ON; use LEFT JOIN LATERAL ... ON true
    left join lateral (
      select
        case
          when p.payee_entity_kind = 'CANDIDATE' then c_pay.display_name
          else u_pay.name
        end as payee_name,
        case
          when p.payee_entity_kind = 'CANDIDATE' then c_pay.account_holder
          else u_pay.name
        end as account_holder,
        coalesce(c_pay.bank_name, u_pay.bank_name) as bank_name,
        coalesce(c_pay.sort_code, u_pay.sort_code) as sort_code,
        coalesce(c_pay.account_number, u_pay.account_number) as account_number,
        case when p.payee_entity_kind = 'CANDIDATE' then 'personal' else 'business' end as account_type,
        (
          p.bank_details_hash is null
          or btrim(p.bank_details_hash) = ''
          or nullif(btrim(coalesce(
                case
                  when p.payee_entity_kind = 'CANDIDATE' then c_pay.account_holder
                  else u_pay.name
                end,
                ''
              )), '') is null
          or nullif(btrim(coalesce(coalesce(c_pay.sort_code, u_pay.sort_code), '')), '') is null
          or nullif(btrim(coalesce(coalesce(c_pay.account_number, u_pay.account_number), '')), '') is null
        ) as is_missing_bank_details
    ) b on true

    left join public.bank_name_checks bnc
      on bnc.rail_provider = v_rail_provider_default
     and bnc.rail_env = v_rail_env_default
     and bnc.entity_kind = p.payee_entity_kind
     and bnc.entity_id = p.payee_entity_id
     and bnc.bank_details_hash is not distinct from p.bank_details_hash
    left join public.bank_payee_map bpm
      on bpm.rail_provider = v_rail_provider_default
     and bpm.rail_env = v_rail_env_default
     and bpm.entity_kind = p.payee_entity_kind
     and bpm.entity_id = p.payee_entity_id
     and bpm.bank_details_hash is not distinct from p.bank_details_hash
  ),
  payees_json as (
    select
      coalesce(
        jsonb_agg(
          jsonb_build_object(
            'payee_entity_kind', pe.payee_entity_kind,
            'payee_entity_id', pe.payee_entity_id::text,
            'bank_details_hash', pe.bank_details_hash,

            -- ✅ Bank fields required to call the rail
            'payee_name', pe.payee_name,
            'account_holder', pe.account_holder,
            'bank_name', pe.bank_name,
            'sort_code', pe.sort_code,
            'account_number', pe.account_number,
            'account_type', pe.account_type,

            -- ✅ Name-check details (status + timestamps + override metadata)
            'name_check', jsonb_build_object(
              'status', pe.name_check_status,
              'checked_at_utc', pe.name_check_checked_at_utc,
              'has_override', (pe.name_check_has_override = true),
              'override_reason', pe.name_check_override_reason,
              'override_by_user_id', case when pe.name_check_override_by_user_id is null then null else pe.name_check_override_by_user_id::text end,
              'override_at_utc', pe.name_check_override_at_utc,
              'override_hash', pe.name_check_override_hash
            ),

            -- ✅ Payee map details (presence + IDs)
            'payee_map', jsonb_build_object(
              'present', (pe.payee_map_present = true),
              'payee_id', pe.payee_map_payee_id,
              'payee_account_id', pe.payee_map_payee_account_id
            ),

            'blockers',
              (
                (case when pe.is_missing_bank_details then jsonb_build_array('BLOCKED_BANK_DETAILS') else '[]'::jsonb end)
                ||
                (case when pe.is_name_check_blocked then jsonb_build_array('BLOCKED_NAME_CHECK') else '[]'::jsonb end)
                ||
                (case when pe.is_payee_map_blocked then jsonb_build_array('BLOCKED_NO_PAYEE_MAP') else '[]'::jsonb end)
              )
          )
          order by pe.payee_entity_kind, pe.payee_entity_id::text
        ),
        '[]'::jsonb
      ) as payees
    from payees_enriched pe
  ),
  cand_payee0 as (
    select
      ce.*,
      case when ce.cand_pay_method = 'PAYE' then 'CANDIDATE' else 'UMBRELLA' end as payee_entity_kind,
      case when ce.cand_pay_method = 'PAYE' then ce.candidate_id else ce.cand_umbrella_id end as payee_entity_id,
      case when ce.cand_pay_method = 'PAYE' then nullif(btrim(coalesce(ce.candidate_bank_hash,'')), '') else nullif(btrim(coalesce(ce.umbrella_bank_hash,'')), '') end as payee_bank_hash
    from cand_enriched ce
  ),
  cand_payee as (
    select
      cp.*,

      coalesce(pe.name_check_status, 'UNVERIFIED') as payee_name_check_status,
      coalesce(pe.name_check_has_override, false) as payee_name_check_has_override,
      coalesce(pe.payee_map_present, false) as payee_map_present,

      (
        (case when (cp.payee_entity_id is null or cp.payee_bank_hash is null or btrim(cp.payee_bank_hash) = '') then jsonb_build_array('BLOCKED_BANK_DETAILS') else '[]'::jsonb end)
        ||
        (case
           when (cp.payee_entity_id is not null and cp.payee_bank_hash is not null and v_need_name_check = true and coalesce(pe.name_check_status,'UNVERIFIED') <> 'PASS' and not coalesce(pe.name_check_has_override,false))
           then jsonb_build_array('BLOCKED_NAME_CHECK')
           else '[]'::jsonb
         end)
        ||
        (case
           when (cp.payee_entity_id is not null and cp.payee_bank_hash is not null and v_requires_payee_map = true and not coalesce(pe.payee_map_present,false))
           then jsonb_build_array('BLOCKED_NO_PAYEE_MAP')
           else '[]'::jsonb
         end)
      ) as blockers,

      (
        jsonb_array_length(
          (
            (case when (cp.payee_entity_id is null or cp.payee_bank_hash is null or btrim(cp.payee_bank_hash) = '') then jsonb_build_array('BLOCKED_BANK_DETAILS') else '[]'::jsonb end)
            ||
            (case
               when (cp.payee_entity_id is not null and cp.payee_bank_hash is not null and v_need_name_check = true and coalesce(pe.name_check_status,'UNVERIFIED') <> 'PASS' and not coalesce(pe.name_check_has_override,false))
               then jsonb_build_array('BLOCKED_NAME_CHECK')
               else '[]'::jsonb
             end)
            ||
            (case
               when (cp.payee_entity_id is not null and cp.payee_bank_hash is not null and v_requires_payee_map = true and not coalesce(pe.payee_map_present,false))
               then jsonb_build_array('BLOCKED_NO_PAYEE_MAP')
               else '[]'::jsonb
             end)
          )
        ) = 0
      ) as is_ready_for_draft
    from cand_payee0 cp
    left join payees_enriched pe
      on pe.payee_entity_kind = cp.payee_entity_kind
     and pe.payee_entity_id = cp.payee_entity_id
     and pe.bank_details_hash is not distinct from cp.payee_bank_hash
  ),
  summary_json as (
    select
      jsonb_build_object(
        'readiness', jsonb_build_object(
          'payees_total', pr.payees_total,
          'payees_need_name_check', pr.payees_need_name_check,
          'payees_need_payee_map', pr.payees_need_payee_map,
          'payees_missing_bank_details', pr.payees_missing_bank_details
        ),
        'candidates', jsonb_build_object(
          'ready_count', cr.ready_count,
          'review_required_count', cr.review_required_count,
          'total_candidates', cr.total_candidates
        )
      ) as summary
    from (
      select
        count(*)::int as payees_total,
        sum(case when pe.is_missing_bank_details then 1 else 0 end)::int as payees_missing_bank_details,
        sum(case when pe.is_name_check_blocked then 1 else 0 end)::int as payees_need_name_check,
        sum(case when pe.is_payee_map_blocked then 1 else 0 end)::int as payees_need_payee_map
      from payees_enriched pe
    ) pr
    cross join (
      select
        count(*)::int as total_candidates,
        sum(
          case when
            (
              (coalesce(cp.non_mismatch_total_ex,0) <> 0
               or coalesce(cp.mismatch_source_paye_ex,0) <> 0
               or coalesce(cp.mismatch_source_umbrella_ex,0) <> 0)
              and cp.is_ready_for_draft = true
              and coalesce(cp.blocked_count,0) = 0
              and coalesce(cp.do_not_pay_count,0) = 0
              and cp.has_mismatch = false
            )
          then 1 else 0 end
        )::int as ready_count,
        sum(
          case when
            (
              (coalesce(cp.non_mismatch_total_ex,0) <> 0
               or coalesce(cp.mismatch_source_paye_ex,0) <> 0
               or coalesce(cp.mismatch_source_umbrella_ex,0) <> 0)
              and (
                cp.has_mismatch = true
                or jsonb_array_length(cp.blockers) > 0
                or coalesce(cp.blocked_count,0) > 0
                or coalesce(cp.do_not_pay_count,0) > 0
              )
            )
          then 1 else 0 end
        )::int as review_required_count
      from cand_payee cp
    ) cr
  )
  select
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

            'candidate_has_bank_details', ce.candidate_has_bank_details,
            'candidate_bank_hash', ce.candidate_bank_hash,
            'umbrella_has_bank_details', null,
            'umbrella_bank_hash', null,

            'payee_entity_kind', ce.payee_entity_kind,
            'payee_entity_id', case when ce.payee_entity_id is null then null else ce.payee_entity_id::text end,
            'payee_bank_hash', ce.payee_bank_hash,
            'payee_map_present', ce.payee_map_present,
            'name_check_status', ce.payee_name_check_status,
            'name_check_has_override', ce.payee_name_check_has_override,
            'blockers', ce.blockers,
            'is_ready_for_draft', ce.is_ready_for_draft,

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
        from cand_payee ce
        where ce.cand_pay_method = 'PAYE'
      ),
      '[]'::jsonb
    ),
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

            'candidate_has_bank_details', ce.candidate_has_bank_details,
            'candidate_bank_hash', ce.candidate_bank_hash,
            'umbrella_has_bank_details', case when ce.cand_pay_method <> 'PAYE' then ce.umbrella_has_bank_details else null end,
            'umbrella_bank_hash', case when ce.cand_pay_method <> 'PAYE' then ce.umbrella_bank_hash else null end,

            'payee_entity_kind', ce.payee_entity_kind,
            'payee_entity_id', case when ce.payee_entity_id is null then null else ce.payee_entity_id::text end,
            'payee_bank_hash', ce.payee_bank_hash,
            'payee_map_present', ce.payee_map_present,
            'name_check_status', ce.payee_name_check_status,
            'name_check_has_override', ce.payee_name_check_has_override,
            'blockers', ce.blockers,
            'is_ready_for_draft', ce.is_ready_for_draft,

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
        from cand_payee ce
        where ce.cand_pay_method <> 'PAYE'
      ),
      '[]'::jsonb
    ),
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
    ),
    coalesce((select pj.payees from payees_json pj), '[]'::jsonb),
    coalesce((select sj.summary from summary_json sj), '{}'::jsonb)
  into v_paye, v_nonpaye, v_blocked, v_do_not_pay, v_snoozed, v_payees, v_summary;

  return jsonb_build_object(
    'pay_date', p_pay_date::text,
    'pay_week_start', v_week_start::text,
    'week_ending_cutoff_date', p_week_ending_cutoff::text,
    'eligibility', jsonb_build_object(
      'today_uk', v_today_uk::text,
      'from_date', v_eligibility_from_date::text,
      'to_date', v_eligibility_to_date::text,
      'months_back', v_pay_eligibility_months_back,
      'weeks_ahead', v_pay_eligibility_weeks_ahead
    ),
    'filters', jsonb_build_object(
      'candidate_id', case when p_candidate_id is null then null else p_candidate_id::text end,
      'client_id', case when p_client_id is null then null else p_client_id::text end
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
        'supports_auto_execute', v_rail_supports_auto_execute,
        'need_name_check', v_need_name_check,
        'requires_payee_map', v_requires_payee_map
      ),
      'schedule_defaults', jsonb_build_object(
        'umbrella_local', v_default_schedule_umbrella_local,
        'paye_local', v_default_schedule_paye_local
      ),
      'funds_warning_hours_json', v_funds_warning_hours_json
    ),
    'summary', v_summary,
    'payees', v_payees,
    'paye_candidates', v_paye,
    'non_paye_payees', v_nonpaye,
    'blocked_items', v_blocked,
    'do_not_pay_items', v_do_not_pay,
    'snoozed_items', v_snoozed
  );
end;
$function$;




CREATE OR REPLACE FUNCTION public.pay_create_draft_batch(
  p_pay_date date,
  p_week_ending_cutoff date,
  p_pay_channel_scope text,
  p_actor_user_id uuid,
  p_preview_decisions_json jsonb,
  p_candidate_id uuid default null,
  p_client_id uuid default null
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_scope text := upper(btrim(coalesce(p_pay_channel_scope,'')));

  v_week_start date := public._pay_week_start_monday(p_pay_date);

  -- ✅ UK “today” anchor for eligibility window (Option A)
  v_today_uk date := (now() at time zone 'Europe/London')::date;

  -- ✅ Eligibility window knobs (from settings_defaults; fallback to defaults if column absent)
  v_pay_eligibility_months_back int := 6;
  v_pay_eligibility_weeks_ahead int := 2;

  -- ✅ Computed eligibility window
  v_eligibility_from_date date;
  v_eligibility_to_date date;

  -- ✅ Optional filters (single candidate/client)
  v_candidate_filter_single uuid := p_candidate_id;
  v_client_filter_single uuid := p_client_id;
  v_filter_text text;

  v_vat_rate_pct numeric;
  v_erni_pct numeric;

  v_settings record;

  v_need_name_check boolean := false;

  v_requires_payee_map boolean := false;

  v_blocked_candidates jsonb := '[]'::jsonb;

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

  v_sum_scope numeric;
  v_debt_scope numeric;

  -- breakdown integrity vars
  v_breakdown_missing_ct int := 0;
  v_breakdown_bad_ct int := 0;
  v_breakdown_bad jsonb := '[]'::jsonb;

  ----------------------------------------------------------------
  -- DEBUG / AUDIT vars (non-functional)
  ----------------------------------------------------------------
  v_stage text := 'INIT';
  v_rows int := 0;
  v_rows_ins_items int := 0;
  v_rows_ins_candidates int := 0;
  v_rows_del_candidates int := 0;
  v_rows_upd_candidates_loan int := 0;
  v_rows_ins_loan_items int := 0;
  v_rows_ins_debt_items int := 0;
  v_rows_upd_candidates_debt int := 0;
  v_rows_upd_candidates_summaries int := 0;
  v_rows_ins_snapshots int := 0;
  v_rows_ins_breakdowns int := 0;
  v_sample_candidate_ids jsonb := '[]'::jsonb;
  v_preview_candidate_filter_ct int := 0;
  v_preview_decisions_keys jsonb := '{}'::jsonb;
begin
  v_stage := 'STAGE_00_INPUTS';

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:STAGE_00_INPUTS',
      jsonb_build_object(
        'pay_date', coalesce(p_pay_date::text, null),
        'week_ending_cutoff', coalesce(p_week_ending_cutoff::text, null),
        'pay_channel_scope_raw', coalesce(p_pay_channel_scope, null),
        'scope_norm', coalesce(v_scope, null),
        'actor_user_id', coalesce(p_actor_user_id::text, null),
        'candidate_id_param', coalesce(p_candidate_id::text, null),
        'client_id_param', coalesce(p_client_id::text, null),
        'preview_decisions_json_type', jsonb_typeof(p_preview_decisions_json),
        'preview_decisions_json_keys_sample', (
          select coalesce(jsonb_agg(k.key order by k.key), '[]'::jsonb)
          from (
            select e.key
            from jsonb_each(coalesce(p_preview_decisions_json,'{}'::jsonb)) e
            order by e.key
            limit 50
          ) k
        )
      ),
      'pay_create_draft_batch',
      coalesce('pay_date:'||p_pay_date::text, 'pay_date:null'),
      null,
      null,
      null,
      null,
      null
    );
  exception when others then
    null;
  end;

  if p_pay_date is null then
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCH:ERROR_PAY_DATE_REQUIRED',
        jsonb_build_object(
          'stage', v_stage,
          'error', 'pay_date is required'
        ),
        'pay_create_draft_batch',
        'pay_date:null',
        null, null, null, null, null
      );
    exception when others then null; end;

    raise exception 'pay_date is required';
  end if;

  if p_week_ending_cutoff is null then
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCH:ERROR_WEEK_ENDING_CUTOFF_REQUIRED',
        jsonb_build_object(
          'stage', v_stage,
          'pay_date', p_pay_date::text,
          'error', 'week_ending_cutoff is required'
        ),
        'pay_create_draft_batch',
        'pay_date:'||p_pay_date::text,
        null, null, null, null, null
      );
    exception when others then null; end;

    raise exception 'week_ending_cutoff is required';
  end if;

  if v_scope not in ('PAYE','UMBRELLA') then
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCH:ERROR_INVALID_SCOPE',
        jsonb_build_object(
          'stage', v_stage,
          'pay_date', p_pay_date::text,
          'scope_norm', v_scope,
          'error', 'Invalid pay_channel_scope (expected PAYE or UMBRELLA)'
        ),
        'pay_create_draft_batch',
        'pay_date:'||p_pay_date::text,
        null, null, null, null, null
      );
    exception when others then null; end;

    raise exception 'Invalid pay_channel_scope (expected PAYE or UMBRELLA)';
  end if;

  if to_regclass('public.settings_defaults') is null then
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCH:ERROR_SETTINGS_DEFAULTS_MISSING',
        jsonb_build_object(
          'stage', v_stage,
          'pay_date', p_pay_date::text,
          'error', 'settings_defaults missing'
        ),
        'pay_create_draft_batch',
        'pay_date:'||p_pay_date::text,
        null, null, null, null, null
      );
    exception when others then null; end;

    raise exception 'settings_defaults missing';
  end if;
  if to_regclass('public.settings_finance_windows') is null then
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCH:ERROR_SETTINGS_FINANCE_WINDOWS_MISSING',
        jsonb_build_object(
          'stage', v_stage,
          'pay_date', p_pay_date::text,
          'error', 'settings_finance_windows missing'
        ),
        'pay_create_draft_batch',
        'pay_date:'||p_pay_date::text,
        null, null, null, null, null
      );
    exception when others then null; end;

    raise exception 'settings_finance_windows missing';
  end if;

  v_stage := 'STAGE_01_FILTERS_BACKCOMPAT';

  -- Optional filters can be supplied via preview_decisions_json for backward-compatible callers
  begin
    if v_candidate_filter_single is null then
      v_filter_text := nullif(btrim(coalesce(p_preview_decisions_json->>'candidate_filter_id','')), '');
      if v_filter_text is not null then
        v_candidate_filter_single := v_filter_text::uuid;
      end if;
    end if;

    if v_client_filter_single is null then
      v_filter_text := nullif(btrim(coalesce(p_preview_decisions_json->>'client_filter_id','')), '');
      if v_filter_text is not null then
        v_client_filter_single := v_filter_text::uuid;
      end if;
    end if;

    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCH:STAGE_01_FILTERS_BACKCOMPAT',
        jsonb_build_object(
          'stage', v_stage,
          'candidate_filter_single', coalesce(v_candidate_filter_single::text, null),
          'client_filter_single', coalesce(v_client_filter_single::text, null),
          'candidate_filter_id_raw', nullif(btrim(coalesce(p_preview_decisions_json->>'candidate_filter_id','')), ''),
          'client_filter_id_raw', nullif(btrim(coalesce(p_preview_decisions_json->>'client_filter_id','')), '')
        ),
        'pay_create_draft_batch',
        'pay_date:'||p_pay_date::text,
        null, null, null, null, null
      );
    exception when others then null; end;

  exception when invalid_text_representation then
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCH:ERROR_INVALID_FILTER_UUID',
        jsonb_build_object(
          'stage', v_stage,
          'candidate_filter_id_raw', nullif(btrim(coalesce(p_preview_decisions_json->>'candidate_filter_id','')), ''),
          'client_filter_id_raw', nullif(btrim(coalesce(p_preview_decisions_json->>'client_filter_id','')), ''),
          'error', 'Invalid candidate_filter_id/client_filter_id (must be UUID)'
        ),
        'pay_create_draft_batch',
        'pay_date:'||p_pay_date::text,
        null, null, null, null, null
      );
    exception when others then null; end;

    raise exception 'Invalid candidate_filter_id/client_filter_id (must be UUID)';
  end;

  v_stage := 'STAGE_02_FINANCE_WINDOW';

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

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:STAGE_02_FINANCE_WINDOW',
      jsonb_build_object(
        'stage', v_stage,
        'pay_date', p_pay_date::text,
        'vat_rate_pct', v_vat_rate_pct,
        'erni_pct', v_erni_pct
      ),
      'pay_create_draft_batch',
      'pay_date:'||p_pay_date::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  if v_vat_rate_pct is null or v_erni_pct is null then
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCH:ERROR_NO_FINANCE_WINDOW',
        jsonb_build_object(
          'stage', v_stage,
          'pay_date', p_pay_date::text,
          'vat_rate_pct', v_vat_rate_pct,
          'erni_pct', v_erni_pct,
          'error', 'No finance window found'
        ),
        'pay_create_draft_batch',
        'pay_date:'||p_pay_date::text,
        null, null, null, null, null
      );
    exception when others then null; end;

    raise exception 'No finance window found for pay_date %', p_pay_date;
  end if;

  v_stage := 'STAGE_03_SETTINGS_DEFAULTS';

  select
    sd.banking_system,
    sd.external_paye_system,
    sd.rail_provider_default,
    sd.rail_env_default,
    sd.rail_supports_name_check
  into v_settings
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:STAGE_03_SETTINGS_DEFAULTS',
      jsonb_build_object(
        'stage', v_stage,
        'banking_system', coalesce(v_settings.banking_system, null),
        'external_paye_system', coalesce(v_settings.external_paye_system, null),
        'rail_provider_default', coalesce(v_settings.rail_provider_default, null),
        'rail_env_default', coalesce(v_settings.rail_env_default, null),
        'rail_supports_name_check', coalesce(v_settings.rail_supports_name_check, null)
      ),
      'settings_defaults',
      '1',
      null, null, null, null, null
    );
  exception when others then null; end;

  if v_settings.banking_system is null or v_settings.external_paye_system is null then
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCH:ERROR_SETTINGS_DEFAULTS_MISSING_BANKING_EXTERNAL_PAYE',
        jsonb_build_object(
          'stage', v_stage,
          'banking_system', coalesce(v_settings.banking_system, null),
          'external_paye_system', coalesce(v_settings.external_paye_system, null),
          'error', 'settings_defaults missing banking_system/external_paye_system'
        ),
        'settings_defaults',
        '1',
        null, null, null, null, null
      );
    exception when others then null; end;

    raise exception 'settings_defaults missing banking_system/external_paye_system';
  end if;

  if v_settings.rail_provider_default is null or v_settings.rail_env_default is null then
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCH:ERROR_SETTINGS_DEFAULTS_MISSING_RAIL_DEFAULTS',
        jsonb_build_object(
          'stage', v_stage,
          'rail_provider_default', coalesce(v_settings.rail_provider_default, null),
          'rail_env_default', coalesce(v_settings.rail_env_default, null),
          'error', 'settings_defaults missing rail_provider_default/rail_env_default'
        ),
        'settings_defaults',
        '1',
        null, null, null, null, null
      );
    exception when others then null; end;

    raise exception 'settings_defaults missing rail_provider_default/rail_env_default';
  end if;

  v_need_name_check := (coalesce(v_settings.rail_supports_name_check,false) = true)
                       and (upper(btrim(coalesce(v_settings.rail_provider_default,''))) <> 'CSV');

  v_requires_payee_map := (upper(btrim(coalesce(v_settings.rail_provider_default,''))) <> 'CSV');

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:STAGE_03B_DERIVED_RAIL_FLAGS',
      jsonb_build_object(
        'stage', 'STAGE_03B_DERIVED_RAIL_FLAGS',
        'scope', v_scope,
        'rail_provider_default_norm', upper(btrim(coalesce(v_settings.rail_provider_default,''))),
        'rail_env_default_norm', upper(btrim(coalesce(v_settings.rail_env_default,''))),
        'need_name_check', v_need_name_check,
        'requires_payee_map', v_requires_payee_map
      ),
      'pay_create_draft_batch',
      'pay_date:'||p_pay_date::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  -- ✅ Load eligibility window knobs from settings_defaults (Option A)
  -- NOTE: use exception guard so function is robust even if column is absent at runtime.
  v_stage := 'STAGE_04_ELIGIBILITY_KNOBS';
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

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:STAGE_04_ELIGIBILITY_WINDOW',
      jsonb_build_object(
        'stage', v_stage,
        'today_uk', v_today_uk::text,
        'pay_eligibility_months_back', v_pay_eligibility_months_back,
        'pay_eligibility_weeks_ahead', v_pay_eligibility_weeks_ahead,
        'eligibility_from_date', v_eligibility_from_date::text,
        'eligibility_to_date', v_eligibility_to_date::text,
        'week_ending_cutoff', p_week_ending_cutoff::text
      ),
      'pay_create_draft_batch',
      'pay_date:'||p_pay_date::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  v_stage := 'STAGE_05_CANDIDATE_FILTER_ARRAY';

  if jsonb_typeof(p_preview_decisions_json->'candidate_ids') = 'array' then
    select coalesce(array_agg((x::text)::uuid), array[]::uuid[])
    into v_candidate_filter
    from jsonb_array_elements_text(p_preview_decisions_json->'candidate_ids') x;
  end if;

  -- Treat empty array as "ALL" (null filter)
  if v_candidate_filter is not null and array_length(v_candidate_filter, 1) is null then
    v_candidate_filter := null;
  end if;

  begin
    select coalesce(jsonb_agg(x.id order by x.id), '[]'::jsonb), count(*)::int
    into v_sample_candidate_ids, v_preview_candidate_filter_ct
    from (
      select u::text as id
      from unnest(coalesce(v_candidate_filter, array[]::uuid[])) u
      limit 50
    ) x;

    v_preview_decisions_keys := jsonb_build_object(
      'mismatch_choices_keys_sample', (
        select coalesce(jsonb_agg(k.k order by k.k), '[]'::jsonb)
        from (
          select e.key as k
          from jsonb_each(coalesce(v_mismatch_choices,'{}'::jsonb)) e
          order by e.key
          limit 50
        ) k
      ),
      'loan_caps_keys_sample', (
        select coalesce(jsonb_agg(k.k order by k.k), '[]'::jsonb)
        from (
          select e.key as k
          from jsonb_each(coalesce(v_loan_caps,'{}'::jsonb)) e
          order by e.key
          limit 50
        ) k
      )
    );

    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:STAGE_05_CANDIDATE_FILTER_ARRAY',
      jsonb_build_object(
        'stage', v_stage,
        'candidate_filter_array_ct', v_preview_candidate_filter_ct,
        'candidate_filter_array_sample', v_sample_candidate_ids,
        'candidate_filter_single', coalesce(v_candidate_filter_single::text, null),
        'client_filter_single', coalesce(v_client_filter_single::text, null),
        'preview_decisions_key_samples', v_preview_decisions_keys
      ),
      'pay_create_draft_batch',
      'pay_date:'||p_pay_date::text,
      null, null, null, null, null
    );
  exception when others then
    null;
  end;

  v_stage := 'STAGE_06_BUILD_CANDIDATE_SET_FROM_PREVIEW';

  -- Candidate set from pay_preview.
  -- Compatible with both preview shapes:
  --  - if pay_preview returns has_any_delta => use it
  --  - otherwise derive "has_any_delta" from totals
  with preview as (
    select public.pay_preview(p_pay_date, p_week_ending_cutoff, p_actor_user_id, null, null) as j
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
    and (v_candidate_filter_single is null or (cand->>'candidate_id')::uuid = v_candidate_filter_single)
  )
  select coalesce(array_agg(s.candidate_id), array[]::uuid[])
  into v_candidate_ids
  from selected s;

  begin
    select coalesce(jsonb_agg(x.candidate_id::text order by x.candidate_id::text), '[]'::jsonb)
    into v_sample_candidate_ids
    from (
      select unnest(coalesce(v_candidate_ids, array[]::uuid[])) as candidate_id
      limit 50
    ) x;

    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:STAGE_06_CANDIDATE_SET_RESULT',
      jsonb_build_object(
        'stage', v_stage,
        'candidate_ids_count', coalesce(array_length(v_candidate_ids,1),0),
        'candidate_ids_sample', v_sample_candidate_ids
      ),
      'pay_create_draft_batch',
      'pay_date:'||p_pay_date::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  -- Apply client filter (single) as an additional narrowing step (must not create candidates outside the requested client)
  v_stage := 'STAGE_07_APPLY_CLIENT_FILTER_SINGLE';
  if v_client_filter_single is not null then
    with cand_ids as (
      select unnest(v_candidate_ids) as candidate_id
    ),
    cand_ok as (
      select distinct tf.candidate_id
      from public.timesheets_financials tf
      join public.timesheets ts
        on ts.timesheet_id = tf.timesheet_id
       and ts.is_current = true
      join cand_ids ci
        on ci.candidate_id = tf.candidate_id
      join public.candidates c
        on c.id = tf.candidate_id
      where tf.is_current = true
        and coalesce(tf.pay_on_hold,false) = false
        and ts.authorised_at_server is not null
        and coalesce(tf.has_rate_issue,false) = false
        and coalesce(tf.has_pay_channel_issue,false) = false
        and upper(coalesce(tf.processing_status::text,'')) not in ('UNASSIGNED','CLIENT_UNRESOLVED','RATE_MISSING','PAY_CHANNEL_MISSING')
        and upper(coalesce(c.pay_method,'')) in ('PAYE','UMBRELLA')
        and tf.client_id = v_client_filter_single
        and ts.week_ending_date::date >= v_eligibility_from_date
        and ts.week_ending_date::date <= v_eligibility_to_date
        and ts.week_ending_date::date <= p_week_ending_cutoff
    )
    select coalesce(array_agg(ci.candidate_id), array[]::uuid[])
    into v_candidate_ids
    from cand_ids ci
    join cand_ok ok
      on ok.candidate_id = ci.candidate_id;

    begin
      select coalesce(jsonb_agg(x.candidate_id::text order by x.candidate_id::text), '[]'::jsonb)
      into v_sample_candidate_ids
      from (
        select unnest(coalesce(v_candidate_ids, array[]::uuid[])) as candidate_id
        limit 50
      ) x;

      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCH:STAGE_07_CLIENT_FILTER_RESULT',
        jsonb_build_object(
          'stage', v_stage,
          'client_filter_single', v_client_filter_single::text,
          'candidate_ids_count_after_client_filter', coalesce(array_length(v_candidate_ids,1),0),
          'candidate_ids_sample_after_client_filter', v_sample_candidate_ids
        ),
        'pay_create_draft_batch',
        'pay_date:'||p_pay_date::text,
        null, null, null, null, null
      );
    exception when others then null; end;
  else
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCH:STAGE_07_CLIENT_FILTER_SKIPPED',
        jsonb_build_object(
          'stage', v_stage,
          'client_filter_single', null
        ),
        'pay_create_draft_batch',
        'pay_date:'||p_pay_date::text,
        null, null, null, null, null
      );
    exception when others then null; end;
  end if;

  v_stage := 'STAGE_08_ASSERT_NON_EMPTY_CANDIDATES';

  if array_length(v_candidate_ids,1) is null or array_length(v_candidate_ids,1) = 0 then
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCH:ERROR_NOTHING_TO_PAY_AFTER_BLOCKERS',
        jsonb_build_object(
          'stage', v_stage,
          'candidate_ids_count', coalesce(array_length(v_candidate_ids,1),0),
          'error', 'Nothing to pay (no payable deltas after blockers)'
        ),
        'pay_create_draft_batch',
        'pay_date:'||p_pay_date::text,
        null, null, null, null, null
      );
    exception when others then null; end;

    raise exception 'Nothing to pay (no payable deltas after blockers)';
  end if;

  v_stage := 'STAGE_09_VALIDATE_MISMATCH_DECISIONS';

  -- Validate mismatch decisions completeness for included candidates (scope-agnostic)
  with preview as (
    select public.pay_preview(p_pay_date, p_week_ending_cutoff, p_actor_user_id, null, null) as j
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

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:STAGE_09_MISMATCH_DECISIONS_RESULT',
      jsonb_build_object(
        'stage', v_stage,
        'missing_mismatch_decisions_candidate_ids', v_reserved,
        'missing_count', jsonb_array_length(v_reserved)
      ),
      'pay_create_draft_batch',
      'pay_date:'||p_pay_date::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  if jsonb_array_length(v_reserved) > 0 then
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCH:ERROR_MISMATCH_DECISIONS_REQUIRED',
        jsonb_build_object(
          'stage', v_stage,
          'missing_candidates', v_reserved,
          'error', 'MISMATCH_DECISIONS_REQUIRED'
        ),
        'pay_create_draft_batch',
        'pay_date:'||p_pay_date::text,
        null, null, null, null, null
      );
    exception when others then null; end;

    raise exception 'MISMATCH_DECISIONS_REQUIRED for candidates %', v_reserved::text;
  end if;

  v_stage := 'STAGE_10_CREATE_BATCH';

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

  GET DIAGNOSTICS v_rows = ROW_COUNT;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:STAGE_10_BATCH_CREATED',
      jsonb_build_object(
        'stage', v_stage,
        'row_count', v_rows,
        'pay_batch_id', v_batch_id::text,
        'status', 'DRAFT',
        'pay_date', p_pay_date::text,
        'scope', v_scope,
        'rail_provider_snapshot', coalesce(v_settings.rail_provider_default, null),
        'rail_env_snapshot', coalesce(v_settings.rail_env_default, null)
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  v_stage := 'STAGE_11_INSERT_PAY_BATCH_CANDIDATES';

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
    case when v_scope = 'PAYE' then 'PENDING_NET' else null end,
    nullif(v_mismatch_choices->>c.id::text,''),
    null, null, 0, 0
  from public.candidates c
  where c.id = any(v_candidate_ids);

  GET DIAGNOSTICS v_rows_ins_candidates = ROW_COUNT;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:STAGE_11_CANDIDATES_INSERTED',
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', v_batch_id::text,
        'scope', v_scope,
        'inserted_candidate_rows', v_rows_ins_candidates,
        'candidate_ids_count', coalesce(array_length(v_candidate_ids,1),0),
        'candidate_ids_sample', (
          select coalesce(jsonb_agg(x.candidate_id::text order by x.candidate_id::text), '[]'::jsonb)
          from (
            select unnest(coalesce(v_candidate_ids, array[]::uuid[])) as candidate_id
            limit 50
          ) x
        )
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  v_stage := 'STAGE_12_INSERT_PAY_BATCH_ITEMS';

  -- Build pay_batch_items with segment-level ref gating:
  -- blocked-positive segments (missing ref_num where required) are excluded (delta forced to 0).
  -- Also:
  --  - exclude items already reserved in other active batches (aligns with preview)
  --  - exclude items snoozed (any kind) (aligns with preview UX)
  --  - for UMBRELLA draft, include only "ready" umbrella items (bank details + name-check accept/override + payee-map if required)
  --  - for PAYE draft, include only "ready" candidate items (bank details + name-check accept/override + payee-map if required)
  with finance as (
    select v_vat_rate_pct as vat_rate_pct, v_erni_pct as erni_pct
  ),
  active_snoozes as (
    select
      s.candidate_id,
      s.timesheet_id,
      s.segment_id,
      s.source_ref,
      s.snooze_until_date
    from public.pay_item_snoozes s
    where s.cleared_at_utc is null
      and (
        s.snooze_until_date is null
        or s.snooze_until_date >= p_pay_date
      )
  ),
  reserved_batch_items as (
    select distinct
      p2.timesheet_id,
      p2.segment_key,
      p2.source_ref
    from public.pay_batch_items p2
    join public.pay_batch_candidates pbc2
      on pbc2.id = p2.pay_batch_candidate_id
    join public.pay_batches pb2
      on pb2.id = pbc2.pay_batch_id
    where pb2.id <> v_batch_id
      and p2.timesheet_id is not null
      and p2.pay_channel in ('PAYE','UMBRELLA')
      and p2.item_type <> 'DEBT_CREATED'
      and upper(coalesce(pb2.status,'')) in (
        'DRAFT',
        'DRAFT_CREATED',
        'READY',
        'WAITING_BANK_CONFIRM',
        'PARTIAL',
        'FAILED',
        'BLOCKED_FUNDS',
        'SCHEDULED',
        'EXECUTING',
        'AWAITING_AUTHORISATION',
        'AUTHORISED_FOR_PAYMENT'
      )
  ),
  eligible_tf as (
    select
      tf.timesheet_id,
      tf.candidate_id,
      tf.client_id,
      upper(coalesce(tf.pay_method,'')) as ts_pay_method,
      upper(coalesce(c.pay_method,'')) as cand_pay_method,
      c.umbrella_id as umbrella_id,
      c.bank_details_hash as cand_bank_hash,

      ts.contract_id,
      ts.reference_number,

      ct.overrideclientsettings,

      coalesce(
        case when ct.overrideclientsettings then ct.require_reference_to_pay end,
        cs.pay_reference_required,
        false
      ) as require_reference_to_pay,

      tf.invoice_breakdown_json,

      -- ✅ hours + rates snapshots (required for canonical breakdown)
      tf.hours_day,
      tf.hours_night,
      tf.hours_sat,
      tf.hours_sun,
      tf.hours_bh,
      tf.pay_day,
      tf.pay_night,
      tf.pay_sat,
      tf.pay_sun,
      tf.pay_bh,

      tf.additional_units_json,
      tf.mileage_units,
      tf.mileage_pay_rate,

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
      and upper(coalesce(tf.processing_status::text,'')) not in ('UNASSIGNED','CLIENT_UNRESOLVED','RATE_MISSING','PAY_CHANNEL_MISSING')
      and upper(coalesce(c.pay_method,'')) in ('PAYE','UMBRELLA')
      and tf.candidate_id = any(v_candidate_ids)
      and (v_client_filter_single is null or tf.client_id = v_client_filter_single)

      -- ✅ Option A: match pay_preview eligibility window (relative to UK “today”).
      and ts.week_ending_date::date >= v_eligibility_from_date
      and ts.week_ending_date::date <= v_eligibility_to_date

      -- ✅ New: week-ending cutoff (user-controlled)
      and ts.week_ending_date::date <= p_week_ending_cutoff
  ),
  umb as (
    select
      u.id as umbrella_id,
      coalesce(u.vat_chargeable,false) as vat_chargeable,
      coalesce(u.enabled,false) as enabled,
      u.bank_details_hash as bank_details_hash
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
      coalesce(u.enabled,false) as umb_enabled,
      u.bank_details_hash as umb_bank_hash,

      case
        when e.invoice_breakdown_json is not null
         and jsonb_typeof(e.invoice_breakdown_json)='object'
         and upper(coalesce(e.invoice_breakdown_json->>'mode',''))='SEGMENTS'
         and jsonb_typeof(e.invoice_breakdown_json->'segments')='array'
        then (
          select coalesce(jsonb_agg(
            jsonb_build_object(
              'segment_id', nullif(btrim(coalesce(seg->>'segment_id','')),''),
              'date', nullif(btrim(coalesce(seg->>'date','')),''),
              'start_utc', nullif(btrim(coalesce(seg->>'start_utc','')),''),
              'end_utc', nullif(btrim(coalesce(seg->>'end_utc','')),''),
              'break_mins', coalesce(nullif(seg->>'break_mins','')::numeric,0),
              'breaks', coalesce(seg->'breaks','[]'::jsonb),
              'hours_day', coalesce(nullif(seg->>'hours_day','')::numeric,0),
              'hours_night', coalesce(nullif(seg->>'hours_night','')::numeric,0),
              'hours_sat', coalesce(nullif(seg->>'hours_sat','')::numeric,0),
              'hours_sun', coalesce(nullif(seg->>'hours_sun','')::numeric,0),
              'hours_bh', coalesce(nullif(seg->>'hours_bh','')::numeric,0),
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
      c.umb_enabled,
      c.cand_bank_hash,
      c.umb_bank_hash,
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
      d.umb_enabled,
      d.cand_bank_hash,
      d.umb_bank_hash,
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
      d.umb_enabled,
      d.cand_bank_hash,
      d.umb_bank_hash,
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
      d.umb_enabled,
      d.cand_bank_hash,
      d.umb_bank_hash,
      'ADDITIONAL'::text as kind,
      null::text as segment_key,
      'additional'::text as source_ref,
      round(d.cur_additional - coalesce(nullif(d.base_json->>'additional_pay_ex_vat','')::numeric,0), 2) as delta_ex
    from deltas d
    union all
    select d.candidate_id, d.timesheet_id, d.ts_pay_method, d.cand_pay_method, d.umbrella_id, d.umb_vat_chargeable, d.umb_enabled, d.cand_bank_hash, d.umb_bank_hash,
      'EXPENSES', null, 'expenses', round(d.expenses_pay_ex_vat - coalesce(nullif(d.base_json #>> '{expenses,expenses_pay_ex_vat}','')::numeric,0),2)
    from deltas d
    union all
    select d.candidate_id, d.timesheet_id, d.ts_pay_method, d.cand_pay_method, d.umbrella_id, d.umb_vat_chargeable, d.umb_enabled, d.cand_bank_hash, d.umb_bank_hash,
      'TRAVEL', null, 'travel', round(d.travel_pay_ex_vat - coalesce(nullif(d.base_json #>> '{expenses,travel_pay_ex_vat}','')::numeric,0),2)
    from deltas d
    union all
    select d.candidate_id, d.timesheet_id, d.ts_pay_method, d.cand_pay_method, d.umbrella_id, d.umb_vat_chargeable, d.umb_enabled, d.cand_bank_hash, d.umb_bank_hash,
      'ACCOMMODATION', null, 'accommodation', round(d.accommodation_pay_ex_vat - coalesce(nullif(d.base_json #>> '{expenses,accommodation_pay_ex_vat}','')::numeric,0),2)
    from deltas d
    union all
    select d.candidate_id, d.timesheet_id, d.ts_pay_method, d.cand_pay_method, d.umbrella_id, d.umb_vat_chargeable, d.umb_enabled, d.cand_bank_hash, d.umb_bank_hash,
      'OTHER', null, 'other', round(d.other_pay_ex_vat - coalesce(nullif(d.base_json #>> '{expenses,other_pay_ex_vat}','')::numeric,0),2)
    from deltas d
    union all
    select d.candidate_id, d.timesheet_id, d.ts_pay_method, d.cand_pay_method, d.umbrella_id, d.umb_vat_chargeable, d.umb_enabled, d.cand_bank_hash, d.umb_bank_hash,
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
      s.umb_enabled,
      s.cand_bank_hash,
      s.umb_bank_hash,
      'SEGMENT'::text as kind,
      s.segment_key,
      ('seg:' || s.segment_key)::text as source_ref,
      s.delta_ex
    from segment_delta_rows s
    union all
    select
      a.candidate_id, a.timesheet_id, a.ts_pay_method, a.cand_pay_method, a.umbrella_id, a.umb_vat_chargeable, a.umb_enabled, a.cand_bank_hash, a.umb_bank_hash,
      'ADJUSTMENT'::text, null::text, a.source_ref, a.delta_ex
    from adj_delta_rows a
    union all
    select
      o.candidate_id, o.timesheet_id, o.ts_pay_method, o.cand_pay_method, o.umbrella_id, o.umb_vat_chargeable, o.umb_enabled, o.cand_bank_hash, o.umb_bank_hash,
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
  filtered as (
    select
      r.*
    from routed r
    where r.pay_channel = v_scope
      and not exists (
        select 1
        from reserved_batch_items rb
        where rb.timesheet_id is not distinct from r.timesheet_id
          and (
            (r.segment_key is not null and rb.segment_key is not null and rb.segment_key = r.segment_key)
            or (r.source_ref is not null and rb.source_ref is not null and rb.source_ref = r.source_ref)
          )
        limit 1
      )
      and not exists (
        select 1
        from active_snoozes sn
        where sn.candidate_id = r.candidate_id
          and sn.timesheet_id is not distinct from r.timesheet_id
          and (
            (sn.segment_id is not null and r.segment_key is not null and sn.segment_id = r.segment_key)
            or (sn.source_ref is not null and r.source_ref is not null and sn.source_ref = r.source_ref)
          )
        limit 1
      )
      and (
        v_scope <> 'PAYE'
        or (
          r.cand_bank_hash is not null
          and btrim(r.cand_bank_hash) <> ''
          and (
            v_need_name_check = false
            or exists (
              select 1
              from public.bank_name_checks bnc
              where bnc.rail_provider = upper(btrim(coalesce(v_settings.rail_provider_default,'')))
                and bnc.rail_env = upper(btrim(coalesce(v_settings.rail_env_default,'')))
                and bnc.entity_kind = 'CANDIDATE'
                and bnc.entity_id = r.candidate_id
                and bnc.bank_details_hash = r.cand_bank_hash
                and (
                  upper(coalesce(bnc.status,'')) = 'PASS'
                  or (
                    bnc.override_reason is not null
                    and bnc.override_hash is not null
                    and bnc.override_hash = r.cand_bank_hash
                  )
                )
              limit 1
            )
          )
          and (
            v_requires_payee_map = false
            or exists (
              select 1
              from public.bank_payee_map bpm
              where bpm.rail_provider = upper(btrim(coalesce(v_settings.rail_provider_default,'')))
                and bpm.rail_env = upper(btrim(coalesce(v_settings.rail_env_default,'')))
                and bpm.entity_kind = 'CANDIDATE'
                and bpm.entity_id = r.candidate_id
                and bpm.bank_details_hash = r.cand_bank_hash
              limit 1
            )
          )
        )
      )
      and (
        v_scope <> 'UMBRELLA'
        or (
          r.umbrella_id is not null
          and coalesce(r.umb_enabled,false) = true
          and r.umb_bank_hash is not null
          and btrim(r.umb_bank_hash) <> ''
          and (
            v_need_name_check = false
            or exists (
              select 1
              from public.bank_name_checks bnc
              where bnc.rail_provider = upper(btrim(coalesce(v_settings.rail_provider_default,'')))
                and bnc.rail_env = upper(btrim(coalesce(v_settings.rail_env_default,'')))
                and bnc.entity_kind = 'UMBRELLA'
                and bnc.entity_id = r.umbrella_id
                and bnc.bank_details_hash = r.umb_bank_hash
                and (
                  upper(coalesce(bnc.status,'')) = 'PASS'
                  or (
                    bnc.override_reason is not null
                    and bnc.override_hash is not null
                    and bnc.override_hash = r.umb_bank_hash
                  )
                )
              limit 1
            )
          )
          and (
            v_requires_payee_map = false
            or exists (
              select 1
              from public.bank_payee_map bpm
              where bpm.rail_provider = upper(btrim(coalesce(v_settings.rail_provider_default,'')))
                and bpm.rail_env = upper(btrim(coalesce(v_settings.rail_env_default,'')))
                and bpm.entity_kind = 'UMBRELLA'
                and bpm.entity_id = r.umbrella_id
                and bpm.bank_details_hash = r.umb_bank_hash
              limit 1
            )
          )
        )
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
    from filtered r
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

  GET DIAGNOSTICS v_rows_ins_items = ROW_COUNT;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:STAGE_12_ITEMS_INSERTED',
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', v_batch_id::text,
        'scope', v_scope,
        'inserted_item_rows', v_rows_ins_items
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  v_stage := 'STAGE_13_DELETE_EMPTY_CANDIDATES';

  -- Remove any candidate rows that ended up with no items for this scoped batch
  delete from public.pay_batch_candidates pbc_del
  where pbc_del.pay_batch_id = v_batch_id
    and not exists (
      select 1
      from public.pay_batch_items pbi_chk
      where pbi_chk.pay_batch_candidate_id = pbc_del.id
        and pbi_chk.item_type <> 'DEBT_CREATED'
      limit 1
    );

  GET DIAGNOSTICS v_rows_del_candidates = ROW_COUNT;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:STAGE_13_EMPTY_CANDIDATES_DELETED',
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', v_batch_id::text,
        'deleted_candidate_rows', v_rows_del_candidates
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  v_stage := 'STAGE_14_ASSERT_ANY_CANDIDATES_REMAIN';

  if not exists (
    select 1
    from public.pay_batch_candidates pbc_any
    where pbc_any.pay_batch_id = v_batch_id
    limit 1
  ) then
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCH:ERROR_NOTHING_TO_PAY_FOR_SCOPE_AFTER_BLOCKERS',
        jsonb_build_object(
          'stage', v_stage,
          'pay_batch_id', v_batch_id::text,
          'scope', v_scope,
          'error', 'Nothing to pay (no payable items for scope after blockers)'
        ),
        'pay_batches',
        v_batch_id::text,
        null, null, null, null, null
      );
    exception when others then null; end;

    raise exception 'Nothing to pay (no payable items for scope % after blockers)', v_scope;
  end if;

  v_stage := 'STAGE_15_DOUBLE_PAY_CONFLICT_CHECK';

  -- Double-pay prevention (race-safe). Align reserving statuses with preview exclusion.
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
    where upper(coalesce(pb2.status,'')) in (
        'DRAFT',
        'DRAFT_CREATED',
        'READY',
        'WAITING_BANK_CONFIRM',
        'PARTIAL',
        'FAILED',
        'BLOCKED_FUNDS',
        'SCHEDULED',
        'EXECUTING',
        'AWAITING_AUTHORISATION',
        'AUTHORISED_FOR_PAYMENT'
      )
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

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:STAGE_15_DOUBLE_PAY_CONFLICTS_RESULT',
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', v_batch_id::text,
        'conflicts', v_reserved,
        'conflict_count', jsonb_array_length(v_reserved)
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  if jsonb_array_length(v_reserved) > 0 then
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCH:ERROR_DOUBLE_PAY_BLOCK',
        jsonb_build_object(
          'stage', v_stage,
          'pay_batch_id', v_batch_id::text,
          'conflicts', v_reserved,
          'error', 'DOUBLE_PAY_BLOCK'
        ),
        'pay_batches',
        v_batch_id::text,
        null, null, null, null, null
      );
    exception when others then null; end;

    raise exception 'DOUBLE_PAY_BLOCK: items already reserved in batches %', v_reserved::text;
  end if;

  v_stage := 'STAGE_16_APPLY_LOAN_REPAYMENTS';

  -- Apply loan repayments (catch-up: include overdue weeks <= v_week_start; oldest-first)
  -- IMPORTANT: to avoid double-deduction when you create two batches (PAYE + UMBRELLA),
  -- only apply loans in the batch that matches the candidate's CURRENT pay_method.
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

    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCH:LOAN_LOOP_CANDIDATE_START',
        jsonb_build_object(
          'stage', v_stage,
          'pay_batch_id', v_batch_id::text,
          'pay_batch_candidate_id', v_pbci::text,
          'candidate_id', v_rec.candidate_id::text,
          'cand_pay_method', v_cand_pm,
          'scope', v_scope,
          'umbrella_id', coalesce(v_cand_umb::text, null)
        ),
        'pay_batches',
        v_batch_id::text,
        null, null, null, null, null
      );
    exception when others then null; end;

    if v_cand_pm is distinct from v_scope then
      begin
        perform public._imp_debug_audit(
          p_actor_user_id,
          'PAY_CREATE_DRAFT_BATCH:LOAN_LOOP_SKIP_CANDIDATE_SCOPE_MISMATCH',
          jsonb_build_object(
            'stage', v_stage,
            'pay_batch_id', v_batch_id::text,
            'candidate_id', v_rec.candidate_id::text,
            'cand_pay_method', v_cand_pm,
            'scope', v_scope,
            'decision', 'continue'
          ),
          'pay_batches',
          v_batch_id::text,
          null, null, null, null, null
        );
      exception when others then null; end;

      continue;
    end if;

    if v_scope = 'UMBRELLA' then
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

    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCH:LOAN_LOOP_CAPS_AND_GROSS',
        jsonb_build_object(
          'stage', v_stage,
          'pay_batch_id', v_batch_id::text,
          'candidate_id', v_rec.candidate_id::text,
          'gross_main', v_gross_main,
          'loan_caps_raw', v_cap,
          'min_take_home', v_min_take,
          'max_deduction', v_max_ded
        ),
        'pay_batches',
        v_batch_id::text,
        null, null, null, null, null
      );
    exception when others then null; end;

    if v_min_take is not null and v_min_take < 0 then
      begin
        perform public._imp_debug_audit(
          p_actor_user_id,
          'PAY_CREATE_DRAFT_BATCH:ERROR_INVALID_MIN_TAKE_HOME',
          jsonb_build_object(
            'stage', v_stage,
            'pay_batch_id', v_batch_id::text,
            'candidate_id', v_rec.candidate_id::text,
            'min_take_home', v_min_take,
            'error', 'Invalid min_take_home'
          ),
          'pay_batches',
          v_batch_id::text,
          null, null, null, null, null
        );
      exception when others then null; end;

      raise exception 'Invalid min_take_home for candidate %', v_rec.candidate_id::text;
    end if;
    if v_max_ded is not null and v_max_ded < 0 then
      begin
        perform public._imp_debug_audit(
          p_actor_user_id,
          'PAY_CREATE_DRAFT_BATCH:ERROR_INVALID_MAX_DEDUCTION',
          jsonb_build_object(
            'stage', v_stage,
            'pay_batch_id', v_batch_id::text,
            'candidate_id', v_rec.candidate_id::text,
            'max_deduction', v_max_ded,
            'error', 'Invalid max_deduction'
          ),
          'pay_batches',
          v_batch_id::text,
          null, null, null, null, null
        );
      exception when others then null; end;

      raise exception 'Invalid max_deduction for candidate %', v_rec.candidate_id::text;
    end if;

    v_remaining := v_gross_main;

    if v_min_take is not null then
      v_remaining := greatest(round(v_gross_main - v_min_take,2), 0);
    end if;
    if v_max_ded is not null then
      v_remaining := least(v_remaining, round(v_max_ded,2));
    end if;

    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCH:LOAN_LOOP_REMAINING_COMPUTED',
        jsonb_build_object(
          'stage', v_stage,
          'pay_batch_id', v_batch_id::text,
          'candidate_id', v_rec.candidate_id::text,
          'gross_main', v_gross_main,
          'remaining_after_caps', v_remaining
        ),
        'pay_batches',
        v_batch_id::text,
        null, null, null, null, null
      );
    exception when others then null; end;

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
        begin
          perform public._imp_debug_audit(
            p_actor_user_id,
            'PAY_CREATE_DRAFT_BATCH:LOAN_LOOP_ADVANCE_SKIP_NON_POSITIVE',
            jsonb_build_object(
              'stage', v_stage,
              'pay_batch_id', v_batch_id::text,
              'candidate_id', v_rec.candidate_id::text,
              'advance_id', v_adv.advance_id::text,
              'due_week_start', coalesce(v_adv.due_week_start::text, null),
              'due_amt', v_adv.due_amt,
              'sched_amt', v_sched_amt,
              'decision', 'continue'
            ),
            'pay_batches',
            v_batch_id::text,
            null, null, null, null, null
          );
        exception when others then null; end;

        continue;
      end if;

      v_take_amt := least(v_sched_amt, v_remaining);
      v_take_amt := round(greatest(v_take_amt,0),2);

      begin
        perform public._imp_debug_audit(
          p_actor_user_id,
          'PAY_CREATE_DRAFT_BATCH:LOAN_LOOP_ADVANCE_EVAL',
          jsonb_build_object(
            'stage', v_stage,
            'pay_batch_id', v_batch_id::text,
            'candidate_id', v_rec.candidate_id::text,
            'advance_id', v_adv.advance_id::text,
            'due_week_start', coalesce(v_adv.due_week_start::text, null),
            'sched_amt', v_sched_amt,
            'remaining_before', v_remaining,
            'take_amt', v_take_amt
          ),
          'pay_batches',
          v_batch_id::text,
          null, null, null, null, null
        );
      exception when others then null; end;

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
          v_scope,
          case when v_scope = 'UMBRELLA' then v_cand_umb else null end,
          v_adv.due_week_start
        );

        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_rows_ins_loan_items := coalesce(v_rows_ins_loan_items,0) + coalesce(v_rows,0);

        begin
          perform public._imp_debug_audit(
            p_actor_user_id,
            'PAY_CREATE_DRAFT_BATCH:LOAN_LOOP_ITEM_INSERTED',
            jsonb_build_object(
              'stage', v_stage,
              'pay_batch_id', v_batch_id::text,
              'candidate_id', v_rec.candidate_id::text,
              'pay_batch_candidate_id', v_pbci::text,
              'advance_id', v_adv.advance_id::text,
              'repayment_week_start', coalesce(v_adv.due_week_start::text, null),
              'take_amt', v_take_amt,
              'row_count', v_rows
            ),
            'pay_batches',
            v_batch_id::text,
            null, null, null, null, null
          );
        exception when others then null; end;

        v_remaining := round(v_remaining - v_take_amt,2);
      end if;

      if v_remaining <= 0 then
        begin
          perform public._imp_debug_audit(
            p_actor_user_id,
            'PAY_CREATE_DRAFT_BATCH:LOAN_LOOP_EXIT_NO_REMAINING',
            jsonb_build_object(
              'stage', v_stage,
              'pay_batch_id', v_batch_id::text,
              'candidate_id', v_rec.candidate_id::text,
              'remaining', v_remaining,
              'decision', 'exit'
            ),
            'pay_batches',
            v_batch_id::text,
            null, null, null, null, null
          );
        exception when others then null; end;

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

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_rows_upd_candidates_loan := coalesce(v_rows_upd_candidates_loan,0) + coalesce(v_rows,0);

    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCH:LOAN_LOOP_CANDIDATE_UPDATED_SUMMARY',
        jsonb_build_object(
          'stage', v_stage,
          'pay_batch_id', v_batch_id::text,
          'candidate_id', v_rec.candidate_id::text,
          'pay_batch_candidate_id', v_pbci::text,
          'row_count', v_rows,
          'remaining_final', v_remaining
        ),
        'pay_batches',
        v_batch_id::text,
        null, null, null, null, null
      );
    exception when others then null; end;
  end loop;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:STAGE_16_LOAN_REPAYMENTS_SUMMARY',
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', v_batch_id::text,
        'loan_items_inserted_total', coalesce(v_rows_ins_loan_items,0),
        'candidates_updated_total', coalesce(v_rows_upd_candidates_loan,0)
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  v_stage := 'STAGE_17_CLIP_NEGATIVES_TO_DEBT_CREATED';

  -- Clip negatives into DEBT_CREATED (single-channel)
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

    if v_scope = 'PAYE' then
      select round(coalesce(sum(pbi.amount_ex_vat),0),2)
      into v_sum_scope
      from public.pay_batch_items pbi
      where pbi.pay_batch_candidate_id = v_pbci
        and pbi.pay_channel = 'PAYE'
        and pbi.item_type <> 'DEBT_CREATED';

      v_debt_scope := round(greatest(-coalesce(v_sum_scope,0),0),2);

      begin
        perform public._imp_debug_audit(
          p_actor_user_id,
          'PAY_CREATE_DRAFT_BATCH:DEBT_CLIP_EVAL_PAYE',
          jsonb_build_object(
            'stage', v_stage,
            'pay_batch_id', v_batch_id::text,
            'candidate_id', v_rec.candidate_id::text,
            'pay_batch_candidate_id', v_pbci::text,
            'sum_scope', v_sum_scope,
            'debt_scope', v_debt_scope
          ),
          'pay_batches',
          v_batch_id::text,
          null, null, null, null, null
        );
      exception when others then null; end;

      if v_debt_scope > 0 then
        insert into public.pay_batch_items(
          pay_batch_candidate_id,item_type,timesheet_id,segment_key,source_ref,description,
          amount_ex_vat,amount_vat,amount_inc_vat,pay_channel,umbrella_id
        )
        values (
          v_pbci,'DEBT_CREATED',null,null,'debt:paye','Debt created (clipped negative)',
          v_debt_scope,0,v_debt_scope,'PAYE',null
        );

        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_rows_ins_debt_items := coalesce(v_rows_ins_debt_items,0) + coalesce(v_rows,0);

        begin
          perform public._imp_debug_audit(
            p_actor_user_id,
            'PAY_CREATE_DRAFT_BATCH:DEBT_CLIP_INSERTED_PAYE',
            jsonb_build_object(
              'stage', v_stage,
              'pay_batch_id', v_batch_id::text,
              'candidate_id', v_rec.candidate_id::text,
              'pay_batch_candidate_id', v_pbci::text,
              'debt_scope', v_debt_scope,
              'row_count', v_rows
            ),
            'pay_batches',
            v_batch_id::text,
            null, null, null, null, null
          );
        exception when others then null; end;
      end if;

      update public.pay_batch_candidates pbc
      set debt_created = round(coalesce(v_debt_scope,0),2)
      where pbc.id = v_pbci;

      GET DIAGNOSTICS v_rows = ROW_COUNT;
      v_rows_upd_candidates_debt := coalesce(v_rows_upd_candidates_debt,0) + coalesce(v_rows,0);

    else
      select round(coalesce(sum(pbi.amount_inc_vat),0),2)
      into v_sum_scope
      from public.pay_batch_items pbi
      where pbi.pay_batch_candidate_id = v_pbci
        and pbi.pay_channel = 'UMBRELLA'
        and pbi.item_type <> 'DEBT_CREATED';

      v_debt_scope := round(greatest(-coalesce(v_sum_scope,0),0),2);

      begin
        perform public._imp_debug_audit(
          p_actor_user_id,
          'PAY_CREATE_DRAFT_BATCH:DEBT_CLIP_EVAL_UMBRELLA',
          jsonb_build_object(
            'stage', v_stage,
            'pay_batch_id', v_batch_id::text,
            'candidate_id', v_rec.candidate_id::text,
            'pay_batch_candidate_id', v_pbci::text,
            'sum_scope', v_sum_scope,
            'debt_scope', v_debt_scope,
            'umbrella_id', coalesce(v_rec.umbrella_id::text, null)
          ),
          'pay_batches',
          v_batch_id::text,
          null, null, null, null, null
        );
      exception when others then null; end;

      if v_debt_scope > 0 then
        insert into public.pay_batch_items(
          pay_batch_candidate_id,item_type,timesheet_id,segment_key,source_ref,description,
          amount_ex_vat,amount_vat,amount_inc_vat,pay_channel,umbrella_id
        )
        values (
          v_pbci,'DEBT_CREATED',null,null,'debt:umbrella','Debt created (clipped negative)',
          v_debt_scope,0,v_debt_scope,'UMBRELLA',v_rec.umbrella_id
        );

        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_rows_ins_debt_items := coalesce(v_rows_ins_debt_items,0) + coalesce(v_rows,0);

        begin
          perform public._imp_debug_audit(
            p_actor_user_id,
            'PAY_CREATE_DRAFT_BATCH:DEBT_CLIP_INSERTED_UMBRELLA',
            jsonb_build_object(
              'stage', v_stage,
              'pay_batch_id', v_batch_id::text,
              'candidate_id', v_rec.candidate_id::text,
              'pay_batch_candidate_id', v_pbci::text,
              'debt_scope', v_debt_scope,
              'umbrella_id', coalesce(v_rec.umbrella_id::text, null),
              'row_count', v_rows
            ),
            'pay_batches',
            v_batch_id::text,
            null, null, null, null, null
          );
        exception when others then null; end;
      end if;

      update public.pay_batch_candidates pbc
      set debt_created = round(coalesce(v_debt_scope,0),2)
      where pbc.id = v_pbci;

      GET DIAGNOSTICS v_rows = ROW_COUNT;
      v_rows_upd_candidates_debt := coalesce(v_rows_upd_candidates_debt,0) + coalesce(v_rows,0);
    end if;
  end loop;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:STAGE_17_DEBT_CLIP_SUMMARY',
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', v_batch_id::text,
        'debt_items_inserted_total', coalesce(v_rows_ins_debt_items,0),
        'candidates_updated_total', coalesce(v_rows_upd_candidates_debt,0)
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  v_stage := 'STAGE_18_POPULATE_CANDIDATE_SUMMARIES';

  -- Populate gross_preview/net_bank_amount summaries (single-channel)
  update public.pay_batch_candidates pbc
  set
    gross_preview = case
      when v_scope = 'PAYE'
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
      when v_scope = 'PAYE' then null
      else round((
        select greatest(coalesce(sum(pbi.amount_inc_vat),0),0)
        from public.pay_batch_items pbi
        where pbi.pay_batch_candidate_id = pbc.id
          and pbi.pay_channel = 'UMBRELLA'
          and pbi.item_type <> 'DEBT_CREATED'
      ),2)
    end,
    mismatch_settlement_choice = nullif(v_mismatch_choices->>pbc.candidate_id::text,'')
  where pbc.pay_batch_id = v_batch_id;

  GET DIAGNOSTICS v_rows_upd_candidates_summaries = ROW_COUNT;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:STAGE_18_SUMMARIES_UPDATED',
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', v_batch_id::text,
        'updated_candidate_rows', v_rows_upd_candidates_summaries
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  v_stage := 'STAGE_19_CREATE_TIMESHEET_SNAPSHOTS';

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
      tf.hours_day,
      tf.hours_night,
      tf.hours_sat,
      tf.hours_sun,
      tf.hours_bh,
      tf.pay_day,
      tf.pay_night,
      tf.pay_sat,
      tf.pay_sun,
      tf.pay_bh,
      tf.additional_units_json,
      tf.mileage_units,
      tf.mileage_pay_rate,
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
              'date', nullif(btrim(coalesce(seg->>'date','')),''),
              'start_utc', nullif(btrim(coalesce(seg->>'start_utc','')),''),
              'end_utc', nullif(btrim(coalesce(seg->>'end_utc','')),''),
              'break_mins', coalesce(nullif(seg->>'break_mins','')::numeric,0),
              'breaks', coalesce(seg->'breaks','[]'::jsonb),
              'hours_day', coalesce(nullif(seg->>'hours_day','')::numeric,0),
              'hours_night', coalesce(nullif(seg->>'hours_night','')::numeric,0),
              'hours_sat', coalesce(nullif(seg->>'hours_sat','')::numeric,0),
              'hours_sun', coalesce(nullif(seg->>'hours_sun','')::numeric,0),
              'hours_bh', coalesce(nullif(seg->>'hours_bh','')::numeric,0),
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
      end as cur_additional,
      coalesce(
        (
          select coalesce(
            jsonb_agg(
              jsonb_build_object(
                'id', a.id::text,
                'delta_pay_ex_vat', round(coalesce(a.delta_pay_ex_vat,0),2)
              )
              order by a.id
            ),
            '[]'::jsonb
          )
          from public.ts_pay_adjustments a
          where a.timesheet_id = t.timesheet_id
            and a.as_advance = false
        ),
        '[]'::jsonb
      ) as cur_adjs
    from tf0 t
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
    md5(s.target_snapshot_json::text),
    now()
  from (
    select
      c.timesheet_id,
      c.candidate_id,
      coalesce(c.base_json, '{}'::jsonb) as base_snapshot_json,
      jsonb_build_object(
        'segments', coalesce(c.cur_segments, '[]'::jsonb),
        'additional_pay_ex_vat', round(coalesce(c.cur_additional,0),2),
        'additional_units_json', coalesce(c.additional_units_json, '{}'::jsonb),
        'hours_day', round(coalesce(c.hours_day,0),2),
        'hours_night', round(coalesce(c.hours_night,0),2),
        'hours_sat', round(coalesce(c.hours_sat,0),2),
        'hours_sun', round(coalesce(c.hours_sun,0),2),
        'hours_bh', round(coalesce(c.hours_bh,0),2),
        'pay_day', round(coalesce(c.pay_day,0),2),
        'pay_night', round(coalesce(c.pay_night,0),2),
        'pay_sat', round(coalesce(c.pay_sat,0),2),
        'pay_sun', round(coalesce(c.pay_sun,0),2),
        'pay_bh', round(coalesce(c.pay_bh,0),2),
        'mileage_units', round(coalesce(c.mileage_units,0),2),
        'mileage_pay_rate', c.mileage_pay_rate,
        'expenses', jsonb_build_object(
          'expenses_pay_ex_vat', round(coalesce(c.expenses_pay_ex_vat,0),2),
          'travel_pay_ex_vat', round(coalesce(c.travel_pay_ex_vat,0),2),
          'accommodation_pay_ex_vat', round(coalesce(c.accommodation_pay_ex_vat,0),2),
          'other_pay_ex_vat', round(coalesce(c.other_pay_ex_vat,0),2),
          'mileage_pay_ex_vat', round(coalesce(c.mileage_pay_ex_vat,0),2)
        ),
        'adjustments', coalesce(c.cur_adjs, '[]'::jsonb)
      ) as target_snapshot_json
    from cur0 c
  ) s
  join ts_channel tc
    on tc.timesheet_id = s.timesheet_id;

  GET DIAGNOSTICS v_rows_ins_snapshots = ROW_COUNT;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:STAGE_19_SNAPSHOTS_INSERTED',
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', v_batch_id::text,
        'inserted_snapshot_rows', v_rows_ins_snapshots
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  v_stage := 'STAGE_20_BUILD_ITEM_BREAKDOWNS';

  -- ✅ NEW: Build canonical breakdown lines for ALL items in this batch (including LOAN_REPAYMENT + DEBT_CREATED)
  with my_items as (
    select
      pbi.id as pay_batch_item_id,
      pbi.item_type,
      pbi.timesheet_id,
      pbi.segment_key,
      pbi.source_ref,
      pbi.description,
      pbi.amount_ex_vat,
      pbi.amount_vat,
      pbi.amount_inc_vat,
      pbi.pay_channel
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = v_batch_id
      and not exists (
        select 1
        from public.pay_batch_item_breakdowns pbib
        where pbib.pay_batch_item_id = pbi.id
        limit 1
      )
  ),
  cur_fin as (
    select
      mi.*,
      tf.invoice_breakdown_json as cur_invoice_breakdown_json,
      tf.hours_day as cur_hours_day,
      tf.hours_night as cur_hours_night,
      tf.hours_sat as cur_hours_sat,
      tf.hours_sun as cur_hours_sun,
      tf.hours_bh as cur_hours_bh,
      tf.pay_day as cur_pay_day,
      tf.pay_night as cur_pay_night,
      tf.pay_sat as cur_pay_sat,
      tf.pay_sun as cur_pay_sun,
      tf.pay_bh as cur_pay_bh,
      tf.additional_units_json as cur_additional_units_json,
      tf.mileage_units as cur_mileage_units,
      tf.mileage_pay_rate as cur_mileage_pay_rate,
      tps.last_settled_at_utc as last_settled_at_utc
    from my_items mi
    left join public.timesheets_financials tf
      on tf.timesheet_id = mi.timesheet_id
     and tf.is_current = true
    left join public.timesheet_pay_state tps
      on tps.timesheet_id = mi.timesheet_id
  ),
  base_fin as (
    select
      cf.*,
      btf.invoice_breakdown_json as base_invoice_breakdown_json,
      btf.hours_day as base_hours_day,
      btf.hours_night as base_hours_night,
      btf.hours_sat as base_hours_sat,
      btf.hours_sun as base_hours_sun,
      btf.hours_bh as base_hours_bh,
      btf.pay_day as base_pay_day,
      btf.pay_night as base_pay_night,
      btf.pay_sat as base_pay_sat,
      btf.pay_sun as base_pay_sun,
      btf.pay_bh as base_pay_bh,
      btf.additional_units_json as base_additional_units_json,
      btf.mileage_units as base_mileage_units,
      btf.mileage_pay_rate as base_mileage_pay_rate
    from cur_fin cf
    left join lateral (
      select btf0.*
      from public.timesheets_financials btf0
      where btf0.timesheet_id = cf.timesheet_id
        and btf0.is_current = false
        and cf.last_settled_at_utc is not null
        and btf0.updated_at <= cf.last_settled_at_utc
      order by btf0.updated_at desc, btf0.id desc
      limit 1
    ) btf on true
  ),
  seg_join as (
    select
      bf.*,
      cur_seg.seg as cur_seg,
      bas_seg.seg as bas_seg
    from base_fin bf
    left join lateral (
      select s as seg
      from jsonb_array_elements(coalesce(bf.cur_invoice_breakdown_json->'segments','[]'::jsonb)) s
      where s is not null and jsonb_typeof(s)='object'
        and nullif(btrim(coalesce(s->>'segment_id','')),'') = bf.segment_key
      limit 1
    ) cur_seg on true
    left join lateral (
      select s as seg
      from jsonb_array_elements(coalesce(bf.base_invoice_breakdown_json->'segments','[]'::jsonb)) s
      where s is not null and jsonb_typeof(s)='object'
        and nullif(btrim(coalesce(s->>'segment_id','')),'') = bf.segment_key
      limit 1
    ) bas_seg on true
  ),
  bucket_src as (
    select
      sj.pay_batch_item_id,
      sj.item_type,
      sj.pay_channel,
      sj.amount_ex_vat as parent_ex,
      sj.amount_vat as parent_vat,
      sj.amount_inc_vat as parent_inc,

      sj.segment_key,

      -- payable hours: if exclude_from_pay then treat as 0
      (case when coalesce(nullif(sj.cur_seg->>'exclude_from_pay','')::boolean,false) then 0 else coalesce(nullif(sj.cur_seg->>'hours_day','')::numeric, sj.cur_hours_day, 0) end) as cur_h_day,
      (case when coalesce(nullif(sj.cur_seg->>'exclude_from_pay','')::boolean,false) then 0 else coalesce(nullif(sj.cur_seg->>'hours_night','')::numeric, sj.cur_hours_night, 0) end) as cur_h_night,
      (case when coalesce(nullif(sj.cur_seg->>'exclude_from_pay','')::boolean,false) then 0 else coalesce(nullif(sj.cur_seg->>'hours_sat','')::numeric, sj.cur_hours_sat, 0) end) as cur_h_sat,
      (case when coalesce(nullif(sj.cur_seg->>'exclude_from_pay','')::boolean,false) then 0 else coalesce(nullif(sj.cur_seg->>'hours_sun','')::numeric, sj.cur_hours_sun, 0) end) as cur_h_sun,
      (case when coalesce(nullif(sj.cur_seg->>'exclude_from_pay','')::boolean,false) then 0 else coalesce(nullif(sj.cur_seg->>'hours_bh','')::numeric, sj.cur_hours_bh, 0) end) as cur_h_bh,

      (case when coalesce(nullif(sj.bas_seg->>'exclude_from_pay','')::boolean,false) then 0 else coalesce(nullif(sj.bas_seg->>'hours_day','')::numeric, sj.base_hours_day, 0) end) as bas_h_day,
      (case when coalesce(nullif(sj.bas_seg->>'exclude_from_pay','')::boolean,false) then 0 else coalesce(nullif(sj.bas_seg->>'hours_night','')::numeric, sj.base_hours_night, 0) end) as bas_h_night,
      (case when coalesce(nullif(sj.bas_seg->>'exclude_from_pay','')::boolean,false) then 0 else coalesce(nullif(sj.bas_seg->>'hours_sat','')::numeric, sj.base_hours_sat, 0) end) as bas_h_sat,
      (case when coalesce(nullif(sj.bas_seg->>'exclude_from_pay','')::boolean,false) then 0 else coalesce(nullif(sj.bas_seg->>'hours_sun','')::numeric, sj.base_hours_sun, 0) end) as bas_h_sun,
      (case when coalesce(nullif(sj.bas_seg->>'exclude_from_pay','')::boolean,false) then 0 else coalesce(nullif(sj.bas_seg->>'hours_bh','')::numeric, sj.base_hours_bh, 0) end) as bas_h_bh,

      coalesce(sj.cur_pay_day,0) as r_day,
      coalesce(sj.cur_pay_night,0) as r_night,
      coalesce(sj.cur_pay_sat,0) as r_sat,
      coalesce(sj.cur_pay_sun,0) as r_sun,
      coalesce(sj.cur_pay_bh,0) as r_bh
    from seg_join sj
    where sj.item_type = 'SEGMENT_DELTA'
  ),
  bucket_rows as (
    select
      bs.pay_batch_item_id,
      'TS_BUCKET'::text as line_kind,
      b.bucket_code,
      b.unit_name,
      b.units,
      b.rate,
      round(b.units * b.rate, 2) as ex_calc,
      bs.parent_ex,
      bs.parent_vat,
      bs.parent_inc
    from bucket_src bs
    join lateral (
      values
        ('DAY'::text, 'Day'::text, round(bs.cur_h_day - bs.bas_h_day, 2), round(bs.r_day, 2)),
        ('NIGHT'::text, 'Night'::text, round(bs.cur_h_night - bs.bas_h_night, 2), round(bs.r_night, 2)),
        ('SAT'::text, 'Sat'::text, round(bs.cur_h_sat - bs.bas_h_sat, 2), round(bs.r_sat, 2)),
        ('SUN'::text, 'Sun'::text, round(bs.cur_h_sun - bs.bas_h_sun, 2), round(bs.r_sun, 2)),
        ('BH'::text, 'BH'::text, round(bs.cur_h_bh - bs.bas_h_bh, 2), round(bs.r_bh, 2))
    ) as b(bucket_code, unit_name, units, rate)
      on true
    where b.units <> 0
  ),
  bucket_ranked as (
    select
      br.*,
      sum(br.ex_calc) over (partition by br.pay_batch_item_id) as sum_ex,
      sum(abs(br.ex_calc)) over (partition by br.pay_batch_item_id) as sum_abs_ex,
      row_number() over (partition by br.pay_batch_item_id order by abs(br.ex_calc) desc, br.bucket_code) as rn
    from bucket_rows br
  ),
  bucket_fixed as (
    select
      br.pay_batch_item_id,
      br.line_kind,
      br.bucket_code,
      br.unit_name,
      br.units,
      br.rate,
      case
        when br.rn = 1 then round(br.parent_ex - (br.sum_ex - br.ex_calc), 2)
        else br.ex_calc
      end as amount_ex_vat,
      br.parent_vat,
      br.parent_inc,
      br.sum_abs_ex,
      br.rn
    from bucket_ranked br
  ),
  bucket_vat as (
    select
      bf.pay_batch_item_id,
      bf.line_kind,
      bf.bucket_code,
      bf.unit_name,
      bf.units,
      bf.rate,
      bf.amount_ex_vat,
      case
        when bf.parent_vat = 0 then 0::numeric
        when bf.sum_abs_ex = 0 then 0::numeric
        when bf.rn = 1 then round(bf.parent_vat - (sum(round(bf.parent_vat * (abs(bf.amount_ex_vat) / bf.sum_abs_ex), 2)) over (partition by bf.pay_batch_item_id) - round(bf.parent_vat * (abs(bf.amount_ex_vat) / bf.sum_abs_ex), 2)), 2)
        else round(bf.parent_vat * (abs(bf.amount_ex_vat) / bf.sum_abs_ex), 2)
      end as amount_vat
    from bucket_fixed bf
  ),
  bucket_final as (
    select
      bv.pay_batch_item_id,
      bv.line_kind,
      bv.bucket_code,
      bv.unit_name,
      bv.units,
      bv.rate,
      bv.amount_ex_vat,
      bv.amount_vat,
      round(bv.amount_ex_vat + bv.amount_vat, 2) as amount_inc_vat,
      '{}'::jsonb as meta_json
    from bucket_vat bv
  ),
  addl_keys as (
    select
      bf.pay_batch_item_id,
      key as code
    from base_fin bf
    join lateral (
      select key
      from jsonb_each(coalesce(bf.cur_additional_units_json,'{}'::jsonb))
      union
      select key
      from jsonb_each(coalesce(bf.base_additional_units_json,'{}'::jsonb))
    ) k on true
    where bf.item_type = 'EXPENSE_DELTA'
      and bf.source_ref = 'additional'
  ),
  addl_rows_raw as (
    select
      bf.pay_batch_item_id,
      'ADDITIONAL_UNIT'::text as line_kind,
      ak.code as bucket_code,
      coalesce(
        nullif(btrim(coalesce((bf.cur_additional_units_json->ak.code)->>'bucket_name','')),''),
        nullif(btrim(coalesce((bf.base_additional_units_json->ak.code)->>'bucket_name','')),''),
        ak.code
      ) || ' - ' ||
      coalesce(
        nullif(btrim(coalesce((bf.cur_additional_units_json->ak.code)->>'unit_name','')),''),
        nullif(btrim(coalesce((bf.base_additional_units_json->ak.code)->>'unit_name','')),''),
        ak.code
      ) as unit_name,
      round(
        coalesce(nullif((bf.cur_additional_units_json->ak.code)->>'unit_count','')::numeric,0)
        -
        coalesce(nullif((bf.base_additional_units_json->ak.code)->>'unit_count','')::numeric,0),
        2
      ) as units_delta,
      round(coalesce(nullif((bf.cur_additional_units_json->ak.code)->>'pay_rate','')::numeric,0),2) as cur_rate,
      round(coalesce(nullif((bf.base_additional_units_json->ak.code)->>'pay_rate','')::numeric,0),2) as bas_rate,
      bf.amount_ex_vat as parent_ex,
      bf.amount_vat as parent_vat,
      bf.amount_inc_vat as parent_inc
    from base_fin bf
    join addl_keys ak
      on ak.pay_batch_item_id = bf.pay_batch_item_id
  ),
  addl_lines as (
    select
      ar.pay_batch_item_id,
      ar.line_kind,
      ak.code as bucket_code,
      ar.unit_name,
      ar.units_delta as units,
      ar.cur_rate as rate,
      round(ar.units_delta * ar.cur_rate, 2) as ex_calc,
      ar.parent_ex,
      ar.parent_vat,
      ar.parent_inc,
      'HOURS'::text as component
    from addl_rows_raw ar
    join addl_keys ak
      on ak.pay_batch_item_id = ar.pay_batch_item_id
     and ak.code = ar.bucket_code
    where ar.units_delta <> 0
    union all
    select
      ar.pay_batch_item_id,
      ar.line_kind,
      ar.bucket_code,
      (ar.unit_name || ' (rate change)') as unit_name,
      round(coalesce(nullif(ar.base_units->>'unit_count','')::numeric,0),2) as units,
      round(ar.cur_rate - ar.bas_rate, 2) as rate,
      round(round(coalesce(nullif(ar.base_units->>'unit_count','')::numeric,0),2) * round(ar.cur_rate - ar.bas_rate,2),2) as ex_calc,
      ar.parent_ex,
      ar.parent_vat,
      ar.parent_inc,
      'RATE'::text as component
    from (
      select
        ar0.*,
        (bf.base_additional_units_json->ar0.bucket_code) as base_units
      from addl_rows_raw ar0
      join base_fin bf
        on bf.pay_batch_item_id = ar0.pay_batch_item_id
    ) ar
    where round(ar.cur_rate - ar.bas_rate, 2) <> 0
      and round(coalesce(nullif(ar.base_units->>'unit_count','')::numeric,0),2) <> 0
  ),
  addl_ranked as (
    select
      al.*,
      sum(al.ex_calc) over (partition by al.pay_batch_item_id) as sum_ex,
      sum(abs(al.ex_calc)) over (partition by al.pay_batch_item_id) as sum_abs_ex,
      row_number() over (partition by al.pay_batch_item_id order by abs(al.ex_calc) desc, al.bucket_code) as rn
    from addl_lines al
  ),
  addl_fixed as (
    select
      ar.pay_batch_item_id,
      ar.line_kind,
      ar.bucket_code,
      ar.unit_name,
      ar.units,
      ar.rate,
      case
        when ar.rn = 1 then round(ar.parent_ex - (ar.sum_ex - ar.ex_calc), 2)
        else ar.ex_calc
      end as amount_ex_vat,
      ar.parent_vat,
      ar.parent_inc,
      ar.sum_abs_ex,
      ar.rn,
      ar.component
    from addl_ranked ar
  ),
  addl_vat as (
    select
      af.pay_batch_item_id,
      af.line_kind,
      af.bucket_code,
      af.unit_name,
      af.units,
      af.rate,
      af.amount_ex_vat,
      case
        when af.parent_vat = 0 then 0::numeric
        when af.sum_abs_ex = 0 then 0::numeric
        when af.rn = 1 then round(af.parent_vat - (sum(round(af.parent_vat * (abs(af.amount_ex_vat) / af.sum_abs_ex), 2)) over (partition by af.pay_batch_item_id) - round(af.parent_vat * (abs(af.amount_ex_vat) / af.sum_abs_ex), 2)), 2)
        else round(af.parent_vat * (abs(af.amount_ex_vat) / af.sum_abs_ex), 2)
      end as amount_vat,
      af.component
    from addl_fixed af
  ),
  addl_final as (
    select
      av.pay_batch_item_id,
      av.line_kind,
      av.bucket_code,
      av.unit_name,
      av.units,
      av.rate,
      av.amount_ex_vat,
      av.amount_vat,
      round(av.amount_ex_vat + av.amount_vat, 2) as amount_inc_vat,
      jsonb_build_object('component', av.component) as meta_json
    from addl_vat av
  ),
  simple_lines as (
    select
      bf.pay_batch_item_id,
      case
        when bf.item_type = 'MILEAGE_DELTA' then 'MILEAGE'
        when bf.item_type = 'EXPENSE_DELTA' then 'EXPENSE'
        when bf.item_type = 'ADJUSTMENT_DELTA' then 'ADJUSTMENT'
        when bf.item_type = 'CONVERSION_ADJ' then 'CONVERSION_ADJ'
        when bf.item_type = 'LOAN_REPAYMENT' then 'LOAN_REPAYMENT'
        when bf.item_type = 'DEBT_CREATED' then 'DEBT_CREATED'
        else 'ADJUSTMENT'
      end as line_kind,
      null::text as bucket_code,
      case
        when bf.item_type = 'MILEAGE_DELTA' then 'Mileage'
        when bf.item_type = 'EXPENSE_DELTA' then initcap(coalesce(bf.source_ref,'Expense'))
        when bf.item_type = 'ADJUSTMENT_DELTA' then 'Adjustment'
        when bf.item_type = 'CONVERSION_ADJ' then 'Conversion adjustment'
        when bf.item_type = 'LOAN_REPAYMENT' then 'Loan repayment'
        when bf.item_type = 'DEBT_CREATED' then 'Debt created'
        else 'Adjustment'
      end as unit_name,
      null::numeric as units,
      null::numeric as rate,
      bf.amount_ex_vat,
      bf.amount_vat,
      bf.amount_inc_vat,
      '{}'::jsonb as meta_json
    from base_fin bf
    where not (bf.item_type = 'EXPENSE_DELTA' and bf.source_ref = 'additional')
      and bf.item_type <> 'SEGMENT_DELTA'
  ),
  all_lines as (
    select * from bucket_final
    union all
    select * from addl_final
    union all
    select * from simple_lines
  )
  insert into public.pay_batch_item_breakdowns(
    pay_batch_item_id,
    line_kind,
    bucket_code,
    unit_name,
    units,
    rate,
    amount_ex_vat,
    amount_vat,
    amount_inc_vat,
    meta_json
  )
  select
    al.pay_batch_item_id,
    al.line_kind,
    al.bucket_code,
    al.unit_name,
    al.units,
    al.rate,
    al.amount_ex_vat,
    al.amount_vat,
    al.amount_inc_vat,
    al.meta_json
  from all_lines al;

  GET DIAGNOSTICS v_rows_ins_breakdowns = ROW_COUNT;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:STAGE_20_BREAKDOWNS_INSERTED',
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', v_batch_id::text,
        'inserted_breakdown_rows', v_rows_ins_breakdowns
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  v_stage := 'STAGE_21_BREAKDOWN_INTEGRITY_MISSING';

  -- ✅ Integrity checks: every item must have ≥1 breakdown row; sums must match exactly
  select count(*)
  into v_breakdown_missing_ct
  from public.pay_batch_items pbi_m
  join public.pay_batch_candidates pbc_m
    on pbc_m.id = pbi_m.pay_batch_candidate_id
  where pbc_m.pay_batch_id = v_batch_id
    and not exists (
      select 1
      from public.pay_batch_item_breakdowns pbib_m
      where pbib_m.pay_batch_item_id = pbi_m.id
      limit 1
    );

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:STAGE_21_BREAKDOWN_MISSING_COUNT',
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', v_batch_id::text,
        'breakdown_missing_ct', coalesce(v_breakdown_missing_ct,0)
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  if coalesce(v_breakdown_missing_ct,0) > 0 then
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCH:ERROR_BREAKDOWN_MISSING',
        jsonb_build_object(
          'stage', v_stage,
          'pay_batch_id', v_batch_id::text,
          'breakdown_missing_ct', coalesce(v_breakdown_missing_ct,0),
          'error', 'PAY_BATCH_BREAKDOWN_MISSING'
        ),
        'pay_batches',
        v_batch_id::text,
        null, null, null, null, null
      );
    exception when others then null; end;

    raise exception 'PAY_BATCH_BREAKDOWN_MISSING: % items have no breakdown rows', v_breakdown_missing_ct;
  end if;

  v_stage := 'STAGE_22_BREAKDOWN_INTEGRITY_SUM_MATCH';

  with sums as (
    select
      pbi_s.id as pay_batch_item_id,
      round(pbi_s.amount_ex_vat,2) as item_ex,
      round(pbi_s.amount_vat,2) as item_vat,
      round(pbi_s.amount_inc_vat,2) as item_inc,
      round(coalesce(sum(pbib_s.amount_ex_vat),0),2) as sum_ex,
      round(coalesce(sum(pbib_s.amount_vat),0),2) as sum_vat,
      round(coalesce(sum(pbib_s.amount_inc_vat),0),2) as sum_inc
    from public.pay_batch_items pbi_s
    join public.pay_batch_candidates pbc_s
      on pbc_s.id = pbi_s.pay_batch_candidate_id
    left join public.pay_batch_item_breakdowns pbib_s
      on pbib_s.pay_batch_item_id = pbi_s.id
    where pbc_s.pay_batch_id = v_batch_id
    group by pbi_s.id, pbi_s.amount_ex_vat, pbi_s.amount_vat, pbi_s.amount_inc_vat
  ),
  bad as (
    select
      s.pay_batch_item_id,
      s.item_ex, s.sum_ex,
      s.item_vat, s.sum_vat,
      s.item_inc, s.sum_inc
    from sums s
    where s.item_ex <> s.sum_ex
       or s.item_vat <> s.sum_vat
       or s.item_inc <> s.sum_inc
  )
  select
    count(*),
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'pay_batch_item_id', b.pay_batch_item_id::text,
          'item_ex', b.item_ex, 'sum_ex', b.sum_ex,
          'item_vat', b.item_vat, 'sum_vat', b.sum_vat,
          'item_inc', b.item_inc, 'sum_inc', b.sum_inc
        )
        order by b.pay_batch_item_id::text
      ),
      '[]'::jsonb
    )
  into v_breakdown_bad_ct, v_breakdown_bad
  from bad b;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:STAGE_22_BREAKDOWN_SUMS_RESULT',
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', v_batch_id::text,
        'breakdown_bad_ct', coalesce(v_breakdown_bad_ct,0),
        'breakdown_bad_sample', (
          case
            when jsonb_typeof(v_breakdown_bad) = 'array' then (
              select coalesce(jsonb_agg(x order by (x->>'pay_batch_item_id')), '[]'::jsonb)
              from (
                select e as x
                from jsonb_array_elements(v_breakdown_bad) e
                limit 50
              ) s
            )
            else v_breakdown_bad
          end
        )
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  if coalesce(v_breakdown_bad_ct,0) > 0 then
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCH:ERROR_BREAKDOWN_MISMATCH',
        jsonb_build_object(
          'stage', v_stage,
          'pay_batch_id', v_batch_id::text,
          'breakdown_bad_ct', coalesce(v_breakdown_bad_ct,0),
          'breakdown_bad', v_breakdown_bad,
          'error', 'PAY_BATCH_BREAKDOWN_MISMATCH'
        ),
        'pay_batches',
        v_batch_id::text,
        null, null, null, null, null
      );
    exception when others then null; end;

    raise exception 'PAY_BATCH_BREAKDOWN_MISMATCH %', v_breakdown_bad::text;
  end if;

  v_stage := 'STAGE_23_FINAL_SAFETY_ASSERT_BLOCKERS';

  -- Final safety assertion: draft must not contain candidates blocked by bank readiness (for this scope).
  with cands as (
    select
      pbc.candidate_id,
      c.umbrella_id as umbrella_id,
      c.bank_details_hash as cand_bank_hash
    from public.pay_batch_candidates pbc
    join public.candidates c
      on c.id = pbc.candidate_id
    where pbc.pay_batch_id = v_batch_id
  ),
  umb as (
    select
      u.id as umbrella_id,
      coalesce(u.enabled,false) as umb_enabled,
      u.bank_details_hash as umb_bank_hash
    from public.umbrellas u
  ),
  checks as (
    select
      ca.candidate_id,
      (
        -- PAYE readiness blockers (payee is candidate)
        (case
           when v_scope = 'PAYE' and (ca.cand_bank_hash is null or btrim(ca.cand_bank_hash) = '') then jsonb_build_array('BLOCKED_BANK_DETAILS')
           else '[]'::jsonb
         end)
        ||
        (case
           when v_scope = 'PAYE'
            and v_need_name_check = true
            and ca.cand_bank_hash is not null and btrim(ca.cand_bank_hash) <> ''
            and not exists (
              select 1
              from public.bank_name_checks bnc
              where bnc.rail_provider = upper(btrim(coalesce(v_settings.rail_provider_default,'')))
                and bnc.rail_env = upper(btrim(coalesce(v_settings.rail_env_default,'')))
                and bnc.entity_kind = 'CANDIDATE'
                and bnc.entity_id = ca.candidate_id
                and bnc.bank_details_hash = ca.cand_bank_hash
                and (
                  upper(coalesce(bnc.status,'')) = 'PASS'
                  or (
                    bnc.override_reason is not null
                    and bnc.override_hash is not null
                    and bnc.override_hash = ca.cand_bank_hash
                  )
                )
              limit 1
            )
           then jsonb_build_array('BLOCKED_NAME_CHECK')
           else '[]'::jsonb
         end)
        ||
        (case
           when v_scope = 'PAYE'
            and v_requires_payee_map = true
            and ca.cand_bank_hash is not null and btrim(ca.cand_bank_hash) <> ''
            and not exists (
              select 1
              from public.bank_payee_map bpm
              where bpm.rail_provider = upper(btrim(coalesce(v_settings.rail_provider_default,'')))
                and bpm.rail_env = upper(btrim(coalesce(v_settings.rail_env_default,'')))
                and bpm.entity_kind = 'CANDIDATE'
                and bpm.entity_id = ca.candidate_id
                and bpm.bank_details_hash = ca.cand_bank_hash
              limit 1
            )
           then jsonb_build_array('BLOCKED_NO_PAYEE_MAP')
           else '[]'::jsonb
         end)
        ||
        -- UMBRELLA readiness blockers (payee is umbrella)
        (case
           when v_scope = 'UMBRELLA'
            and (
              ca.umbrella_id is null
              or coalesce(u.umb_enabled,false) = false
              or u.umb_bank_hash is null
              or btrim(u.umb_bank_hash) = ''
            )
           then jsonb_build_array('BLOCKED_BANK_DETAILS')
           else '[]'::jsonb
         end)
        ||
        (case
           when v_scope = 'UMBRELLA'
            and v_need_name_check = true
            and ca.umbrella_id is not null
            and coalesce(u.umb_enabled,false) = true
            and u.umb_bank_hash is not null and btrim(u.umb_bank_hash) <> ''
            and not exists (
              select 1
              from public.bank_name_checks bnc
              where bnc.rail_provider = upper(btrim(coalesce(v_settings.rail_provider_default,'')))
                and bnc.rail_env = upper(btrim(coalesce(v_settings.rail_env_default,'')))
                and bnc.entity_kind = 'UMBRELLA'
                and bnc.entity_id = ca.umbrella_id
                and bnc.bank_details_hash = u.umb_bank_hash
                and (
                  upper(coalesce(bnc.status,'')) = 'PASS'
                  or (
                    bnc.override_reason is not null
                    and bnc.override_hash is not null
                    and bnc.override_hash = u.umb_bank_hash
                  )
                )
              limit 1
            )
           then jsonb_build_array('BLOCKED_NAME_CHECK')
           else '[]'::jsonb
         end)
        ||
        (case
           when v_scope = 'UMBRELLA'
            and v_requires_payee_map = true
            and ca.umbrella_id is not null
            and coalesce(u.umb_enabled,false) = true
            and u.umb_bank_hash is not null and btrim(u.umb_bank_hash) <> ''
            and not exists (
              select 1
              from public.bank_payee_map bpm
              where bpm.rail_provider = upper(btrim(coalesce(v_settings.rail_provider_default,'')))
                and bpm.rail_env = upper(btrim(coalesce(v_settings.rail_env_default,'')))
                and bpm.entity_kind = 'UMBRELLA'
                and bpm.entity_id = ca.umbrella_id
                and bpm.bank_details_hash = u.umb_bank_hash
              limit 1
            )
           then jsonb_build_array('BLOCKED_NO_PAYEE_MAP')
           else '[]'::jsonb
         end)
      ) as blockers
    from cands ca
    left join umb u
      on u.umbrella_id = ca.umbrella_id
  ),
  bad as (
    select
      x.candidate_id,
      x.blockers
    from checks x
    where jsonb_array_length(x.blockers) > 0
  )
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'candidate_id', b.candidate_id::text,
        'blockers', b.blockers
      )
      order by b.candidate_id::text
    ),
    '[]'::jsonb
  )
  into v_blocked_candidates
  from bad b;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:STAGE_23_BLOCKER_ASSERT_RESULT',
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', v_batch_id::text,
        'blocked_candidates', (
          case
            when jsonb_typeof(v_blocked_candidates) = 'array' then (
              select coalesce(jsonb_agg(x order by (x->>'candidate_id')), '[]'::jsonb)
              from (
                select e as x
                from jsonb_array_elements(v_blocked_candidates) e
                limit 50
              ) s
            )
            else v_blocked_candidates
          end
        ),
        'blocked_count', jsonb_array_length(v_blocked_candidates)
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  if jsonb_array_length(v_blocked_candidates) > 0 then
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCH:ERROR_DRAFT_CONTAINS_BLOCKED_ITEMS',
        jsonb_build_object(
          'stage', v_stage,
          'pay_batch_id', v_batch_id::text,
          'blocked_candidates', v_blocked_candidates,
          'error', 'DRAFT_CONTAINS_BLOCKED_ITEMS'
        ),
        'pay_batches',
        v_batch_id::text,
        null, null, null, null, null
      );
    exception when others then null; end;

    raise exception 'DRAFT_CONTAINS_BLOCKED_ITEMS %', v_blocked_candidates::text;
  end if;

  v_stage := 'STAGE_24_RETURN';

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:STAGE_24_RETURN',
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', v_batch_id::text,
        'return', jsonb_build_object(
          'ok', true,
          'pay_batch_id', v_batch_id::text,
          'pay_date', p_pay_date::text,
          'pay_week_start', v_week_start::text,
          'week_ending_cutoff_date', p_week_ending_cutoff::text,
          'pay_channel_scope', v_scope,
          'banking_system_snapshot', v_settings.banking_system,
          'external_paye_system_snapshot', v_settings.external_paye_system,
          'rail_provider_snapshot', v_settings.rail_provider_default,
          'rail_env_snapshot', v_settings.rail_env_default
        ),
        'dml_summary', jsonb_build_object(
          'batch_insert_rowcount', v_rows,
          'candidates_inserted', v_rows_ins_candidates,
          'items_inserted', v_rows_ins_items,
          'candidates_deleted_empty', v_rows_del_candidates,
          'loan_items_inserted', coalesce(v_rows_ins_loan_items,0),
          'debt_items_inserted', coalesce(v_rows_ins_debt_items,0),
          'snapshots_inserted', v_rows_ins_snapshots,
          'breakdowns_inserted', v_rows_ins_breakdowns,
          'breakdown_missing_ct', coalesce(v_breakdown_missing_ct,0),
          'breakdown_bad_ct', coalesce(v_breakdown_bad_ct,0)
        )
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', v_batch_id::text,
    'pay_date', p_pay_date::text,
    'pay_week_start', v_week_start::text,
    'week_ending_cutoff_date', p_week_ending_cutoff::text,
    'pay_channel_scope', v_scope,
    'banking_system_snapshot', v_settings.banking_system,
    'external_paye_system_snapshot', v_settings.external_paye_system,
    'rail_provider_snapshot', v_settings.rail_provider_default,
    'rail_env_snapshot', v_settings.rail_env_default
  );

exception when others then
  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:UNHANDLED_EXCEPTION',
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', coalesce(v_batch_id::text, null),
        'pay_date', coalesce(p_pay_date::text, null),
        'week_ending_cutoff', coalesce(p_week_ending_cutoff::text, null),
        'scope', coalesce(v_scope, null),
        'candidate_filter_single', coalesce(v_candidate_filter_single::text, null),
        'client_filter_single', coalesce(v_client_filter_single::text, null),
        'candidate_ids_count', coalesce(array_length(v_candidate_ids,1),0),
        'candidate_ids_sample', (
          select coalesce(jsonb_agg(x.candidate_id::text order by x.candidate_id::text), '[]'::jsonb)
          from (
            select unnest(coalesce(v_candidate_ids, array[]::uuid[])) as candidate_id
            limit 50
          ) x
        ),
        'last_reserved_json', v_reserved,
        'blocked_candidates', v_blocked_candidates,
        'breakdown_missing_ct', coalesce(v_breakdown_missing_ct,0),
        'breakdown_bad_ct', coalesce(v_breakdown_bad_ct,0),
        'breakdown_bad', v_breakdown_bad,
        'sqlstate', sqlstate,
        'sqlerrm', sqlerrm
      ),
      'pay_batches',
      coalesce(v_batch_id::text, 'pay_date:'||coalesce(p_pay_date::text,'null')),
      null, null, null, null, null
    );
  exception when others then
    null;
  end;

  raise;
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

  -- ✅ NEW: derived batch kind + channels
  v_batch_kind text := null;
  v_pay_channels_present jsonb := '[]'::jsonb;

  -- ✅ NEW: child modal support payloads
  v_candidate_breakdown jsonb := '[]'::jsonb;
  v_candidate_lines jsonb := '[]'::jsonb;

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

  -- ✅ NEW: derive batch kind + channels from items (excludes DEBT_CREATED)
  select
    case
      when ch.channels is null then null
      when array_position(ch.channels,'PAYE') is not null and array_position(ch.channels,'UMBRELLA') is not null then 'MIXED'
      when array_position(ch.channels,'PAYE') is not null then 'PAYE'
      when array_position(ch.channels,'UMBRELLA') is not null then 'UMBRELLA'
      else null
    end as batch_kind,
    coalesce(to_jsonb(ch.channels), '[]'::jsonb) as channels_json
  into
    v_batch_kind,
    v_pay_channels_present
  from (
    select
      array_agg(distinct upper(coalesce(pbi.pay_channel,'')) order by upper(coalesce(pbi.pay_channel,'')))
        filter (where upper(coalesce(pbi.pay_channel,'')) in ('PAYE','UMBRELLA')) as channels
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = p_pay_batch_id
      and pbi.item_type <> 'DEBT_CREATED'
  ) ch;

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

  -- Items unchanged (still returned for audit/debug and UI fallback)
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
        'umbrella_id', case when pbi.umbrella_id is null then null else pbi.umbrella_id::text end,
        'pay_bank_transfer_id', case when pbi.pay_bank_transfer_id is null then null else pbi.pay_bank_transfer_id::text end,
        'repayment_week_start', case when pbi.repayment_week_start is null then null else pbi.repayment_week_start::text end
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

  -- ✅ NEW: candidate_breakdown for child modal (aggregates + status rollup)
  with cand as (
    select
      pbc.id as pay_batch_candidate_id,
      pbc.candidate_id,
      pbc.candidate_tms_ref,
      pbc.candidate_display_name,
      pbc.paye_state
    from public.pay_batch_candidates pbc
    where pbc.pay_batch_id = p_pay_batch_id
  ),
  ni as (
    select
      pbc.id as pay_batch_candidate_id,
      pni.net_amount,
      pni.source,
      pni.imported_at_utc,
      pni.file_name
    from public.pay_batch_candidates pbc
    left join lateral (
      select
        pni0.net_amount,
        pni0.source,
        pni0.imported_at_utc,
        pni0.file_name
      from public.pay_batch_paye_net_inputs pni0
      where pni0.pay_batch_candidate_id = pbc.id
      order by pni0.imported_at_utc desc
      limit 1
    ) pni on true
    where pbc.pay_batch_id = p_pay_batch_id
  ),
  sums as (
    select
      pbc.id as pay_batch_candidate_id,
      round(coalesce(sum(case when upper(coalesce(pbi.pay_channel,'')) = 'PAYE' then coalesce(pbi.amount_ex_vat,0) else 0 end) filter (where pbi.item_type <> 'DEBT_CREATED'),0),2) as paye_total_ex_vat,
      round(coalesce(sum(case when upper(coalesce(pbi.pay_channel,'')) = 'UMBRELLA' then coalesce(pbi.amount_inc_vat,0) else 0 end) filter (where pbi.item_type <> 'DEBT_CREATED'),0),2) as umbrella_total_inc_vat
    from public.pay_batch_candidates pbc
    left join public.pay_batch_items pbi
      on pbi.pay_batch_candidate_id = pbc.id
    where pbc.pay_batch_id = p_pay_batch_id
    group by pbc.id
  ),
  tr as (
    select
      pbc.id as pay_batch_candidate_id,
      count(distinct pbi.pay_bank_transfer_id) filter (
        where pbi.item_type <> 'DEBT_CREATED'
          and pbi.pay_bank_transfer_id is not null
      )::int as total_transfers,
      count(distinct pbi.pay_bank_transfer_id) filter (
        where pbi.item_type <> 'DEBT_CREATED'
          and pbi.pay_bank_transfer_id is not null
          and upper(coalesce(pbt.status,'')) = 'BLOCKED'
      )::int as blocked_transfers,
      count(distinct pbi.pay_bank_transfer_id) filter (
        where pbi.item_type <> 'DEBT_CREATED'
          and pbi.pay_bank_transfer_id is not null
          and upper(coalesce(pbt.status,'')) = 'FAILED'
      )::int as failed_transfers,
      count(distinct pbi.pay_bank_transfer_id) filter (
        where pbi.item_type <> 'DEBT_CREATED'
          and pbi.pay_bank_transfer_id is not null
          and upper(coalesce(pbt.status,'')) = 'COMPLETED'
      )::int as completed_transfers,
      count(distinct pbi.pay_bank_transfer_id) filter (
        where pbi.item_type <> 'DEBT_CREATED'
          and pbi.pay_bank_transfer_id is not null
          and upper(coalesce(pbt.status,'')) = 'PENDING'
      )::int as pending_transfers
    from public.pay_batch_candidates pbc
    left join public.pay_batch_items pbi
      on pbi.pay_batch_candidate_id = pbc.id
    left join public.pay_bank_transfers pbt
      on pbt.id = pbi.pay_bank_transfer_id
     and pbt.pay_batch_id = p_pay_batch_id
    where pbc.pay_batch_id = p_pay_batch_id
    group by pbc.id
  )
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'pay_batch_candidate_id', c.pay_batch_candidate_id::text,
        'candidate_id', c.candidate_id::text,
        'candidate_tms_ref', c.candidate_tms_ref,
        'candidate_display_name', c.candidate_display_name,
        'batch_kind', v_batch_kind,
        'paye_state', c.paye_state,
        'paye_net_amount', n.net_amount,
        'paye_net_source', n.source,
        'paye_net_imported_at_utc', n.imported_at_utc,
        'paye_net_file_name', n.file_name,
        'awaiting_net_amount',
          (case
            when v_batch_kind = 'PAYE' then (n.net_amount is null)
            else false
          end),
        'paye_total_ex_vat', s.paye_total_ex_vat,
        'umbrella_total_inc_vat', s.umbrella_total_inc_vat,
        'payment_amount',
          (case
            when v_batch_kind = 'PAYE' then s.paye_total_ex_vat
            when v_batch_kind = 'UMBRELLA' then s.umbrella_total_inc_vat
            else round(coalesce(s.paye_total_ex_vat,0) + coalesce(s.umbrella_total_inc_vat,0),2)
          end),
        'transfer_rollup', jsonb_build_object(
          'total', tr0.total_transfers,
          'pending', tr0.pending_transfers,
          'blocked', tr0.blocked_transfers,
          'failed', tr0.failed_transfers,
          'completed', tr0.completed_transfers
        ),
        'payment_status',
          (case
            when coalesce(tr0.total_transfers,0) = 0 then 'DRAFT'
            when coalesce(tr0.blocked_transfers,0) > 0 then 'BLOCKED'
            when coalesce(tr0.failed_transfers,0) > 0 then 'FAILED'
            when coalesce(tr0.completed_transfers,0) = coalesce(tr0.total_transfers,0) then 'PAID'
            else 'PENDING'
          end)
      )
      order by c.candidate_display_name nulls last, c.candidate_tms_ref nulls last, c.candidate_id
    ),
    '[]'::jsonb
  )
  into v_candidate_breakdown
  from cand c
  left join ni n
    on n.pay_batch_candidate_id = c.pay_batch_candidate_id
  left join sums s
    on s.pay_batch_candidate_id = c.pay_batch_candidate_id
  left join tr tr0
    on tr0.pay_batch_candidate_id = c.pay_batch_candidate_id;

  -- ✅ UPDATED: candidate_lines for expandable table
  -- HARD RULE: do NOT derive units/rate from total_hours; use canonical pay_batch_item_breakdowns.
  with pbci as (
    select
      pbc.id as pay_batch_candidate_id,
      pbc.candidate_id,
      pbc.candidate_display_name,
      pbc.candidate_tms_ref
    from public.pay_batch_candidates pbc
    where pbc.pay_batch_id = p_pay_batch_id
  ),
  ts_groups as (
    select
      pbc.id as pay_batch_candidate_id,
      pbc.candidate_id as candidate_id,
      pbi.timesheet_id as timesheet_id,
      round(coalesce(sum(case when upper(coalesce(pbi.pay_channel,''))='PAYE' then coalesce(pbi.amount_ex_vat,0) else 0 end) filter (where pbi.item_type <> 'DEBT_CREATED'),0),2) as subtotal_paye_ex_vat,
      round(coalesce(sum(case when upper(coalesce(pbi.pay_channel,''))='UMBRELLA' then coalesce(pbi.amount_inc_vat,0) else 0 end) filter (where pbi.item_type <> 'DEBT_CREATED'),0),2) as subtotal_umbrella_inc_vat
    from public.pay_batch_candidates pbc
    join public.pay_batch_items pbi
      on pbi.pay_batch_candidate_id = pbc.id
    where pbc.pay_batch_id = p_pay_batch_id
      and pbi.timesheet_id is not null
    group by pbc.id, pbc.candidate_id, pbi.timesheet_id
  ),
  ts_enriched as (
    select
      tg.pay_batch_candidate_id,
      tg.candidate_id,
      tg.timesheet_id,
      vts.week_ending_date,
      vts.client_id,
      vts.client_name,
      nullif(btrim(coalesce(vts.hospital_norm,'')), '') as hospital_norm,
      tg.subtotal_paye_ex_vat,
      tg.subtotal_umbrella_inc_vat,
      case
        when v_batch_kind = 'PAYE' then tg.subtotal_paye_ex_vat
        when v_batch_kind = 'UMBRELLA' then tg.subtotal_umbrella_inc_vat
        else round(coalesce(tg.subtotal_paye_ex_vat,0) + coalesce(tg.subtotal_umbrella_inc_vat,0),2)
      end as payment_amount
    from ts_groups tg
    left join public.v_timesheets_summary_base vts
      on vts.timesheet_id = tg.timesheet_id
  ),
  ts_breakdown_rows as (
    select
      pbc.id as pay_batch_candidate_id,
      pbc.candidate_id as candidate_id,
      pbi.timesheet_id as timesheet_id,
      pbib.line_kind as line_kind,
      pbib.bucket_code as bucket_code,
      pbib.unit_name as unit_name,
      pbib.rate as rate,
      case
        when bool_and(pbib.units is null) then null::numeric
        else round(sum(coalesce(pbib.units,0)),2)
      end as units,
      round(coalesce(sum(coalesce(pbib.amount_ex_vat,0)),0),2) as amount_ex_vat,
      round(coalesce(sum(coalesce(pbib.amount_vat,0)),0),2) as amount_vat,
      round(coalesce(sum(coalesce(pbib.amount_inc_vat,0)),0),2) as amount_inc_vat
    from public.pay_batch_candidates pbc
    join public.pay_batch_items pbi
      on pbi.pay_batch_candidate_id = pbc.id
    join public.pay_batch_item_breakdowns pbib
      on pbib.pay_batch_item_id = pbi.id
    where pbc.pay_batch_id = p_pay_batch_id
      and pbi.timesheet_id is not null
    group by
      pbc.id,
      pbc.candidate_id,
      pbi.timesheet_id,
      pbib.line_kind,
      pbib.bucket_code,
      pbib.unit_name,
      pbib.rate
    having
      round(coalesce(sum(coalesce(pbib.amount_ex_vat,0)),0),2) <> 0
      or round(coalesce(sum(coalesce(pbib.amount_inc_vat,0)),0),2) <> 0
      or (case when bool_and(pbib.units is null) then 0 else round(sum(coalesce(pbib.units,0)),2) end) <> 0
  ),
  ts_breakdown_json as (
    select
      tbr.pay_batch_candidate_id,
      tbr.candidate_id,
      tbr.timesheet_id,
      coalesce(
        jsonb_agg(
          jsonb_build_object(
            'line_kind', tbr.line_kind,
            'bucket_code', tbr.bucket_code,
            'unit_name', tbr.unit_name,
            'units', tbr.units,
            'rate', tbr.rate,
            'amount_ex_vat', tbr.amount_ex_vat,
            'amount_vat', tbr.amount_vat,
            'amount_inc_vat', tbr.amount_inc_vat
          )
          order by
            case upper(coalesce(tbr.line_kind,''))
              when 'TS_BUCKET' then 1
              when 'ADDITIONAL_UNIT' then 2
              when 'MILEAGE' then 3
              when 'EXPENSE' then 4
              when 'ADJUSTMENT' then 5
              when 'CONVERSION_ADJ' then 6
              when 'LOAN_REPAYMENT' then 7
              when 'DEBT_CREATED' then 8
              else 99
            end,
            case upper(coalesce(tbr.bucket_code,''))
              when 'DAY' then 1
              when 'NIGHT' then 2
              when 'SAT' then 3
              when 'SUN' then 4
              when 'BH' then 5
              else 99
            end,
            coalesce(tbr.unit_name,'') asc,
            coalesce(tbr.rate,0) asc
        ),
        '[]'::jsonb
      ) as breakdown_lines
    from ts_breakdown_rows tbr
    group by tbr.pay_batch_candidate_id, tbr.candidate_id, tbr.timesheet_id
  ),
  ts_lines as (
    select
      te.pay_batch_candidate_id,
      te.candidate_id,
      jsonb_build_object(
        'timesheet_id', te.timesheet_id::text,
        'week_ending_date', case when te.week_ending_date is null then null else te.week_ending_date::text end,
        'client_id', case when te.client_id is null then null else te.client_id::text end,
        'client_name', te.client_name,
        'hospital_norm', te.hospital_norm,
        'subtotal_paye_ex_vat', te.subtotal_paye_ex_vat,
        'subtotal_umbrella_inc_vat', te.subtotal_umbrella_inc_vat,
        'payment_amount', te.payment_amount,
        'breakdown_lines', coalesce(tbj.breakdown_lines, '[]'::jsonb)
      ) as ts_line
    from ts_enriched te
    left join ts_breakdown_json tbj
      on tbj.pay_batch_candidate_id = te.pay_batch_candidate_id
     and tbj.timesheet_id = te.timesheet_id
  ),
  non_ts_groups as (
    select
      pbc.id as pay_batch_candidate_id,
      pbc.candidate_id as candidate_id,
      pbi.item_type as item_type,
      pbi.source_ref as source_ref,
      pbi.repayment_week_start as repayment_week_start,
      max(pbi.description) as description,
      round(coalesce(sum(case when upper(coalesce(pbi.pay_channel,''))='PAYE' then coalesce(pbi.amount_ex_vat,0) else 0 end),0),2) as subtotal_paye_ex_vat,
      round(coalesce(sum(case when upper(coalesce(pbi.pay_channel,''))='UMBRELLA' then coalesce(pbi.amount_inc_vat,0) else 0 end),0),2) as subtotal_umbrella_inc_vat
    from public.pay_batch_candidates pbc
    join public.pay_batch_items pbi
      on pbi.pay_batch_candidate_id = pbc.id
    where pbc.pay_batch_id = p_pay_batch_id
      and pbi.timesheet_id is null
    group by pbc.id, pbc.candidate_id, pbi.item_type, pbi.source_ref, pbi.repayment_week_start
  ),
  non_ts_breakdown_rows as (
    select
      pbc.id as pay_batch_candidate_id,
      pbc.candidate_id as candidate_id,
      pbi.item_type as item_type,
      pbi.source_ref as source_ref,
      pbi.repayment_week_start as repayment_week_start,
      pbib.line_kind as line_kind,
      pbib.bucket_code as bucket_code,
      pbib.unit_name as unit_name,
      pbib.rate as rate,
      case
        when bool_and(pbib.units is null) then null::numeric
        else round(sum(coalesce(pbib.units,0)),2)
      end as units,
      round(coalesce(sum(coalesce(pbib.amount_ex_vat,0)),0),2) as amount_ex_vat,
      round(coalesce(sum(coalesce(pbib.amount_vat,0)),0),2) as amount_vat,
      round(coalesce(sum(coalesce(pbib.amount_inc_vat,0)),0),2) as amount_inc_vat
    from public.pay_batch_candidates pbc
    join public.pay_batch_items pbi
      on pbi.pay_batch_candidate_id = pbc.id
    join public.pay_batch_item_breakdowns pbib
      on pbib.pay_batch_item_id = pbi.id
    where pbc.pay_batch_id = p_pay_batch_id
      and pbi.timesheet_id is null
    group by
      pbc.id,
      pbc.candidate_id,
      pbi.item_type,
      pbi.source_ref,
      pbi.repayment_week_start,
      pbib.line_kind,
      pbib.bucket_code,
      pbib.unit_name,
      pbib.rate
  ),
  non_ts_breakdown_json as (
    select
      ntr.pay_batch_candidate_id,
      ntr.candidate_id,
      ntr.item_type,
      ntr.source_ref,
      ntr.repayment_week_start,
      coalesce(
        jsonb_agg(
          jsonb_build_object(
            'line_kind', ntr.line_kind,
            'bucket_code', ntr.bucket_code,
            'unit_name', ntr.unit_name,
            'units', ntr.units,
            'rate', ntr.rate,
            'amount_ex_vat', ntr.amount_ex_vat,
            'amount_vat', ntr.amount_vat,
            'amount_inc_vat', ntr.amount_inc_vat
          )
          order by
            case upper(coalesce(ntr.line_kind,''))
              when 'TS_BUCKET' then 1
              when 'ADDITIONAL_UNIT' then 2
              when 'MILEAGE' then 3
              when 'EXPENSE' then 4
              when 'ADJUSTMENT' then 5
              when 'CONVERSION_ADJ' then 6
              when 'LOAN_REPAYMENT' then 7
              when 'DEBT_CREATED' then 8
              else 99
            end,
            coalesce(ntr.unit_name,'') asc
        ),
        '[]'::jsonb
      ) as breakdown_lines
    from non_ts_breakdown_rows ntr
    group by ntr.pay_batch_candidate_id, ntr.candidate_id, ntr.item_type, ntr.source_ref, ntr.repayment_week_start
  ),
  non_ts_lines as (
    select
      ng.pay_batch_candidate_id,
      ng.candidate_id,
      jsonb_build_object(
        'item_type', ng.item_type,
        'source_ref', ng.source_ref,
        'repayment_week_start', case when ng.repayment_week_start is null then null else ng.repayment_week_start::text end,
        'week_ending_date', case when ng.repayment_week_start is null then null else (ng.repayment_week_start + 6)::text end,
        'description', ng.description,
        'subtotal_paye_ex_vat', ng.subtotal_paye_ex_vat,
        'subtotal_umbrella_inc_vat', ng.subtotal_umbrella_inc_vat,
        'payment_amount',
          case
            when v_batch_kind = 'PAYE' then ng.subtotal_paye_ex_vat
            when v_batch_kind = 'UMBRELLA' then ng.subtotal_umbrella_inc_vat
            else round(coalesce(ng.subtotal_paye_ex_vat,0) + coalesce(ng.subtotal_umbrella_inc_vat,0),2)
          end,
        'breakdown_lines', coalesce(ntbj.breakdown_lines, '[]'::jsonb)
      ) as non_ts_line
    from non_ts_groups ng
    left join non_ts_breakdown_json ntbj
      on ntbj.pay_batch_candidate_id = ng.pay_batch_candidate_id
     and ntbj.item_type = ng.item_type
     and ntbj.source_ref is not distinct from ng.source_ref
     and ntbj.repayment_week_start is not distinct from ng.repayment_week_start
  ),
  cand_lines as (
    select
      p.pay_batch_candidate_id,
      p.candidate_id,
      p.candidate_display_name,
      p.candidate_tms_ref,
      coalesce(
        jsonb_agg(ts.ts_line order by (ts.ts_line->>'week_ending_date')::date desc nulls last, ts.ts_line->>'timesheet_id')
          filter (where ts.ts_line is not null),
        '[]'::jsonb
      ) as ts_lines,
      coalesce(
        jsonb_agg(nt.non_ts_line order by nt.non_ts_line->>'item_type', nt.non_ts_line->>'source_ref')
          filter (where nt.non_ts_line is not null),
        '[]'::jsonb
      ) as non_ts_lines
    from pbci p
    left join ts_lines ts
      on ts.pay_batch_candidate_id = p.pay_batch_candidate_id
    left join non_ts_lines nt
      on nt.pay_batch_candidate_id = p.pay_batch_candidate_id
    group by p.pay_batch_candidate_id, p.candidate_id, p.candidate_display_name, p.candidate_tms_ref
  )
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'pay_batch_candidate_id', cl.pay_batch_candidate_id::text,
        'candidate_id', cl.candidate_id::text,
        'candidate_display_name', cl.candidate_display_name,
        'candidate_tms_ref', cl.candidate_tms_ref,
        'ts_lines', cl.ts_lines,
        'non_ts_lines', cl.non_ts_lines
      )
      order by cl.candidate_display_name nulls last, cl.candidate_tms_ref nulls last, cl.candidate_id
    ),
    '[]'::jsonb
  )
  into v_candidate_lines
  from cand_lines cl;

  return jsonb_build_object(
    'ok', true,

    -- ✅ NEW: batch kind + channels for UI
    'batch_kind', v_batch_kind,
    'pay_channels_present', v_pay_channels_present,

    -- ✅ child modal datasets
    'candidate_breakdown', v_candidate_breakdown,
    'candidate_lines', v_candidate_lines,

    -- schedule recommendations for UI preselect (includes default funding account)
    'schedule_recommendations', jsonb_build_object(
      'default_schedule_umbrella_local', v_default_schedule_umbrella_local,
      'default_schedule_paye_local', v_default_schedule_paye_local,
      'funds_warning_hours_json', v_funds_warning_hours_json,
      'rail_default_funding_account_ref', v_rail_default_funding_account_ref
    ),

    -- authorisation summary for Banking UI
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

    -- ✅ Defensive validation: latest net amount must be >= 0 and exactly 2dp
    if exists (
      select 1
      from public.pay_batch_candidates pbc_chk3
      left join lateral (
        select pni_chk3.net_amount
        from public.pay_batch_paye_net_inputs pni_chk3
        where pni_chk3.pay_batch_candidate_id = pbc_chk3.id
        order by pni_chk3.imported_at_utc desc
        limit 1
      ) ni_chk3 on true
      where pbc_chk3.pay_batch_id = p_pay_batch_id
        and pbc_chk3.paye_state is not null
        and ni_chk3.net_amount is not null
        and (
          ni_chk3.net_amount < 0
          or ni_chk3.net_amount <> round(ni_chk3.net_amount, 2)
        )
      limit 1
    ) then
      raise exception 'PAYE_NET_INVALID: net pay must be >= 0 and 2dp';
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

  truncate table _tmp_pay_transfer_groups;

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
        nullif(min(pbi0.umbrella_id::text), '')::uuid as umb_id,
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

