create or replace function public.pay_search(
  p_query text,
  p_limit int default 20,
  p_offset int default 0
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_q text := nullif(btrim(coalesce(p_query, '')), '');
  v_q_digits text := null;
  v_limit int := greatest(coalesce(p_limit, 20), 0);
  v_offset int := greatest(coalesce(p_offset, 0), 0);

  v_is_digits7 boolean := false;
  v_is_digits15 boolean := false;
  v_is_digits8 boolean := false;

  v_digits7_int int := null;
  v_amount_query numeric := null;

  v_batches jsonb := '[]'::jsonb;
  v_matches jsonb := '[]'::jsonb;
begin
  if v_q is null then
    return jsonb_build_object(
      'query', coalesce(p_query, ''),
      'query_digits', '',
      'batches', '[]'::jsonb,
      'matches', '[]'::jsonb
    );
  end if;

  v_q_digits := regexp_replace(v_q, '[^0-9]', '', 'g');

  v_is_digits7 := (length(v_q_digits) = 7);
  v_is_digits15 := (length(v_q_digits) = 15);
  v_is_digits8 := (length(v_q_digits) = 8);

  if v_is_digits7 then
    begin
      v_digits7_int := v_q_digits::int;
    exception when others then
      v_digits7_int := null;
    end;
  end if;

  -- numeric amount search (optional): allow "123" or "123.45" (strip commas/spaces)
  begin
    if regexp_replace(v_q, '[ ,]', '', 'g') ~ '^[0-9]+(\.[0-9]{1,2})?$' then
      v_amount_query := regexp_replace(v_q, '[ ,]', '', 'g')::numeric;
    end if;
  exception when others then
    v_amount_query := null;
  end;

  -- ---------------------------------------------------------
  -- BATCH matches
  -- ---------------------------------------------------------
  with batches_raw as (
    select
      pb.id as batch_id,
      pb.pay_date,
      pb.bulk_reference,
      pb.bulk_ref_num,
      pb.bulk_ref_date,
      pb.status,
      pb.total_bank_out,
      pb.total_debt_created
    from public.pay_batches pb
    where
      (
        -- 7 digits -> bulk_ref_num
        (v_is_digits7 and v_digits7_int is not null and pb.bulk_ref_num = v_digits7_int)
        or
        -- 15 digits -> bulk_reference (support digits-only or hyphenated storage)
        (v_is_digits15 and pb.bulk_reference is not null
          and (
            pb.bulk_reference = v_q_digits
            or regexp_replace(pb.bulk_reference, '[^0-9]', '', 'g') = v_q_digits
          )
        )
        or
        -- 8 digits -> DDMMYYYY match against bulk_ref_date or pay_date
        (v_is_digits8 and (
          to_char(pb.pay_date, 'DDMMYYYY') = v_q_digits
          or (pb.bulk_ref_date is not null and to_char(pb.bulk_ref_date, 'DDMMYYYY') = v_q_digits)
        ))
        or
        -- ISO date
        (v_q ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' and pb.pay_date = v_q::date)
        or
        -- free-text match against bulk_reference (if present)
        (not v_is_digits7 and not v_is_digits15 and not v_is_digits8
          and pb.bulk_reference is not null and pb.bulk_reference ilike ('%' || v_q || '%')
        )
      )
    order by pb.pay_date desc, pb.created_at_utc desc, pb.id desc
    offset v_offset
    limit v_limit
  )
  select coalesce(
    jsonb_agg(jsonb_build_object(
      'batch_id', br.batch_id,
      'pay_date', br.pay_date,
      'bulk_reference', br.bulk_reference,
      'status', br.status,
      'total_bank_out', br.total_bank_out,
      'total_debt_created', br.total_debt_created
    )),
    '[]'::jsonb
  )
  into v_batches
  from batches_raw br;

  -- ---------------------------------------------------------
  -- TRANSFER matches (and link back to batch bulk ref)
  -- ---------------------------------------------------------
  with matches_raw as (
    select
      pbt.id as transfer_id,
      pbt.pay_batch_id,
      pb.pay_date,
      pb.bulk_reference,
      pbt.pay_channel,
      pbt.amount,
      pbt.currency,
      pbt.status as transfer_status,
      pbt.payment_reference,

      pbt.candidate_id,
      c.display_name as candidate_display_name,
      c.first_name as candidate_first_name,
      c.last_name as candidate_last_name,

      pbt.umbrella_id,
      u.name as umbrella_name
    from public.pay_bank_transfers pbt
    join public.pay_batches pb
      on pb.id = pbt.pay_batch_id
    left join public.candidates c
      on c.id = pbt.candidate_id
    left join public.umbrellas u
      on u.id = pbt.umbrella_id
    where
      (
        -- 7 digits -> match batch bulk_ref_num
        (v_is_digits7 and v_digits7_int is not null and pb.bulk_ref_num = v_digits7_int)
        or
        -- 15 digits -> match batch bulk_reference (digits-only or hyphenated)
        (v_is_digits15 and pb.bulk_reference is not null
          and (
            pb.bulk_reference = v_q_digits
            or regexp_replace(pb.bulk_reference, '[^0-9]', '', 'g') = v_q_digits
          )
        )
        or
        -- 8 digits -> DDMMYYYY match against bulk_ref_date or pay_date
        (v_is_digits8 and (
          to_char(pb.pay_date, 'DDMMYYYY') = v_q_digits
          or (pb.bulk_ref_date is not null and to_char(pb.bulk_ref_date, 'DDMMYYYY') = v_q_digits)
        ))
        or
        -- ISO date
        (v_q ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' and pb.pay_date = v_q::date)
        or
        -- free-text against payment_reference, candidate, umbrella, or bulk_reference
        (
          not v_is_digits7 and not v_is_digits15 and not v_is_digits8 and
          (
            (pbt.payment_reference is not null and pbt.payment_reference ilike ('%' || v_q || '%'))
            or
            (pb.bulk_reference is not null and pb.bulk_reference ilike ('%' || v_q || '%'))
            or
            (u.name is not null and u.name ilike ('%' || v_q || '%'))
            or
            (c.display_name is not null and c.display_name ilike ('%' || v_q || '%'))
            or
            (trim(concat_ws(' ', c.first_name, c.last_name)) <> '' and trim(concat_ws(' ', c.first_name, c.last_name)) ilike ('%' || v_q || '%'))
            or
            (v_amount_query is not null and pbt.amount = v_amount_query)
          )
        )
      )
    order by pb.pay_date desc, pbt.id desc
    offset v_offset
    limit v_limit
  )
  select coalesce(
    jsonb_agg(jsonb_build_object(
      'transfer_id', mr.transfer_id,
      'pay_batch_id', mr.pay_batch_id,
      'pay_date', mr.pay_date,
      'bulk_reference', mr.bulk_reference,
      'pay_channel', mr.pay_channel,
      'amount', mr.amount,
      'currency', mr.currency,
      'transfer_status', mr.transfer_status,
      'payment_reference', mr.payment_reference,
      'candidate_id', mr.candidate_id,
      'candidate_name', coalesce(mr.candidate_display_name, nullif(btrim(concat_ws(' ', mr.candidate_first_name, mr.candidate_last_name)), '')),
      'umbrella_id', mr.umbrella_id,
      'umbrella_name', mr.umbrella_name
    )),
    '[]'::jsonb
  )
  into v_matches
  from matches_raw mr;

  return jsonb_build_object(
    'query', v_q,
    'query_digits', coalesce(v_q_digits, ''),
    'batches', v_batches,
    'matches', v_matches
  );
end;
$$;

create or replace function public.pay_export_bank_csv(
  p_pay_batch_id uuid,
  p_scope text default 'ALL'
)
returns text
language plpgsql
security definer
set search_path = public
as $$
declare
  v_scope text := upper(btrim(coalesce(p_scope, 'ALL')));
  v_header text := 'Payment reference,Payee name,Sort code,Bank account number,Bank account type,Amount';
  v_body text := '';
  v_csv text := '';
  v_missing_count int := 0;
  v_row_count int := 0;
begin
  if v_scope not in ('ALL','PAYE','UMBRELLA') then
    raise exception 'pay_export_bank_csv: invalid scope "%". Expected ALL|PAYE|UMBRELLA.', v_scope;
  end if;

  if not exists (select 1 from public.pay_batches pb where pb.id = p_pay_batch_id) then
    raise exception 'pay_export_bank_csv: pay batch % not found.', p_pay_batch_id;
  end if;

  -- Ensure required snapshot fields exist for all *pending* transfers in scope.
  select count(*)
  into v_missing_count
  from public.pay_bank_transfers pbt
  where pbt.pay_batch_id = p_pay_batch_id
    and (v_scope = 'ALL' or pbt.pay_channel = v_scope)
    and pbt.status = 'PENDING'
    and (
      pbt.payment_reference is null
      or pbt.payee_name is null
      or pbt.sort_code is null
      or pbt.account_number is null
      or pbt.account_type is null
    );

  if v_missing_count > 0 then
    raise exception 'pay_export_bank_csv: % pending transfer(s) missing required snapshot fields (payment_reference/payee_name/sort_code/account_number/account_type). Execute-bank must populate these first.', v_missing_count;
  end if;

  -- If no rows, raise to avoid silent "empty file".
  select count(*)
  into v_row_count
  from public.pay_bank_transfers pbt
  where pbt.pay_batch_id = p_pay_batch_id
    and (v_scope = 'ALL' or pbt.pay_channel = v_scope)
    and pbt.status = 'PENDING';

  if v_row_count = 0 then
    raise exception 'pay_export_bank_csv: no pending transfers found for batch % (scope=%).', p_pay_batch_id, v_scope;
  end if;

  with rows as (
    select
      pbt.id as transfer_id,
      pbt.payment_reference,
      pbt.payee_name,
      pbt.sort_code,
      pbt.account_number,
      pbt.account_type,
      pbt.amount
    from public.pay_bank_transfers pbt
    where pbt.pay_batch_id = p_pay_batch_id
      and (v_scope = 'ALL' or pbt.pay_channel = v_scope)
      and pbt.status = 'PENDING'
    order by pbt.id
  ),
  lines as (
    select
      (
        -- CSV escaping: quote when contains comma/quote/newline, double quotes inside.
        (case when r.payment_reference ~ '[,"\r\n]' then '"' || replace(r.payment_reference, '"', '""') || '"' else r.payment_reference end) || ',' ||
        (case when r.payee_name ~ '[,"\r\n]' then '"' || replace(r.payee_name, '"', '""') || '"' else r.payee_name end) || ',' ||
        (case when r.sort_code ~ '[,"\r\n]' then '"' || replace(r.sort_code, '"', '""') || '"' else r.sort_code end) || ',' ||
        (case when r.account_number ~ '[,"\r\n]' then '"' || replace(r.account_number, '"', '""') || '"' else r.account_number end) || ',' ||
        (case when r.account_type ~ '[,"\r\n]' then '"' || replace(r.account_type, '"', '""') || '"' else r.account_type end) || ',' ||
        to_char(r.amount, 'FM9999999990.00')
      ) as line
    from rows r
  )
  select string_agg(l.line, E'\n')
  into v_body
  from lines l;

  v_csv := v_header || E'\n' || coalesce(v_body, '');
  return v_csv;
end;
$$;

create or replace function public.weekly_import_apply_cancellations(
  p_import_id uuid,
  p_actions jsonb,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_actions jsonb := coalesce(p_actions, '[]'::jsonb);

  v_cancelled int := 0;
  v_blocked int := 0;

  v_blocked_items jsonb := '[]'::jsonb;

  v_idx int;
  v_item jsonb;

  v_shift_id_text text;
  v_shift_id uuid;
  v_reason text;
  v_force boolean;

  v_timesheet_id uuid;

  v_invoice_id uuid;
  v_invoice_status text;

  v_tf_locked_by_invoice_id uuid;
  v_tf_paid_at_utc timestamptz;
begin
  -- basic validation
  if jsonb_typeof(v_actions) <> 'array' then
    raise exception 'weekly_import_apply_cancellations: p_actions must be a JSON array.';
  end if;

  if not exists (select 1 from public.hr_imports hi where hi.id = p_import_id) then
    raise exception 'weekly_import_apply_cancellations: import % not found in hr_imports.', p_import_id;
  end if;

  -- iterate with ordinality for better error messages
  for v_idx, v_item in
    select (e.ord)::int, e.value
    from jsonb_array_elements(v_actions) with ordinality as e(value, ord)
  loop
    v_shift_id_text := nullif(btrim(v_item->>'shift_id'), '');
    v_reason := nullif(btrim(v_item->>'reason'), '');
    if v_reason is null then
      raise exception 'weekly_import_apply_cancellations: item % missing "reason".', v_idx;
    end if;

    -- force must be boolean if present; default false
    if (v_item ? 'force') then
      begin
        v_force := (v_item->>'force')::boolean;
      exception when others then
        raise exception 'weekly_import_apply_cancellations: item % has invalid "force" value (must be true/false).', v_idx;
      end;
    else
      v_force := false;
    end if;

    if v_shift_id_text is null then
      raise exception 'weekly_import_apply_cancellations: item % missing "shift_id".', v_idx;
    end if;

    begin
      v_shift_id := v_shift_id_text::uuid;
    exception when invalid_text_representation then
      raise exception 'weekly_import_apply_cancellations: item % has invalid shift_id "%".', v_idx, v_shift_id_text;
    end;

    -- lock the shift row
    select
      ns.timesheet_id,
      ns.invoice_id,
      ns.invoice_status
    into
      v_timesheet_id,
      v_invoice_id,
      v_invoice_status
    from public.nhsp_shifts ns
    where ns.id = v_shift_id
    for update;

    if not found then
      raise exception 'weekly_import_apply_cancellations: item % shift % not found in nhsp_shifts.', v_idx, v_shift_id;
    end if;

    -- gate invoiced shifts unless force
    if v_invoice_id is not null and not v_force then
      v_blocked := v_blocked + 1;
      v_blocked_items := v_blocked_items || jsonb_build_object(
        'shift_id', v_shift_id,
        'blocked_reason', 'SHIFT_INVOICED',
        'invoice_id', v_invoice_id,
        'invoice_status', v_invoice_status
      );
      continue;
    end if;

    -- gate locked/paid timesheets unless force
    if v_timesheet_id is not null then
      select
        tf.locked_by_invoice_id,
        tf.paid_at_utc
      into
        v_tf_locked_by_invoice_id,
        v_tf_paid_at_utc
      from public.timesheets_financials tf
      where tf.timesheet_id = v_timesheet_id
        and tf.is_current = true
      limit 1;

      if (v_tf_locked_by_invoice_id is not null or v_tf_paid_at_utc is not null) and not v_force then
        v_blocked := v_blocked + 1;
        v_blocked_items := v_blocked_items || jsonb_build_object(
          'shift_id', v_shift_id,
          'blocked_reason', 'TIMESHEET_LOCKED_OR_PAID',
          'timesheet_id', v_timesheet_id,
          'locked_by_invoice_id', v_tf_locked_by_invoice_id,
          'paid_at_utc', v_tf_paid_at_utc
        );
        continue;
      end if;
    end if;

    -- apply cancellation + detach from timesheet
    update public.nhsp_shifts ns
    set
      cancelled_at_utc = now(),
      cancelled_by_import_id = p_import_id,
      cancelled_reason = v_reason,
      timesheet_id = null
    where ns.id = v_shift_id;

    v_cancelled := v_cancelled + 1;
  end loop;

  return jsonb_build_object(
    'import_id', p_import_id,
    'cancelled_count', v_cancelled,
    'blocked_count', v_blocked,
    'blocked_items', v_blocked_items
  );
end;
$$;


