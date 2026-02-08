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
  v_now timestamptz := now();

  v_actions jsonb := coalesce(p_actions, '[]'::jsonb);
  v_cancelled_count int := 0;

  v_import_source_system text;
  v_import_client_id uuid;

  v_idx int;
  v_item jsonb;

  v_shift_id_text text;
  v_shift_id uuid;
  v_reason text;

  -- shift fields
  v_timesheet_id uuid;
  v_shift_invoice_id uuid;

  v_shift_source_system text;
  v_shift_candidate_id uuid;
  v_shift_client_id uuid;
  v_shift_contract_id uuid;
  v_shift_work_date date;
  v_shift_cancelled_at timestamptz;

  v_shift_external_row_key text;
  v_shift_hr_request_id text;
  v_shift_request_norm text;

  v_shift_start_utc timestamptz;
  v_shift_end_utc timestamptz;
  v_shift_break_mins int;
  v_shift_ward text;
  v_shift_week_ending_date date;

  -- ✅ evidence pointer: shift.latest_import_id (may be overridden by anchor evidence)
  v_shift_latest_import_id uuid;

  -- file request-id set
  v_file_request_count int := 0;
  v_present_in_file boolean := false;

  -- invoiced-at-all detection (segment-level)
  v_tf_locked_by_invoice_id uuid;
  v_tf_invoice_breakdown_json jsonb;

  v_seg_json jsonb := null;
  v_seg_invoice_id uuid;
  v_invoice_id_detected uuid := null;
  v_invoiced_detected boolean := false;

  v_branch text := null;

  -- correction timesheet creation
  v_base_ts_week_ending date;
  v_contract_week_ending_weekday_snapshot int := 0;
  v_week_ending_date date;

  v_contract_display_site text;
  v_contract_ward_hint text;
  v_contract_role text;

  v_client_name text;
  v_candidate_display_name text;
  v_candidate_tms_ref text;

  v_correction_id text;
  v_kind text := 'CANCEL_SHIFT_REVERSAL';

  v_shift_label text;
  v_shift_label_norm text;

  v_booking_base text;
  v_booking_id text;
  v_hash_hex text;

  v_schedule jsonb;
  v_hint jsonb;

  v_base_week_id uuid;

  v_existing_ts_id uuid;
  v_existing_cw_id uuid;

  v_cw_id uuid;
  v_next_additional_seq int;
  v_try int;

  v_ts_id uuid;
  v_correction_ts_id uuid;

  -- fnv1a32 helper vars (deterministic correction_id)
  v_fnv_h bigint;
  v_fnv_i int;
  v_fnv_s text;
  v_fnv_hex text;

  -- return arrays
  v_timesheet_ids uuid[] := array[]::uuid[];
  v_invoice_ids uuid[] := array[]::uuid[];
  v_credit_note_ids uuid[] := array[]::uuid[];
  v_pdf_jobs_enqueued int := 0;

  -- debug sample
  v_sample jsonb := '[]'::jsonb;
  v_sample_n int := 0;
  v_last_shift_id uuid := null;

  v_sqlstate text;
  v_err text;

  -- ✅ Cancellation anchor (what we reverse)
  v_anchor_start_utc timestamptz := null;
  v_anchor_end_utc timestamptz := null;
  v_anchor_break_mins int := 0;
  v_anchor_import_id uuid := null;

  -- ✅ Detect invoiced replacement (POS) to decide anchor
  v_pos_ts_id uuid := null;
  v_pos_schedule jsonb := null;
  v_pos_tf_locked_by_invoice_id uuid := null;
  v_pos_tf_invoice_breakdown_json jsonb := null;
  v_pos_seg_invoice_id uuid := null;
  v_pos_is_invoiced boolean := false;

  -- ✅ Base evidence import via existing CHANGED_HOURS_REVERSAL schedule (when POS is NOT invoiced)
  v_base_evidence_import_id uuid := null;

  -- ✅ Cleanup: remove uninvoiced CHANGED_HOURS corrections when cancelling (POS not invoiced)
  v_cleanup_ts_ids uuid[] := array[]::uuid[];
  v_cleanup_count int := 0;
begin
  -- Validate import exists and is HEALTHROSTER + has client_id (Guard B)
  select
    upper(coalesce(hi.source_system::text, '')),
    hi.client_id
  into
    v_import_source_system,
    v_import_client_id
  from public.hr_imports hi
  where hi.id = p_import_id
  limit 1;

  if v_import_source_system is null or v_import_source_system = '' then
    raise exception 'weekly_import_apply_cancellations: import % not found in hr_imports.', p_import_id;
  end if;

  if v_import_source_system <> 'HEALTHROSTER' then
    raise exception 'weekly_import_apply_cancellations: import % source_system=%; expected HEALTHROSTER.', p_import_id, v_import_source_system;
  end if;

  if v_import_client_id is null then
    raise exception 'weekly_import_apply_cancellations: import % has null client_id (cannot apply HR cancellations safely).', p_import_id;
  end if;

  -- Validate actions payload
  if jsonb_typeof(v_actions) <> 'array' then
    raise exception 'weekly_import_apply_cancellations: p_actions must be a JSON array.';
  end if;

  if jsonb_array_length(v_actions) = 0 then
    return jsonb_build_object(
      'import_id', p_import_id,
      'cancelled_count', 0,
      'affected_timesheet_ids', to_jsonb(array[]::uuid[]),
      'affected_invoice_ids', to_jsonb(array[]::uuid[]),
      'credit_note_ids_created', to_jsonb(array[]::uuid[]),
      'invoice_pdf_jobs_enqueued', 0
    );
  end if;

  -- Build file request-id set (identity key = HR Request ID)
  create temporary table tmp_file_request_set(
    req_norm text primary key,
    req_raw  text
  ) on commit drop;

  insert into tmp_file_request_set(req_norm, req_raw)
  select
    lower(regexp_replace(btrim(src.req_raw), '\s+', ' ', 'g')) as req_norm,
    src.req_raw as req_raw
  from (
    select distinct
      nullif(
        btrim(
          coalesce(
            nullif(r.hr_request_id, ''),
            nullif(r.payload_json->>'request_id','')
          )
        ),
        ''
      ) as req_raw
    from public.hr_rows r
    where r.import_id = p_import_id
  ) as src
  where src.req_raw is not null
  on conflict (req_norm) do nothing;

  select count(*)::int
  into v_file_request_count
  from tmp_file_request_set;

  -- Apply is transactional: any invalid selected row fails whole apply.
  for v_idx, v_item in
    select (e.ord)::int, e.value
    from jsonb_array_elements(v_actions) with ordinality as e(value, ord)
  loop
    -- reset per-item
    v_anchor_start_utc := null;
    v_anchor_end_utc := null;
    v_anchor_break_mins := 0;
    v_anchor_import_id := null;

    v_pos_ts_id := null;
    v_pos_schedule := null;
    v_pos_tf_locked_by_invoice_id := null;
    v_pos_tf_invoice_breakdown_json := null;
    v_pos_seg_invoice_id := null;
    v_pos_is_invoiced := false;

    v_base_evidence_import_id := null;

    v_cleanup_ts_ids := array[]::uuid[];
    v_cleanup_count := 0;

    v_seg_json := null;
    v_seg_invoice_id := null;

    -- Policy: no force
    if (v_item ? 'force') then
      raise exception 'weekly_import_apply_cancellations: item % contains disallowed field "force" (policy forbids force/override).', v_idx;
    end if;

    v_shift_id_text := nullif(btrim(coalesce(v_item->>'shift_id','')), '');
    v_reason := nullif(btrim(coalesce(v_item->>'reason','')), '');

    if v_shift_id_text is null then
      raise exception 'weekly_import_apply_cancellations: item % missing "shift_id".', v_idx;
    end if;
    if v_reason is null then
      raise exception 'weekly_import_apply_cancellations: item % missing "reason".', v_idx;
    end if;

    begin
      v_shift_id := v_shift_id_text::uuid;
    exception when invalid_text_representation then
      raise exception 'weekly_import_apply_cancellations: item % has invalid shift_id "%".', v_idx, v_shift_id_text;
    end;

    v_last_shift_id := v_shift_id;

    -- Lock shift row + load required fields
    select
      ns.timesheet_id,
      ns.invoice_id,
      upper(coalesce(ns.source_system::text,'')) as shift_source_system,
      ns.candidate_id,
      ns.client_id,
      ns.contract_id,
      ns.work_date,
      ns.cancelled_at_utc,
      ns.external_row_key,
      ns.hr_request_id,
      ns.start_utc,
      ns.end_utc,
      ns.break_mins,
      ns.ward,
      ns.week_ending_date,
      ns.latest_import_id
    into
      v_timesheet_id,
      v_shift_invoice_id,
      v_shift_source_system,
      v_shift_candidate_id,
      v_shift_client_id,
      v_shift_contract_id,
      v_shift_work_date,
      v_shift_cancelled_at,
      v_shift_external_row_key,
      v_shift_hr_request_id,
      v_shift_start_utc,
      v_shift_end_utc,
      v_shift_break_mins,
      v_shift_ward,
      v_shift_week_ending_date,
      v_shift_latest_import_id
    from public.nhsp_shifts ns
    where ns.id = v_shift_id
    for update;

    if not found then
      raise exception 'weekly_import_apply_cancellations: item % shift % not found in nhsp_shifts.', v_idx, v_shift_id;
    end if;

    if v_shift_cancelled_at is not null then
      raise exception 'weekly_import_apply_cancellations: item % shift % is already cancelled (cancelled_at_utc not null).', v_idx, v_shift_id;
    end if;

    -- Guard: cancellations RPC only operates on HEALTHROSTER shifts
    if v_shift_source_system <> 'HEALTHROSTER' then
      raise exception 'weekly_import_apply_cancellations: item % shift % source_system=%; expected HEALTHROSTER.',
        v_idx, v_shift_id, v_shift_source_system;
    end if;

    -- Guard B: shift must belong to the import client
    if v_shift_client_id is null or v_shift_client_id <> v_import_client_id then
      raise exception 'weekly_import_apply_cancellations: item % shift % client_id mismatch (import_client_id=% shift_client_id=%).',
        v_idx, v_shift_id, v_import_client_id, v_shift_client_id;
    end if;

    if v_shift_contract_id is null then
      raise exception 'weekly_import_apply_cancellations: item % shift % missing contract_id.', v_idx, v_shift_id;
    end if;

    if v_shift_candidate_id is null then
      raise exception 'weekly_import_apply_cancellations: item % shift % missing candidate_id.', v_idx, v_shift_id;
    end if;

    if v_shift_work_date is null then
      raise exception 'weekly_import_apply_cancellations: item % shift % missing work_date.', v_idx, v_shift_id;
    end if;

    -- Guard A: require non-empty nhsp_shifts.hr_request_id
    if nullif(btrim(coalesce(v_shift_hr_request_id,'')), '') is null then
      raise exception
        'weekly_import_apply_cancellations: item % shift % has empty hr_request_id; cannot use request-id cancellation identity.',
        v_idx, v_shift_id;
    end if;

    v_shift_request_norm := lower(regexp_replace(btrim(v_shift_hr_request_id), '\s+', ' ', 'g'));

    -- Presence test: if request id is present in file, cancellation is not eligible
    select exists (
      select 1
      from tmp_file_request_set fr
      where fr.req_norm = v_shift_request_norm
    )
    into v_present_in_file;

    if v_present_in_file then
      raise exception
        'weekly_import_apply_cancellations: item % shift % hr_request_id is present in the import file; cancellation rejected (not missing).',
        v_idx, v_shift_id;
    end if;

    -- Derive week_ending_date for cleanup/pos lookup
    v_week_ending_date := v_shift_week_ending_date;
    if v_week_ending_date is null then
      select coalesce(c.week_ending_weekday_snapshot, 0)
      into v_contract_week_ending_weekday_snapshot
      from public.contracts c
      where c.id = v_shift_contract_id
      limit 1;

      v_week_ending_date :=
        (v_shift_work_date + (((v_contract_week_ending_weekday_snapshot - extract(dow from v_shift_work_date)::int + 7) % 7))::int)::date;
    end if;

    if v_week_ending_date is null then
      raise exception 'weekly_import_apply_cancellations: shift % cannot resolve week_ending_date.', v_shift_id;
    end if;

    -- ─────────────────────────────────────────────
    -- Invoiced-at-all detection (segment-level authoritative)
    -- Also capture matched segment JSON for anchor when POS not invoiced
    -- ─────────────────────────────────────────────
    v_tf_locked_by_invoice_id := null;
    v_tf_invoice_breakdown_json := null;
    v_seg_invoice_id := null;
    v_invoice_id_detected := null;
    v_invoiced_detected := false;

    if v_timesheet_id is not null then
      select
        tf.locked_by_invoice_id,
        tf.invoice_breakdown_json
      into
        v_tf_locked_by_invoice_id,
        v_tf_invoice_breakdown_json
      from public.timesheets_financials tf
      where tf.timesheet_id = v_timesheet_id
        and tf.is_current = true
      limit 1;

      begin
        select s2.seg
        into v_seg_json
        from (
          select s2.seg
          from jsonb_array_elements(
            case
              when v_tf_invoice_breakdown_json is not null
               and jsonb_typeof(v_tf_invoice_breakdown_json) = 'object'
               and upper(coalesce(v_tf_invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
               and jsonb_typeof(v_tf_invoice_breakdown_json->'segments') = 'array'
              then v_tf_invoice_breakdown_json->'segments'
              else '[]'::jsonb
            end
          ) as s2(seg)
          where
            (s2.seg->>'nhsp_shift_id') = v_shift_id::text
            or (
              v_shift_external_row_key is not null
              and (s2.seg->>'external_row_key') = v_shift_external_row_key
            )
          order by
            case when (s2.seg->>'nhsp_shift_id') = v_shift_id::text then 0 else 1 end
          limit 1
        ) as s2;
      exception when others then
        v_seg_json := null;
      end;

      if v_seg_json is not null then
        begin
          v_seg_invoice_id := nullif(btrim(coalesce(v_seg_json->>'invoice_locked_invoice_id','')), '')::uuid;
        exception when others then
          v_seg_invoice_id := null;
        end;
      end if;
    end if;

    v_invoice_id_detected := coalesce(v_seg_invoice_id, v_tf_locked_by_invoice_id, v_shift_invoice_id);
    v_invoiced_detected := (v_invoice_id_detected is not null);

    if v_invoice_id_detected is not null then
      v_invoice_ids := array_append(v_invoice_ids, v_invoice_id_detected);
    end if;

    -- ─────────────────────────────────────────────
    -- Branch: INPLACE vs CORRECTION
    -- ─────────────────────────────────────────────
    if v_invoiced_detected = false then
      v_branch := 'INPLACE';

      -- Cancel truth + detach
      update public.nhsp_shifts ns2
      set
        cancelled_at_utc = v_now,
        cancelled_by_import_id = p_import_id,
        cancelled_reason = v_reason,
        timesheet_id = null
      where ns2.id = v_shift_id;

      v_cancelled_count := v_cancelled_count + 1;

      -- TSFIN recompute required for base timesheet
      if v_timesheet_id is not null then
        v_timesheet_ids := array_append(v_timesheet_ids, v_timesheet_id);

        update public.timesheets_financials tfu
        set
          is_stale = true,
          stale_reason = 'IMPORT_CANCEL_DETACH',
          updated_at = v_now
        where tfu.is_current = true
          and tfu.timesheet_id = v_timesheet_id;

        perform public.enqueue_ts_financials_priority(array[v_timesheet_id]::uuid[], 'CONTEXT_CHANGED'::public.ts_fin_reason_enum);
      end if;

      v_correction_ts_id := null;

    else
      v_branch := 'CORRECTION';

      -- ✅ Determine cancellation anchor:
      -- Default anchor = current shift truth (fallback only)
      v_anchor_start_utc := v_shift_start_utc;
      v_anchor_end_utc := v_shift_end_utc;
      v_anchor_break_mins := greatest(0, coalesce(v_shift_break_mins, 0));
      v_anchor_import_id := v_shift_latest_import_id;

      -- Find latest POS (replacement) correction timesheet for this shift/week
      begin
        select
          tpos.timesheet_id,
          tpos.actual_schedule_json
        into
          v_pos_ts_id,
          v_pos_schedule
        from public.timesheets tpos
        where tpos.is_adjustment is true
          and tpos.is_current is true
          and tpos.correction_kind = 'CHANGED_HOURS_REPLACEMENT'
          and tpos.contract_id = v_shift_contract_id
          and tpos.week_ending_date = v_week_ending_date
          and jsonb_typeof(tpos.actual_schedule_json) = 'array'
          and (
            tpos.actual_schedule_json @> jsonb_build_array(jsonb_build_object('shift_id', v_shift_id::text))
            or (
              v_shift_external_row_key is not null
              and tpos.actual_schedule_json @> jsonb_build_array(jsonb_build_object('external_row_key', v_shift_external_row_key))
            )
          )
        order by tpos.updated_at desc nulls last, tpos.created_at desc nulls last
        limit 1
        for update;
      exception when others then
        v_pos_ts_id := null;
        v_pos_schedule := null;
      end;

      if v_pos_ts_id is not null then
        select
          tf.locked_by_invoice_id,
          tf.invoice_breakdown_json
        into
          v_pos_tf_locked_by_invoice_id,
          v_pos_tf_invoice_breakdown_json
        from public.timesheets_financials tf
        where tf.timesheet_id = v_pos_ts_id
          and tf.is_current = true
        order by tf.created_at desc
        limit 1;

        begin
          select
            nullif(btrim(coalesce(s2.seg->>'invoice_locked_invoice_id','')), '')::uuid
          into v_pos_seg_invoice_id
          from (
            select s2.seg
            from jsonb_array_elements(
              case
                when v_pos_tf_invoice_breakdown_json is not null
                 and jsonb_typeof(v_pos_tf_invoice_breakdown_json)='object'
                 and jsonb_typeof(v_pos_tf_invoice_breakdown_json->'segments')='array'
                then v_pos_tf_invoice_breakdown_json->'segments'
                else '[]'::jsonb
              end
            ) s2(seg)
            where nullif(btrim(coalesce(s2.seg->>'invoice_locked_invoice_id','')), '') is not null
            limit 1
          ) as s2;
        exception when others then
          v_pos_seg_invoice_id := null;
        end;

        v_pos_is_invoiced :=
          (v_pos_tf_locked_by_invoice_id is not null)
          or (v_pos_seg_invoice_id is not null);

        -- If POS is invoiced, reverse POS (anchor = POS schedule)
        if v_pos_is_invoiced is true and v_pos_schedule is not null and jsonb_typeof(v_pos_schedule) = 'array' then
          begin
            v_anchor_start_utc := nullif(btrim(coalesce((v_pos_schedule->0)->>'start_utc','')), '')::timestamptz;
          exception when others then
            v_anchor_start_utc := v_shift_start_utc;
          end;

          begin
            v_anchor_end_utc := nullif(btrim(coalesce((v_pos_schedule->0)->>'end_utc','')), '')::timestamptz;
          exception when others then
            v_anchor_end_utc := v_shift_end_utc;
          end;

          begin
            v_anchor_break_mins := greatest(
              0,
              coalesce(nullif(btrim(coalesce((v_pos_schedule->0)->>'break_mins','')), '')::int, 0)
            );
          exception when others then
            v_anchor_break_mins := greatest(0, coalesce(v_shift_break_mins, 0));
          end;

          begin
            if ((v_pos_schedule->0) ? 'import_id')
              and nullif(btrim(coalesce((v_pos_schedule->0)->>'import_id','')), '') is not null
              and (v_pos_schedule->0)->>'import_id' ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            then
              v_anchor_import_id := ((v_pos_schedule->0)->>'import_id')::uuid;
            end if;
          exception when others then
            null;
          end;
        end if;
      end if;

      -- If POS is NOT invoiced, anchor to base locked segment (and use base evidence import_id when available)
      if v_pos_is_invoiced is not true then
        -- Base evidence import id: from CHANGED_HOURS_REVERSAL schedule if present
        begin
          select
            nullif(btrim(coalesce((tneg.actual_schedule_json->0)->>'import_id','')), '')::uuid
          into v_base_evidence_import_id
          from public.timesheets tneg
          where tneg.is_adjustment is true
            and tneg.is_current is true
            and tneg.correction_kind = 'CHANGED_HOURS_REVERSAL'
            and tneg.contract_id = v_shift_contract_id
            and tneg.week_ending_date = v_week_ending_date
            and jsonb_typeof(tneg.actual_schedule_json)='array'
            and (
              tneg.actual_schedule_json @> jsonb_build_array(jsonb_build_object('shift_id', v_shift_id::text))
              or (
                v_shift_external_row_key is not null
                and tneg.actual_schedule_json @> jsonb_build_array(jsonb_build_object('external_row_key', v_shift_external_row_key))
              )
            )
          order by tneg.updated_at desc nulls last, tneg.created_at desc nulls last
          limit 1;
        exception when others then
          v_base_evidence_import_id := null;
        end;

        if v_base_evidence_import_id is not null then
          v_anchor_import_id := v_base_evidence_import_id;
        end if;

        if v_seg_json is not null then
          begin
            if nullif(btrim(coalesce(v_seg_json->>'start_utc','')), '') is not null then
              v_anchor_start_utc := (v_seg_json->>'start_utc')::timestamptz;
            end if;
          exception when others then
            null;
          end;

          begin
            if nullif(btrim(coalesce(v_seg_json->>'end_utc','')), '') is not null then
              v_anchor_end_utc := (v_seg_json->>'end_utc')::timestamptz;
            end if;
          exception when others then
            null;
          end;

          begin
            if nullif(btrim(coalesce(v_seg_json->>'break_mins','')), '') is not null then
              v_anchor_break_mins := greatest(0, (v_seg_json->>'break_mins')::int);
            end if;
          exception when others then
            null;
          end;
        end if;
      end if;

      if v_anchor_start_utc is null or v_anchor_end_utc is null then
        raise exception 'weekly_import_apply_cancellations: shift % missing anchor start/end; cannot create schedule-driven cancellation correction.', v_shift_id;
      end if;

      -- Deterministic correction id (fnv1a32 over stable string using anchor times)
      v_fnv_s :=
        coalesce(p_import_id::text,'') || '|' ||
        coalesce(v_shift_id::text,'') || '|' ||
        coalesce(v_shift_hr_request_id,'') || '|' ||
        coalesce(coalesce(v_shift_external_row_key,''),'') || '|' ||
        coalesce(coalesce(v_anchor_start_utc::text,''),'') || '|' ||
        coalesce(coalesce(v_anchor_end_utc::text,''),'') || '|' ||
        coalesce(coalesce(v_anchor_break_mins,0)::text,'');

      v_fnv_h := 2166136261;
      for v_fnv_i in 1..char_length(v_fnv_s) loop
        v_fnv_h := (v_fnv_h # ascii(substring(v_fnv_s from v_fnv_i for 1)));
        v_fnv_h := (v_fnv_h * 16777619) % 4294967296;
      end loop;
      v_fnv_hex := lpad(lower(to_hex(v_fnv_h)), 8, '0');

      v_correction_id := 'hrcan:' || p_import_id::text || ':' || v_shift_id::text || ':' || v_fnv_hex;

      -- Load contract display fields (best-effort; may be null)
      select
        c2.display_site,
        c2.ward_hint,
        c2.role
      into
        v_contract_display_site,
        v_contract_ward_hint,
        v_contract_role
      from public.contracts c2
      where c2.id = v_shift_contract_id
      limit 1;

      select cl.name
      into v_client_name
      from public.clients cl
      where cl.id = v_shift_client_id
      limit 1;

      select cand.display_name, cand.tms_ref
      into v_candidate_display_name, v_candidate_tms_ref
      from public.candidates cand
      where cand.id = v_shift_candidate_id
      limit 1;

      -- Schedule entry (anchor-based) + evidence import_id
      v_schedule := jsonb_build_array(
        jsonb_build_object(
          'date', v_shift_work_date::text,
          'ward', nullif(btrim(coalesce(v_shift_ward, v_contract_ward_hint, '')), ''),
          'start_utc', v_anchor_start_utc::text,
          'end_utc', v_anchor_end_utc::text,
          'start', to_char((v_anchor_start_utc at time zone 'Europe/London')::time, 'HH24:MI'),
          'end', to_char((v_anchor_end_utc at time zone 'Europe/London')::time, 'HH24:MI'),
          'break_mins', greatest(0, coalesce(v_anchor_break_mins, 0)),
          'hr_request_id', nullif(btrim(coalesce(v_shift_hr_request_id,'')), ''),
          'ref_num', nullif(btrim(coalesce(v_shift_hr_request_id,'')), ''),
          'shift_id', v_shift_id::text,
          'external_row_key', v_shift_external_row_key,
          'import_id', case when v_anchor_import_id is null then null else v_anchor_import_id::text end
        )
      );

      v_hint := jsonb_build_object(
        'import_cancellation', jsonb_build_object(
          'import_id', p_import_id::text,
          'trigger_import_id', p_import_id::text,
          'evidence_import_id', case when v_anchor_import_id is null then null else v_anchor_import_id::text end,
          'key_type', 'HR_REQUEST_ID',
          'hr_request_id', nullif(btrim(coalesce(v_shift_hr_request_id,'')), ''),
          'ref_num', nullif(btrim(coalesce(v_shift_hr_request_id,'')), ''),
          'external_row_key', v_shift_external_row_key,
          'shift_id', v_shift_id::text,
          'correction_id', v_correction_id,
          'correction_kind', v_kind,
          'invoice_id_detected', case when v_invoice_id_detected is null then null else v_invoice_id_detected::text end,
          'anchor_mode', case when v_pos_is_invoiced is true then 'INVOICED_REPLACEMENT' else 'BASE_LOCKED' end
        )
      );

      v_shift_label := 'weekly-hr-cancel-reversal-' || v_correction_id;

      v_shift_label_norm :=
        regexp_replace(
          regexp_replace(lower(trim(v_shift_label)), '\s+', ' ', 'g'),
          '[^\w\s\-@&\/,:]',
          '',
          'g'
        );

      v_booking_base :=
        'scope=WEEKLY' || '|' ||
        'contract_id=' || coalesce(v_shift_contract_id::text,'') || '|' ||
        'candidate_id=' || coalesce(v_shift_candidate_id::text,'') || '|' ||
        'client_id=' || coalesce(v_shift_client_id::text,'') || '|' ||
        'week_ending_date=' || coalesce(v_week_ending_date::text,'') || '|' ||
        'correction_id=' || coalesce(v_correction_id,'') || '|' ||
        'correction_kind=' || v_kind;

      v_hash_hex := substring(encode(extensions.digest(convert_to(v_booking_base, 'utf8'), 'sha256'::text), 'hex') from 1 for 16);
      v_booking_id := 'bk_' || v_hash_hex;

      -- Ensure base contract_week exists (seq=0). Do not overwrite if it exists.
      insert into public.contract_weeks(
        contract_id,
        week_ending_date,
        additional_seq,
        status,
        submission_mode_snapshot,
        timesheet_id,
        planned_schedule_json,
        created_at,
        updated_at,
        is_adjustment
      )
      values (
        v_shift_contract_id,
        v_week_ending_date,
        0,
        'OPEN'::public.contract_week_status_enum,
        'MANUAL'::public.submission_mode_enum,
        null,
        null,
        v_now,
        v_now,
        false
      )
      on conflict (contract_id, week_ending_date, additional_seq) do nothing;

      select cw0.id
      into v_base_week_id
      from public.contract_weeks cw0
      where cw0.contract_id = v_shift_contract_id
        and cw0.week_ending_date = v_week_ending_date
        and cw0.additional_seq = 0
      limit 1
      for update;

      if v_base_week_id is null then
        raise exception 'weekly_import_apply_cancellations: failed to ensure base contract_week exists (contract_id=% week_ending=%).',
          v_shift_contract_id, v_week_ending_date;
      end if;

      -- Idempotency: reuse existing correction timesheet (correction_id+kind)
      v_existing_ts_id := null;

      select t2.timesheet_id
      into v_existing_ts_id
      from public.timesheets t2
      where t2.correction_id = v_correction_id
        and t2.correction_kind = v_kind
      order by t2.is_current desc, t2.version desc
      limit 1
      for update;

      if v_existing_ts_id is not null then
        v_correction_ts_id := v_existing_ts_id;

        -- Ensure adjustment contract_week exists and links to the correction timesheet
        v_existing_cw_id := null;

        select cw2.id
        into v_existing_cw_id
        from public.contract_weeks cw2
        where cw2.timesheet_id = v_existing_ts_id
          and cw2.contract_id = v_shift_contract_id
          and cw2.week_ending_date = v_week_ending_date
        limit 1
        for update;

        if v_existing_cw_id is null then
          perform 1
          from public.contract_weeks cwlock
          where cwlock.contract_id = v_shift_contract_id
            and cwlock.week_ending_date = v_week_ending_date
          for update;

          v_try := 0;
          loop
            v_try := v_try + 1;
            if v_try > 10 then
              raise exception 'weekly_import_apply_cancellations: failed to allocate additional_seq after retries (contract_id=% week_ending=%).',
                v_shift_contract_id, v_week_ending_date;
            end if;

            select coalesce(max(cwmax.additional_seq), 0) + 1
            into v_next_additional_seq
            from public.contract_weeks cwmax
            where cwmax.contract_id = v_shift_contract_id
              and cwmax.week_ending_date = v_week_ending_date;

            begin
              insert into public.contract_weeks(
                contract_id,
                week_ending_date,
                additional_seq,
                is_adjustment,
                submission_mode_snapshot,
                status,
                created_at,
                updated_at,
                timesheet_id
              )
              values (
                v_shift_contract_id,
                v_week_ending_date,
                v_next_additional_seq,
                true,
                'MANUAL'::public.submission_mode_enum,
                'SUBMITTED'::public.contract_week_status_enum,
                v_now,
                v_now,
                v_existing_ts_id
              )
              returning id into v_existing_cw_id;

              exit;
            exception when unique_violation then
              v_existing_cw_id := null;
            end;
          end loop;
        end if;

        -- Update existing correction timesheet
        update public.timesheets tu
        set
          booking_id = v_booking_id,
          version = 1,
          is_current = true,
          status = 'RECEIVED'::public.timesheet_status_enum,
          sheet_scope = 'WEEKLY'::public.timesheet_scope_enum,
          submission_mode = 'MANUAL'::public.submission_mode_enum,
          line_type = 'HOURS'::public.timesheet_line_type_enum,
          authorised_at_server = null,
          occupant_key_norm = lower(coalesce(v_candidate_tms_ref, v_candidate_display_name, v_shift_candidate_id::text)),
          hospital_norm = lower(coalesce(v_contract_display_site, v_client_name, v_shift_client_id::text)),
          ward_norm = lower(coalesce(v_contract_ward_hint, 'contract')),
          job_title_norm = lower(coalesce(v_contract_role, 'weekly')),
          shift_label_norm = v_shift_label_norm,
          week_ending_date = v_week_ending_date,
          contract_id = v_shift_contract_id,
          manual_pdf_r2_key = null,
          actual_schedule_json = v_schedule,
          qr_payload_json = v_hint,
          candidate_hint_text = v_hint,
          is_adjustment = true,
          correction_id = v_correction_id,
          correction_kind = v_kind,
          adjustment_origin = 'IMPORT_CANCELLATION',
          updated_at = v_now
        where tu.timesheet_id = v_existing_ts_id;

      else
        -- Create a new adjustment contract_week and new correction timesheet linked to it
        perform 1
        from public.contract_weeks cwlock2
        where cwlock2.contract_id = v_shift_contract_id
          and cwlock2.week_ending_date = v_week_ending_date
        for update;

        v_try := 0;
        loop
          v_try := v_try + 1;
          if v_try > 10 then
            raise exception 'weekly_import_apply_cancellations: failed to allocate additional_seq after retries (contract_id=% week_ending=%).',
              v_shift_contract_id, v_week_ending_date;
          end if;

          select coalesce(max(cwmax2.additional_seq), 0) + 1
          into v_next_additional_seq
          from public.contract_weeks cwmax2
          where cwmax2.contract_id = v_shift_contract_id
            and cwmax2.week_ending_date = v_week_ending_date;

          begin
            insert into public.contract_weeks(
              contract_id,
              week_ending_date,
              additional_seq,
              is_adjustment,
              submission_mode_snapshot,
              status,
              created_at,
              updated_at
            )
            values (
              v_shift_contract_id,
              v_week_ending_date,
              v_next_additional_seq,
              true,
              'MANUAL'::public.submission_mode_enum,
              'SUBMITTED'::public.contract_week_status_enum,
              v_now,
              v_now
            )
            returning id into v_cw_id;

            exit;
          exception when unique_violation then
            v_cw_id := null;
          end;
        end loop;

        v_ts_id := null;

        insert into public.timesheets(
          booking_id,
          version,
          is_current,
          status,
          occupant_key_norm,
          hospital_norm,
          ward_norm,
          job_title_norm,
          shift_label_norm,
          week_ending_date,
          contract_id,
          submission_mode,
          manual_pdf_r2_key,
          line_type,
          sheet_scope,
          actual_schedule_json,
          additional_units_week,
          additional_units_per_day,
          day_references_json,
          authorised_at_server,
          qr_payload_json,
          is_adjustment,
          candidate_hint_text,
          correction_id,
          correction_kind,
          adjustment_origin,
          created_at,
          updated_at
        )
        values (
          v_booking_id,
          1,
          true,
          'RECEIVED'::public.timesheet_status_enum,
          lower(coalesce(v_candidate_tms_ref, v_candidate_display_name, v_shift_candidate_id::text)),
          lower(coalesce(v_contract_display_site, v_client_name, v_shift_client_id::text)),
          lower(coalesce(v_contract_ward_hint, 'contract')),
          lower(coalesce(v_contract_role, 'weekly')),
          v_shift_label_norm,
          v_week_ending_date,
          v_shift_contract_id,
          'MANUAL'::public.submission_mode_enum,
          null,
          'HOURS'::public.timesheet_line_type_enum,
          'WEEKLY'::public.timesheet_scope_enum,
          v_schedule,
          '{}'::jsonb,
          '{}'::jsonb,
          null,
          null,
          v_hint,
          true,
          v_hint,
          v_correction_id,
          v_kind,
          'IMPORT_CANCELLATION',
          v_now,
          v_now
        )
        returning timesheet_id into v_ts_id;

        v_correction_ts_id := v_ts_id;

        update public.contract_weeks cwlink
        set
          timesheet_id = v_correction_ts_id,
          status = 'SUBMITTED'::public.contract_week_status_enum,
          submission_mode_snapshot = 'MANUAL'::public.submission_mode_enum,
          is_adjustment = true,
          updated_at = v_now
        where cwlink.id = v_cw_id;

      end if;

      -- Enqueue TSFIN for correction timesheet only
      if v_correction_ts_id is not null then
        v_timesheet_ids := array_append(v_timesheet_ids, v_correction_ts_id);
        perform public.enqueue_ts_financials_priority(array[v_correction_ts_id]::uuid[], 'CONTEXT_CHANGED'::public.ts_fin_reason_enum);
      end if;

      -- Update truth (cancel) + detach; do NOT recompute base TSFIN here
      update public.nhsp_shifts ns3
      set
        cancelled_at_utc = v_now,
        cancelled_by_import_id = p_import_id,
        cancelled_reason = v_reason,
        timesheet_id = null
      where ns3.id = v_shift_id;

      v_cancelled_count := v_cancelled_count + 1;

      -- ✅ Cleanup: if POS is NOT invoiced, delete any uninvoiced CHANGED_HOURS corrections for this shift/week
      begin
        create temporary table tmp_cleanup_candidates(timesheet_id uuid primary key) on commit drop;

        insert into tmp_cleanup_candidates(timesheet_id)
        select distinct t.timesheet_id
        from public.timesheets t
        where t.is_adjustment is true
          and t.is_current is true
          and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
          and t.contract_id = v_shift_contract_id
          and t.week_ending_date = v_week_ending_date
          and jsonb_typeof(t.actual_schedule_json)='array'
          and (
            t.actual_schedule_json @> jsonb_build_array(jsonb_build_object('shift_id', v_shift_id::text))
            or (
              v_shift_external_row_key is not null
              and t.actual_schedule_json @> jsonb_build_array(jsonb_build_object('external_row_key', v_shift_external_row_key))
            )
          )
        on conflict do nothing;

        create temporary table tmp_cleanup_delete(timesheet_id uuid primary key) on commit drop;

        insert into tmp_cleanup_delete(timesheet_id)
        select c.timesheet_id
        from tmp_cleanup_candidates c
        left join public.timesheets_financials tf
          on tf.timesheet_id = c.timesheet_id
         and tf.is_current = true
        where coalesce(tf.locked_by_invoice_id, null) is null
          and not exists (
            select 1
            from jsonb_array_elements(
              case
                when tf.invoice_breakdown_json is not null
                 and jsonb_typeof(tf.invoice_breakdown_json)='object'
                 and jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
                then tf.invoice_breakdown_json->'segments'
                else '[]'::jsonb
              end
            ) s(seg)
            where nullif(btrim(coalesce(s.seg->>'invoice_locked_invoice_id','')), '') is not null
          )
        on conflict do nothing;

        select coalesce(array_agg(d.timesheet_id), array[]::uuid[])
        into v_cleanup_ts_ids
        from tmp_cleanup_delete d;

        v_cleanup_count := coalesce(array_length(v_cleanup_ts_ids, 1), 0);

        if v_pos_is_invoiced is not true and v_cleanup_count > 0 then
          delete from public.hr_issue_emails hie where hie.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.timesheet_validations tv where tv.timesheet_id = any(v_cleanup_ts_ids);

          delete from public.pay_item_snoozes ps where ps.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.pay_batch_items pbi where pbi.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.ts_pay_adjustments tpa where tpa.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.timesheet_pay_state_history tpsh where tpsh.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.timesheet_pay_state tps where tps.timesheet_id = any(v_cleanup_ts_ids);

          delete from public.timesheet_evidence te where te.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.manual_timesheet_queue mtq where mtq.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.ts_pdfs_outbox tpo where tpo.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.ts_financials_outbox tfo where tfo.timesheet_id = any(v_cleanup_ts_ids);

          delete from public.timesheets_financials tfz where tfz.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.contract_weeks cwz where cwz.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.timesheets tz where tz.timesheet_id = any(v_cleanup_ts_ids);
        end if;
      exception when others then
        null;
      end;

      -- ✅ User-facing audit (UNGATED): correction timesheet + invoice history
      begin
        if v_correction_ts_id is not null then
          perform public._audit_insert(
            'timesheets',
            v_correction_ts_id::text,
            'HR_IMPORT_CANCELLATION_CORRECTION_CREATED',
            null,
            jsonb_build_object(
              'trigger_import_id', p_import_id::text,
              'evidence_import_id', case when v_anchor_import_id is null then null else v_anchor_import_id::text end,
              'branch', v_branch,
              'shift_id', v_shift_id::text,
              'work_date', v_shift_work_date::text,
              'external_row_key', v_shift_external_row_key,
              'hr_request_id', nullif(btrim(coalesce(v_shift_hr_request_id,'')), ''),
              'ref_num', nullif(btrim(coalesce(v_shift_hr_request_id,'')), ''),
              'cancel_reason', v_reason,
              'invoice_id', case when v_invoice_id_detected is null then null else v_invoice_id_detected::text end,
              'correction_id', v_correction_id,
              'correction_kind', v_kind,
              'anchor_mode', case when v_pos_is_invoiced is true then 'INVOICED_REPLACEMENT' else 'BASE_LOCKED' end,
              'cleanup_deleted_changed_hours_count', v_cleanup_count,
              'cleanup_deleted_timesheet_ids', to_jsonb(coalesce(v_cleanup_ts_ids, array[]::uuid[]))
            ),
            'IMPORT_CANCELLATION_CORRECTION',
            p_actor_user_id
          );
        end if;

        if v_invoice_id_detected is not null then
          perform public._inv_write_audit(
            p_actor_user_id,
            'HR_IMPORT_CANCELLATION_CORRECTION_CREATED',
            jsonb_build_object(
              'trigger_import_id', p_import_id::text,
              'evidence_import_id', case when v_anchor_import_id is null then null else v_anchor_import_id::text end,
              'shift_id', v_shift_id::text,
              'work_date', v_shift_work_date::text,
              'external_row_key', v_shift_external_row_key,
              'hr_request_id', nullif(btrim(coalesce(v_shift_hr_request_id,'')), ''),
              'ref_num', nullif(btrim(coalesce(v_shift_hr_request_id,'')), ''),
              'invoice_id', v_invoice_id_detected::text,
              'correction_timesheet_id', case when v_correction_ts_id is null then null else v_correction_ts_id::text end,
              'correction_id', v_correction_id,
              'correction_kind', v_kind,
              'anchor_mode', case when v_pos_is_invoiced is true then 'INVOICED_REPLACEMENT' else 'BASE_LOCKED' end,
              'cleanup_deleted_changed_hours_count', v_cleanup_count
            ),
            'invoices',
            v_invoice_id_detected::text,
            null,
            'IMPORT_CANCELLATION_CORRECTION',
            null,
            null,
            null
          );
        end if;
      exception when others then
        null;
      end;

    end if;

    -- Debug sample (cap 30)
    if v_sample_n < 30 then
      v_sample := v_sample || jsonb_build_array(jsonb_build_object(
        'shift_id', v_shift_id::text,
        'key_type', 'HR_REQUEST_ID',
        'hr_request_id', v_shift_hr_request_id,
        'present_in_file', v_present_in_file,
        'timesheet_id', case when v_timesheet_id is null then null else v_timesheet_id::text end,
        'invoice_id_detected', case when v_invoice_id_detected is null then null else v_invoice_id_detected::text end,
        'invoiced_detected', v_invoiced_detected,
        'branch', v_branch,
        'correction_timesheet_id', case when v_correction_ts_id is null then null else v_correction_ts_id::text end,
        'anchor_mode', case when v_pos_is_invoiced is true then 'INVOICED_REPLACEMENT' else 'BASE_LOCKED' end,
        'cleanup_deleted_changed_hours_count', v_cleanup_count
      ));
      v_sample_n := v_sample_n + 1;
    end if;

  end loop;

  -- Deduplicate arrays
  select coalesce(array_agg(distinct x), array[]::uuid[])
  into v_timesheet_ids
  from unnest(v_timesheet_ids) x
  where x is not null;

  select coalesce(array_agg(distinct x), array[]::uuid[])
  into v_invoice_ids
  from unnest(v_invoice_ids) x
  where x is not null;

  -- Debug audit (invoice_debug gated inside _imp_debug_audit)
  perform public._imp_debug_audit(
    p_actor_user_id,
    'HR_CANCEL_APPLY_DEBUG',
    jsonb_build_object(
      'import_id', p_import_id::text,
      'key_type', 'HR_REQUEST_ID',
      'selected_count', jsonb_array_length(v_actions),
      'cancelled_count', v_cancelled_count,
      'file_request_count', v_file_request_count,
      'affected_timesheet_ids_count', coalesce(array_length(v_timesheet_ids, 1), 0),
      'affected_invoice_ids_count', coalesce(array_length(v_invoice_ids, 1), 0),
      'sample', v_sample
    ),
    'hr_imports',
    p_import_id::text,
    null,
    null,
    null,
    null
  );

  v_credit_note_ids := array[]::uuid[];
  v_pdf_jobs_enqueued := 0;

  return jsonb_build_object(
    'import_id', p_import_id,
    'cancelled_count', v_cancelled_count,
    'affected_timesheet_ids', to_jsonb(v_timesheet_ids),
    'affected_invoice_ids', to_jsonb(v_invoice_ids),
    'credit_note_ids_created', to_jsonb(v_credit_note_ids),
    'invoice_pdf_jobs_enqueued', v_pdf_jobs_enqueued
  );

exception when others then
  get stacked diagnostics v_sqlstate = returned_sqlstate, v_err = message_text;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'HR_CANCEL_APPLY_ERROR',
      jsonb_build_object(
        'import_id', p_import_id::text,
        'key_type', 'HR_REQUEST_ID',
        'last_shift_id', case when v_last_shift_id is null then null else v_last_shift_id::text end,
        'selected_count', case when jsonb_typeof(v_actions) = 'array' then jsonb_array_length(v_actions) else null end,
        'cancelled_count', v_cancelled_count,
        'file_request_count', v_file_request_count,
        'sqlstate', v_sqlstate,
        'error', v_err
      ),
      'hr_imports',
      p_import_id::text,
      null,
      null,
      null,
      null
    );
  exception when others then
    null;
  end;

  raise;
end;
$$;






create or replace function public.hr_weekly_validation_apply_send_emails(
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

  v_import record;
  v_client_id uuid;

  v_results jsonb := '[]'::jsonb;
  v_ok_count int := 0;

  v_idx int;
  v_item jsonb;

  v_timesheet_id_text text;
  v_timesheet_id uuid;

  v_issue_fingerprint text;
  v_reason_code text;

  v_staff_norm text;
  v_hospital_norm text;
  v_work_date_text text;
  v_work_date date;

  v_contract_id uuid;
  v_recipient_email text;

  v_already boolean := false;
begin
  -- ⚠️ UPDATED (locked architecture):
  -- This RPC is now READ-ONLY job builder.
  -- It MUST NOT write hr_issue_emails or any other state.
  -- Transactional apply (hr_weekly_apply_transactional) performs hr_issue_emails upsert.
  --
  -- This function remains only as a safe helper to validate/resolve recipients and compute EMAIL vs REEMAIL.

  if p_import_id is null then
    raise exception 'hr_weekly_validation_apply_send_emails: import_id is required';
  end if;

  select
    hi.id,
    hi.source_system,
    hi.import_scope,
    hi.client_id
  into v_import
  from public.hr_imports hi
  where hi.id = p_import_id
  limit 1;

  if v_import.id is null then
    raise exception 'hr_weekly_validation_apply_send_emails: import % not found', p_import_id;
  end if;

  if upper(coalesce(v_import.source_system::text,'')) <> 'HEALTHROSTER' then
    raise exception 'hr_weekly_validation_apply_send_emails: import % is not HEALTHROSTER (source_system=%)', p_import_id, v_import.source_system;
  end if;

  -- Allow when import_scope is NULL (pre-apply); reject only if explicitly set to something else.
  if v_import.import_scope is not null and upper(coalesce(v_import.import_scope::text,'')) <> 'HR_WEEKLY' then
    raise exception 'hr_weekly_validation_apply_send_emails: import % is not HR_WEEKLY (import_scope=%)', p_import_id, v_import.import_scope;
  end if;

  v_client_id := v_import.client_id;
  if v_client_id is null then
    raise exception 'hr_weekly_validation_apply_send_emails: import % has no client_id', p_import_id;
  end if;

  if jsonb_typeof(v_actions) <> 'array' then
    raise exception 'hr_weekly_validation_apply_send_emails: p_actions must be a JSON array';
  end if;

  if jsonb_array_length(v_actions) = 0 then
    return jsonb_build_object(
      'import_id', p_import_id::text,
      'queued', 0,
      'results', '[]'::jsonb
    );
  end if;

  for v_idx, v_item in
    select (e.ord)::int, e.value
    from jsonb_array_elements(v_actions) with ordinality as e(value, ord)
  loop
    v_timesheet_id_text := nullif(btrim(coalesce(v_item->>'timesheet_id','')), '');
    v_issue_fingerprint := nullif(btrim(coalesce(v_item->>'issue_fingerprint','')), '');
    v_reason_code := nullif(btrim(coalesce(v_item->>'reason_code','')), '');
    v_staff_norm := nullif(btrim(coalesce(v_item->>'staff_norm','')), '');
    v_hospital_norm := nullif(btrim(coalesce(v_item->>'hospital_norm','')), '');
    v_work_date_text := nullif(btrim(coalesce(v_item->>'work_date','')), '');

    if v_timesheet_id_text is null then
      raise exception 'hr_weekly_validation_apply_send_emails: item % missing timesheet_id', v_idx;
    end if;
    if v_issue_fingerprint is null then
      raise exception 'hr_weekly_validation_apply_send_emails: item % missing issue_fingerprint', v_idx;
    end if;
    if v_reason_code is null then
      v_reason_code := 'HEALTHROSTER_WEEKLY';
    end if;

    begin
      v_timesheet_id := v_timesheet_id_text::uuid;
    exception when invalid_text_representation then
      raise exception 'hr_weekly_validation_apply_send_emails: item % invalid timesheet_id "%"', v_idx, v_timesheet_id_text;
    end;

    begin
      if v_work_date_text is not null and v_work_date_text ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then
        v_work_date := v_work_date_text::date;
      else
        v_work_date := null;
      end if;
    exception when others then
      v_work_date := null;
    end;

    select exists(
      select 1
      from public.hr_issue_emails e
      where e.issue_fingerprint = v_issue_fingerprint
    )
    into v_already;

    select
      t.contract_id
    into v_contract_id
    from public.timesheets t
    where t.timesheet_id = v_timesheet_id
      and t.is_current = true
    limit 1;

    if v_contract_id is null then
      v_results := v_results || jsonb_build_object(
        'timesheet_id', v_timesheet_id::text,
        'issue_fingerprint', v_issue_fingerprint,
        'ok', false,
        'error', 'TIMESHEET_NOT_FOUND'
      );
      continue;
    end if;

    -- Resolve recipient from client (locked: clients.ts_queries_email)
    select nullif(btrim(coalesce(cli.ts_queries_email,'')), '')
    into v_recipient_email
    from public.clients cli
    where cli.id = v_client_id
    limit 1;

    if v_recipient_email is null then
      v_results := v_results || jsonb_build_object(
        'timesheet_id', v_timesheet_id::text,
        'issue_fingerprint', v_issue_fingerprint,
        'ok', false,
        'error', 'NO_TS_QUERIES_EMAIL'
      );
      continue;
    end if;

    v_ok_count := v_ok_count + 1;

    -- Return a “job” for the backend to enqueue (backend will ensure PDF and insert mail_outbox)
    v_results := v_results || jsonb_build_object(
      'timesheet_id', v_timesheet_id::text,
      'client_id', v_client_id::text,
      'recipient_email', v_recipient_email,
      'issue_fingerprint', v_issue_fingerprint,
      'reason_code', v_reason_code,
      'staff_norm', v_staff_norm,
      'hospital_norm', v_hospital_norm,
      'work_date', case when v_work_date is null then null else v_work_date::text end,
      'email_kind', case when v_already then 'REEMAIL' else 'EMAIL' end,
      'ok', true
    );
  end loop;

  return jsonb_build_object(
    'import_id', p_import_id::text,
    'queued', v_ok_count,
    'results', v_results
  );
end;
$$;


create or replace function public.hr_weekly_validation_preview(
  p_import_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_import record;
  v_client_id uuid;
  v_we_dow int := 0;
  v_recipient_email text;

  v_rows jsonb := '[]'::jsonb;
  v_unmapped_candidates int := 0;
  v_unmatched_timesheets int := 0;
begin
  if p_import_id is null then
    raise exception 'hr_weekly_validation_preview: import_id is required';
  end if;

  select
    hi.id,
    hi.source_system,
    hi.import_scope,
    hi.client_id
  into v_import
  from public.hr_imports hi
  where hi.id = p_import_id
  limit 1;

  if v_import.id is null then
    raise exception 'hr_weekly_validation_preview: import % not found', p_import_id;
  end if;

  if upper(coalesce(v_import.source_system::text,'')) <> 'HEALTHROSTER' then
    raise exception 'hr_weekly_validation_preview: import % is not HEALTHROSTER (source_system=%)', p_import_id, v_import.source_system;
  end if;

  -- ✅ UPDATED (locked policy): allow running weekly validation before apply sets import_scope.
  -- Only reject if import_scope is explicitly set to a different value.
  if v_import.import_scope is not null and upper(coalesce(v_import.import_scope::text,'')) <> 'HR_WEEKLY' then
    raise exception 'hr_weekly_validation_preview: import % is not HR_WEEKLY (import_scope=%)', p_import_id, v_import.import_scope;
  end if;

  v_client_id := v_import.client_id;
  if v_client_id is null then
    raise exception 'hr_weekly_validation_preview: import % has no client_id', p_import_id;
  end if;

  -- Resolve client week-ending weekday (fallback 0 = Sunday)
  select coalesce(cs.week_ending_weekday, 0)::int
  into v_we_dow
  from public.client_settings cs
  where cs.client_id = v_client_id
  order by cs.effective_from desc nulls last, cs.created_at desc
  limit 1;

  -- Resolve recipient (for UI messaging / can_email)
  select nullif(btrim(coalesce(c.ts_queries_email,'')), '')
  into v_recipient_email
  from public.clients c
  where c.id = v_client_id
  limit 1;

  with hr_raw as (
    select
      r.id as hr_row_id,
      r.external_row_key,
      nullif(btrim(coalesce(r.payload_json->>'staff_name','')), '') as staff_name_payload,
      nullif(btrim(coalesce(r.staff_raw,'')), '') as staff_raw,
      nullif(btrim(coalesce(r.staff_norm,'')), '') as staff_norm_col,
      (r.payload_json->>'start_utc')::timestamptz as start_utc_raw,
      (r.payload_json->>'end_utc')::timestamptz as end_utc_raw,
      coalesce(
        nullif(r.payload_json->>'break_mins','')::int,
        nullif(r.payload_json->>'break_minutes','')::int,
        nullif(r.payload_json->>'actual_break_minutes','')::int,
        0
      ) as break_mins
    from public.hr_rows r
    where r.import_id = p_import_id
      and r.external_row_key is not null
      and (r.payload_json->>'start_utc') is not null
      and (r.payload_json->>'end_utc') is not null
  ),
  hr_normed as (
    select
      h.hr_row_id,
      h.external_row_key,
      coalesce(h.staff_name_payload, h.staff_raw, h.staff_norm_col) as staff_name,
      nullif(lower(trim(coalesce(coalesce(h.staff_name_payload, h.staff_raw, h.staff_norm_col), ''))), '') as staff_norm,
      nullif(regexp_replace(lower(coalesce(coalesce(h.staff_name_payload, h.staff_raw, h.staff_norm_col), '')), '[^a-z0-9]+', '', 'g'), '') as staff_norm2,
      date_trunc('minute', h.start_utc_raw) as start_utc,
      date_trunc('minute', h.end_utc_raw) as end_utc,
      coalesce(h.break_mins, 0) as break_mins,
      ((date_trunc('minute', h.start_utc_raw) at time zone 'Europe/London')::date) as work_date
    from hr_raw h
  ),
  hr_resolved as (
    select
      n.*,

      coalesce(
        cand_alias.id,
        cand_map.candidate_id,
        cand_exact_unique.cid
      ) as candidate_id,

      coalesce(
        cand_alias.display_name,
        cand_map.display_name,
        cand_exact_unique.cname
      ) as candidate_name

    from hr_normed n

    left join lateral (
      select c.id, c.display_name
      from public.candidates c
      where c.nhsp_hr_name_aliases is not null
        and (
          (n.staff_norm  is not null and c.nhsp_hr_name_aliases @> to_jsonb(array[n.staff_norm]::text[]))
          or
          (n.staff_norm2 is not null and c.nhsp_hr_name_aliases @> to_jsonb(array[n.staff_norm2]::text[]))
        )
      limit 1
    ) cand_alias on true

    left join lateral (
      select hm.candidate_id, c.display_name
      from public.hr_name_mappings hm
      join public.candidates c
        on c.id = hm.candidate_id
      where hm.active = true
        and (
          (n.staff_norm  is not null and hm.hr_name_norm = n.staff_norm)
          or
          (n.staff_norm2 is not null and hm.hr_name_norm = n.staff_norm2)
        )
      order by hm.created_at desc
      limit 1
    ) cand_map on cand_alias.id is null

    left join lateral (
      with matches as (
        select
          c.id as cid,
          c.display_name as cname
        from public.candidates c
        where c.active = true
          and n.staff_norm2 is not null
          and (
            regexp_replace(lower(coalesce(c.first_name,'') || coalesce(c.last_name,'')), '[^a-z0-9]+', '', 'g') = n.staff_norm2
            or
            regexp_replace(lower(coalesce(c.last_name,'')  || coalesce(c.first_name,'')), '[^a-z0-9]+', '', 'g') = n.staff_norm2
          )
      )
      select
        case when count(*) = 1 then (array_agg(cid order by cid::text))[1] end as cid,
        case when count(*) = 1 then (array_agg(cname order by cid::text))[1] end as cname
      from matches
    ) cand_exact_unique on (cand_alias.id is null and cand_map.candidate_id is null)
  ),
  hr_with_we as (
    select
      r.*,
      (
        r.work_date
        + (
            (v_we_dow - extract(dow from r.work_date)::int + 7) % 7
          )
      )::date as week_ending_date,
      to_char((r.start_utc at time zone 'Europe/London'), 'HH24:MI') as hr_start_hhmm,
      to_char((r.end_utc at time zone 'Europe/London'), 'HH24:MI') as hr_end_hhmm,
      (substring(to_char((r.start_utc at time zone 'Europe/London'), 'HH24:MI'),1,2)::int * 60
        + substring(to_char((r.start_utc at time zone 'Europe/London'), 'HH24:MI'),4,2)::int
      ) as hr_start_min,
      (
        case
          when (
            (substring(to_char((r.end_utc at time zone 'Europe/London'), 'HH24:MI'),1,2)::int * 60
             + substring(to_char((r.end_utc at time zone 'Europe/London'), 'HH24:MI'),4,2)::int
            )
            <=
            (substring(to_char((r.start_utc at time zone 'Europe/London'), 'HH24:MI'),1,2)::int * 60
             + substring(to_char((r.start_utc at time zone 'Europe/London'), 'HH24:MI'),4,2)::int
            )
          )
          then
            (substring(to_char((r.end_utc at time zone 'Europe/London'), 'HH24:MI'),1,2)::int * 60
             + substring(to_char((r.end_utc at time zone 'Europe/London'), 'HH24:MI'),4,2)::int
            ) + 1440
          else
            (substring(to_char((r.end_utc at time zone 'Europe/London'), 'HH24:MI'),1,2)::int * 60
             + substring(to_char((r.end_utc at time zone 'Europe/London'), 'HH24:MI'),4,2)::int
            )
        end
      ) as hr_end_min,
      greatest(
        0,
        (
          (
            case
              when (
                (substring(to_char((r.end_utc at time zone 'Europe/London'), 'HH24:MI'),1,2)::int * 60
                 + substring(to_char((r.end_utc at time zone 'Europe/London'), 'HH24:MI'),4,2)::int
                )
                <=
                (substring(to_char((r.start_utc at time zone 'Europe/London'), 'HH24:MI'),1,2)::int * 60
                 + substring(to_char((r.start_utc at time zone 'Europe/London'), 'HH24:MI'),4,2)::int
                )
              )
              then
                (substring(to_char((r.end_utc at time zone 'Europe/London'), 'HH24:MI'),1,2)::int * 60
                 + substring(to_char((r.end_utc at time zone 'Europe/London'), 'HH24:MI'),4,2)::int
                ) + 1440
              else
                (substring(to_char((r.end_utc at time zone 'Europe/London'), 'HH24:MI'),1,2)::int * 60
                 + substring(to_char((r.end_utc at time zone 'Europe/London'), 'HH24:MI'),4,2)::int
                )
            end
          )
          -
          (substring(to_char((r.start_utc at time zone 'Europe/London'), 'HH24:MI'),1,2)::int * 60
           + substring(to_char((r.start_utc at time zone 'Europe/London'), 'HH24:MI'),4,2)::int
          )
          - coalesce(r.break_mins,0)
        )::int
      ) as hr_paid_minutes
    from hr_resolved r
  ),
  hr_day_totals as (
    select
      h.candidate_id,
      h.candidate_name,
      h.week_ending_date,
      h.work_date,
      sum(h.hr_paid_minutes)::int as hr_paid_minutes
    from hr_with_we h
    where h.candidate_id is not null
    group by h.candidate_id, h.candidate_name, h.week_ending_date, h.work_date
  ),
  hr_day_entries as (
    select
      h.candidate_id,
      h.week_ending_date,
      h.work_date,
      jsonb_agg(
        jsonb_build_object(
          'hr_row_id', h.hr_row_id::text,
          'start_hhmm', h.hr_start_hhmm,
          'end_hhmm', h.hr_end_hhmm,
          'break_mins', h.break_mins,
          'start_minute', h.hr_start_min,
          'end_minute', h.hr_end_min
        )
        order by h.hr_start_min asc, h.hr_end_min asc, h.hr_row_id::text
      ) as hr_entries_json,
      count(*)::int as hr_entry_count
    from hr_with_we h
    where h.candidate_id is not null
    group by h.candidate_id, h.week_ending_date, h.work_date
  ),
  hr_triples as (
    select distinct
      h.candidate_id,
      h.candidate_name,
      h.week_ending_date
    from hr_with_we h
    where h.candidate_id is not null
  ),
  ts_matches as (
    select
      tr.candidate_id,
      tr.candidate_name,
      tr.week_ending_date,
      vf.timesheet_id
    from hr_triples tr
    left join public.v_timesheets_funnel vf
      on vf.kind = 'WEEK'
     and vf.client_id = v_client_id
     and vf.candidate_id = tr.candidate_id
     and vf.week_ending_date = tr.week_ending_date
     and vf.timesheet_id is not null
  ),
  ts_schedule as (
    select
      tm.candidate_id,
      tm.candidate_name,
      tm.week_ending_date,
      tm.timesheet_id,
      t.actual_schedule_json
    from ts_matches tm
    join public.timesheets t
      on t.timesheet_id = tm.timesheet_id
     and t.is_current = true
  ),
  ts_entries_indexed as (
    select
      s.candidate_id,
      s.candidate_name,
      s.week_ending_date,
      s.timesheet_id,
      d.work_date,
      d.start_hhmm,
      d.end_hhmm,
      d.start_minute,
      d.end_minute,
      d.break_mins,
      greatest(0, (d.end_minute - d.start_minute - d.break_mins))::int as paid_minutes,
      row_number() over (partition by s.timesheet_id, d.work_date order by d.start_minute asc, d.end_minute asc) as worker_entry_index
    from ts_schedule s
    cross join lateral (
      select
        (day_ymd)::date as work_date,
        start_hhmm,
        end_hhmm,
        start_minute,
        end_minute,
        break_mins
      from (
        select
          nullif(btrim(coalesce((e.elem->>'date')::text, '')), '') as day_ymd,
          case
            when nullif(btrim(coalesce(e.elem->>'start','')), '') is not null then nullif(btrim(coalesce(e.elem->>'start','')), '')
            when nullif(btrim(coalesce(e.elem->>'start_utc','')), '') is not null then to_char(((e.elem->>'start_utc')::timestamptz at time zone 'Europe/London'), 'HH24:MI')
            else null
          end as start_hhmm,
          case
            when nullif(btrim(coalesce(e.elem->>'end','')), '') is not null then nullif(btrim(coalesce(e.elem->>'end','')), '')
            when nullif(btrim(coalesce(e.elem->>'end_utc','')), '') is not null then to_char(((e.elem->>'end_utc')::timestamptz at time zone 'Europe/London'), 'HH24:MI')
            else null
          end as end_hhmm,
          case
            when (e.elem ? 'break_minutes') and nullif(btrim(coalesce(e.elem->>'break_minutes','')), '') is not null
              then greatest(((e.elem->>'break_minutes')::int), 0)
            when (e.elem ? 'break_mins') and nullif(btrim(coalesce(e.elem->>'break_mins','')), '') is not null
              then greatest(((e.elem->>'break_mins')::int), 0)
            when jsonb_typeof(e.elem->'breaks') = 'array' then (
              select coalesce(sum(
                case
                  when (b->>'start') ~ '^[0-9]{2}:[0-9]{2}$' and (b->>'end') ~ '^[0-9]{2}:[0-9]{2}$' then
                    (
                      (case when substring(b->>'end',1,2)::int*60 + substring(b->>'end',4,2)::int
                                 <= substring(b->>'start',1,2)::int*60 + substring(b->>'start',4,2)::int
                            then (substring(b->>'end',1,2)::int*60 + substring(b->>'end',4,2)::int) + 1440
                            else (substring(b->>'end',1,2)::int*60 + substring(b->>'end',4,2)::int)
                       end)
                      -
                      (substring(b->>'start',1,2)::int*60 + substring(b->>'start',4,2)::int)
                    )
                  else 0
                end
              )::int, 0)
              from jsonb_array_elements(e.elem->'breaks') b
            )
            when nullif(btrim(coalesce(e.elem->>'break_start','')), '') is not null
              and nullif(btrim(coalesce(e.elem->>'break_end','')), '') is not null
              and (e.elem->>'break_start') ~ '^[0-9]{2}:[0-9]{2}$'
              and (e.elem->>'break_end') ~ '^[0-9]{2}:[0-9]{2}$'
              then
                greatest(
                  (
                    (case when substring(e.elem->>'break_end',1,2)::int*60 + substring(e.elem->>'break_end',4,2)::int
                               <= substring(e.elem->>'break_start',1,2)::int*60 + substring(e.elem->>'break_start',4,2)::int
                          then (substring(e.elem->>'break_end',1,2)::int*60 + substring(e.elem->>'break_end',4,2)::int) + 1440
                          else (substring(e.elem->>'break_end',1,2)::int*60 + substring(e.elem->>'break_end',4,2)::int)
                     end)
                    -
                    (substring(e.elem->>'break_start',1,2)::int*60 + substring(e.elem->>'break_start',4,2)::int)
                  )::int,
                  0
                )
            else 0
          end as break_mins
        from jsonb_array_elements(coalesce(s.actual_schedule_json, '[]'::jsonb)) as e(elem)
      ) base
      cross join lateral (
        select
          case when base.day_ymd ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then (base.day_ymd)::date else null::date end as day_date,
          base.start_hhmm,
          base.end_hhmm,
          base.break_mins
      ) dd
      cross join lateral (
        select
          case when base.start_hhmm ~ '^[0-9]{2}:[0-9]{2}$'
            then (substring(base.start_hhmm,1,2)::int*60 + substring(base.start_hhmm,4,2)::int)
            else null::int
          end as start_minute_raw,
          case when base.end_hhmm ~ '^[0-9]{2}:[0-9]{2}$'
            then (substring(base.end_hhmm,1,2)::int*60 + substring(base.end_hhmm,4,2)::int)
            else null::int
          end as end_minute_raw
      ) mm
      cross join lateral (
        select
          dd.day_date as work_date,
          base.start_hhmm as start_hhmm,
          base.end_hhmm as end_hhmm,
          mm.start_minute_raw as start_minute,
          case
            when mm.start_minute_raw is null or mm.end_minute_raw is null then null::int
            when mm.end_minute_raw <= mm.start_minute_raw then mm.end_minute_raw + 1440
            else mm.end_minute_raw
          end as end_minute,
          greatest(coalesce(base.break_mins,0),0) as break_mins
      ) outx
      where outx.work_date is not null
        and outx.start_minute is not null
        and outx.end_minute is not null
    ) d
  ),
  ts_day_totals as (
    select
      t.candidate_id,
      t.week_ending_date,
      t.timesheet_id,
      t.work_date,
      sum(t.paid_minutes)::int as ts_paid_minutes
    from ts_entries_indexed t
    group by t.candidate_id, t.week_ending_date, t.timesheet_id, t.work_date
  ),
  ts_day_entries as (
    select
      t.timesheet_id,
      t.work_date,
      jsonb_agg(
        jsonb_build_object(
          'worker_entry_index', t.worker_entry_index,
          'start_hhmm', t.start_hhmm,
          'end_hhmm', t.end_hhmm,
          'break_mins', t.break_mins,
          'start_minute', t.start_minute,
          'end_minute', t.end_minute,
          'paid_minutes', t.paid_minutes
        )
        order by t.worker_entry_index asc
      ) as worker_entries_json,
      count(*)::int as worker_entry_count
    from ts_entries_indexed t
    group by t.timesheet_id, t.work_date
  ),
  hr_entries_flat as (
    select
      h.candidate_id,
      h.week_ending_date,
      h.work_date,
      h.hr_row_id,
      h.hr_start_hhmm,
      h.hr_end_hhmm,
      h.hr_start_min,
      h.hr_end_min,
      h.break_mins as hr_break_mins
    from hr_with_we h
    where h.candidate_id is not null
  ),
  pairing_counts as (
    select
      w.timesheet_id,
      w.work_date,
      w.worker_entry_index,
      w.start_hhmm as worker_start_hhmm,
      w.end_hhmm as worker_end_hhmm,
      w.start_minute as worker_start_min,
      w.end_minute as worker_end_min,
      w.break_mins as worker_break_mins,
      w.paid_minutes as worker_paid_minutes,

      count(h.hr_row_id)::int as match_count,

      case when count(h.hr_row_id) = 1 then (array_agg(h.hr_row_id order by h.hr_row_id::text))[1] end as matched_hr_row_id,
      case when count(h.hr_row_id) = 1 then (array_agg(h.hr_start_hhmm order by h.hr_row_id::text))[1] end as matched_hr_start_hhmm,
      case when count(h.hr_row_id) = 1 then (array_agg(h.hr_end_hhmm order by h.hr_row_id::text))[1] end as matched_hr_end_hhmm,
      case when count(h.hr_row_id) = 1 then (array_agg(h.hr_start_min order by h.hr_row_id::text))[1] end as matched_hr_start_min,
      case when count(h.hr_row_id) = 1 then (array_agg(h.hr_end_min order by h.hr_row_id::text))[1] end as matched_hr_end_min,
      case when count(h.hr_row_id) = 1 then (array_agg(h.hr_break_mins order by h.hr_row_id::text))[1] end as matched_hr_break_mins
    from ts_entries_indexed w
    join ts_matches tm
      on tm.timesheet_id = w.timesheet_id
    left join hr_entries_flat h
      on h.candidate_id = tm.candidate_id
     and h.week_ending_date = tm.week_ending_date
     and h.work_date = w.work_date
     and (least(w.end_minute, h.hr_end_min) - greatest(w.start_minute, h.hr_start_min)) >= 1
    group by
      w.timesheet_id, w.work_date, w.worker_entry_index,
      w.start_hhmm, w.end_hhmm, w.start_minute, w.end_minute, w.break_mins, w.paid_minutes
  ),
  pairings_json as (
    select
      p.timesheet_id,
      p.work_date,
      jsonb_agg(
        jsonb_build_object(
          'worker_entry_index', p.worker_entry_index,
          'worker_start_hhmm', p.worker_start_hhmm,
          'worker_end_hhmm', p.worker_end_hhmm,
          'worker_break_mins', p.worker_break_mins,
          'match_status', case
            when p.match_count = 1 then 'MATCHED'
            when p.match_count = 0 then 'UNMATCHED'
            else 'AMBIGUOUS'
          end,
          'hr_row_id', case when p.match_count = 1 then p.matched_hr_row_id::text else null end,
          'hr_start_hhmm', case when p.match_count = 1 then p.matched_hr_start_hhmm else null end,
          'hr_end_hhmm', case when p.match_count = 1 then p.matched_hr_end_hhmm else null end,
          'hr_break_mins', case when p.match_count = 1 then p.matched_hr_break_mins else null end,
          'start_diff_mins', case when p.match_count = 1 then (p.worker_start_min - p.matched_hr_start_min) else null end,
          'end_diff_mins', case when p.match_count = 1 then (p.worker_end_min - p.matched_hr_end_min) else null end,
          'break_diff_mins', case when p.match_count = 1 then (coalesce(p.worker_break_mins,0) - coalesce(p.matched_hr_break_mins,0)) else null end
        )
        order by p.worker_entry_index asc
      ) as pairings,
      bool_or(p.match_count <> 1) as has_pairing_ambiguity,
      bool_or(
        p.match_count = 1 and (
          (p.worker_start_min - p.matched_hr_start_min) <> 0
          or (p.worker_end_min - p.matched_hr_end_min) <> 0
          or (coalesce(p.worker_break_mins,0) - coalesce(p.matched_hr_break_mins,0)) <> 0
        )
      ) as has_shift_diffs
    from pairing_counts p
    group by p.timesheet_id, p.work_date
  ),
  day_set as (
    select distinct
      tm.timesheet_id,
      tm.candidate_id,
      tm.candidate_name,
      tm.week_ending_date,
      coalesce(hdt.work_date, tdt.work_date) as work_date
    from ts_matches tm
    left join hr_day_totals hdt
      on hdt.candidate_id = tm.candidate_id
     and hdt.week_ending_date = tm.week_ending_date
    left join ts_day_totals tdt
      on tdt.timesheet_id = tm.timesheet_id
     and tdt.work_date = hdt.work_date

    union

    select distinct
      tm2.timesheet_id,
      tm2.candidate_id,
      tm2.candidate_name,
      tm2.week_ending_date,
      tdt2.work_date
    from ts_matches tm2
    join ts_day_totals tdt2
      on tdt2.timesheet_id = tm2.timesheet_id
    where not exists (
      select 1
      from hr_day_totals h2
      where h2.candidate_id = tm2.candidate_id
        and h2.week_ending_date = tm2.week_ending_date
        and h2.work_date = tdt2.work_date
    )
  ),
  day_eval as (
    select
      ds.timesheet_id,
      ds.candidate_id,
      ds.candidate_name,
      ds.week_ending_date,
      ds.work_date,

      hdt.hr_paid_minutes,
      tdt.ts_paid_minutes,

      (coalesce(hdt.hr_paid_minutes,0) - coalesce(tdt.ts_paid_minutes,0)) as delta_minutes,

      hde.hr_entries_json,
      hde.hr_entry_count,

      tde.worker_entries_json,
      tde.worker_entry_count,

      pj.pairings,
      pj.has_pairing_ambiguity,
      pj.has_shift_diffs,

      case
        when (hdt.hr_paid_minutes is distinct from tdt.ts_paid_minutes) then 'FAIL_TOTALS'
        when coalesce(hde.hr_entry_count,0) <> coalesce(tde.worker_entry_count,0) then 'FAIL_AMBIGUOUS'
        when coalesce(pj.has_pairing_ambiguity,false) then 'FAIL_AMBIGUOUS'
        when coalesce(pj.has_shift_diffs,false) then 'FAIL_SHIFT_DETAILS'
        else 'OK'
      end as day_status
    from day_set ds
    left join hr_day_totals hdt
      on hdt.candidate_id = ds.candidate_id
     and hdt.week_ending_date = ds.week_ending_date
     and hdt.work_date = ds.work_date
    left join ts_day_totals tdt
      on tdt.timesheet_id = ds.timesheet_id
     and tdt.work_date = ds.work_date
    left join hr_day_entries hde
      on hde.candidate_id = ds.candidate_id
     and hde.week_ending_date = ds.week_ending_date
     and hde.work_date = ds.work_date
    left join ts_day_entries tde
      on tde.timesheet_id = ds.timesheet_id
     and tde.work_date = ds.work_date
    left join pairings_json pj
      on pj.timesheet_id = ds.timesheet_id
     and pj.work_date = ds.work_date
  ),
  per_ts as (
    select
      de.candidate_id,
      de.candidate_name,
      de.week_ending_date,
      de.timesheet_id,

      jsonb_agg(
        jsonb_build_object(
          'date', de.work_date::text,
          'hr_minutes', de.hr_paid_minutes,
          'ts_minutes', de.ts_paid_minutes,
          'delta_minutes', de.delta_minutes,
          'day_status', de.day_status,
          'worker_entries', coalesce(de.worker_entries_json, '[]'::jsonb),
          'hr_entries', coalesce(de.hr_entries_json, '[]'::jsonb),
          'pairings', coalesce(de.pairings, '[]'::jsonb)
        )
        order by de.work_date asc
      ) as days_json,

      bool_or(de.day_status <> 'OK') as has_mismatch,

      case
        when bool_or(de.day_status = 'FAIL_AMBIGUOUS') then 'AMBIGUOUS'
        when bool_or(de.day_status <> 'OK') then 'FAIL'
        else 'OK'
      end as overall_status,

      jsonb_agg(
        distinct
        case
          when de.day_status = 'FAIL_TOTALS' then
            (de.work_date::text || ' totals mismatch: HR=' || coalesce(de.hr_paid_minutes,0)::text || ' TS=' || coalesce(de.ts_paid_minutes,0)::text)
          when de.day_status = 'FAIL_AMBIGUOUS' then
            (de.work_date::text || ' ambiguous/unmatched shift pairing; totals match but entries cannot be paired 1:1')
          when de.day_status = 'FAIL_SHIFT_DETAILS' then
            (de.work_date::text || ' shift detail mismatch (start/end/break differs)')
          else null
        end
      ) filter (where de.day_status <> 'OK') as failure_reasons_json,

      string_agg(
        (
          de.work_date::text || ':' || de.day_status || ':' ||
          coalesce(de.hr_paid_minutes,0)::text || ',' || coalesce(de.ts_paid_minutes,0)::text || ':' ||
          coalesce(de.hr_entry_count,0)::text || ',' || coalesce(de.worker_entry_count,0)::text || ':' ||
          coalesce(
            (select string_agg(
              (
                (p->>'worker_entry_index') || '=' || (p->>'match_status') ||
                case when (p->>'match_status') = 'MATCHED' then
                  ('[' ||
                    coalesce(p->>'start_diff_mins','') || ',' ||
                    coalesce(p->>'end_diff_mins','') || ',' ||
                    coalesce(p->>'break_diff_mins','') || ']'
                  )
                else ''
                end
              ),
              ',' order by (p->>'worker_entry_index')::int
            )
            from jsonb_array_elements(coalesce(de.pairings,'[]'::jsonb)) p
            ),
            ''
          )
        ),
        ';' order by de.work_date asc
      ) as sig_text

    from day_eval de
    group by de.candidate_id, de.candidate_name, de.week_ending_date, de.timesheet_id
  ),
  with_fp as (
    select
      p.*,
      case
        when p.has_mismatch and p.timesheet_id is not null then
          ('HEALTHROSTER_WEEKLY|validation|' || p.timesheet_id::text || '|' || p.week_ending_date::text || '|' || p.overall_status || '|' || coalesce(p.sig_text,''))
        else null
      end as issue_fingerprint
    from per_ts p
  ),
  with_sent as (
    select
      w.*,
      (e.issue_fingerprint is not null) as emailed_already
    from with_fp w
    left join public.hr_issue_emails e
      on e.issue_fingerprint = w.issue_fingerprint
  )
  select
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'client_id', v_client_id::text,
          'recipient_email', v_recipient_email,

          'candidate_id', ws.candidate_id::text,
          'candidate_name', ws.candidate_name,
          'week_ending_date', ws.week_ending_date::text,
          'timesheet_id', ws.timesheet_id::text,

          'contract_id', tts.contract_id::text,

          'overall_status', ws.overall_status,
          'has_mismatch', ws.has_mismatch,
          'failure_reasons', coalesce(ws.failure_reasons_json, '[]'::jsonb),

          'issue_fingerprint', ws.issue_fingerprint,
          'emailed_already', ws.emailed_already,
          'can_email', (ws.has_mismatch and ws.timesheet_id is not null and v_recipient_email is not null and length(btrim(v_recipient_email)) > 0),

          'days', ws.days_json
        )
        order by ws.week_ending_date asc, ws.candidate_name nulls last, ws.timesheet_id::text
      ),
      '[]'::jsonb
    )
  into v_rows
  from with_sent ws
  left join public.timesheets tts
    on tts.timesheet_id = ws.timesheet_id
   and tts.is_current = true;

  select count(*)::int
  into v_unmapped_candidates
  from (
    select 1
    from hr_with_we h
    where h.candidate_id is null
    limit 1000000
  ) x;

  select count(*)::int
  into v_unmatched_timesheets
  from (
    select 1
    from ts_matches tm
    where tm.timesheet_id is null
    limit 1000000
  ) y;

  return jsonb_build_object(
    'import_id', p_import_id::text,
    'client_id', v_client_id::text,
    'week_ending_weekday', v_we_dow,
    'recipient_email', v_recipient_email,
    'unmapped_candidate_rows', v_unmapped_candidates,
    'unmatched_timesheet_triples', v_unmatched_timesheets,
    'rows', v_rows
  );
end;
$$;

