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

  v_idx int;
  v_item jsonb;

  v_shift_id_text text;
  v_shift_id uuid;
  v_reason text;

  v_timesheet_id uuid;
  v_invoice_id uuid;

  v_invoice_status text;
  v_invoice_credit_mark timestamptz;

  v_credit_note_id uuid;
  v_unlocked_snapshots int;

  v_timesheet_ids uuid[] := array[]::uuid[];
  v_invoice_ids uuid[] := array[]::uuid[];
  v_credit_note_ids uuid[] := array[]::uuid[];

  v_pdf_jobs_enqueued int := 0;
begin
  -- Validate import exists (transactional safety)
  if not exists (
    select 1
    from public.hr_imports hi
    where hi.id = p_import_id
  ) then
    raise exception 'weekly_import_apply_cancellations: import % not found in hr_imports.', p_import_id;
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

  -- Apply is transactional: any invalid selected row fails the whole apply.
  for v_idx, v_item in
    select (e.ord)::int, e.value
    from jsonb_array_elements(v_actions) with ordinality as e(value, ord)
  loop
    -- Policy: no "force" mechanism other than ticking RED rows in preview.
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

    -- Lock shift row (serializes concurrent apply)
    select
      ns.timesheet_id,
      ns.invoice_id
    into
      v_timesheet_id,
      v_invoice_id
    from public.nhsp_shifts ns
    where ns.id = v_shift_id
    for update;

    if not found then
      raise exception 'weekly_import_apply_cancellations: item % shift % not found in nhsp_shifts.', v_idx, v_shift_id;
    end if;

    -- Always record affected ids for return payload
    if v_timesheet_id is not null then
      v_timesheet_ids := array_append(v_timesheet_ids, v_timesheet_id);
    end if;
    if v_invoice_id is not null then
      v_invoice_ids := array_append(v_invoice_ids, v_invoice_id);
    end if;

    -- If shift is linked to an invoice, apply invoice policy:
    -- - DRAFT/ON_HOLD: editable, so remove the shift from that invoice and force PDF regen.
    -- - ISSUED/PAID: immutable, so create reversal-only artefact via full credit note + unlock (idempotent via marker).
    if v_invoice_id is not null then
      -- Lock invoice row and load status + credit marker
      select
        i.status::text,
        i.credit_note_created_at_utc
      into
        v_invoice_status,
        v_invoice_credit_mark
      from public.invoices i
      where i.id = v_invoice_id
      for update;

      if not found then
        raise exception 'weekly_import_apply_cancellations: item % invoice % not found in invoices.', v_idx, v_invoice_id;
      end if;

      if v_invoice_status in ('ISSUED','PAID') then
        -- Issued/paid invoices are immutable: create reversal-only credit artefact if not already done
        if v_invoice_credit_mark is null then
          select
            x.credit_note_id,
            x.unlocked_snapshots
          into
            v_credit_note_id,
            v_unlocked_snapshots
          from public.invoice_create_credit_note_and_unlock(v_invoice_id, p_actor_user_id) x
          limit 1;

          if v_credit_note_id is null then
            raise exception 'weekly_import_apply_cancellations: credit note creation returned null (invoice_id=%).', v_invoice_id;
          end if;

          update public.invoices i2
          set
            credit_note_created_at_utc = v_now,
            updated_at = v_now
          where i2.id = v_invoice_id
            and i2.credit_note_created_at_utc is null;

          v_credit_note_ids := array_append(v_credit_note_ids, v_credit_note_id);
        end if;

      else
        -- Draft/On-hold invoices are editable: remove the shift from the invoice now.
        perform 1
        from public.invoice_remove_nhsp_shifts(v_invoice_id, array[v_shift_id]::uuid[], p_actor_user_id);

        -- Invalidate/regenerate invoice PDF + attachments (force)
        -- Clear PDF keys to prevent stale bundles; enqueue FORCE_REGEN job.
        update public.invoices i3
        set
          invoice_pdf_r2_key = null,
          invoice_pdf_generated_at_utc = null,
          updated_at = v_now
        where i3.id = v_invoice_id;

        v_pdf_jobs_enqueued := v_pdf_jobs_enqueued + public.invpdf_enqueue_one(v_invoice_id, true);
      end if;
    end if;

    -- Apply cancellation data state: mark cancelled + detach (no deletions)
    update public.nhsp_shifts ns2
    set
      cancelled_at_utc = v_now,
      cancelled_by_import_id = p_import_id,
      cancelled_reason = v_reason,
      timesheet_id = null
    where ns2.id = v_shift_id;

    v_cancelled_count := v_cancelled_count + 1;

    -- TSFIN recompute is mandatory for any cancel/detach/change.
    -- We mark current TSFIN stale and enqueue priority recompute.
    if v_timesheet_id is not null then
      update public.timesheets_financials tf
      set
        is_stale = true,
        stale_reason = 'IMPORT_CANCEL_DETACH',
        updated_at = v_now
      where tf.is_current = true
        and tf.timesheet_id = v_timesheet_id;

      perform public.enqueue_ts_financials_priority(array[v_timesheet_id]::uuid[], 'CONTEXT_CHANGED'::public.ts_fin_reason_enum);
    end if;

  end loop;

  -- Deduplicate id arrays for output
  select coalesce(array_agg(distinct x), array[]::uuid[])
  into v_timesheet_ids
  from unnest(v_timesheet_ids) x
  where x is not null;

  select coalesce(array_agg(distinct x), array[]::uuid[])
  into v_invoice_ids
  from unnest(v_invoice_ids) x
  where x is not null;

  select coalesce(array_agg(distinct x), array[]::uuid[])
  into v_credit_note_ids
  from unnest(v_credit_note_ids) x
  where x is not null;

  return jsonb_build_object(
    'import_id', p_import_id,
    'cancelled_count', v_cancelled_count,
    'affected_timesheet_ids', to_jsonb(v_timesheet_ids),
    'affected_invoice_ids', to_jsonb(v_invoice_ids),
    'credit_note_ids_created', to_jsonb(v_credit_note_ids),
    'invoice_pdf_jobs_enqueued', v_pdf_jobs_enqueued
  );
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
  v_now timestamptz := now();
  v_actions jsonb := coalesce(p_actions, '[]'::jsonb);

  v_import record;

  v_results jsonb := '[]'::jsonb;

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
  v_client_id uuid;
  v_recipient_email text;

  v_already boolean := false;
begin
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

  if upper(coalesce(v_import.import_scope::text,'')) <> 'HR_WEEKLY' then
    raise exception 'hr_weekly_validation_apply_send_emails: import % is not HR_WEEKLY (import_scope=%)', p_import_id, v_import.import_scope;
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
      v_reason_code := 'actual_hours_mismatch';
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
      select 1 from public.hr_issue_emails e
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

    select
      c.id,
      c.client_id
    into
      v_contract_id,
      v_client_id
    from public.contracts c
    where c.id = v_contract_id
    limit 1;

    if v_client_id is null then
      v_results := v_results || jsonb_build_object(
        'timesheet_id', v_timesheet_id::text,
        'issue_fingerprint', v_issue_fingerprint,
        'ok', false,
        'error', 'CLIENT_NOT_FOUND'
      );
      continue;
    end if;

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

    -- Upsert hr_issue_emails (unique on issue_fingerprint). Re-email updates last_sent_at.
    insert into public.hr_issue_emails(
      source_system,
      import_id,
      client_id,
      timesheet_id,
      hr_row_id,
      staff_norm,
      hospital_norm,
      work_date,
      reason_code,
      issue_fingerprint,
      last_sent_at,
      created_at,
      updated_at
    )
    values (
      'HEALTHROSTER_WEEKLY',
      p_import_id,
      v_client_id,
      v_timesheet_id,
      null::uuid,
      v_staff_norm,
      v_hospital_norm,
      v_work_date,
      v_reason_code,
      v_issue_fingerprint,
      v_now,
      v_now,
      v_now
    )
    on conflict (issue_fingerprint)
    do update set
      last_sent_at = excluded.last_sent_at,
      updated_at = excluded.updated_at,
      import_id = excluded.import_id,
      client_id = excluded.client_id,
      timesheet_id = excluded.timesheet_id,
      staff_norm = excluded.staff_norm,
      hospital_norm = excluded.hospital_norm,
      work_date = excluded.work_date,
      reason_code = excluded.reason_code;

    -- Return a “job” for the backend to enqueue (backend will ensure PDF and insert mail_outbox)
    v_results := v_results || jsonb_build_object(
      'timesheet_id', v_timesheet_id::text,
      'client_id', v_client_id::text,
      'recipient_email', v_recipient_email,
      'issue_fingerprint', v_issue_fingerprint,
      'reason_code', v_reason_code,
      'email_kind', case when v_already then 'REEMAIL' else 'EMAIL' end,
      'ok', true
    );
  end loop;

  return jsonb_build_object(
    'import_id', p_import_id::text,
    'queued', jsonb_array_length(v_results),
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

  if upper(coalesce(v_import.import_scope::text,'')) <> 'HR_WEEKLY' then
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
  -- Flatten HR entries for overlap matching
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
  -- For each worker entry, find HR matches by overlap >= 1 minute
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
  -- Day set = union of days present in HR import totals or worker schedule totals for this timesheet-week
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
        when p.has_mismatch then
          ('HEALTHROSTER_WEEKLY|validation|' || p_import_id::text || '|' || p.timesheet_id::text || '|' || p.week_ending_date::text || '|' || p.overall_status || '|' || coalesce(p.sig_text,''))
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

          'overall_status', ws.overall_status,
          'has_mismatch', ws.has_mismatch,
          'failure_reasons', coalesce(ws.failure_reasons_json, '[]'::jsonb),

          'issue_fingerprint', ws.issue_fingerprint,
          'emailed_already', ws.emailed_already,
          'can_email', (ws.has_mismatch and v_recipient_email is not null and length(btrim(v_recipient_email)) > 0),

          'days', ws.days_json
        )
        order by ws.week_ending_date asc, ws.candidate_name nulls last, ws.timesheet_id::text
      ),
      '[]'::jsonb
    )
  into v_rows
  from with_sent ws;

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

