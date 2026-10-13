create or replace function public.invoice_create_credit_note_and_unlock(
  p_invoice_id uuid,
  p_actor_user_id uuid
)
returns table (
  credit_note_id uuid,
  unlocked_snapshots int
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();

  v_inv record;
  v_base_hdr jsonb := '{}'::jsonb;

  v_original_issued_at timestamptz;
  v_anchor_ymd date;

  v_stationery_key text;
  v_margins jsonb;
  v_hide_bank_footer boolean;

  v_bank jsonb;
  v_vat_reg text;

  v_client_name text;
  v_client_addr text;
  v_client_email text;
  v_vat_chargeable boolean;
  v_terms_days int;

  v_applied_vat numeric;
  v_global_vat numeric := 20;
  v_client_vat_override numeric;

  v_due_at timestamptz;

  v_credit_id uuid;

  v_ts_ids uuid[];

  v_cn_ex numeric := 0;
  v_cn_vat numeric := 0;
  v_cn_inc numeric := 0;
begin
  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  -- Load original invoice
  select *
  into v_inv
  from public.invoices
  where id = p_invoice_id for update;

  if not found then
    raise exception 'Invoice not found';
  end if;

  -- Final physical legacy writer shares the original-I1 lock with async credits and Unissue.
  if exists(select 1 from public.invoices cn where cn.original_invoice_id=p_invoice_id
    and cn.type='CREDIT_NOTE' and cn.status in('DRAFT','ISSUED','PAID')) then
    raise exception 'CREDIT_ALREADY_EXISTS' using errcode='P0001';
  end if;

  if exists(select 1 from public.invoice_lines line where line.invoice_id=p_invoice_id
    and line.timesheet_id is not null and private.timesheet_correction_origin_v1(line.timesheet_id) is not null)
    or exists(select 1 from private.timesheet_correction_requests request where request.original_invoice_id=p_invoice_id) then
    raise exception 'CREDIT_PROTECTED_ORIGIN_REQUIRES_SCOPED_PIPELINE' using errcode='55000';
  end if;

  if v_inv.type::text = 'CREDIT_NOTE' then
    raise exception 'Cannot credit a CREDIT_NOTE';
  end if;

  if jsonb_typeof(v_inv.header_snapshot_json) = 'object' then
    v_base_hdr := v_inv.header_snapshot_json;
  end if;

  -- Original issued time (prefer invoice.issued_at_utc, else snapshot.issued_at_utc, else now)
  v_original_issued_at := v_inv.issued_at_utc;
  if v_original_issued_at is null and (v_base_hdr ? 'issued_at_utc') then
    begin
      v_original_issued_at := (v_base_hdr->>'issued_at_utc')::timestamptz;
    exception when others then
      v_original_issued_at := null;
    end;
  end if;
  if v_original_issued_at is null then
    v_original_issued_at := v_now;
  end if;

  v_anchor_ymd := (v_original_issued_at at time zone 'Europe/London')::date;

  -- Stationery key
  v_stationery_key := nullif(btrim(coalesce(v_base_hdr->>'stationery_key','')), '');
  if v_stationery_key is null then
    v_stationery_key := 'Assets/Stationery/Letterhead/A4/Letterhead_v1@300dpi.png';
  end if;

  if right(lower(v_stationery_key), 4) = '.pdf' then
    v_stationery_key := left(v_stationery_key, length(v_stationery_key) - 4) || '@300dpi.png';
  end if;

  while left(v_stationery_key, 1) = '/' loop
    v_stationery_key := substr(v_stationery_key, 2);
  end loop;

  -- Margins
  v_margins := v_base_hdr->'stationery_margins_mm';
  if jsonb_typeof(v_margins) = 'array' and jsonb_array_length(v_margins) = 4 then
    v_margins := jsonb_build_object(
      'top',    coalesce((v_margins->>0)::numeric, 32),
      'right',  coalesce((v_margins->>1)::numeric, 12),
      'bottom', coalesce((v_margins->>2)::numeric, 20),
      'left',   coalesce((v_margins->>3)::numeric, 12)
    );
  elsif jsonb_typeof(v_margins) = 'object' then
    v_margins := jsonb_build_object(
      'top',    coalesce((v_margins->>'top')::numeric, 32),
      'right',  coalesce((v_margins->>'right')::numeric, 12),
      'bottom', coalesce((v_margins->>'bottom')::numeric, 20),
      'left',   coalesce((v_margins->>'left')::numeric, 12)
    );
  else
    v_margins := jsonb_build_object('top',32,'right',12,'bottom',20,'left',12);
  end if;

  -- hide_bank_footer default TRUE
  if jsonb_typeof(v_base_hdr->'hide_bank_footer') = 'boolean' then
    v_hide_bank_footer := (v_base_hdr->>'hide_bank_footer')::boolean;
  else
    v_hide_bank_footer := true;
  end if;

  -- Bank + VAT registration
  if jsonb_typeof(v_base_hdr->'bank') = 'object' then
    v_bank := v_base_hdr->'bank';
  else
    v_bank := null;
  end if;

  v_vat_reg := nullif(btrim(coalesce(v_base_hdr->>'vat_registration_number','')), '');

  if v_bank is null or v_vat_reg is null then
    declare
      v_def record;
    begin
      select bank_name, bank_sort_code, bank_account_number, vat_registration_number
      into v_def
      from public.settings_defaults
      where id = 1
      limit 1;

      if v_bank is null then
        v_bank := jsonb_build_object(
          'name', v_def.bank_name,
          'sort_code', v_def.bank_sort_code,
          'account_number', v_def.bank_account_number
        );
      end if;

      if v_vat_reg is null then
        v_vat_reg := v_def.vat_registration_number;
      end if;
    end;
  end if;

  -- Client info
  v_client_name  := nullif(btrim(coalesce(v_base_hdr->>'client_name','')), '');
  v_client_addr  := nullif(btrim(coalesce(v_base_hdr->>'client_invoice_address','')), '');
  v_client_email := nullif(btrim(coalesce(v_base_hdr->>'client_primary_invoice_email','')), '');

  if jsonb_typeof(v_base_hdr->'vat_chargeable') = 'boolean' then
    v_vat_chargeable := (v_base_hdr->>'vat_chargeable')::boolean;
  else
    v_vat_chargeable := null;
  end if;

  if (v_base_hdr ? 'payment_terms_days') then
    begin
      v_terms_days := (v_base_hdr->>'payment_terms_days')::int;
    exception when others then
      v_terms_days := null;
    end;
  else
    v_terms_days := null;
  end if;

  if v_client_name is null or v_client_addr is null or v_vat_chargeable is null or v_terms_days is null then
    declare
      v_cli record;
    begin
      select name, invoice_address, primary_invoice_email, vat_chargeable, payment_terms_days
      into v_cli
      from public.clients
      where id = v_inv.client_id
      limit 1;

      if v_client_name is null then v_client_name := v_cli.name; end if;
      if v_client_addr is null then v_client_addr := v_cli.invoice_address; end if;
      if v_client_email is null then v_client_email := v_cli.primary_invoice_email; end if;

      if v_vat_chargeable is null then
        v_vat_chargeable := coalesce(v_cli.vat_chargeable, true);
      end if;

      if v_terms_days is null then
        v_terms_days := coalesce(v_cli.payment_terms_days, 30);
      end if;
    end;
  end if;

  -- VAT % (prefer original snapshot applied_vat_rate_pct; else compute anchored)
  v_applied_vat := null;
  if (v_base_hdr ? 'applied_vat_rate_pct') then
    begin
      v_applied_vat := (v_base_hdr->>'applied_vat_rate_pct')::numeric;
    exception when others then
      v_applied_vat := null;
    end;
  end if;

  if v_applied_vat is null then
    select coalesce(sf.vat_rate_pct, 20)
    into v_global_vat
    from public.settings_finance_pick(v_anchor_ymd) sf
    limit 1;

    select cs.vat_rate_pct
    into v_client_vat_override
    from public.client_settings cs
    where cs.client_id = v_inv.client_id
      and cs.effective_from <= v_anchor_ymd
    order by cs.effective_from desc
    limit 1;

    v_applied_vat := case
      when v_vat_chargeable = false then 0
      else coalesce(v_client_vat_override, v_global_vat, 20)
    end;
  else
    if v_vat_chargeable = false then
      v_applied_vat := 0;
    end if;
  end if;

  v_due_at := v_now + make_interval(days => coalesce(v_terms_days, 30));

  -- Create the credit note invoice row
  insert into public.invoices (
    client_id,
    type,
    status,
    status_date_utc,
    issued_at_utc,
    due_at_utc,
    subtotal_ex_vat,
    vat_amount,
    total_inc_vat,
    original_invoice_id,
    header_snapshot_json
  )
  values (
    v_inv.client_id,
    'CREDIT_NOTE'::public.invoice_type_enum,
    'ISSUED'::public.invoice_status_enum,
    v_now,
    v_now,
    v_due_at,
    0,
    0,
    0,
    v_inv.id,
    jsonb_build_object(
      'client_id', v_inv.client_id::text,
      'client_name', v_client_name,
      'client_invoice_address', v_client_addr,
      'client_primary_invoice_email', v_client_email,
      'vat_chargeable', coalesce(v_vat_chargeable, true),
      'applied_vat_rate_pct', coalesce(v_applied_vat, 0),
      'payment_terms_days', coalesce(v_terms_days, 30),
      'issued_at_utc', public._inv_iso_utc(v_now),
      'due_at_utc', public._inv_iso_utc(v_due_at),
      'stationery_key', v_stationery_key,
      'stationery_margins_mm', v_margins,
      'hide_bank_footer', v_hide_bank_footer,
      'bank', v_bank,
      'vat_registration_number', v_vat_reg,
      'meta', jsonb_build_object(
        'source', 'CREDIT_NOTE',
        'original_invoice_id', v_inv.id::text,
        'vat_anchor_ymd', v_anchor_ymd::text,
        'original_invoice_issued_at_utc', public._inv_iso_utc(v_original_issued_at)
      )
    )
  )
  returning id into v_credit_id;

  -- ✅ Insert negative mirror lines (one-for-one from original invoice_lines)
  insert into public.invoice_lines(
    invoice_id, timesheet_id, booking_id, description,
    hours_day, hours_night, hours_sat, hours_sun, hours_bh,
    pay_day, pay_night, pay_sat, pay_sun, pay_bh,
    charge_day, charge_night, charge_sat, charge_sun, charge_bh,
    total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
    vat_rate_pct, vat_amount, total_inc_vat,
    paper_ts_r2_key, meta_json, source_key
  )
  select
    v_credit_id,
    l.timesheet_id,
    l.booking_id,
    ('CREDIT NOTE – ' || coalesce(l.description,'')),

    l.hours_day, l.hours_night, l.hours_sat, l.hours_sun, l.hours_bh,

    l.pay_day, l.pay_night, l.pay_sat, l.pay_sun, l.pay_bh,
    l.charge_day, l.charge_night, l.charge_sat, l.charge_sun, l.charge_bh,

    public._inv_round2(-1 * coalesce(l.total_pay_ex_vat,0)),
    public._inv_round2(-1 * coalesce(l.total_charge_ex_vat,0)),
    public._inv_round2(-1 * coalesce(l.margin_ex_vat,0)),

    l.vat_rate_pct,
    public._inv_round2(-1 * coalesce(l.vat_amount,0)),
    public._inv_round2(-1 * coalesce(l.total_inc_vat,0)),

    l.paper_ts_r2_key,

    (coalesce(l.meta_json,'{}'::jsonb) ||
      jsonb_build_object(
        'credit_note', true,
        'original_invoice_id', v_inv.id::text,
        'original_invoice_line_id', l.id::text
      )
    ),

    ('CN:' || v_credit_id::text || ':LINE:' || l.id::text)
  from public.invoice_lines l
  where l.invoice_id = p_invoice_id;

  -- Update credit note totals from its lines
  select
    coalesce(sum(l2.total_charge_ex_vat),0)::numeric,
    coalesce(sum(l2.vat_amount),0)::numeric,
    coalesce(sum(l2.total_inc_vat),0)::numeric
  into v_cn_ex, v_cn_vat, v_cn_inc
  from public.invoice_lines l2
  where l2.invoice_id = v_credit_id;

  update public.invoices
  set
    subtotal_ex_vat = public._inv_round2(v_cn_ex),
    vat_amount      = public._inv_round2(v_cn_vat),
    total_inc_vat   = public._inv_round2(v_cn_inc),
    updated_at      = v_now
  where id = v_credit_id;

  -- Audit credit note creation (includes totals)
  perform public._audit_insert(
    'invoice',
    v_credit_id::text,
    'CREDIT_NOTE_CREATED',
    null,
    jsonb_build_object(
      'credit_note_id', v_credit_id::text,
      'original_invoice_id', v_inv.id::text,
      'subtotal_ex_vat', public._inv_round2(v_cn_ex),
      'vat_amount', public._inv_round2(v_cn_vat),
      'total_inc_vat', public._inv_round2(v_cn_inc)
    ),
    null,
    p_actor_user_id
  );

  -- Unlock snapshots locked by the original invoice
  select array_agg(distinct tf.timesheet_id)
  into v_ts_ids
  from public.timesheets_financials tf
  where tf.is_current = true
    and tf.locked_by_invoice_id = p_invoice_id
    and tf.timesheet_id is not null;

  unlocked_snapshots := coalesce(array_length(v_ts_ids, 1), 0);

  if unlocked_snapshots > 0 then
    update public.timesheets_financials tf
    set locked_by_invoice_id = null,
        locked_at_utc = null,
        unlocked_by_credit_note_id = v_credit_id,
        is_stale = true,
        stale_reason = 'UNLOCKED_BY_CREDIT',
        updated_at = v_now
    where tf.is_current = true
      and tf.locked_by_invoice_id = p_invoice_id;

    -- Enqueue recompute (batch, idempotent)
    insert into public.ts_financials_outbox(timesheet_id, reason, attempt_count, next_attempt_at, last_error, created_at)
    select
      x.timesheet_id,
      'VERSION_ROTATED'::public.ts_fin_reason_enum,
      0,
      v_now,
      null,
      v_now
    from (select unnest(v_ts_ids) as timesheet_id) x
    on conflict on constraint uq_tsfin_outbox do nothing;

    perform public._audit_insert(
      'invoice',
      v_credit_id::text,
      'CREDIT_NOTE_UNLOCKED_SNAPSHOTS',
      null,
      jsonb_build_object(
        'credit_note_id', v_credit_id::text,
        'original_invoice_id', v_inv.id::text,
        'timesheet_ids', to_jsonb(coalesce(v_ts_ids, array[]::uuid[])),
        'unlocked_count', unlocked_snapshots
      ),
      null,
      p_actor_user_id
    );
  end if;

  credit_note_id := v_credit_id;
  return next;
end;
$$;
