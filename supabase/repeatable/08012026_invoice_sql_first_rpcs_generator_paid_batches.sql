-- ============================================================
-- CloudTMS: SQL-first invoice generator (supports HOURS + BY_WEEK)
-- RPC: public.invoice_generate_from_outbox_batch(p_outbox_ids uuid[], p_actor_user_id uuid)
--
-- 100% aligned to your pasted JS logic for:
-- - handleCreateInvoiceTsfin (HOURS)
-- - handleCreateInvoiceTsfinByWeek (BY_WEEK)
-- - extractBillableSegmentsForWeek
-- - lockSegmentsForInvoice
-- - findOrCreateSelfBillInvoice
-- - writeAudit semantics (but SQL has no req headers → ip/user_agent/correlation_id only from payload if provided)
--
-- Safe to re-run: all functions are CREATE OR REPLACE.
-- ============================================================

-- --------------------------
-- Helpers: rounding + ISO UTC
-- --------------------------
create or replace function public._inv_round2(n numeric)
returns numeric
language sql
immutable
as $$
  select round(coalesce(n,0)::numeric, 2);
$$;

create or replace function public._inv_iso_utc(ts timestamptz)
returns text
language sql
immutable
as $$
  select to_char((ts at time zone 'UTC'), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"');
$$;

-- ------------------------------------------
-- Helpers: schedule-aware reference gating
-- Mirrors handleCreateInvoiceTsfin local helpers
-- ------------------------------------------
create or replace function public._inv_day_refs_has_any(day_refs jsonb)
returns boolean
language sql
immutable
as $$
  select exists (
    select 1
    from jsonb_each_text(coalesce(day_refs, '{}'::jsonb)) e
    where btrim(coalesce(e.value,'')) <> ''
  );
$$;

create or replace function public._inv_weekly_manual_schedule_has_complete_refs(schedule jsonb)
returns boolean
language plpgsql
immutable
as $$
declare
  seg jsonb;
  v_start text;
  v_end   text;
  v_ref   text;
  n int;
begin
  if schedule is null or jsonb_typeof(schedule) <> 'array' then
    return false;
  end if;

  n := jsonb_array_length(schedule);
  if n <= 0 then
    return false;
  end if;

  for seg in
    select value from jsonb_array_elements(schedule) value
  loop
    if seg is null or jsonb_typeof(seg) <> 'object' then
      continue;
    end if;

    v_start := btrim(coalesce(seg->>'start',''));
    v_end   := btrim(coalesce(seg->>'end',''));

    if v_start <> '' and v_end <> '' then
      v_ref := btrim(coalesce(seg->>'ref_num',''));
      if v_ref = '' then
        return false;
      end if;
    end if;
  end loop;

  return true;
end;
$$;

create or replace function public._inv_timesheet_has_invoice_reference(
  p_sheet_scope text,
  p_submission_mode text,
  p_reference_number text,
  p_day_references_json jsonb,
  p_actual_schedule_json jsonb
)
returns boolean
language plpgsql
immutable
as $$
declare
  scope text := upper(btrim(coalesce(p_sheet_scope,'')));
  mode  text := upper(btrim(coalesce(p_submission_mode,'')));
begin
  if scope = 'WEEKLY' and mode = 'MANUAL' then
    return public._inv_weekly_manual_schedule_has_complete_refs(p_actual_schedule_json);
  end if;

  if scope = 'WEEKLY' then
    return (btrim(coalesce(p_reference_number,'')) <> '') or public._inv_day_refs_has_any(p_day_references_json);
  end if;

  return (btrim(coalesce(p_reference_number,'')) <> '');
end;
$$;

create or replace function public._inv_collect_weekly_manual_schedule_refs(
  p_sheet_scope text,
  p_submission_mode text,
  p_actual_schedule_json jsonb
)
returns jsonb
language plpgsql
immutable
as $$
declare
  scope text := upper(btrim(coalesce(p_sheet_scope,'')));
  mode  text := upper(btrim(coalesce(p_submission_mode,'')));
  seg jsonb;
  v_start text;
  v_end   text;
  v_ref   text;
  seen text[] := array[]::text[];
  out jsonb := '[]'::jsonb;
begin
  if not (scope = 'WEEKLY' and mode = 'MANUAL') then
    return '[]'::jsonb;
  end if;

  if p_actual_schedule_json is null or jsonb_typeof(p_actual_schedule_json) <> 'array' then
    return '[]'::jsonb;
  end if;

  for seg in
    select value from jsonb_array_elements(p_actual_schedule_json) value
  loop
    if seg is null or jsonb_typeof(seg) <> 'object' then
      continue;
    end if;

    v_start := btrim(coalesce(seg->>'start',''));
    v_end   := btrim(coalesce(seg->>'end',''));
    if not (v_start <> '' and v_end <> '') then
      continue;
    end if;

    v_ref := btrim(coalesce(seg->>'ref_num',''));
    if v_ref = '' then
      continue;
    end if;

    if v_ref = any(seen) then
      continue;
    end if;

    seen := array_append(seen, v_ref);
    out := out || jsonb_build_array(v_ref);
  end loop;

  return out;
end;
$$;

-- ------------------------------------------
-- Helper: write audit row (mirrors writeAudit, but no req headers)
-- ip/user_agent/correlation_id can be optionally supplied by payload.
-- ------------------------------------------
create or replace function public._inv_write_audit(
  p_actor_user_id uuid,
  p_action text,
  p_after_json jsonb,
  p_entity text,
  p_subject_id text,
  p_before_json jsonb default null,
  p_reason text default null,
  p_ip text default null,
  p_user_agent text default null,
  p_correlation_id text default null
)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_actor_display text := null;
  v_actor_role text := 'system';
begin
  if p_actor_user_id is not null then
    select
      nullif(btrim(coalesce(u.display_name,'')), ''),
      nullif(btrim(coalesce(u.role,'')), '')
    into v_actor_display, v_actor_role
    from public.tms_users u
    where u.id = p_actor_user_id
    limit 1;

    if v_actor_display is null then
      select nullif(btrim(coalesce(u.email,'')), '')
      into v_actor_display
      from public.tms_users u
      where u.id = p_actor_user_id
      limit 1;
    end if;
  end if;

  if v_actor_display is null then
    v_actor_display := 'CloudTMS server';
  end if;

  if v_actor_role is null then
    v_actor_role := 'system';
  end if;

  insert into public.audit_events(
    object_type,
    object_id_text,
    action,
    before_json,
    after_json,
    reason,
    actor_user_id,
    actor_display,
    actor_role_at_time,
    ip,
    user_agent,
    correlation_id
  )
  values (
    coalesce(nullif(btrim(p_entity),''), 'generic'),
    nullif(btrim(p_subject_id), ''),
    coalesce(nullif(btrim(p_action),''), 'EVENT'),
    p_before_json,
    p_after_json,
    p_reason,
    p_actor_user_id,
    v_actor_display,
    v_actor_role,
    nullif(btrim(p_ip), ''),
    nullif(btrim(p_user_agent), ''),
    nullif(btrim(p_correlation_id), '')
  );
end;
$$;

-- ------------------------------------------
-- Helper: lock ALL segments in invoice_breakdown_json (HOURS endpoint)
-- Matches handleCreateInvoiceTsfin step 7 (locks every seg without lock).
-- ------------------------------------------
create or replace function public._inv_lock_all_segments_json(p_ib jsonb, p_invoice_id uuid)
returns jsonb
language plpgsql
immutable
as $$
declare
  seg jsonb;
  out_segs jsonb := '[]'::jsonb;
begin
  if p_ib is null or jsonb_typeof(p_ib) <> 'object' then
    return p_ib;
  end if;

  if coalesce(p_ib->>'mode','') <> 'SEGMENTS' then
    return p_ib;
  end if;

  if jsonb_typeof(p_ib->'segments') <> 'array' then
    return p_ib;
  end if;

  for seg in
    select value from jsonb_array_elements(p_ib->'segments') value
  loop
    if seg is null or jsonb_typeof(seg) <> 'object' then
      out_segs := out_segs || jsonb_build_array(seg);
    else
      if nullif(coalesce(seg->>'invoice_locked_invoice_id',''), '') is null then
        seg := jsonb_set(seg, '{invoice_locked_invoice_id}', to_jsonb(p_invoice_id::text), true);
      end if;
      out_segs := out_segs || jsonb_build_array(seg);
    end if;
  end loop;

  return jsonb_set(p_ib, '{segments}', out_segs, true);
end;
$$;
-- ------------------------------------------
-- Helper: lock segments for BY_WEEK (exact mirror of lockSegmentsForInvoice JS)
-- segmentRefs_json must be jsonb array of objects: {tsfin_id, segment_id}
-- ------------------------------------------
create or replace function public._inv_lock_segments_for_invoice(p_invoice_id uuid, p_segment_refs jsonb)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now_iso text := public._inv_iso_utc(now());
  ref jsonb;
  v_tsfin_id uuid;

  -- per-tsfin
  v_lock_whole boolean;
  v_seg_ids text[];

  snap record;
  ib jsonb;
  basis text;
  is_selfbill_or_nhsp boolean;
  all_locked boolean;

  seg jsonb;
  segs_out jsonb;
  sid text;
  locked text;

  i int;

  -- =====================================================
  -- DEBUG (invoice_debug): single audit row per RPC call
  -- =====================================================
  v_invoice_debug boolean := false;
  v_dbg_started_at timestamptz := now();
  v_dbg_steps jsonb := '[]'::jsonb;
  v_dbg_sqlstate text := null;
  v_dbg_error text := null;
  v_dbg_stats jsonb := '{}'::jsonb;

  v_tsfins_seen int := 0;
  v_tsfins_updated int := 0;
  v_segments_newly_locked int := 0;
  v_summaries_set int := 0;
begin
  -- Load invoice_debug flag (safe even if column not yet present)
  begin
    select coalesce(sd.invoice_debug, false)
    into v_invoice_debug
    from public.settings_defaults sd
    where sd.id = 1
    limit 1;
  exception when undefined_column then
    v_invoice_debug := false;
  end;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','start',
        'at_utc', public._inv_iso_utc(v_dbg_started_at),
        'invoice_id', coalesce(p_invoice_id::text,'')
      )
    );
  end if;

  if p_segment_refs is null or jsonb_typeof(p_segment_refs) <> 'array' then
    return;
  end if;

  -- Build distinct tsfin_ids
  for ref in
    select value from jsonb_array_elements(p_segment_refs) value
  loop
    if jsonb_typeof(ref) <> 'object' then
      continue;
    end if;

    if nullif(coalesce(ref->>'tsfin_id',''), '') is null then
      continue;
    end if;
  end loop;

  -- Process each tsfin_id separately (mirror JS loop)
  for v_tsfin_id in
    select distinct (x->>'tsfin_id')::uuid
    from jsonb_array_elements(p_segment_refs) x
    where nullif(coalesce(x->>'tsfin_id',''), '') is not null
  loop
    v_tsfins_seen := v_tsfins_seen + 1;

    -- Gather ref set for this tsfin_id
    select
      bool_or(nullif(coalesce(x->>'segment_id',''), '') is null) as lock_whole,
      array_agg(distinct (x->>'segment_id')) filter (where nullif(coalesce(x->>'segment_id',''), '') is not null) as seg_ids
    into v_lock_whole, v_seg_ids
    from jsonb_array_elements(p_segment_refs) x
    where (x->>'tsfin_id')::uuid = v_tsfin_id;

    v_lock_whole := coalesce(v_lock_whole,false);
    v_seg_ids := coalesce(v_seg_ids, array[]::text[]);

    -- Load snapshot (✅ FIX: lock the tsfin row to prevent concurrent lost-updates)
    select tf.id, tf.basis, tf.locked_by_invoice_id, tf.invoice_breakdown_json
    into snap
    from public.timesheets_financials tf
    where tf.id = v_tsfin_id
    for update;

    if not found then
      continue;
    end if;

    ib := snap.invoice_breakdown_json;
    basis := upper(coalesce(snap.basis::text,''));

    is_selfbill_or_nhsp :=
      basis = 'NHSP' or
      basis = 'NHSP_ADJUSTMENT' or
      basis = 'HEALTHROSTER_SELF_BILL' or
      basis = 'HEALTHROSTER_ADJUSTMENT';

    all_locked := true;
    segs_out := null;

    if ib is not null
       and jsonb_typeof(ib) = 'object'
       and coalesce(ib->>'mode','') = 'SEGMENTS'
       and jsonb_typeof(ib->'segments') = 'array'
    then
      segs_out := '[]'::jsonb;

      for seg in
        select value from jsonb_array_elements(ib->'segments') value
      loop
        if seg is null or jsonb_typeof(seg) <> 'object' then
          segs_out := segs_out || jsonb_build_array(seg);

          -- ✅ FIX (defensive, does not affect valid data):
          -- If we are NOT explicitly locking whole, invalid segment elements must prevent
          -- whole-timesheet lock inference (avoids marking locked_by_invoice_id due to JSON nulls).
          if not v_lock_whole then
            all_locked := false;
          end if;

          continue;
        end if;

        sid := coalesce(seg->>'segment_id','');
        locked := nullif(coalesce(seg->>'invoice_locked_invoice_id',''), '');

        if v_lock_whole or (sid <> '' and sid = any(v_seg_ids)) then
          if locked is null then
            locked := p_invoice_id::text;
            v_segments_newly_locked := v_segments_newly_locked + 1;
          end if;
        end if;

        if locked is null then
          all_locked := false;
        end if;

    seg := jsonb_set(seg, '{invoice_locked_invoice_id}', coalesce(to_jsonb(locked), 'null'::jsonb), true);

        segs_out := segs_out || jsonb_build_array(seg);
      end loop;

      ib := jsonb_set(ib, '{segments}', segs_out, true);
    else
      -- No segments
      all_locked := (snap.locked_by_invoice_id is not null) or v_lock_whole;
    end if;

    if (not is_selfbill_or_nhsp) and all_locked then
      v_summaries_set := v_summaries_set + 1;
    end if;

    -- Patch
    if ib is not null then
      update public.timesheets_financials
      set
        updated_at = now(),
        invoice_breakdown_json = ib,
        locked_by_invoice_id = case
          when (not is_selfbill_or_nhsp) and all_locked then p_invoice_id
          else locked_by_invoice_id
        end,
        locked_at_utc = case
          when (not is_selfbill_or_nhsp) and all_locked then now()
          else locked_at_utc
        end
      where id = v_tsfin_id;
    else
      update public.timesheets_financials
      set
        updated_at = now(),
        locked_by_invoice_id = case
          when (not is_selfbill_or_nhsp) and all_locked then p_invoice_id
          else locked_by_invoice_id
        end,
        locked_at_utc = case
          when (not is_selfbill_or_nhsp) and all_locked then now()
          else locked_at_utc
        end
      where id = v_tsfin_id;
    end if;

    v_tsfins_updated := v_tsfins_updated + 1;
  end loop;

  if v_invoice_debug then
    begin
      v_dbg_stats := jsonb_build_object(
        'tsfins_seen', v_tsfins_seen,
        'tsfins_updated', v_tsfins_updated,
        'segments_newly_locked', v_segments_newly_locked,
        'summaries_set', v_summaries_set
      );

      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','finish',
          'at_utc', public._inv_iso_utc(now()),
          'stats', v_dbg_stats
        )
      );

      perform public._inv_write_audit(
        null,
        'INV_LOCK_SEGMENTS_DEBUG',
        jsonb_build_object(
          'invoice_id', coalesce(p_invoice_id::text,''),
          'stats', v_dbg_stats,
          'steps', v_dbg_steps
        ),
        'timesheets_financials',
        ('invoice:' || coalesce(p_invoice_id::text,'')),
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

exception when others then
  v_dbg_sqlstate := SQLSTATE;
  v_dbg_error := SQLERRM;

  if v_invoice_debug then
    begin
      v_dbg_stats := jsonb_build_object(
        'tsfins_seen', v_tsfins_seen,
        'tsfins_updated', v_tsfins_updated,
        'segments_newly_locked', v_segments_newly_locked,
        'summaries_set', v_summaries_set
      );

      perform public._inv_write_audit(
        null,
        'INV_LOCK_SEGMENTS_ERROR',
        jsonb_build_object(
          'invoice_id', coalesce(p_invoice_id::text,''),
          'sqlstate', v_dbg_sqlstate,
          'error', v_dbg_error,
          'stats', v_dbg_stats,
          'steps', v_dbg_steps
        ),
        'timesheets_financials',
        ('invoice:' || coalesce(p_invoice_id::text,'')),
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

  raise;
end;
$$;

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
  where id = p_invoice_id;

  if not found then
    raise exception 'Invoice not found';
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
