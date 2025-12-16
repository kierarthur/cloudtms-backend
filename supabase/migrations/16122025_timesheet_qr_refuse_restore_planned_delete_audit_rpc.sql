-- ============================================================
-- QR refusal + restore + planned-week delete + audit feed
-- ============================================================

-- Safety: ensure stable search path for SECURITY DEFINER
-- (You can remove SECURITY DEFINER if you prefer RLS-only RPCs.)
-- ============================================================

-- ------------------------------------------------------------
-- 1) Refuse QR hours: rotate current TS to history + reset to
--    "pre-hours-submitted" QR Scenario 1, reset TSFIN, audit.
-- ------------------------------------------------------------
create or replace function public.timesheet_qr_refuse_and_reset(
  p_timesheet_id uuid,
  p_reason text,
  p_actor_user_id uuid
)
returns table (
  timesheet_id uuid,
  old_version int,
  new_version int,
  sheet_scope text,
  submission_mode text,
  qr_status text,
  qr_token text,
  processing_status text
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_ts public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_new_version int;
begin
  if p_timesheet_id is null then
    raise exception 'timesheet_id is required';
  end if;

  -- Lock current TSFIN row (if present) and block if paid/invoiced
  select *
  into v_fin
  from public.timesheets_financials
  where timesheet_id = p_timesheet_id
    and is_current = true
  for update;

  if found then
    if v_fin.locked_by_invoice_id is not null or v_fin.paid_at_utc is not null then
      raise exception 'Cannot refuse hours: timesheet is invoiced/locked or paid';
    end if;
  end if;

  -- Lock current timesheet version
  select *
  into v_ts
  from public.timesheets
  where timesheet_id = p_timesheet_id
    and is_current = true
  for update;

  if not found then
    raise exception 'Timesheet not found or not current';
  end if;

  v_new_version := coalesce(v_ts.version, 1) + 1;

  -- 1) Rotate current version -> history (keep evidence + QR metadata, but invalidate token)
  update public.timesheets
  set
    is_current      = false,
    status          = 'REVOKED'::public.timesheet_status_enum,
    revoked_reason  = nullif(btrim(p_reason), ''),
    revoked_by      = case when p_actor_user_id is null then null else p_actor_user_id::text end,
    -- invalidate QR for that historical version so any later scan is rejected
    qr_status       = 'CANCELLED'::public.timesheet_qr_status_enum,
    updated_at      = v_now
  where timesheet_id = p_timesheet_id
    and is_current = true;

  -- 2) Insert new current version = "pre-hours-submitted" QR Scenario 1
  --    (QR enabled, but no token and no hours/evidence)
  insert into public.timesheets (
    timesheet_id,
    booking_id,
    version,
    is_current,
    status,
    revoked_reason,
    revoked_by,
    contract_id,
    submission_mode,
    line_type,
    sheet_scope,

    occupant_key_norm,
    hospital_norm,
    ward_norm,
    job_title_norm,
    shift_label_norm,
    week_ending_date,

    -- Daily worked/break fields
    worked_start_iso,
    worked_end_iso,
    break_start_iso,
    break_end_iso,
    break_minutes,

    -- Weekly schedule / extras
    actual_schedule_json,
    additional_units_week,
    additional_units_per_day,

    -- Evidence / references / auth
    manual_pdf_r2_key,
    authorised_at_server,
    reference_number,
    day_references_json,

    -- QR fields (Scenario 1)
    qr_token,
    qr_status,
    qr_payload_json,
    qr_generated_at,
    qr_scanned_at,
    qr_scan_info_json,
    qr_r2_key,

    created_at,
    updated_at
  )
  select
    v_ts.timesheet_id,
    v_ts.booking_id,
    v_new_version,
    true,
    'RECEIVED'::public.timesheet_status_enum,
    null,
    null,
    v_ts.contract_id,
    v_ts.submission_mode,
    v_ts.line_type,
    v_ts.sheet_scope,

    v_ts.occupant_key_norm,
    v_ts.hospital_norm,
    v_ts.ward_norm,
    v_ts.job_title_norm,
    v_ts.shift_label_norm,
    v_ts.week_ending_date,

    -- Daily: clear all worked times/breaks
    null::timestamptz,
    null::timestamptz,
    null::timestamptz,
    null::timestamptz,
    null::int,

    -- Weekly: clear schedule
    null::jsonb,
    coalesce(v_ts.additional_units_week, '{}'::jsonb),
    coalesce(v_ts.additional_units_per_day, '{}'::jsonb),

    -- Clear evidence + auth + refs for a clean re-submit
    null::text,
    null::timestamptz,
    null::text,
    null::jsonb,

    -- QR Scenario 1
    null::text,
    'PENDING'::public.timesheet_qr_status_enum,
    '{}'::jsonb,          -- qr_payload_json is NOT NULL in schema :contentReference[oaicite:6]{index=6}
    null::timestamptz,
    null::timestamptz,
    null::jsonb,
    null::text,

    v_now,
    v_now;

  -- 3) Reset TSFIN in-place (safe: avoids having to list 80+ columns for insert)
  if v_fin.id is not null then
    update public.timesheets_financials
    set
      timesheet_version       = v_new_version,
      processing_status       = 'UNASSIGNED'::public.ts_fin_processing_status_enum,

      -- Hours MUST remain NOT NULL (set to 0 rather than NULL) :contentReference[oaicite:7]{index=7}
      hours_day               = 0,
      hours_night             = 0,
      hours_sat               = 0,
      hours_sun               = 0,
      hours_bh                = 0,
      total_hours             = 0,

      total_pay_ex_vat        = 0,
      total_charge_ex_vat     = 0,
      margin_ex_vat           = 0,
      additional_pay_ex_vat   = 0,
      additional_charge_ex_vat= 0,
      additional_margin_ex_vat= 0,

      authorised_at_utc       = null,
      authorised_by_user_id   = null,
      locked_by_invoice_id    = null,
      locked_at_utc           = null,
      paid_at_utc             = null,
      paid_by_user_id         = null,
      payment_reference       = null,

      -- keep policy_snapshot_json / rate_source_refs_json as-is (NOT NULL constraints)
      updated_at              = v_now
    where id = v_fin.id;

    -- 4) Enqueue TSFIN recompute (optional but useful)
    insert into public.ts_financials_outbox(timesheet_id, reason, attempt_count, next_attempt_at, last_error, created_at)
    values (p_timesheet_id, 'REVOKED'::public.ts_fin_reason_enum, 0, v_now, null, v_now)
    on conflict (timesheet_id, reason) do nothing;
  end if;

  -- 5) Audit event (timesheet)
  insert into public.audit_events(
    object_type,
    object_id_text,
    action,
    before_json,
    after_json,
    reason,
    actor_user_id,
    ts_utc
  )
  values(
    'timesheet',
    p_timesheet_id::text,
    'QR_HOURS_REFUSED',
    jsonb_build_object('timesheet_id', p_timesheet_id::text, 'old_version', v_ts.version),
    jsonb_build_object('timesheet_id', p_timesheet_id::text, 'new_version', v_new_version),
    nullif(btrim(p_reason), ''),
    p_actor_user_id,
    v_now
  );

  timesheet_id := p_timesheet_id;
  old_version := v_ts.version;
  new_version := v_new_version;
  sheet_scope := v_ts.sheet_scope::text;
  submission_mode := v_ts.submission_mode::text;
  qr_status := 'PENDING';
  qr_token := null;
  processing_status := 'UNASSIGNED';
  return next;
end;
$$;


-- ------------------------------------------------------------
-- 2) Restore a previously revoked QR version (pending or signed)
-- ------------------------------------------------------------
create or replace function public.timesheet_qr_restore_version(
  p_timesheet_id uuid,
  p_restore_kind text,  -- 'PENDING' or 'SIGNED'
  p_actor_user_id uuid
)
returns table (
  timesheet_id uuid,
  restored_version int,
  sheet_scope text,
  submission_mode text,
  qr_status text,
  qr_token text,
  restored_has_signed_pdf boolean
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_current public.timesheets%rowtype;
  v_restore public.timesheets%rowtype;
  v_fin_id uuid;
begin
  if p_timesheet_id is null then
    raise exception 'timesheet_id is required';
  end if;

  if upper(coalesce(p_restore_kind,'')) not in ('PENDING','SIGNED') then
    raise exception 'restore_kind must be PENDING or SIGNED';
  end if;

  -- Lock current
  select *
  into v_current
  from public.timesheets
  where timesheet_id = p_timesheet_id
    and is_current = true
  for update;

  if not found then
    raise exception 'Timesheet not found or not current';
  end if;

  -- Block restore if invoiced/paid
  select id
  into v_fin_id
  from public.timesheets_financials
  where timesheet_id = p_timesheet_id
    and is_current = true
    and (locked_by_invoice_id is not null or paid_at_utc is not null)
  limit 1;

  if v_fin_id is not null then
    raise exception 'Cannot restore: timesheet is invoiced/locked or paid';
  end if;

  -- Pick the target version to restore
  if upper(p_restore_kind) = 'PENDING' then
    -- "pending" = not scanned / not signed
    select *
    into v_restore
    from public.timesheets
    where timesheet_id = p_timesheet_id
      and is_current = false
      and status = 'REVOKED'::public.timesheet_status_enum
      and (qr_scanned_at is null)
    order by version desc
    limit 1;
  else
    -- "signed" = scanned_at present (and typically manual_pdf_r2_key populated with signed PDF)
    select *
    into v_restore
    from public.timesheets
    where timesheet_id = p_timesheet_id
      and is_current = false
      and status = 'REVOKED'::public.timesheet_status_enum
      and (qr_scanned_at is not null)
    order by version desc
    limit 1;
  end if;

  if not found then
    raise exception 'No matching revoked version found to restore';
  end if;

  -- Flip current -> not current
  update public.timesheets
  set
    is_current = false,
    updated_at = v_now
  where timesheet_id = p_timesheet_id
    and version = v_current.version
    and is_current = true;

  -- Flip restore -> current
  update public.timesheets
  set
    is_current = true,
    updated_at = v_now
  where timesheet_id = p_timesheet_id
    and version = v_restore.version;

  -- Update TSFIN snapshot version marker and enqueue recompute
  update public.timesheets_financials
  set
    timesheet_version = v_restore.version,
    updated_at = v_now
  where timesheet_id = p_timesheet_id
    and is_current = true;

  insert into public.ts_financials_outbox(timesheet_id, reason, attempt_count, next_attempt_at, last_error, created_at)
  values (p_timesheet_id, 'VERSION_ROTATED'::public.ts_fin_reason_enum, 0, v_now, null, v_now)
  on conflict (timesheet_id, reason) do nothing;

  -- Audit
  insert into public.audit_events(
    object_type, object_id_text, action, before_json, after_json, reason, actor_user_id, ts_utc
  )
  values(
    'timesheet',
    p_timesheet_id::text,
    'QR_RESTORED',
    jsonb_build_object('from_version', v_current.version),
    jsonb_build_object('to_version', v_restore.version, 'kind', upper(p_restore_kind)),
    null,
    p_actor_user_id,
    v_now
  );

  timesheet_id := p_timesheet_id;
  restored_version := v_restore.version;
  sheet_scope := v_restore.sheet_scope::text;
  submission_mode := v_restore.submission_mode::text;
  qr_status := coalesce(v_restore.qr_status::text, null);
  qr_token := v_restore.qr_token;
  restored_has_signed_pdf := (v_restore.manual_pdf_r2_key is not null and btrim(v_restore.manual_pdf_r2_key) <> '');
  return next;
end;
$$;


-- ------------------------------------------------------------
-- 3) Delete a planned-only contract week (no timesheet_id)
-- ------------------------------------------------------------
create or replace function public.contract_week_delete_planned(
  p_contract_week_id uuid,
  p_actor_user_id uuid
)
returns table (
  deleted boolean,
  contract_week_id uuid
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_cw public.contract_weeks%rowtype;
begin
  if p_contract_week_id is null then
    raise exception 'contract_week_id is required';
  end if;

  select *
  into v_cw
  from public.contract_weeks
  where id = p_contract_week_id
  for update;

  if not found then
    raise exception 'contract_week not found';
  end if;

  if v_cw.timesheet_id is not null then
    raise exception 'Not a planned-only week: timesheet_id is present';
  end if;

  delete from public.contract_weeks
  where id = p_contract_week_id;

  insert into public.audit_events(
    object_type, object_id_text, action, before_json, after_json, reason, actor_user_id, ts_utc
  )
  values(
    'contract_week',
    p_contract_week_id::text,
    'CONTRACT_WEEK_DELETED_PLANNED',
    jsonb_build_object('contract_id', v_cw.contract_id, 'week_ending_date', v_cw.week_ending_date, 'additional_seq', v_cw.additional_seq),
    null,
    null,
    p_actor_user_id,
    v_now
  );

  deleted := true;
  contract_week_id := p_contract_week_id;
  return next;
end;
$$;


-- ------------------------------------------------------------
-- 4) Timesheet audit feed (timesheet + related contract_week)
-- ------------------------------------------------------------
create or replace function public.timesheet_audit_feed(
  p_timesheet_id uuid
)
returns table (
  id uuid,
  ts_utc timestamptz,
  object_type text,
  object_id_text text,
  action text,
  reason text,
  actor_user_id uuid,
  actor_display text,
  actor_role_at_time text,
  before_json jsonb,
  after_json jsonb,
  correlation_id text
)
language sql
security definer
set search_path = public
as $$
  with cw as (
    select cw.id::text as contract_week_id_text
    from public.contract_weeks cw
    where cw.timesheet_id = p_timesheet_id
    order by cw.week_ending_date desc, cw.additional_seq desc
    limit 1
  )
  select
    a.id,
    a.ts_utc,
    a.object_type,
    a.object_id_text,
    a.action,
    a.reason,
    a.actor_user_id,
    a.actor_display,
    a.actor_role_at_time,
    a.before_json,
    a.after_json,
    a.correlation_id
  from public.audit_events a
  where
    (
      a.object_id_text = p_timesheet_id::text
      and a.object_type in ('timesheet','timesheets')
    )
    or
    (
      exists (select 1 from cw)
      and a.object_id_text = (select contract_week_id_text from cw)
      and a.object_type in ('contract_week','contract_weeks')
    )
  order by a.ts_utc desc, a.id desc;
$$;
