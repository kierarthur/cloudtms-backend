-- ============================================================
-- QR refusal + restore + planned-week delete + audit feed
-- ============================================================
--
-- FIXES (rotation-safe, booking_id is series key; timesheet_id is row PK):
-- 1) timesheet_qr_refuse_and_reset:
--    - Accepts a stale/historical p_timesheet_id.
--    - Resolves booking_id, locks CURRENT row, rotates CURRENT row to history.
--    - Inserts a NEW current row (new timesheet_id).
--    - Moves the CURRENT TSFIN row to the NEW timesheet_id and resets fields.
--    - Enqueues outbox for the NEW timesheet_id.
--    - Writes audit against the NEW current timesheet_id (and includes old id/version in before_json).
--
-- 2) timesheet_qr_restore_version:
--    - Accepts a stale/historical p_timesheet_id.
--    - Resolves booking_id, locks CURRENT row, promotes a chosen revoked version to current.
--    - Moves contract_weeks pointer (if present) to the restored timesheet_id.
--    - Moves the CURRENT TSFIN row to the restored timesheet_id and updates version marker.
--    - Enqueues outbox for the restored timesheet_id.
--    - Writes audit against the restored (new current) timesheet_id.
--
-- 3) contract_week_delete_planned:
--    - unchanged
--
-- 4) timesheet_audit_feed:
--    - Accepts any timesheet_id (stale or current).
--    - Resolves booking_id and returns audit for ALL timesheet_ids in that booking series.
--    - Also includes related contract_week audit (by current contract_weeks pointer).
--
-- Safety: stable search_path for SECURITY DEFINER.
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

  v_any public.timesheets%rowtype;
  v_current public.timesheets%rowtype;

  v_fin public.timesheets_financials%rowtype;

  v_booking_id uuid;
  v_new_version int;
  v_new_timesheet_id uuid;
begin
  if p_timesheet_id is null then
    raise exception 'timesheet_id is required';
  end if;

  -- 0) Resolve booking_id from ANY row (stale or current)
  select *
  into v_any
  from public.timesheets
  where timesheet_id = p_timesheet_id
  limit 1;

  if not found then
    raise exception 'Timesheet not found';
  end if;

  v_booking_id := v_any.booking_id;
  if v_booking_id is null then
    raise exception 'Timesheet booking_id is missing; cannot rotate versions';
  end if;

  -- 1) Lock CURRENT timesheet row for this booking_id
  select *
  into v_current
  from public.timesheets
  where booking_id = v_booking_id
    and is_current = true
  for update;

  if not found then
    raise exception 'Current timesheet not found for booking_id';
  end if;

  -- 2) Lock CURRENT TSFIN row (keyed to the CURRENT timesheet_id) and block if paid/invoiced
  select *
  into v_fin
  from public.timesheets_financials
  where timesheet_id = v_current.timesheet_id
    and is_current = true
  for update;

  if found then
    if v_fin.locked_by_invoice_id is not null or v_fin.paid_at_utc is not null then
      raise exception 'Cannot refuse hours: timesheet is invoiced/locked or paid';
    end if;
  end if;

  v_new_version := coalesce(v_current.version, 1) + 1;

  -- 3) Rotate CURRENT version -> history (invalidate QR on that historical version)
  update public.timesheets
  set
    is_current      = false,
    status          = 'REVOKED'::public.timesheet_status_enum,
    revoked_reason  = nullif(btrim(p_reason), ''),
    revoked_by      = case when p_actor_user_id is null then null else p_actor_user_id::text end,
    qr_status       = 'CANCELLED'::public.timesheet_qr_status_enum,
    updated_at      = v_now
  where timesheet_id = v_current.timesheet_id
    and is_current = true;

  -- 4) Insert NEW current version = "pre-hours-submitted" QR Scenario 1
  --    NOTE: do NOT supply timesheet_id (row PK); let default generate it.
  insert into public.timesheets (
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
  values (
    v_current.booking_id,
    v_new_version,
    true,
    'RECEIVED'::public.timesheet_status_enum,
    null,
    null,
    v_current.contract_id,
    v_current.submission_mode,
    v_current.line_type,
    v_current.sheet_scope,

    v_current.occupant_key_norm,
    v_current.hospital_norm,
    v_current.ward_norm,
    v_current.job_title_norm,
    v_current.shift_label_norm,
    v_current.week_ending_date,

    -- Daily: clear all worked times/breaks
    null::timestamptz,
    null::timestamptz,
    null::timestamptz,
    null::timestamptz,
    null::int,

    -- Weekly: clear schedule; keep additional units as empty objects if present
    null::jsonb,
    coalesce(v_current.additional_units_week, '{}'::jsonb),
    coalesce(v_current.additional_units_per_day, '{}'::jsonb),

    -- Clear evidence + auth + refs for a clean re-submit
    null::text,
    null::timestamptz,
    null::text,
    null::jsonb,

    -- QR Scenario 1
    null::text,
    'PENDING'::public.timesheet_qr_status_enum,
    '{}'::jsonb,
    null::timestamptz,
    null::timestamptz,
    null::jsonb,
    null::text,

    v_now,
    v_now
  )
  returning timesheet_id into v_new_timesheet_id;

  if v_new_timesheet_id is null then
    raise exception 'Insert succeeded but no new timesheet_id returned';
  end if;

  -- 5) Move contract_week pointer (if any) from OLD current id to NEW current id
  update public.contract_weeks
  set timesheet_id = v_new_timesheet_id,
      updated_at   = v_now
  where timesheet_id = v_current.timesheet_id;

  -- 6) Reset TSFIN in-place, and MOVE it to the new timesheet_id
  if v_fin.id is not null then
    update public.timesheets_financials
    set
      timesheet_id            = v_new_timesheet_id,
      timesheet_version       = v_new_version,
      processing_status       = 'UNASSIGNED'::public.ts_fin_processing_status_enum,

      -- Hours MUST remain NOT NULL (set to 0 rather than NULL)
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

    -- 7) Enqueue TSFIN recompute (optional but useful) for NEW timesheet_id
    insert into public.ts_financials_outbox(timesheet_id, reason, attempt_count, next_attempt_at, last_error, created_at)
    values (v_new_timesheet_id, 'REVOKED'::public.ts_fin_reason_enum, 0, v_now, null, v_now)
    on conflict (timesheet_id, reason) do nothing;
  end if;

  -- 8) Audit event (timesheet) — write against NEW current id; include old in before_json
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
    v_new_timesheet_id::text,
    'QR_HOURS_REFUSED',
    jsonb_build_object(
      'booking_id', v_booking_id::text,
      'old_timesheet_id', v_current.timesheet_id::text,
      'old_version', v_current.version
    ),
    jsonb_build_object(
      'new_timesheet_id', v_new_timesheet_id::text,
      'new_version', v_new_version
    ),
    nullif(btrim(p_reason), ''),
    p_actor_user_id,
    v_now
  );

  -- Return (timesheet_id = NEW current id)
  timesheet_id := v_new_timesheet_id;
  old_version := v_current.version;
  new_version := v_new_version;
  sheet_scope := v_current.sheet_scope::text;
  submission_mode := v_current.submission_mode::text;
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

  v_any public.timesheets%rowtype;
  v_current public.timesheets%rowtype;
  v_restore public.timesheets%rowtype;

  v_booking_id uuid;

  v_fin public.timesheets_financials%rowtype;
begin
  if p_timesheet_id is null then
    raise exception 'timesheet_id is required';
  end if;

  if upper(coalesce(p_restore_kind,'')) not in ('PENDING','SIGNED') then
    raise exception 'restore_kind must be PENDING or SIGNED';
  end if;

  -- 0) Resolve booking_id from ANY row (stale or current)
  select *
  into v_any
  from public.timesheets
  where timesheet_id = p_timesheet_id
  limit 1;

  if not found then
    raise exception 'Timesheet not found';
  end if;

  v_booking_id := v_any.booking_id;
  if v_booking_id is null then
    raise exception 'Timesheet booking_id is missing; cannot restore';
  end if;

  -- 1) Lock CURRENT row for this booking_id
  select *
  into v_current
  from public.timesheets
  where booking_id = v_booking_id
    and is_current = true
  for update;

  if not found then
    raise exception 'Current timesheet not found for booking_id';
  end if;

  -- 2) Lock CURRENT TSFIN and block restore if invoiced/paid
  select *
  into v_fin
  from public.timesheets_financials
  where timesheet_id = v_current.timesheet_id
    and is_current = true
  for update;

  if found then
    if v_fin.locked_by_invoice_id is not null or v_fin.paid_at_utc is not null then
      raise exception 'Cannot restore: timesheet is invoiced/locked or paid';
    end if;
  end if;

  -- 3) Pick the target version to restore (within booking_id series)
  if upper(p_restore_kind) = 'PENDING' then
    -- "pending" = not scanned / not signed
    select *
    into v_restore
    from public.timesheets
    where booking_id = v_booking_id
      and is_current = false
      and status = 'REVOKED'::public.timesheet_status_enum
      and qr_scanned_at is null
    order by version desc
    limit 1;
  else
    -- "signed" = scanned_at present
    select *
    into v_restore
    from public.timesheets
    where booking_id = v_booking_id
      and is_current = false
      and status = 'REVOKED'::public.timesheet_status_enum
      and qr_scanned_at is not null
    order by version desc
    limit 1;
  end if;

  if not found then
    raise exception 'No matching revoked version found to restore';
  end if;

  -- 4) Demote current -> not current
  update public.timesheets
  set
    is_current = false,
    updated_at = v_now
  where timesheet_id = v_current.timesheet_id
    and is_current = true;

  -- 5) Promote restore -> current
  update public.timesheets
  set
    is_current = true,
    updated_at = v_now
  where timesheet_id = v_restore.timesheet_id;

  -- 6) Move contract_week pointer if it points at the old current row
  update public.contract_weeks
  set timesheet_id = v_restore.timesheet_id,
      updated_at   = v_now
  where timesheet_id = v_current.timesheet_id;

  -- 7) Move TSFIN row to restored current id + update version marker + enqueue recompute
  if v_fin.id is not null then
    update public.timesheets_financials
    set
      timesheet_id      = v_restore.timesheet_id,
      timesheet_version = v_restore.version,
      updated_at        = v_now
    where id = v_fin.id;

    insert into public.ts_financials_outbox(timesheet_id, reason, attempt_count, next_attempt_at, last_error, created_at)
    values (v_restore.timesheet_id, 'VERSION_ROTATED'::public.ts_fin_reason_enum, 0, v_now, null, v_now)
    on conflict (timesheet_id, reason) do nothing;
  end if;

  -- 8) Audit — write against restored (new current) timesheet_id
  insert into public.audit_events(
    object_type, object_id_text, action, before_json, after_json, reason, actor_user_id, ts_utc
  )
  values(
    'timesheet',
    v_restore.timesheet_id::text,
    'QR_RESTORED',
    jsonb_build_object(
      'booking_id', v_booking_id::text,
      'from_timesheet_id', v_current.timesheet_id::text,
      'from_version', v_current.version
    ),
    jsonb_build_object(
      'to_timesheet_id', v_restore.timesheet_id::text,
      'to_version', v_restore.version,
      'kind', upper(p_restore_kind)
    ),
    null,
    p_actor_user_id,
    v_now
  );

  -- Return (timesheet_id = restored current id)
  timesheet_id := v_restore.timesheet_id;
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
--    FIX: resolve booking_id so it works even if p_timesheet_id is stale.
--    Also includes audit for ALL timesheet ids in the booking series.
-- ------------------------------------------------------------
create or replace function public.timesheet_audit_feed(
  p_timesheet_id uuid
)
returns table (
  id uuid,
  ts_utc timestamptz,
  actor_user_id uuid,
  actor_display text,
  actor_role_at_time text,
  object_type text,
  object_id_text text,
  action text,
  before_json jsonb,
  after_json jsonb,
  reason text,
  ip text,
  user_agent text,
  correlation_id text
)
language sql
stable
security definer
set search_path = public
as $$
  with base as (
    select
      t.booking_id,
      t.timesheet_id as any_timesheet_id
    from public.timesheets t
    where t.timesheet_id = p_timesheet_id
    limit 1
  ),
  cur as (
    select
      t.timesheet_id as current_timesheet_id
    from public.timesheets t
    join base b on b.booking_id = t.booking_id
    where t.is_current = true
    limit 1
  ),
  ts_ids as (
    -- all versions in the series (if booking_id known)
    select t.timesheet_id::text as ts_id_text
    from public.timesheets t
    join base b on b.booking_id = t.booking_id

    union all

    -- fallback: if p_timesheet_id not in timesheets, still allow audit on that id
    select p_timesheet_id::text
    where not exists (select 1 from base)
  ),
  cw as (
    -- contract_week is linked to CURRENT timesheet_id pointer (not necessarily the passed id)
    select id::text as cw_id_text
    from public.contract_weeks
    where timesheet_id = (select current_timesheet_id from cur)
    order by updated_at desc nulls last, week_ending_date desc
    limit 1
  )
  select
    ae.id,
    ae.ts_utc,
    ae.actor_user_id,
    coalesce(ae.actor_display, tu.display_name, tu.email, 'CloudTMS server') as actor_display,
    coalesce(ae.actor_role_at_time, tu.role, 'system') as actor_role_at_time,
    ae.object_type,
    ae.object_id_text,
    ae.action,
    ae.before_json,
    ae.after_json,
    ae.reason,
    ae.ip,
    ae.user_agent,
    ae.correlation_id
  from public.audit_events ae
  left join public.tms_users tu
    on tu.id = ae.actor_user_id
  where
    (
      ae.object_type in ('timesheet','timesheets')
      and ae.object_id_text in (select ts_id_text from ts_ids)
    )
    or
    (
      (select cw_id_text from cw) is not null
      and ae.object_type in ('contract_week','contract_weeks')
      and ae.object_id_text = (select cw_id_text from cw)
    )
  order by ae.ts_utc desc, ae.id desc
  limit 500;
$$;
