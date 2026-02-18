-- ============================================================
-- QR refusal + restore + planned-week delete + audit feed
-- FIX: avoid PL/pgSQL ambiguity with RETURNS TABLE timesheet_id
-- by using ON CONFLICT ON CONSTRAINT uq_tsfin_outbox
-- ============================================================

begin;

-- Optional safety cleanup: drop any legacy overloads (won’t error if absent)
drop function if exists public.timesheet_qr_refuse_and_reset(uuid, text, uuid);
drop function if exists public.timesheet_qr_restore_version(uuid, text, uuid);

-- ------------------------------------------------------------
-- 1) Refuse QR hours: rotate current TS to history + reset to
--    "pre-hours-submitted" QR Scenario 1, reset TSFIN, audit.
-- ------------------------------------------------------------
create or replace function public.timesheet_qr_refuse_and_reset(
  p_timesheet_id uuid,
  p_expected_timesheet_id uuid,
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

  v_booking_id text;          -- timesheets.booking_id is TEXT
  v_new_version int;
  v_new_timesheet_id uuid;
begin
  if p_timesheet_id is null then
    raise exception 'timesheet_id is required';
  end if;

  if p_expected_timesheet_id is null then
    raise exception '%',
      jsonb_build_object('error','EXPECTED_TIMESHEET_ID_REQUIRED')::text;
  end if;

  -- 0) Resolve booking_id from ANY row (stale or current)
  select t.*
  into v_any
  from public.timesheets t
  where t.timesheet_id = p_timesheet_id
  limit 1;

  if not found then
    raise exception 'Timesheet not found';
  end if;

  v_booking_id := v_any.booking_id;
  if v_booking_id is null or btrim(v_booking_id) = '' then
    raise exception 'Timesheet booking_id is missing; cannot rotate versions';
  end if;

  -- 1) Lock CURRENT timesheet row for this booking_id
  select t.*
  into v_current
  from public.timesheets t
  where t.booking_id = v_booking_id
    and t.is_current = true
  for update;

  if not found then
    raise exception 'Current timesheet not found for booking_id';
  end if;

  -- ✅ ATOMIC expected-guard (no side effects beyond row lock)
  if p_expected_timesheet_id <> v_current.timesheet_id then
    raise exception '%',
      jsonb_build_object(
        'error','TIMESHEET_MOVED',
        'current_timesheet_id', v_current.timesheet_id
      )::text;
  end if;

  -- 2) Lock CURRENT TSFIN row (keyed to CURRENT timesheet_id) and block if paid/invoiced
  select f.*
  into v_fin
  from public.timesheets_financials f
  where f.timesheet_id = v_current.timesheet_id
    and f.is_current = true
  for update;

  if found then
    if v_fin.locked_by_invoice_id is not null or v_fin.paid_at_utc is not null then
      raise exception 'Cannot refuse hours: timesheet is invoiced/locked or paid';
    end if;
  end if;

  v_new_version := coalesce(v_current.version, 1) + 1;

  -- 3) Rotate CURRENT version -> history (invalidate QR on historical)
  update public.timesheets as t
  set
    is_current      = false,
    status          = 'REVOKED'::public.timesheet_status_enum,
    revoked_reason  = nullif(btrim(p_reason), ''),
    revoked_by      = case when p_actor_user_id is null then null else p_actor_user_id::text end,
    qr_status       = 'CANCELLED'::public.timesheet_qr_status_enum,
    updated_at      = v_now
  where t.timesheet_id = v_current.timesheet_id
    and t.is_current = true;

  -- 4) Insert NEW current version = Scenario 1
  insert into public.timesheets as nt (
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

    worked_start_iso,
    worked_end_iso,
    break_start_iso,
    break_end_iso,
    break_minutes,

    actual_schedule_json,
    additional_units_week,
    additional_units_per_day,

    manual_pdf_r2_key,
    authorised_at_server,
    reference_number,
    day_references_json,

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

    null::timestamptz,
    null::timestamptz,
    null::timestamptz,
    null::timestamptz,
    null::int,

    null::jsonb,
    coalesce(v_current.additional_units_week, '{}'::jsonb),
    coalesce(v_current.additional_units_per_day, '{}'::jsonb),

    null::text,
    null::timestamptz,
    null::text,
    null::jsonb,

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
  returning nt.timesheet_id into v_new_timesheet_id;

  if v_new_timesheet_id is null then
    raise exception 'Insert succeeded but no new timesheet_id returned';
  end if;

  -- 5) Move contract_week pointer (if any) from OLD current id -> NEW current id
  update public.contract_weeks as cw
  set timesheet_id = v_new_timesheet_id,
      updated_at   = v_now
  where cw.timesheet_id = v_current.timesheet_id;

  -- 6) Reset TSFIN in-place and MOVE it to new timesheet_id
  if v_fin.id is not null then
    update public.timesheets_financials as f
    set
      timesheet_id             = v_new_timesheet_id,
      timesheet_version        = v_new_version,
      processing_status        = 'UNASSIGNED'::public.ts_fin_processing_status_enum,

      hours_day                = 0,
      hours_night              = 0,
      hours_sat                = 0,
      hours_sun                = 0,
      hours_bh                 = 0,
      total_hours              = 0,

      total_pay_ex_vat         = 0,
      total_charge_ex_vat      = 0,
      margin_ex_vat            = 0,
      additional_pay_ex_vat    = 0,
      additional_charge_ex_vat = 0,
      additional_margin_ex_vat = 0,

      authorised_at_utc        = null,
      authorised_by_user_id    = null,
      locked_by_invoice_id     = null,
      locked_at_utc            = null,
      paid_at_utc              = null,
      paid_by_user_id          = null,
      payment_reference        = null,

      updated_at               = v_now
    where f.id = v_fin.id;

    -- 7) Enqueue TSFIN recompute (optional)
    -- ✅ FIX: use constraint name to avoid ambiguous (timesheet_id, reason)
    insert into public.ts_financials_outbox(timesheet_id, reason, attempt_count, next_attempt_at, last_error, created_at)
    values (v_new_timesheet_id, 'REVOKED'::public.ts_fin_reason_enum, 0, v_now, null, v_now)
    on conflict on constraint uq_tsfin_outbox do nothing;
  end if;

  -- 8) Audit event
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
      'booking_id', v_booking_id,
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

  -- Return
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
  p_expected_timesheet_id uuid,
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

  v_booking_id text;          -- timesheets.booking_id is TEXT
  v_fin public.timesheets_financials%rowtype;
begin
  if p_timesheet_id is null then
    raise exception 'timesheet_id is required';
  end if;

  if p_expected_timesheet_id is null then
    raise exception '%',
      jsonb_build_object('error','EXPECTED_TIMESHEET_ID_REQUIRED')::text;
  end if;

  if upper(coalesce(p_restore_kind,'')) not in ('PENDING','SIGNED') then
    raise exception 'restore_kind must be PENDING or SIGNED';
  end if;

  -- 0) Resolve booking_id from ANY row
  select t.*
  into v_any
  from public.timesheets t
  where t.timesheet_id = p_timesheet_id
  limit 1;

  if not found then
    raise exception 'Timesheet not found';
  end if;

  v_booking_id := v_any.booking_id;
  if v_booking_id is null or btrim(v_booking_id) = '' then
    raise exception 'Timesheet booking_id is missing; cannot restore';
  end if;

  -- 1) Lock CURRENT row for booking_id
  select t.*
  into v_current
  from public.timesheets t
  where t.booking_id = v_booking_id
    and t.is_current = true
  for update;

  if not found then
    raise exception 'Current timesheet not found for booking_id';
  end if;

  if p_expected_timesheet_id <> v_current.timesheet_id then
    raise exception '%',
      jsonb_build_object(
        'error','TIMESHEET_MOVED',
        'current_timesheet_id', v_current.timesheet_id
      )::text;
  end if;

  -- 2) Lock current TSFIN and block if invoiced/paid
  select f.*
  into v_fin
  from public.timesheets_financials f
  where f.timesheet_id = v_current.timesheet_id
    and f.is_current = true
  for update;

  if found then
    if v_fin.locked_by_invoice_id is not null or v_fin.paid_at_utc is not null then
      raise exception 'Cannot restore: timesheet is invoiced/locked or paid';
    end if;
  end if;

  -- 3) Pick target revoked version
  if upper(p_restore_kind) = 'PENDING' then
    select t.*
    into v_restore
    from public.timesheets t
    where t.booking_id = v_booking_id
      and t.is_current = false
      and t.status = 'REVOKED'::public.timesheet_status_enum
      and t.qr_scanned_at is null
    order by t.version desc
    limit 1;
  else
    select t.*
    into v_restore
    from public.timesheets t
    where t.booking_id = v_booking_id
      and t.is_current = false
      and t.status = 'REVOKED'::public.timesheet_status_enum
      and t.qr_scanned_at is not null
    order by t.version desc
    limit 1;
  end if;

  if not found then
    raise exception 'No matching revoked version found to restore';
  end if;

  -- 4) Demote current -> not current
  update public.timesheets as t
  set is_current = false,
      updated_at = v_now
  where t.timesheet_id = v_current.timesheet_id
    and t.is_current = true;

  -- 5) Promote restored -> current
  update public.timesheets as t
  set is_current = true,
      updated_at = v_now
  where t.timesheet_id = v_restore.timesheet_id;

  -- 6) Move contract_week pointer if it points at old current row
  update public.contract_weeks as cw
  set timesheet_id = v_restore.timesheet_id,
      updated_at   = v_now
  where cw.timesheet_id = v_current.timesheet_id;

  -- 7) Move TSFIN row + enqueue recompute
  if v_fin.id is not null then
    update public.timesheets_financials as f
    set
      timesheet_id      = v_restore.timesheet_id,
      timesheet_version = v_restore.version,
      updated_at        = v_now
    where f.id = v_fin.id;

    -- ✅ FIX: use constraint name to avoid ambiguous (timesheet_id, reason)
    insert into public.ts_financials_outbox(timesheet_id, reason, attempt_count, next_attempt_at, last_error, created_at)
    values (v_restore.timesheet_id, 'VERSION_ROTATED'::public.ts_fin_reason_enum, 0, v_now, null, v_now)
    on conflict on constraint uq_tsfin_outbox do nothing;
  end if;

  -- 8) Audit
  insert into public.audit_events(
    object_type, object_id_text, action, before_json, after_json, reason, actor_user_id, ts_utc
  )
  values(
    'timesheet',
    v_restore.timesheet_id::text,
    'QR_RESTORED',
    jsonb_build_object(
      'booking_id', v_booking_id,
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

  -- Return
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
    select t.booking_id
    from public.timesheets t
    where t.timesheet_id = p_timesheet_id
    limit 1
  ),
  cur as (
    select t.timesheet_id as current_timesheet_id
    from public.timesheets t
    join base b on b.booking_id = t.booking_id
    where t.is_current = true
    limit 1
  ),
  ts_ids as (
    select t.timesheet_id::text as ts_id_text
    from public.timesheets t
    join base b on b.booking_id = t.booking_id

    union all

    select p_timesheet_id::text
    where not exists (select 1 from base)
  ),
  cw as (
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

commit;
