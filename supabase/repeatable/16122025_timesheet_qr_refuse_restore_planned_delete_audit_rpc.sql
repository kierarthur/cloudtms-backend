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
    join base b
      on b.booking_id = t.booking_id
    where t.is_current = true
    limit 1
  ),
  ts_ids as (
    select t.timesheet_id::text as ts_id_text
    from public.timesheets t
    join base b
      on b.booking_id = t.booking_id

    union all

    select p_timesheet_id::text
    where not exists (select 1 from base)
  ),
  cw as (
    select cw1.id::text as cw_id_text
    from public.contract_weeks cw1
    where cw1.timesheet_id = (select c.current_timesheet_id from cur c)
    order by cw1.updated_at desc nulls last, cw1.week_ending_date desc
    limit 1
  ),
  ae_scope as (
    select
      ae.id,
      ae.ts_utc,
      ae.actor_user_id,
      ae.actor_display,
      ae.actor_role_at_time,
      ae.object_type,
      ae.object_id_text,
      ae.action,
      ae.before_json,
      ae.after_json,
      ae.reason,
      ae.ip,
      ae.user_agent,
      ae.correlation_id,
      coalesce(ae.after_json, ae.before_json, '{}'::jsonb) as payload_json
    from public.audit_events ae
  )
  select
    aes.id,
    aes.ts_utc,
    aes.actor_user_id,
    coalesce(aes.actor_display, tu.display_name, tu.email, 'CloudTMS server') as actor_display,
    coalesce(aes.actor_role_at_time, tu.role, 'system') as actor_role_at_time,
    aes.object_type,
    aes.object_id_text,
    aes.action,
    aes.before_json,
    aes.after_json,
    aes.reason,
    aes.ip,
    aes.user_agent,
    aes.correlation_id
  from ae_scope aes
  left join public.tms_users tu
    on tu.id = aes.actor_user_id
  where
    (
      aes.object_type in ('timesheet', 'timesheets')
      and aes.object_id_text in (
        select ti.ts_id_text
        from ts_ids ti
      )
    )
    or
    (
      exists (
        select 1
        from cw
      )
      and aes.object_type in ('contract_week', 'contract_weeks')
      and aes.object_id_text = (
        select c2.cw_id_text
        from cw c2
      )
    )
    or
    (
      aes.object_type in ('manual_timesheet_queue', 'manual_timesheet_queues')
      and aes.action in (
        'MANUAL_TIMESHEET_QUEUE_ATTACHED',
        'MANUAL_TIMESHEET_QUEUE_STAGED',
        'MANUAL_TIMESHEET_QUEUE_STAGED_KIND_UPDATED',
        'MANUAL_TIMESHEET_QUEUE_STAGED_RETURNED_TO_QUEUE',
        'MANUAL_TIMESHEET_QUEUE_STAGED_DELETED'
      )
      and (
        trim(coalesce(aes.payload_json ->> 'current_timesheet_id', '')) in (
          select ti.ts_id_text
          from ts_ids ti
        )
        or trim(coalesce(aes.payload_json ->> 'requested_timesheet_id', '')) in (
          select ti.ts_id_text
          from ts_ids ti
        )
        or trim(coalesce(aes.payload_json ->> 'timesheet_id', '')) in (
          select ti.ts_id_text
          from ts_ids ti
        )
        or trim(coalesce(aes.payload_json ->> 'materialised_to_timesheet_id', '')) in (
          select ti.ts_id_text
          from ts_ids ti
        )
        or trim(coalesce(aes.payload_json ->> 'dematerialised_from_timesheet_id', '')) in (
          select ti.ts_id_text
          from ts_ids ti
        )
        or (
          exists (
            select 1
            from base b2
          )
          and trim(coalesce(aes.payload_json ->> 'dematerialised_from_booking_id', '')) = (
            select b3.booking_id
            from base b3
          )
        )
        or (
          exists (
            select 1
            from cw
          )
          and trim(coalesce(aes.payload_json ->> 'contract_week_id', '')) = (
            select c3.cw_id_text
            from cw c3
          )
        )
      )
    )
  order by aes.ts_utc desc, aes.id desc
  limit 500;
$$;

commit;
CREATE OR REPLACE FUNCTION public.timesheet_route_version_rotate(
  p_current_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_target_action text,
  p_actor_user_id uuid,
  p_allow_manual_only boolean DEFAULT false
)
RETURNS jsonb
LANGUAGE plpgsql
AS $function$
DECLARE
  v_action text := upper(btrim(coalesce(p_target_action, '')));
  v_now timestamptz := now();

  v_requested_row public.timesheets%ROWTYPE;
  v_current_row public.timesheets%ROWTYPE;
  v_new_row public.timesheets%ROWTYPE;
  v_electronic_row public.timesheets%ROWTYPE;
  v_contract_week_row public.contract_weeks%ROWTYPE;
  v_tsfin_row public.timesheets_financials%ROWTYPE;

  v_booking_id text;
  v_requested_timesheet_id uuid;
  v_current_timesheet_id uuid;
  v_new_timesheet_id uuid;
  v_contract_week_id uuid;
  v_contract_id uuid;
  v_client_id uuid;

  v_next_version integer;
  v_old_version integer;
  v_new_version integer;
  v_electronic_version integer;

  v_scope text;
  v_submission_mode text;
  v_qr_status text;
  v_basis text;

  v_has_qr_token boolean := false;
  v_has_qr_generated boolean := false;
  v_has_qr_scanned boolean := false;
  v_is_manual_only boolean := false;
  v_hard_locked boolean := false;
  v_has_segment_invoice_lock boolean := false;
  v_supports_electronic boolean := false;
  v_was_stale boolean := false;

  v_summary_route_type text;
  v_summary_client_no_timesheet_required boolean := false;
  v_contract_is_nhsp boolean := false;
  v_contract_autoprocess_hr boolean := false;
  v_contract_no_timesheet_required boolean := false;
  v_manual_adjustment_editable boolean := false;
  v_is_import_authoritative boolean := false;

  v_reverted boolean := false;
  v_switched boolean := false;
  v_converted boolean := false;

  v_current_adjustment_origin text;
  v_current_parent_timesheet_id_text text;
  v_contract_week_is_adjustment boolean := false;
  v_contract_week_is_contract_week_only boolean := false;
BEGIN
  IF p_current_timesheet_id IS NULL THEN
    RAISE EXCEPTION 'timesheet_id is required';
  END IF;

  IF p_expected_timesheet_id IS NULL THEN
    RAISE EXCEPTION 'expected_timesheet_id is required';
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'actor_user_id is required';
  END IF;

  IF v_action NOT IN (
    'CONVERT_QR_TO_MANUAL',
    'ALLOW_ELECTRONIC_AGAIN',
    'ALLOW_QR_AGAIN',
    'SWITCH_TO_MANUAL',
    'SWITCH_DAILY_TO_MANUAL',
    'REVERT_TO_ELECTRONIC'
  ) THEN
    RAISE EXCEPTION 'Unsupported target action: %', p_target_action;
  END IF;

  SELECT t_req.*
  INTO v_requested_row
  FROM public.timesheets AS t_req
  WHERE t_req.timesheet_id = p_current_timesheet_id
  LIMIT 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Timesheet not found';
  END IF;

  v_requested_timesheet_id := v_requested_row.timesheet_id;
  v_booking_id := v_requested_row.booking_id;

  IF v_booking_id IS NULL OR btrim(v_booking_id) = '' THEN
    RAISE EXCEPTION 'Timesheet booking_id is missing; cannot version';
  END IF;

  PERFORM 1
  FROM public.timesheets AS t_lock
  WHERE t_lock.booking_id = v_booking_id
  FOR UPDATE;

  SELECT t_cur.*
  INTO v_current_row
  FROM public.timesheets AS t_cur
  WHERE t_cur.booking_id = v_booking_id
    AND t_cur.is_current = true
  ORDER BY t_cur.version DESC, t_cur.updated_at DESC, t_cur.created_at DESC
  LIMIT 1;

  IF NOT FOUND THEN
    IF coalesce(v_requested_row.is_current, false) = true THEN
      v_current_row := v_requested_row;
    ELSE
      RAISE EXCEPTION 'Timesheet not found';
    END IF;
  END IF;

  v_current_timesheet_id := v_current_row.timesheet_id;
  v_was_stale := (v_requested_timesheet_id IS DISTINCT FROM v_current_timesheet_id);

  IF v_current_timesheet_id IS DISTINCT FROM p_expected_timesheet_id THEN
    RAISE EXCEPTION 'TIMESHEET_MOVED'
      USING DETAIL = jsonb_build_object(
        'current_timesheet_id', v_current_timesheet_id::text
      )::text;
  END IF;

  v_old_version := coalesce(v_current_row.version, 1);
  v_scope := upper(coalesce(v_current_row.sheet_scope::text, ''));
  v_submission_mode := upper(coalesce(v_current_row.submission_mode::text, ''));
  v_qr_status := upper(coalesce(v_current_row.qr_status::text, ''));

  SELECT cw_cur.*
  INTO v_contract_week_row
  FROM public.contract_weeks AS cw_cur
  WHERE cw_cur.timesheet_id = v_current_timesheet_id
  ORDER BY cw_cur.updated_at DESC, cw_cur.created_at DESC
  LIMIT 1;

  IF FOUND THEN
    v_contract_week_id := v_contract_week_row.id;
    PERFORM 1
    FROM public.contract_weeks AS cw_lock
    WHERE cw_lock.id = v_contract_week_id
    FOR UPDATE;
  ELSE
    v_contract_week_id := NULL;
  END IF;

  SELECT tf_cur.*
  INTO v_tsfin_row
  FROM public.timesheets_financials AS tf_cur
  WHERE tf_cur.timesheet_id = v_current_timesheet_id
    AND tf_cur.is_current = true
  ORDER BY tf_cur.updated_at DESC, tf_cur.created_at DESC
  LIMIT 1;

  IF v_tsfin_row.id IS NOT NULL THEN
    PERFORM 1
    FROM public.timesheets_financials AS tf_lock
    WHERE tf_lock.id = v_tsfin_row.id
    FOR UPDATE;
  END IF;

  v_contract_id := coalesce(v_contract_week_row.contract_id, v_current_row.contract_id);
  v_client_id := v_tsfin_row.client_id;

  IF v_action IN ('CONVERT_QR_TO_MANUAL', 'ALLOW_ELECTRONIC_AGAIN', 'SWITCH_TO_MANUAL') THEN
    BEGIN
      SELECT
        upper(coalesce(vts.route_type, '')),
        coalesce(vts.client_no_timesheet_required, false),
        coalesce(vts.contract_id, v_contract_id),
        coalesce(vts.contract_week_id, v_contract_week_id)
      INTO
        v_summary_route_type,
        v_summary_client_no_timesheet_required,
        v_contract_id,
        v_contract_week_id
      FROM public.v_timesheets_summary AS vts
      WHERE vts.timesheet_id = v_current_timesheet_id
      LIMIT 1;
    EXCEPTION WHEN OTHERS THEN
      v_summary_route_type := '';
      v_summary_client_no_timesheet_required := false;
    END;

    IF v_contract_week_id IS NOT NULL AND v_contract_week_row.id IS DISTINCT FROM v_contract_week_id THEN
      SELECT cw_reload.*
      INTO v_contract_week_row
      FROM public.contract_weeks AS cw_reload
      WHERE cw_reload.id = v_contract_week_id
      LIMIT 1;

      IF v_contract_week_row.id IS NOT NULL THEN
        PERFORM 1
        FROM public.contract_weeks AS cw_reload_lock
        WHERE cw_reload_lock.id = v_contract_week_row.id
        FOR UPDATE;
      END IF;
    END IF;

    v_current_adjustment_origin := upper(coalesce(v_current_row.adjustment_origin, ''));
    v_current_parent_timesheet_id_text := btrim(coalesce(v_current_row.parent_timesheet_id::text, ''));
    v_contract_week_is_adjustment := (
      coalesce(v_contract_week_row.is_adjustment, false)
      OR coalesce(v_contract_week_row.additional_seq, 0) > 0
    );
    v_contract_week_is_contract_week_only := (btrim(coalesce(v_contract_week_row.timesheet_id::text, '')) = '');

    IF v_summary_route_type = 'WEEKLY_NHSP_ADJUSTMENT' THEN
      IF (
        coalesce(v_current_row.is_adjustment, false) = true
        AND upper(coalesce(v_current_row.submission_mode::text, '')) = 'MANUAL'
        AND v_current_adjustment_origin NOT IN ('IMPORT_CORRECTION', 'IMPORT_CANCELLATION')
        AND (
          v_current_adjustment_origin = 'MANUAL_ADJUSTMENT'
          OR v_current_parent_timesheet_id_text <> ''
          OR v_contract_week_is_adjustment = true
        )
      ) THEN
        v_manual_adjustment_editable := true;
      ELSIF (
        v_contract_week_row.id IS NOT NULL
        AND v_contract_week_is_adjustment = true
        AND upper(coalesce(v_contract_week_row.submission_mode_snapshot::text, '')) = 'MANUAL'
        AND v_contract_week_is_contract_week_only = true
      ) THEN
        v_manual_adjustment_editable := true;
      ELSE
        v_manual_adjustment_editable := false;
      END IF;
    ELSE
      v_manual_adjustment_editable := false;
    END IF;

    IF v_manual_adjustment_editable = false THEN
      v_is_import_authoritative := (
        v_summary_route_type = 'WEEKLY_NHSP'
        OR v_summary_route_type = 'WEEKLY_NHSP_ADJUSTMENT'
        OR (v_summary_route_type = 'WEEKLY_HEALTHROSTER' AND v_summary_client_no_timesheet_required = true)
      );
    ELSE
      v_is_import_authoritative := false;
    END IF;

    IF v_is_import_authoritative = false AND v_contract_id IS NOT NULL THEN
      BEGIN
        SELECT
          coalesce(c.is_nhsp, false),
          coalesce(c.autoprocess_hr, false),
          coalesce(c.no_timesheet_required, false),
          coalesce(c.client_id, v_client_id)
        INTO
          v_contract_is_nhsp,
          v_contract_autoprocess_hr,
          v_contract_no_timesheet_required,
          v_client_id
        FROM public.contracts AS c
        WHERE c.id = v_contract_id
        LIMIT 1;
      EXCEPTION WHEN OTHERS THEN
        v_contract_is_nhsp := false;
        v_contract_autoprocess_hr := false;
        v_contract_no_timesheet_required := false;
      END;

      IF v_manual_adjustment_editable = false THEN
        v_is_import_authoritative := (
          v_contract_is_nhsp = true
          OR (v_contract_autoprocess_hr = true AND v_contract_no_timesheet_required = true)
        );
      END IF;
    END IF;
  END IF;

  IF v_action = 'CONVERT_QR_TO_MANUAL' THEN
    IF (
      v_current_row.authorised_at_server IS NOT NULL
      AND btrim(v_current_row.authorised_at_server::text) <> ''
      AND (v_current_row.revoked_at IS NULL OR btrim(v_current_row.revoked_at::text) = '')
    ) THEN
      RAISE EXCEPTION 'TIMESHEET_AUTHORISED_EDIT_BLOCKED: This timesheet is authorised. Unauthorise it before converting QR to manual.';
    END IF;

    IF v_is_import_authoritative = true THEN
      RAISE EXCEPTION 'Import-authoritative timesheets (NHSP / HealthRoster weekly no-timesheets) cannot change submission route';
    END IF;

    IF v_scope <> '' AND v_scope NOT IN ('WEEKLY', 'DAILY') THEN
      RAISE EXCEPTION 'Unsupported sheet_scope for QR conversion';
    END IF;

    IF v_submission_mode <> 'MANUAL' THEN
      RAISE EXCEPTION 'Timesheet is not in MANUAL mode; QR conversion not applicable';
    END IF;

    IF v_qr_status = '' THEN
      RAISE EXCEPTION 'Timesheet does not have QR metadata; nothing to convert';
    END IF;

    IF v_tsfin_row.id IS NULL THEN
      RAISE EXCEPTION 'No financial snapshot for timesheet';
    END IF;

    v_hard_locked := (
      coalesce(v_tsfin_row.paid_at_utc IS NOT NULL, false)
      OR coalesce(v_tsfin_row.locked_by_invoice_id IS NOT NULL, false)
                      );

    IF v_hard_locked THEN
      RAISE EXCEPTION 'Cannot convert QR: timesheet already invoiced or paid';
    END IF;
  ELSIF v_action = 'ALLOW_ELECTRONIC_AGAIN' THEN
    IF v_current_row.is_adjustment = true OR coalesce(v_contract_week_row.is_adjustment, false) = true THEN
      RAISE EXCEPTION 'Adjustment timesheets cannot be converted to electronic submission';
    END IF;

    IF v_is_import_authoritative = true THEN
      RAISE EXCEPTION 'Import-authoritative timesheets (NHSP / HealthRoster weekly no-timesheets) cannot use Allow electronic again';
    END IF;

    v_hard_locked := (
      coalesce(v_tsfin_row.locked_by_invoice_id IS NOT NULL, false)
      OR coalesce(v_tsfin_row.paid_at_utc IS NOT NULL, false)
    );
    IF v_hard_locked THEN
      RAISE EXCEPTION 'Cannot allow electronic again: timesheet is invoiced/locked or paid';
    END IF;

    v_has_qr_token := (btrim(coalesce(v_current_row.qr_token, '')) <> '');
    v_has_qr_generated := (v_current_row.qr_generated_at IS NOT NULL);
    v_has_qr_scanned := (v_current_row.qr_scanned_at IS NOT NULL);
    v_is_manual_only := (
      v_submission_mode = 'MANUAL'
      AND v_qr_status = ''
      AND v_has_qr_token = false
      AND v_has_qr_generated = false
      AND v_has_qr_scanned = false
    );

    IF v_is_manual_only = false THEN
      RAISE EXCEPTION 'Allow electronic again is only valid for manual-only timesheets';
    END IF;

    IF v_client_id IS NULL AND v_contract_id IS NOT NULL THEN
      BEGIN
        SELECT c_client.client_id
        INTO v_client_id
        FROM public.contracts AS c_client
        WHERE c_client.id = v_contract_id
        LIMIT 1;
      EXCEPTION WHEN OTHERS THEN
        v_client_id := NULL;
      END;
    END IF;

    IF v_client_id IS NULL THEN
      RAISE EXCEPTION 'Cannot resolve client_id for electronic eligibility check';
    END IF;

    BEGIN
      SELECT (upper(coalesce(cs.default_submission_mode::text, '')) = 'ELECTRONIC')
      INTO v_supports_electronic
      FROM public.client_settings AS cs
      WHERE cs.client_id = v_client_id
      ORDER BY cs.updated_at DESC NULLS LAST, cs.created_at DESC NULLS LAST
      LIMIT 1;
    EXCEPTION WHEN OTHERS THEN
      v_supports_electronic := false;
    END;

    IF coalesce(v_supports_electronic, false) = false THEN
      RAISE EXCEPTION 'Client does not support electronic submission';
    END IF;
  ELSIF v_action = 'ALLOW_QR_AGAIN' THEN
    IF v_current_row.is_adjustment = true THEN
      RAISE EXCEPTION 'Adjustment timesheets cannot be converted to QR submission';
    END IF;

    v_hard_locked := (
      coalesce(v_tsfin_row.locked_by_invoice_id IS NOT NULL, false)
      OR coalesce(v_tsfin_row.paid_at_utc IS NOT NULL, false)
    );
    IF v_hard_locked THEN
      RAISE EXCEPTION 'Cannot allow QR again: timesheet is invoiced/locked or paid';
    END IF;

    v_has_qr_token := (btrim(coalesce(v_current_row.qr_token, '')) <> '');
    v_has_qr_generated := (v_current_row.qr_generated_at IS NOT NULL);
    v_has_qr_scanned := (v_current_row.qr_scanned_at IS NOT NULL);
    v_is_manual_only := (
      v_submission_mode = 'MANUAL'
      AND v_qr_status = ''
      AND v_has_qr_token = false
      AND v_has_qr_generated = false
      AND v_has_qr_scanned = false
    );

    IF v_is_manual_only = false THEN
      RAISE EXCEPTION 'Allow QR again is only valid for manual-only timesheets';
    END IF;
  ELSIF v_action = 'SWITCH_TO_MANUAL' THEN
    IF (
      v_current_row.authorised_at_server IS NOT NULL
      AND btrim(v_current_row.authorised_at_server::text) <> ''
      AND (v_current_row.revoked_at IS NULL OR btrim(v_current_row.revoked_at::text) = '')
    ) THEN
      RAISE EXCEPTION 'TIMESHEET_AUTHORISED_EDIT_BLOCKED: This timesheet is authorised. Unauthorise it before switching to manual.';
    END IF;

    IF v_scope <> 'WEEKLY' THEN
      RAISE EXCEPTION 'Only WEEKLY timesheets can be switched to manual';
    END IF;

    IF v_submission_mode <> 'ELECTRONIC' THEN
      RAISE EXCEPTION 'Timesheet is not an electronic weekly timesheet';
    END IF;

    IF v_contract_week_row.id IS NULL THEN
      RAISE EXCEPTION 'Timesheet not linked to a contract week';
    END IF;

    IF v_is_import_authoritative = true THEN
      RAISE EXCEPTION 'Import-authoritative timesheets (NHSP / HealthRoster weekly no-timesheets) cannot be converted to manual';
    END IF;

    IF v_tsfin_row.id IS NULL THEN
      RAISE EXCEPTION 'No financial snapshot to switch';
    END IF;

    v_hard_locked := (
      coalesce(v_tsfin_row.paid_at_utc IS NOT NULL, false)
      OR coalesce(v_tsfin_row.locked_by_invoice_id IS NOT NULL, false)
                      );
    IF v_hard_locked THEN
      RAISE EXCEPTION 'Cannot switch: timesheet already invoiced or paid';
    END IF;

    v_basis := upper(coalesce(v_tsfin_row.basis::text, ''));
    IF v_basis <> 'CONTRACT_WEEKLY' THEN
      RAISE EXCEPTION 'Only CONTRACT_WEEKLY timesheets can be switched to manual';
    END IF;
  ELSIF v_action = 'SWITCH_DAILY_TO_MANUAL' THEN
    IF (
      v_current_row.authorised_at_server IS NOT NULL
      AND btrim(v_current_row.authorised_at_server::text) <> ''
      AND (v_current_row.revoked_at IS NULL OR btrim(v_current_row.revoked_at::text) = '')
    ) THEN
      RAISE EXCEPTION 'TIMESHEET_AUTHORISED_EDIT_BLOCKED: This timesheet is authorised. Unauthorise it before switching to manual.';
    END IF;

    IF v_booking_id IS NULL
      OR btrim(v_booking_id) = ''
      OR btrim(v_booking_id) = '{}'
      OR lower(btrim(v_booking_id)) = 'null'
      OR lower(btrim(v_booking_id)) = 'undefined'
    THEN
      RAISE EXCEPTION 'Timesheet booking_id is invalid; cannot rotate versions. Please repair booking_id.';
    END IF;

    IF v_scope <> 'DAILY' THEN
      RAISE EXCEPTION 'Only DAILY timesheets can be switched to manual via this endpoint';
    END IF;

    IF v_submission_mode <> 'ELECTRONIC' THEN
      RAISE EXCEPTION 'Timesheet is not an electronic daily timesheet';
    END IF;

    IF v_tsfin_row.id IS NULL THEN
      RAISE EXCEPTION 'No financial snapshot to switch';
    END IF;

    v_hard_locked := (
      coalesce(v_tsfin_row.paid_at_utc IS NOT NULL, false)
      OR coalesce(v_tsfin_row.locked_by_invoice_id IS NOT NULL, false)
                      );
    IF v_hard_locked THEN
      RAISE EXCEPTION 'Cannot switch: timesheet already invoiced or paid';
    END IF;

    v_basis := upper(coalesce(v_tsfin_row.basis::text, ''));
    IF v_basis IN ('NHSP', 'NHSP_ADJUSTMENT', 'HEALTHROSTER_SELF_BILL', 'HEALTHROSTER_ADJUSTMENT') THEN
      RAISE EXCEPTION 'Cannot switch NHSP/HR-based daily timesheets to manual';
    END IF;
  ELSIF v_action = 'REVERT_TO_ELECTRONIC' THEN
    IF v_current_row.is_adjustment = true THEN
      RAISE EXCEPTION 'Adjustment timesheets cannot be reverted/converted to electronic submission';
    END IF;

    SELECT t_elec.*
    INTO v_electronic_row
    FROM public.timesheets AS t_elec
    WHERE t_elec.booking_id = v_booking_id
      AND upper(coalesce(t_elec.submission_mode::text, '')) = 'ELECTRONIC'
    ORDER BY t_elec.version DESC, t_elec.updated_at DESC, t_elec.created_at DESC
    LIMIT 1;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'No electronic version exists for this timesheet';
    END IF;

    IF v_tsfin_row.id IS NOT NULL THEN
      v_has_segment_invoice_lock := EXISTS (
        SELECT 1
        FROM jsonb_array_elements(
          CASE
            WHEN v_tsfin_row.invoice_breakdown_json IS NOT NULL
             AND jsonb_typeof(v_tsfin_row.invoice_breakdown_json) = 'object'
             AND jsonb_typeof(v_tsfin_row.invoice_breakdown_json -> 'segments') = 'array'
            THEN v_tsfin_row.invoice_breakdown_json -> 'segments'
            ELSE '[]'::jsonb
          END
        ) AS s(seg)
        WHERE nullif(btrim(coalesce(s.seg ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL
      );

      v_hard_locked := (
        coalesce(v_tsfin_row.paid_at_utc IS NOT NULL, false)
        OR coalesce(v_tsfin_row.locked_by_invoice_id IS NOT NULL, false)
                                OR v_has_segment_invoice_lock
      );

      IF v_hard_locked THEN
        RAISE EXCEPTION 'Cannot revert: current timesheet is invoiced or paid';
      END IF;
    END IF;

    v_has_qr_token := (btrim(coalesce(v_current_row.qr_token, '')) <> '');
    v_has_qr_generated := (v_current_row.qr_generated_at IS NOT NULL);
    v_has_qr_scanned := (v_current_row.qr_scanned_at IS NOT NULL);
    v_is_manual_only := (
      v_submission_mode = 'MANUAL'
      AND v_qr_status = ''
      AND v_has_qr_token = false
      AND v_has_qr_generated = false
      AND v_has_qr_scanned = false
    );

    IF v_is_manual_only = true AND coalesce(p_allow_manual_only, false) = false THEN
      RAISE EXCEPTION 'Cannot revert: current is manual-only (set allow_manual_only=true to override)';
    END IF;

    IF v_current_timesheet_id = v_electronic_row.timesheet_id AND coalesce(v_current_row.is_current, false) = true THEN
      RETURN jsonb_build_object(
        'ok', true,
        'action', v_action,
        'reverted', false,
        'reason', 'Electronic version is already current',
        'booking_id', v_booking_id,
        'current_timesheet_id', v_current_timesheet_id::text,
        'current_version', coalesce(v_electronic_row.version, 1),
        'requested_timesheet_id', v_requested_timesheet_id::text,
        'was_stale', v_was_stale
      );
    END IF;
  END IF;

  SELECT coalesce(max(t_ver.version), 0) + 1
  INTO v_next_version
  FROM public.timesheets AS t_ver
  WHERE t_ver.booking_id = v_booking_id;

  IF v_action <> 'REVERT_TO_ELECTRONIC' THEN
    UPDATE public.timesheets AS t_demote
    SET
      is_current = false,
      status = CASE
        WHEN v_action = 'REVERT_TO_ELECTRONIC' THEN t_demote.status
        ELSE 'REVOKED'::public.timesheet_status_enum
      END,
      revoked_reason = CASE
        WHEN v_action = 'CONVERT_QR_TO_MANUAL' THEN 'QR_CONVERTED_TO_MANUAL'
        WHEN v_action = 'ALLOW_ELECTRONIC_AGAIN' THEN 'ALLOW_ELECTRONIC_AGAIN'
        WHEN v_action = 'ALLOW_QR_AGAIN' THEN 'ALLOW_QR_AGAIN'
        WHEN v_action IN ('SWITCH_TO_MANUAL', 'SWITCH_DAILY_TO_MANUAL') THEN 'SWITCHED_TO_MANUAL'
        ELSE t_demote.revoked_reason
      END,
      revoked_by = CASE
        WHEN v_action IN ('CONVERT_QR_TO_MANUAL', 'ALLOW_ELECTRONIC_AGAIN', 'ALLOW_QR_AGAIN', 'SWITCH_TO_MANUAL', 'SWITCH_DAILY_TO_MANUAL')
        THEN p_actor_user_id::text
        ELSE t_demote.revoked_by
      END,
      updated_at = v_now
    WHERE t_demote.booking_id = v_booking_id
      AND t_demote.is_current = true;

    INSERT INTO public.timesheets (
      booking_id,
      occupant_key_norm,
      hospital_norm,
      ward_norm,
      job_title_norm,
      shift_label_norm,
      scheduled_start_iso,
      scheduled_end_iso,
      worked_start_iso,
      worked_end_iso,
      break_start_iso,
      break_end_iso,
      break_minutes,
      worked_minutes,
      week_ending_date,
      auth_name,
      auth_job_title,
      authorised_at_server,
      r2_nurse_key,
      r2_auth_key,
      img_sha256_nurse,
      img_sha256_auth,
      reference_number,
      reference_set_at,
      status,
      idempotency_key,
      client_hash,
      client_ua,
      created_at,
      updated_at,
      version,
      is_current,
      revoked_at,
      revoked_reason,
      revoked_by,
      contract_id,
      submission_mode,
      manual_pdf_r2_key,
      line_type,
      sheet_scope,
      actual_schedule_json,
      additional_units_week,
      additional_units_per_day,
      qr_token,
      qr_status,
      qr_payload_json,
      qr_generated_at,
      qr_scanned_at,
      qr_scan_info_json,
      qr_r2_key,
      day_references_json,
      manual_pdf_rotation_degrees,
      qr_last_sent_hash,
      qr_last_sent_at_utc,
      qr_signed_hash,
      qr_signed_at_utc,
      candidate_hint_text,
      band,
      generated_pdf_at_utc,
      is_adjustment,
      parent_timesheet_id,
      generated_pdf_refs_sig,
      generated_pdf_refs_snapshot_json,
      generated_pdf_refs_captured_at_utc,
      qr_sent_refs_sig,
      qr_sent_refs_snapshot_json,
      qr_sent_refs_captured_at_utc,
      correction_id,
      correction_kind,
      adjustment_origin
    )
    VALUES (
      v_booking_id,
      v_current_row.occupant_key_norm,
      v_current_row.hospital_norm,
      v_current_row.ward_norm,
      v_current_row.job_title_norm,
      v_current_row.shift_label_norm,
      v_current_row.scheduled_start_iso,
      v_current_row.scheduled_end_iso,
      v_current_row.worked_start_iso,
      v_current_row.worked_end_iso,
      v_current_row.break_start_iso,
      v_current_row.break_end_iso,
      v_current_row.break_minutes,
      v_current_row.worked_minutes,
      v_current_row.week_ending_date,
      v_current_row.auth_name,
      v_current_row.auth_job_title,
      CASE
        WHEN v_action = 'ALLOW_QR_AGAIN' THEN v_current_row.authorised_at_server
        ELSE NULL
      END,
      CASE
        WHEN v_action = 'ALLOW_QR_AGAIN' THEN NULL
        ELSE NULL
      END,
      CASE
        WHEN v_action = 'ALLOW_QR_AGAIN' THEN NULL
        ELSE NULL
      END,
      v_current_row.img_sha256_nurse,
      v_current_row.img_sha256_auth,
      v_current_row.reference_number,
      v_current_row.reference_set_at,
      CASE
        WHEN v_action IN ('ALLOW_ELECTRONIC_AGAIN', 'ALLOW_QR_AGAIN') THEN 'RECEIVED'::public.timesheet_status_enum
        ELSE v_current_row.status
      END,
      v_current_row.idempotency_key,
      v_current_row.client_hash,
      v_current_row.client_ua,
      v_now,
      v_now,
      v_next_version,
      true,
      NULL,
      NULL,
      NULL,
      v_current_row.contract_id,
      CASE
        WHEN v_action = 'ALLOW_ELECTRONIC_AGAIN' THEN 'ELECTRONIC'::public.submission_mode_enum
        ELSE 'MANUAL'::public.submission_mode_enum
      END,
      NULL,
      v_current_row.line_type,
      v_current_row.sheet_scope,
      v_current_row.actual_schedule_json,
      v_current_row.additional_units_week,
      v_current_row.additional_units_per_day,
      NULL,
      CASE
        WHEN v_action = 'ALLOW_QR_AGAIN' THEN 'PENDING'::public.timesheet_qr_status_enum
        ELSE NULL
      END,
      '{}'::jsonb,
      NULL,
      NULL,
      NULL,
      NULL,
      v_current_row.day_references_json,
      v_current_row.manual_pdf_rotation_degrees,
      CASE
        WHEN v_action = 'ALLOW_QR_AGAIN' THEN NULL
        ELSE v_current_row.qr_last_sent_hash
      END,
      CASE
        WHEN v_action = 'ALLOW_QR_AGAIN' THEN NULL
        ELSE v_current_row.qr_last_sent_at_utc
      END,
      CASE
        WHEN v_action = 'ALLOW_QR_AGAIN' THEN NULL
        ELSE v_current_row.qr_signed_hash
      END,
      CASE
        WHEN v_action = 'ALLOW_QR_AGAIN' THEN NULL
        ELSE v_current_row.qr_signed_at_utc
      END,
      v_current_row.candidate_hint_text,
      v_current_row.band,
      v_current_row.generated_pdf_at_utc,
      v_current_row.is_adjustment,
      v_current_row.parent_timesheet_id,
      v_current_row.generated_pdf_refs_sig,
      v_current_row.generated_pdf_refs_snapshot_json,
      v_current_row.generated_pdf_refs_captured_at_utc,
      v_current_row.qr_sent_refs_sig,
      v_current_row.qr_sent_refs_snapshot_json,
      v_current_row.qr_sent_refs_captured_at_utc,
      v_current_row.correction_id,
      v_current_row.correction_kind,
      v_current_row.adjustment_origin
    )
    RETURNING * INTO v_new_row;

    v_new_timesheet_id := v_new_row.timesheet_id;
    v_new_version := v_new_row.version;

    IF v_new_timesheet_id IS NULL THEN
      RAISE EXCEPTION 'Insert succeeded but no timesheet_id returned';
    END IF;

    IF v_action = 'SWITCH_TO_MANUAL' THEN
      IF v_contract_week_row.id IS NULL THEN
        RAISE EXCEPTION 'Timesheet not linked to a contract week';
      END IF;

      UPDATE public.contract_weeks AS cw_upd
      SET
        timesheet_id = v_new_timesheet_id,
        submission_mode_snapshot = 'MANUAL'::public.submission_mode_enum,
        updated_at = v_now
      WHERE cw_upd.id = v_contract_week_row.id;
    ELSIF v_action IN ('CONVERT_QR_TO_MANUAL', 'ALLOW_ELECTRONIC_AGAIN', 'ALLOW_QR_AGAIN') THEN
      IF v_contract_week_row.id IS NOT NULL THEN
        UPDATE public.contract_weeks AS cw_move
        SET
          timesheet_id = v_new_timesheet_id,
          updated_at = v_now
        WHERE cw_move.id = v_contract_week_row.id;
      END IF;
    END IF;

    IF v_action IN ('CONVERT_QR_TO_MANUAL', 'SWITCH_TO_MANUAL', 'SWITCH_DAILY_TO_MANUAL') THEN
      IF v_tsfin_row.id IS NULL THEN
        RAISE EXCEPTION 'No financial snapshot to switch';
      END IF;

      UPDATE public.timesheets_financials AS tf_upd
      SET
        timesheet_id = v_new_timesheet_id,
        timesheet_version = coalesce(v_new_row.version, v_next_version),
        processing_status = CASE
          WHEN v_action = 'CONVERT_QR_TO_MANUAL' THEN 'AWAITING_MANUAL_SIGNATURE'::public.ts_fin_processing_status_enum
          ELSE tf_upd.processing_status
        END,
        authorised_by_user_id = NULL,
        authorised_at_utc = NULL,
        updated_at = v_now
      WHERE tf_upd.id = v_tsfin_row.id;
    ELSIF v_action IN ('ALLOW_ELECTRONIC_AGAIN', 'ALLOW_QR_AGAIN') THEN
      IF v_tsfin_row.id IS NOT NULL THEN
        UPDATE public.timesheets_financials AS tf_upd
        SET
          timesheet_id = v_new_timesheet_id,
          timesheet_version = coalesce(v_new_row.version, v_next_version),
          processing_status = CASE
            WHEN v_action = 'ALLOW_ELECTRONIC_AGAIN' THEN 'UNASSIGNED'::public.ts_fin_processing_status_enum
            WHEN v_action = 'ALLOW_QR_AGAIN' THEN 'AWAITING_MANUAL_SIGNATURE'::public.ts_fin_processing_status_enum
            ELSE tf_upd.processing_status
          END,
          updated_at = v_now
        WHERE tf_upd.id = v_tsfin_row.id;
      END IF;
    END IF;

    IF v_action = 'CONVERT_QR_TO_MANUAL' THEN
      v_converted := true;
    ELSE
      v_switched := true;
    END IF;

    RETURN jsonb_build_object(
      'ok', true,
      'action', v_action,
      'converted', v_converted,
      'switched', v_switched,
      'reverted', false,
      'booking_id', v_booking_id,
      'contract_week_id', CASE WHEN v_contract_week_row.id IS NULL THEN NULL ELSE v_contract_week_row.id::text END,
      'old_timesheet_id', v_current_timesheet_id::text,
      'new_timesheet_id', v_new_timesheet_id::text,
      'current_timesheet_id', v_new_timesheet_id::text,
      'old_version', v_old_version,
      'new_version', v_new_version,
      'new_submission_mode', CASE
        WHEN v_action = 'ALLOW_ELECTRONIC_AGAIN' THEN 'ELECTRONIC'
        ELSE 'MANUAL'
      END,
      'has_electronic_original', CASE WHEN v_action IN ('SWITCH_TO_MANUAL', 'SWITCH_DAILY_TO_MANUAL') THEN true ELSE NULL END,
      'electronic_version', CASE WHEN v_action IN ('SWITCH_TO_MANUAL', 'SWITCH_DAILY_TO_MANUAL') THEN v_old_version ELSE NULL END,
      'requested_timesheet_id', v_requested_timesheet_id::text,
      'was_stale', v_was_stale
    );
  ELSE
    UPDATE public.timesheets AS t_demote
    SET
      is_current = false,
      updated_at = v_now
    WHERE t_demote.booking_id = v_booking_id
      AND t_demote.is_current = true;

    UPDATE public.timesheets AS t_promote
    SET
      is_current = true,
      qr_status = NULL,
      qr_token = NULL,
      qr_generated_at = NULL,
      qr_scanned_at = NULL,
      qr_scan_info_json = NULL,
      qr_r2_key = NULL,
      qr_payload_json = '{}'::jsonb,
      qr_last_sent_hash = NULL,
      qr_last_sent_at_utc = NULL,
      qr_signed_hash = NULL,
      qr_signed_at_utc = NULL,
      updated_at = v_now
    WHERE t_promote.timesheet_id = v_electronic_row.timesheet_id;

    IF v_contract_week_row.id IS NOT NULL THEN
      UPDATE public.contract_weeks AS cw_move
      SET
        timesheet_id = v_electronic_row.timesheet_id,
        updated_at = v_now
      WHERE cw_move.id = v_contract_week_row.id;
    END IF;

    IF v_tsfin_row.id IS NOT NULL THEN
      UPDATE public.timesheets_financials AS tf_move
      SET
        timesheet_id = v_electronic_row.timesheet_id,
        timesheet_version = coalesce(v_electronic_row.version, 1),
        updated_at = v_now
      WHERE tf_move.id = v_tsfin_row.id;
    END IF;

    v_reverted := true;
    v_electronic_version := coalesce(v_electronic_row.version, 1);

    RETURN jsonb_build_object(
      'ok', true,
      'action', v_action,
      'converted', false,
      'switched', false,
      'reverted', v_reverted,
      'booking_id', v_booking_id,
      'contract_week_id', CASE WHEN v_contract_week_row.id IS NULL THEN NULL ELSE v_contract_week_row.id::text END,
      'previous_current_timesheet_id', v_current_timesheet_id::text,
      'old_timesheet_id', v_current_timesheet_id::text,
      'new_timesheet_id', v_electronic_row.timesheet_id::text,
      'current_timesheet_id', v_electronic_row.timesheet_id::text,
      'electronic_version', v_electronic_version,
      'new_submission_mode', 'ELECTRONIC',
      'requested_timesheet_id', v_requested_timesheet_id::text,
      'was_stale', v_was_stale
    );
  END IF;
END;
$function$;
