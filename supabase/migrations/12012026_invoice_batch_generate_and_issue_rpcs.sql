-- ============================================================
-- NEW: public.invoice_batch_generate_candidates(p_allow_early, p_limit)
-- Purpose:
--   - Returns "ready to invoice" timesheets grouped by client, then week ending
--   - Default behaviour (p_allow_early=false): only include weeks already ended
--   - UI override (p_allow_early=true): include future week-ending too
--
-- Eligibility source of truth:
--   - tf.is_current = true
--   - tf.processing_status = READY_FOR_INVOICE
--   - tf.locked_by_invoice_id is null
--   - timesheets is_current + not revoked
--   - v_ts_invoice_precheck.precheck_status = OK
--
-- Returns:
--   jsonb array: [{client_id, client_name, weeks:[{invoice_week_start, week_ending_date, subtotal_ex_vat, total_hours, timesheets:[...]}]}]
-- ============================================================

create or replace function public.invoice_batch_generate_candidates(
  p_allow_early boolean default false,
  p_limit int default 5000
)
returns jsonb
language sql
stable
security definer
set search_path = public
as $$
with anchor as (
  select (now() at time zone 'Europe/London')::date as anchor_ymd
),

-- ------------------------------------------------------------
-- Base TSFIN rows that are "ready for invoice" at the timesheet level.
-- Segment gating is applied later.
-- ------------------------------------------------------------
base as (
  select
    tf.timesheet_id,
    tf.client_id,
    ts.week_ending_date::date as ts_week_ending_date,
    (ts.week_ending_date::date - interval '6 days')::date as ts_invoice_week_start,

    s.client_name,
    s.candidate_name,

    tf.total_charge_ex_vat,
    tf.total_hours,
    tf.basis,
    ts.submission_mode,
    s.validation_status,

    coalesce(s.hr_validation_required_for_invoice, false) as hr_validation_required_for_invoice,
    (
      coalesce(s.hr_validation_required_for_invoice, false) = true
      and (
        s.validation_status is null
        or s.validation_status <> all (array[
          'VALIDATION_OK'::public.validation_status_enum,
          'OVERRIDDEN'::public.validation_status_enum
        ])
      )
    ) as blocked_by_hr_validation,

    tf.invoice_breakdown_json

  from public.timesheets_financials tf
  join public.timesheets ts
    on ts.timesheet_id = tf.timesheet_id
   and ts.is_current = true
  join public.v_ts_invoice_precheck pc
    on pc.timesheet_id = tf.timesheet_id
  left join public.v_timesheets_summary_base s
    on s.timesheet_id = tf.timesheet_id

  where tf.is_current = true
    and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
    and tf.locked_by_invoice_id is null
    and ts.revoked_at is null
    and upper(coalesce(pc.precheck_status,'')) = 'OK'
),

-- ------------------------------------------------------------
-- SEGMENTS mode:
-- Include ONLY unlocked segments.
--
-- Eligibility rules (as requested):
--  A) If the parent timesheet week-ending has passed:
--       include segments that are NOT delayed.
--  B) If allow_early = true:
--       include segments that are NOT delayed even if week-ending not passed.
--  C) If a segment IS delayed (invoice_target_week_start differs from the
--       timesheet's natural week start), include it only once its delay date
--       has arrived: invoice_target_week_start <= LondonToday.
--
-- Grouping:
--   invoice_week_start is the segment's target week (if set) else the
--   timesheet's natural week start, so delayed segments appear in later weeks.
-- ------------------------------------------------------------
seg_rows as (
  select
    b.timesheet_id,
    b.client_id,

    coalesce(t.tgt_start, b.ts_invoice_week_start) as invoice_week_start,
    (coalesce(t.tgt_start, b.ts_invoice_week_start) + interval '6 days')::date as week_ending_date,

    b.client_name,
    b.candidate_name,

    coalesce(nullif(seg->>'charge_amount','')::numeric, 0) as seg_charge_ex_vat,

    (
      coalesce(nullif(seg->>'hours_day','')::numeric, 0) +
      coalesce(nullif(seg->>'hours_night','')::numeric, 0) +
      coalesce(nullif(seg->>'hours_sat','')::numeric, 0) +
      coalesce(nullif(seg->>'hours_sun','')::numeric, 0) +
      coalesce(nullif(seg->>'hours_bh','')::numeric, 0)
    ) as seg_hours_total,

    b.basis,
    b.submission_mode,
    b.validation_status,
    b.hr_validation_required_for_invoice,
    b.blocked_by_hr_validation

  from base b
  cross join lateral jsonb_array_elements(coalesce(b.invoice_breakdown_json->'segments','[]'::jsonb)) seg
  cross join lateral (
    select nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date as tgt_start
  ) t
  cross join anchor a

  where coalesce(b.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
    -- only segments not already locked to an invoice
    and nullif(btrim(coalesce(seg->>'invoice_locked_invoice_id','')), '') is null
    and (
      -- NOT DELAYED: either no target week, or target week equals the natural week start
      (
        (t.tgt_start is null or t.tgt_start = b.ts_invoice_week_start)
        and (
          p_allow_early = true
          or b.ts_week_ending_date < a.anchor_ymd
        )
      )
      or
      -- DELAYED: target week differs from natural week start; include only when delay date has arrived
      (
        t.tgt_start is not null
        and t.tgt_start <> b.ts_invoice_week_start
        and t.tgt_start <= a.anchor_ymd
      )
    )
),

seg_agg as (
  select
    r.timesheet_id,
    r.client_id,
    r.invoice_week_start,
    r.week_ending_date,

    r.client_name,
    r.candidate_name,

    round(coalesce(sum(r.seg_charge_ex_vat),0), 2) as total_charge_ex_vat,
    round(coalesce(sum(r.seg_hours_total),0), 2) as total_hours,

    r.basis,
    r.submission_mode,
    r.validation_status,
    r.hr_validation_required_for_invoice,
    r.blocked_by_hr_validation
  from seg_rows r
  group by
    r.timesheet_id, r.client_id, r.invoice_week_start, r.week_ending_date,
    r.client_name, r.candidate_name,
    r.basis, r.submission_mode, r.validation_status,
    r.hr_validation_required_for_invoice, r.blocked_by_hr_validation
),

-- ------------------------------------------------------------
-- NON-SEGMENTS mode:
-- allow_early controls whether future week-ending timesheets can be included.
-- ------------------------------------------------------------
nonseg as (
  select
    b.timesheet_id,
    b.client_id,
    b.ts_invoice_week_start as invoice_week_start,
    b.ts_week_ending_date as week_ending_date,

    b.client_name,
    b.candidate_name,

    b.total_charge_ex_vat,
    b.total_hours,
    b.basis,
    b.submission_mode,
    b.validation_status,
    b.hr_validation_required_for_invoice,
    b.blocked_by_hr_validation
  from base b
  cross join anchor a
  where coalesce(b.invoice_breakdown_json->>'mode','') <> 'SEGMENTS'
    and (
      p_allow_early = true
      or b.ts_week_ending_date < a.anchor_ymd
    )
),

eligible as (
  select * from seg_agg
  union all
  select * from nonseg
),

eligible_limited as (
  select *
  from eligible
  order by week_ending_date desc nulls last, client_name nulls last, candidate_name nulls last
  limit greatest(1, least(coalesce(p_limit, 5000), 20000))
),

weeks as (
  select
    e.client_id,
    max(e.client_name) as client_name,
    e.invoice_week_start,
    e.week_ending_date,

    round(coalesce(sum(e.total_charge_ex_vat),0), 2) as subtotal_ex_vat,
    round(coalesce(sum(e.total_hours),0), 2) as total_hours,

    jsonb_agg(
      jsonb_build_object(
        'timesheet_id', e.timesheet_id::text,
        'candidate_name', e.candidate_name,
        'week_ending_date', e.week_ending_date::text,
        'total_charge_ex_vat', round(coalesce(e.total_charge_ex_vat,0),2),
        'total_hours', round(coalesce(e.total_hours,0),2),
        'basis', e.basis::text,
        'submission_mode', coalesce(e.submission_mode::text, ''),
        'validation_status', coalesce(e.validation_status::text, ''),
        'hr_validation_required_for_invoice', e.hr_validation_required_for_invoice,
        'blocked_by_hr_validation', e.blocked_by_hr_validation
      )
      order by e.candidate_name nulls last, e.timesheet_id::text
    ) as timesheets

  from eligible_limited e
  group by e.client_id, e.invoice_week_start, e.week_ending_date
),

clients as (
  select
    w.client_id,
    max(w.client_name) as client_name,
    jsonb_agg(
      jsonb_build_object(
        'invoice_week_start', w.invoice_week_start::text,
        'week_ending_date', w.week_ending_date::text,
        'subtotal_ex_vat', w.subtotal_ex_vat,
        'total_hours', w.total_hours,
        'timesheets', w.timesheets
      )
      order by w.week_ending_date desc
    ) as weeks
  from weeks w
  group by w.client_id
)

select coalesce(
  jsonb_agg(
    jsonb_build_object(
      'client_id', c.client_id::text,
      'client_name', c.client_name,
      'weeks', c.weeks
    )
    order by c.client_name nulls last, c.client_id::text
  ),
  '[]'::jsonb
)
from clients c;
$$;


-- ============================================================
-- NEW: public.invoice_outbox_enqueue_by_week_selected(p_rows, p_actor_user_id, p_allow_early, p_meta)
-- Purpose:
--   - Enqueue 1 BY_WEEK outbox job per (client_id, invoice_week_start)
--   - Payload includes timesheet_ids restriction (selected-only invoicing)
--   - Includes allow_early boolean for whole modal run
--
-- Safety:
--   - Validates all selected timesheets are eligible + match the specified (client, week)
--   - Default behaviour (p_allow_early=false): refuses future week-ending batches
--   - UI override (p_allow_early=true): allows future week-ending batches
--
-- Idempotency:
--   - If an outbox row already exists for (client_id, invoice_week_start), it UPDATEs payload by merge
--   - Otherwise inserts a new row
--
-- Input p_rows JSON format (array of objects):
--   [
--     {
--       "client_id": "<uuid>",
--       "invoice_week_start": "YYYY-MM-DD",
--       "timesheet_ids": ["<uuid>", ...]
--     },
--     ...
--   ]
--
-- Returns rows:
--   (client_id, invoice_week_start, outbox_id, action)
-- ============================================================

create or replace function public.invoice_outbox_enqueue_by_week_selected(
  p_rows jsonb,
  p_actor_user_id uuid,
  p_allow_early boolean default false,
  p_meta jsonb default null
)
returns table (
  client_id uuid,
  invoice_week_start date,
  outbox_id uuid,
  action text
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_anchor_ymd date := (now() at time zone 'Europe/London')::date;

  v_row jsonb;

  v_client_id uuid;
  v_week_start date;
  v_week_end date;

  v_in_ids uuid[];
  v_ok_ids uuid[];

  v_sig text;

  v_existing_id uuid;
  v_payload jsonb;
  v_update_payload jsonb;
begin
  if p_rows is null or jsonb_typeof(p_rows) <> 'array' then
    raise exception 'p_rows must be a JSON array';
  end if;

  if coalesce(jsonb_array_length(p_rows), 0) = 0 then
    raise exception 'p_rows must not be empty';
  end if;

  for v_row in
    select value
    from jsonb_array_elements(p_rows) t(value)
  loop
    if jsonb_typeof(v_row) <> 'object' then
      raise exception 'each element of p_rows must be a JSON object';
    end if;

    v_client_id := nullif(btrim(coalesce(v_row->>'client_id','')), '')::uuid;
    v_week_start := (v_row->>'invoice_week_start')::date;

    if v_client_id is null then
      raise exception 'row missing client_id';
    end if;

    if v_week_start is null then
      raise exception 'row missing invoice_week_start';
    end if;

    v_week_end := (v_week_start + interval '6 days')::date;

    -- default: do not allow future week-ending; UI override allows
    if (p_allow_early is not true) and (v_week_end >= v_anchor_ymd) then
      raise exception 'Week ending % has not passed (London today=%). Use allow_early to override.', v_week_end, v_anchor_ymd;
    end if;

    -- parse timesheet_ids
    if not (v_row ? 'timesheet_ids') then
      raise exception 'row missing timesheet_ids';
    end if;

    if jsonb_typeof(v_row->'timesheet_ids') <> 'array' then
      raise exception 'row timesheet_ids must be a JSON array';
    end if;

    -- NOTE: Postgres disallows ORDER BY expressions in DISTINCT aggregates unless they are part of the DISTINCT argument list.
    -- We dedupe in a subquery, then apply deterministic ordering in array_agg.
    select array_agg(x order by x::text)
    into v_in_ids
    from (
      select distinct (val)::uuid as x
      from jsonb_array_elements_text(v_row->'timesheet_ids') as t(val)
      where t.val ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ) q;

    if v_in_ids is null or coalesce(array_length(v_in_ids, 1), 0) = 0 then
      raise exception 'row timesheet_ids empty/invalid';
    end if;

    -- Validate that selected ids are eligible AND match (client, week)
    -- NOTE: Postgres disallows ORDER BY expressions in DISTINCT aggregates unless they are part of the DISTINCT argument list.
    -- We dedupe in a subquery, then apply deterministic ordering in array_agg.
    select array_agg(x order by x::text)
    into v_ok_ids
    from (
      select distinct tf.timesheet_id as x
      from public.timesheets_financials tf
      join public.timesheets ts
        on ts.timesheet_id = tf.timesheet_id
       and ts.is_current = true
      join public.v_ts_invoice_precheck pc
        on pc.timesheet_id = tf.timesheet_id
      where tf.is_current = true
        and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
        and tf.locked_by_invoice_id is null
        and ts.revoked_at is null
        and upper(coalesce(pc.precheck_status,'')) = 'OK'
        and tf.client_id = v_client_id
        and ts.week_ending_date::date = v_week_end
        and tf.timesheet_id = any(v_in_ids)
    ) q;

    if v_ok_ids is null or coalesce(array_length(v_ok_ids, 1), 0) = 0 then
      raise exception 'No eligible timesheets for client=% and week_end=%', v_client_id, v_week_end;
    end if;

    if array_length(v_ok_ids, 1) <> array_length(v_in_ids, 1) then
      raise exception 'Some selected timesheets are not eligible or do not match client/week (client=% week_end=%)', v_client_id, v_week_end;
    end if;

    -- deterministic signature
    v_sig := md5(array_to_string(v_ok_ids::text[], '|'));

    -- payload to merge into invoice_jobs_outbox.payload
    v_payload := jsonb_build_object(
      'client_id', v_client_id::text,
      'invoice_week_start', v_week_start::text,
      'timesheet_ids', to_jsonb(v_ok_ids),
      'timesheet_ids_sig', v_sig,
      'allow_early', coalesce(p_allow_early, false)
    );

    if p_actor_user_id is not null then
      v_payload := v_payload || jsonb_build_object('actor_user_id', p_actor_user_id::text);
    end if;

    if p_meta is not null then
      if jsonb_typeof(p_meta) = 'object' then
        v_payload := v_payload || p_meta;
      else
        v_payload := v_payload || jsonb_build_object('meta', p_meta);
      end if;
    end if;

    -- find existing outbox row for this (client, week)
    select o.id
    into v_existing_id
    from public.invoice_jobs_outbox o
    where o.kind = 'BY_WEEK'
      and (o.payload->>'client_id') = v_client_id::text
      and (o.payload->>'invoice_week_start') = v_week_start::text
    order by o.created_at desc
    limit 1;

    if v_existing_id is not null then
      -- merge payload so we don't drop any existing keys; new keys override old
      update public.invoice_jobs_outbox o
      set payload = coalesce(o.payload, '{}'::jsonb) || v_payload
      where o.id = v_existing_id;

      client_id := v_client_id;
      invoice_week_start := v_week_start;
      outbox_id := v_existing_id;
      action := 'UPDATED';
      return next;
    else
      insert into public.invoice_jobs_outbox(kind, payload)
      values ('BY_WEEK', v_payload)
      returning id into v_existing_id;

      client_id := v_client_id;
      invoice_week_start := v_week_start;
      outbox_id := v_existing_id;
      action := 'INSERTED';
      return next;
    end if;

  end loop;

end;
$$;


-- ============================================================
-- NEW: public.invoice_batch_issue_candidates(p_allow_early, p_limit)
-- SAFE TO RE-RUN: CREATE OR REPLACE FUNCTION
--
-- Returns JSON grouped by:
--   client -> week_ending_date -> invoices[]
--
-- Week ending derivation (robust):
--   1) Prefer MAX(timesheets.week_ending_date) from invoice_lines.timesheet_id
--   2) Fallback to header_snapshot_json meta.invoice_week_start + 6 days (if present)
--
-- Default: exclude weeks that have not ended yet (London date).
-- Override: p_allow_early=true includes future week-ending groups.
-- ============================================================

create or replace function public.invoice_batch_issue_candidates(
  p_allow_early boolean default false,
  p_limit int default 2000
)
returns jsonb
language sql
stable
security definer
set search_path = public
as $$
with anchor as (
  select (now() at time zone 'Europe/London')::date as anchor_ymd
),
inv_base as (
  select
    i.id as invoice_id,
    i.client_id,
    i.invoice_no,
    i.status::text as status,
    i.on_hold_reason,
    coalesce(i.subtotal_ex_vat, 0)::numeric as subtotal_ex_vat,
    coalesce(i.vat_amount, 0)::numeric as vat_amount,
    coalesce(i.total_inc_vat, 0)::numeric as total_inc_vat,
    i.header_snapshot_json,

    -- detect self-bill from header meta (robust string test)
    (
      lower(coalesce(i.header_snapshot_json #>> '{meta,self_bill}', i.header_snapshot_json->>'self_bill', '')) in ('true','t','1','yes')
    ) as is_self_bill,

    -- week_start fallback from header meta (robust)
    nullif(btrim(coalesce(i.header_snapshot_json #>> '{meta,invoice_week_start}',
                          i.header_snapshot_json->>'invoice_week_start', '')), '') as hdr_week_start_txt

  from public.invoices i
  where i.type::text = 'INVOICE'
    and i.status::text in ('DRAFT','ON_HOLD')
  order by i.created_at desc nulls last
  limit greatest(1, least(coalesce(p_limit, 2000), 20000))
),
inv_week as (
  select
    b.invoice_id,
    b.client_id,
    b.invoice_no,
    b.status,
    b.on_hold_reason,
    b.subtotal_ex_vat,
    b.vat_amount,
    b.total_inc_vat,
    b.is_self_bill,
    b.header_snapshot_json,

    -- compute week_start from header meta (if parseable)
    case
      when b.hdr_week_start_txt ~ '^\d{4}-\d{2}-\d{2}$' then b.hdr_week_start_txt::date
      else null::date
    end as invoice_week_start,

    -- compute week_end:
    --   prefer max(timesheets.week_ending_date), else week_start+6
    coalesce(
      max(ts.week_ending_date)::date,
      case
        when b.hdr_week_start_txt ~ '^\d{4}-\d{2}-\d{2}$'
          then (b.hdr_week_start_txt::date + interval '6 days')::date
        else null::date
      end
    ) as week_ending_date

  from inv_base b
  left join public.invoice_lines il
    on il.invoice_id = b.invoice_id
   and il.timesheet_id is not null
  left join public.timesheets ts
    on ts.timesheet_id = il.timesheet_id

  group by
    b.invoice_id, b.client_id, b.invoice_no, b.status, b.on_hold_reason,
    b.subtotal_ex_vat, b.vat_amount, b.total_inc_vat, b.is_self_bill, b.header_snapshot_json,
    b.hdr_week_start_txt
),
filtered as (
  select
    w.*,
    c.name as client_name
  from inv_week w
  join public.clients c on c.id = w.client_id
  cross join anchor a
  where
    -- default: only weeks already ended
    (p_allow_early = true)
    or (w.week_ending_date is null)              -- if unknown week_end, don't block listing
    or (w.week_ending_date < a.anchor_ymd)
),
weeks as (
  select
    f.client_id,
    max(f.client_name) as client_name,
    f.week_ending_date,
    f.invoice_week_start,

    round(coalesce(sum(f.subtotal_ex_vat),0),2) as subtotal_ex_vat_sum,
    round(coalesce(sum(f.total_inc_vat),0),2) as total_inc_vat_sum,

    jsonb_agg(
      jsonb_build_object(
        'invoice_id', f.invoice_id::text,
        'invoice_no', f.invoice_no,
        'status', f.status,
        'on_hold_reason', f.on_hold_reason,
        'subtotal_ex_vat', round(coalesce(f.subtotal_ex_vat,0),2),
        'vat_amount', round(coalesce(f.vat_amount,0),2),
        'total_inc_vat', round(coalesce(f.total_inc_vat,0),2),
        'is_self_bill', f.is_self_bill
      )
      order by f.status desc, f.invoice_no nulls last, f.invoice_id::text
    ) as invoices
  from filtered f
  group by f.client_id, f.week_ending_date, f.invoice_week_start
),
clients as (
  select
    w.client_id,
    max(w.client_name) as client_name,
    jsonb_agg(
      jsonb_build_object(
        'invoice_week_start', case when w.invoice_week_start is null then null else w.invoice_week_start::text end,
        'week_ending_date', case when w.week_ending_date is null then null else w.week_ending_date::text end,
        'subtotal_ex_vat_sum', w.subtotal_ex_vat_sum,
        'total_inc_vat_sum', w.total_inc_vat_sum,
        'invoices', w.invoices
      )
      order by w.week_ending_date desc nulls last
    ) as weeks
  from weeks w
  group by w.client_id
)
select coalesce(
  jsonb_agg(
    jsonb_build_object(
      'client_id', c.client_id::text,
      'client_name', c.client_name,
      'weeks', c.weeks
    )
    order by c.client_name nulls last, c.client_id::text
  ),
  '[]'::jsonb
)
from clients c;
$$;

-- ============================================================
-- NEW: public.invoice_issue_and_queue_emails_batch(...)
-- SAFE TO RE-RUN: CREATE OR REPLACE FUNCTION
--
-- Behaviour:
--  1) Applies week-ended gate unless p_allow_early=true:
--       week_ending_date < LondonToday
--     (week_ending derived from invoice_lines->timesheets, fallback header meta week_start+6)
--
--  2) Calls public.invoice_issue_batch(...) for allowed invoice_ids.
--     Any "not due yet" invoices are returned as failures with error='NOT_DUE_YET'.
--
--  3) For successfully issued invoices:
--     - Skips email enqueue if invoice is self-bill (header meta self_bill=true)
--     - Groups by (client_id, week_ending_date, to_email)
--     - Chunks attachments by settings_defaults.max_attachments_per_email (default 30)
--     - Inserts mail_outbox rows with attachments = [{invoice_id, filename}, ...]
--
-- Return:
--   jsonb object:
--     {
--       "invoice_results": [...],          -- per-invoice results (includes NOT_DUE_YET)
--       "email_outbox":   [...],           -- each queued email row summary
--       "max_attachments_per_email": N
--     }
-- ============================================================

create or replace function public.invoice_issue_and_queue_emails_batch(
  p_invoice_ids uuid[],
  p_actor_user_id uuid,
  p_allow_early boolean default false
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_anchor_ymd date := (now() at time zone 'Europe/London')::date;

  v_ids uuid[];
  v_allowed uuid[];
  v_not_due uuid[];

  v_max_attach int := 30;

  v_issue_json jsonb := '[]'::jsonb;
  v_not_due_json jsonb := '[]'::jsonb;
  v_email_json jsonb := '[]'::jsonb;

begin
  if p_invoice_ids is null or coalesce(array_length(p_invoice_ids,1),0) = 0 then
    raise exception 'invoice_ids[] required';
  end if;

  -- normalize ids
  select array_agg(x order by x::text)
  into v_ids
  from (
    select distinct unnest(p_invoice_ids) as x
  ) q
  where q.x is not null;

  if v_ids is null or coalesce(array_length(v_ids,1),0) = 0 then
    raise exception 'invoice_ids[] required';
  end if;

  -- global chunk size
  select coalesce(sd.max_attachments_per_email, 30)
  into v_max_attach
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;

  if v_max_attach is null or v_max_attach < 1 then
    v_max_attach := 30;
  end if;

  -- gate by week end unless allow_early=true
  create temporary table tmp_gate on commit drop as
  with inv as (
    select
      i.id as invoice_id,
      i.client_id,
      i.invoice_no,
      i.header_snapshot_json,
      i.status::text as status,

      -- self-bill detect
      (
        lower(coalesce(i.header_snapshot_json #>> '{meta,self_bill}', i.header_snapshot_json->>'self_bill', '')) in ('true','t','1','yes')
      ) as is_self_bill,

      -- week_start fallback (header)
      nullif(btrim(coalesce(i.header_snapshot_json #>> '{meta,invoice_week_start}',
                            i.header_snapshot_json->>'invoice_week_start', '')), '') as hdr_week_start_txt
    from public.invoices i
    where i.id = any(v_ids)
      and i.type::text = 'INVOICE'
      and i.status::text in ('DRAFT','ON_HOLD','ISSUED')  -- allow idempotency if already issued
  ),
  wk as (
    select
      inv.invoice_id,
      inv.client_id,
      inv.invoice_no,
      inv.status,
      inv.is_self_bill,

      case
        when inv.hdr_week_start_txt ~ '^\d{4}-\d{2}-\d{2}$' then inv.hdr_week_start_txt::date
        else null::date
      end as invoice_week_start,

      coalesce(
        max(ts.week_ending_date)::date,
        case
          when inv.hdr_week_start_txt ~ '^\d{4}-\d{2}-\d{2}$'
            then (inv.hdr_week_start_txt::date + interval '6 days')::date
          else null::date
        end
      ) as week_ending_date

    from inv
    left join public.invoice_lines il
      on il.invoice_id = inv.invoice_id
     and il.timesheet_id is not null
    left join public.timesheets ts
      on ts.timesheet_id = il.timesheet_id
    group by inv.invoice_id, inv.client_id, inv.invoice_no, inv.status, inv.is_self_bill, inv.hdr_week_start_txt
  )
  select
    wk.invoice_id,
    wk.client_id,
    wk.invoice_no,
    wk.status,
    wk.is_self_bill,
    wk.invoice_week_start,
    wk.week_ending_date,
    (
      p_allow_early = true
      or wk.week_ending_date is null
      or wk.week_ending_date < v_anchor_ymd
    ) as due_ok
  from wk;

  -- split allowed vs not_due
  select array_agg(g.invoice_id order by g.invoice_id::text)
  into v_allowed
  from tmp_gate g
  where g.due_ok = true;

  select array_agg(g.invoice_id order by g.invoice_id::text)
  into v_not_due
  from tmp_gate g
  where g.due_ok = false;

  -- build NOT_DUE_YET results (for UI)
  if v_not_due is not null and array_length(v_not_due,1) > 0 then
    select coalesce(
      jsonb_agg(
        jsonb_build_object(
          'invoice_id', g.invoice_id::text,
          'ok', false,
          'status', null,
          'issued_at_utc', null,
          'on_hold_reason', null,
          'reasons', null,
          'error', 'NOT_DUE_YET'
        )
        order by g.invoice_id::text
      ),
      '[]'::jsonb
    )
    into v_not_due_json
    from tmp_gate g
    where g.invoice_id = any(v_not_due);
  end if;

  -- issue allowed invoices (if any)
  create temporary table tmp_issue on commit drop as
  select *
  from public.invoice_issue_batch(coalesce(v_allowed, array[]::uuid[]), p_actor_user_id);

  select coalesce(jsonb_agg(to_jsonb(t) order by t.invoice_id::text), '[]'::jsonb)
  into v_issue_json
  from tmp_issue t;

  -- queue emails for successfully ISSUED invoices, excluding self-bill
  create temporary table tmp_to_email on commit drop as
  select
    i.id as invoice_id,
    i.client_id,
    i.invoice_no,
    g.week_ending_date,

    -- recipient: prefer snapshot primary invoice email if present, else clients.primary_invoice_email
    coalesce(
      nullif(btrim(coalesce(i.header_snapshot_json->>'client_primary_invoice_email','')), ''),
      nullif(btrim(coalesce(c.primary_invoice_email,'')), '')
    ) as to_email,

    g.is_self_bill
  from tmp_issue r
  join public.invoices i
    on i.id = r.invoice_id
  join tmp_gate g
    on g.invoice_id = i.id
  join public.clients c
    on c.id = i.client_id
  where r.ok = true
    and upper(coalesce(r.status,'')) = 'ISSUED'
    and coalesce(g.is_self_bill,false) = false;

  -- build queued mail_outbox rows in chunks
  create temporary table tmp_mail_rows on commit drop as
  with base as (
    select
      t.client_id,
      t.week_ending_date,
      t.to_email,
      t.invoice_id,
      t.invoice_no
    from tmp_to_email t
    where t.to_email is not null and length(btrim(t.to_email)) > 0
  ),
  numbered as (
    select
      b.*,
      row_number() over (
        partition by b.client_id, b.week_ending_date, b.to_email
        order by b.invoice_no nulls last, b.invoice_id::text
      ) as rn
    from base b
  ),
  chunked as (
    select
      n.client_id,
      n.week_ending_date,
      n.to_email,
      floor((n.rn - 1)::numeric / v_max_attach)::int as chunk_idx,
      jsonb_agg(
        jsonb_build_object(
          'invoice_id', n.invoice_id::text,
          'filename', case
            when n.invoice_no is not null and length(btrim(n.invoice_no)) > 0
              then ('Invoice_' || btrim(n.invoice_no) || '.pdf')
            else ('Invoice_' || n.invoice_id::text || '.pdf')
          end
        )
        order by n.rn
      ) as attachments,
      array_agg(n.invoice_id order by n.rn) as invoice_ids
    from numbered n
    group by n.client_id, n.week_ending_date, n.to_email, floor((n.rn - 1)::numeric / v_max_attach)::int
  )
  select * from chunked;

  -- insert into mail_outbox
  -- NOTE: attachments are invoice_id placeholders; worker will resolve PDFs at send time.
  insert into public.mail_outbox(
    type,
    "to",
    cc,
    subject,
    body_text,
    attachments,
    status,
    reference,
    created_at_utc,
    created_by
  )
  select
    'INVOICE'::text,
    m.to_email,
    null::text,
    'Invoices – Week ending ' || coalesce(m.week_ending_date::text, ''),
    'Please find the attached invoices.',
    m.attachments,
    'QUEUED'::public.mail_status_enum,
    'invoice_batch:' || m.client_id::text || ':' || coalesce(m.week_ending_date::text,'') || ':part:' || (m.chunk_idx + 1)::text,
    v_now,
    p_actor_user_id
  from tmp_mail_rows m
  returning
    id,
    reference,
    "to",
    subject;

  -- collect email outbox rows as json
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'mail_outbox_id', o.id::text,
        'to', o."to",
        'subject', o.subject,
        'reference', o.reference
      )
      order by o.id::text
    ),
    '[]'::jsonb
  )
  into v_email_json
  from (
    select id, reference, "to", subject
    from public.mail_outbox
    where created_at_utc = v_now
      and created_by is not distinct from p_actor_user_id
      and type = 'INVOICE'
      and reference like 'invoice_batch:%'
  ) o;

  return jsonb_build_object(
    'invoice_results', (v_issue_json || v_not_due_json),
    'email_outbox', v_email_json,
    'max_attachments_per_email', v_max_attach,
    'allow_early', coalesce(p_allow_early,false)
  );
end;
$$;
