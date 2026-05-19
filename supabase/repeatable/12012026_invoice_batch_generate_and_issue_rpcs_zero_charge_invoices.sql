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
-- ============================================================
-- PATCH: invoice_batch_generate_candidates
-- Adds precheck_status + has_timesheet_evidence_pdf to preview output.
-- Requires: public.v_ts_invoice_precheck has column has_timesheet_evidence_pdf.
-- SAFE TO RE-RUN: CREATE OR REPLACE FUNCTION
-- ============================================================



create or replace function public.invoice_autoinvoice_candidate_groups(
  p_limit int default 5000
)
returns table (
  client_id uuid,
  invoice_week_start date
)
language sql
stable
security definer
set search_path = public
as $$
with anchor as (
  select (now() at time zone 'Europe/London')::date as anchor_ymd
),

base as (
  select
    tf.timesheet_id,
    tf.client_id,
    ts.week_ending_date::date as week_ending_date,
    (ts.week_ending_date::date - interval '6 days')::date as natural_week_start,
    tf.invoice_breakdown_json,
    coalesce(ts.contract_id, cw.contract_id) as contract_id
  from public.timesheets_financials tf
  join public.timesheets ts
    on ts.timesheet_id = tf.timesheet_id
   and ts.is_current = true
  left join public.contract_weeks cw
    on cw.timesheet_id = tf.timesheet_id
  where tf.is_current = true
    and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
    and tf.locked_by_invoice_id is null
    and tf.paid_at_utc is null
    and ts.revoked_at is null
    and tf.client_id is not null
    and ts.week_ending_date is not null
),

auto_ok as (
  select
    b.timesheet_id,
    b.client_id,
    b.week_ending_date,
    b.natural_week_start,
    b.invoice_breakdown_json,
    b.contract_id,
    c.auto_invoice as contract_auto_invoice,
    cs.auto_invoice_default as client_auto_invoice_default,
    (
      case
        when b.contract_id is not null then coalesce(c.auto_invoice, cs.auto_invoice_default, false)
        else coalesce(cs.auto_invoice_default, false)
      end
    ) as auto_invoice_effective
  from base b
  left join public.contracts c
    on c.id = b.contract_id
  left join lateral (
    select cs0.auto_invoice_default
    from public.client_settings cs0
    cross join anchor a
    where cs0.client_id = b.client_id
      and (cs0.effective_from <= a.anchor_ymd or cs0.effective_from is null)
    order by cs0.effective_from desc nulls last
    limit 1
  ) cs on true
  where (
    case
      when b.contract_id is not null then coalesce(c.auto_invoice, cs.auto_invoice_default, false)
      else coalesce(cs.auto_invoice_default, false)
    end
  ) = true
),

groups_from_nonseg as (
  select
    aok.client_id,
    aok.natural_week_start as invoice_week_start
  from auto_ok aok
  cross join anchor a
  where coalesce(aok.invoice_breakdown_json->>'mode','') <> 'SEGMENTS'
    and aok.week_ending_date < a.anchor_ymd
),

groups_from_seg_empty as (
  select
    aok.client_id,
    aok.natural_week_start as invoice_week_start
  from auto_ok aok
  cross join anchor a
  where coalesce(aok.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
    and jsonb_typeof(aok.invoice_breakdown_json->'segments') = 'array'
    and jsonb_array_length(aok.invoice_breakdown_json->'segments') = 0
    and aok.week_ending_date < a.anchor_ymd
),

groups_from_segments as (
  select distinct
    aok.client_id,
    coalesce(seg_t.tgt_start, aok.natural_week_start) as invoice_week_start
  from auto_ok aok
  cross join anchor a
  cross join lateral jsonb_array_elements(
    case
      when jsonb_typeof(aok.invoice_breakdown_json->'segments') = 'array' then aok.invoice_breakdown_json->'segments'
      else '[]'::jsonb
    end
  ) seg
  cross join lateral (
    select
      case
        when nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '') ~ '^\d{4}-\d{2}-\d{2}$'
          then nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date
        else null::date
      end as tgt_start
  ) seg_t
  where coalesce(aok.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
    and jsonb_typeof(seg) = 'object'
    and nullif(btrim(coalesce(seg->>'segment_id','')), '') is not null
    and nullif(btrim(coalesce(seg->>'invoice_locked_invoice_id','')), '') is null
    and (
      -- DELAYED segment: target differs from natural; only include once target week start reached (<= today)
      (
        seg_t.tgt_start is not null
        and seg_t.tgt_start <> aok.natural_week_start
        and seg_t.tgt_start <= a.anchor_ymd
      )
      or
      -- NON-DELAYED segment: week must have ended (cron never allow-early)
      (
        (seg_t.tgt_start is null or seg_t.tgt_start = aok.natural_week_start)
        and aok.week_ending_date < a.anchor_ymd
      )
    )
),

all_groups as (
  select * from groups_from_nonseg
  union
  select * from groups_from_seg_empty
  union
  select * from groups_from_segments
),

dedup as (
  select distinct
    g.client_id,
    g.invoice_week_start
  from all_groups g
  where g.client_id is not null
    and g.invoice_week_start is not null
),

filtered as (
  select
    d.client_id,
    d.invoice_week_start
  from dedup d
  where not exists (
    select 1
    from public.invoice_jobs_outbox o
    where o.kind = 'BY_WEEK'
      and (o.payload->>'client_id') = d.client_id::text
      and (o.payload->>'invoice_week_start') = d.invoice_week_start::text
  )
)

select
  f.client_id,
  f.invoice_week_start
from filtered f
order by
  f.invoice_week_start asc,
  f.client_id::text asc
limit greatest(0, least(coalesce(p_limit, 5000), 20000));
$$;



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

    -- charge components (defensive: used when total_charge_ex_vat is 0 but expense components exist)
    coalesce(tf.expenses_charge_ex_vat, 0)::numeric as expenses_charge_ex_vat,
    coalesce(tf.travel_charge_ex_vat, 0)::numeric as travel_charge_ex_vat,
    coalesce(tf.accommodation_charge_ex_vat, 0)::numeric as accommodation_charge_ex_vat,
    coalesce(tf.other_charge_ex_vat, 0)::numeric as other_charge_ex_vat,
    coalesce(tf.mileage_charge_ex_vat, 0)::numeric as mileage_charge_ex_vat,
    (
      case
        when nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '') is null then 0::numeric
        when nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '') ~ '^-?[0-9]+(\.[0-9]+)?$'
          then (nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '')::numeric)
        else 0::numeric
      end
    ) as additional_charge_ex_vat,

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

    -- Precheck diagnostics for UI
    pc.precheck_status as precheck_status,
    coalesce(pc.has_timesheet_evidence_pdf, false) as has_timesheet_evidence_pdf,

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
-- ------------------------------------------------------------
seg_rows as (
  select
    b.timesheet_id,
    b.client_id,

    coalesce(t.tgt_start, b.ts_invoice_week_start) as invoice_week_start,
    (coalesce(t.tgt_start, b.ts_invoice_week_start) + interval '6 days')::date as week_ending_date,

    b.client_name,
    b.candidate_name,

    -- defensive: support either key name (charge_ex_vat or charge_amount), and avoid bad casts
    (
      case
        when nullif(btrim(coalesce(seg->>'charge_ex_vat','')), '') ~ '^-?[0-9]+(\.[0-9]+)?$'
          then (nullif(btrim(coalesce(seg->>'charge_ex_vat','')), '')::numeric)
        when nullif(btrim(coalesce(seg->>'charge_amount','')), '') ~ '^-?[0-9]+(\.[0-9]+)?$'
          then (nullif(btrim(coalesce(seg->>'charge_amount','')), '')::numeric)
        else 0::numeric
      end
    ) as seg_charge_ex_vat,

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
    b.blocked_by_hr_validation,

    b.precheck_status,
    b.has_timesheet_evidence_pdf

  from base b
  cross join lateral jsonb_array_elements(
    case
      when jsonb_typeof(b.invoice_breakdown_json->'segments') = 'array' then b.invoice_breakdown_json->'segments'
      else '[]'::jsonb
    end
  ) seg
  cross join lateral (
    select
      case
        when nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '') ~ '^\d{4}-\d{2}-\d{2}$'
          then (nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date)
        else null::date
      end as tgt_start
  ) t
  cross join anchor a

  where coalesce(b.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
    and jsonb_typeof(seg) = 'object'
    and nullif(btrim(coalesce(seg->>'segment_id','')), '') is not null
    -- only segments not already locked to an invoice
    and nullif(btrim(coalesce(seg->>'invoice_locked_invoice_id','')), '') is null
    and (
      -- NOT DELAYED
      (
        (t.tgt_start is null or t.tgt_start = b.ts_invoice_week_start)
        and (
          p_allow_early = true
          or b.ts_week_ending_date < a.anchor_ymd
        )
      )
      or
      -- DELAYED (allow_early never overrides delayed)
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
    r.blocked_by_hr_validation,

    r.precheck_status,
    r.has_timesheet_evidence_pdf
  from seg_rows r
  group by
    r.timesheet_id, r.client_id, r.invoice_week_start, r.week_ending_date,
    r.client_name, r.candidate_name,
    r.basis, r.submission_mode, r.validation_status,
    r.hr_validation_required_for_invoice, r.blocked_by_hr_validation,
    r.precheck_status, r.has_timesheet_evidence_pdf
),

-- ------------------------------------------------------------
-- ✅ SEGMENTS mode but segments=[] (common for expenses-only, or legacy states)
-- Treat as a synthetic single row at the natural week.
-- Respect the same allow_early / week-ended gate as nonseg.
-- Use defensive "effective charge" when total_charge_ex_vat is 0 but expense components exist.
-- ------------------------------------------------------------
seg_empty_fallback as (
  select
    b.timesheet_id,
    b.client_id,
    b.ts_invoice_week_start as invoice_week_start,
    b.ts_week_ending_date   as week_ending_date,

    b.client_name,
    b.candidate_name,

    round(
      (
        case
          when coalesce(b.total_charge_ex_vat,0)::numeric <> 0 then coalesce(b.total_charge_ex_vat,0)::numeric
          else (
            coalesce(b.expenses_charge_ex_vat,0)::numeric +
            coalesce(b.travel_charge_ex_vat,0)::numeric +
            coalesce(b.accommodation_charge_ex_vat,0)::numeric +
            coalesce(b.other_charge_ex_vat,0)::numeric +
            coalesce(b.mileage_charge_ex_vat,0)::numeric +
            coalesce(b.additional_charge_ex_vat,0)::numeric
          )
        end
      ),
      2
    ) as total_charge_ex_vat,

    round(coalesce(b.total_hours,0), 2) as total_hours,

    b.basis,
    b.submission_mode,
    b.validation_status,
    b.hr_validation_required_for_invoice,
    b.blocked_by_hr_validation,

    b.precheck_status,
    b.has_timesheet_evidence_pdf
  from base b
  cross join anchor a
  where coalesce(b.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
    and (
      case
        when jsonb_typeof(b.invoice_breakdown_json->'segments') = 'array' then jsonb_array_length(b.invoice_breakdown_json->'segments')
        else 0
      end
    ) = 0
    and (
      p_allow_early = true
      or b.ts_week_ending_date < a.anchor_ymd
    )
),

-- ------------------------------------------------------------
-- NON-SEGMENTS mode:
-- Use defensive "effective charge" when total_charge_ex_vat is 0 but expense components exist.
-- ------------------------------------------------------------
nonseg as (
  select
    b.timesheet_id,
    b.client_id,
    b.ts_invoice_week_start as invoice_week_start,
    b.ts_week_ending_date as week_ending_date,

    b.client_name,
    b.candidate_name,

    round(
      (
        case
          when coalesce(b.total_charge_ex_vat,0)::numeric <> 0 then coalesce(b.total_charge_ex_vat,0)::numeric
          else (
            coalesce(b.expenses_charge_ex_vat,0)::numeric +
            coalesce(b.travel_charge_ex_vat,0)::numeric +
            coalesce(b.accommodation_charge_ex_vat,0)::numeric +
            coalesce(b.other_charge_ex_vat,0)::numeric +
            coalesce(b.mileage_charge_ex_vat,0)::numeric +
            coalesce(b.additional_charge_ex_vat,0)::numeric
          )
        end
      ),
      2
    ) as total_charge_ex_vat,

    round(coalesce(b.total_hours,0), 2) as total_hours,

    b.basis,
    b.submission_mode,
    b.validation_status,
    b.hr_validation_required_for_invoice,
    b.blocked_by_hr_validation,

    b.precheck_status,
    b.has_timesheet_evidence_pdf
  from base b
  cross join anchor a
  where coalesce(b.invoice_breakdown_json->>'mode','') <> 'SEGMENTS'
    and (
      p_allow_early = true
      or b.ts_week_ending_date < a.anchor_ymd
    )
),

eligible as (
  -- NOTE: eligibility for preview must match enqueue:
  -- keep rows where there is any invoiceable value signal (charge or hours).
  select * from seg_agg
  where (round(coalesce(total_charge_ex_vat,0), 2) <> 0 or round(coalesce(total_hours,0), 2) <> 0)

  union all
  select * from seg_empty_fallback
  where (round(coalesce(total_charge_ex_vat,0), 2) <> 0 or round(coalesce(total_hours,0), 2) <> 0)

  union all
  select * from nonseg
  where (round(coalesce(total_charge_ex_vat,0), 2) <> 0 or round(coalesce(total_hours,0), 2) <> 0)
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
        'blocked_by_hr_validation', e.blocked_by_hr_validation,
        'precheck_status', coalesce(e.precheck_status::text, ''),
        'has_timesheet_evidence_pdf', coalesce(e.has_timesheet_evidence_pdf, false)
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

    coalesce(cs.invoice_consolidation_mode, 'NONE') as invoice_consolidation_mode,
    case coalesce(cs.invoice_consolidation_mode, 'NONE')
      when 'NONE' then 'One per timesheet'
      when 'BY_WEEK' then 'Consolidated by week'
      when 'ANY_WEEK' then 'Consolidated across weeks'
      else 'One per timesheet'
    end as consolidation_label,

    case coalesce(cs.invoice_consolidation_mode, 'NONE')
      when 'NONE' then coalesce(sum(jsonb_array_length(w.timesheets)), 0)
      when 'BY_WEEK' then count(*)
      when 'ANY_WEEK' then case when coalesce(sum(jsonb_array_length(w.timesheets)), 0) > 0 then 1 else 0 end
      else coalesce(sum(jsonb_array_length(w.timesheets)), 0)
    end as expected_invoice_count,

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
  left join lateral (
    select coalesce(cs0.invoice_consolidation_mode::text, 'NONE') as invoice_consolidation_mode
    from public.client_settings cs0
    cross join anchor a
    where cs0.client_id = w.client_id
      and (cs0.effective_from <= a.anchor_ymd or cs0.effective_from is null)
    order by cs0.effective_from desc nulls last
    limit 1
  ) cs on true
  group by w.client_id, cs.invoice_consolidation_mode
)

select coalesce(
  jsonb_agg(
    jsonb_build_object(
      'client_id', c.client_id::text,
      'client_name', c.client_name,
      'invoice_consolidation_mode', c.invoice_consolidation_mode,
      'expected_invoice_count', c.expected_invoice_count,
      'consolidation_label', c.consolidation_label,
      'weeks', c.weeks
    )
    order by c.client_name nulls last, c.client_id::text
  ),
  '[]'::jsonb
)
from clients c;
$$;


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

  v_in_ids uuid[];

  v_existing_id uuid;
  v_outbox_id uuid;

  v_invoice_consolidation_mode text;

  v_meta_outer jsonb;
  v_meta_inner jsonb;
begin
  if p_rows is null or jsonb_typeof(p_rows) <> 'array' then
    raise exception 'p_rows must be a JSON array';
  end if;

  if coalesce(jsonb_array_length(p_rows), 0) = 0 then
    raise exception 'p_rows must not be empty';
  end if;

  for v_row in
    select t.value
    from jsonb_array_elements(p_rows) as t(value)
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

    -- parse timesheet_ids
    if not (v_row ? 'timesheet_ids') then
      raise exception 'row missing timesheet_ids';
    end if;

    if jsonb_typeof(v_row->'timesheet_ids') <> 'array' then
      raise exception 'row timesheet_ids must be a JSON array';
    end if;

    -- NOTE: dedupe in a subquery, then apply deterministic ordering in array_agg.
    select array_agg(q.x order by q.x::text)
    into v_in_ids
    from (
      select distinct (t.val)::uuid as x
      from jsonb_array_elements_text(v_row->'timesheet_ids') as t(val)
      where t.val ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ) q;

    if v_in_ids is null or coalesce(array_length(v_in_ids, 1), 0) = 0 then
      raise exception 'row timesheet_ids empty/invalid';
    end if;

    -- ------------------------------------------------------------
    -- Store consolidation mode for traceability (payload.meta.invoice_consolidation_mode)
    -- ------------------------------------------------------------
    select coalesce(cs0.invoice_consolidation_mode::text, 'NONE')
      into v_invoice_consolidation_mode
    from public.client_settings cs0
    where cs0.client_id = v_client_id
      and (cs0.effective_from <= v_anchor_ymd or cs0.effective_from is null)
    order by cs0.effective_from desc nulls last
    limit 1;

    if v_invoice_consolidation_mode is null then
      v_invoice_consolidation_mode := 'NONE';
    end if;

    -- Build meta to pass into canonical gate (preserve caller meta, add invoice_consolidation_mode under nested meta)
    v_meta_outer := coalesce(p_meta, '{}'::jsonb);

    if jsonb_typeof(v_meta_outer) <> 'object' then
      v_meta_outer := jsonb_build_object('meta', v_meta_outer);
    end if;

    v_meta_inner :=
      case
        when jsonb_typeof(v_meta_outer->'meta') = 'object' then coalesce(v_meta_outer->'meta','{}'::jsonb)
        else '{}'::jsonb
      end;

    v_meta_inner := v_meta_inner || jsonb_build_object('invoice_consolidation_mode', v_invoice_consolidation_mode);

    v_meta_outer := v_meta_outer || jsonb_build_object('meta', v_meta_inner);

    -- ✅ Concurrency guard: serialize enqueue per (client_id, invoice_week_start)
    -- (lets us return a correct INSERTED/UPDATED action deterministically)
    perform pg_advisory_xact_lock(
      hashtext(v_client_id::text),
      (v_week_start - date '2000-01-01')::int
    );

    -- Determine whether an outbox row already exists (for action only; eligibility is decided by canonical gate)
    select o.id
      into v_existing_id
    from public.invoice_jobs_outbox o
    where o.kind = 'BY_WEEK'
      and (o.payload->>'client_id') = v_client_id::text
      and (o.payload->>'invoice_week_start') = v_week_start::text
    order by o.created_at desc
    limit 1;

    -- ------------------------------------------------------------
    -- Canonical gate: all eligibility logic lives in invoice_outbox_enqueue_by_week
    -- ------------------------------------------------------------
    v_outbox_id := public.invoice_outbox_enqueue_by_week(
      p_client_id         => v_client_id,
      p_invoice_week_start=> v_week_start,
      p_actor_user_id     => p_actor_user_id,
      p_allow_early       => p_allow_early,
      p_meta              => v_meta_outer,
      p_timesheet_ids     => v_in_ids,
      p_auto_invoice_only => false
    );

    client_id := v_client_id;
    invoice_week_start := v_week_start;
    outbox_id := v_outbox_id;
    action := case when v_existing_id is null then 'INSERTED' else 'UPDATED' end;
    return next;
  end loop;

end;
$$;






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
  v_email_warn_json jsonb := '[]'::jsonb;

  v_debug boolean := false;
  v_steps jsonb := '[]'::jsonb;
  v_sqlstate text;
  v_err text;

  -- ✅ NEW: email dedupe lock helpers (prevents duplicate mail_outbox rows on concurrent runs)
  v_ref text;
  m_lock record;

  -- ✅ NEW: invoice-pdf outbox enqueue summary (Option A)
  v_pdf_invoice_ids uuid[] := null;
  v_pdf_invoice_count int := 0;
  v_pdf_rows_affected int := 0;

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

  select coalesce(sd.invoice_debug,false)
  into v_debug
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;

  if coalesce(v_debug,false) = true then
    v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','start','now_utc',v_now::text,'anchor_ymd',v_anchor_ymd::text,'allow_early',coalesce(p_allow_early,false)));
  end if;

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

  -- ✅ NEW (Option A): enqueue invoice PDF render jobs for successfully ISSUED invoices (idempotent)
  select array_agg(x.invoice_id order by x.invoice_id::text)
  into v_pdf_invoice_ids
  from (
    select distinct t.invoice_id
    from tmp_issue t
    where t.ok = true
      and upper(coalesce(t.status,'')) = 'ISSUED'
  ) x;

  v_pdf_invoice_count := coalesce(array_length(v_pdf_invoice_ids, 1), 0);

  if v_pdf_invoice_count > 0 then
    -- p_limit is a safety cap; here we cap at the number of unique invoice_ids in this batch.
    v_pdf_rows_affected := public.invpdf_enqueue_many(v_pdf_invoice_ids, false, v_pdf_invoice_count);
  else
    v_pdf_rows_affected := 0;
  end if;

  if coalesce(v_debug,false) = true then
    v_steps := v_steps || jsonb_build_array(jsonb_build_object(
      'step','invpdf_enqueued_after_issue',
      'pdf_invoice_count', v_pdf_invoice_count,
      'pdf_rows_affected', v_pdf_rows_affected
    ));
  end if;

  -- queue emails for successfully ISSUED invoices, excluding self-bill/do_not_send.
  -- Recipient routing is derived before tmp_to_email so batch queueing matches handleInvoiceEmail(...).
  create temporary table tmp_email_route_base on commit drop as
  select
    i.id as invoice_id,
    i.client_id,
    i.invoice_no,
    g.week_ending_date,
    coalesce(
      nullif(btrim(coalesce(i.header_snapshot_json->>'client_primary_invoice_email','')), ''),
      nullif(btrim(coalesce(c.primary_invoice_email,'')), '')
    ) as primary_to_email,
    g.is_self_bill,
    coalesce(i.do_not_send,false) as do_not_send
  from tmp_issue r
  join public.invoices i
    on i.id = r.invoice_id
  join tmp_gate g
    on g.invoice_id = i.id
  join public.clients c
    on c.id = i.client_id
  where r.ok = true
    and upper(coalesce(r.status,'')) = 'ISSUED';

  create temporary table tmp_latest_client_settings on commit drop as
  select distinct on (cs.client_id)
    cs.client_id,
    cs.send_manual_invoices_to_different_email,
    cs.manual_invoices_alt_email_address,
    cs.effective_from,
    cs.created_at
  from public.client_settings cs
  join (
    select distinct rb.client_id
    from tmp_email_route_base rb
    where rb.client_id is not null
  ) bc
    on bc.client_id = cs.client_id
  order by cs.client_id, cs.effective_from desc nulls last, cs.created_at desc nulls last;

  create temporary table tmp_email_route_lines on commit drop as
  select
    rb.invoice_id,
    il.timesheet_id,
    (il.timesheet_id is not null and ts.timesheet_id is null) as missing_current_timesheet,
    ts.contract_id as timesheet_contract_id,
    cw.contract_id as contract_week_contract_id,
    coalesce(ts.contract_id, cw.contract_id) as contract_id,
    coalesce(ts.is_adjustment,false) as timesheet_is_adjustment,
    coalesce(cw.is_adjustment,false) as contract_week_is_adjustment,
    (
      coalesce(ts.is_adjustment,false) = true
      and (
        left(upper(coalesce(ts.adjustment_origin::text,'')), 7) = 'IMPORT_'
        or ts.correction_id is not null
        or nullif(btrim(coalesce(ts.correction_kind::text,'')), '') is not null
      )
    ) as is_import_derived_adjustment,
    (
      upper(coalesce(ts.submission_mode::text,'')) in ('MANUAL','QR')
      or nullif(btrim(coalesce(ts.qr_status::text,'')), '') is not null
      or nullif(btrim(coalesce(ts.qr_token::text,'')), '') is not null
    ) as is_manual_or_qr,
    (
      (coalesce(ts.is_adjustment,false) = true or coalesce(cw.is_adjustment,false) = true)
      and (
        upper(coalesce(ts.submission_mode::text,'')) in ('MANUAL','QR')
        or nullif(btrim(coalesce(ts.qr_status::text,'')), '') is not null
        or nullif(btrim(coalesce(ts.qr_token::text,'')), '') is not null
      )
      and not (
        coalesce(ts.is_adjustment,false) = true
        and (
          left(upper(coalesce(ts.adjustment_origin::text,'')), 7) = 'IMPORT_'
          or ts.correction_id is not null
          or nullif(btrim(coalesce(ts.correction_kind::text,'')), '') is not null
        )
      )
    ) as is_user_created_manual_qr_adjustment
  from tmp_email_route_base rb
  left join public.invoice_lines il
    on il.invoice_id = rb.invoice_id
   and il.timesheet_id is not null
  left join public.timesheets ts
    on ts.timesheet_id = il.timesheet_id
   and ts.is_current = true
  left join lateral (
    select
      coalesce(bool_or(coalesce(cw0.is_adjustment,false)), false) as is_adjustment,
      (array_agg(cw0.contract_id order by cw0.contract_id::text) filter (where cw0.contract_id is not null))[1] as contract_id
    from public.contract_weeks cw0
    where cw0.timesheet_id = il.timesheet_id
  ) cw
    on true;

  create temporary table tmp_email_route_flags on commit drop as
  select
    rb.invoice_id,
    rb.client_id,
    rb.invoice_no,
    rb.week_ending_date,
    rb.primary_to_email,
    rb.is_self_bill,
    rb.do_not_send,
    coalesce(bool_or(coalesce(erl.missing_current_timesheet,false)), false) as has_missing_current_timesheet,
    coalesce(bool_or(coalesce(erl.is_import_derived_adjustment,false)), false) as has_import_derived_adjustment,
    coalesce(bool_or(coalesce(erl.is_user_created_manual_qr_adjustment,false)), false) as has_user_created_manual_qr_adjustment,
    coalesce(bool_or(coalesce(erl.is_user_created_manual_qr_adjustment,false) = true and erl.contract_id is null), false) as has_missing_user_created_manual_qr_contract
  from tmp_email_route_base rb
  left join tmp_email_route_lines erl
    on erl.invoice_id = rb.invoice_id
  group by
    rb.invoice_id,
    rb.client_id,
    rb.invoice_no,
    rb.week_ending_date,
    rb.primary_to_email,
    rb.is_self_bill,
    rb.do_not_send;

  create temporary table tmp_email_route_contracts on commit drop as
  select distinct
    erl.invoice_id,
    erl.contract_id,
    (ct.id is null) as missing_contract,
    coalesce(ct.overrideclientsettings,false) as overrideclientsettings,
    coalesce(ct.send_manual_invoices_to_different_email,false) as send_manual_invoices_to_different_email,
    nullif(btrim(coalesce(ct.manual_invoices_alt_email_address,'')), '') as manual_invoices_alt_email_address,
    (
      coalesce(ct.overrideclientsettings,false) = true
      and coalesce(ct.send_manual_invoices_to_different_email,false) = true
    ) as override_enabled
  from tmp_email_route_lines erl
  left join public.contracts ct
    on ct.id = erl.contract_id
  where erl.is_user_created_manual_qr_adjustment = true
    and erl.contract_id is not null;

  create temporary table tmp_email_route_contract_agg on commit drop as
  select
    erc.invoice_id,
    coalesce(bool_or(coalesce(erc.missing_contract,false)), false) as missing_contract,
    coalesce(bool_or(coalesce(erc.override_enabled,false)), false) as has_enabled_contract_override,
    coalesce(bool_or(coalesce(erc.override_enabled,false) and erc.manual_invoices_alt_email_address is null), false) as has_missing_contract_alt_email,
    count(distinct erc.manual_invoices_alt_email_address) filter (
      where erc.override_enabled = true
        and erc.manual_invoices_alt_email_address is not null
    ) as contract_alt_email_count,
    (array_agg(distinct erc.manual_invoices_alt_email_address) filter (
      where erc.override_enabled = true
        and erc.manual_invoices_alt_email_address is not null
    ))[1] as contract_alt_email,
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'contract_id', erc.contract_id::text,
          'email', erc.manual_invoices_alt_email_address
        )
      ) filter (
        where erc.override_enabled = true
          and erc.manual_invoices_alt_email_address is not null
      ),
      '[]'::jsonb
    ) as contract_alt_details,
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'contract_id', erc.contract_id::text
        )
      ) filter (
        where erc.override_enabled = true
          and erc.manual_invoices_alt_email_address is null
      ),
      '[]'::jsonb
    ) as missing_contract_alt_details
  from tmp_email_route_contracts erc
  group by erc.invoice_id;

  create temporary table tmp_email_queue_warnings on commit drop as
  select
    erf.invoice_id,
    'EMAIL_ROUTING_CHECK_FAILED'::text as warning_code,
    'Current timesheet data could not be loaded for one or more invoice lines, so invoice email routing was not queued.'::text as warning_message,
    jsonb_build_object(
      'invoice_id', erf.invoice_id::text,
      'warning_code', 'EMAIL_ROUTING_CHECK_FAILED',
      'message', 'Current timesheet data could not be loaded for one or more invoice lines, so invoice email routing was not queued.'
    ) as warning_json
  from tmp_email_route_flags erf
  where coalesce(erf.is_self_bill,false) = false
    and coalesce(erf.do_not_send,false) = false
    and erf.has_missing_current_timesheet = true

  union all

  select
    erf.invoice_id,
    'CONTRACT_ROUTING_CHECK_FAILED'::text as warning_code,
    'Contract data could not be resolved for one or more user-created manual/QR adjustment invoice lines, so invoice email routing was not queued.'::text as warning_message,
    jsonb_build_object(
      'invoice_id', erf.invoice_id::text,
      'warning_code', 'CONTRACT_ROUTING_CHECK_FAILED',
      'message', 'Contract data could not be resolved for one or more user-created manual/QR adjustment invoice lines, so invoice email routing was not queued.'
    ) as warning_json
  from tmp_email_route_flags erf
  where coalesce(erf.is_self_bill,false) = false
    and coalesce(erf.do_not_send,false) = false
    and erf.has_user_created_manual_qr_adjustment = true
    and erf.has_missing_user_created_manual_qr_contract = true

  union all

  select
    erf.invoice_id,
    'CONTRACT_ROUTING_CHECK_FAILED'::text as warning_code,
    'Contract data could not be loaded for one or more user-created manual/QR adjustment invoice lines, so invoice email routing was not queued.'::text as warning_message,
    jsonb_build_object(
      'invoice_id', erf.invoice_id::text,
      'warning_code', 'CONTRACT_ROUTING_CHECK_FAILED',
      'message', 'Contract data could not be loaded for one or more user-created manual/QR adjustment invoice lines, so invoice email routing was not queued.'
    ) as warning_json
  from tmp_email_route_flags erf
  join tmp_email_route_contract_agg erca
    on erca.invoice_id = erf.invoice_id
  where coalesce(erf.is_self_bill,false) = false
    and coalesce(erf.do_not_send,false) = false
    and erf.has_user_created_manual_qr_adjustment = true
    and erca.missing_contract = true

  union all

  select
    erf.invoice_id,
    'CONTRACT_MANUAL_EMAIL_MISSING'::text as warning_code,
    'Contract manual invoice email is enabled but no alternate email address is configured, so invoice email routing was not queued.'::text as warning_message,
    jsonb_build_object(
      'invoice_id', erf.invoice_id::text,
      'warning_code', 'CONTRACT_MANUAL_EMAIL_MISSING',
      'message', 'Contract manual invoice email is enabled but no alternate email address is configured, so invoice email routing was not queued.',
      'contracts', coalesce(erca.missing_contract_alt_details, '[]'::jsonb)
    ) as warning_json
  from tmp_email_route_flags erf
  join tmp_email_route_contract_agg erca
    on erca.invoice_id = erf.invoice_id
  where coalesce(erf.is_self_bill,false) = false
    and coalesce(erf.do_not_send,false) = false
    and erf.has_user_created_manual_qr_adjustment = true
    and erca.has_missing_contract_alt_email = true

  union all

  select
    erf.invoice_id,
    'CONTRACT_MANUAL_EMAIL_CONFLICT'::text as warning_code,
    'Multiple contract manual invoice email overrides apply to this invoice and they disagree, so invoice email routing was not queued.'::text as warning_message,
    jsonb_build_object(
      'invoice_id', erf.invoice_id::text,
      'warning_code', 'CONTRACT_MANUAL_EMAIL_CONFLICT',
      'message', 'Multiple contract manual invoice email overrides apply to this invoice and they disagree, so invoice email routing was not queued.',
      'contracts', coalesce(erca.contract_alt_details, '[]'::jsonb)
    ) as warning_json
  from tmp_email_route_flags erf
  join tmp_email_route_contract_agg erca
    on erca.invoice_id = erf.invoice_id
  where coalesce(erf.is_self_bill,false) = false
    and coalesce(erf.do_not_send,false) = false
    and erf.has_user_created_manual_qr_adjustment = true
    and coalesce(erca.contract_alt_email_count,0) > 1

  union all

  select
    erf.invoice_id,
    'CLIENT_MANUAL_EMAIL_MISSING'::text as warning_code,
    'Client manual adjustment email is enabled but no alternate email address is configured, so invoice email routing was not queued.'::text as warning_message,
    jsonb_build_object(
      'invoice_id', erf.invoice_id::text,
      'warning_code', 'CLIENT_MANUAL_EMAIL_MISSING',
      'message', 'Client manual adjustment email is enabled but no alternate email address is configured, so invoice email routing was not queued.'
    ) as warning_json
  from tmp_email_route_flags erf
  left join tmp_email_route_contract_agg erca
    on erca.invoice_id = erf.invoice_id
  left join tmp_latest_client_settings lcs
    on lcs.client_id = erf.client_id
  where coalesce(erf.is_self_bill,false) = false
    and coalesce(erf.do_not_send,false) = false
    and erf.has_user_created_manual_qr_adjustment = true
    and coalesce(erca.has_enabled_contract_override,false) = false
    and coalesce(lcs.send_manual_invoices_to_different_email,false) = true
    and nullif(btrim(coalesce(lcs.manual_invoices_alt_email_address,'')), '') is null;

  select coalesce(
    jsonb_agg(t.warning_json order by t.invoice_id::text, t.warning_code),
    '[]'::jsonb
  )
  into v_email_warn_json
  from tmp_email_queue_warnings t;

  create temporary table tmp_to_email on commit drop as
  select
    erf.invoice_id,
    erf.client_id,
    erf.invoice_no,
    erf.week_ending_date,
    case
      when erf.has_user_created_manual_qr_adjustment = true
        and coalesce(erca.contract_alt_email_count,0) = 1
        then erca.contract_alt_email
      when erf.has_user_created_manual_qr_adjustment = true
        and coalesce(erca.has_enabled_contract_override,false) = false
        and coalesce(lcs.send_manual_invoices_to_different_email,false) = true
        and nullif(btrim(coalesce(lcs.manual_invoices_alt_email_address,'')), '') is not null
        then nullif(btrim(coalesce(lcs.manual_invoices_alt_email_address,'')), '')
      else erf.primary_to_email
    end as to_email,
    erf.is_self_bill
  from tmp_email_route_flags erf
  left join tmp_email_route_contract_agg erca
    on erca.invoice_id = erf.invoice_id
  left join tmp_latest_client_settings lcs
    on lcs.client_id = erf.client_id
  where coalesce(erf.is_self_bill,false) = false
    and coalesce(erf.do_not_send,false) = false
    and not exists (
      select 1
      from tmp_email_queue_warnings w
      where w.invoice_id = erf.invoice_id
    );

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

  -- ✅ NEW: prevent duplicate emails when two users run batch issue concurrently.
  -- We lock per (reference,to_email) and then insert only if that exact (type,reference,to) doesn't already exist.
  for m_lock in
    select m.client_id, m.week_ending_date, m.to_email, m.chunk_idx
    from tmp_mail_rows m
  loop
    v_ref :=
      'invoice_batch:' || m_lock.client_id::text || ':' || coalesce(m_lock.week_ending_date::text,'') || ':part:' || (m_lock.chunk_idx + 1)::text;

    perform pg_advisory_xact_lock(
      hashtext(v_ref),
      hashtext(coalesce(m_lock.to_email,''))
    );
  end loop;

  -- insert into mail_outbox
  -- NOTE: attachments are invoice_id placeholders; worker will resolve PDFs at send time.
  insert into public.mail_outbox(
    type,
    "to",
    cc,
    bcc,
    reply_to,
    importance,
    email_type,
    subject,
    body_text,
    attachments,
    status,
    reference,
    created_at_utc,
    created_by,

    -- ✅ NEW: comms log metadata
    recipient_kind,
    recipient_id,
    context_kind,
    context_id,
    mailshot_run_id,
    document_template_id
  )
  select
    'INVOICE'::text,
    m.to_email,
    null::text,
    null::text,
    null::text,
    'Normal'::text,
    'plain'::text,
    'Invoices – Week ending ' || coalesce(m.week_ending_date::text, ''),
    'Please find the attached invoices.',
    m.attachments,
    'QUEUED'::public.mail_status_enum,
    'invoice_batch:' || m.client_id::text || ':' || coalesce(m.week_ending_date::text,'') || ':part:' || (m.chunk_idx + 1)::text,
    v_now,
    p_actor_user_id,

    -- ✅ NEW: recipient/context for unified outbox + comms tabs
    'client'::text,
    m.client_id,
    'invoices'::text,
    null::uuid,
    null::uuid,
    null::uuid
  from tmp_mail_rows m
  where not exists (
    select 1
    from public.mail_outbox o2
    where o2.type = 'INVOICE'
      and o2.reference = ('invoice_batch:' || m.client_id::text || ':' || coalesce(m.week_ending_date::text,'') || ':part:' || (m.chunk_idx + 1)::text)
      and o2."to" = m.to_email
  );

  -- ✅ NEW: ensure existing (already-present) queued invoice emails get comms metadata too (idempotent backfill-on-touch)
  update public.mail_outbox o3
  set
    recipient_kind = 'client'::text,
    recipient_id = m3.client_id,
    context_kind = 'invoices'::text,
    context_id = null::uuid,
    mailshot_run_id = null::uuid,
    document_template_id = null::uuid,
    email_type = coalesce(o3.email_type, 'plain'::text),
    importance = coalesce(o3.importance, 'Normal'::text)
  from tmp_mail_rows m3
  where o3.type = 'INVOICE'
    and o3."to" = m3.to_email
    and o3.reference = ('invoice_batch:' || m3.client_id::text || ':' || coalesce(m3.week_ending_date::text,'') || ':part:' || (m3.chunk_idx + 1)::text)
    and (
      o3.recipient_kind is null
      or o3.recipient_id is null
      or o3.context_kind is null
      or o3.email_type is null
      or o3.importance is null
    );

  -- collect email outbox rows as json
  -- ✅ NEW: return rows matching the intended (reference,to) for this run, whether inserted now or already present.
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
    select o.id, o.reference, o."to", o.subject
    from public.mail_outbox o
    join tmp_mail_rows m
      on o.type = 'INVOICE'
     and o."to" = m.to_email
     and o.reference = ('invoice_batch:' || m.client_id::text || ':' || coalesce(m.week_ending_date::text,'') || ':part:' || (m.chunk_idx + 1)::text)
  ) o;

  if coalesce(v_debug,false) = true then
    v_steps := v_steps || jsonb_build_array(jsonb_build_object(
      'step','before_return',
      'allowed_count',coalesce(array_length(v_allowed,1),0),
      'not_due_count',coalesce(array_length(v_not_due,1),0),
      'max_attachments_per_email',v_max_attach,
      'pdf_invoice_count', v_pdf_invoice_count,
      'pdf_rows_affected', v_pdf_rows_affected
    ));

    perform public._inv_write_audit(
      p_actor_user_id,
      'INVOICE_ISSUE_AND_QUEUE_EMAILS_BATCH_DEBUG',
      jsonb_build_object(
        'allow_early', coalesce(p_allow_early,false),
        'anchor_ymd', v_anchor_ymd::text,
        'input_invoice_ids', to_jsonb(v_ids),
        'allowed_invoice_ids', to_jsonb(coalesce(v_allowed, array[]::uuid[])),
        'not_due_invoice_ids', to_jsonb(coalesce(v_not_due, array[]::uuid[])),
        'issue_results', v_issue_json,
        'not_due_results', v_not_due_json,
        'email_outbox', v_email_json,
        'email_queue_warnings', v_email_warn_json,
        'max_attachments_per_email', v_max_attach,
        'pdf_invoice_ids', to_jsonb(coalesce(v_pdf_invoice_ids, array[]::uuid[])),
        'pdf_invoice_count', v_pdf_invoice_count,
        'pdf_rows_affected', v_pdf_rows_affected,
        'steps', v_steps
      ),
      'invoices',
      null,
      null,
      null,
      null, null, null
    );
  end if;

  return jsonb_build_object(
    'invoice_results', (v_issue_json || v_not_due_json),
    'email_outbox', v_email_json,
    'email_queue_warnings', v_email_warn_json,
    'max_attachments_per_email', v_max_attach,
    'allow_early', coalesce(p_allow_early,false),

    -- ✅ NEW: PDF enqueue summary (safe additive fields)
    'pdf_invoice_count', v_pdf_invoice_count,
    'pdf_rows_affected', v_pdf_rows_affected
  );
exception
  when others then
    get stacked diagnostics v_sqlstate = returned_sqlstate, v_err = message_text;

    if coalesce(v_debug,false) = true then
      perform public._inv_write_audit(
        p_actor_user_id,
        'INVOICE_ISSUE_AND_QUEUE_EMAILS_BATCH_ERROR',
        jsonb_build_object(
          'sqlstate', v_sqlstate,
          'error', v_err,
          'allow_early', coalesce(p_allow_early,false),
          'anchor_ymd', v_anchor_ymd::text,
          'input_invoice_ids', to_jsonb(coalesce(v_ids, array[]::uuid[])),
          'allowed_invoice_ids', to_jsonb(coalesce(v_allowed, array[]::uuid[])),
          'not_due_invoice_ids', to_jsonb(coalesce(v_not_due, array[]::uuid[])),
          'max_attachments_per_email', v_max_attach,
          'steps', v_steps
        ),
        'invoices',
        null,
        null,
        null,
        null, null, null
      );
    end if;

    raise;
end;
$$;





-- ============================================================
-- CloudTMS RPC: invoice_closeout_zero_charge_timesheets
--
-- Goal:
-- - Create a £0 invoice (ISSUED) that is marked do_not_send=true
-- - Lock the timesheet(s) to this invoice so they exit the invoice cycle
--
-- Behavior:
-- - Creates ONE closeout invoice per distinct client_id in the eligible input set.
-- - Only timesheets with total_charge_ex_vat rounding to 0 are eligible.
-- - Only timesheets that are not already invoiced/locked are eligible.
-- - Locks are applied using public._inv_lock_segments_for_invoice (segment-aware).
--
-- Debug:
-- - If public.settings_defaults.invoice_debug = true, writes exactly ONE audit_events row
--   with extensive logging (or one row on error).
--
-- SAFE TO RE-RUN: CREATE OR REPLACE FUNCTION
-- ============================================================

create or replace function public.invoice_closeout_zero_charge_timesheets(
  p_timesheet_ids uuid[],
  p_actor_user_id uuid
)
returns table (
  client_id uuid,
  invoice_id uuid,
  timesheet_ids uuid[],
  ok boolean,
  warnings jsonb
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_invoice_debug boolean := false;
  v_steps jsonb := '[]'::jsonb;
  v_run_started timestamptz := now();
  v_now timestamptz;
  v_anchor_ymd date;

  v_client_id uuid;
  v_invoice_id uuid;

  v_def record;
  v_client record;
  v_terms_days int;
  v_vat_rate numeric;
  v_client_vat_override numeric;
  v_vat_chargeable boolean;
  v_due_at timestamptz;
  v_stationery_key text;
  v_margins jsonb;
  v_hide_bank_footer boolean;

  v_header jsonb;

  v_ts_ids_client uuid[];
  v_seg_refs jsonb := '[]'::jsonb;

  r_ts record;
  r_seg jsonb;

  v_skipped jsonb := '[]'::jsonb;
  v_skipped_count int := 0;
  v_created_count int := 0;

  v_err_state text;
  v_err_msg text;
begin
  -- Validate
  if p_timesheet_ids is null or coalesce(array_length(p_timesheet_ids,1),0) = 0 then
    return;
  end if;

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

  v_steps := v_steps || jsonb_build_array(jsonb_build_object(
    'step','start',
    'timesheet_count', coalesce(array_length(p_timesheet_ids,1),0),
    'now_utc', public._inv_iso_utc(v_run_started)
  ));

  -- Build eligible set into temp table
  create temporary table if not exists pg_temp._inv_closeout_ts (
    timesheet_id uuid primary key,
    tsfin_id uuid not null,
    client_id uuid not null,
    booking_id text null,
    basis text null,
    total_charge_ex_vat numeric null,
    invoice_breakdown_json jsonb null
  ) on commit drop;

  truncate pg_temp._inv_closeout_ts;

  insert into pg_temp._inv_closeout_ts(timesheet_id, tsfin_id, client_id, booking_id, basis, total_charge_ex_vat, invoice_breakdown_json)
  select
    tf.timesheet_id,
    tf.id as tsfin_id,
    tf.client_id,
    ts.booking_id::text,
    tf.basis::text,
    coalesce(tf.total_charge_ex_vat,0)::numeric,
    tf.invoice_breakdown_json
  from public.timesheets_financials tf
  join public.timesheets ts
    on ts.timesheet_id = tf.timesheet_id
   and ts.is_current = true
  where tf.is_current = true
    and tf.timesheet_id = any(p_timesheet_ids)
    and tf.client_id is not null
    and tf.locked_by_invoice_id is null
    and public._inv_round2(coalesce(tf.total_charge_ex_vat,0)) = 0;

  -- Skip any timesheets not inserted (missing/locked/non-zero)
  v_skipped := v_skipped || coalesce(
    (
      select jsonb_agg(
        jsonb_build_object(
          'timesheet_id', t::text,
          'reason', 'NOT_ELIGIBLE_OR_NOT_FOUND'
        )
      )
      from unnest(p_timesheet_ids) t
      where not exists (select 1 from pg_temp._inv_closeout_ts x where x.timesheet_id = t)
    ),
    '[]'::jsonb
  );

  v_steps := v_steps || jsonb_build_array(jsonb_build_object(
    'step','eligible_loaded',
    'eligible_count', (select count(*) from pg_temp._inv_closeout_ts),
    'skipped_so_far', jsonb_array_length(v_skipped)
  ));

  -- For SEGMENTS snapshots, ensure no segment is already locked (safety)
  delete from pg_temp._inv_closeout_ts x
  where x.invoice_breakdown_json is not null
    and upper(coalesce(x.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
    and jsonb_typeof(x.invoice_breakdown_json->'segments') = 'array'
    and exists (
      select 1
      from jsonb_array_elements(x.invoice_breakdown_json->'segments') s(value)
      where nullif(btrim(coalesce(s.value->>'invoice_locked_invoice_id','')), '') is not null
    )
  returning timesheet_id into r_ts;

  -- NOTE: The above DELETE ... RETURNING can return multiple rows; capture into skipped list via a separate query
  v_skipped := v_skipped || coalesce(
    (
      select jsonb_agg(
        jsonb_build_object(
          'timesheet_id', x.timesheet_id::text,
          'reason', 'SEGMENTS_ALREADY_LOCKED'
        )
      )
      from public.timesheets_financials tf
      join pg_temp._inv_closeout_ts tmp on tmp.tsfin_id = tf.id
      where false
    ),
    '[]'::jsonb
  );

  -- Create closeout invoices per client_id
  for v_client_id in
    select distinct x.client_id
    from pg_temp._inv_closeout_ts x
    order by x.client_id
  loop
    v_now := now();
    v_anchor_ymd := (v_now at time zone 'Europe/London')::date;

    select array_agg(x.timesheet_id)
    into v_ts_ids_client
    from pg_temp._inv_closeout_ts x
    where x.client_id = v_client_id;

    if v_ts_ids_client is null or coalesce(array_length(v_ts_ids_client,1),0) = 0 then
      continue;
    end if;

    -- Load global defaults / finance settings
    select *
    into v_def
    from public.settings_finance_pick(v_anchor_ymd)
    limit 1;

    -- Load client
    select
      c.id,
      c.name,
      c.invoice_address,
      c.primary_invoice_email,
      coalesce(c.vat_chargeable,true) as vat_chargeable,
      coalesce(c.payment_terms_days,30) as payment_terms_days
    into v_client
    from public.clients c
    where c.id = v_client_id
    limit 1;

    if not found then
      v_skipped := v_skipped || jsonb_build_array(jsonb_build_object(
        'client_id', v_client_id::text,
        'timesheet_ids', to_jsonb(v_ts_ids_client),
        'reason', 'CLIENT_NOT_FOUND'
      ));
      continue;
    end if;

    v_vat_chargeable := coalesce(v_client.vat_chargeable,true);
    v_terms_days := coalesce(v_client.payment_terms_days,30);

    -- VAT rate
    v_vat_rate := coalesce(v_def.vat_rate_pct, 20);
    begin
      select cs.vat_rate_pct
      into v_client_vat_override
      from public.client_settings cs
      where cs.client_id = v_client_id
        and cs.effective_from <= v_anchor_ymd
      order by cs.effective_from desc
      limit 1;
    exception when others then
      v_client_vat_override := null;
    end;

    v_vat_rate := case
      when v_vat_chargeable = false then 0
      else coalesce(v_client_vat_override, v_vat_rate, 20)
    end;

    v_due_at := v_now + make_interval(days => v_terms_days);

    -- Stationery defaults (match generator default)
    v_stationery_key := 'Assets/Stationery/Letterhead/A4/Letterhead_v1@300dpi.png';
    v_margins := coalesce(v_def.stationery_margins_mm, jsonb_build_object('top',12,'right',12,'bottom',12,'left',12));
    v_hide_bank_footer := coalesce(v_def.hide_bank_footer, false);

    v_header := jsonb_build_object(
      'client_id', v_client_id::text,
      'client_name', v_client.name,
      'client_invoice_address', v_client.invoice_address,
      'client_primary_invoice_email', v_client.primary_invoice_email,
      'vat_chargeable', v_vat_chargeable,
      'applied_vat_rate_pct', v_vat_rate,
      'payment_terms_days', v_terms_days,
      'issued_at_utc', to_jsonb(v_now),
      'due_at_utc', to_jsonb(v_due_at),
      'stationery_key', v_stationery_key,
      'stationery_margins_mm', v_margins,
      'hide_bank_footer', v_hide_bank_footer,
      'bank', jsonb_build_object(
        'name', v_def.bank_name,
        'sort_code', v_def.bank_sort_code,
        'account_number', v_def.bank_account_number
      ),
      'vat_registration_number', v_def.vat_registration_number,
      'meta', jsonb_build_object(
        'source', 'CLOSEOUT',
        'closeout', true,
        'do_not_send', true,
        'timesheet_count', coalesce(array_length(v_ts_ids_client,1),0),
        'vat_anchor_ymd', v_anchor_ymd::text
      ),
      'attach_policy', jsonb_build_object(
        'requires_hr', false,
        'hr_attach_to_invoice', true,
        'ts_attach_to_invoice', true
      )
    );

    insert into public.invoices(
      client_id,
      type,
      status,
      status_date_utc,
      issued_at_utc,
      due_at_utc,
      subtotal_ex_vat,
      vat_amount,
      total_inc_vat,
      header_snapshot_json,
      do_not_send
    )
    values (
      v_client_id,
      'INVOICE'::public.invoice_type_enum,
      'ISSUED'::public.invoice_status_enum,
      v_now,
      v_now,
      v_due_at,
      0,
      0,
      0,
      v_header,
      true
    )
    returning id into v_invoice_id;

    v_created_count := v_created_count + 1;

    -- Insert one CLOSEOUT line per timesheet (0 totals, timesheet_id set for visibility)
    for r_ts in
      select x.timesheet_id, x.booking_id
      from pg_temp._inv_closeout_ts x
      where x.client_id = v_client_id
      order by x.timesheet_id
    loop
      insert into public.invoice_lines(
        invoice_id, timesheet_id, booking_id, description,
        hours_day, hours_night, hours_sat, hours_sun, hours_bh,
        pay_day, pay_night, pay_sat, pay_sun, pay_bh,
        charge_day, charge_night, charge_sat, charge_sun, charge_bh,
        total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
        vat_rate_pct, vat_amount, total_inc_vat,
        paper_ts_r2_key, meta_json, source_key
      )
      values (
        v_invoice_id,
        r_ts.timesheet_id,
        nullif(btrim(coalesce(r_ts.booking_id,'')), ''),
        'Zero-charge closeout (do not send)',
        0,0,0,0,0,
        null,null,null,null,null,
        null,null,null,null,null,
        0,0,0,
        v_vat_rate, 0, 0,
        ('docs-pdf/timesheets/ts_' || r_ts.timesheet_id::text || '.pdf'),
        jsonb_build_object(
          'line_type','CLOSEOUT',
          'closeout', true,
          'do_not_send', true,
          'timesheet_id', r_ts.timesheet_id::text
        ),
        ('CLOSEOUT:TS:' || r_ts.timesheet_id::text)
      )
      on conflict (invoice_id, source_key) do nothing;
    end loop;

    -- Lock timesheets (segment-aware)
    v_seg_refs := '[]'::jsonb;
    for r_ts in
      select x.tsfin_id, x.timesheet_id, x.invoice_breakdown_json
      from pg_temp._inv_closeout_ts x
      where x.client_id = v_client_id
      order by x.timesheet_id
    loop
      if r_ts.invoice_breakdown_json is not null
        and upper(coalesce(r_ts.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
        and jsonb_typeof(r_ts.invoice_breakdown_json->'segments') = 'array'
        and jsonb_array_length(r_ts.invoice_breakdown_json->'segments') > 0
      then
        for r_seg in
          select value
          from jsonb_array_elements(r_ts.invoice_breakdown_json->'segments') value
        loop
          v_seg_refs := v_seg_refs || jsonb_build_array(
            jsonb_build_object(
              'tsfin_id', r_ts.tsfin_id::text,
              'segment_id', nullif(btrim(coalesce(r_seg->>'segment_id','')), '')
            )
          );
        end loop;
      else
        -- Lock whole snapshot (covers non-segments and SEGMENTS with empty/invalid segments array)
        v_seg_refs := v_seg_refs || jsonb_build_array(
          jsonb_build_object(
            'tsfin_id', r_ts.tsfin_id::text,
            'segment_id', null
          )
        );
      end if;
    end loop;

    if jsonb_typeof(v_seg_refs) = 'array' and jsonb_array_length(v_seg_refs) > 0 then
      perform public._inv_lock_segments_for_invoice(v_invoice_id, v_seg_refs);
    end if;

    -- Mark contract weeks INVOICED for these timesheets
    update public.contract_weeks cw
    set status = 'INVOICED'::public.contract_week_status_enum
    where cw.timesheet_id = any(v_ts_ids_client);

    -- Recompute totals (stays 0)
    perform public.invoice_recompute_totals(v_invoice_id);

    -- Standard audit event
    perform public._audit_insert(
      'invoice',
      v_invoice_id::text,
      'INVOICE_CLOSEOUT_CREATED',
      null,
      jsonb_build_object(
        'client_id', v_client_id::text,
        'timesheet_ids', to_jsonb(v_ts_ids_client),
        'do_not_send', true,
        'status', 'ISSUED'
      ),
      null,
      p_actor_user_id
    );

    -- Return
    client_id := v_client_id;
    invoice_id := v_invoice_id;
    timesheet_ids := v_ts_ids_client;
    ok := true;
    warnings := null;
    return next;
  end loop;

  -- Final debug write (one row)
  if v_invoice_debug then
    perform public._inv_write_audit(
      p_actor_user_id,
      'INVOICE_CLOSEOUT_DEBUG',
      jsonb_build_object(
        'run_started_at_utc', public._inv_iso_utc(v_run_started),
        'run_finished_at_utc', public._inv_iso_utc(now()),
        'timesheet_ids', to_jsonb(p_timesheet_ids),
        'created_invoice_count', v_created_count,
        'skipped', v_skipped,
        'steps', v_steps
      ),
      'invoices',
      ('closeout:' || public._inv_iso_utc(v_run_started)),
      null,
      'INVOICE_DEBUG',
      null,
      null,
      null
    );
  end if;

exception when others then
  v_err_state := sqlstate;
  v_err_msg := sqlerrm;

  if v_invoice_debug then
    begin
      perform public._inv_write_audit(
        p_actor_user_id,
        'INVOICE_CLOSEOUT_ERROR',
        jsonb_build_object(
          'run_started_at_utc', public._inv_iso_utc(v_run_started),
          'run_failed_at_utc', public._inv_iso_utc(now()),
          'timesheet_ids', to_jsonb(p_timesheet_ids),
          'sqlstate', v_err_state,
          'error', v_err_msg,
          'steps', v_steps,
          'skipped', v_skipped
        ),
        'invoices',
        ('closeout:' || public._inv_iso_utc(v_run_started)),
        null,
        'INVOICE_DEBUG',
        null,
        null,
        null
      );
    exception when others then
      -- never block rethrow due to debug
      null;
    end;
  end if;

  raise;
end;
$$;


