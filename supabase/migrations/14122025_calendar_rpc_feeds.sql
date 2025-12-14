-- =========================================================
-- Candidate: contract list for current calendar window
-- =========================================================
create or replace function public.calendar_candidate_contracts_range(
  candidate_id uuid,
  from_date date,
  to_date date
)
returns table (
  contract_id uuid,
  client_id uuid,
  client_name text,
  role text,
  band text,
  start_date date,
  end_date date,
  "from" date,
  "to" date
)
language sql
stable
as $$
  select
    c.id as contract_id,
    c.client_id,
    cl.name as client_name,
    c.role,
    c.band,
    c.start_date,
    c.end_date,
    greatest(c.start_date, $2) as "from",
    least(coalesce(c.end_date, $3), $3) as "to"
  from contracts c
  left join clients cl on cl.id = c.client_id
  where c.candidate_id = $1
    and c.start_date <= $3
    and coalesce(c.end_date, $3) >= $2
  order by greatest(c.start_date, $2), cl.name nulls last, c.role nulls last, c.band nulls last;
$$;


-- =========================================================
-- Candidate: day feed across all candidate contracts
-- =========================================================
create or replace function public.calendar_candidate_day_feed(
  candidate_id uuid,
  from_date date,
  to_date date
)
returns table (
  "date" date,
  contract_id uuid,
  state text,

  planned boolean,
  has_tsfin boolean,
  any_ready boolean,
  pay_on_hold boolean,
  invoice_on_hold boolean,
  invoiced boolean,
  paid boolean,

  pay_line_on_hold boolean,
  invoice_line_on_hold boolean,

  invoice_nos text
)
language sql
stable
as $$
with candidate_contracts as (
  select c.id as contract_id
  from contracts c
  where c.candidate_id = $1
    and c.start_date <= $3
    and coalesce(c.end_date, $3) >= $2
),

planned_days as (
  select
    cw.contract_id,
    (p->>'date')::date as date
  from contract_weeks cw
  join candidate_contracts cc on cc.contract_id = cw.contract_id
  cross join lateral jsonb_array_elements(coalesce(cw.planned_schedule_json, '[]'::jsonb)) p
  where (p->>'date')::date between $2 and $3
),

ts_with_tf as (
  select
    ts.timesheet_id,
    ts.contract_id,

    -- link weekly timesheet to its contract_week via contract_weeks.timesheet_id
    cw.id as contract_week_id,
    cw.planned_schedule_json as cw_planned_schedule_json,

    ts.week_ending_date,
    ts.sheet_scope,
    ts.worked_start_iso,
    ts.actual_schedule_json,

    tf.id as tsfin_id,
    tf.processing_status,
    coalesce(tf.pay_on_hold,false) as pay_on_hold,
    tf.paid_at_utc,
    tf.locked_by_invoice_id,
    tf.invoice_breakdown_json
  from timesheets ts
  join candidate_contracts cc on cc.contract_id = ts.contract_id

  left join contract_weeks cw
    on cw.timesheet_id = ts.timesheet_id

  left join timesheets_financials tf
    on tf.timesheet_id = ts.timesheet_id
   and tf.is_current = true

  where
    (
      -- weekly timesheets relevant to window (+/- 14 days buffer)
      (ts.sheet_scope = 'WEEKLY'
       and ts.week_ending_date between ($2 - 14) and ($3 + 14)
      )
      or
      -- daily timesheets relevant to window
      (ts.sheet_scope = 'DAILY'
       and ts.worked_start_iso is not null
       and ((ts.worked_start_iso)::timestamptz at time zone 'Europe/London')::date between $2 and $3
      )
    )
),

-- 1) SEGMENTS mode: authoritative worked days come from segments[].date
segment_events as (
  select
    t.contract_id,
    (seg->>'date')::date as date,

    t.tsfin_id,
    t.processing_status::text as processing_status,
    t.pay_on_hold,
    t.paid_at_utc,

    -- invoice per segment (NHSP/HR), fallback to tsfin lock if present
    coalesce(
      nullif(seg->>'invoice_locked_invoice_id','')::uuid,
      t.locked_by_invoice_id
    ) as invoice_id,

    -- pay line hold (segment-level)
    coalesce((seg->>'exclude_from_pay')::boolean,false) as pay_line_on_hold,

    -- invoice line delayed/held (segment-level)
    (
      nullif(seg->>'invoice_locked_invoice_id','') is null
      and nullif(seg->>'invoice_target_week_start','') is not null
      and (
        (nullif(seg->>'invoice_target_week_start','')::date) <>
        (case
          when t.week_ending_date is not null then (t.week_ending_date - 6)
          else date_trunc('week', ((seg->>'date')::date)::timestamp)::date
        end)
      )
    ) as invoice_line_on_hold

  from ts_with_tf t
  cross join lateral jsonb_array_elements(coalesce(t.invoice_breakdown_json->'segments','[]'::jsonb)) seg
  where t.tsfin_id is not null
    and coalesce(t.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
    and (seg->>'date')::date between $2 and $3
),

-- 2) WEEKLY non-segments: worked days from timesheets.actual_schedule_json[*].date
weekly_schedule_events as (
  select
    t.contract_id,
    (s->>'date')::date as date,

    t.tsfin_id,
    t.processing_status::text as processing_status,
    t.pay_on_hold,
    t.paid_at_utc,
    t.locked_by_invoice_id as invoice_id,

    false as pay_line_on_hold,
    false as invoice_line_on_hold
  from ts_with_tf t
  cross join lateral jsonb_array_elements(coalesce(t.actual_schedule_json,'[]'::jsonb)) s
  where t.sheet_scope = 'WEEKLY'
    and coalesce(t.invoice_breakdown_json->>'mode','') <> 'SEGMENTS'
    and (s->>'date')::date between $2 and $3
),

-- 3) WEEKLY fallback: TSFIN exists but no segments & no schedule -> apply TSFIN onto linked planned days
weekly_plan_fallback_events as (
  select
    t.contract_id,
    (p->>'date')::date as date,

    t.tsfin_id,
    t.processing_status::text as processing_status,
    t.pay_on_hold,
    t.paid_at_utc,
    t.locked_by_invoice_id as invoice_id,

    false as pay_line_on_hold,
    false as invoice_line_on_hold
  from ts_with_tf t
  cross join lateral jsonb_array_elements(coalesce(t.cw_planned_schedule_json,'[]'::jsonb)) p
  where t.sheet_scope = 'WEEKLY'
    and t.tsfin_id is not null
    and coalesce(t.invoice_breakdown_json->>'mode','') <> 'SEGMENTS'
    and coalesce(jsonb_array_length(coalesce(t.actual_schedule_json,'[]'::jsonb)),0) = 0
    and (p->>'date')::date between $2 and $3
),

-- 4) DAILY: worked day from worked_start_iso UK date
daily_events as (
  select
    t.contract_id,
    ((t.worked_start_iso)::timestamptz at time zone 'Europe/London')::date as date,

    t.tsfin_id,
    t.processing_status::text as processing_status,
    t.pay_on_hold,
    t.paid_at_utc,
    t.locked_by_invoice_id as invoice_id,

    false as pay_line_on_hold,
    false as invoice_line_on_hold
  from ts_with_tf t
  where t.sheet_scope = 'DAILY'
    and t.worked_start_iso is not null
    and ((t.worked_start_iso)::timestamptz at time zone 'Europe/London')::date between $2 and $3
),

all_events as (
  -- planned markers
  select
    p.contract_id,
    p.date,
    true as planned,
    null::uuid as tsfin_id,
    null::text as processing_status,
    false as pay_on_hold,
    null::timestamptz as paid_at_utc,
    null::uuid as invoice_id,
    false as pay_line_on_hold,
    false as invoice_line_on_hold
  from planned_days p

  union all
  select
    e.contract_id,
    e.date,
    false as planned,
    e.tsfin_id,
    e.processing_status,
    e.pay_on_hold,
    e.paid_at_utc,
    e.invoice_id,
    e.pay_line_on_hold,
    e.invoice_line_on_hold
  from segment_events e

  union all
  select
    e.contract_id,
    e.date,
    false, e.tsfin_id, e.processing_status, e.pay_on_hold, e.paid_at_utc, e.invoice_id,
    e.pay_line_on_hold, e.invoice_line_on_hold
  from weekly_schedule_events e

  union all
  select
    e.contract_id,
    e.date,
    false, e.tsfin_id, e.processing_status, e.pay_on_hold, e.paid_at_utc, e.invoice_id,
    e.pay_line_on_hold, e.invoice_line_on_hold
  from weekly_plan_fallback_events e

  union all
  select
    e.contract_id,
    e.date,
    false, e.tsfin_id, e.processing_status, e.pay_on_hold, e.paid_at_utc, e.invoice_id,
    e.pay_line_on_hold, e.invoice_line_on_hold
  from daily_events e
),

events_with_invoice as (
  select
    a.*,
    inv.status::text as invoice_status,
    inv.invoice_no as invoice_no
  from all_events a
  left join invoices inv on inv.id = a.invoice_id
),

agg as (
  select
    date,
    contract_id,

    bool_or(planned) as planned,
    bool_or(tsfin_id is not null) as has_tsfin,
    bool_or(processing_status = 'READY_FOR_INVOICE') as any_ready,

    bool_or(pay_on_hold) as pay_on_hold,
    bool_or(invoice_status = 'ON_HOLD') as invoice_on_hold,
    bool_or(invoice_id is not null and coalesce(invoice_status,'') <> 'ON_HOLD') as invoiced,
    bool_or(paid_at_utc is not null) as paid,

    bool_or(pay_line_on_hold) as pay_line_on_hold,
    bool_or(invoice_line_on_hold) as invoice_line_on_hold,

    string_agg(distinct invoice_no, ',') filter (where invoice_no is not null) as invoice_nos
  from events_with_invoice
  group by date, contract_id
)

select
  date,
  contract_id,
  case
    when paid then 'PAID'
    when pay_on_hold and invoice_on_hold then 'PAY_AND_INVOICE_ON_HOLD'
    when pay_on_hold then 'PAY_ON_HOLD'
    when invoice_on_hold then 'INVOICE_ON_HOLD'
    when invoiced then 'INVOICED'
    when has_tsfin and any_ready then 'READY'
    when has_tsfin then 'PROCESSED_NOT_READY'
    when planned then 'PLANNED'
    else 'EMPTY'
  end as state,

  planned,
  has_tsfin,
  any_ready,
  pay_on_hold,
  invoice_on_hold,
  invoiced,
  paid,
  pay_line_on_hold,
  invoice_line_on_hold,
  invoice_nos
from agg
where date between $2 and $3
order by date, contract_id;
$$;


-- =========================================================
-- Contract: day feed for a single contract
-- (fixed: prevents contract_id shadowing)
-- =========================================================
create or replace function public.calendar_contract_day_feed(
  contract_id uuid,
  from_date date,
  to_date date
)
returns table (
  "date" date,
  contract_id uuid,
  state text,

  planned boolean,
  has_tsfin boolean,
  any_ready boolean,
  pay_on_hold boolean,
  invoice_on_hold boolean,
  invoiced boolean,
  paid boolean,

  pay_line_on_hold boolean,
  invoice_line_on_hold boolean,

  invoice_nos text
)
language sql
stable
as $$
  select *
  from public.calendar_candidate_day_feed(
    (select c.candidate_id from contracts c where c.id = $1),
    $2,
    $3
  ) as f
  where f.contract_id = $1;
$$;

