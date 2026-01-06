-- ============================================================
-- RPC A: settings_finance_pick(p_date date)
-- Returns the single effective finance window for a given date.
-- - If p_date is null: uses "today" in Europe/London.
-- - Picks date_from <= date and (date_to is null or date_to >= date).
-- - Prefers most recent date_from (desc).
-- - Fallback: settings_defaults id=1 (as a single "window") if none exist.
-- ============================================================

create or replace function public.settings_finance_pick(p_date date default null)
returns table (
  id uuid,
  date_from date,
  date_to date,

  vat_rate_pct numeric,
  erni_pct numeric,
  holiday_pay_pct numeric,

  apply_holiday_to text,
  apply_erni_to text,
  margin_includes jsonb,

  source text
)
language sql
stable
as $$
with params as (
  select coalesce(p_date, (now() at time zone 'Europe/London')::date) as d
),
pick as (
  select
    w.id,
    w.date_from,
    w.date_to,
    w.vat_rate_pct,
    w.erni_pct,
    w.holiday_pay_pct,
    w.apply_holiday_to,
    w.apply_erni_to,
    w.margin_includes,
    'FINANCE_WINDOWS'::text as source
  from public.settings_finance_windows w
  join params p on true
  where w.date_from <= p.d
    and (w.date_to is null or w.date_to >= p.d)
  order by w.date_from desc
  limit 1
),
fallback as (
  select
    gen_random_uuid() as id,
    coalesce(sd.effective_from, date '1900-01-01') as date_from,
    null::date as date_to,
    coalesce(sd.vat_rate_pct, 20) as vat_rate_pct,
    coalesce(sd.erni_pct, 0) as erni_pct,
    coalesce(sd.holiday_pay_pct, 0) as holiday_pay_pct,
    sd.apply_holiday_to,
    sd.apply_erni_to,
    sd.margin_includes,
    'SETTINGS_DEFAULTS_FALLBACK'::text as source
  from public.settings_defaults sd
  where sd.id = 1
)
select * from pick
union all
select * from fallback
where not exists (select 1 from pick);
$$;


-- ============================================================
-- RPC B: settings_finance_list()
-- Returns all finance windows ordered newest-first (date_from desc).
-- Intended for front-end management UI.
-- ============================================================

create or replace function public.settings_finance_list()
returns table (
  id uuid,
  date_from date,
  date_to date,

  vat_rate_pct numeric,
  erni_pct numeric,
  holiday_pay_pct numeric,

  apply_holiday_to text,
  apply_erni_to text,
  margin_includes jsonb,

  created_at timestamptz,
  updated_at timestamptz
)
language sql
stable
as $$
select
  w.id,
  w.date_from,
  w.date_to,
  w.vat_rate_pct,
  w.erni_pct,
  w.holiday_pay_pct,
  w.apply_holiday_to,
  w.apply_erni_to,
  w.margin_includes,
  w.created_at,
  w.updated_at
from public.settings_finance_windows w
order by w.date_from desc, w.created_at desc;
$$;
