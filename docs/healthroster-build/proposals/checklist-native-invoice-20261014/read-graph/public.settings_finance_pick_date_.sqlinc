create function public.settings_finance_pick(p_date date default null)
returns table (
  id uuid,
  date_from date,
  date_to date,

  vat_rate_pct numeric,
  erni_pct numeric,
  holiday_pay_pct numeric,

  -- ✅ NEW date-linked mileage defaults (fallbacks)
  mileage_pay_defaults numeric,
  mileage_charge_defaults numeric,

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

    -- ✅ NEW
    w.mileage_pay_defaults,
    w.mileage_charge_defaults,

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
    w.id,
    w.date_from,
    w.date_to,

    w.vat_rate_pct,
    w.erni_pct,
    w.holiday_pay_pct,

    -- ✅ NEW
    w.mileage_pay_defaults,
    w.mileage_charge_defaults,

    w.apply_holiday_to,
    w.apply_erni_to,
    w.margin_includes,

    'FINANCE_WINDOWS_EARLIEST_FALLBACK'::text as source
  from public.settings_finance_windows w
  order by w.date_from asc
  limit 1
)
select * from pick
union all
select * from fallback
where not exists (select 1 from pick);
$$;