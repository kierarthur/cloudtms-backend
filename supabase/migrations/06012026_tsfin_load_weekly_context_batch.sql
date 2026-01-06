-- ============================================================
-- TSFIN: WEEKLY context batch loader (deduped effective IDs)
-- Backend expects:
--   sbRpc('tsfin_load_weekly_context_batch', { p_timesheet_ids })
-- Returns:
--   { timesheet_id, out_cw, out_contract }
--
-- Hardened for live data:
--   A) If booking has NO is_current row → pick highest version
--   B) If cw.timesheet_id points at an older version → fallback matches any
--      timesheet_id in the same booking_id series (not only the effective id)
-- ============================================================
create or replace function public.tsfin_load_weekly_context_batch(p_timesheet_ids uuid[])
returns table (
  timesheet_id uuid,
  out_cw jsonb,
  out_contract jsonb
)
language sql
stable
as $$
with input_ids as (
  select distinct unnest(p_timesheet_ids) as input_timesheet_id
  where p_timesheet_ids is not null
),

t_in as (
  select i.input_timesheet_id, t0.*
  from input_ids i
  join public.timesheets t0
    on t0.timesheet_id = i.input_timesheet_id
),

t_eff_id as (
  select
    t_in.input_timesheet_id,

    -- Effective/current mapping:
    -- 1) Prefer explicit current row for booking_id
    -- 2) If none exists, fall back to highest version for that booking_id
    -- 3) Else fall back to the input id itself
    coalesce(tc.timesheet_id, tmax.timesheet_id, t_in.timesheet_id) as effective_timesheet_id

  from t_in

  left join public.timesheets tc
    on tc.booking_id = t_in.booking_id
   and tc.is_current = true

  left join lateral (
    select t1.timesheet_id
    from public.timesheets t1
    where t_in.booking_id is not null
      and t1.booking_id = t_in.booking_id
    order by
      t1.version desc nulls last,
      t1.updated_at desc nulls last,
      t1.created_at desc nulls last
    limit 1
  ) tmax on true
),

-- one row per effective/current timesheet id
eff_ids as (
  select distinct effective_timesheet_id
  from t_eff_id
  where effective_timesheet_id is not null
),

t_eff as (
  select te.*
  from eff_ids e
  join public.timesheets te
    on te.timesheet_id = e.effective_timesheet_id
),

v as (
  select
    te.timesheet_id,
    te.booking_id,
    te.contract_id as ts_contract_id,
    vts.contract_week_id,
    vts.contract_id
  from t_eff te
  left join public.v_timesheets_summary vts
    on vts.timesheet_id = te.timesheet_id
),

cw_pick as (
  select
    v.timesheet_id,
    cw.id as cw_id,
    cw.contract_id as cw_contract_id,
    to_jsonb(cw) as cw_json
  from v
  left join lateral (
    select cw1.*
    from public.contract_weeks cw1
    where
      -- Preferred: use the view’s resolved contract_week_id
      (v.contract_week_id is not null and cw1.id = v.contract_week_id)

      or
      (
        v.contract_week_id is null
        and
        (
          -- Hardened fallback: cw may point at an older timesheet version.
          -- Match cw.timesheet_id to ANY timesheet_id in this booking_id series.
          (v.booking_id is not null and exists (
            select 1
            from public.timesheets tx
            where tx.booking_id = v.booking_id
              and tx.timesheet_id = cw1.timesheet_id
          ))

          -- If booking_id is absent, last-resort: exact timesheet id
          or (v.booking_id is null and cw1.timesheet_id = v.timesheet_id)
        )
      )
    order by
      case when v.contract_week_id is not null and cw1.id = v.contract_week_id then 0 else 1 end,
      cw1.updated_at desc nulls last,
      cw1.created_at desc nulls last
    limit 1
  ) cw on true
),

contract_pick as (
  select
    cwp.timesheet_id,
    c.id as contract_id,
    to_jsonb(c) as contract_json
  from cw_pick cwp
  join v on v.timesheet_id = cwp.timesheet_id
  left join lateral (
    select c1.*
    from public.contracts c1
    where c1.id = coalesce(v.contract_id, cwp.cw_contract_id, v.ts_contract_id)
    limit 1
  ) c on true
)

select
  v.timesheet_id,
  case when cwp.cw_id is null then null else cwp.cw_json end as out_cw,
  case when cp.contract_id is null then null else cp.contract_json end as out_contract
from v
left join cw_pick cwp on cwp.timesheet_id = v.timesheet_id
left join contract_pick cp on cp.timesheet_id = v.timesheet_id;
$$;
