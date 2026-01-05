-- 05012026_tsfin_resolve_rates_batch.sql
-- Option A: Batch rate resolution (override + unified client default windows)
--
-- INPUT: p_items jsonb = array of objects:
--   {
--     "k": "<string key, e.g. timesheet_id>",
--     "candidate_id": "<uuid or null>",
--     "client_id": "<uuid or null>",
--     "role": "RMN",
--     "band": "Band 6" | null,
--     "date": "YYYY-MM-DD",
--     "rate_type": "PAYE" | "UMBRELLA" | null
--   }
--
-- OUTPUT: one row per input item:
-- - source_kind: 'CANDIDATE_OVERRIDE' | 'CLIENT_DEFAULT' | 'NONE'
-- - override_id/default_id
-- - pay_* and charge_* buckets

create or replace function public.tsfin_resolve_rates_batch(p_items jsonb)
returns table (
  k text,
  candidate_id uuid,
  client_id uuid,
  role text,
  band text,
  date_ymd date,
  rate_type text,

  source_kind text,
  override_id uuid,
  default_id uuid,

  pay_day numeric,
  pay_night numeric,
  pay_sat numeric,
  pay_sun numeric,
  pay_bh numeric,

  charge_day numeric,
  charge_night numeric,
  charge_sat numeric,
  charge_sun numeric,
  charge_bh numeric
)
language sql
stable
as $$
with items as (
  select
    coalesce(nullif(elem->>'k',''), nullif(elem->>'timesheet_id','')) as k,

    nullif(elem->>'candidate_id','')::uuid as candidate_id,
    nullif(elem->>'client_id','')::uuid as client_id,

    nullif(elem->>'role','') as role,
    nullif(elem->>'band','') as band,

    nullif(elem->>'date','')::date as date_ymd,

    upper(coalesce(nullif(elem->>'rate_type',''), 'UMBRELLA')) as rate_type_raw
  from jsonb_array_elements(coalesce(p_items, '[]'::jsonb)) as elem
),
norm as (
  select
    i.*,
    case
      when i.rate_type_raw in ('PAYE','UMBRELLA') then i.rate_type_raw
      else 'UMBRELLA'
    end as rate_type
  from items i
),
resolved as (
  select
    n.k,
    n.candidate_id,
    n.client_id,
    n.role,
    n.band,
    n.date_ymd,
    n.rate_type,

    ov.id as override_id,

    df.id as default_id,

    -- pay buckets: override first, else client defaults by rate_type
    case
      when ov.id is not null then ov.pay_day
      when df.id is not null and n.rate_type = 'PAYE' then df.paye_day
      when df.id is not null and n.rate_type = 'UMBRELLA' then df.umb_day
      else null
    end as pay_day,

    case
      when ov.id is not null then ov.pay_night
      when df.id is not null and n.rate_type = 'PAYE' then df.paye_night
      when df.id is not null and n.rate_type = 'UMBRELLA' then df.umb_night
      else null
    end as pay_night,

    case
      when ov.id is not null then ov.pay_sat
      when df.id is not null and n.rate_type = 'PAYE' then df.paye_sat
      when df.id is not null and n.rate_type = 'UMBRELLA' then df.umb_sat
      else null
    end as pay_sat,

    case
      when ov.id is not null then ov.pay_sun
      when df.id is not null and n.rate_type = 'PAYE' then df.paye_sun
      when df.id is not null and n.rate_type = 'UMBRELLA' then df.umb_sun
      else null
    end as pay_sun,

    case
      when ov.id is not null then ov.pay_bh
      when df.id is not null and n.rate_type = 'PAYE' then df.paye_bh
      when df.id is not null and n.rate_type = 'UMBRELLA' then df.umb_bh
      else null
    end as pay_bh,

    -- charge buckets: always from client defaults (may be null if no default)
    case when df.id is not null then df.charge_day else null end as charge_day,
    case when df.id is not null then df.charge_night else null end as charge_night,
    case when df.id is not null then df.charge_sat else null end as charge_sat,
    case when df.id is not null then df.charge_sun else null end as charge_sun,
    case when df.id is not null then df.charge_bh else null end as charge_bh

  from norm n

  -- Candidate override (pay-only): exact client + exact role + exact rate_type.
  -- Band rule:
  --  - if band provided: allow exact band OR band is null, prefer exact band
  --  - if band null: ONLY band is null (no guessing)
  left join lateral (
    select o.*
    from public.rates_candidate_overrides o
    where n.candidate_id is not null
      and n.client_id is not null
      and n.role is not null
      and n.date_ymd is not null
      and o.candidate_id = n.candidate_id
      and o.client_id = n.client_id
      and o.role = n.role
      and o.rate_type = n.rate_type
      and o.date_from <= n.date_ymd
      and (o.date_to is null or o.date_to >= n.date_ymd)
      and (
        (n.band is null and o.band is null)
        or
        (n.band is not null and (o.band = n.band or o.band is null))
      )
    order by
      case
        when n.band is not null and o.band = n.band then 0
        when o.band is null then 1
        else 9
      end,
      o.date_from desc,
      o.updated_at desc
    limit 1
  ) ov on true

  -- Client defaults (charge always, pay fallback if no override).
  -- Must be enabled: disabled_at_utc is null.
  -- Band rule:
  --  - if band provided: prefer exact band then band null
  --  - if band null: only band null
  left join lateral (
    select d.*
    from public.rates_client_defaults d
    where n.client_id is not null
      and n.role is not null
      and n.date_ymd is not null
      and d.client_id = n.client_id
      and d.role = n.role
      and d.disabled_at_utc is null
      and d.date_from <= n.date_ymd
      and (d.date_to is null or d.date_to >= n.date_ymd)
      and (
        (n.band is null and d.band is null)
        or
        (n.band is not null and (d.band = n.band or d.band is null))
      )
    order by
      case
        when n.band is not null and d.band = n.band then 0
        when d.band is null then 1
        else 9
      end,
      d.date_from desc,
      d.updated_at desc
    limit 1
  ) df on true
)
select
  r.k,
  r.candidate_id,
  r.client_id,
  r.role,
  r.band,
  r.date_ymd,
  r.rate_type,

  case
    when r.override_id is not null then 'CANDIDATE_OVERRIDE'
    when r.default_id is not null then 'CLIENT_DEFAULT'
    else 'NONE'
  end as source_kind,

  r.override_id,
  r.default_id,

  r.pay_day, r.pay_night, r.pay_sat, r.pay_sun, r.pay_bh,
  r.charge_day, r.charge_night, r.charge_sat, r.charge_sun, r.charge_bh
from resolved r;
$$;
