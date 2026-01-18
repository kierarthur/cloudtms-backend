-- ============================================================
-- RPC A: settings_finance_pick(p_date date)
-- Returns the single effective finance window for a given date.
-- - If p_date is null: uses "today" in Europe/London.
-- - Picks date_from <= date and (date_to is null or date_to >= date).
-- - Prefers most recent date_from (desc).
-- - Fallback: earliest finance window (date_from asc) if none match.
--
-- ✅ SAFE TO RE-RUN:
-- Postgres cannot CREATE OR REPLACE when OUT rowtype changes.
-- So we DROP by signature first, then CREATE.
-- ============================================================

drop function if exists public.settings_finance_pick(date);

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


-- ============================================================
-- RPC B: settings_finance_list()
-- Returns all finance windows ordered newest-first (date_from desc).
-- Intended for front-end management UI.
--
-- ✅ SAFE TO RE-RUN:
-- Drop first, then create (avoid return-type mismatch errors).
-- ============================================================

drop function if exists public.settings_finance_list();

create function public.settings_finance_list()
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

  -- ✅ NEW
  w.mileage_pay_defaults,
  w.mileage_charge_defaults,

  w.apply_holiday_to,
  w.apply_erni_to,
  w.margin_includes,

  w.created_at,
  w.updated_at
from public.settings_finance_windows w
order by w.date_from desc, w.created_at desc;
$$;

-- 2026-01-13: Invoice edit support helpers
--   - public.invoice_eligible_timesheets_for_invoice(p_invoice_id uuid)
--   - public.invoice_recompute_totals(p_invoice_id uuid)
--
-- Notes:
--   * These helpers are designed to support the invoice modal “Add Timesheet to Invoice”
--     and any backend endpoints that mutate invoice_lines and need consistent totals/PDF invalidation.
--   * They are written to be rerunnable (CREATE OR REPLACE).

create or replace function public.invoice_eligible_timesheets_for_invoice(
  p_invoice_id uuid
)
returns jsonb
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  v_client_id uuid;
  v_invoice_week_start date;
  v_invoice_week_end date;
begin
  -- Load invoice context (client + invoice week)
  select
    i.client_id,
    nullif(btrim(coalesce(i.header_snapshot_json #>> '{meta,invoice_week_start}', '')), '')::date
  into
    v_client_id,
    v_invoice_week_start
  from public.invoices i
  where i.id = p_invoice_id
  limit 1;

  if not found then
    raise exception 'invoice_eligible_timesheets_for_invoice: invoice % not found', p_invoice_id;
  end if;

  if v_invoice_week_start is null then
    raise exception 'invoice_eligible_timesheets_for_invoice: invoice % missing header_snapshot_json.meta.invoice_week_start', p_invoice_id;
  end if;

  v_invoice_week_end := v_invoice_week_start + 6;

  return (
    with base as (
      select
        tf.timesheet_id,
        tf.client_id,
        tf.candidate_id,
        ts.week_ending_date::date as timesheet_week_ending_date,
        (ts.week_ending_date::date - 6) as timesheet_week_start,
        ts.hospital_norm,
        ts.submission_mode,
        tf.basis,
        tf.total_hours as tsfin_total_hours,
        tf.total_charge_ex_vat as tsfin_total_charge_ex_vat,
        tf.invoice_breakdown_json,
        s.client_name,
        s.candidate_name,
        s.validation_status,
        coalesce(s.hr_validation_required_for_invoice, false) as hr_validation_required_for_invoice
      from public.timesheets_financials tf
      join public.timesheets ts
        on ts.timesheet_id = tf.timesheet_id
       and ts.is_current = true
      join public.v_ts_invoice_precheck pc
        on pc.timesheet_id = tf.timesheet_id
      left join public.v_timesheets_summary_base s
        on s.timesheet_id = tf.timesheet_id
      where tf.is_current = true
        and tf.client_id = v_client_id
        and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
        and tf.locked_by_invoice_id is null
        and ts.revoked_at is null
        and upper(coalesce(pc.precheck_status, '')) = 'OK'
    ),
    seg_agg as (
      select
        b.timesheet_id,
        sum((coalesce(nullif(seg->>'charge_amount', ''), '0'))::numeric) as invoiceable_charge_ex_vat,
        sum(
            (coalesce(nullif(seg->>'hours_day', ''), '0'))::numeric
          + (coalesce(nullif(seg->>'hours_night', ''), '0'))::numeric
          + (coalesce(nullif(seg->>'hours_sat', ''), '0'))::numeric
          + (coalesce(nullif(seg->>'hours_sun', ''), '0'))::numeric
          + (coalesce(nullif(seg->>'hours_bh', ''), '0'))::numeric
        ) as invoiceable_hours,
        count(*)::int as invoiceable_segments_count
      from base b
      cross join lateral jsonb_array_elements(coalesce(b.invoice_breakdown_json->'segments', '[]'::jsonb)) seg
      where upper(coalesce(b.invoice_breakdown_json->>'mode', '')) = 'SEGMENTS'
        and nullif(btrim(coalesce(seg->>'invoice_locked_invoice_id', '')), '') is null
        and coalesce(
              nullif(btrim(coalesce(seg->>'invoice_target_week_start', '')), '')::date,
              b.timesheet_week_start
            ) = v_invoice_week_start
      group by b.timesheet_id
    ),
    eligible as (
      select
        b.timesheet_id,
        b.client_id,
        b.candidate_id,
        b.client_name,
        b.candidate_name,
        b.hospital_norm,
        b.submission_mode,
        b.basis,
        b.validation_status,
        b.hr_validation_required_for_invoice,
        (b.hr_validation_required_for_invoice and upper(coalesce(b.validation_status, '')) <> 'OK') as blocked_by_hr_validation,
        upper(coalesce(b.invoice_breakdown_json->>'mode', '')) as invoice_breakdown_mode,
        case
          when upper(coalesce(b.invoice_breakdown_json->>'mode', '')) = 'SEGMENTS'
            then coalesce(sa.invoiceable_hours, 0)
            else coalesce(b.tsfin_total_hours, 0)
        end::numeric as invoiceable_hours,
        case
          when upper(coalesce(b.invoice_breakdown_json->>'mode', '')) = 'SEGMENTS'
            then coalesce(sa.invoiceable_charge_ex_vat, 0)
            else coalesce(b.tsfin_total_charge_ex_vat, 0)
        end::numeric as invoiceable_charge_ex_vat,
        coalesce(sa.invoiceable_segments_count, 0) as invoiceable_segments_count,
        b.timesheet_week_ending_date
      from base b
      left join seg_agg sa on sa.timesheet_id = b.timesheet_id
      where
        (
          upper(coalesce(b.invoice_breakdown_json->>'mode', '')) = 'SEGMENTS'
          and (coalesce(sa.invoiceable_hours, 0) <> 0 or coalesce(sa.invoiceable_charge_ex_vat, 0) <> 0)
        )
        or
        (
          upper(coalesce(b.invoice_breakdown_json->>'mode', '')) <> 'SEGMENTS'
          and b.timesheet_week_start = v_invoice_week_start
        )
    )
    select jsonb_build_object(
      'invoice_id', p_invoice_id,
      'client_id', v_client_id,
      'invoice_week_start', to_char(v_invoice_week_start, 'YYYY-MM-DD'),
      'invoice_week_ending', to_char(v_invoice_week_end, 'YYYY-MM-DD'),
      'timesheets', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'timesheet_id', e.timesheet_id::text,
            'candidate_id', e.candidate_id::text,
            'client_name', e.client_name,
            'candidate_name', e.candidate_name,
            'hospital_norm', e.hospital_norm,
            'submission_mode', e.submission_mode,
            'basis', e.basis,

            -- Week this invoice will bill for
            'invoice_week_start', to_char(v_invoice_week_start, 'YYYY-MM-DD'),
            'invoice_week_ending', to_char(v_invoice_week_end, 'YYYY-MM-DD'),

            -- Original timesheet's week ending (may differ for delayed SEGMENTS)
            'source_week_ending_date', case when e.timesheet_week_ending_date is null then null else to_char(e.timesheet_week_ending_date, 'YYYY-MM-DD') end,

            -- Segment-aware invoiceable totals
            'invoiceable_hours', round(e.invoiceable_hours, 2),
            'invoiceable_charge_ex_vat', round(e.invoiceable_charge_ex_vat, 2),
            'invoice_breakdown_mode', e.invoice_breakdown_mode,
            'invoiceable_segments_count', e.invoiceable_segments_count,

            -- HR validation signals for UI gating
            'hr_validation_required_for_invoice', e.hr_validation_required_for_invoice,
            'blocked_by_hr_validation', e.blocked_by_hr_validation,
            'validation_status', e.validation_status
          )
          order by e.candidate_name nulls last, e.timesheet_week_ending_date asc nulls last, e.timesheet_id::text
        )
        from (
          select * from eligible
        ) e
      ), '[]'::jsonb)
    )
  );
end;
$$;




create or replace function public.invoice_recompute_totals(
  p_invoice_id uuid
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = public
as $$
declare
  v_subtotal_ex_vat numeric;
  v_vat_amount numeric;
  v_total_inc_vat numeric;
begin
  -- Lock the invoice row to avoid concurrent totals races.
  perform 1
  from public.invoices i
  where i.id = p_invoice_id
  for update;

  if not found then
    raise exception 'invoice_recompute_totals: invoice % not found', p_invoice_id;
  end if;

  select
    coalesce(sum(l.total_charge_ex_vat), 0)::numeric,
    coalesce(sum(l.vat_amount), 0)::numeric,
    coalesce(sum(l.total_inc_vat), 0)::numeric
  into
    v_subtotal_ex_vat,
    v_vat_amount,
    v_total_inc_vat
  from public.invoice_lines l
  where l.invoice_id = p_invoice_id;

  v_subtotal_ex_vat := round(v_subtotal_ex_vat, 2);
  v_vat_amount := round(v_vat_amount, 2);
  v_total_inc_vat := round(v_total_inc_vat, 2);

  update public.invoices
  set
    subtotal_ex_vat = v_subtotal_ex_vat,
    vat_amount = v_vat_amount,
    total_inc_vat = v_total_inc_vat,
    invoice_pdf_r2_key = null,
    updated_at = now()
  where id = p_invoice_id;

  return jsonb_build_object(
    'ok', true,
    'invoice_id', p_invoice_id,
    'subtotal_ex_vat', v_subtotal_ex_vat,
    'vat_amount', v_vat_amount,
    'total_inc_vat', v_total_inc_vat,
    'invoice_pdf_r2_key_cleared', true
  );
end;
$$;

