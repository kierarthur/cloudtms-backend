-- ============================================================
-- 2.5: Batch context loader (one row per requested timesheet_id)
-- SAFE TO RE-RUN: CREATE OR REPLACE
--
-- ✅ Includes new TSFIN category columns automatically via:
--     out_cur_fin = to_jsonb(tf)
-- because tf is a row from timesheets_financials and to_jsonb(row) includes all columns.
-- ============================================================

create or replace function public._tsfin_invalid_segment_count(invoice_breakdown_json jsonb)
returns int
language sql
immutable
as $$
  select case
    when invoice_breakdown_json is null then 0

    -- invoice_breakdown_json should always be an object; if it's not, it's structurally invalid
    when jsonb_typeof(invoice_breakdown_json) <> 'object' then 1

    -- only validate segments in SEGMENTS mode
    when upper(coalesce(invoice_breakdown_json->>'mode','')) <> 'SEGMENTS' then 0

    -- SEGMENTS mode must have a segments array
    when jsonb_typeof(invoice_breakdown_json->'segments') <> 'array' then 1

    -- count invalid segment elements (JSON nulls or non-objects, or missing/blank segment_id)
    else (
      select count(*)::int
      from jsonb_array_elements(invoice_breakdown_json->'segments') as seg(value)
      where jsonb_typeof(seg.value) <> 'object'
         or nullif(btrim(coalesce(seg.value->>'segment_id','')), '') is null
    )
  end;
$$;




create or replace function public.tsfin_load_context_batch(p_timesheet_ids uuid[])
returns table (
  effective_timesheet_id uuid,
  out_timesheet jsonb,
  out_cur_fin jsonb,
  out_candidate jsonb,
  out_umbrella jsonb,
  out_client_id uuid,
  out_effective_flags jsonb,
  out_policy jsonb
)
language sql
stable
as $$
with input_ids as (
  select distinct unnest(p_timesheet_ids) as input_timesheet_id
  where p_timesheet_ids is not null
),
t_in as (
  select
    i.input_timesheet_id,
    t0.*
  from input_ids i
  join public.timesheets t0
    on t0.timesheet_id = i.input_timesheet_id
),
t_eff_id as (
  select
    t_in.input_timesheet_id,
    coalesce(tc.timesheet_id, t_in.timesheet_id) as effective_timesheet_id
  from t_in
  left join public.timesheets tc
    on tc.booking_id = t_in.booking_id
   and tc.is_current = true
),
t_eff as (
  select
    e.input_timesheet_id,
    e.effective_timesheet_id,
    te.*
  from t_eff_id e
  join public.timesheets te
    on te.timesheet_id = e.effective_timesheet_id
),
-- settings_defaults is NON-FINANCE ONLY now (explicit select list; no select *)
def as (
  select
    timezone_id,
    day_start, day_end,
    night_start, night_end,
    sat_start, sat_end,
    sun_start, sun_end,
    bh_start, bh_end,
    bh_list,
    hr_attach_to_invoice,
    ts_attach_to_invoice
  from public.settings_defaults
  where id = 1
  limit 1
),
base as (
  select
    te.effective_timesheet_id,

    -- TIME anchor date for client_settings.effective_from selection:
    -- DAILY: worked_start_iso local date
    -- WEEKLY: week_ending_date (fallback)
    coalesce(
      case
        when te.worked_start_iso is not null
          then (te.worked_start_iso at time zone 'Europe/London')::date
        else null
      end,
      te.week_ending_date::date
    ) as time_anchor_date,

    -- FINANCE anchor date for finance windows:
    -- authorised_at_server local date if present, else "today" local date
    coalesce(
      case
        when te.authorised_at_server is not null
          then (te.authorised_at_server at time zone 'Europe/London')::date
        else null
      end,
      (now() at time zone 'Europe/London')::date
    ) as finance_anchor_date,

    -- ✅ Ensure new correction fields are always present in out_timesheet (even if null)
    (to_jsonb(te)
      || jsonb_build_object(
        'correction_id', te.correction_id,
        'correction_kind', te.correction_kind,
        'adjustment_origin', te.adjustment_origin
      )
    ) as out_timesheet,

    -- ✅ This will now include travel_/accommodation_/other_ columns automatically
    to_jsonb(tf) as out_cur_fin,

    to_jsonb(c)  as out_candidate,
    to_jsonb(u)  as out_umbrella,

    cid.client_id as out_client_id,

    -- ✅ Expand “effective flags” for weekly consumers (still source-of-truth = v_timesheets_summary)
    jsonb_build_object(
      'route_type',                    v.route_type,

      'contract_id',                   v.contract_id,
      'contract_week_id',              v.contract_week_id,
      'contract_week_ending_date',     v.contract_week_ending_date,
      'basis',                         v.basis,

      'client_requires_hr',            v.client_requires_hr,
      'client_autoprocess_hr',         v.client_autoprocess_hr,
      'client_no_timesheet_required',  v.client_no_timesheet_required,
      'client_is_nhsp',                v.client_is_nhsp,

      'require_reference_to_pay',      v.require_reference_to_pay,
      'require_reference_to_invoice',  v.require_reference_to_invoice,

      'client_hr_validation_required', v.client_hr_validation_required,
      'client_ts_reference_required',  v.client_ts_reference_required,
      'client_pay_reference_required', v.client_pay_reference_required,
      'client_invoice_reference_required', v.client_invoice_reference_required,

      'pay_method',                    v.pay_method,
      'processing_status',             v.processing_status,
      'authorised_at_server',          v.authorised_at_server
    ) as out_effective_flags,

    cs as cs_row,
    def as def_row,
    ct as ct_row
  from t_eff te
  cross join def

  left join public.timesheets_financials tf
    on tf.timesheet_id = te.effective_timesheet_id
   and tf.is_current = true

  left join lateral (
    select c1.*
    from public.candidates c1
    where
      (
        c1.key_norm = te.occupant_key_norm
        or (
          te.occupant_key_norm is not null
          and c1.nhsp_hr_name_aliases @> to_jsonb(array[te.occupant_key_norm]::text[])
        )
      )
    order by
      case
        when c1.key_norm = te.occupant_key_norm then 0
        else 1
      end,
      c1.updated_at desc nulls last,
      c1.created_at desc nulls last
    limit 1
  ) c on true

  left join public.umbrellas u
    on (c.umbrella_id is not null and u.id = c.umbrella_id)

  left join public.v_timesheets_summary v
    on v.timesheet_id = te.effective_timesheet_id

  left join public.contracts ct
    on ct.id = te.contract_id

  left join lateral (
    select ch.client_id
    from public.client_hospitals ch
    where te.hospital_norm is not null
      and ch.hospital_name_norm @> jsonb_build_array(te.hospital_norm)
    limit 1
  ) ch on true

  -- ✅ Resolve a single client_id for this timesheet (works for WEEKLY and DAILY)
  left join lateral (
    select coalesce(v.client_id, tf.client_id, ch.client_id) as client_id
  ) cid on true

  -- client_settings chosen by TIME anchor (work date / week ending)
  left join lateral (
    select cs1.*
    from public.client_settings cs1
    where cid.client_id is not null
      and cs1.client_id = cid.client_id
    order by
      case
        when (coalesce(
          case when te.worked_start_iso is not null then (te.worked_start_iso at time zone 'Europe/London')::date end,
          te.week_ending_date::date
        )) is null then 0

        when cs1.effective_from is not null
         and cs1.effective_from <= coalesce(
           case when te.worked_start_iso is not null then (te.worked_start_iso at time zone 'Europe/London')::date end,
           te.week_ending_date::date
         ) then 0

        when cs1.effective_from is null then 1

        else 2
      end,
      cs1.effective_from desc nulls last,
      cs1.created_at desc
    limit 1
  ) cs on true
),
ctx as (
  select
    b.effective_timesheet_id,

    b.out_timesheet,
    b.out_cur_fin,
    b.out_candidate,
    b.out_umbrella,
    b.out_client_id,
    b.out_effective_flags,

    -- Finance window row in-scope for FINANCE anchor date (authorised date else today)
    jsonb_build_object(
      'timezone_id', coalesce((b.cs_row).timezone_id, (b.def_row).timezone_id, 'Europe/London'),

      'day_start',   coalesce(to_char((b.cs_row).day_start,   'HH24:MI:SS'), to_char((b.def_row).day_start,   'HH24:MI:SS'), '06:00:00'),
      'day_end',     coalesce(to_char((b.cs_row).day_end,     'HH24:MI:SS'), to_char((b.def_row).day_end,     'HH24:MI:SS'), '20:00:00'),
      'night_start', coalesce(to_char((b.cs_row).night_start, 'HH24:MI:SS'), to_char((b.def_row).night_start, 'HH24:MI:SS'), '20:00:00'),
      'night_end',   coalesce(to_char((b.cs_row).night_end,   'HH24:MI:SS'), to_char((b.def_row).night_end,   'HH24:MI:SS'), '06:00:00'),

      'sat_start',   coalesce(to_char((b.cs_row).sat_start, 'HH24:MI:SS'), to_char((b.def_row).sat_start, 'HH24:MI:SS'), '00:00:00'),
      'sat_end',     coalesce(to_char((b.cs_row).sat_end,   'HH24:MI:SS'), to_char((b.def_row).sat_end,   'HH24:MI:SS'), '00:00:00'),
      'sun_start',   coalesce(to_char((b.cs_row).sun_start, 'HH24:MI:SS'), to_char((b.def_row).sun_start, 'HH24:MI:SS'), '00:00:00'),
      'sun_end',     coalesce(to_char((b.cs_row).sun_end,   'HH24:MI:SS'), to_char((b.def_row).sun_end,   'HH24:MI:SS'), '00:00:00'),

      'bh_start',    coalesce(to_char((b.cs_row).bh_start, 'HH24:MI:SS'), to_char((b.def_row).bh_start, 'HH24:MI:SS'), '00:00:00'),
      'bh_end',      coalesce(to_char((b.cs_row).bh_end,   'HH24:MI:SS'), to_char((b.def_row).bh_end,   'HH24:MI:SS'), '00:00:00'),

      'vat_rate_pct',
      coalesce((b.cs_row).vat_rate_pct, fin.vat_rate_pct, 20::numeric),

      'holiday_pay_pct',
      coalesce((b.cs_row).holiday_pay_pct, fin.holiday_pay_pct, 12.07::numeric),

      'erni_pct',
      coalesce(fin.erni_pct, 13.8::numeric),

      'mileage_pay_defaults',
      fin.mileage_pay_defaults,

      'mileage_charge_defaults',
      fin.mileage_charge_defaults,

      'apply_holiday_to',
      coalesce((b.cs_row).apply_holiday_to, fin.apply_holiday_to, 'PAYE_ONLY'),

      'apply_erni_to',
      coalesce((b.cs_row).apply_erni_to, fin.apply_erni_to, 'PAYE_ONLY'),

      'margin_includes',
      jsonb_build_object(
        'expenses',
        coalesce(
          nullif((
            case
              when (b.cs_row).margin_includes is null then null
              when jsonb_typeof((b.cs_row).margin_includes) = 'object' then (b.cs_row).margin_includes
              when jsonb_typeof((b.cs_row).margin_includes) = 'string'
                   and ((b.cs_row).margin_includes #>> '{}') ~ '^\s*\{'
                then ((b.cs_row).margin_includes #>> '{}')::jsonb
              else null
            end
          ) ->> 'expenses', '')::boolean,

          nullif((
            case
              when fin.margin_includes is null then null
              when jsonb_typeof(fin.margin_includes) = 'object' then fin.margin_includes
              when jsonb_typeof(fin.margin_includes) = 'string'
                   and (fin.margin_includes #>> '{}') ~ '^\s*\{'
                then (fin.margin_includes #>> '{}')::jsonb
              else null
            end
          ) ->> 'expenses', '')::boolean,

          false
        ),

        'mileage',
        coalesce(
          nullif((
            case
              when (b.cs_row).margin_includes is null then null
              when jsonb_typeof((b.cs_row).margin_includes) = 'object' then (b.cs_row).margin_includes
              when jsonb_typeof((b.cs_row).margin_includes) = 'string'
                   and ((b.cs_row).margin_includes #>> '{}') ~ '^\s*\{'
                then ((b.cs_row).margin_includes #>> '{}')::jsonb
              else null
            end
          ) ->> 'mileage', '')::boolean,

          nullif((
            case
              when fin.margin_includes is null then null
              when jsonb_typeof(fin.margin_includes) = 'object' then fin.margin_includes
              when jsonb_typeof(fin.margin_includes) = 'string'
                   and (fin.margin_includes #>> '{}') ~ '^\s*\{'
                then (fin.margin_includes #>> '{}')::jsonb
              else null
            end
          ) ->> 'mileage', '')::boolean,

          false
        )
      ),

      'bh_list',
      case
        when (b.def_row).bh_list is null then '[]'::jsonb
        when jsonb_typeof((b.def_row).bh_list) = 'array' then (b.def_row).bh_list
        when jsonb_typeof((b.def_row).bh_list) = 'string'
             and ((b.def_row).bh_list #>> '{}') ~ '^\s*\['
          then ((b.def_row).bh_list #>> '{}')::jsonb
        else '[]'::jsonb
      end,

      'hr_attach_to_invoice', coalesce((b.ct_row).hr_attach_to_invoice, (b.cs_row).hr_attach_to_invoice, (b.def_row).hr_attach_to_invoice, true),
      'ts_attach_to_invoice', coalesce((b.ct_row).ts_attach_to_invoice, (b.cs_row).ts_attach_to_invoice, (b.def_row).ts_attach_to_invoice, true),

      'week_ending_weekday',     coalesce((b.cs_row).week_ending_weekday, 0),
      'default_submission_mode', coalesce((b.cs_row).default_submission_mode, 'ELECTRONIC')
    ) as out_policy
  from base b
  left join lateral public.settings_finance_pick(p_date => b.finance_anchor_date) fin on true
)
select
  effective_timesheet_id,
  out_timesheet,
  out_cur_fin,
  out_candidate,
  out_umbrella,
  out_client_id,
  out_effective_flags,
  out_policy
from ctx;
$$;

grant execute on function public.tsfin_load_context_batch(uuid[]) to service_role;
grant execute on function public.tsfin_load_context_batch(uuid[]) to authenticated;

select pg_notify('pgrst', 'reload schema');


-- ============================================================
-- 2.6: Batch writer + outbox complete/fail (restore-on-fail safe)
-- UPDATED: persists new TSFIN category expense columns and preserves them
-- SAFE TO RE-RUN: CREATE OR REPLACE
-- ============================================================

create or replace function public.tsfin_write_snapshots_and_complete(p_rows jsonb)
returns table (
  ok_count integer,
  fail_count integer,
  errors jsonb
)
language plpgsql
as $function$
declare
  v_ok int := 0;
  v_fail int := 0;
  v_errors jsonb := '[]'::jsonb;

  r record;
  snap jsonb;

  prev public.timesheets_financials%rowtype;
  v_prev_id uuid;

  v_outbox_id uuid;
  v_timesheet_id uuid;

  v_err text;
  did_prepare boolean;

  -- ✅ validate segments JSON before writing
  v_ib  jsonb;
  v_bad int;

  -- ✅ expense component rollup (sum components; fallback to expenses_* only if sum == 0)
  v_cat_pay numeric;
  v_cat_charge numeric;
  v_fallback_exp_pay numeric;
  v_fallback_exp_charge numeric;
  v_exp_pay numeric;
  v_exp_charge numeric;

  -- ✅ NEW: additional + mileage rollup + patched totals/breakdown
  v_add_units_json jsonb;
  v_add_pay numeric;
  v_add_charge numeric;
  v_mil_pay numeric;
  v_mil_charge numeric;

  v_nonseg_pay numeric;
  v_nonseg_charge numeric;
  v_nonseg_margin numeric;

  v_core_pay numeric;
  v_core_charge numeric;

  v_total_pay numeric;
  v_total_charge numeric;
  v_margin numeric;

  v_add_obj jsonb;
  v_tot_obj jsonb;

  v_mode text;
  v_seg jsonb;
  v_exclude boolean;

  v_policy jsonb;
  v_apply_to text;
  v_erni_pct_raw numeric;
  v_erni_mult numeric;
  v_pay_method_u text;
  v_erni_applies boolean;
  v_pay_cost numeric;

  v_processing_status public.ts_fin_processing_status_enum;
  v_candidate_assignment public.candidate_assignment_enum;
  v_basis public.timesheet_fin_basis_enum;
  v_timesheet_version int;
  v_is_stale boolean;
  v_stale_reason text;
begin
  if p_rows is null or jsonb_typeof(p_rows) <> 'array' then
    ok_count := 0;
    fail_count := 0;
    errors := '[]'::jsonb;
    return next;
    return;
  end if;

  for r in
    select
      nullif(elem->>'outbox_id','')::uuid     as outbox_id,
      nullif(elem->>'timesheet_id','')::uuid  as timesheet_id,
      elem->'snapshot'                        as snapshot
    from jsonb_array_elements(p_rows) as elem
  loop
    v_outbox_id := r.outbox_id;
    v_timesheet_id := r.timesheet_id;
    snap := r.snapshot;

    did_prepare := false;
    v_prev_id := null;

    begin
      if v_outbox_id is null or v_timesheet_id is null or snap is null then
        raise exception 'INVALID_BATCH_ROW';
      end if;

      -- SAFETY: do not write if current snapshot is paid
      if exists (
        select 1
        from public.timesheets_financials tf
        where tf.timesheet_id = v_timesheet_id
          and tf.is_current = true
          and tf.paid_at_utc is not null
      ) then
        perform public.tsfin_work_success(v_outbox_id);
        v_ok := v_ok + 1;
        continue;
      end if;

      -- Capture current snapshot (for restore-on-fail + preserving manual fields)
      select *
      into prev
      from public.timesheets_financials tf
      where tf.timesheet_id = v_timesheet_id
        and tf.is_current = true
      limit 1;

      v_prev_id := prev.id;

      -- Resolve enums/typed fields once (avoid divergence between flags and stored status)
      v_timesheet_version :=
        coalesce(nullif(snap->>'timesheet_version','')::int, 1);

      v_basis :=
        coalesce(
          nullif(snap->>'basis','')::public.timesheet_fin_basis_enum,
          'SELF_REPORTED'::public.timesheet_fin_basis_enum
        );

      v_candidate_assignment :=
        coalesce(
          nullif(snap->>'candidate_assignment','')::public.candidate_assignment_enum,
          'UNASSIGNED'::public.candidate_assignment_enum
        );

      v_processing_status :=
        coalesce(
          nullif(snap->>'processing_status','')::public.ts_fin_processing_status_enum,
          'UNASSIGNED'::public.ts_fin_processing_status_enum
        );

      -- ✅ validate invoice_breakdown_json (prevents persisting null/invalid segments)
      v_ib := coalesce(snap->'invoice_breakdown_json', '{}'::jsonb);
      if v_ib is null or jsonb_typeof(v_ib) <> 'object' then
        v_ib := '{}'::jsonb;
      end if;

      v_bad := public._tsfin_invalid_segment_count(v_ib);
      if v_bad > 0 then
        raise exception 'INVALID_SEGMENTS_JSON:%', v_bad;
      end if;

      -- Guard + rotate current -> history (invoice-lock protected)
      perform public.tsfin_prepare_write(v_timesheet_id);
      did_prepare := true;

      -- ✅ compute expense totals (sum components first; fallback to expenses_* only if sum == 0)
      v_cat_pay :=
          coalesce(nullif(snap->>'travel_pay_ex_vat','')::numeric, prev.travel_pay_ex_vat, 0)
        + coalesce(nullif(snap->>'accommodation_pay_ex_vat','')::numeric, prev.accommodation_pay_ex_vat, 0)
        + coalesce(nullif(snap->>'other_pay_ex_vat','')::numeric, prev.other_pay_ex_vat, 0);

      v_cat_charge :=
          coalesce(nullif(snap->>'travel_charge_ex_vat','')::numeric, prev.travel_charge_ex_vat, 0)
        + coalesce(nullif(snap->>'accommodation_charge_ex_vat','')::numeric, prev.accommodation_charge_ex_vat, 0)
        + coalesce(nullif(snap->>'other_charge_ex_vat','')::numeric, prev.other_charge_ex_vat, 0);

      v_fallback_exp_pay :=
        coalesce(nullif(snap->>'expenses_pay_ex_vat','')::numeric, prev.expenses_pay_ex_vat, 0);

      v_fallback_exp_charge :=
        coalesce(nullif(snap->>'expenses_charge_ex_vat','')::numeric, prev.expenses_charge_ex_vat, 0);

      v_exp_pay :=
        case when coalesce(v_cat_pay, 0) <> 0 then v_cat_pay else v_fallback_exp_pay end;

      v_exp_charge :=
        case when coalesce(v_cat_charge, 0) <> 0 then v_cat_charge else v_fallback_exp_charge end;

      v_exp_pay := round(coalesce(v_exp_pay,0), 2);
      v_exp_charge := round(coalesce(v_exp_charge,0), 2);

      -- ✅ NEW: additional + mileage rollup (snap → prev fallback)
      v_add_units_json := coalesce(snap->'additional_units_json', prev.additional_units_json, '{}'::jsonb);

      v_add_pay := coalesce(nullif(snap->>'additional_pay_ex_vat','')::numeric, prev.additional_pay_ex_vat, 0);
      v_add_charge := coalesce(nullif(snap->>'additional_charge_ex_vat','')::numeric, prev.additional_charge_ex_vat, 0);

      v_mil_pay := coalesce(nullif(snap->>'mileage_pay_ex_vat','')::numeric, prev.mileage_pay_ex_vat, 0);
      v_mil_charge := coalesce(nullif(snap->>'mileage_charge_ex_vat','')::numeric, prev.mileage_charge_ex_vat, 0);

      v_add_pay := round(coalesce(v_add_pay,0), 2);
      v_add_charge := round(coalesce(v_add_charge,0), 2);
      v_mil_pay := round(coalesce(v_mil_pay,0), 2);
      v_mil_charge := round(coalesce(v_mil_charge,0), 2);

      v_nonseg_pay := round(v_add_pay + v_exp_pay + v_mil_pay, 2);
      v_nonseg_charge := round(v_add_charge + v_exp_charge + v_mil_charge, 2);
      v_nonseg_margin := round(v_nonseg_charge - v_nonseg_pay, 2);

      -- ✅ NEW: recompute core totals from breakdown (SEGMENTS sums; else base_hours)
      v_core_pay := 0;
      v_core_charge := 0;

      v_mode := upper(coalesce(v_ib->>'mode',''));

      if v_mode = 'SEGMENTS' and jsonb_typeof(v_ib->'segments') = 'array' then
        for v_seg in
          select value from jsonb_array_elements(v_ib->'segments') value
        loop
          if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
            continue;
          end if;

          -- charge sum includes all segments (negative allowed)
          begin
            v_core_charge := v_core_charge
              + coalesce(nullif(btrim(coalesce(v_seg->>'charge_amount','')), '')::numeric, 0);
          exception when others then
            null;
          end;

          -- pay sum respects exclude_from_pay when present/true
          v_exclude := false;
          begin
            v_exclude := coalesce(nullif(btrim(coalesce(v_seg->>'exclude_from_pay','')), '')::boolean, false);
          exception when others then
            v_exclude := false;
          end;

          if not v_exclude then
            begin
              v_core_pay := v_core_pay
                + coalesce(nullif(btrim(coalesce(v_seg->>'pay_amount','')), '')::numeric, 0);
            exception when others then
              null;
            end;
          end if;
        end loop;
      else
        begin
          v_core_pay := coalesce(nullif(btrim(coalesce(v_ib#>>'{base_hours,pay_ex_vat}','')), '')::numeric, 0);
        exception when others then
          v_core_pay := 0;
        end;

        begin
          v_core_charge := coalesce(nullif(btrim(coalesce(v_ib#>>'{base_hours,charge_ex_vat}','')), '')::numeric, 0);
        exception when others then
          v_core_charge := 0;
        end;
      end if;

      v_core_pay := round(coalesce(v_core_pay,0), 2);
      v_core_charge := round(coalesce(v_core_charge,0), 2);

      v_total_pay := round(v_core_pay + v_nonseg_pay, 2);
      v_total_charge := round(v_core_charge + v_nonseg_charge, 2);

      -- ✅ NEW: ERNI-aware margin (align with existing snapshot builders; safest when policy includes erni_pct)
      v_policy := coalesce(snap->'policy_snapshot_json', prev.policy_snapshot_json, '{}'::jsonb);
      if v_policy is null or jsonb_typeof(v_policy) <> 'object' then
        v_policy := '{}'::jsonb;
      end if;

      v_apply_to := upper(coalesce(nullif(btrim(coalesce(v_policy->>'apply_erni_to','')), ''), 'PAYE_ONLY'));

      v_erni_pct_raw := 0;
      begin
        v_erni_pct_raw := coalesce(nullif(btrim(coalesce(v_policy->>'erni_pct','')), '')::numeric, 0);
      exception when others then
        v_erni_pct_raw := 0;
      end;

      v_erni_mult := 1;
      if coalesce(v_erni_pct_raw,0) > 0 then
        if v_erni_pct_raw > 1 then
          v_erni_mult := 1 + (v_erni_pct_raw / 100);
        else
          v_erni_mult := 1 + v_erni_pct_raw;
        end if;
      end if;

      v_pay_method_u :=
        upper(
          coalesce(
            nullif(btrim(coalesce(snap->>'pay_method','')), ''),
            nullif(btrim(coalesce(prev.pay_method::text,'')), ''),
            ''
          )
        );

      v_erni_applies :=
        (v_apply_to = 'ALL')
        or (v_apply_to = 'PAYE_ONLY' and v_pay_method_u = 'PAYE');

      v_pay_cost := v_total_pay;
      if v_erni_applies then
        v_pay_cost := v_total_pay * v_erni_mult;
      end if;

      v_margin := round(v_total_charge - v_pay_cost, 2);

      -- ✅ NEW: patch invoice_breakdown_json.additional (preserve units if present; else set from stored additional_units_json)
      v_add_obj := case
        when v_ib ? 'additional' and jsonb_typeof(v_ib->'additional') = 'object' then v_ib->'additional'
        else '{}'::jsonb
      end;

      if not (v_add_obj ? 'units') or jsonb_typeof(v_add_obj->'units') <> 'object' then
        if v_add_units_json is null or jsonb_typeof(v_add_units_json) <> 'object' then
          v_add_units_json := '{}'::jsonb;
        end if;
        v_add_obj := jsonb_set(v_add_obj, '{units}', v_add_units_json, true);
      end if;

      v_add_obj := jsonb_set(v_add_obj, '{pay_ex_vat}', to_jsonb(v_nonseg_pay), true);
      v_add_obj := jsonb_set(v_add_obj, '{charge_ex_vat}', to_jsonb(v_nonseg_charge), true);
      v_add_obj := jsonb_set(v_add_obj, '{margin_ex_vat}', to_jsonb(v_nonseg_margin), true);

      v_ib := jsonb_set(v_ib, '{additional}', v_add_obj, true);

      -- ✅ NEW: patch invoice_breakdown_json.totals to match computed totals
      v_tot_obj := case
        when v_ib ? 'totals' and jsonb_typeof(v_ib->'totals') = 'object' then v_ib->'totals'
        else '{}'::jsonb
      end;

      v_tot_obj := jsonb_set(v_tot_obj, '{total_pay_ex_vat}', to_jsonb(v_total_pay), true);
      v_tot_obj := jsonb_set(v_tot_obj, '{total_charge_ex_vat}', to_jsonb(v_total_charge), true);
      v_tot_obj := jsonb_set(v_tot_obj, '{margin_ex_vat}', to_jsonb(v_margin), true);

      v_ib := jsonb_set(v_ib, '{totals}', v_tot_obj, true);

      -- ✅ NEW: stale flag tie-off (clear stale on a successful READY_FOR_INVOICE snapshot)
      v_is_stale := coalesce(nullif(snap->>'is_stale','')::boolean, false);
      v_stale_reason := nullif(snap->>'stale_reason','');

      if v_processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum then
        v_is_stale := false;
        v_stale_reason := null;
      end if;

      -- Insert new current snapshot (explicit columns)
      insert into public.timesheets_financials (
        timesheet_id,
        timesheet_version,
        basis,
        is_current,
        is_stale,
        stale_reason,

        worked_start_iso,
        worked_end_iso,
        break_start_iso,
        break_end_iso,
        break_minutes,

        candidate_id,
        client_id,
        role,
        band,
        pay_method,

        policy_snapshot_json,
        rate_source_refs_json,

        hours_day,
        hours_night,
        hours_sat,
        hours_sun,
        hours_bh,

        pay_day, pay_night, pay_sat, pay_sun, pay_bh,
        charge_day, charge_night, charge_sat, charge_sun, charge_bh,

        total_hours,
        total_pay_ex_vat,
        total_charge_ex_vat,
        margin_ex_vat,

        computed_at_utc,

        occupant_key_norm,
        candidate_assignment,
        processing_status,

        -- Totals + description + legacy evidence pointers
        expenses_pay_ex_vat,
        expenses_charge_ex_vat,
        expenses_description,
        expenses_evidence_r2_key,
        expenses_evidence_manifest,

        -- category expense columns
        travel_pay_ex_vat,
        travel_charge_ex_vat,
        accommodation_pay_ex_vat,
        accommodation_charge_ex_vat,
        other_pay_ex_vat,
        other_charge_ex_vat,

        mileage_pay_ex_vat,
        mileage_charge_ex_vat,
        mileage_units,
        mileage_evidence_r2_key,
        mileage_evidence_manifest,
        mileage_pay_rate,
        mileage_charge_rate,

        po_number,

        pay_on_hold,
        pay_on_hold_reason,
        pay_on_hold_since_utc,

        pay_wtr_rate_pct_snapshot,
        pay_vat_rate_pct_snapshot,
        pay_vat_amount_snapshot,
        pay_total_inc_vat_snapshot,

        payment_reference,
        remittance_last_sent_at_utc,
        remittance_send_count,

        processed_by_user_id,
        processed_at_utc,
        authorised_by_user_id,
        authorised_at_utc,

        additional_units_json,
        additional_pay_ex_vat,
        additional_charge_ex_vat,
        additional_margin_ex_vat,

        invoice_breakdown_json,

        nhsp_import_id,

        has_rate_issue,
        has_pay_channel_issue,

        hr_crosscheck_status,
        hr_crosscheck_issues,

        external_source_rows_json,

        actual_schedule_json,
        actual_minutes_by_day_json
      )
      values (
        v_timesheet_id,
        v_timesheet_version,
        v_basis,
        true,

        v_is_stale,
        v_stale_reason,

        nullif(snap->>'worked_start_iso','')::timestamptz,
        nullif(snap->>'worked_end_iso','')::timestamptz,
        nullif(snap->>'break_start_iso','')::timestamptz,
        nullif(snap->>'break_end_iso','')::timestamptz,
        nullif(snap->>'break_minutes','')::int,

        nullif(snap->>'candidate_id','')::uuid,
        nullif(snap->>'client_id','')::uuid,
        nullif(snap->>'role',''),
        nullif(snap->>'band',''),
        nullif(snap->>'pay_method',''),

        coalesce(snap->'policy_snapshot_json', '{}'::jsonb),
        coalesce(snap->'rate_source_refs_json', '{}'::jsonb),

        coalesce(nullif(snap->>'hours_day','')::numeric, 0),
        coalesce(nullif(snap->>'hours_night','')::numeric, 0),
        coalesce(nullif(snap->>'hours_sat','')::numeric, 0),
        coalesce(nullif(snap->>'hours_sun','')::numeric, 0),
        coalesce(nullif(snap->>'hours_bh','')::numeric, 0),

        nullif(snap->>'pay_day','')::numeric,
        nullif(snap->>'pay_night','')::numeric,
        nullif(snap->>'pay_sat','')::numeric,
        nullif(snap->>'pay_sun','')::numeric,
        nullif(snap->>'pay_bh','')::numeric,

        nullif(snap->>'charge_day','')::numeric,
        nullif(snap->>'charge_night','')::numeric,
        nullif(snap->>'charge_sat','')::numeric,
        nullif(snap->>'charge_sun','')::numeric,
        nullif(snap->>'charge_bh','')::numeric,

        coalesce(nullif(snap->>'total_hours','')::numeric, 0),

        -- ✅ NEW: totals are computed from core + (additional + expenses + mileage)
        coalesce(v_total_pay, 0),
        coalesce(v_total_charge, 0),
        coalesce(v_margin, 0),

        now(),

        nullif(snap->>'occupant_key_norm',''),
        v_candidate_assignment,
        v_processing_status,

        -- expenses_pay_ex_vat: rollup (category sum or fallback)
        coalesce(v_exp_pay, 0),

        -- expenses_charge_ex_vat: rollup (category sum or fallback)
        coalesce(v_exp_charge, 0),

        nullif(snap->>'expenses_description',''),
        nullif(snap->>'expenses_evidence_r2_key',''),
        coalesce(snap->'expenses_evidence_manifest', prev.expenses_evidence_manifest),

        -- category columns (preserved from prev if absent in snapshot)
        coalesce(nullif(snap->>'travel_pay_ex_vat','')::numeric, prev.travel_pay_ex_vat, 0),
        coalesce(nullif(snap->>'travel_charge_ex_vat','')::numeric, prev.travel_charge_ex_vat, 0),
        coalesce(nullif(snap->>'accommodation_pay_ex_vat','')::numeric, prev.accommodation_pay_ex_vat, 0),
        coalesce(nullif(snap->>'accommodation_charge_ex_vat','')::numeric, prev.accommodation_charge_ex_vat, 0),
        coalesce(nullif(snap->>'other_pay_ex_vat','')::numeric, prev.other_pay_ex_vat, 0),
        coalesce(nullif(snap->>'other_charge_ex_vat','')::numeric, prev.other_charge_ex_vat, 0),

        -- ✅ NEW: preserve mileage pay/charge from prev if absent
        coalesce(nullif(snap->>'mileage_pay_ex_vat','')::numeric, prev.mileage_pay_ex_vat, 0),
        coalesce(nullif(snap->>'mileage_charge_ex_vat','')::numeric, prev.mileage_charge_ex_vat, 0),

        coalesce(nullif(snap->>'mileage_units','')::numeric, prev.mileage_units, 0),
        nullif(snap->>'mileage_evidence_r2_key',''),
        coalesce(snap->'mileage_evidence_manifest', prev.mileage_evidence_manifest),
        nullif(snap->>'mileage_pay_rate','')::numeric,
        nullif(snap->>'mileage_charge_rate','')::numeric,

        coalesce(nullif(snap->>'po_number',''), prev.po_number),

        coalesce(nullif(snap->>'pay_on_hold','')::boolean, prev.pay_on_hold, false),
        coalesce(nullif(snap->>'pay_on_hold_reason',''), prev.pay_on_hold_reason),
        coalesce(nullif(snap->>'pay_on_hold_since_utc','')::timestamptz, prev.pay_on_hold_since_utc),

        nullif(snap->>'pay_wtr_rate_pct_snapshot','')::numeric,
        nullif(snap->>'pay_vat_rate_pct_snapshot','')::numeric,
        coalesce(nullif(snap->>'pay_vat_amount_snapshot','')::numeric, 0),
        coalesce(nullif(snap->>'pay_total_inc_vat_snapshot','')::numeric, 0),

        coalesce(nullif(snap->>'payment_reference',''), prev.payment_reference),
        coalesce(nullif(snap->>'remittance_last_sent_at_utc','')::timestamptz, prev.remittance_last_sent_at_utc),
        coalesce(nullif(snap->>'remittance_send_count','')::int, prev.remittance_send_count, 0),

        coalesce(nullif(snap->>'processed_by_user_id','')::uuid, prev.processed_by_user_id),
        coalesce(nullif(snap->>'processed_at_utc','')::timestamptz, prev.processed_at_utc),
        coalesce(nullif(snap->>'authorised_by_user_id','')::uuid, prev.authorised_by_user_id),
        coalesce(nullif(snap->>'authorised_at_utc','')::timestamptz, prev.authorised_at_utc),

        -- ✅ NEW: preserve additional units from prev if absent
        coalesce(snap->'additional_units_json', prev.additional_units_json, '{}'::jsonb),

        -- ✅ NEW: preserve additional pay/charge/margin from prev if absent
        coalesce(nullif(snap->>'additional_pay_ex_vat','')::numeric, prev.additional_pay_ex_vat, 0),
        coalesce(nullif(snap->>'additional_charge_ex_vat','')::numeric, prev.additional_charge_ex_vat, 0),
        coalesce(nullif(snap->>'additional_margin_ex_vat','')::numeric, prev.additional_margin_ex_vat, 0),

        -- ✅ NEW: patched breakdown (additional + totals updated)
        v_ib,

        nullif(snap->>'nhsp_import_id','')::uuid,

        coalesce(nullif(snap->>'has_rate_issue','')::boolean, false),
        coalesce(nullif(snap->>'has_pay_channel_issue','')::boolean, false),

        coalesce(nullif(snap->>'hr_crosscheck_status',''), prev.hr_crosscheck_status),
        coalesce(
          case
            when snap ? 'hr_crosscheck_issues' and jsonb_typeof(snap->'hr_crosscheck_issues') = 'array'
              then array(select jsonb_array_elements_text(snap->'hr_crosscheck_issues'))
            else null
          end,
          prev.hr_crosscheck_issues
        ),

        coalesce(snap->'external_source_rows_json', prev.external_source_rows_json),

        coalesce(snap->'actual_schedule_json', prev.actual_schedule_json),
        coalesce(snap->'actual_minutes_by_day_json', prev.actual_minutes_by_day_json)
      );

      perform public.tsfin_work_success(v_outbox_id);
      v_ok := v_ok + 1;

    exception
      when others then
        v_err := sqlerrm;

        -- Restore previous snapshot if we already rotated current->history and insert failed.
        if did_prepare and v_prev_id is not null then
          update public.timesheets_financials
          set is_current = true,
              updated_at = now()
          where id = v_prev_id
            and locked_by_invoice_id is null
            and paid_at_utc is null;
        end if;

        -- If locked, don't retry forever: delete outbox row
        if v_err = 'TSFIN_LOCKED' then
          perform public.tsfin_work_success(v_outbox_id);
          v_ok := v_ok + 1;
        else
          perform public.tsfin_work_fail(v_outbox_id, v_err);
          v_fail := v_fail + 1;

          v_errors := v_errors || jsonb_build_array(
            jsonb_build_object(
              'outbox_id', v_outbox_id::text,
              'timesheet_id', v_timesheet_id::text,
              'error', v_err
            )
          );
        end if;
    end;
  end loop;

  ok_count := v_ok;
  fail_count := v_fail;
  errors := v_errors;
  return next;
end;
$function$;

grant execute on function public.tsfin_write_snapshots_and_complete(jsonb) to service_role;
grant execute on function public.tsfin_write_snapshots_and_complete(jsonb) to authenticated;

select pg_notify('pgrst', 'reload schema');


