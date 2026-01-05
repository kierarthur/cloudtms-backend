-- 05012026_tsfin_batch_rpcs_part2.sql
-- Implements:
-- 2.5 public.tsfin_load_context_batch(p_timesheet_ids uuid[])
-- 2.6 public.tsfin_write_snapshots_and_complete(p_rows jsonb)
--
-- Notes:
-- - Policy logic matches JS loadPolicy(): settings_defaults(id=1) + client_settings effective row.
-- - IMPORTANT: settings_defaults.bh_list and margin_includes may be stored as JSON-STRINGS inside jsonb
--   (e.g. '"[\"2024-01-01\", ...]"' and '"{\"expenses\": false}"'), so we normalise them here.
-- - Client ID resolved from hospital_norm via client_hospitals.hospital_name_norm JSONB alias array.
-- - Candidate resolved by occupant_key_norm via candidates.key_norm.
-- - Writes use tsfin_prepare_write (guards locked_by_invoice_id), and also guard paid snapshots.
-- - Restore-on-fail ensures we never leave a timesheet with no current TSFIN snapshot if insert fails.

-- ============================================================
-- 2.5: Batch context loader (one row per requested timesheet_id)
-- ============================================================
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
def as (
  select *
  from public.settings_defaults
  where id = 1
  limit 1
),
ctx as (
  select
    te.effective_timesheet_id,
    to_jsonb(te) as out_timesheet,

    to_jsonb(tf) as out_cur_fin,
    to_jsonb(c)  as out_candidate,
    to_jsonb(u)  as out_umbrella,

    ch.client_id as out_client_id,

    jsonb_build_object(
      'route_type',                    v.route_type,
      'client_requires_hr',            v.client_requires_hr,
      'client_autoprocess_hr',         v.client_autoprocess_hr,
      'client_no_timesheet_required',  v.client_no_timesheet_required
    ) as out_effective_flags,

    jsonb_build_object(
      'timezone_id', coalesce(cs.timezone_id, def.timezone_id, 'Europe/London'),

      'day_start',   coalesce(to_char(cs.day_start,   'HH24:MI:SS'), to_char(def.day_start,   'HH24:MI:SS'), '06:00:00'),
      'day_end',     coalesce(to_char(cs.day_end,     'HH24:MI:SS'), to_char(def.day_end,     'HH24:MI:SS'), '20:00:00'),
      'night_start', coalesce(to_char(cs.night_start, 'HH24:MI:SS'), to_char(def.night_start, 'HH24:MI:SS'), '20:00:00'),
      'night_end',   coalesce(to_char(cs.night_end,   'HH24:MI:SS'), to_char(def.night_end,   'HH24:MI:SS'), '06:00:00'),

      'sat_start',   coalesce(to_char(cs.sat_start, 'HH24:MI:SS'), to_char(def.sat_start, 'HH24:MI:SS'), '00:00:00'),
      'sat_end',     coalesce(to_char(cs.sat_end,   'HH24:MI:SS'), to_char(def.sat_end,   'HH24:MI:SS'), '00:00:00'),
      'sun_start',   coalesce(to_char(cs.sun_start, 'HH24:MI:SS'), to_char(def.sun_start, 'HH24:MI:SS'), '00:00:00'),
      'sun_end',     coalesce(to_char(cs.sun_end,   'HH24:MI:SS'), to_char(def.sun_end,   'HH24:MI:SS'), '00:00:00'),

      'bh_start',    coalesce(to_char(cs.bh_start, 'HH24:MI:SS'), to_char(def.bh_start, 'HH24:MI:SS'), '00:00:00'),
      'bh_end',      coalesce(to_char(cs.bh_end,   'HH24:MI:SS'), to_char(def.bh_end,   'HH24:MI:SS'), '00:00:00'),

      'vat_rate_pct',    coalesce(cs.vat_rate_pct, def.vat_rate_pct, 20::numeric),
      'holiday_pay_pct', coalesce(cs.holiday_pay_pct, def.holiday_pay_pct, 12.07::numeric),

      'erni_pct',        coalesce(def.erni_pct, 13.8::numeric),

      'apply_holiday_to',  coalesce(cs.apply_holiday_to, def.apply_holiday_to, 'PAYE_ONLY'),
      'apply_erni_to',     coalesce(cs.apply_erni_to,    def.apply_erni_to,    'PAYE_ONLY'),

      'margin_includes', jsonb_build_object(
        'expenses',
        coalesce(
          nullif((
            case
              when cs.margin_includes is null then null
              when jsonb_typeof(cs.margin_includes) = 'object' then cs.margin_includes
              when jsonb_typeof(cs.margin_includes) = 'string'
                   and (cs.margin_includes #>> '{}') ~ '^\s*\{'
                then (cs.margin_includes #>> '{}')::jsonb
              else null
            end
          ) ->> 'expenses', '')::boolean,

          nullif((
            case
              when def.margin_includes is null then null
              when jsonb_typeof(def.margin_includes) = 'object' then def.margin_includes
              when jsonb_typeof(def.margin_includes) = 'string'
                   and (def.margin_includes #>> '{}') ~ '^\s*\{'
                then (def.margin_includes #>> '{}')::jsonb
              else null
            end
          ) ->> 'expenses', '')::boolean,

          false
        )
      ),

      'bh_list',
      case
        when def.bh_list is null then '[]'::jsonb
        when jsonb_typeof(def.bh_list) = 'array' then def.bh_list
        when jsonb_typeof(def.bh_list) = 'string'
             and (def.bh_list #>> '{}') ~ '^\s*\['
          then (def.bh_list #>> '{}')::jsonb
        else '[]'::jsonb
      end,

      'hr_attach_to_invoice', coalesce(cs.hr_attach_to_invoice, true),
      'ts_attach_to_invoice', coalesce(cs.ts_attach_to_invoice, true)
    ) as out_policy

  from t_eff te
  cross join def

  left join public.timesheets_financials tf
    on tf.timesheet_id = te.effective_timesheet_id
   and tf.is_current = true

  left join public.candidates c
    on c.key_norm = te.occupant_key_norm

  left join public.umbrellas u
    on (c.umbrella_id is not null and u.id = c.umbrella_id)

  left join public.v_timesheets_summary v
    on v.timesheet_id = te.effective_timesheet_id

  left join lateral (
    select ch.client_id
    from public.client_hospitals ch
    where te.hospital_norm is not null
      and ch.hospital_name_norm @> jsonb_build_array(te.hospital_norm)
    limit 1
  ) ch on true

  left join lateral (
    select cs1.*
    from public.client_settings cs1
    where ch.client_id is not null
      and cs1.client_id = ch.client_id
    order by
      case
        when te.worked_start_iso is null then 0
        when cs1.effective_from is not null
             and cs1.effective_from <= ((te.worked_start_iso at time zone 'Europe/London')::date)
          then 0
        when cs1.effective_from is null then 1
        else 2
      end,
      cs1.effective_from desc nulls last,  -- ✅ FIXED HERE
      cs1.created_at desc
    limit 1
  ) cs on true
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


-- ============================================================
-- 2.6: Batch writer + outbox complete/fail (restore-on-fail safe)
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

      -- Guard + rotate current -> history (invoice-lock protected)
      perform public.tsfin_prepare_write(v_timesheet_id);
      did_prepare := true;

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

        expenses_pay_ex_vat,
        expenses_charge_ex_vat,
        expenses_description,
        expenses_evidence_r2_key,
        expenses_evidence_manifest,

        mileage_pay_ex_vat,
        mileage_charge_ex_vat,
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

        coalesce(nullif(snap->>'timesheet_version','')::int, 1),

        coalesce(nullif(snap->>'basis','')::public.timesheet_fin_basis_enum,
                 'SELF_REPORTED'::public.timesheet_fin_basis_enum),

        coalesce(nullif(snap->>'is_current','')::boolean, true),
        coalesce(nullif(snap->>'is_stale','')::boolean, false),
        nullif(snap->>'stale_reason',''),

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
        coalesce(nullif(snap->>'total_pay_ex_vat','')::numeric, 0),
        coalesce(nullif(snap->>'total_charge_ex_vat','')::numeric, 0),
        coalesce(nullif(snap->>'margin_ex_vat','')::numeric, 0),

        now(),

        nullif(snap->>'occupant_key_norm',''),
        coalesce(nullif(snap->>'candidate_assignment','')::public.candidate_assignment_enum,
                 'UNASSIGNED'::public.candidate_assignment_enum),
        coalesce(nullif(snap->>'processing_status','')::public.ts_fin_processing_status_enum,
                 'UNASSIGNED'::public.ts_fin_processing_status_enum),

        coalesce(nullif(snap->>'expenses_pay_ex_vat','')::numeric, 0),
        coalesce(nullif(snap->>'expenses_charge_ex_vat','')::numeric, 0),
        nullif(snap->>'expenses_description',''),
        nullif(snap->>'expenses_evidence_r2_key',''),
        coalesce(snap->'expenses_evidence_manifest', prev.expenses_evidence_manifest),

        coalesce(nullif(snap->>'mileage_pay_ex_vat','')::numeric, 0),
        coalesce(nullif(snap->>'mileage_charge_ex_vat','')::numeric, 0),
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

        coalesce(snap->'additional_units_json', '{}'::jsonb),
        coalesce(nullif(snap->>'additional_pay_ex_vat','')::numeric, 0),
        coalesce(nullif(snap->>'additional_charge_ex_vat','')::numeric, 0),
        coalesce(nullif(snap->>'additional_margin_ex_vat','')::numeric, 0),

        coalesce(snap->'invoice_breakdown_json', '{}'::jsonb),

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
