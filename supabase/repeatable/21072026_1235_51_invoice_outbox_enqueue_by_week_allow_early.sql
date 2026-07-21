-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: 1c1d07683aff.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
CREATE OR REPLACE FUNCTION public.invoice_outbox_enqueue_by_week(p_client_id uuid, p_invoice_week_start date, p_actor_user_id uuid, p_allow_early boolean DEFAULT false, p_meta jsonb DEFAULT NULL::jsonb)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_payload jsonb;
  v_existing uuid;
  v_new uuid;
  v_correction_scope_ids uuid[] := array[]::uuid[];

  v_london_today date := (now() at time zone 'Europe/London')::date;
  v_week_end date := (p_invoice_week_start + interval '6 days')::date;

  v_has_due boolean := false;

  -- ======================================================
  -- DEBUG (optional): single audit row per RPC call
  -- ======================================================
  v_invoice_debug boolean := false;
  v_dbg_run_started timestamptz := now();
  v_dbg_nonseg_due_count int := null;
  v_dbg_seg_due_count int := null;
  v_dbg_nonseg_due_sample jsonb := '[]'::jsonb;
  v_dbg_seg_due_sample jsonb := '[]'::jsonb;
  v_dbg_existing_outbox_id uuid := null;
  v_dbg_new_outbox_id uuid := null;

  -- extra breakdown (why NOT due)
  v_dbg_nonseg_any_count int := null;
  v_dbg_nonseg_fail_sample jsonb := '[]'::jsonb;
  v_dbg_seg_any_count int := null;
  v_dbg_seg_fail_sample jsonb := '[]'::jsonb;
  v_dbg_seg_delayed_not_due_count int := null;

begin
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

  if p_client_id is null then
    raise exception 'client_id is required';
  end if;

  if p_invoice_week_start is null then
    raise exception 'invoice_week_start is required';
  end if;

  v_correction_scope_ids:=public._ctms_invoice_week_candidate_ids_v1(
    p_client_id,p_invoice_week_start,100
  );
  if cardinality(v_correction_scope_ids)>0 then
    v_correction_scope_ids:=public._ctms_expand_correction_member_ids_v1(v_correction_scope_ids,100);
    perform public._ctms_assert_correction_invoice_scope_v1(
      v_correction_scope_ids,null::uuid,p_actor_user_id,true,false,false,'INVOICE_OUTBOX_BY_WEEK'
    );
  end if;

  -- ------------------------------------------------------------
  -- ✅ Due/invoiceable existence check (prevents preview/enqueue mismatch)
  -- Implements the confirmed rules:
  --   - allow_early applies to SEGMENTS + NON-SEGMENTS week-ending gate
  --   - allow_early does NOT override delayed segments
  --   - delayed segments eligible only once delay date reached (target week start <= today)
  --   - SEGMENTS-empty (expense-only) is invoiceable when ANY expense/mileage/additional-charge evidence exists,
  --     even if total_charge_ex_vat is accidentally 0 (defensive fallback).
  --
  -- ✅ HR validation gating:
  --   If hr_validation_required_for_invoice is true, validation_status must be VALIDATION_OK or OVERRIDDEN.
  --   NULL validation_status is treated as blocked when required.
  -- ------------------------------------------------------------
  select exists (
    -- NON-SEGMENTS (or SEGMENTS-empty treated as NON-SEGMENTS): invoice week is natural week
    select 1
    from public.timesheets_financials tf
    join public.timesheets ts
      on ts.timesheet_id = tf.timesheet_id
     and ts.is_current = true
    join public.v_ts_invoice_precheck pc
      on pc.timesheet_id = tf.timesheet_id
    left join public.v_timesheets_summary_base vts
      on vts.timesheet_id = tf.timesheet_id
    where tf.is_current = true
      and tf.client_id = p_client_id
      and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
      and tf.locked_by_invoice_id is null
      and ts.revoked_at is null
      and upper(coalesce(pc.precheck_status,'')) = 'OK'

      -- ✅ HR validation gate
      and not (
        coalesce(vts.hr_validation_required_for_invoice, false)
        and vts.validation_status is distinct from 'VALIDATION_OK'::public.validation_status_enum
        and vts.validation_status is distinct from 'OVERRIDDEN'::public.validation_status_enum
      )

      and (
        coalesce(tf.invoice_breakdown_json->>'mode','') <> 'SEGMENTS'
        or (
          coalesce(tf.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
          and jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array'
          and jsonb_array_length(tf.invoice_breakdown_json->'segments') = 0
          and (
               coalesce(tf.total_charge_ex_vat,0)::numeric <> 0
            or coalesce(tf.expenses_charge_ex_vat,0)::numeric <> 0
            or coalesce(tf.travel_charge_ex_vat,0)::numeric <> 0
            or coalesce(tf.accommodation_charge_ex_vat,0)::numeric <> 0
            or coalesce(tf.other_charge_ex_vat,0)::numeric <> 0
            or coalesce(tf.mileage_charge_ex_vat,0)::numeric <> 0
            or (
              case
                when nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '') is null then 0::numeric
                when nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '') ~ '^-?[0-9]+(\.[0-9]+)?$'
                  then (nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '')::numeric)
                else 0::numeric
              end
            ) <> 0
          )
        )
      )
      and (ts.week_ending_date::date - 6) = p_invoice_week_start
      and (p_allow_early = true or ts.week_ending_date::date < v_london_today)

    union all

    -- SEGMENTS mode: segment-level eligibility for this invoice week
    select 1
    from public.timesheets_financials tf
    join public.timesheets ts
      on ts.timesheet_id = tf.timesheet_id
     and ts.is_current = true
    join public.v_ts_invoice_precheck pc
      on pc.timesheet_id = tf.timesheet_id
    left join public.v_timesheets_summary_base vts
      on vts.timesheet_id = tf.timesheet_id
    cross join lateral jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) seg
    where tf.is_current = true
      and tf.client_id = p_client_id
      and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
      and tf.locked_by_invoice_id is null
      and ts.revoked_at is null
      and upper(coalesce(pc.precheck_status,'')) = 'OK'

      -- ✅ HR validation gate
      and not (
        coalesce(vts.hr_validation_required_for_invoice, false)
        and vts.validation_status is distinct from 'VALIDATION_OK'::public.validation_status_enum
        and vts.validation_status is distinct from 'OVERRIDDEN'::public.validation_status_enum
      )

      and coalesce(tf.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
      and jsonb_typeof(seg) = 'object'
      and nullif(btrim(coalesce(seg->>'segment_id','')), '') is not null
      and nullif(btrim(coalesce(seg->>'invoice_locked_invoice_id','')), '') is null

      -- segment belongs to this invoice_week_start (target week, else natural week)
      and coalesce(
            nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date,
            (ts.week_ending_date::date - 6)
          ) = p_invoice_week_start

      and (
        -- DELAYED segment:
        -- invoice_target_week_start differs from natural week start
        -- eligibility depends ONLY on delay reaching (<= today), NOT allow_early
        (
          nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '') is not null
          and nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date <> (ts.week_ending_date::date - 6)
          and nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date <= v_london_today
        )
        or
        -- NON-DELAYED segment:
        -- (target is null OR equals natural week start)
        -- eligibility uses timesheet week-ending gate with allow_early
        (
          (
            nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '') is null
            or nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date = (ts.week_ending_date::date - 6)
          )
          and (p_allow_early = true or ts.week_ending_date::date < v_london_today)
        )
      )
    limit 1
  ) into v_has_due;

  -- DEBUG: capture due breakdown (no effect unless enabled)
  if v_invoice_debug then
    begin
      -- NON-SEGMENTS (or segments mode with empty segments array but invoiceable via totals/expenses/mileage/additional)
      select
        count(*)::int,
        coalesce(jsonb_agg(s) filter (where s.rn <= 25), '[]'::jsonb)
      into
        v_dbg_nonseg_due_count,
        v_dbg_nonseg_due_sample
      from (
        select
          tf.timesheet_id::text as timesheet_id,
          ts.week_ending_date::date as week_ending_date,
          tf.processing_status::text as processing_status,
          pc.precheck_status as precheck_status,
          coalesce(tf.invoice_breakdown_json->>'mode','') as invoice_mode,
          coalesce(tf.total_charge_ex_vat,0)::numeric as total_charge_ex_vat,
          coalesce(tf.expenses_charge_ex_vat,0)::numeric as expenses_charge_ex_vat,
          coalesce(tf.travel_charge_ex_vat,0)::numeric as travel_charge_ex_vat,
          coalesce(tf.accommodation_charge_ex_vat,0)::numeric as accommodation_charge_ex_vat,
          coalesce(tf.other_charge_ex_vat,0)::numeric as other_charge_ex_vat,
          coalesce(tf.mileage_charge_ex_vat,0)::numeric as mileage_charge_ex_vat,
          coalesce(vts.hr_validation_required_for_invoice, false) as hr_validation_required_for_invoice,
          vts.validation_status::text as validation_status,
          case
            when nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '') is null then 0::numeric
            when nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '') ~ '^-?[0-9]+(\.[0-9]+)?$'
              then (nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '')::numeric)
            else 0::numeric
          end as additional_charge_ex_vat,
          row_number() over (order by ts.updated_at desc nulls last) as rn
        from public.timesheets_financials tf
        join public.timesheets ts
          on ts.timesheet_id = tf.timesheet_id
         and ts.is_current = true
        join public.v_ts_invoice_precheck pc
          on pc.timesheet_id = tf.timesheet_id
        left join public.v_timesheets_summary_base vts
          on vts.timesheet_id = tf.timesheet_id
        where tf.is_current = true
          and tf.client_id = p_client_id
          and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
          and tf.locked_by_invoice_id is null
          and ts.revoked_at is null
          and upper(coalesce(pc.precheck_status,'')) = 'OK'

          -- ✅ HR validation gate (mirror v_has_due)
          and not (
            coalesce(vts.hr_validation_required_for_invoice, false)
            and vts.validation_status is distinct from 'VALIDATION_OK'::public.validation_status_enum
            and vts.validation_status is distinct from 'OVERRIDDEN'::public.validation_status_enum
          )

          and (ts.week_ending_date::date - 6) = p_invoice_week_start
          and (p_allow_early = true or ts.week_ending_date::date < v_london_today)
          and (
            coalesce(tf.invoice_breakdown_json->>'mode','') <> 'SEGMENTS'
            or (
              coalesce(tf.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
              and jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array'
              and jsonb_array_length(tf.invoice_breakdown_json->'segments') = 0
              and (
                   coalesce(tf.total_charge_ex_vat,0)::numeric <> 0
                or coalesce(tf.expenses_charge_ex_vat,0)::numeric <> 0
                or coalesce(tf.travel_charge_ex_vat,0)::numeric <> 0
                or coalesce(tf.accommodation_charge_ex_vat,0)::numeric <> 0
                or coalesce(tf.other_charge_ex_vat,0)::numeric <> 0
                or coalesce(tf.mileage_charge_ex_vat,0)::numeric <> 0
                or (
                  case
                    when nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '') is null then 0::numeric
                    when nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '') ~ '^-?[0-9]+(\.[0-9]+)?$'
                      then (nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '')::numeric)
                    else 0::numeric
                  end
                ) <> 0
              )
            )
          )
      ) s;

      -- SEGMENTS mode (segment-level eligibility)
      select
        count(*)::int,
        coalesce(jsonb_agg(s) filter (where s.rn <= 25), '[]'::jsonb)
      into
        v_dbg_seg_due_count,
        v_dbg_seg_due_sample
      from (
        select
          tf.timesheet_id::text as timesheet_id,
          ts.week_ending_date::date as week_ending_date,
          nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '') as invoice_target_week_start,
          nullif(btrim(coalesce(seg_el.value->>'invoice_locked_invoice_id','')), '') as invoice_locked_invoice_id,
          coalesce(seg_el.value->>'segment_type','') as segment_type,
          coalesce(seg_el.value->>'label','') as label,
          coalesce(seg_el.value->>'charge_ex_vat','') as charge_ex_vat,
          coalesce(vts.hr_validation_required_for_invoice, false) as hr_validation_required_for_invoice,
          vts.validation_status::text as validation_status,
          row_number() over (order by ts.updated_at desc nulls last) as rn
        from public.timesheets_financials tf
        join public.timesheets ts
          on ts.timesheet_id = tf.timesheet_id
         and ts.is_current = true
        join public.v_ts_invoice_precheck pc
          on pc.timesheet_id = tf.timesheet_id
        left join public.v_timesheets_summary_base vts
          on vts.timesheet_id = tf.timesheet_id
        cross join lateral jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) seg_el(value)
        where tf.is_current = true
          and tf.client_id = p_client_id
          and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
          and tf.locked_by_invoice_id is null
          and ts.revoked_at is null
          and upper(coalesce(pc.precheck_status,'')) = 'OK'

          -- ✅ HR validation gate (mirror v_has_due)
          and not (
            coalesce(vts.hr_validation_required_for_invoice, false)
            and vts.validation_status is distinct from 'VALIDATION_OK'::public.validation_status_enum
            and vts.validation_status is distinct from 'OVERRIDDEN'::public.validation_status_enum
          )

          and coalesce(tf.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
          and jsonb_typeof(seg_el.value) = 'object'
          and nullif(btrim(coalesce(seg_el.value->>'segment_id','')), '') is not null
          and nullif(btrim(coalesce(seg_el.value->>'invoice_locked_invoice_id','')), '') is null
          and coalesce(
                nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '')::date,
                (ts.week_ending_date::date - 6)
              ) = p_invoice_week_start
          and (
            (
              nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '') is not null
              and nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '')::date <> (ts.week_ending_date::date - 6)
              and nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '')::date <= v_london_today
            )
            or
            (
              (
                nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '') is null
                or nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '')::date = (ts.week_ending_date::date - 6)
              )
              and (p_allow_early = true or ts.week_ending_date::date < v_london_today)
            )
          )
      ) s;

      -- Breakdown: candidates for this client/week that are NOT due (helps explain why v_has_due=false)
      -- NON-SEGMENTS candidates (natural week start = invoice_week_start)
      select
        count(*)::int,
        coalesce(jsonb_agg(s) filter (where s.rn <= 25), '[]'::jsonb)
      into
        v_dbg_nonseg_any_count,
        v_dbg_nonseg_fail_sample
      from (
        select
          tf.timesheet_id::text as timesheet_id,
          ts.week_ending_date::date as week_ending_date,
          tf.processing_status::text as processing_status,
          (tf.locked_by_invoice_id is not null) as locked_by_invoice,
          (ts.revoked_at is not null) as revoked,
          pc.precheck_status as precheck_status,
          coalesce(tf.invoice_breakdown_json->>'mode','') as invoice_mode,
          jsonb_typeof(tf.invoice_breakdown_json->'segments') as segments_type,
          coalesce(tf.total_charge_ex_vat,0)::numeric as total_charge_ex_vat,
          coalesce(tf.expenses_charge_ex_vat,0)::numeric as expenses_charge_ex_vat,
          coalesce(tf.travel_charge_ex_vat,0)::numeric as travel_charge_ex_vat,
          coalesce(tf.accommodation_charge_ex_vat,0)::numeric as accommodation_charge_ex_vat,
          coalesce(tf.other_charge_ex_vat,0)::numeric as other_charge_ex_vat,
          coalesce(tf.mileage_charge_ex_vat,0)::numeric as mileage_charge_ex_vat,
          coalesce(vts.hr_validation_required_for_invoice, false) as hr_validation_required_for_invoice,
          vts.validation_status::text as validation_status,
          case
            when nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '') is null then 0::numeric
            when nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '') ~ '^-?[0-9]+(\.[0-9]+)?$'
              then (nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '')::numeric)
            else 0::numeric
          end as additional_charge_ex_vat,
          case
            when tf.processing_status <> 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum then 'NOT_READY_FOR_INVOICE'
            when tf.locked_by_invoice_id is not null then 'LOCKED_BY_INVOICE'
            when ts.revoked_at is not null then 'REVOKED'
            when upper(coalesce(pc.precheck_status,'')) <> 'OK' then 'PRECHECK_NOT_OK'
            when (
              coalesce(vts.hr_validation_required_for_invoice, false)
              and vts.validation_status is distinct from 'VALIDATION_OK'::public.validation_status_enum
              and vts.validation_status is distinct from 'OVERRIDDEN'::public.validation_status_enum
            ) then 'HR_VALIDATION_BLOCKED'
            when (p_allow_early is not true) and (ts.week_ending_date::date >= v_london_today) then 'WEEK_NOT_PASSED'
            when (
              (coalesce(tf.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
               and jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array'
               and jsonb_array_length(tf.invoice_breakdown_json->'segments') = 0)
              and (
                   coalesce(tf.total_charge_ex_vat,0)::numeric = 0
               and coalesce(tf.expenses_charge_ex_vat,0)::numeric = 0
               and coalesce(tf.travel_charge_ex_vat,0)::numeric = 0
               and coalesce(tf.accommodation_charge_ex_vat,0)::numeric = 0
               and coalesce(tf.other_charge_ex_vat,0)::numeric = 0
               and coalesce(tf.mileage_charge_ex_vat,0)::numeric = 0
               and (
                 case
                   when nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '') is null then 0::numeric
                   when nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '') ~ '^-?[0-9]+(\.[0-9]+)?$'
                     then (nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '')::numeric)
                   else 0::numeric
                 end
               ) = 0
              )
            ) then 'SEGMENTS_EMPTY_AND_ZERO_TOTAL'
            else 'OTHER'
          end as fail_reason,
          row_number() over (order by ts.updated_at desc nulls last) as rn
        from public.timesheets_financials tf
        join public.timesheets ts
          on ts.timesheet_id = tf.timesheet_id
         and ts.is_current = true
        join public.v_ts_invoice_precheck pc
          on pc.timesheet_id = tf.timesheet_id
        left join public.v_timesheets_summary_base vts
          on vts.timesheet_id = tf.timesheet_id
        where tf.is_current = true
          and tf.client_id = p_client_id
          and (ts.week_ending_date::date - 6) = p_invoice_week_start
      ) s;

      -- SEGMENTS candidates for this invoice_week_start (segment-level), including delayed-not-due reasons
      select
        count(*)::int,
        coalesce(jsonb_agg(s) filter (where s.rn <= 25), '[]'::jsonb),
        count(*) filter (where s.fail_reason = 'DELAYED_NOT_DUE')::int
      into
        v_dbg_seg_any_count,
        v_dbg_seg_fail_sample,
        v_dbg_seg_delayed_not_due_count
      from (
        select
          tf.timesheet_id::text as timesheet_id,
          ts.week_ending_date::date as week_ending_date,
          nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '') as invoice_target_week_start,
          nullif(btrim(coalesce(seg_el.value->>'invoice_locked_invoice_id','')), '') as invoice_locked_invoice_id,
          coalesce(seg_el.value->>'segment_type','') as segment_type,
          coalesce(seg_el.value->>'label','') as label,
          coalesce(seg_el.value->>'charge_ex_vat','') as charge_ex_vat,
          coalesce(vts.hr_validation_required_for_invoice, false) as hr_validation_required_for_invoice,
          vts.validation_status::text as validation_status,
          case
            when tf.processing_status <> 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum then 'NOT_READY_FOR_INVOICE'
            when tf.locked_by_invoice_id is not null then 'LOCKED_BY_INVOICE'
            when ts.revoked_at is not null then 'REVOKED'
            when upper(coalesce(pc.precheck_status,'')) <> 'OK' then 'PRECHECK_NOT_OK'
            when (
              coalesce(vts.hr_validation_required_for_invoice, false)
              and vts.validation_status is distinct from 'VALIDATION_OK'::public.validation_status_enum
              and vts.validation_status is distinct from 'OVERRIDDEN'::public.validation_status_enum
            ) then 'HR_VALIDATION_BLOCKED'
            when nullif(btrim(coalesce(seg_el.value->>'invoice_locked_invoice_id','')), '') is not null then 'SEGMENT_LOCKED'
            when (
              nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '') is not null
              and nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '')::date <> (ts.week_ending_date::date - 6)
              and nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '')::date > v_london_today
            ) then 'DELAYED_NOT_DUE'
            when (
              (nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '') is null
               or nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '')::date = (ts.week_ending_date::date - 6))
              and (p_allow_early is not true)
              and (ts.week_ending_date::date >= v_london_today)
            ) then 'WEEK_NOT_PASSED'
            else 'OTHER'
          end as fail_reason,
          row_number() over (order by ts.updated_at desc nulls last) as rn
        from public.timesheets_financials tf
        join public.timesheets ts
          on ts.timesheet_id = tf.timesheet_id
         and ts.is_current = true
        join public.v_ts_invoice_precheck pc
          on pc.timesheet_id = tf.timesheet_id
        left join public.v_timesheets_summary_base vts
          on vts.timesheet_id = tf.timesheet_id
        cross join lateral jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) seg_el(value)
        where tf.is_current = true
          and tf.client_id = p_client_id
          and coalesce(tf.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
          and jsonb_typeof(seg_el.value) = 'object'
          and nullif(btrim(coalesce(seg_el.value->>'segment_id','')), '') is not null
          and coalesce(
                nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '')::date,
                (ts.week_ending_date::date - 6)
              ) = p_invoice_week_start
      ) s;
    exception when others then
      null;
    end;
  end if;

  if not v_has_due then
    -- Mirror existing UX: if week hasn't passed and allow_early is false, show that message.
    if (p_allow_early is not true) and (v_week_end >= v_london_today) then
      if v_invoice_debug then
        begin
          perform public._inv_write_audit(
            p_actor_user_id,
            'INVOICE_OUTBOX_ENQUEUE_BY_WEEK_REJECTED',
            jsonb_build_object(
              'reason', 'week_not_passed_allow_early_false',
              'client_id', p_client_id::text,
              'invoice_week_start', p_invoice_week_start::text,
              'week_ending', v_week_end::text,
              'london_today', v_london_today::text,
              'allow_early', coalesce(p_allow_early,false),
              'has_due', v_has_due,
              'nonseg_due_count', v_dbg_nonseg_due_count,
              'seg_due_count', v_dbg_seg_due_count,
              'nonseg_due_sample', v_dbg_nonseg_due_sample,
              'seg_due_sample', v_dbg_seg_due_sample,
              'nonseg_any_count', v_dbg_nonseg_any_count,
              'seg_any_count', v_dbg_seg_any_count,
              'seg_delayed_not_due_count', v_dbg_seg_delayed_not_due_count,
              'nonseg_fail_sample', v_dbg_nonseg_fail_sample,
              'seg_fail_sample', v_dbg_seg_fail_sample,
              'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
              'run_finished_at_utc', public._inv_iso_utc(now())
            ),
            'invoice_jobs_outbox',
            ('by_week:' || public._inv_iso_utc(v_dbg_run_started)),
            null,
            'INVOICE_DEBUG',
            null, null, null
          );
        exception when others then
          null;
        end;
      end if;
      raise exception 'Week ending % has not passed (London today=%). Use allow_early to override.', v_week_end, v_london_today;
    end if;

    if v_invoice_debug then
      begin
        perform public._inv_write_audit(
          p_actor_user_id,
          'INVOICE_OUTBOX_ENQUEUE_BY_WEEK_REJECTED',
          jsonb_build_object(
            'reason', 'no_invoiceable_items',
            'client_id', p_client_id::text,
            'invoice_week_start', p_invoice_week_start::text,
            'week_ending', v_week_end::text,
            'london_today', v_london_today::text,
            'allow_early', coalesce(p_allow_early,false),
            'has_due', v_has_due,
            'nonseg_due_count', v_dbg_nonseg_due_count,
            'seg_due_count', v_dbg_seg_due_count,
            'nonseg_due_sample', v_dbg_nonseg_due_sample,
            'seg_due_sample', v_dbg_seg_due_sample,
            'nonseg_any_count', v_dbg_nonseg_any_count,
            'seg_any_count', v_dbg_seg_any_count,
            'seg_delayed_not_due_count', v_dbg_seg_delayed_not_due_count,
            'nonseg_fail_sample', v_dbg_nonseg_fail_sample,
            'seg_fail_sample', v_dbg_seg_fail_sample,
            'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
            'run_finished_at_utc', public._inv_iso_utc(now())
          ),
          'invoice_jobs_outbox',
          ('by_week:' || public._inv_iso_utc(v_dbg_run_started)),
          null,
          'INVOICE_DEBUG',
          null, null, null
        );
      exception when others then
        null;
      end;
    end if;
    raise exception 'No invoiceable timesheets/segments for client=% and invoice_week_start=%', p_client_id, p_invoice_week_start;
  end if;

  -- Build payload
  v_payload := jsonb_build_object(
    'client_id', p_client_id::text,
    'invoice_week_start', p_invoice_week_start::text,
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

  -- ✅ Concurrency guard: serialize enqueue per (client_id, invoice_week_start)
  -- Prevents duplicate BY_WEEK outbox rows from concurrent check+insert races.
  perform pg_advisory_xact_lock(
    hashtext(p_client_id::text),
    (p_invoice_week_start - date '2000-01-01')::int
  );

  -- Idempotent: reuse existing outbox row if present
  select o.id
  into v_existing
  from public.invoice_jobs_outbox o
  where o.kind = 'BY_WEEK'
    and (o.payload->>'client_id') = p_client_id::text
    and (o.payload->>'invoice_week_start') = p_invoice_week_start::text
  order by o.created_at desc
  limit 1;

  if v_existing is not null then
    -- Merge allow_early/actor/meta into existing payload so subsequent calls are consistent
    update public.invoice_jobs_outbox o
    set payload = coalesce(o.payload, '{}'::jsonb) || v_payload
    where o.id = v_existing;

    if v_invoice_debug then
      begin
        perform public._inv_write_audit(
          p_actor_user_id,
          'INVOICE_OUTBOX_ENQUEUE_BY_WEEK_REUSED',
          jsonb_build_object(
            'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
            'run_finished_at_utc', public._inv_iso_utc(now()),
            'client_id', p_client_id::text,
            'invoice_week_start', p_invoice_week_start::text,
            'week_ending', v_week_end::text,
            'london_today', v_london_today::text,
            'allow_early', coalesce(p_allow_early,false),
            'has_due', v_has_due,
            'nonseg_due_count', v_dbg_nonseg_due_count,
            'seg_due_count', v_dbg_seg_due_count,
            'existing_outbox_id', v_existing::text,
            'payload_merge', v_payload
          ),
          'invoice_jobs_outbox',
          v_existing::text,
          null,
          'INVOICE_DEBUG',
          null, null, null
        );
      exception when others then
        null;
      end;
    end if;

    return v_existing;
  end if;

  insert into public.invoice_jobs_outbox(kind, payload)
  values ('BY_WEEK'::text, v_payload)
  returning id into v_new;

  if v_invoice_debug then
    begin
      perform public._inv_write_audit(
        p_actor_user_id,
        'INVOICE_OUTBOX_ENQUEUE_BY_WEEK_INSERTED',
        jsonb_build_object(
          'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
          'run_finished_at_utc', public._inv_iso_utc(now()),
          'client_id', p_client_id::text,
          'invoice_week_start', p_invoice_week_start::text,
          'week_ending', v_week_end::text,
          'london_today', v_london_today::text,
          'allow_early', coalesce(p_allow_early,false),
          'has_due', v_has_due,
          'nonseg_due_count', v_dbg_nonseg_due_count,
          'seg_due_count', v_dbg_seg_due_count,
          'new_outbox_id', v_new::text,
          'payload', v_payload
        ),
        'invoice_jobs_outbox',
        v_new::text,
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

  return v_new;
end;
$function$;
