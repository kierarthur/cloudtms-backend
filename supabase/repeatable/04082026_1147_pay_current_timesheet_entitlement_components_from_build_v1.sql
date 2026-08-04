-- Banking Pay bounded-scope V1.2.4: installed entitlement logic over exact build facts.
-- Generated from installed TEST pg_get_functiondef baseline; only input and settled/fallback
-- discovery are replaced. Grouping, Policy X key handling, rounding and ordering are retained.

CREATE OR REPLACE FUNCTION private.pay_current_timesheet_entitlement_components_from_build_v1(p_build_id uuid, p_dependency_unit_key text DEFAULT NULL::text)
 RETURNS TABLE(timesheet_id uuid, key_type text, key_value text, truth_ex_vat numeric, baseline_ex_vat numeric, truth_inc_vat numeric, baseline_inc_vat numeric)
 LANGUAGE sql
 STABLE SECURITY INVOKER
 SET search_path TO ''
AS $function$
with
inp as (
  select coalesce(array_agg(scope_row.timesheet_id order by scope_row.stable_ordinal),array[]::uuid[]) as ts_ids
  from private.banking_pay_workbench_economic_build_scope as scope_row
  where scope_row.build_id=p_build_id
    and scope_row.closure_status='SEALED'
    and (p_dependency_unit_key is null or scope_row.dependency_unit_key=p_dependency_unit_key)
),
rotation_scope_rows as (
  select
    scope_rows.requested_timesheet_id,
    scope_rows.booking_id,
    scope_rows.canonical_timesheet_id,
    scope_rows.family_timesheet_id,
    scope_rows.family_is_current,
    scope_rows.family_version,
    scope_rows.requested_is_canonical
  from inp as input_scope
  join public._pay_timesheet_rotation_scope(input_scope.ts_ids) as scope_rows
    on true
),
rotation_scope_keyed as (
  select
    rotation_scope_rows.requested_timesheet_id,
    rotation_scope_rows.booking_id,
    rotation_scope_rows.canonical_timesheet_id,
    rotation_scope_rows.family_timesheet_id,
    rotation_scope_rows.family_is_current,
    rotation_scope_rows.family_version,
    rotation_scope_rows.requested_is_canonical,
    coalesce(rotation_scope_rows.booking_id, rotation_scope_rows.requested_timesheet_id::text) as scope_family_key
  from rotation_scope_rows
  where rotation_scope_rows.requested_timesheet_id is not null
),
projection_targets as (
  select
    rotation_scope_keyed.scope_family_key,
    coalesce(
      (
        array_agg(distinct rotation_scope_keyed.canonical_timesheet_id order by rotation_scope_keyed.canonical_timesheet_id)
        filter (
          where coalesce(rotation_scope_keyed.requested_is_canonical, false) = true
            and rotation_scope_keyed.canonical_timesheet_id is not null
        )
      )[1],
      (
        array_agg(distinct rotation_scope_keyed.requested_timesheet_id order by rotation_scope_keyed.requested_timesheet_id)
        filter (where rotation_scope_keyed.requested_timesheet_id is not null)
      )[1],
      (
        array_agg(distinct rotation_scope_keyed.canonical_timesheet_id order by rotation_scope_keyed.canonical_timesheet_id)
        filter (where rotation_scope_keyed.canonical_timesheet_id is not null)
      )[1]
    ) as projected_timesheet_id,
    (
      array_agg(distinct rotation_scope_keyed.canonical_timesheet_id order by rotation_scope_keyed.canonical_timesheet_id)
      filter (where rotation_scope_keyed.canonical_timesheet_id is not null)
    )[1] as canonical_timesheet_id
  from rotation_scope_keyed
  group by rotation_scope_keyed.scope_family_key
),
family_to_projection as (
  select distinct
    rotation_scope_keyed.family_timesheet_id,
    projection_targets.projected_timesheet_id,
    projection_targets.canonical_timesheet_id
  from rotation_scope_keyed
  join projection_targets
    on projection_targets.scope_family_key = rotation_scope_keyed.scope_family_key
  where rotation_scope_keyed.family_timesheet_id is not null
    and projection_targets.projected_timesheet_id is not null
),
canonical_projection as (
  select distinct
    family_to_projection.canonical_timesheet_id,
    family_to_projection.projected_timesheet_id
  from family_to_projection
  where family_to_projection.canonical_timesheet_id is not null
    and family_to_projection.projected_timesheet_id is not null
),
tf as (
  select
    canonical_projection.projected_timesheet_id as timesheet_id,
    canonical_projection.canonical_timesheet_id as canonical_timesheet_id,
    tfin.total_pay_ex_vat,
    tfin.invoice_breakdown_json,
    tfin.additional_units_json,
    tfin.expenses_pay_ex_vat,
    tfin.travel_pay_ex_vat,
    tfin.accommodation_pay_ex_vat,
    tfin.other_pay_ex_vat,
    tfin.mileage_pay_ex_vat,
    timesheet_rows.booking_id,
    upper(coalesce(timesheet_rows.sheet_scope::text,'')) as sheet_scope,
    timesheet_rows.reference_number,
    timesheet_rows.worked_start_iso as ts_worked_start_iso,
    timesheet_rows.worked_end_iso as ts_worked_end_iso,
    timesheet_rows.break_start_iso as ts_break_start_iso,
    timesheet_rows.break_end_iso as ts_break_end_iso,
    timesheet_rows.break_minutes as ts_break_minutes,
    timesheet_rows.actual_schedule_json as ts_actual_schedule_json,
    tfin.worked_start_iso as tf_worked_start_iso,
    tfin.worked_end_iso as tf_worked_end_iso,
    tfin.break_start_iso as tf_break_start_iso,
    tfin.break_end_iso as tf_break_end_iso,
    tfin.break_minutes as tf_break_minutes,
    tfin.actual_schedule_json as tf_actual_schedule_json
  from canonical_projection
  join public.timesheets_financials as tfin
    on tfin.is_current = true
   and tfin.timesheet_id = canonical_projection.canonical_timesheet_id
  join public.timesheets as timesheet_rows
    on timesheet_rows.timesheet_id = canonical_projection.canonical_timesheet_id
   and timesheet_rows.is_current = true
   and timesheet_rows.revoked_at is null
   and timesheet_rows.archived_at_utc is null
   and timesheet_rows.authorised_at_server is not null
),
truth_enriched as (
  select
    tf0.timesheet_id,
    tf0.canonical_timesheet_id,
    tf0.total_pay_ex_vat,
    tf0.invoice_breakdown_json,
    tf0.additional_units_json,
    tf0.expenses_pay_ex_vat,
    tf0.travel_pay_ex_vat,
    tf0.accommodation_pay_ex_vat,
    tf0.other_pay_ex_vat,
    tf0.mileage_pay_ex_vat,
    tf0.booking_id,
    tf0.sheet_scope,
    tf0.reference_number,
    coalesce(tf0.tf_worked_start_iso, tf0.ts_worked_start_iso) as effective_worked_start_iso,
    coalesce(tf0.tf_worked_end_iso, tf0.ts_worked_end_iso) as effective_worked_end_iso,
    coalesce(tf0.tf_break_start_iso, tf0.ts_break_start_iso) as effective_break_start_iso,
    coalesce(tf0.tf_break_end_iso, tf0.ts_break_end_iso) as effective_break_end_iso,
    coalesce(tf0.tf_break_minutes, tf0.ts_break_minutes) as effective_break_minutes,
    case
      when jsonb_typeof(tf0.tf_actual_schedule_json) = 'object' then tf0.tf_actual_schedule_json
      when jsonb_typeof(tf0.tf_actual_schedule_json) = 'array' then (
        select tf_sched_item.value
        from jsonb_array_elements(tf0.tf_actual_schedule_json) as tf_sched_item(value)
        where tf_sched_item.value is not null
          and jsonb_typeof(tf_sched_item.value) = 'object'
        limit 1
      )
      when jsonb_typeof(tf0.ts_actual_schedule_json) = 'object' then tf0.ts_actual_schedule_json
      when jsonb_typeof(tf0.ts_actual_schedule_json) = 'array' then (
        select ts_sched_item.value
        from jsonb_array_elements(tf0.ts_actual_schedule_json) as ts_sched_item(value)
        where ts_sched_item.value is not null
          and jsonb_typeof(ts_sched_item.value) = 'object'
        limit 1
      )
      else null::jsonb
    end as effective_daily_schedule_json
  from tf tf0
),
truth_segments as (
  select
    te.timesheet_id,
    te.canonical_timesheet_id,
    case
      when te.invoice_breakdown_json is not null
       and jsonb_typeof(te.invoice_breakdown_json) = 'object'
       and upper(coalesce(te.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
       and jsonb_typeof(te.invoice_breakdown_json->'segments') = 'array'
      then (
        select coalesce(
          jsonb_agg(seg.value),
          '[]'::jsonb
        )
        from jsonb_array_elements(te.invoice_breakdown_json->'segments') as seg(value)
        where seg.value is not null
          and jsonb_typeof(seg.value) = 'object'
      )
      when te.sheet_scope = 'DAILY'
      then jsonb_build_array(
        jsonb_build_object(
          'segment_id', ('ts:' || te.timesheet_id::text),
          'pay_amount', round(coalesce(te.total_pay_ex_vat,0),2),
          'exclude_from_pay', false,
          'date', case
            when te.effective_worked_start_iso is not null then ((te.effective_worked_start_iso at time zone 'Europe/London')::date)::text
            else coalesce(
              nullif(btrim(coalesce(te.effective_daily_schedule_json->>'date','')), ''),
              nullif(btrim(coalesce(te.effective_daily_schedule_json->>'work_date','')), ''),
              nullif(btrim(coalesce(te.effective_daily_schedule_json->>'ymd','')), ''),
              nullif(btrim(coalesce(te.effective_daily_schedule_json->>'date_ymd','')), '')
            )
          end,
          'segment_key', ('ts:' || te.timesheet_id::text),
          'segment_stable_key', ('timesheet:' || coalesce(te.booking_id, te.timesheet_id::text)),
          'ref_num', nullif(btrim(coalesce(te.reference_number,'')), ''),
          'start_utc', case
            when te.effective_worked_start_iso is not null then te.effective_worked_start_iso::text
            else null
          end,
          'end_utc', case
            when te.effective_worked_end_iso is not null then te.effective_worked_end_iso::text
            else null
          end,
          'start', case
            when te.effective_worked_start_iso is not null then to_char((te.effective_worked_start_iso at time zone 'Europe/London'), 'HH24:MI')
            else coalesce(
              nullif(btrim(coalesce(te.effective_daily_schedule_json->>'start','')), ''),
              nullif(btrim(coalesce(te.effective_daily_schedule_json->>'worked_start','')), '')
            )
          end,
          'end', case
            when te.effective_worked_end_iso is not null then to_char((te.effective_worked_end_iso at time zone 'Europe/London'), 'HH24:MI')
            else coalesce(
              nullif(btrim(coalesce(te.effective_daily_schedule_json->>'end','')), ''),
              nullif(btrim(coalesce(te.effective_daily_schedule_json->>'worked_end','')), '')
            )
          end,
          'break_start', case
            when te.effective_break_start_iso is not null then to_char((te.effective_break_start_iso at time zone 'Europe/London'), 'HH24:MI')
            else nullif(btrim(coalesce(te.effective_daily_schedule_json->>'break_start','')), '')
          end,
          'break_end', case
            when te.effective_break_end_iso is not null then to_char((te.effective_break_end_iso at time zone 'Europe/London'), 'HH24:MI')
            else nullif(btrim(coalesce(te.effective_daily_schedule_json->>'break_end','')), '')
          end,
          'break_mins', coalesce(
            te.effective_break_minutes::numeric,
            nullif(btrim(coalesce(te.effective_daily_schedule_json->>'break_minutes','')), '')::numeric,
            nullif(btrim(coalesce(te.effective_daily_schedule_json->>'break_mins','')), '')::numeric,
            nullif(btrim(coalesce(te.effective_daily_schedule_json->>'breakMinutes','')), '')::numeric,
            nullif(btrim(coalesce(te.effective_daily_schedule_json->>'breakMin','')), '')::numeric,
            case
              when te.effective_break_start_iso is not null and te.effective_break_end_iso is not null
                then greatest(
                  0::numeric,
                  round((extract(epoch from (te.effective_break_end_iso - te.effective_break_start_iso)) / 60.0)::numeric, 0)
                )
              else null::numeric
            end
          ),
          'breaks', case
            when jsonb_typeof(te.effective_daily_schedule_json->'breaks') = 'array' then te.effective_daily_schedule_json->'breaks'
            when te.effective_break_start_iso is not null and te.effective_break_end_iso is not null
              then jsonb_build_array(
                jsonb_build_object(
                  'start', to_char((te.effective_break_start_iso at time zone 'Europe/London'), 'HH24:MI'),
                  'end', to_char((te.effective_break_end_iso at time zone 'Europe/London'), 'HH24:MI'),
                  'break_mins', coalesce(
                    te.effective_break_minutes::numeric,
                    case
                      when te.effective_break_start_iso is not null and te.effective_break_end_iso is not null
                        then greatest(
                          0::numeric,
                          round((extract(epoch from (te.effective_break_end_iso - te.effective_break_start_iso)) / 60.0)::numeric, 0)
                        )
                      else null::numeric
                    end
                  )
                )
              )
            when nullif(btrim(coalesce(te.effective_daily_schedule_json->>'break_start','')), '') is not null
             and nullif(btrim(coalesce(te.effective_daily_schedule_json->>'break_end','')), '') is not null
              then jsonb_build_array(
                jsonb_build_object(
                  'start', nullif(btrim(coalesce(te.effective_daily_schedule_json->>'break_start','')), ''),
                  'end', nullif(btrim(coalesce(te.effective_daily_schedule_json->>'break_end','')), ''),
                  'break_mins', coalesce(
                    te.effective_break_minutes::numeric,
                    nullif(btrim(coalesce(te.effective_daily_schedule_json->>'break_minutes','')), '')::numeric,
                    nullif(btrim(coalesce(te.effective_daily_schedule_json->>'break_mins','')), '')::numeric,
                    nullif(btrim(coalesce(te.effective_daily_schedule_json->>'breakMinutes','')), '')::numeric,
                    nullif(btrim(coalesce(te.effective_daily_schedule_json->>'breakMin','')), '')::numeric
                  )
                )
              )
            else '[]'::jsonb
          end
        )
      )
      else jsonb_build_array(
        jsonb_build_object(
          'segment_id', ('ts:' || te.timesheet_id::text),
          'pay_amount', round(coalesce(te.total_pay_ex_vat,0),2),
          'exclude_from_pay', false
        )
      )
    end as segments_json
  from truth_enriched te
),
adjustment_truth_rows as (
  select distinct on (
    family_to_projection.projected_timesheet_id,
    adjustment_rows.id
  )
    family_to_projection.projected_timesheet_id as timesheet_id,
    adjustment_rows.id as adjustment_id,
    adjustment_rows.created_at as adjustment_created_at,
    round(coalesce(adjustment_rows.delta_pay_ex_vat, 0), 2) as delta_pay_ex_vat
  from family_to_projection
  join public.ts_pay_adjustments as adjustment_rows
    on adjustment_rows.timesheet_id = family_to_projection.family_timesheet_id
  where adjustment_rows.as_advance = false
    and adjustment_rows.timesheet_id is not null
    and adjustment_rows.id is not null
    and family_to_projection.projected_timesheet_id is not null
  order by
    family_to_projection.projected_timesheet_id,
    adjustment_rows.id,
    adjustment_rows.created_at nulls last,
    family_to_projection.family_timesheet_id
),
truth_snapshot_like as (
  select
    truth_segment_rows.timesheet_id,
    jsonb_build_object(
      'segments', truth_segment_rows.segments_json,
      'additional_units_json', coalesce(tf1.additional_units_json,'{}'::jsonb),
      'additional_pay_ex_vat', 0,
      'expenses', jsonb_build_object(
        'expenses_pay_ex_vat', round(coalesce(tf1.expenses_pay_ex_vat,0),2),
        'travel_pay_ex_vat', round(coalesce(tf1.travel_pay_ex_vat,0),2),
        'accommodation_pay_ex_vat', round(coalesce(tf1.accommodation_pay_ex_vat,0),2),
        'other_pay_ex_vat', round(coalesce(tf1.other_pay_ex_vat,0),2),
        'mileage_pay_ex_vat', round(coalesce(tf1.mileage_pay_ex_vat,0),2)
      ),
      'adjustments', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'id', adjustment_truth_rows.adjustment_id::text,
            'delta_pay_ex_vat', adjustment_truth_rows.delta_pay_ex_vat
          )
          order by adjustment_truth_rows.adjustment_created_at nulls last, adjustment_truth_rows.adjustment_id
        )
        from adjustment_truth_rows
        where adjustment_truth_rows.timesheet_id = truth_segment_rows.timesheet_id
      ), '[]'::jsonb)
    ) as snap_json
  from truth_segments as truth_segment_rows
  join tf as tf1
    on tf1.timesheet_id = truth_segment_rows.timesheet_id
),
truth_components_raw as (
  select
    truth_snapshot_rows.timesheet_id,
    upper(nullif(btrim(coalesce(timesheet_component_rows.key_type, '')), '')) as raw_key_type,
    nullif(btrim(coalesce(timesheet_component_rows.key_value, '')), '') as raw_key_value,
    timesheet_component_rows.amount_ex_vat,
    timesheet_component_rows.amount_inc_vat,
    case
      when upper(nullif(btrim(coalesce(timesheet_component_rows.key_type, '')), '')) in ('TS_DAY','TS_TOTAL')
        then 'SEGMENT_DELTA'
      when upper(nullif(btrim(coalesce(timesheet_component_rows.key_type, '')), '')) = 'ADJUSTMENT_CODE'
        then 'ADJUSTMENT_DELTA'
      when upper(nullif(btrim(coalesce(timesheet_component_rows.key_type, '')), '')) = 'EXPENSE_CODE'
       and upper(nullif(btrim(coalesce(timesheet_component_rows.key_value, '')), '')) = 'MILEAGE'
        then 'MILEAGE_DELTA'
      when upper(nullif(btrim(coalesce(timesheet_component_rows.key_type, '')), '')) in ('ADDITIONAL_CODE','EXPENSE_CODE')
        then 'EXPENSE_DELTA'
      else null::text
    end as resolver_item_type
  from truth_snapshot_like as truth_snapshot_rows
  join lateral public._pay_timesheet_components(truth_snapshot_rows.snap_json) as timesheet_component_rows
    on true
),
truth_components as (
  select
    truth_component_rows.timesheet_id,
    resolved_truth_keys.key_type,
    resolved_truth_keys.key_value,
    truth_component_rows.amount_ex_vat,
    truth_component_rows.amount_inc_vat
  from truth_components_raw as truth_component_rows
  join lateral public._pay_policy_x_resolve_pre_draft_economic_key(
    p_timesheet_id => truth_component_rows.timesheet_id,
    p_live_source_json => jsonb_build_object(
      'timesheet_id', truth_component_rows.timesheet_id::text,
      'item_type', truth_component_rows.resolver_item_type,
      'component_key_type', truth_component_rows.raw_key_type,
      'component_key_value', truth_component_rows.raw_key_value,
      'work_date', case when truth_component_rows.raw_key_type = 'TS_DAY' then truth_component_rows.raw_key_value else null::text end
    ),
    p_item_type => truth_component_rows.resolver_item_type,
    p_key_type_hint => truth_component_rows.raw_key_type,
    p_key_value_hint => truth_component_rows.raw_key_value,
    p_work_date => case
      when truth_component_rows.raw_key_type = 'TS_DAY'
       and truth_component_rows.raw_key_value ~ '^\d{4}-\d{2}-\d{2}$'
        then truth_component_rows.raw_key_value::date
      else null::date
    end
  ) as resolved_truth_keys
    on true
  where resolved_truth_keys.key_resolution_failure_reason is null
),
active_settled_components as (
  select fact_row.timesheet_id,fact_row.economic_key_type as key_type,
         fact_row.economic_key_value as key_value,
         fact_row.amount_ex_vat,fact_row.amount_inc_vat
  from private.banking_pay_workbench_economic_build_facts as fact_row
  where fact_row.build_id=p_build_id
    and fact_row.fact_family='FROZEN_SETTLED_COMPONENT'
    and (p_dependency_unit_key is null or fact_row.dependency_unit_key=p_dependency_unit_key)
),
legacy_baseline_timesheets as (
  select null::uuid as source_timesheet_id,null::uuid as projected_timesheet_id where false
),
legacy_baseline_components_raw as (
  select fact_row.timesheet_id,
         fact_row.economic_key_type as raw_key_type,
         fact_row.economic_key_value as raw_key_value,
         fact_row.amount_ex_vat,fact_row.amount_inc_vat,
         case
           when fact_row.economic_key_type in ('TS_DAY','TS_TOTAL') then 'SEGMENT_DELTA'
           when fact_row.economic_key_type='ADJUSTMENT_CODE' then 'ADJUSTMENT_DELTA'
           when fact_row.economic_key_type='EXPENSE_CODE' and upper(fact_row.economic_key_value)='MILEAGE' then 'MILEAGE_DELTA'
           when fact_row.economic_key_type in ('ADDITIONAL_CODE','EXPENSE_CODE') then 'EXPENSE_DELTA'
           else null::text
         end as resolver_item_type
  from private.banking_pay_workbench_economic_build_facts as fact_row
  where fact_row.build_id=p_build_id
    and fact_row.fact_family='PAY_STATE_FALLBACK'
    and (p_dependency_unit_key is null or fact_row.dependency_unit_key=p_dependency_unit_key)
),
legacy_baseline_components as (
  select
    legacy_baseline_rows.timesheet_id,
    resolved_baseline_keys.key_type,
    resolved_baseline_keys.key_value,
    legacy_baseline_rows.amount_ex_vat,
    legacy_baseline_rows.amount_inc_vat
  from legacy_baseline_components_raw as legacy_baseline_rows
  join lateral public._pay_policy_x_resolve_pre_draft_economic_key(
    p_timesheet_id => legacy_baseline_rows.timesheet_id,
    p_live_source_json => jsonb_build_object(
      'timesheet_id', legacy_baseline_rows.timesheet_id::text,
      'item_type', legacy_baseline_rows.resolver_item_type,
      'component_key_type', legacy_baseline_rows.raw_key_type,
      'component_key_value', legacy_baseline_rows.raw_key_value,
      'work_date', case when legacy_baseline_rows.raw_key_type = 'TS_DAY' then legacy_baseline_rows.raw_key_value else null::text end
    ),
    p_item_type => legacy_baseline_rows.resolver_item_type,
    p_key_type_hint => legacy_baseline_rows.raw_key_type,
    p_key_value_hint => legacy_baseline_rows.raw_key_value,
    p_work_date => case
      when legacy_baseline_rows.raw_key_type = 'TS_DAY'
       and legacy_baseline_rows.raw_key_value ~ '^\d{4}-\d{2}-\d{2}$'
        then legacy_baseline_rows.raw_key_value::date
      else null::date
    end
  ) as resolved_baseline_keys
    on true
  where resolved_baseline_keys.key_resolution_failure_reason is null
),
baseline_components as (
  select
    active_settled_components.timesheet_id,
    active_settled_components.key_type,
    active_settled_components.key_value,
    active_settled_components.amount_ex_vat,
    active_settled_components.amount_inc_vat
  from active_settled_components
  union all
  select
    legacy_baseline_components.timesheet_id,
    legacy_baseline_components.key_type,
    legacy_baseline_components.key_value,
    legacy_baseline_components.amount_ex_vat,
    legacy_baseline_components.amount_inc_vat
  from legacy_baseline_components
),
truth_grouped as (
  select
    truth_components.timesheet_id,
    upper(nullif(btrim(coalesce(truth_components.key_type, '')), '')) as key_type,
    nullif(btrim(coalesce(truth_components.key_value, '')), '') as key_value,
    round(sum(coalesce(truth_components.amount_ex_vat, 0)), 2) as truth_ex_vat,
    round(sum(coalesce(truth_components.amount_inc_vat, 0)), 2) as truth_inc_vat
  from truth_components
  where truth_components.timesheet_id is not null
    and truth_components.key_type is not null
    and btrim(truth_components.key_type) <> ''
    and truth_components.key_value is not null
    and btrim(truth_components.key_value) <> ''
    and truth_components.key_type in ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE')
    and not (truth_components.key_type = 'TS_DAY' and truth_components.key_value !~ '^\d{4}-\d{2}-\d{2}$')
  group by
    truth_components.timesheet_id,
    upper(nullif(btrim(coalesce(truth_components.key_type, '')), '')),
    nullif(btrim(coalesce(truth_components.key_value, '')), '')
),
baseline_grouped as (
  select
    baseline_components.timesheet_id,
    upper(nullif(btrim(coalesce(baseline_components.key_type, '')), '')) as key_type,
    nullif(btrim(coalesce(baseline_components.key_value, '')), '') as key_value,
    round(sum(coalesce(baseline_components.amount_ex_vat, 0)), 2) as baseline_ex_vat,
    round(sum(coalesce(baseline_components.amount_inc_vat, 0)), 2) as baseline_inc_vat
  from baseline_components
  where baseline_components.timesheet_id is not null
    and baseline_components.key_type is not null
    and btrim(baseline_components.key_type) <> ''
    and baseline_components.key_value is not null
    and btrim(baseline_components.key_value) <> ''
    and baseline_components.key_type in ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE')
    and not (baseline_components.key_type = 'TS_DAY' and baseline_components.key_value !~ '^\d{4}-\d{2}-\d{2}$')
  group by
    baseline_components.timesheet_id,
    upper(nullif(btrim(coalesce(baseline_components.key_type, '')), '')),
    nullif(btrim(coalesce(baseline_components.key_value, '')), '')
),
all_keys as (
  select
    truth_grouped.timesheet_id,
    truth_grouped.key_type,
    truth_grouped.key_value
  from truth_grouped

  union

  select
    baseline_grouped.timesheet_id,
    baseline_grouped.key_type,
    baseline_grouped.key_value
  from baseline_grouped
)
select
  all_keys.timesheet_id,
  all_keys.key_type,
  all_keys.key_value,
  round(coalesce(truth_grouped.truth_ex_vat, 0), 2) as truth_ex_vat,
  round(coalesce(baseline_grouped.baseline_ex_vat, 0), 2) as baseline_ex_vat,
  round(coalesce(truth_grouped.truth_inc_vat, 0), 2) as truth_inc_vat,
  round(coalesce(baseline_grouped.baseline_inc_vat, 0), 2) as baseline_inc_vat
from all_keys
left join truth_grouped
  on truth_grouped.timesheet_id = all_keys.timesheet_id
 and truth_grouped.key_type = all_keys.key_type
 and truth_grouped.key_value = all_keys.key_value
left join baseline_grouped
  on baseline_grouped.timesheet_id = all_keys.timesheet_id
 and baseline_grouped.key_type = all_keys.key_type
 and baseline_grouped.key_value = all_keys.key_value
where all_keys.timesheet_id is not null
  and all_keys.key_type in ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE')
  and all_keys.key_value is not null
  and btrim(all_keys.key_value) <> ''
  and not (all_keys.key_type = 'TS_DAY' and all_keys.key_value !~ '^\d{4}-\d{2}-\d{2}$')
order by
  all_keys.timesheet_id,
  all_keys.key_type,
  all_keys.key_value;
$function$
;

ALTER FUNCTION private.pay_current_timesheet_entitlement_components_from_build_v1(uuid,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_current_timesheet_entitlement_components_from_build_v1(uuid,text) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_current_timesheet_entitlement_components_from_build_v1(uuid,text) TO postgres;

