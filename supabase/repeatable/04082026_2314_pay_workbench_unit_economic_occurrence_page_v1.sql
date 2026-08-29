-- Banking Pay bounded-scope V1.2.9: bounded physical economic occurrences
-- for one sealed dependency-unit projection. No page-local rotation expansion,
-- grouped fallback multiplicity or unscoped reservation aggregation.

CREATE OR REPLACE FUNCTION private.pay_workbench_unit_economic_occurrence_page_v1(
  p_build_id uuid,
  p_dependency_unit_key text,
  p_fact_family text,
  p_projected_timesheet_id uuid,
  p_last_source_key text DEFAULT NULL::text,
  p_limit integer DEFAULT 25
)
RETURNS TABLE(
  source_key text,
  natural_key text,
  timesheet_id uuid,
  subject_timesheet_ids uuid[],
  source_relation text,
  source_id uuid,
  economic_key_type text,
  economic_key_value text,
  amount_ex_vat numeric,
  amount_inc_vat numeric,
  truth_ex_vat numeric,
  truth_inc_vat numeric,
  baseline_ex_vat numeric,
  baseline_inc_vat numeric,
  source_payload_json jsonb,
  financial_digest text,
  resolution_failure text
)
LANGUAGE plpgsql
STABLE
PARALLEL UNSAFE
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_limit integer:=LEAST(GREATEST(COALESCE(p_limit,25),1),25)+1;
  v_family text:=UPPER(NULLIF(BTRIM(COALESCE(p_fact_family,'')),''));
BEGIN
  IF p_build_id IS NULL OR NULLIF(BTRIM(COALESCE(p_dependency_unit_key,'')),'') IS NULL
     OR p_projected_timesheet_id IS NULL
     OR v_family NOT IN ('LIVE_ENTITLEMENT_INPUT','FROZEN_SETTLED_COMPONENT','PAY_STATE_FALLBACK') THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_BUILD_CURSOR_INVALID' USING ERRCODE='22023';
  END IF;

  IF NOT EXISTS(
    SELECT 1
    FROM private.banking_pay_workbench_economic_build_facts projection
    WHERE projection.build_id=p_build_id
      AND projection.fact_family='LIVE_ENTITLEMENT_INPUT'
      AND projection.dependency_unit_key=p_dependency_unit_key
      AND projection.source_relation='UNIT_PROJECTION'
      AND projection.timesheet_id=p_projected_timesheet_id
  ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_UNIT_PROJECTION_INCOMPLETE' USING ERRCODE='23514';
  END IF;

  IF v_family='LIVE_ENTITLEMENT_INPUT' THEN
    RETURN QUERY
    WITH build_authority AS MATERIALIZED (
      SELECT build.*
      FROM private.banking_pay_workbench_economic_builds build
      WHERE build.id=p_build_id
    ), session_authority AS MATERIALIZED (
      SELECT build.*,session.id AS authority_session_id,
        session.version AS authority_session_version,session.pay_date,
        session.week_ending_cutoff
      FROM build_authority build
      LEFT JOIN public.banking_pay_workbench_sessions session
        ON session.id=build.session_id
    ), target_authority AS MATERIALIZED (
      SELECT authority.*,
        candidate.id AS authority_candidate_id,
        UPPER(NULLIF(BTRIM(candidate.pay_method),'')) AS target_pay_method,
        candidate.rev AS candidate_rev,candidate.updated_at AS candidate_updated_at,
        candidate.umbrella_id,
        umbrella.enabled AS umbrella_enabled,
        umbrella.vat_chargeable AS umbrella_vat_chargeable,
        umbrella.updated_at AS umbrella_updated_at
      FROM session_authority authority
      LEFT JOIN public.candidates candidate ON candidate.id=authority.candidate_id
      LEFT JOIN public.umbrellas umbrella ON umbrella.id=candidate.umbrella_id
    ), finance_window_authority AS MATERIALIZED (
      SELECT authority.*,
        finance_window.id AS finance_window_id,
        finance_window.date_from AS finance_window_date_from,
        finance_window.date_to AS finance_window_date_to,
        finance_window.erni_pct,finance_window.vat_rate_pct,
        finance_window.updated_at AS finance_window_updated_at,
        COALESCE(finance_window.winning_date_match_count,0)::integer
          AS finance_window_winning_date_match_count
      FROM target_authority authority
      LEFT JOIN LATERAL (
        SELECT matched.*,
          count(*) OVER(PARTITION BY matched.date_from) AS winning_date_match_count
        FROM public.settings_finance_windows matched
        WHERE authority.pay_date>=matched.date_from
          AND authority.pay_date<=COALESCE(matched.date_to,'infinity'::date)
        ORDER BY matched.date_from DESC,matched.id DESC
        LIMIT 1
      ) finance_window ON true
    ), projection_members AS (
      SELECT DISTINCT projection.source_id AS family_timesheet_id
      FROM private.banking_pay_workbench_economic_build_facts projection
      WHERE projection.build_id=p_build_id
        AND projection.fact_family='LIVE_ENTITLEMENT_INPUT'
        AND projection.dependency_unit_key=p_dependency_unit_key
        AND projection.source_relation='UNIT_PROJECTION'
        AND projection.timesheet_id=p_projected_timesheet_id
        AND projection.source_id IS NOT NULL
    ), canonical AS (
      SELECT ts.*,tf.id AS financial_id,tf.timesheet_version AS financial_timesheet_version,
        tf.computed_at_utc AS financial_computed_at_utc,
        tf.updated_at AS financial_updated_at,tf.candidate_id AS financial_candidate_id,
        tf.client_id AS financial_client_id,
        UPPER(NULLIF(BTRIM(tf.pay_method),'')) AS source_pay_method,
        tf.total_pay_ex_vat,tf.total_charge_ex_vat,tf.invoice_breakdown_json,
        tf.additional_units_json,tf.expenses_pay_ex_vat,tf.expenses_charge_ex_vat,
        tf.travel_pay_ex_vat,tf.travel_charge_ex_vat,
        tf.accommodation_pay_ex_vat,tf.accommodation_charge_ex_vat,
        tf.other_pay_ex_vat,tf.other_charge_ex_vat,
        tf.mileage_pay_ex_vat,tf.mileage_charge_ex_vat,
        tf.hours_day,tf.hours_night,tf.hours_sat,tf.hours_sun,tf.hours_bh,
        tf.pay_day,tf.pay_night,tf.pay_sat,tf.pay_sun,tf.pay_bh,
        tf.charge_day,tf.charge_night,tf.charge_sat,tf.charge_sun,tf.charge_bh,
        tf.current_financial_count,
        tf.worked_start_iso AS tf_worked_start_iso,
        tf.worked_end_iso AS tf_worked_end_iso,
        tf.break_start_iso AS tf_break_start_iso,
        tf.break_end_iso AS tf_break_end_iso,
        tf.break_minutes AS tf_break_minutes,
        tf.actual_schedule_json AS tf_actual_schedule_json,
        authority.candidate_id AS build_candidate_id,
        authority.session_id AS build_session_id,
        authority.session_version AS build_session_version,
        authority.captured_candidate_generation,authority.source_change_seq,
        authority.authority_fingerprint_version,authority.authority_fingerprint,
        authority.source_build_run_id,authority.source_job_id,
        authority.authority_session_id,authority.authority_session_version,
        authority.pay_date,authority.week_ending_cutoff,
        authority.authority_candidate_id,authority.target_pay_method,
        authority.candidate_rev,authority.candidate_updated_at,
        authority.umbrella_id,authority.umbrella_enabled,
        authority.umbrella_vat_chargeable,authority.umbrella_updated_at,
        authority.finance_window_id,authority.finance_window_date_from,
        authority.finance_window_date_to,authority.erni_pct,authority.vat_rate_pct,
        authority.finance_window_updated_at,
        authority.finance_window_winning_date_match_count
      FROM public.timesheets ts
      CROSS JOIN finance_window_authority authority
      LEFT JOIN LATERAL (
        SELECT current_financial.*,
          count(*) OVER()::integer AS current_financial_count
        FROM public.timesheets_financials current_financial
        WHERE current_financial.timesheet_id=ts.timesheet_id
          AND current_financial.is_current
        ORDER BY current_financial.computed_at_utc DESC NULLS LAST,
          current_financial.id DESC
        LIMIT 1
      ) tf ON true
      WHERE ts.timesheet_id=p_projected_timesheet_id
        AND ts.is_current AND ts.revoked_at IS NULL
        AND ts.archived_at_utc IS NULL AND ts.authorised_at_server IS NOT NULL
      LIMIT 1
    ), segment_rows AS (
      SELECT
        '10:'||p_projected_timesheet_id::text||':SEGMENT:'||LPAD(segment.ordinality::text,12,'0') AS source_key,
        'timesheets_financials'::text AS source_relation,
        canonical.financial_id AS source_id,
        CASE WHEN NULLIF(BTRIM(COALESCE(segment.value->>'date','')),'') ~ '^\d{4}-\d{2}-\d{2}$'
          THEN 'TS_DAY' ELSE 'TS_TOTAL' END AS raw_key_type,
        CASE WHEN NULLIF(BTRIM(COALESCE(segment.value->>'date','')),'') ~ '^\d{4}-\d{2}-\d{2}$'
          THEN BTRIM(segment.value->>'date') ELSE 'TOTAL' END AS raw_key_value,
        ROUND(CASE WHEN lower(COALESCE(NULLIF(segment.value->>'exclude_from_pay',''),'false'))='true'
          THEN 0::numeric
          WHEN COALESCE(segment.value->>'pay_amount','') ~ '^-?\d+(\.\d+)?$'
          THEN (segment.value->>'pay_amount')::numeric ELSE 0::numeric END,2) AS amount_ex_vat,
        jsonb_build_object('role','LIVE_COMPONENT','source_kind','SEGMENT',
          'projected_timesheet_id',p_projected_timesheet_id,'segment',segment.value,
          'source_ordinal',segment.ordinality) AS payload,
        CASE
          WHEN jsonb_typeof(canonical.invoice_breakdown_json)='object'
            AND UPPER(COALESCE(canonical.invoice_breakdown_json->>'mode',''))='SEGMENTS'
            AND jsonb_typeof(canonical.invoice_breakdown_json->'segments') IS DISTINCT FROM 'array'
            THEN 'SEGMENT_INPUT_NOT_ARRAY'
          WHEN jsonb_typeof(segment.value)<>'object' THEN 'SEGMENT_NOT_OBJECT'
          WHEN NULLIF(BTRIM(COALESCE(segment.value->>'segment_id','')),'') IS NULL
            THEN 'SEGMENT_ID_MISSING'
          WHEN NULLIF(segment.value->>'exclude_from_pay','') IS NOT NULL
            AND lower(segment.value->>'exclude_from_pay') NOT IN ('true','false')
            THEN 'SEGMENT_EXCLUDE_INVALID'
          WHEN lower(COALESCE(NULLIF(segment.value->>'exclude_from_pay',''),'false'))='false'
            AND COALESCE(segment.value->>'pay_amount','') !~ '^-?\d+(\.\d+)?$'
            THEN 'SEGMENT_AMOUNT_INVALID'
          WHEN NULLIF(BTRIM(COALESCE(segment.value->>'date','')),'') IS NOT NULL
            AND NOT (
              segment.value->>'date' ~ '^\d{4}-\d{2}-\d{2}$'
              AND pg_input_is_valid(segment.value->>'date','date')
              AND CASE WHEN pg_input_is_valid(segment.value->>'date','date')
                THEN ((segment.value->>'date')::date)::text=segment.value->>'date'
                ELSE false END)
            THEN 'SEGMENT_DATE_INVALID'
        END AS raw_failure
      FROM canonical
      CROSS JOIN LATERAL (
        SELECT canonical.invoice_breakdown_json->'segments'->bounded_index.zero_index AS value,
          bounded_index.zero_index+1 AS ordinality
        FROM LATERAL generate_series(
          CASE WHEN p_last_source_key LIKE '10:'||p_projected_timesheet_id::text||':SEGMENT:%'
            THEN COALESCE(NULLIF(regexp_replace(p_last_source_key,
              '^.*:SEGMENT:0*','','g'),'')::integer,0) ELSE 0 END,
          LEAST(jsonb_array_length(CASE WHEN jsonb_typeof(canonical.invoice_breakdown_json)='object'
              AND jsonb_typeof(canonical.invoice_breakdown_json->'segments')='array'
            THEN canonical.invoice_breakdown_json->'segments' ELSE '[]'::jsonb END)-1,
            CASE WHEN p_last_source_key LIKE '10:'||p_projected_timesheet_id::text||':SEGMENT:%'
              THEN COALESCE(NULLIF(regexp_replace(p_last_source_key,
                '^.*:SEGMENT:0*','','g'),'')::integer,0) ELSE 0 END + v_limit-1)
        ) bounded_index(zero_index)
        WHERE jsonb_typeof(canonical.invoice_breakdown_json)='object'
          AND UPPER(COALESCE(canonical.invoice_breakdown_json->>'mode',''))='SEGMENTS'
          AND jsonb_typeof(canonical.invoice_breakdown_json->'segments')='array'
        UNION ALL
        SELECT jsonb_build_object(
            'segment_id','ts:'||canonical.timesheet_id::text,
            'segment_key','ts:'||canonical.timesheet_id::text,
            'segment_stable_key','ts:'||canonical.timesheet_id::text,
            'hours_day',ROUND(COALESCE(canonical.hours_day,0),6),
            'hours_night',ROUND(COALESCE(canonical.hours_night,0),6),
            'hours_sat',ROUND(COALESCE(canonical.hours_sat,0),6),
            'hours_sun',ROUND(COALESCE(canonical.hours_sun,0),6),
            'hours_bh',ROUND(COALESCE(canonical.hours_bh,0),6),
            'pay_amount',ROUND(COALESCE(canonical.total_pay_ex_vat,0),2),
            'charge_amount',ROUND(COALESCE(canonical.total_charge_ex_vat,0),2),
            'exclude_from_pay',false,
            'date',CASE
              WHEN UPPER(COALESCE(canonical.sheet_scope::text,''))='DAILY'
               AND COALESCE(canonical.tf_worked_start_iso,canonical.worked_start_iso) IS NOT NULL
                THEN ((COALESCE(canonical.tf_worked_start_iso,canonical.worked_start_iso)
                  AT TIME ZONE 'Europe/London')::date)::text
              WHEN UPPER(COALESCE(canonical.sheet_scope::text,''))='DAILY' THEN COALESCE(
                NULLIF(BTRIM(COALESCE(canonical.tf_actual_schedule_json->>'date','')),''),
                NULLIF(BTRIM(COALESCE(canonical.actual_schedule_json->>'date','')),''))
              ELSE NULL END),
          1::bigint
        WHERE NOT (jsonb_typeof(canonical.invoice_breakdown_json)='object'
          AND UPPER(COALESCE(canonical.invoice_breakdown_json->>'mode',''))='SEGMENTS'
          AND jsonb_typeof(canonical.invoice_breakdown_json->'segments')='array')
      ) segment
    ), additional_rows AS (
      SELECT '10:'||p_projected_timesheet_id::text||':ADDITIONAL:'||
          LPAD(octet_length(additional.key)::text,10,'0')||':'||
          encode(convert_to(additional.key,'UTF8'),'hex') AS source_key,
        'timesheets_financials'::text AS source_relation,canonical.financial_id AS source_id,
        'ADDITIONAL_CODE'::text AS raw_key_type,UPPER(BTRIM(additional.key)) AS raw_key_value,
        ROUND(COALESCE(
          CASE WHEN COALESCE(additional.value->>'pay_ex_vat','') ~ '^-?\d+(\.\d+)?$'
            THEN (additional.value->>'pay_ex_vat')::numeric END,
          CASE WHEN COALESCE(additional.value->>'amount_ex_vat','') ~ '^-?\d+(\.\d+)?$'
            THEN (additional.value->>'amount_ex_vat')::numeric END,
          COALESCE(CASE WHEN COALESCE(additional.value->>'unit_count','') ~ '^-?\d+(\.\d+)?$'
            THEN (additional.value->>'unit_count')::numeric END,
            CASE WHEN COALESCE(additional.value->>'units_week','') ~ '^-?\d+(\.\d+)?$'
              THEN (additional.value->>'units_week')::numeric END,0)
          * COALESCE(CASE WHEN COALESCE(additional.value->>'pay_rate','') ~ '^-?\d+(\.\d+)?$'
            THEN (additional.value->>'pay_rate')::numeric END,
            CASE WHEN COALESCE(additional.value->>'rate','') ~ '^-?\d+(\.\d+)?$'
              THEN (additional.value->>'rate')::numeric END,0),0),2) AS amount_ex_vat,
        jsonb_build_object('role','LIVE_COMPONENT','source_kind','ADDITIONAL',
           'projected_timesheet_id',p_projected_timesheet_id,'raw_additional_code',additional.key,
           'additional_code',UPPER(BTRIM(additional.key)),
          'source_value',additional.value) AS payload,
        CASE
          WHEN pg_column_size(COALESCE(canonical.additional_units_json,'{}'::jsonb))>65536
            THEN 'ADDITIONAL_INPUT_EXCEEDS_ENVELOPE'
          WHEN jsonb_typeof(COALESCE(canonical.additional_units_json,'{}'::jsonb))<>'object'
            THEN 'ADDITIONAL_INPUT_NOT_OBJECT'
          WHEN NULLIF(BTRIM(additional.key),'') IS NULL THEN 'ADDITIONAL_CODE_MISSING'
          WHEN jsonb_typeof(additional.value)<>'object' THEN 'ADDITIONAL_VALUE_NOT_OBJECT'
          WHEN NULLIF(COALESCE(additional.value->>'pay_ex_vat',additional.value->>'amount_ex_vat',''),'') IS NOT NULL
            AND COALESCE(additional.value->>'pay_ex_vat',additional.value->>'amount_ex_vat','') !~ '^-?\d+(\.\d+)?$'
            THEN 'ADDITIONAL_AMOUNT_INVALID'
          WHEN EXISTS(SELECT 1 FROM unnest(ARRAY['unit_count','units_week','pay_rate','rate']) field_name
            WHERE NULLIF(additional.value->>field_name,'') IS NOT NULL
              AND additional.value->>field_name !~ '^-?\d+(\.\d+)?$')
            THEN 'ADDITIONAL_RATE_INPUT_INVALID'
        END AS raw_failure
      FROM canonical
      CROSS JOIN LATERAL (
        SELECT entry.key,entry.value
        FROM jsonb_each(CASE
          WHEN pg_column_size(COALESCE(canonical.additional_units_json,'{}'::jsonb))<=65536
           AND jsonb_typeof(COALESCE(canonical.additional_units_json,'{}'::jsonb))='object'
            THEN COALESCE(canonical.additional_units_json,'{}'::jsonb)
          ELSE '{}'::jsonb
        END) entry
        WHERE p_last_source_key IS NULL OR
          '10:'||p_projected_timesheet_id::text||':ADDITIONAL:'||
            LPAD(octet_length(entry.key)::text,10,'0')||':'||
            encode(convert_to(entry.key,'UTF8'),'hex')>p_last_source_key
        ORDER BY octet_length(entry.key),encode(convert_to(entry.key,'UTF8'),'hex') LIMIT v_limit
      ) additional
    ), additional_container_failure AS (
      SELECT '10:'||p_projected_timesheet_id::text||':ADDITIONAL:!INVALID' AS source_key,
        'timesheets_financials'::text AS source_relation,canonical.financial_id AS source_id,
        NULL::text AS raw_key_type,NULL::text AS raw_key_value,0::numeric AS amount_ex_vat,
        jsonb_build_object('role','LIVE_COMPONENT','source_kind','ADDITIONAL_CONTAINER',
          'projected_timesheet_id',p_projected_timesheet_id) AS payload,
        CASE WHEN pg_column_size(COALESCE(canonical.additional_units_json,'{}'::jsonb))>65536
          THEN 'ADDITIONAL_INPUT_EXCEEDS_ENVELOPE' ELSE 'ADDITIONAL_INPUT_NOT_OBJECT' END AS raw_failure
      FROM canonical
      WHERE (jsonb_typeof(COALESCE(canonical.additional_units_json,'{}'::jsonb))<>'object'
          OR pg_column_size(COALESCE(canonical.additional_units_json,'{}'::jsonb))>65536)
        AND (p_last_source_key IS NULL OR
          '10:'||p_projected_timesheet_id::text||':ADDITIONAL:!INVALID'>p_last_source_key)
    ), expense_source AS (
      SELECT canonical.*,
        COALESCE(canonical.travel_pay_ex_vat,0)::numeric AS travel_ex,
        COALESCE(canonical.accommodation_pay_ex_vat,0)::numeric AS accommodation_ex,
        COALESCE(canonical.other_pay_ex_vat,0)::numeric AS other_ex,
        COALESCE(canonical.mileage_pay_ex_vat,0)::numeric AS mileage_ex,
        COALESCE(canonical.expenses_pay_ex_vat,0)::numeric AS expenses_ex,
        COALESCE(canonical.travel_charge_ex_vat,0)::numeric AS travel_charge_ex,
        COALESCE(canonical.accommodation_charge_ex_vat,0)::numeric AS accommodation_charge_ex,
        COALESCE(canonical.other_charge_ex_vat,0)::numeric AS other_charge_ex,
        COALESCE(canonical.mileage_charge_ex_vat,0)::numeric AS mileage_charge_ex,
        COALESCE(canonical.expenses_charge_ex_vat,0)::numeric AS expenses_charge_ex
      FROM canonical
    ), expense_rows AS (
      SELECT '10:'||p_projected_timesheet_id::text||':EXPENSE:'||expense.key_value AS source_key,
        'timesheets_financials'::text AS source_relation,expense_source.financial_id AS source_id,
        'EXPENSE_CODE'::text AS raw_key_type,expense.key_value,ROUND(expense.amount_ex_vat,2) AS amount_ex_vat,
        jsonb_build_object('role','LIVE_COMPONENT','source_kind','EXPENSE',
          'projected_timesheet_id',p_projected_timesheet_id,'expense_code',expense.key_value,
          'source_charge_ex_vat',ROUND(expense.charge_ex_vat,2)) AS payload,
        NULL::text AS raw_failure
      FROM expense_source
      CROSS JOIN LATERAL (VALUES
        ('TRAVEL'::text,expense_source.travel_ex,expense_source.travel_charge_ex),
        ('ACCOMMODATION'::text,expense_source.accommodation_ex,expense_source.accommodation_charge_ex),
        ('OTHER'::text,expense_source.other_ex,expense_source.other_charge_ex),
        ('MILEAGE'::text,expense_source.mileage_ex,expense_source.mileage_charge_ex),
        ('EXPENSES'::text,CASE WHEN expense_source.travel_ex=0 AND expense_source.accommodation_ex=0
          AND expense_source.other_ex=0 AND expense_source.mileage_ex=0
          THEN expense_source.expenses_ex ELSE 0::numeric END,
          CASE WHEN expense_source.travel_ex=0 AND expense_source.accommodation_ex=0
            AND expense_source.other_ex=0 AND expense_source.mileage_ex=0
            THEN expense_source.expenses_charge_ex ELSE 0::numeric END)
      ) expense(key_value,amount_ex_vat,charge_ex_vat)
      WHERE ROUND(COALESCE(expense.amount_ex_vat,0),2)<>0
    ), adjustment_rows AS (
      SELECT '10:'||p_projected_timesheet_id::text||':ADJUSTMENT:'||adjustment.id::text AS source_key,
        'ts_pay_adjustments'::text AS source_relation,adjustment.id AS source_id,
        'ADJUSTMENT_CODE'::text AS raw_key_type,adjustment.id::text AS raw_key_value,
        ROUND(COALESCE(adjustment.delta_pay_ex_vat,0),2) AS amount_ex_vat,
        jsonb_build_object('role','LIVE_COMPONENT','source_kind','ADJUSTMENT',
          'projected_timesheet_id',p_projected_timesheet_id,'family_timesheet_id',adjustment.timesheet_id,
          'adjustment_id',adjustment.id) AS payload,
        NULL::text AS raw_failure
      FROM projection_members member
      JOIN public.ts_pay_adjustments adjustment ON adjustment.timesheet_id=member.family_timesheet_id
      WHERE adjustment.as_advance IS FALSE AND adjustment.id IS NOT NULL
        AND ROUND(COALESCE(adjustment.delta_pay_ex_vat,0),2)<>0
        AND (p_last_source_key IS NULL OR
          '10:'||p_projected_timesheet_id::text||':ADJUSTMENT:'||adjustment.id::text>p_last_source_key)
      ORDER BY adjustment.id LIMIT v_limit
    ), raw_rows AS (
      SELECT * FROM segment_rows
      UNION ALL SELECT additional.* FROM additional_rows additional
        WHERE additional.amount_ex_vat<>0 OR additional.raw_failure IS NOT NULL
      UNION ALL SELECT * FROM additional_container_failure
      UNION ALL SELECT * FROM expense_rows
      UNION ALL SELECT * FROM adjustment_rows
    ), paged_raw_rows AS MATERIALIZED (
      SELECT raw.* FROM raw_rows raw
      WHERE p_last_source_key IS NULL OR raw.source_key>p_last_source_key
      ORDER BY raw.source_key
      -- One extra parent beyond the row look-ahead is required because the
      -- resume cursor can point inside the final component of the prior item.
      -- Without it, a 25-row page can falsely report source exhaustion at an
      -- item boundary (for example, exactly 50 or 51 physical occurrences).
      LIMIT v_limit+1
    ), resolved AS (
      SELECT raw.*,
        resolved_key.key_type,resolved_key.key_value,
        resolved_key.key_resolution_source,resolved_key.key_resolution_failure_reason
      FROM paged_raw_rows raw
      LEFT JOIN LATERAL public._pay_policy_x_resolve_pre_draft_economic_key(
        p_timesheet_id=>p_projected_timesheet_id,
        p_live_source_json=>raw.payload||jsonb_build_object(
          'timesheet_id',p_projected_timesheet_id,'component_key_type',raw.raw_key_type,
          'component_key_value',raw.raw_key_value),
        p_item_type=>CASE raw.raw_key_type WHEN 'TS_DAY' THEN 'SEGMENT_DELTA'
          WHEN 'TS_TOTAL' THEN 'SEGMENT_DELTA' WHEN 'ADJUSTMENT_CODE' THEN 'ADJUSTMENT_DELTA'
          WHEN 'ADDITIONAL_CODE' THEN 'EXPENSE_DELTA' WHEN 'EXPENSE_CODE' THEN
            CASE WHEN raw.raw_key_value='MILEAGE' THEN 'MILEAGE_DELTA' ELSE 'EXPENSE_DELTA' END END,
        p_key_type_hint=>raw.raw_key_type,p_key_value_hint=>raw.raw_key_value,
        p_work_date=>NULL::date
      ) resolved_key ON raw.raw_failure IS NULL
    ), rate_inputs AS MATERIALIZED (
      SELECT resolved.*,canonical.*,
        CASE resolved.payload->>'source_kind'
          WHEN 'SEGMENT' THEN 'WORKED_TIME'
          WHEN 'ADDITIONAL' THEN 'ADDITIONAL_UNIT'
          WHEN 'EXPENSE' THEN 'EXPENSE'
          WHEN 'ADJUSTMENT' THEN 'ADJUSTMENT'
          ELSE 'UNKNOWN'
        END AS component_kind,
        COALESCE(
          NULLIF(BTRIM(resolved.payload#>>'{segment,segment_stable_key}'),''),
          NULLIF(BTRIM(resolved.payload#>>'{segment,segment_id}'),''),
          NULLIF(BTRIM(resolved.payload#>>'{segment,segment_key}'),''),
          NULLIF(BTRIM(resolved.payload#>>'{segment,date}'),''),
          NULLIF(BTRIM(resolved.payload#>>'{segment,ref_num}'),''),
          'segment-fallback:'||md5(jsonb_build_object(
            'timesheet_id',p_projected_timesheet_id::text,
            'source_kind',resolved.payload->>'source_kind',
            'source_ordinal',resolved.payload->'source_ordinal',
            'date',NULLIF(BTRIM(resolved.payload#>>'{segment,date}'),''),
            'start_utc',NULLIF(BTRIM(resolved.payload#>>'{segment,start_utc}'),''),
            'end_utc',NULLIF(BTRIM(resolved.payload#>>'{segment,end_utc}'),''),
            'start',NULLIF(BTRIM(resolved.payload#>>'{segment,start}'),''),
            'end',NULLIF(BTRIM(resolved.payload#>>'{segment,end}'),''),
            'ref_num',NULLIF(BTRIM(resolved.payload#>>'{segment,ref_num}'),''),
            'shift_id',COALESCE(NULLIF(BTRIM(resolved.payload#>>'{segment,shift_id}'),''),
              NULLIF(BTRIM(resolved.payload#>>'{segment,nhsp_shift_id}'),'')),
            'request_id',COALESCE(NULLIF(BTRIM(resolved.payload#>>'{segment,request_id}'),''),
              NULLIF(BTRIM(resolved.payload#>>'{segment,hr_request_id}'),'')),
            'external_row_key',NULLIF(BTRIM(resolved.payload#>>'{segment,external_row_key}'),''),
            'latest_import_id',NULLIF(BTRIM(resolved.payload#>>'{segment,latest_import_id}'),'')
          )::text)
        ) AS segment_stable_key,
        CASE resolved.payload->>'source_kind'
          WHEN 'SEGMENT' THEN COALESCE(
            NULLIF(BTRIM(resolved.payload#>>'{segment,segment_stable_key}'),''),
            NULLIF(BTRIM(resolved.payload#>>'{segment,segment_id}'),''),
            NULLIF(BTRIM(resolved.payload#>>'{segment,segment_key}'),''),
            NULLIF(BTRIM(resolved.payload#>>'{segment,date}'),''),
            NULLIF(BTRIM(resolved.payload#>>'{segment,ref_num}'),''),
            'segment-fallback:'||md5(jsonb_build_object(
              'timesheet_id',p_projected_timesheet_id::text,
              'source_kind',resolved.payload->>'source_kind',
              'source_ordinal',resolved.payload->'source_ordinal',
              'date',NULLIF(BTRIM(resolved.payload#>>'{segment,date}'),''),
              'start_utc',NULLIF(BTRIM(resolved.payload#>>'{segment,start_utc}'),''),
              'end_utc',NULLIF(BTRIM(resolved.payload#>>'{segment,end_utc}'),''),
              'start',NULLIF(BTRIM(resolved.payload#>>'{segment,start}'),''),
              'end',NULLIF(BTRIM(resolved.payload#>>'{segment,end}'),''),
              'ref_num',NULLIF(BTRIM(resolved.payload#>>'{segment,ref_num}'),''),
              'shift_id',COALESCE(NULLIF(BTRIM(resolved.payload#>>'{segment,shift_id}'),''),
                NULLIF(BTRIM(resolved.payload#>>'{segment,nhsp_shift_id}'),'')),
              'request_id',COALESCE(NULLIF(BTRIM(resolved.payload#>>'{segment,request_id}'),''),
                NULLIF(BTRIM(resolved.payload#>>'{segment,hr_request_id}'),'')),
              'external_row_key',NULLIF(BTRIM(resolved.payload#>>'{segment,external_row_key}'),''),
              'latest_import_id',NULLIF(BTRIM(resolved.payload#>>'{segment,latest_import_id}'),'')
            )::text)
          )
          WHEN 'ADDITIONAL' THEN 'additional:'||UPPER(BTRIM(resolved.payload->>'additional_code'))
          WHEN 'EXPENSE' THEN 'expense:'||UPPER(BTRIM(resolved.payload->>'expense_code'))
          WHEN 'ADJUSTMENT' THEN 'adjustment:'||BTRIM(resolved.payload->>'adjustment_id')
        END AS component_member_identity,
        CASE resolved.payload->>'source_kind'
          WHEN 'SEGMENT' THEN ROUND(COALESCE(
            CASE WHEN COALESCE(resolved.payload#>>'{segment,charge_amount}','') ~ '^-?\d+(\.\d+)?$'
              THEN (resolved.payload#>>'{segment,charge_amount}')::numeric END,
            CASE WHEN COALESCE(resolved.payload#>>'{segment,charge_ex_vat}','') ~ '^-?\d+(\.\d+)?$'
              THEN (resolved.payload#>>'{segment,charge_ex_vat}')::numeric END),2)
          WHEN 'ADDITIONAL' THEN ROUND(COALESCE(
            CASE WHEN COALESCE(resolved.payload#>>'{source_value,charge_ex_vat}','') ~ '^-?\d+(\.\d+)?$'
              THEN (resolved.payload#>>'{source_value,charge_ex_vat}')::numeric END,
            CASE WHEN COALESCE(resolved.payload#>>'{source_value,charge_amount_ex_vat}','') ~ '^-?\d+(\.\d+)?$'
              THEN (resolved.payload#>>'{source_value,charge_amount_ex_vat}')::numeric END),2)
          WHEN 'EXPENSE' THEN ROUND(CASE WHEN COALESCE(resolved.payload->>'source_charge_ex_vat','')
              ~ '^-?\d+(\.\d+)?$' THEN (resolved.payload->>'source_charge_ex_vat')::numeric END,2)
        END AS parent_source_charge_ex_vat,
        md5(jsonb_build_object(
          'financial_revision_version',1,'timesheet_id',p_projected_timesheet_id::text,
          'timesheet_version',canonical.version,
          'timesheet_updated_at_utc',to_char(canonical.updated_at AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
          'financial_row_id',canonical.financial_id::text,
          'financial_timesheet_version',canonical.financial_timesheet_version,
          'financial_computed_at_utc',to_char(canonical.financial_computed_at_utc AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
          'financial_updated_at_utc',to_char(canonical.financial_updated_at AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
          'financial_is_current',canonical.current_financial_count=1,
          'source_pay_method',canonical.source_pay_method)::text) AS financial_revision_digest,
        md5(jsonb_build_object(
          'target_authority_version',1,'candidate_id',canonical.authority_candidate_id::text,
          'target_pay_method',canonical.target_pay_method,'candidate_rev',canonical.candidate_rev,
          'candidate_updated_at_utc',to_char(canonical.candidate_updated_at AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
          'umbrella_id',canonical.umbrella_id::text,
          'umbrella_enabled',canonical.umbrella_enabled,
          'umbrella_vat_chargeable',canonical.umbrella_vat_chargeable,
          'umbrella_updated_at_utc',to_char(canonical.umbrella_updated_at AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'))::text) AS target_authority_digest,
        md5(jsonb_build_object(
          'conversion_context_version',1,'pay_date',canonical.pay_date::text,
          'finance_window_id',canonical.finance_window_id::text,
          'date_from',canonical.finance_window_date_from::text,
          'date_to',canonical.finance_window_date_to::text,
          'erni_pct',ROUND(canonical.erni_pct,6),'vat_rate_pct',ROUND(canonical.vat_rate_pct,6),
          'finance_window_updated_at_utc',to_char(canonical.finance_window_updated_at AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
          'target_pay_method',canonical.target_pay_method,
          'umbrella_vat_chargeable',canonical.umbrella_vat_chargeable)::text)
          AS conversion_context_digest
      FROM resolved CROSS JOIN canonical
    ), physical_rows AS MATERIALIZED (
      SELECT input.source_key,input.component_kind,input.component_member_identity,
        NULLIF(BTRIM(input.payload#>>'{segment,segment_id}'),'') AS segment_id,
        COALESCE(
          NULLIF(BTRIM(input.payload#>>'{segment,segment_key}'),''),
          NULLIF(BTRIM(input.payload#>>'{segment,segment_id}'),'')
        ) AS segment_key,
        CASE WHEN input.component_kind='WORKED_TIME' THEN input.segment_stable_key END
          AS segment_stable_key,
        NULLIF(BTRIM(input.payload#>>'{segment,date}'),'') AS work_date,
        NULLIF(BTRIM(input.payload#>>'{segment,ref_num}'),'') AS ref_num,
        CASE WHEN input.component_kind='ADDITIONAL_UNIT'
          THEN UPPER(NULLIF(BTRIM(input.payload->>'additional_code'),'')) END AS additional_code,
        CASE WHEN input.component_kind='EXPENSE'
          THEN UPPER(NULLIF(BTRIM(input.payload->>'expense_code'),'')) END AS expense_code,
        CASE WHEN input.component_kind='ADJUSTMENT'
          THEN NULLIF(BTRIM(input.payload->>'adjustment_id'),'') END AS adjustment_id,
        bucket.bucket_code,bucket.bucket_sort_ordinal,
        CASE WHEN bucket.source_units IS NULL THEN NULL ELSE ROUND(bucket.source_units,6) END
          AS source_units,
        CASE WHEN bucket.source_rate IS NULL THEN NULL ELSE ROUND(bucket.source_rate,6) END
          AS source_rate,
        CASE WHEN bucket.source_charge_rate IS NULL THEN NULL
          ELSE ROUND(bucket.source_charge_rate,6) END AS source_charge_rate,
        ROUND(bucket.source_pay_ex_vat,2) AS source_pay_ex_vat,
        CASE WHEN bucket.source_charge_ex_vat IS NULL THEN NULL
          ELSE ROUND(bucket.source_charge_ex_vat,2) END AS source_charge_ex_vat,
        bucket.is_rate_bearing
      FROM rate_inputs input
      CROSS JOIN LATERAL (
        SELECT worked.bucket_code,worked.bucket_sort_ordinal,worked.source_units,
          worked.source_rate,worked.source_charge_rate,
          worked.source_units*worked.source_rate AS source_pay_ex_vat,
          worked.source_units*worked.source_charge_rate AS source_charge_ex_vat,true AS is_rate_bearing
        FROM (VALUES
          ('DAY'::text,1,
            CASE WHEN COALESCE(input.payload#>>'{segment,hours_day}','') ~ '^-?\d+(\.\d+)?$'
              THEN (input.payload#>>'{segment,hours_day}')::numeric END,input.pay_day,input.charge_day),
          ('NIGHT'::text,2,
            CASE WHEN COALESCE(input.payload#>>'{segment,hours_night}','') ~ '^-?\d+(\.\d+)?$'
              THEN (input.payload#>>'{segment,hours_night}')::numeric END,input.pay_night,input.charge_night),
          ('SAT'::text,3,
            CASE WHEN COALESCE(input.payload#>>'{segment,hours_sat}','') ~ '^-?\d+(\.\d+)?$'
              THEN (input.payload#>>'{segment,hours_sat}')::numeric END,input.pay_sat,input.charge_sat),
          ('SUN'::text,4,
            CASE WHEN COALESCE(input.payload#>>'{segment,hours_sun}','') ~ '^-?\d+(\.\d+)?$'
              THEN (input.payload#>>'{segment,hours_sun}')::numeric END,input.pay_sun,input.charge_sun),
          ('BH'::text,5,
            CASE WHEN COALESCE(input.payload#>>'{segment,hours_bh}','') ~ '^-?\d+(\.\d+)?$'
              THEN (input.payload#>>'{segment,hours_bh}')::numeric END,input.pay_bh,input.charge_bh)
        ) worked(bucket_code,bucket_sort_ordinal,source_units,source_rate,source_charge_rate)
        WHERE input.component_kind='WORKED_TIME' AND COALESCE(worked.source_units,0)<>0
        UNION ALL
        SELECT 'ADDITIONAL',6,
          COALESCE(
            CASE WHEN COALESCE(input.payload#>>'{source_value,unit_count}','') ~ '^-?\d+(\.\d+)?$'
              THEN (input.payload#>>'{source_value,unit_count}')::numeric END,
            CASE WHEN COALESCE(input.payload#>>'{source_value,units_week}','') ~ '^-?\d+(\.\d+)?$'
              THEN (input.payload#>>'{source_value,units_week}')::numeric END),
          COALESCE(
            CASE WHEN COALESCE(input.payload#>>'{source_value,pay_rate}','') ~ '^-?\d+(\.\d+)?$'
              THEN (input.payload#>>'{source_value,pay_rate}')::numeric END,
            CASE WHEN COALESCE(input.payload#>>'{source_value,rate}','') ~ '^-?\d+(\.\d+)?$'
              THEN (input.payload#>>'{source_value,rate}')::numeric END),
          CASE WHEN COALESCE(input.payload#>>'{source_value,charge_rate}','') ~ '^-?\d+(\.\d+)?$'
            THEN (input.payload#>>'{source_value,charge_rate}')::numeric END,
          input.amount_ex_vat,input.parent_source_charge_ex_vat,true
        WHERE input.component_kind='ADDITIONAL_UNIT'
        UNION ALL
        SELECT 'FIXED',7,NULL::numeric,NULL::numeric,NULL::numeric,
          input.amount_ex_vat,input.parent_source_charge_ex_vat,false
        WHERE input.component_kind IN ('EXPENSE','ADJUSTMENT')
      ) bucket
    ), physical_keyed AS MATERIALIZED (
      SELECT physical.*,
        concat_ws('|','RATE_BUCKET_V1',p_projected_timesheet_id::text,
          'timesheet:'||p_projected_timesheet_id::text,input.key_type,input.key_value,
          physical.component_member_identity,physical.bucket_code) AS physical_bucket_key,
        md5(jsonb_build_object(
          'builder_comparison_version',1,'timesheet_id',p_projected_timesheet_id::text,
          'source_family_key','timesheet:'||p_projected_timesheet_id::text,
          'component_key_type',UPPER(BTRIM(input.key_type)),
          'component_key_value',BTRIM(input.key_value),
          'segment_id',physical.segment_id,'segment_key',physical.segment_key,
          'segment_stable_key',physical.segment_stable_key,'work_date',physical.work_date,
          'ref_num',physical.ref_num,'additional_code',physical.additional_code,
          'expense_code',physical.expense_code,'adjustment_id',physical.adjustment_id,
          'bucket_code',physical.bucket_code,'source_units',physical.source_units,
          'source_rate',physical.source_rate,'source_charge_rate',physical.source_charge_rate,
          'source_pay_ex_vat',physical.source_pay_ex_vat,
          'source_charge_ex_vat',physical.source_charge_ex_vat,
          'source_pay_method',input.source_pay_method,
          'target_pay_method',input.target_pay_method)::text) AS builder_comparison_digest
      FROM physical_rows physical
      JOIN rate_inputs input USING(source_key)
    ), physical_digested AS MATERIALIZED (
      SELECT keyed.*,
        jsonb_build_object(
          'physical_bucket_version',1,'physical_bucket_key',keyed.physical_bucket_key,
          'component_kind',keyed.component_kind,
          'component_member_identity',keyed.component_member_identity,
          'bucket_code',keyed.bucket_code,'source_units',keyed.source_units,
          'source_rate',keyed.source_rate,'source_charge_rate',keyed.source_charge_rate,
          'source_pay_ex_vat',keyed.source_pay_ex_vat,
          'source_charge_ex_vat',keyed.source_charge_ex_vat,
          -- Baseline and reservation are separate sealed finance authority.
          -- They are attributed to this physical source only after all build
          -- fact families have been persisted; do not invent a live zero here.
          'baseline_source_pay_ex_vat',NULL::numeric,
          'reserved_source_pay_ex_vat',NULL::numeric,
          'outstanding_source_pay_ex_vat',NULL::numeric,
          'source_pay_method',input.source_pay_method,
          'target_pay_method',input.target_pay_method) AS physical_canonical_json
      FROM physical_keyed keyed JOIN rate_inputs input USING(source_key)
    ), rate_documents_pre AS MATERIALIZED (
      SELECT input.*,bucket.physical_buckets,bucket.physical_bucket_digest,
        bucket.physical_pay_total,bucket.physical_charge_total,
        CASE
          WHEN input.authority_session_id IS NULL THEN 'RATE_AUTHORITY_SESSION_NOT_FOUND'
          WHEN input.authority_session_version IS DISTINCT FROM input.build_session_version
            THEN 'RATE_AUTHORITY_SESSION_VERSION_MISMATCH'
          WHEN input.authority_candidate_id IS NULL THEN 'RATE_AUTHORITY_CANDIDATE_NOT_FOUND'
          WHEN input.financial_candidate_id IS DISTINCT FROM input.build_candidate_id
            THEN 'RATE_AUTHORITY_CANDIDATE_MISMATCH'
          WHEN COALESCE(input.current_financial_count,0)=0
            THEN 'RATE_AUTHORITY_FINANCIAL_ROW_NOT_FOUND'
          WHEN input.current_financial_count<>1 THEN 'RATE_AUTHORITY_FINANCIAL_ROW_AMBIGUOUS'
          WHEN input.financial_timesheet_version IS DISTINCT FROM input.version
            THEN 'RATE_AUTHORITY_FINANCIAL_VERSION_MISMATCH'
          WHEN input.source_pay_method IS NULL
            OR input.source_pay_method NOT IN ('PAYE','UMBRELLA')
            THEN 'RATE_AUTHORITY_SOURCE_PAY_METHOD_MISSING'
          WHEN input.target_pay_method IS NULL
            OR input.target_pay_method NOT IN ('PAYE','UMBRELLA')
            THEN 'RATE_AUTHORITY_TARGET_PAY_METHOD_MISSING'
          WHEN input.target_pay_method='UMBRELLA' AND input.umbrella_id IS NULL
            THEN 'RATE_AUTHORITY_UMBRELLA_REQUIRED'
          WHEN input.target_pay_method='UMBRELLA' AND input.umbrella_enabled IS NULL
            THEN 'RATE_AUTHORITY_UMBRELLA_NOT_FOUND'
          WHEN input.target_pay_method='UMBRELLA' AND input.umbrella_enabled IS DISTINCT FROM true
            THEN 'RATE_AUTHORITY_UMBRELLA_DISABLED'
          WHEN input.finance_window_id IS NULL THEN 'RATE_AUTHORITY_FINANCE_WINDOW_NOT_FOUND'
          WHEN input.finance_window_winning_date_match_count<>1
            THEN 'RATE_AUTHORITY_FINANCE_WINDOW_AMBIGUOUS'
          WHEN input.component_kind IN ('WORKED_TIME','ADDITIONAL_UNIT')
            AND input.source_pay_method IS DISTINCT FROM input.target_pay_method
            AND NOT EXISTS(SELECT 1 FROM physical_digested p
              WHERE p.source_key=input.source_key AND COALESCE(p.source_units,0)<>0)
            THEN 'RATE_AUTHORITY_UNITS_INVALID'
          WHEN input.component_kind IN ('WORKED_TIME','ADDITIONAL_UNIT')
            AND input.source_pay_method IS DISTINCT FROM input.target_pay_method
            AND EXISTS(SELECT 1 FROM physical_digested p
              WHERE p.source_key=input.source_key AND p.source_rate IS NULL)
            THEN 'RATE_AUTHORITY_SOURCE_PAY_RATE_MISSING'
          WHEN input.component_kind IN ('WORKED_TIME','ADDITIONAL_UNIT')
            AND input.source_pay_method IS DISTINCT FROM input.target_pay_method
            AND EXISTS(SELECT 1 FROM physical_digested p
              WHERE p.source_key=input.source_key AND p.source_charge_rate IS NULL)
            THEN 'RATE_AUTHORITY_SOURCE_CHARGE_RATE_MISSING'
          WHEN input.component_kind IN ('WORKED_TIME','ADDITIONAL_UNIT')
            AND ROUND(COALESCE(bucket.physical_pay_total,0),2)
              IS DISTINCT FROM ROUND(COALESCE(input.amount_ex_vat,0),2)
            THEN 'RATE_AUTHORITY_PARENT_PAY_MISMATCH'
          WHEN input.component_kind IN ('WORKED_TIME','ADDITIONAL_UNIT')
            AND input.parent_source_charge_ex_vat IS NOT NULL
            AND ROUND(COALESCE(bucket.physical_charge_total,0),2)
              IS DISTINCT FROM ROUND(input.parent_source_charge_ex_vat,2)
            THEN 'RATE_AUTHORITY_PARENT_CHARGE_MISMATCH'
        END AS rate_authority_failure_code
      FROM rate_inputs input
      LEFT JOIN LATERAL (
        SELECT COALESCE(jsonb_agg(
            jsonb_build_object(
              'physical_bucket_key',physical.physical_bucket_key,
              'component_kind',physical.component_kind,
              'component_member_identity',physical.component_member_identity,
              'segment_id',physical.segment_id,'segment_key',physical.segment_key,
              'segment_stable_key',physical.segment_stable_key,'work_date',physical.work_date,
              'ref_num',physical.ref_num,'additional_code',physical.additional_code,
              'expense_code',physical.expense_code,'adjustment_id',physical.adjustment_id,
              'bucket_code',physical.bucket_code,
              'bucket_sort_ordinal',physical.bucket_sort_ordinal,
              'source_units',physical.source_units,'source_rate',physical.source_rate,
              'source_charge_rate',physical.source_charge_rate,
              'source_pay_ex_vat',physical.source_pay_ex_vat,
              'source_charge_ex_vat',physical.source_charge_ex_vat,
              'baseline_source_pay_ex_vat',NULL::numeric,
              'reserved_source_pay_ex_vat',NULL::numeric,
              'outstanding_source_pay_ex_vat',NULL::numeric,
              'is_rate_bearing',physical.is_rate_bearing,
              'is_actionable_candidate',physical.is_rate_bearing
                AND input.source_pay_method IS DISTINCT FROM input.target_pay_method,
              'builder_comparison_digest',physical.builder_comparison_digest,
              'physical_bucket_digest',md5(physical.physical_canonical_json::text))
            ORDER BY physical.bucket_sort_ordinal,physical.physical_bucket_key),'[]'::jsonb)
            AS physical_buckets,
          md5(COALESCE(jsonb_agg(physical.physical_canonical_json
            ORDER BY physical.bucket_sort_ordinal,physical.physical_bucket_key)::text,'[]'))
            AS physical_bucket_digest,
          ROUND(COALESCE(SUM(physical.source_pay_ex_vat),0),2) AS physical_pay_total,
          ROUND(COALESCE(SUM(COALESCE(physical.source_charge_ex_vat,0)),0),2)
            AS physical_charge_total
        FROM physical_digested physical WHERE physical.source_key=input.source_key
      ) bucket ON true
    ), rate_documents AS MATERIALIZED (
      SELECT prepared.*,
        md5(jsonb_build_object(
          'sealed_evidence_version',1,
          'financial_revision_digest',prepared.financial_revision_digest,
          'target_authority_digest',prepared.target_authority_digest,
          'conversion_context_digest',prepared.conversion_context_digest,
          'physical_bucket_digest',prepared.physical_bucket_digest,
          'economic_key_type',prepared.key_type,'economic_key_value',prepared.key_value,
          'parent_source_pay_ex_vat',ROUND(prepared.amount_ex_vat,2),
          'parent_source_charge_ex_vat',ROUND(prepared.parent_source_charge_ex_vat,2),
          'source_pay_method',prepared.source_pay_method,
          'target_pay_method',prepared.target_pay_method)::text) AS sealed_evidence_digest
      FROM rate_documents_pre prepared
    ), completed AS MATERIALIZED (
      SELECT document.*,
        COALESCE(document.raw_failure,document.key_resolution_failure_reason,
          CASE WHEN document.key_type IS NULL OR document.key_value IS NULL
            THEN 'ECONOMIC_KEY_MISSING' END,
          document.rate_authority_failure_code) AS final_resolution_failure,
        jsonb_build_object(
          'rate_authority_version',1,
          'authority_state',CASE
            WHEN document.rate_authority_failure_code IS NOT NULL THEN 'FAILED'
            WHEN document.component_kind IN ('EXPENSE','ADJUSTMENT')
              OR document.source_pay_method IS NOT DISTINCT FROM document.target_pay_method
              THEN 'FIXED_NO_RATE' ELSE 'COMPLETE' END,
          'failure_code',document.rate_authority_failure_code,
          'failure_detail',CASE WHEN document.rate_authority_failure_code IS NULL THEN NULL
            ELSE jsonb_build_object('code',document.rate_authority_failure_code,
              'timesheet_id',p_projected_timesheet_id::text,'source_key',document.source_key) END,
          'sealed_evidence_digest',document.sealed_evidence_digest,
          'build',jsonb_build_object(
            'build_id',p_build_id::text,'candidate_id',document.build_candidate_id::text,
            'session_id',document.build_session_id::text,
            'session_version',document.build_session_version,
            'captured_candidate_generation',document.captured_candidate_generation,
            'source_change_seq',document.source_change_seq,
            'authority_fingerprint_version',document.authority_fingerprint_version,
            'authority_fingerprint',document.authority_fingerprint,
            'pay_date',document.pay_date::text,
            'week_ending_cutoff',document.week_ending_cutoff::text),
          'source',jsonb_build_object(
            'timesheet_id',p_projected_timesheet_id::text,
            'timesheet_version',document.version,
            'timesheet_updated_at_utc',to_char(document.updated_at AT TIME ZONE 'UTC',
              'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
            'financial_row_id',document.financial_id::text,
            'financial_timesheet_version',document.financial_timesheet_version,
            'financial_computed_at_utc',to_char(document.financial_computed_at_utc AT TIME ZONE 'UTC',
              'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
            'financial_updated_at_utc',to_char(document.financial_updated_at AT TIME ZONE 'UTC',
              'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
            'candidate_id',document.financial_candidate_id::text,
            'client_id',document.financial_client_id::text,
            'contract_id',document.contract_id::text,
            'booking_id',NULLIF(BTRIM(document.booking_id),''),
            'source_pay_method',document.source_pay_method,
            'financial_revision_digest',document.financial_revision_digest),
          'target',jsonb_build_object(
            'candidate_id',document.authority_candidate_id::text,
            'target_pay_method',document.target_pay_method,
            'candidate_rev',document.candidate_rev,
            'candidate_updated_at_utc',to_char(document.candidate_updated_at AT TIME ZONE 'UTC',
              'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
            'umbrella_id',document.umbrella_id::text,
            'umbrella_enabled',document.umbrella_enabled,
            'umbrella_vat_chargeable',document.umbrella_vat_chargeable,
            'umbrella_updated_at_utc',to_char(document.umbrella_updated_at AT TIME ZONE 'UTC',
              'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
            'target_authority_digest',document.target_authority_digest),
          'conversion',jsonb_build_object(
            'finance_window_id',document.finance_window_id::text,
            'date_from',document.finance_window_date_from::text,
            'date_to',document.finance_window_date_to::text,
            'erni_pct',ROUND(document.erni_pct,6),'vat_rate_pct',ROUND(document.vat_rate_pct,6),
            'finance_window_updated_at_utc',to_char(document.finance_window_updated_at AT TIME ZONE 'UTC',
              'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
            'conversion_context_digest',document.conversion_context_digest),
          'economic',jsonb_build_object(
            'source_family_key','timesheet:'||p_projected_timesheet_id::text,
            'economic_key_type',document.key_type,'economic_key_value',document.key_value,
            'component_kind',document.component_kind,
            'parent_source_pay_ex_vat',ROUND(document.amount_ex_vat,2),
            'parent_source_charge_ex_vat',ROUND(document.parent_source_charge_ex_vat,2),
            'truth_ex_vat',ROUND(document.amount_ex_vat,2),'baseline_ex_vat',NULL::numeric,
            'reserved_ex_vat',NULL::numeric,'outstanding_ex_vat',NULL::numeric,
            'physical_bucket_digest',document.physical_bucket_digest),
          'physical_buckets',document.physical_buckets,
          'lineage',jsonb_build_object(
            'source_kind',document.payload->>'source_kind',
            'source_ordinal',document.payload->'source_ordinal',
            'booking_id',NULLIF(BTRIM(document.booking_id),''),
            'source_system',document.payload#>>'{segment,source_system}',
            'shift_id',COALESCE(document.payload#>>'{segment,shift_id}',
              document.payload#>>'{segment,nhsp_shift_id}'),
            'request_id',COALESCE(document.payload#>>'{segment,request_id}',
              document.payload#>>'{segment,hr_request_id}'),
            'external_row_key',document.payload#>>'{segment,external_row_key}',
            'latest_import_id',document.payload#>>'{segment,latest_import_id}',
            'correction_root_id',document.payload#>>'{segment,correction_root_id}',
            'correction_member_id',document.payload#>>'{segment,correction_member_id}',
            'parent_timesheet_id',document.parent_timesheet_id::text,
            'build_id',p_build_id::text,
            'source_build_run_id',document.source_build_run_id::text,
            'source_job_id',document.source_job_id::text)
        ) AS rate_authority_json
      FROM rate_documents document
    )
    SELECT completed.source_key,md5('LIVE:'||completed.source_key),p_projected_timesheet_id,
      ARRAY[p_projected_timesheet_id],completed.source_relation,completed.source_id,
      completed.key_type,completed.key_value,NULL::numeric,NULL::numeric,
      completed.amount_ex_vat,completed.amount_ex_vat,NULL::numeric,NULL::numeric,
      completed.payload||jsonb_build_object('source_key',completed.source_key,
        'raw_key_type',completed.raw_key_type,'raw_key_value',completed.raw_key_value,
        'key_resolution_source',completed.key_resolution_source,
        'resolution_failure',completed.final_resolution_failure,
        'rate_authority',completed.rate_authority_json),
      md5(jsonb_build_object('financial_digest_version',2,
        'source_key',completed.source_key,'timesheet_id',p_projected_timesheet_id::text,
        'economic_key_type',completed.key_type,'economic_key_value',completed.key_value,
        'truth_ex_vat',ROUND(completed.amount_ex_vat,2),
        'truth_inc_vat',ROUND(completed.amount_ex_vat,2),
        'baseline_ex_vat',0::numeric,'baseline_inc_vat',0::numeric,
        'financial_revision_digest',completed.financial_revision_digest,
        'target_authority_digest',completed.target_authority_digest,
        'conversion_context_digest',completed.conversion_context_digest,
        'physical_bucket_digest',completed.physical_bucket_digest,
        'resolution_failure',completed.final_resolution_failure)::text),
      completed.final_resolution_failure
    FROM completed
    WHERE p_last_source_key IS NULL OR completed.source_key>p_last_source_key
    ORDER BY completed.source_key
    LIMIT v_limit;
    RETURN;
  END IF;

  IF v_family='FROZEN_SETTLED_COMPONENT' THEN
    RETURN QUERY
    WITH projection_members AS (
      SELECT DISTINCT projection.source_id AS family_timesheet_id
      FROM private.banking_pay_workbench_economic_build_facts projection
      WHERE projection.build_id=p_build_id AND projection.fact_family='LIVE_ENTITLEMENT_INPUT'
        AND projection.dependency_unit_key=p_dependency_unit_key
        AND projection.source_relation='UNIT_PROJECTION'
        AND projection.timesheet_id=p_projected_timesheet_id
        AND projection.source_id IS NOT NULL
    ), standard_items AS (
      SELECT item.id,item.timesheet_id,item.item_type,to_jsonb(item) AS payload
      FROM projection_members member
      JOIN public.pay_batch_items item ON item.timesheet_id=member.family_timesheet_id
      JOIN public.pay_batch_candidates candidate ON candidate.id=item.pay_batch_candidate_id
      LEFT JOIN public.pay_bank_transfers transfer ON transfer.id=item.pay_bank_transfer_id
      WHERE COALESCE(item.is_voided,false) IS FALSE
        AND UPPER(BTRIM(COALESCE(item.item_type,''))) IN
          ('SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA')
        AND (UPPER(BTRIM(COALESCE(candidate.settlement_status,'')))='SETTLED'
          OR candidate.settled_at_utc IS NOT NULL
          OR UPPER(BTRIM(COALESCE(transfer.status,'')))='COMPLETED'
          OR transfer.completed_at_utc IS NOT NULL
          OR EXISTS(SELECT 1 FROM public.timesheet_pay_state_history history
            WHERE history.pay_batch_id=candidate.pay_batch_id AND history.timesheet_id=item.timesheet_id))
        AND NOT EXISTS(SELECT 1 FROM public.pay_payment_correction_items correction
          WHERE correction.pay_batch_item_id=item.id AND correction.status='APPLIED'
            AND correction.correction_item_kind IN ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL'))
    ), standard_items_page AS MATERIALIZED (
      SELECT item.* FROM standard_items item
      WHERE p_last_source_key IS NULL OR
        '20:'||p_projected_timesheet_id::text||':'||item.id::text||':99999999'>p_last_source_key
      ORDER BY item.id
      -- Keep the current parent plus one parent beyond the page look-ahead so
      -- that an exact 25-row component boundary cannot look terminal.
      LIMIT v_limit+1
    ), standard_components AS (
      SELECT item.id AS pay_batch_item_id,item.timesheet_id AS source_timesheet_id,
        item.payload,economic.item_type,economic.key_type,economic.key_value,
        economic.source_amount_ex_vat,economic.source_amount_inc_vat,
        economic.key_resolution_source,economic.key_resolution_failure_reason,
        ROW_NUMBER() OVER(PARTITION BY item.id ORDER BY economic.key_type,economic.key_value,
          economic.item_type,economic.source_amount_ex_vat,economic.source_amount_inc_vat) AS occurrence_ordinal
      FROM standard_items_page item
      LEFT JOIN LATERAL public._pay_batch_item_economic_components(
        p_pay_batch_id=>NULL::uuid,p_pay_batch_item_ids=>ARRAY[item.id]) economic ON true
    ), /* Version 1.2.8 deliberately retires the candidate-global finance-item
          discovery branch below. Finance movement authority is captured once,
          physically paged, in the GLOBAL FINANCE_ITEM_AUTHORITY stream before
          unit derivation. Keeping the superseded text inside this comment for
          one repeatable revision makes the financial formula provenance
          inspectable without leaving it in the executable query plan.
       legacy_finance_components_v127 AS (
      SELECT item.id AS pay_batch_item_id,owner_resolution.timesheet_id AS source_timesheet_id,
        to_jsonb(item)||jsonb_build_object(
          'resolved_owner_timesheet_id',owner_resolution.timesheet_id,
          'finance_case_id',COALESCE(item.finance_case_id,component.finance_case_id),
          'finance_component_id',item.finance_component_id,
          'owner_timesheet_ids',owner_resolution.owner_ids,
          'owner_candidate_ids',owner_resolution.candidate_ids,
          'owner_finance_case_ids',owner_resolution.case_ids,
          'resolution_failure',CASE
            WHEN COALESCE(cardinality(owner_resolution.owner_ids),0)=0
              THEN 'FROZEN_FINANCE_OWNER_UNRESOLVED'
            WHEN cardinality(owner_resolution.owner_ids)>1
              THEN 'FROZEN_FINANCE_OWNER_CONFLICT'
            WHEN cardinality(owner_resolution.case_ids)>1
              THEN 'FROZEN_FINANCE_CASE_CONFLICT'
            WHEN cardinality(owner_resolution.candidate_ids)<>1
              OR owner_resolution.candidate_ids[1] IS DISTINCT FROM build_row.candidate_id
              THEN 'FROZEN_FINANCE_CANDIDATE_CONFLICT'
          END) AS payload,
        UPPER(BTRIM(item.item_type)) AS item_type,UPPER(BTRIM(item.frozen_component_key_type)) AS key_type,
        BTRIM(item.frozen_component_key_value) AS key_value,
        ROUND(CASE WHEN UPPER(BTRIM(item.item_type))='OVERPAYMENT_RECOVERY' THEN -1 ELSE 1 END
          * source_amount.amount_ex_vat,2) AS source_amount_ex_vat,
        ROUND(CASE WHEN UPPER(BTRIM(item.item_type))='OVERPAYMENT_RECOVERY' THEN -1 ELSE 1 END
          * COALESCE(NULLIF(ABS(item.amount_inc_vat),0),source_amount.amount_ex_vat),2) AS source_amount_inc_vat,
        'FROZEN_COMPONENT_KEY'::text AS key_resolution_source,
        CASE WHEN UPPER(BTRIM(COALESCE(item.frozen_component_key_type,''))) NOT IN
          ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE','MANUAL_CARRY_FORWARD')
          OR NULLIF(BTRIM(COALESCE(item.frozen_component_key_value,'')),'') IS NULL
          OR (UPPER(BTRIM(item.frozen_component_key_type))='TS_DAY' AND NOT (
            item.frozen_component_key_value ~ '^\d{4}-\d{2}-\d{2}$'
            AND pg_input_is_valid(item.frozen_component_key_value,'date')
            AND CASE WHEN pg_input_is_valid(item.frozen_component_key_value,'date')
              THEN (item.frozen_component_key_value::date)::text=item.frozen_component_key_value
              ELSE false END))
          THEN 'FROZEN_ECONOMIC_KEY_INVALID' END AS key_resolution_failure_reason,
        1::bigint AS occurrence_ordinal
      FROM private.banking_pay_workbench_economic_builds build_row
      JOIN public.pay_batch_candidates candidate ON candidate.candidate_id=build_row.candidate_id
      JOIN public.pay_batch_items item ON item.pay_batch_candidate_id=candidate.id
      LEFT JOIN public.pay_finance_case_components component ON component.id=item.finance_component_id
      LEFT JOIN public.pay_advances finance_case
        ON finance_case.id=COALESCE(item.finance_case_id,component.finance_case_id)
      LEFT JOIN LATERAL (
        SELECT
          ARRAY(SELECT DISTINCT owner_id FROM (VALUES
            (item.timesheet_id),(component.linked_timesheet_id),(finance_case.linked_timesheet_id),
            (CASE WHEN COALESCE(item.frozen_source_basis_json->>'timesheet_id','')
              ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              THEN (item.frozen_source_basis_json->>'timesheet_id')::uuid END)
          ) owner(owner_id) WHERE owner_id IS NOT NULL ORDER BY owner_id) AS owner_ids,
          ARRAY(SELECT DISTINCT candidate_id FROM (VALUES
            (candidate.candidate_id),(component.candidate_id),(finance_case.candidate_id),
            (CASE WHEN COALESCE(item.frozen_source_basis_json->>'candidate_id','')
              ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              THEN (item.frozen_source_basis_json->>'candidate_id')::uuid END)
          ) owner_candidate(candidate_id) WHERE candidate_id IS NOT NULL ORDER BY candidate_id) AS candidate_ids,
          ARRAY(SELECT DISTINCT finance_case_id FROM (VALUES
            (item.finance_case_id),(component.finance_case_id),
            (CASE WHEN COALESCE(item.frozen_source_basis_json->>'finance_case_id','')
              ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              THEN (item.frozen_source_basis_json->>'finance_case_id')::uuid END)
          ) owner_case(finance_case_id) WHERE finance_case_id IS NOT NULL ORDER BY finance_case_id) AS case_ids
      ) owner_candidates ON true
      LEFT JOIN LATERAL (
        SELECT owner_candidates.owner_ids,owner_candidates.candidate_ids,owner_candidates.case_ids,
          owner_candidates.owner_ids[1] AS timesheet_id
      ) owner_resolution ON true
      LEFT JOIN projection_members member ON member.family_timesheet_id=owner_resolution.timesheet_id
      LEFT JOIN public.pay_bank_transfers transfer ON transfer.id=item.pay_bank_transfer_id
      LEFT JOIN LATERAL (
        SELECT BOOL_OR(UPPER(BTRIM(COALESCE(reservation_row.status,'')))='SETTLED'
            OR reservation_row.settled_at_utc IS NOT NULL) AS has_settled,
          ROUND(COALESCE(SUM(ABS(COALESCE(reservation_row.reserved_source_amount,
            reservation_row.reserved_amount,0))) FILTER(
              WHERE UPPER(BTRIM(COALESCE(reservation_row.status,'')))='SETTLED'
                 OR reservation_row.settled_at_utc IS NOT NULL),0),2) AS settled_source_amount
        FROM public.pay_advance_reservations reservation_row
        WHERE reservation_row.pay_batch_item_id=item.id
      ) reservation ON true
      CROSS JOIN LATERAL (SELECT ROUND(ABS(COALESCE(NULLIF(item.frozen_source_amount,0),
        NULLIF(reservation.settled_source_amount,0),NULLIF(item.amount_ex_vat,0),
        NULLIF(item.amount_inc_vat,0))),2) AS amount_ex_vat) source_amount
      WHERE build_row.id=p_build_id
        AND (item.timesheet_id IN (SELECT member_scope.family_timesheet_id FROM projection_members member_scope)
          OR item.finance_component_id IN (SELECT scoped_component.id
            FROM public.pay_finance_case_components scoped_component
            JOIN projection_members member_scope
              ON member_scope.family_timesheet_id=scoped_component.linked_timesheet_id)
          OR item.finance_case_id IN (SELECT scoped_case.id
            FROM public.pay_advances scoped_case
            JOIN projection_members member_scope
              ON member_scope.family_timesheet_id=scoped_case.linked_timesheet_id)
          OR item.timesheet_id IS NULL)
        AND (EXISTS(SELECT 1 FROM unnest(owner_resolution.owner_ids) owner(owner_id)
              JOIN projection_members scoped_member ON scoped_member.family_timesheet_id=owner.owner_id)
          OR (
          cardinality(owner_resolution.owner_ids)=0
          AND p_dependency_unit_key=(SELECT MIN(scope_row.dependency_unit_key)
            FROM private.banking_pay_workbench_economic_build_scope scope_row
            WHERE scope_row.build_id=p_build_id)
          AND p_projected_timesheet_id=(SELECT MIN(projection.timesheet_id)
            FROM private.banking_pay_workbench_economic_build_facts projection
            WHERE projection.build_id=p_build_id AND projection.fact_family='LIVE_ENTITLEMENT_INPUT'
              AND projection.source_relation='UNIT_PROJECTION'
              AND projection.dependency_unit_key=p_dependency_unit_key)
        ))
        AND COALESCE(item.is_voided,false) IS FALSE
        AND UPPER(BTRIM(COALESCE(item.item_type,''))) IN ('OVERPAYMENT_RECOVERY','UNDERPAYMENT_PAYMENT')
        AND source_amount.amount_ex_vat>0
        AND (UPPER(BTRIM(COALESCE(candidate.settlement_status,'')))='SETTLED'
          OR candidate.settled_at_utc IS NOT NULL
          OR UPPER(BTRIM(COALESCE(transfer.status,'')))='COMPLETED'
          OR transfer.completed_at_utc IS NOT NULL OR COALESCE(reservation.has_settled,false))
        AND NOT EXISTS(SELECT 1 FROM public.pay_payment_correction_items correction
          WHERE correction.pay_batch_item_id=item.id AND correction.status='APPLIED'
            AND correction.correction_item_kind IN ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL'))
        AND (p_last_source_key IS NULL OR
          '20:'||p_projected_timesheet_id::text||':'||item.id::text||':99999999'>p_last_source_key)
      ORDER BY item.id
      LIMIT v_limit+1
    ) */ finance_components AS (
      SELECT authority.source_id AS pay_batch_item_id,
        authority.timesheet_id AS source_timesheet_id,
        authority.source_payload_json AS payload,
        UPPER(BTRIM(authority.source_payload_json->>'item_type')) AS item_type,
        authority.economic_key_type AS key_type,
        authority.economic_key_value AS key_value,
        authority.amount_ex_vat AS source_amount_ex_vat,
        authority.amount_inc_vat AS source_amount_inc_vat,
        'FINANCE_ITEM_AUTHORITY'::text AS key_resolution_source,
        NULL::text AS key_resolution_failure_reason,
        1::bigint AS occurrence_ordinal
      FROM private.banking_pay_workbench_economic_build_facts authority
      JOIN projection_members member
        ON member.family_timesheet_id=authority.timesheet_id
      WHERE authority.build_id=p_build_id
        AND authority.fact_family='FINANCE_ITEM_AUTHORITY'
        AND authority.dependency_unit_key='GLOBAL'
        AND COALESCE((authority.source_payload_json->>'settled_authority')::boolean,false)
        AND COALESCE((authority.source_payload_json->>'authoritative_in_scope')::boolean,false)
        AND COALESCE((authority.source_payload_json->>'evidence_only')::boolean,false) IS FALSE
    ), components AS (
      SELECT * FROM standard_components
      UNION ALL SELECT * FROM finance_components
    ), keyed AS (
      SELECT '20:'||p_projected_timesheet_id::text||':'||component.pay_batch_item_id::text||':'||
          LPAD(component.occurrence_ordinal::text,8,'0') AS source_key,component.*
      FROM components component
    )
    SELECT keyed.source_key,md5('SETTLED:'||keyed.source_key),p_projected_timesheet_id,
      ARRAY[p_projected_timesheet_id], 'pay_batch_items'::text,keyed.pay_batch_item_id,
      UPPER(BTRIM(keyed.key_type)),BTRIM(keyed.key_value),keyed.source_amount_ex_vat,
      keyed.source_amount_inc_vat,NULL::numeric,NULL::numeric,NULL::numeric,NULL::numeric,
      jsonb_build_object('role','FROZEN_SETTLED_COMPONENT','projected_timesheet_id',p_projected_timesheet_id,
        'source_timesheet_id',keyed.source_timesheet_id,'item_type',keyed.item_type,
        'key_resolution_source',keyed.key_resolution_source,'pay_batch_item',keyed.payload,
        'finance_case_id',keyed.payload->>'finance_case_id',
        'finance_component_id',keyed.payload->>'finance_component_id',
        'resolution_failure',COALESCE(keyed.payload->>'resolution_failure',keyed.key_resolution_failure_reason,
          CASE WHEN keyed.key_type IS NULL OR keyed.key_value IS NULL THEN 'ECONOMIC_COMPONENT_MISSING' END)),
      md5(jsonb_build_object('source_key',keyed.source_key,'timesheet_id',p_projected_timesheet_id,
        'key_type',UPPER(BTRIM(keyed.key_type)),'key_value',BTRIM(keyed.key_value),
        'amount_ex_vat',keyed.source_amount_ex_vat,'amount_inc_vat',keyed.source_amount_inc_vat)::text),
      COALESCE(keyed.payload->>'resolution_failure',keyed.key_resolution_failure_reason,
        CASE WHEN keyed.key_type IS NULL OR keyed.key_value IS NULL THEN 'ECONOMIC_COMPONENT_MISSING' END)
    FROM keyed
    WHERE p_last_source_key IS NULL OR keyed.source_key>p_last_source_key
    ORDER BY keyed.source_key
    LIMIT v_limit;
    RETURN;
  END IF;

  RETURN QUERY
  WITH projection_members AS (
    SELECT DISTINCT projection.source_id AS family_timesheet_id
    FROM private.banking_pay_workbench_economic_build_facts projection
    WHERE projection.build_id=p_build_id AND projection.fact_family='LIVE_ENTITLEMENT_INPUT'
      AND projection.dependency_unit_key=p_dependency_unit_key
      AND projection.source_relation='UNIT_PROJECTION'
      AND projection.timesheet_id=p_projected_timesheet_id
      AND projection.source_id IS NOT NULL
  ), fallback_states AS MATERIALIZED (
    SELECT state.timesheet_id,state.last_settled_signature,
      COALESCE(state.last_settled_snapshot_json,'{}'::jsonb) AS snapshot
    FROM projection_members member
    JOIN public.timesheet_pay_state state ON state.timesheet_id=member.family_timesheet_id
    WHERE state.last_settled_snapshot_json<>'{}'::jsonb
      AND NOT EXISTS(SELECT 1 FROM private.banking_pay_workbench_economic_build_facts active
        WHERE active.build_id=p_build_id AND active.fact_family='FROZEN_SETTLED_COMPONENT'
          AND active.dependency_unit_key=p_dependency_unit_key
          AND active.timesheet_id=p_projected_timesheet_id)
      AND (p_last_source_key IS NULL OR
        '30:'||p_projected_timesheet_id::text||':'||state.timesheet_id::text||':~'>p_last_source_key)
    ORDER BY state.timesheet_id
    -- Include the current parent plus enough following parents to provide the
    -- complete 25-row page and one row of look-ahead after cursor filtering.
    LIMIT v_limit+1
  ), validation_failure AS (
    SELECT state.timesheet_id AS source_timesheet_id,state.last_settled_signature,
      0::bigint AS source_ordinal,
      '30:'||p_projected_timesheet_id::text||':'||state.timesheet_id::text||':00:CONTAINER' AS source_key,
      NULL::text AS raw_key_type,NULL::text AS raw_key_value,
      NULL::numeric AS amount_ex_vat,NULL::numeric AS amount_inc_vat,
      CASE WHEN jsonb_typeof(state.snapshot)<>'object' THEN 'FALLBACK_SNAPSHOT_NOT_OBJECT'
        WHEN pg_column_size(state.snapshot)>65536 THEN 'FALLBACK_SNAPSHOT_EXCEEDS_ENVELOPE'
        WHEN state.snapshot ? 'segments' AND jsonb_typeof(state.snapshot->'segments')<>'array'
          THEN 'FALLBACK_SEGMENTS_NOT_ARRAY'
        WHEN state.snapshot ? 'additional_units_json'
          AND jsonb_typeof(state.snapshot->'additional_units_json')<>'object'
          THEN 'FALLBACK_ADDITIONAL_NOT_OBJECT'
        WHEN state.snapshot ? 'adjustments' AND jsonb_typeof(state.snapshot->'adjustments')<>'array'
          THEN 'FALLBACK_ADJUSTMENTS_NOT_ARRAY' END AS raw_failure
    FROM fallback_states state
    WHERE jsonb_typeof(state.snapshot)<>'object' OR pg_column_size(state.snapshot)>65536
      OR (state.snapshot ? 'segments' AND jsonb_typeof(state.snapshot->'segments')<>'array')
      OR (state.snapshot ? 'additional_units_json'
        AND jsonb_typeof(state.snapshot->'additional_units_json')<>'object')
      OR (state.snapshot ? 'adjustments' AND jsonb_typeof(state.snapshot->'adjustments')<>'array')
    UNION ALL
    SELECT state.timesheet_id,state.last_settled_signature,segment.ordinality,
      '30:'||p_projected_timesheet_id::text||':'||state.timesheet_id::text||':10:SEGMENT:'||
        LPAD(segment.ordinality::text,12,'0'),NULL,NULL,NULL,NULL,
      CASE WHEN jsonb_typeof(segment.value)<>'object' THEN 'FALLBACK_SEGMENT_NOT_OBJECT'
        WHEN NULLIF(BTRIM(COALESCE(segment.value->>'segment_id','')),'') IS NULL
          THEN 'FALLBACK_SEGMENT_ID_MISSING'
        WHEN NULLIF(segment.value->>'exclude_from_pay','') IS NOT NULL
          AND lower(segment.value->>'exclude_from_pay') NOT IN ('true','false')
          THEN 'FALLBACK_SEGMENT_EXCLUDE_INVALID'
        WHEN lower(COALESCE(NULLIF(segment.value->>'exclude_from_pay',''),'false'))='false'
          AND COALESCE(segment.value->>'pay_amount','') !~ '^-?\d+(\.\d+)?$'
          THEN 'FALLBACK_SEGMENT_AMOUNT_INVALID'
        WHEN NULLIF(BTRIM(COALESCE(segment.value->>'date','')),'') IS NOT NULL
          AND NOT (segment.value->>'date' ~ '^\d{4}-\d{2}-\d{2}$'
            AND pg_input_is_valid(segment.value->>'date','date')
            AND CASE WHEN pg_input_is_valid(segment.value->>'date','date')
              THEN ((segment.value->>'date')::date)::text=segment.value->>'date' ELSE false END)
          THEN 'FALLBACK_SEGMENT_DATE_INVALID' END
    FROM fallback_states state
    CROSS JOIN LATERAL jsonb_array_elements(CASE
      WHEN jsonb_typeof(state.snapshot->'segments')='array' AND pg_column_size(state.snapshot)<=65536
        THEN state.snapshot->'segments' ELSE '[]'::jsonb END)
      WITH ORDINALITY segment(value,ordinality)
    WHERE jsonb_typeof(segment.value)<>'object'
      OR NULLIF(BTRIM(COALESCE(segment.value->>'segment_id','')),'') IS NULL
      OR (NULLIF(segment.value->>'exclude_from_pay','') IS NOT NULL
        AND lower(segment.value->>'exclude_from_pay') NOT IN ('true','false'))
      OR (lower(COALESCE(NULLIF(segment.value->>'exclude_from_pay',''),'false'))='false'
        AND COALESCE(segment.value->>'pay_amount','') !~ '^-?\d+(\.\d+)?$')
      OR (NULLIF(BTRIM(COALESCE(segment.value->>'date','')),'') IS NOT NULL
        AND NOT (segment.value->>'date' ~ '^\d{4}-\d{2}-\d{2}$'
          AND pg_input_is_valid(segment.value->>'date','date')
          AND CASE WHEN pg_input_is_valid(segment.value->>'date','date')
            THEN ((segment.value->>'date')::date)::text=segment.value->>'date' ELSE false END))
    UNION ALL
    SELECT state.timesheet_id,state.last_settled_signature,0,
      '30:'||p_projected_timesheet_id::text||':'||state.timesheet_id::text||':20:ADDITIONAL:'||
        LPAD(octet_length(entry.key)::text,10,'0')||':'||
        encode(convert_to(entry.key,'UTF8'),'hex'),NULL,NULL,NULL,NULL,
      CASE WHEN NULLIF(BTRIM(entry.key),'') IS NULL THEN 'FALLBACK_ADDITIONAL_CODE_MISSING'
        WHEN jsonb_typeof(entry.value)<>'object' THEN 'FALLBACK_ADDITIONAL_VALUE_NOT_OBJECT'
        WHEN NULLIF(COALESCE(entry.value->>'pay_ex_vat',entry.value->>'amount_ex_vat',''),'') IS NOT NULL
          AND COALESCE(entry.value->>'pay_ex_vat',entry.value->>'amount_ex_vat','') !~ '^-?\d+(\.\d+)?$'
          THEN 'FALLBACK_ADDITIONAL_AMOUNT_INVALID'
        WHEN EXISTS(SELECT 1 FROM unnest(ARRAY['unit_count','units_week','pay_rate','rate']) field_name
          WHERE NULLIF(entry.value->>field_name,'') IS NOT NULL
            AND entry.value->>field_name !~ '^-?\d+(\.\d+)?$')
          THEN 'FALLBACK_ADDITIONAL_RATE_INPUT_INVALID' END
    FROM fallback_states state
    CROSS JOIN LATERAL jsonb_each(CASE
      WHEN jsonb_typeof(state.snapshot->'additional_units_json')='object'
       AND pg_column_size(state.snapshot)<=65536
        THEN state.snapshot->'additional_units_json' ELSE '{}'::jsonb END) entry
    WHERE NULLIF(BTRIM(entry.key),'') IS NULL
      OR jsonb_typeof(entry.value)<>'object'
      OR (NULLIF(COALESCE(entry.value->>'pay_ex_vat',entry.value->>'amount_ex_vat',''),'') IS NOT NULL
        AND COALESCE(entry.value->>'pay_ex_vat',entry.value->>'amount_ex_vat','') !~ '^-?\d+(\.\d+)?$')
      OR EXISTS(SELECT 1 FROM unnest(ARRAY['unit_count','units_week','pay_rate','rate']) field_name
        WHERE NULLIF(entry.value->>field_name,'') IS NOT NULL
          AND entry.value->>field_name !~ '^-?\d+(\.\d+)?$')
    UNION ALL
    SELECT state.timesheet_id,state.last_settled_signature,0,
      '30:'||p_projected_timesheet_id::text||':'||state.timesheet_id::text||':30:NUMERIC',
      NULL,NULL,NULL,NULL,'FALLBACK_NUMERIC_INPUT_INVALID'
    FROM fallback_states state
    WHERE (NULLIF(state.snapshot->>'additional_pay_ex_vat','') IS NOT NULL
        AND state.snapshot->>'additional_pay_ex_vat' !~ '^-?\d+(\.\d+)?$')
      OR EXISTS(
        SELECT 1 FROM (VALUES
          (state.snapshot #>> '{expenses,travel_pay_ex_vat}'),
          (state.snapshot #>> '{expenses,accommodation_pay_ex_vat}'),
          (state.snapshot #>> '{expenses,other_pay_ex_vat}'),
          (state.snapshot #>> '{expenses,mileage_pay_ex_vat}'),
          (state.snapshot #>> '{expenses,expenses_pay_ex_vat}')
        ) numeric_value(value)
        WHERE NULLIF(numeric_value.value,'') IS NOT NULL
          AND numeric_value.value !~ '^-?\d+(\.\d+)?$')
    UNION ALL
    SELECT state.timesheet_id,state.last_settled_signature,adjustment.ordinality,
      '30:'||p_projected_timesheet_id::text||':'||state.timesheet_id::text||':40:ADJUSTMENT:'||
        LPAD(adjustment.ordinality::text,12,'0'),NULL,NULL,NULL,NULL,
      CASE WHEN jsonb_typeof(adjustment.value)<>'object' THEN 'FALLBACK_ADJUSTMENT_NOT_OBJECT'
        WHEN NULLIF(BTRIM(COALESCE(adjustment.value->>'id','')),'') IS NULL
          THEN 'FALLBACK_ADJUSTMENT_ID_MISSING'
        WHEN COALESCE(adjustment.value->>'delta_pay_ex_vat','') !~ '^-?\d+(\.\d+)?$'
          THEN 'FALLBACK_ADJUSTMENT_AMOUNT_INVALID' END
    FROM fallback_states state
    CROSS JOIN LATERAL jsonb_array_elements(CASE
      WHEN jsonb_typeof(state.snapshot->'adjustments')='array' AND pg_column_size(state.snapshot)<=65536
        THEN state.snapshot->'adjustments' ELSE '[]'::jsonb END)
      WITH ORDINALITY adjustment(value,ordinality)
    WHERE jsonb_typeof(adjustment.value)<>'object'
      OR NULLIF(BTRIM(COALESCE(adjustment.value->>'id','')),'') IS NULL
      OR COALESCE(adjustment.value->>'delta_pay_ex_vat','') !~ '^-?\d+(\.\d+)?$'
  ), segment_occurrence AS (
    SELECT state.timesheet_id AS source_timesheet_id,state.last_settled_signature,
      segment.ordinality::bigint AS source_ordinal,
      '30:'||p_projected_timesheet_id::text||':'||state.timesheet_id::text||':10:SEGMENT:'||
        LPAD(segment.ordinality::text,12,'0') AS source_key,
      CASE WHEN NULLIF(BTRIM(COALESCE(segment.value->>'date','')),'') IS NOT NULL
        THEN 'TS_DAY' ELSE 'TS_TOTAL' END AS raw_key_type,
      CASE WHEN NULLIF(BTRIM(COALESCE(segment.value->>'date','')),'') IS NOT NULL
        THEN BTRIM(segment.value->>'date') ELSE 'TOTAL' END AS raw_key_value,
      ROUND(CASE WHEN lower(COALESCE(NULLIF(segment.value->>'exclude_from_pay',''),'false'))='true'
        THEN 0::numeric ELSE (segment.value->>'pay_amount')::numeric END,2) AS amount_ex_vat,
      ROUND(CASE WHEN lower(COALESCE(NULLIF(segment.value->>'exclude_from_pay',''),'false'))='true'
        THEN 0::numeric ELSE (segment.value->>'pay_amount')::numeric END,2) AS amount_inc_vat,
      NULL::text AS raw_failure,false AS evidence_only,'SEGMENT'::text AS source_kind
    FROM fallback_states state
    CROSS JOIN LATERAL jsonb_array_elements(CASE
      WHEN jsonb_typeof(state.snapshot->'segments')='array' AND pg_column_size(state.snapshot)<=65536
        THEN state.snapshot->'segments' ELSE '[]'::jsonb END)
      WITH ORDINALITY segment(value,ordinality)
    WHERE jsonb_typeof(segment.value)='object'
      AND NULLIF(BTRIM(COALESCE(segment.value->>'segment_id','')),'') IS NOT NULL
      AND lower(COALESCE(NULLIF(segment.value->>'exclude_from_pay',''),'false')) IN ('true','false')
      AND (lower(COALESCE(NULLIF(segment.value->>'exclude_from_pay',''),'false'))='true'
        OR COALESCE(segment.value->>'pay_amount','') ~ '^-?\d+(\.\d+)?$')
      AND (NULLIF(BTRIM(COALESCE(segment.value->>'date','')),'') IS NULL OR (
        segment.value->>'date' ~ '^\d{4}-\d{2}-\d{2}$'
        AND pg_input_is_valid(segment.value->>'date','date')
        AND CASE WHEN pg_input_is_valid(segment.value->>'date','date')
          THEN ((segment.value->>'date')::date)::text=segment.value->>'date' ELSE false END))
  ), additional_occurrence AS (
    SELECT state.timesheet_id AS source_timesheet_id,state.last_settled_signature,0::bigint,
      '30:'||p_projected_timesheet_id::text||':'||state.timesheet_id::text||':20:ADDITIONAL:'||
        LPAD(octet_length(entry.key)::text,10,'0')||':'||
        encode(convert_to(entry.key,'UTF8'),'hex'),
      'ADDITIONAL_CODE'::text,UPPER(BTRIM(entry.key)),
      amount.amount_ex_vat AS amount_ex_vat,amount.amount_ex_vat AS amount_inc_vat,
      NULL::text,false,'ADDITIONAL'::text
    FROM fallback_states state
    CROSS JOIN LATERAL jsonb_each(CASE
      WHEN jsonb_typeof(state.snapshot->'additional_units_json')='object'
       AND pg_column_size(state.snapshot)<=65536
        THEN state.snapshot->'additional_units_json' ELSE '{}'::jsonb END) entry
    CROSS JOIN LATERAL (SELECT ROUND(COALESCE(
      CASE WHEN COALESCE(entry.value->>'pay_ex_vat','') ~ '^-?\d+(\.\d+)?$'
        THEN (entry.value->>'pay_ex_vat')::numeric END,
      CASE WHEN COALESCE(entry.value->>'amount_ex_vat','') ~ '^-?\d+(\.\d+)?$'
        THEN (entry.value->>'amount_ex_vat')::numeric END,
      COALESCE(CASE WHEN COALESCE(entry.value->>'unit_count','') ~ '^-?\d+(\.\d+)?$'
        THEN (entry.value->>'unit_count')::numeric END,
        CASE WHEN COALESCE(entry.value->>'units_week','') ~ '^-?\d+(\.\d+)?$'
          THEN (entry.value->>'units_week')::numeric END,0)
      * COALESCE(CASE WHEN COALESCE(entry.value->>'pay_rate','') ~ '^-?\d+(\.\d+)?$'
        THEN (entry.value->>'pay_rate')::numeric END,
        CASE WHEN COALESCE(entry.value->>'rate','') ~ '^-?\d+(\.\d+)?$'
          THEN (entry.value->>'rate')::numeric END,0),0),2) AS amount_ex_vat) amount
    WHERE jsonb_typeof(entry.value)='object' AND NULLIF(BTRIM(entry.key),'') IS NOT NULL
      AND (NULLIF(COALESCE(entry.value->>'pay_ex_vat',entry.value->>'amount_ex_vat',''),'') IS NULL
        OR COALESCE(entry.value->>'pay_ex_vat',entry.value->>'amount_ex_vat','') ~ '^-?\d+(\.\d+)?$')
      AND NOT EXISTS(SELECT 1 FROM unnest(ARRAY['unit_count','units_week','pay_rate','rate']) field_name
        WHERE NULLIF(entry.value->>field_name,'') IS NOT NULL
          AND entry.value->>field_name !~ '^-?\d+(\.\d+)?$')
      AND amount.amount_ex_vat<>0
  ), additional_total_occurrence AS (
    SELECT state.timesheet_id AS source_timesheet_id,state.last_settled_signature,0::bigint,
      '30:'||p_projected_timesheet_id::text||':'||state.timesheet_id::text||':25:ADDITIONAL_TOTAL',
      'ADDITIONAL_CODE'::text,'TOTAL'::text,
      ROUND((state.snapshot->>'additional_pay_ex_vat')::numeric,2) AS amount_ex_vat,
      ROUND((state.snapshot->>'additional_pay_ex_vat')::numeric,2) AS amount_inc_vat,
      NULL::text,false,'ADDITIONAL_TOTAL'::text
    FROM fallback_states state
    WHERE COALESCE(state.snapshot->>'additional_pay_ex_vat','') ~ '^-?\d+(\.\d+)?$'
      AND ROUND((state.snapshot->>'additional_pay_ex_vat')::numeric,2)<>0
      AND COALESCE((SELECT ROUND(SUM(existing.amount_ex_vat),2)
        FROM additional_occurrence existing
        WHERE existing.source_timesheet_id=state.timesheet_id),0)=0
  ), expense_occurrence AS (
    SELECT state.timesheet_id AS source_timesheet_id,state.last_settled_signature,0::bigint,
      '30:'||p_projected_timesheet_id::text||':'||state.timesheet_id::text||':30:EXPENSE:'||expense.key_value,
      'EXPENSE_CODE'::text,expense.key_value,
      expense.amount_ex_vat AS amount_ex_vat,expense.amount_ex_vat AS amount_inc_vat,
      NULL::text,false,'EXPENSE'::text
    FROM fallback_states state
    CROSS JOIN LATERAL (SELECT
      ROUND(CASE WHEN COALESCE(state.snapshot#>>'{expenses,travel_pay_ex_vat}','') ~ '^-?\d+(\.\d+)?$'
        THEN (state.snapshot#>>'{expenses,travel_pay_ex_vat}')::numeric ELSE 0 END,2) AS travel_ex,
      ROUND(CASE WHEN COALESCE(state.snapshot#>>'{expenses,accommodation_pay_ex_vat}','') ~ '^-?\d+(\.\d+)?$'
        THEN (state.snapshot#>>'{expenses,accommodation_pay_ex_vat}')::numeric ELSE 0 END,2) AS accommodation_ex,
      ROUND(CASE WHEN COALESCE(state.snapshot#>>'{expenses,other_pay_ex_vat}','') ~ '^-?\d+(\.\d+)?$'
        THEN (state.snapshot#>>'{expenses,other_pay_ex_vat}')::numeric ELSE 0 END,2) AS other_ex,
      ROUND(CASE WHEN COALESCE(state.snapshot#>>'{expenses,mileage_pay_ex_vat}','') ~ '^-?\d+(\.\d+)?$'
        THEN (state.snapshot#>>'{expenses,mileage_pay_ex_vat}')::numeric ELSE 0 END,2) AS mileage_ex,
      ROUND(CASE WHEN COALESCE(state.snapshot#>>'{expenses,expenses_pay_ex_vat}','') ~ '^-?\d+(\.\d+)?$'
        THEN (state.snapshot#>>'{expenses,expenses_pay_ex_vat}')::numeric ELSE 0 END,2) AS expenses_ex
    ) totals
    CROSS JOIN LATERAL (VALUES
      ('TRAVEL'::text,totals.travel_ex),('ACCOMMODATION'::text,totals.accommodation_ex),
      ('OTHER'::text,totals.other_ex),('MILEAGE'::text,totals.mileage_ex),
      ('EXPENSES'::text,CASE WHEN totals.travel_ex=0 AND totals.accommodation_ex=0
        AND totals.other_ex=0 AND totals.mileage_ex=0 THEN totals.expenses_ex ELSE 0 END)
    ) expense(key_value,amount_ex_vat)
    WHERE expense.amount_ex_vat<>0
  ), adjustment_occurrence AS (
    SELECT state.timesheet_id AS source_timesheet_id,state.last_settled_signature,
      adjustment.ordinality::bigint,
      '30:'||p_projected_timesheet_id::text||':'||state.timesheet_id::text||':40:ADJUSTMENT:'||
        LPAD(adjustment.ordinality::text,12,'0'),
      'ADJUSTMENT_CODE'::text,BTRIM(adjustment.value->>'id'),
      ROUND((adjustment.value->>'delta_pay_ex_vat')::numeric,2) AS amount_ex_vat,
      ROUND((adjustment.value->>'delta_pay_ex_vat')::numeric,2) AS amount_inc_vat,
      NULL::text,false,'ADJUSTMENT'::text
    FROM fallback_states state
    CROSS JOIN LATERAL jsonb_array_elements(CASE
      WHEN jsonb_typeof(state.snapshot->'adjustments')='array' AND pg_column_size(state.snapshot)<=65536
        THEN state.snapshot->'adjustments' ELSE '[]'::jsonb END)
      WITH ORDINALITY adjustment(value,ordinality)
    WHERE jsonb_typeof(adjustment.value)='object'
      AND NULLIF(BTRIM(COALESCE(adjustment.value->>'id','')),'') IS NOT NULL
      AND COALESCE(adjustment.value->>'delta_pay_ex_vat','') ~ '^-?\d+(\.\d+)?$'
      AND ROUND((adjustment.value->>'delta_pay_ex_vat')::numeric,2)<>0
  ), valid_occurrence AS (
    SELECT * FROM segment_occurrence
    UNION ALL SELECT * FROM additional_occurrence
    UNION ALL SELECT * FROM additional_total_occurrence
    UNION ALL SELECT * FROM expense_occurrence
    UNION ALL SELECT * FROM adjustment_occurrence
  ), evidence_occurrence AS (
    SELECT state.timesheet_id AS source_timesheet_id,state.last_settled_signature,0::bigint,
      '30:'||p_projected_timesheet_id::text||':'||state.timesheet_id::text||':99:STATE_EVIDENCE',
      'TS_TOTAL'::text,'TOTAL'::text,0::numeric,0::numeric,NULL::text,true,'STATE_EVIDENCE'::text
    FROM fallback_states state
    WHERE NOT EXISTS(SELECT 1 FROM valid_occurrence valid
        WHERE valid.source_timesheet_id=state.timesheet_id)
      AND NOT EXISTS(SELECT 1 FROM validation_failure failure
        WHERE failure.source_timesheet_id=state.timesheet_id)
  ), raw_occurrence AS (
    SELECT valid.* FROM valid_occurrence valid
    UNION ALL
    SELECT failure.source_timesheet_id,failure.last_settled_signature,failure.source_ordinal,
      failure.source_key,failure.raw_key_type,failure.raw_key_value,
      failure.amount_ex_vat,failure.amount_inc_vat,failure.raw_failure,false,'VALIDATION_FAILURE'::text
    FROM validation_failure failure
    UNION ALL SELECT evidence.* FROM evidence_occurrence evidence
  ), paged_occurrence AS MATERIALIZED (
    SELECT occurrence.* FROM raw_occurrence occurrence
    WHERE p_last_source_key IS NULL OR occurrence.source_key>p_last_source_key
    ORDER BY occurrence.source_key LIMIT v_limit
  ), resolved AS (
    SELECT occurrence.*,resolved_key.key_type,resolved_key.key_value,
      resolved_key.key_resolution_source,resolved_key.key_resolution_failure_reason
    FROM paged_occurrence occurrence
    LEFT JOIN LATERAL public._pay_policy_x_resolve_pre_draft_economic_key(
      p_timesheet_id=>p_projected_timesheet_id,
      p_live_source_json=>jsonb_build_object('timesheet_id',p_projected_timesheet_id,
        'component_key_type',occurrence.raw_key_type,'component_key_value',occurrence.raw_key_value,
        'work_date',CASE WHEN occurrence.raw_key_type='TS_DAY' THEN occurrence.raw_key_value END),
      p_item_type=>CASE WHEN occurrence.raw_key_type IN ('TS_DAY','TS_TOTAL') THEN 'SEGMENT_DELTA'
        WHEN occurrence.raw_key_type='ADJUSTMENT_CODE' THEN 'ADJUSTMENT_DELTA'
        WHEN occurrence.raw_key_type='EXPENSE_CODE' AND UPPER(occurrence.raw_key_value)='MILEAGE'
          THEN 'MILEAGE_DELTA'
        WHEN occurrence.raw_key_type IN ('ADDITIONAL_CODE','EXPENSE_CODE') THEN 'EXPENSE_DELTA' END,
      p_key_type_hint=>occurrence.raw_key_type,p_key_value_hint=>occurrence.raw_key_value,
      p_work_date=>NULL::date
    ) resolved_key ON occurrence.raw_failure IS NULL
  )
  SELECT resolved.source_key,md5('FALLBACK:'||resolved.source_key),p_projected_timesheet_id,
    ARRAY[p_projected_timesheet_id],'timesheet_pay_state'::text,resolved.source_timesheet_id,
    resolved.key_type,resolved.key_value,resolved.amount_ex_vat,resolved.amount_inc_vat,
    NULL::numeric,NULL::numeric,NULL::numeric,NULL::numeric,
    jsonb_build_object('role','PAY_STATE_FALLBACK','projected_timesheet_id',p_projected_timesheet_id,
      'source_timesheet_id',resolved.source_timesheet_id,'source_ordinal',resolved.source_ordinal,
      'source_kind',resolved.source_kind,'evidence_only',resolved.evidence_only,
      'last_settled_signature',resolved.last_settled_signature,'raw_key_type',resolved.raw_key_type,
      'raw_key_value',resolved.raw_key_value,'key_resolution_source',resolved.key_resolution_source,
      'resolution_failure',COALESCE(resolved.raw_failure,resolved.key_resolution_failure_reason)),
    md5(jsonb_build_object('source_key',resolved.source_key,'timesheet_id',p_projected_timesheet_id,
      'key_type',resolved.key_type,'key_value',resolved.key_value,
      'amount_ex_vat',resolved.amount_ex_vat,'amount_inc_vat',resolved.amount_inc_vat)::text),
    COALESCE(resolved.raw_failure,resolved.key_resolution_failure_reason,
      CASE WHEN resolved.key_type IS NULL OR resolved.key_value IS NULL THEN 'ECONOMIC_KEY_MISSING' END)
  FROM resolved
  WHERE p_last_source_key IS NULL OR resolved.source_key>p_last_source_key
  ORDER BY resolved.source_key
  LIMIT v_limit;
END;
$function$;

ALTER FUNCTION private.pay_workbench_unit_economic_occurrence_page_v1(uuid,text,text,uuid,text,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_unit_economic_occurrence_page_v1(uuid,text,text,uuid,text,integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION private.pay_workbench_unit_economic_occurrence_page_v1(uuid,text,text,uuid,text,integer) FROM anon;
REVOKE ALL ON FUNCTION private.pay_workbench_unit_economic_occurrence_page_v1(uuid,text,text,uuid,text,integer) FROM authenticated;
REVOKE ALL ON FUNCTION private.pay_workbench_unit_economic_occurrence_page_v1(uuid,text,text,uuid,text,integer) FROM service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_unit_economic_occurrence_page_v1(uuid,text,text,uuid,text,integer) TO postgres;
