-- Banking Pay bounded-scope V1.2.7: bounded physical economic occurrences
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
    WITH projection_members AS (
      SELECT DISTINCT projection.source_id AS family_timesheet_id
      FROM private.banking_pay_workbench_economic_build_facts projection
      WHERE projection.build_id=p_build_id
        AND projection.fact_family='LIVE_ENTITLEMENT_INPUT'
        AND projection.dependency_unit_key=p_dependency_unit_key
        AND projection.source_relation='UNIT_PROJECTION'
        AND projection.timesheet_id=p_projected_timesheet_id
        AND projection.source_id IS NOT NULL
    ), canonical AS (
      SELECT ts.*,tf.id AS financial_id,tf.total_pay_ex_vat,tf.invoice_breakdown_json,
        tf.additional_units_json,tf.expenses_pay_ex_vat,tf.travel_pay_ex_vat,
        tf.accommodation_pay_ex_vat,tf.other_pay_ex_vat,tf.mileage_pay_ex_vat,
        tf.worked_start_iso AS tf_worked_start_iso,
        tf.worked_end_iso AS tf_worked_end_iso,
        tf.break_start_iso AS tf_break_start_iso,
        tf.break_end_iso AS tf_break_end_iso,
        tf.break_minutes AS tf_break_minutes,
        tf.actual_schedule_json AS tf_actual_schedule_json
      FROM public.timesheets ts
      JOIN public.timesheets_financials tf
        ON tf.timesheet_id=ts.timesheet_id AND tf.is_current
      WHERE ts.timesheet_id=p_projected_timesheet_id
        AND ts.is_current AND ts.revoked_at IS NULL
        AND ts.archived_at_utc IS NULL AND ts.authorised_at_server IS NOT NULL
      ORDER BY tf.computed_at_utc DESC NULLS LAST,tf.id DESC
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
            AND segment.value->>'date' !~ '^\d{4}-\d{2}-\d{2}$'
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
            'pay_amount',ROUND(COALESCE(canonical.total_pay_ex_vat,0),2),
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
      SELECT '10:'||p_projected_timesheet_id::text||':ADDITIONAL:'||UPPER(BTRIM(additional.key)) AS source_key,
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
          'projected_timesheet_id',p_projected_timesheet_id,'additional_code',UPPER(BTRIM(additional.key)),
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
          '10:'||p_projected_timesheet_id::text||':ADDITIONAL:'||UPPER(BTRIM(entry.key))>p_last_source_key
        ORDER BY UPPER(BTRIM(entry.key)) LIMIT v_limit
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
        COALESCE(canonical.expenses_pay_ex_vat,0)::numeric AS expenses_ex
      FROM canonical
    ), expense_rows AS (
      SELECT '10:'||p_projected_timesheet_id::text||':EXPENSE:'||expense.key_value AS source_key,
        'timesheets_financials'::text AS source_relation,expense_source.financial_id AS source_id,
        'EXPENSE_CODE'::text AS raw_key_type,expense.key_value,ROUND(expense.amount_ex_vat,2) AS amount_ex_vat,
        jsonb_build_object('role','LIVE_COMPONENT','source_kind','EXPENSE',
          'projected_timesheet_id',p_projected_timesheet_id,'expense_code',expense.key_value) AS payload,
        NULL::text AS raw_failure
      FROM expense_source
      CROSS JOIN LATERAL (VALUES
        ('TRAVEL'::text,expense_source.travel_ex),
        ('ACCOMMODATION'::text,expense_source.accommodation_ex),
        ('OTHER'::text,expense_source.other_ex),
        ('MILEAGE'::text,expense_source.mileage_ex),
        ('EXPENSES'::text,CASE WHEN expense_source.travel_ex=0 AND expense_source.accommodation_ex=0
          AND expense_source.other_ex=0 AND expense_source.mileage_ex=0
          THEN expense_source.expenses_ex ELSE 0::numeric END)
      ) expense(key_value,amount_ex_vat)
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
    )
    SELECT resolved.source_key,md5('LIVE:'||resolved.source_key),p_projected_timesheet_id,
      ARRAY[p_projected_timesheet_id],resolved.source_relation,resolved.source_id,
      resolved.key_type,resolved.key_value,NULL::numeric,NULL::numeric,
      resolved.amount_ex_vat,resolved.amount_ex_vat,NULL::numeric,NULL::numeric,
      resolved.payload||jsonb_build_object('raw_key_type',resolved.raw_key_type,
        'raw_key_value',resolved.raw_key_value,'key_resolution_source',resolved.key_resolution_source,
        'resolution_failure',COALESCE(resolved.raw_failure,resolved.key_resolution_failure_reason)),
      md5(jsonb_build_object('source_key',resolved.source_key,'timesheet_id',p_projected_timesheet_id,
        'key_type',resolved.key_type,'key_value',resolved.key_value,
        'truth_ex_vat',resolved.amount_ex_vat,'truth_inc_vat',resolved.amount_ex_vat)::text),
      COALESCE(resolved.raw_failure,resolved.key_resolution_failure_reason,
        CASE WHEN resolved.key_type IS NULL OR resolved.key_value IS NULL THEN 'ECONOMIC_KEY_MISSING' END)
    FROM resolved
    WHERE p_last_source_key IS NULL OR resolved.source_key>p_last_source_key
    ORDER BY resolved.source_key
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
    ), finance_components AS (
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
            AND pg_input_is_valid(item.frozen_component_key_value,'date'::regtype)
            AND CASE WHEN pg_input_is_valid(item.frozen_component_key_value,'date'::regtype)
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
            AND pg_input_is_valid(segment.value->>'date','date'::regtype)
            AND CASE WHEN pg_input_is_valid(segment.value->>'date','date'::regtype)
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
          AND pg_input_is_valid(segment.value->>'date','date'::regtype)
          AND CASE WHEN pg_input_is_valid(segment.value->>'date','date'::regtype)
            THEN ((segment.value->>'date')::date)::text=segment.value->>'date' ELSE false END))
    UNION ALL
    SELECT state.timesheet_id,state.last_settled_signature,0,
      '30:'||p_projected_timesheet_id::text||':'||state.timesheet_id::text||':20:ADDITIONAL:'||
        UPPER(BTRIM(entry.key)),NULL,NULL,NULL,NULL,
      CASE WHEN jsonb_typeof(entry.value)<>'object' THEN 'FALLBACK_ADDITIONAL_VALUE_NOT_OBJECT'
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
    WHERE jsonb_typeof(entry.value)<>'object'
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
        AND pg_input_is_valid(segment.value->>'date','date'::regtype)
        AND CASE WHEN pg_input_is_valid(segment.value->>'date','date'::regtype)
          THEN ((segment.value->>'date')::date)::text=segment.value->>'date' ELSE false END))
  ), additional_occurrence AS (
    SELECT state.timesheet_id,state.last_settled_signature,0::bigint,
      '30:'||p_projected_timesheet_id::text||':'||state.timesheet_id::text||':20:ADDITIONAL:'||
        UPPER(BTRIM(entry.key)),
      'ADDITIONAL_CODE'::text,UPPER(BTRIM(entry.key)),amount.amount_ex_vat,amount.amount_ex_vat,
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
    SELECT state.timesheet_id,state.last_settled_signature,0::bigint,
      '30:'||p_projected_timesheet_id::text||':'||state.timesheet_id::text||':25:ADDITIONAL_TOTAL',
      'ADDITIONAL_CODE'::text,'TOTAL'::text,ROUND((state.snapshot->>'additional_pay_ex_vat')::numeric,2),
      ROUND((state.snapshot->>'additional_pay_ex_vat')::numeric,2),NULL::text,false,'ADDITIONAL_TOTAL'::text
    FROM fallback_states state
    WHERE COALESCE(state.snapshot->>'additional_pay_ex_vat','') ~ '^-?\d+(\.\d+)?$'
      AND ROUND((state.snapshot->>'additional_pay_ex_vat')::numeric,2)<>0
      AND COALESCE((SELECT ROUND(SUM(existing.amount_ex_vat),2)
        FROM additional_occurrence existing
        WHERE existing.source_timesheet_id=state.timesheet_id),0)=0
  ), expense_occurrence AS (
    SELECT state.timesheet_id,state.last_settled_signature,0::bigint,
      '30:'||p_projected_timesheet_id::text||':'||state.timesheet_id::text||':30:EXPENSE:'||expense.key_value,
      'EXPENSE_CODE'::text,expense.key_value,expense.amount_ex_vat,expense.amount_ex_vat,
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
    SELECT state.timesheet_id,state.last_settled_signature,adjustment.ordinality::bigint,
      '30:'||p_projected_timesheet_id::text||':'||state.timesheet_id::text||':40:ADJUSTMENT:'||
        LPAD(adjustment.ordinality::text,12,'0'),
      'ADJUSTMENT_CODE'::text,BTRIM(adjustment.value->>'id'),
      ROUND((adjustment.value->>'delta_pay_ex_vat')::numeric,2),
      ROUND((adjustment.value->>'delta_pay_ex_vat')::numeric,2),NULL::text,false,'ADJUSTMENT'::text
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
    SELECT state.timesheet_id,state.last_settled_signature,0::bigint,
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
