-- Policy X: post-draft breakdowns are derived only from the item's frozen
-- resolution payload. Nested resolution bucket targets are expanded without
-- consulting live finance components or deriving units/rate from amount arithmetic.

CREATE OR REPLACE FUNCTION public.pay_batch_build_item_breakdowns(p_pay_batch_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_operation_id uuid DEFAULT NULL::uuid, p_candidate_scope_ids jsonb DEFAULT NULL::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_scope_ids jsonb := COALESCE(p_candidate_scope_ids, '[]'::jsonb);
  v_scope_id_count integer := 0;
  v_inserted_count integer := 0;
  v_reused_count integer := 0;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'pay_batch_id is required';
  END IF;

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'p_operation_id is required for row-backed breakdown creation';
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'p_actor_user_id is required for row-backed breakdown creation';
  END IF;

  IF p_candidate_scope_ids IS NULL OR jsonb_typeof(v_scope_ids) <> 'array' OR jsonb_array_length(v_scope_ids) = 0 THEN
    RAISE EXCEPTION 'p_candidate_scope_ids must be a non-empty JSON array';
  END IF;

  v_scope_id_count := jsonb_array_length(v_scope_ids);

  IF v_scope_id_count > 100 THEN
    RAISE EXCEPTION 'p_candidate_scope_ids exceeds the 100 row cap: %', v_scope_id_count;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM jsonb_array_elements(v_scope_ids) AS supplied_scope(scope_value)
    WHERE NOT ((supplied_scope.scope_value #>> '{}') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
  ) THEN
    RAISE EXCEPTION 'p_candidate_scope_ids must contain UUID strings only';
  END IF;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_batch_breakdown_item_page;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_batch_breakdown_item_page ON COMMIT DROP AS
  SELECT pay_batch_item.*
  FROM public.pay_batch_items AS pay_batch_item
  JOIN public.pay_batch_candidates AS pay_batch_candidate
    ON pay_batch_candidate.id = pay_batch_item.pay_batch_candidate_id
  JOIN public.banking_pay_operation_candidate_allocation_rows AS allocation_row
    ON allocation_row.pay_batch_item_id = pay_batch_item.id
   AND allocation_row.operation_id = p_operation_id
  WHERE pay_batch_candidate.pay_batch_id = p_pay_batch_id
    AND allocation_row.candidate_scope_id IN (
      SELECT (supplied_scope.scope_value #>> '{}')::uuid
      FROM jsonb_array_elements(v_scope_ids) AS supplied_scope(scope_value)
    )
    AND COALESCE(pay_batch_item.is_voided, false) = false
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_batch_item_breakdowns AS existing_breakdown
      WHERE existing_breakdown.pay_batch_item_id = pay_batch_item.id
    )
  ORDER BY allocation_row.candidate_scope_id, allocation_row.sort_order, pay_batch_item.id
  LIMIT 100;

  PERFORM private.pay_workbench_draft_expected_effects_v1(
    p_operation_id,'BUILD_ITEM_BREAKDOWNS','REGISTER',
    jsonb_build_array(
      jsonb_build_object('relation_name','pay_batch_item_breakdowns','operation','INSERT'),
      jsonb_build_object('relation_name','pay_batch_item_breakdowns','operation','UPDATE')
    ),jsonb_build_object('pay_batch_id',p_pay_batch_id)
  );

  WITH segment_component_wrapper_source AS (
    SELECT
      item_page.*,
      wrapper_entry.component_json AS wrapper_component_json,
      wrapper_entry.component_ordinal::integer AS wrapper_component_ordinal
    FROM pg_temp.tmp_pay_batch_breakdown_item_page AS item_page
    JOIN LATERAL jsonb_array_elements(
      CASE
        WHEN jsonb_typeof(item_page.frozen_resolution_payload_json->'case_components') = 'array'
          THEN item_page.frozen_resolution_payload_json->'case_components'
        ELSE '[]'::jsonb
      END
    ) WITH ORDINALITY AS wrapper_entry(component_json, component_ordinal)
      ON item_page.item_type = 'SEGMENT_DELTA'
  ), segment_component_source AS (
    SELECT
      item_page.*,
      component_entry.component_json,
      component_entry.component_ordinal
    FROM segment_component_wrapper_source AS item_page
    JOIN LATERAL (
      SELECT
        (
          item_page.wrapper_component_json
          || resolution_entry.resolution_json
          || bucket_entry.bucket_json
        ) AS component_json,
        (
          item_page.wrapper_component_ordinal * 1000000
          + resolution_entry.resolution_ordinal * 1000
          + bucket_entry.bucket_ordinal
        )::integer AS component_ordinal
      FROM jsonb_array_elements(
        CASE
          WHEN jsonb_typeof(item_page.wrapper_component_json->'resolution_rows') = 'array'
            THEN item_page.wrapper_component_json->'resolution_rows'
          ELSE '[]'::jsonb
        END
      ) WITH ORDINALITY AS resolution_entry(resolution_json, resolution_ordinal)
      JOIN LATERAL jsonb_array_elements(
        CASE
          WHEN jsonb_typeof(resolution_entry.resolution_json#>'{payload_json,bucket_resolutions}') = 'array'
            THEN resolution_entry.resolution_json#>'{payload_json,bucket_resolutions}'
          ELSE '[]'::jsonb
        END
      ) WITH ORDINALITY AS bucket_entry(bucket_json, bucket_ordinal)
        ON true

      UNION ALL

      SELECT
        item_page.wrapper_component_json AS component_json,
        (item_page.wrapper_component_ordinal * 1000000)::integer AS component_ordinal
      WHERE NOT EXISTS (
        SELECT 1
        FROM jsonb_array_elements(
          CASE
            WHEN jsonb_typeof(item_page.wrapper_component_json->'resolution_rows') = 'array'
              THEN item_page.wrapper_component_json->'resolution_rows'
            ELSE '[]'::jsonb
          END
        ) AS fallback_resolution(resolution_json)
        CROSS JOIN LATERAL jsonb_array_elements(
          CASE
            WHEN jsonb_typeof(fallback_resolution.resolution_json#>'{payload_json,bucket_resolutions}') = 'array'
              THEN fallback_resolution.resolution_json#>'{payload_json,bucket_resolutions}'
            ELSE '[]'::jsonb
          END
        ) AS fallback_bucket(bucket_json)
      )
    ) AS component_entry
      ON true
    CROSS JOIN LATERAL (
      SELECT
        UPPER(NULLIF(BTRIM(COALESCE(
          item_page.frozen_component_key_type,
          item_page.frozen_source_basis_json->>'key_type',
          item_page.frozen_source_basis_json#>>'{economic_key,key_type}',
          item_page.frozen_component_snapshot_json->>'component_key_type',
          item_page.frozen_component_snapshot_json#>>'{source_basis_json,component_key_type}',
          item_page.frozen_component_snapshot_json#>>'{source_basis_json,key_type}',
          ''
        )), '')) AS item_economic_key_type,
        NULLIF(BTRIM(COALESCE(
          item_page.frozen_component_key_value,
          item_page.frozen_source_basis_json->>'key_value',
          item_page.frozen_source_basis_json#>>'{economic_key,key_value}',
          item_page.frozen_component_snapshot_json->>'component_key_value',
          item_page.frozen_component_snapshot_json#>>'{source_basis_json,component_key_value}',
          item_page.frozen_component_snapshot_json#>>'{source_basis_json,key_value}',
          ''
        )), '') AS item_economic_key_value,
        LOWER(NULLIF(BTRIM(COALESCE(
          CASE WHEN item_page.timesheet_id IS NULL THEN NULL ELSE item_page.timesheet_id::text END,
          item_page.frozen_source_basis_json->>'timesheet_id',
          item_page.frozen_source_basis_json#>>'{economic_key,timesheet_id}',
          item_page.frozen_component_snapshot_json->>'timesheet_id',
          item_page.frozen_component_snapshot_json#>>'{source_basis_json,timesheet_id}',
          ''
        )), '')) AS item_timesheet_id_text,
        UPPER(NULLIF(BTRIM(COALESCE(
          item_page.frozen_component_snapshot_json->>'bucket_code',
          item_page.frozen_source_basis_json->>'bucket_code',
          ''
        )), '')) AS item_bucket_code
    ) AS item_match
    CROSS JOIN LATERAL (
      SELECT
        UPPER(NULLIF(BTRIM(COALESCE(
          component_entry.component_json->>'component_key_type',
          component_entry.component_json->>'key_type',
          component_entry.component_json#>>'{economic_key,key_type}',
          component_entry.component_json#>>'{source_basis_json,component_key_type}',
          component_entry.component_json#>>'{source_basis_json,key_type}',
          component_entry.component_json#>>'{source_basis_json,economic_key,key_type}',
          ''
        )), '')) AS component_economic_key_type,
        NULLIF(BTRIM(COALESCE(
          component_entry.component_json->>'component_key_value',
          component_entry.component_json->>'key_value',
          component_entry.component_json#>>'{economic_key,key_value}',
          component_entry.component_json#>>'{source_basis_json,component_key_value}',
          component_entry.component_json#>>'{source_basis_json,key_value}',
          component_entry.component_json#>>'{source_basis_json,economic_key,key_value}',
          ''
        )), '') AS component_economic_key_value,
        NULLIF(BTRIM(COALESCE(
          component_entry.component_json#>>'{source_basis_json,work_date}',
          component_entry.component_json->>'work_date',
          component_entry.component_json#>>'{source_basis_json,date}',
          component_entry.component_json->>'date',
          component_entry.component_json->>'source_work_date',
          component_entry.component_json->>'source_basis_work_date',
          ''
        )), '') AS component_work_date_value,
        LOWER(NULLIF(BTRIM(COALESCE(
          component_entry.component_json->>'timesheet_id',
          component_entry.component_json->>'real_business_timesheet_id',
          component_entry.component_json#>>'{economic_key,timesheet_id}',
          component_entry.component_json#>>'{source_basis_json,timesheet_id}',
          component_entry.component_json#>>'{source_basis_json,real_business_timesheet_id}',
          component_entry.component_json#>>'{source_basis_json,economic_key,timesheet_id}',
          ''
        )), '')) AS component_timesheet_id_text,
        UPPER(NULLIF(BTRIM(COALESCE(
          component_entry.component_json->>'bucket_code',
          component_entry.component_json#>>'{source_basis_json,bucket_code}',
          component_entry.component_json->>'label',
          ''
        )), '')) AS component_bucket_code
    ) AS component_match
    WHERE component_entry.component_json IS NOT NULL
      AND jsonb_typeof(component_entry.component_json) = 'object'
      AND (
        NULLIF(BTRIM(COALESCE(component_entry.component_json->>'label', '')), '') IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(component_entry.component_json->>'bucket_code', component_entry.component_json #>> '{source_basis_json,bucket_code}', '')), '') IS NOT NULL
        OR COALESCE(component_entry.component_json->>'target_units', component_entry.component_json->>'source_units', component_entry.component_json #>> '{source_basis_json,source_units}', component_entry.component_json #>> '{source_basis_json,units}', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
        OR COALESCE(component_entry.component_json->>'target_rate', component_entry.component_json->>'source_rate', component_entry.component_json #>> '{source_basis_json,source_rate}', component_entry.component_json #>> '{source_basis_json,rate}', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
        OR COALESCE(component_entry.component_json->>'target_pay_ex_vat', component_entry.component_json->>'target_amount_ex_vat', component_entry.component_json->>'component_amount_ex_vat', component_entry.component_json->>'preview_component_amount_ex_vat', component_entry.component_json->>'ready_preview_amount_ex_vat', component_entry.component_json->>'source_pay_ex_vat', component_entry.component_json->>'source_amount_ex_vat', component_entry.component_json #>> '{source_basis_json,source_pay_ex_vat}', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
      )
      AND (
        (
          item_match.item_economic_key_type = 'TS_DAY'
          AND item_match.item_economic_key_value IS NOT NULL
          AND (
            component_match.component_economic_key_type = 'TS_DAY'
            OR component_match.component_economic_key_type IS NULL
          )
          AND (
            component_match.component_work_date_value = item_match.item_economic_key_value
            OR component_match.component_economic_key_value = item_match.item_economic_key_value
          )
          AND (
            component_match.component_timesheet_id_text IS NULL
            OR (
              item_match.item_timesheet_id_text IS NOT NULL
              AND component_match.component_timesheet_id_text = item_match.item_timesheet_id_text
            )
          )
        )
        OR (
          item_match.item_economic_key_type IS NOT NULL
          AND item_match.item_economic_key_type <> 'TS_DAY'
          AND item_match.item_economic_key_value IS NOT NULL
          AND component_match.component_economic_key_type = item_match.item_economic_key_type
          AND component_match.component_economic_key_value = item_match.item_economic_key_value
          AND (
            component_match.component_timesheet_id_text IS NULL
            OR (
              item_match.item_timesheet_id_text IS NOT NULL
              AND component_match.component_timesheet_id_text = item_match.item_timesheet_id_text
            )
          )
        )
      )
      AND (
        item_match.item_bucket_code IS NULL
        OR component_match.component_bucket_code = item_match.item_bucket_code
      )
      AND NOT (
        NULLIF(BTRIM(COALESCE(component_entry.component_json->>'resolved_rate_resolution_id', '')), '') IS NULL
        AND UPPER(BTRIM(COALESCE(component_entry.component_json->>'resolution_state', ''))) = 'FIXED'
        AND LOWER(BTRIM(COALESCE(component_entry.component_json->>'is_actionable_resolution_row', 'false')))
              IN ('false','f','0','no','n','off')
        AND COALESCE(component_entry.component_json->>'source_pay_ex_vat', '') ~ '^-[0-9]+(\.[0-9]+)?$'
        AND EXISTS (
          SELECT 1
          FROM jsonb_array_elements(
            CASE
              WHEN jsonb_typeof(item_page.frozen_resolution_payload_json->'case_components') = 'array'
                THEN item_page.frozen_resolution_payload_json->'case_components'
              ELSE '[]'::jsonb
            END
          ) AS explicit_resolved_component(component_json)
          WHERE UPPER(BTRIM(COALESCE(explicit_resolved_component.component_json->>'component_key_type', ''))) = item_match.item_economic_key_type
            AND BTRIM(COALESCE(explicit_resolved_component.component_json->>'component_key_value', '')) = item_match.item_economic_key_value
            AND NULLIF(BTRIM(COALESCE(explicit_resolved_component.component_json->>'resolved_rate_resolution_id', '')), '') IS NOT NULL
        )
      )
  ), segment_component_derived AS (
    SELECT
      segment_component_source.*,
      NULLIF(UPPER(BTRIM(COALESCE(
        segment_component_source.component_json->>'bucket_code',
        segment_component_source.component_json #>> '{source_basis_json,bucket_code}',
        segment_component_source.component_json->>'label',
        ''
      ))), '') AS derived_bucket_code,
      COALESCE(
        NULLIF(BTRIM(segment_component_source.component_json->>'label'), ''),
        NULLIF(UPPER(BTRIM(segment_component_source.component_json->>'bucket_code')), ''),
        NULLIF(UPPER(BTRIM(segment_component_source.component_json #>> '{source_basis_json,bucket_code}')), ''),
        NULLIF(BTRIM(segment_component_source.frozen_component_snapshot_json->>'label'), ''),
        NULLIF(BTRIM(segment_component_source.description), ''),
        segment_component_source.item_type
      ) AS derived_unit_name,
      NULLIF(BTRIM(COALESCE(
        segment_component_source.component_json->>'target_units',
        segment_component_source.component_json->>'source_units',
        segment_component_source.component_json #>> '{source_basis_json,source_units}',
        segment_component_source.component_json #>> '{source_basis_json,units}',
        segment_component_source.frozen_component_snapshot_json->>'target_units',
        segment_component_source.frozen_source_basis_json->>'units',
        ''
      )), '') AS derived_units_text,
      NULLIF(BTRIM(COALESCE(
        segment_component_source.component_json->>'target_rate',
        segment_component_source.component_json->>'source_rate',
        segment_component_source.component_json #>> '{source_basis_json,source_rate}',
        segment_component_source.component_json #>> '{source_basis_json,rate}',
        segment_component_source.frozen_component_snapshot_json->>'target_rate',
        segment_component_source.frozen_source_basis_json->>'rate',
        ''
      )), '') AS derived_rate_text,
      NULLIF(BTRIM(COALESCE(
        segment_component_source.component_json->>'target_pay_ex_vat',
        segment_component_source.component_json->>'target_amount_ex_vat',
        segment_component_source.component_json->>'component_amount_ex_vat',
        segment_component_source.component_json->>'preview_component_amount_ex_vat',
        segment_component_source.component_json->>'ready_preview_amount_ex_vat',
        segment_component_source.component_json->>'source_pay_ex_vat',
        segment_component_source.component_json->>'source_amount_ex_vat',
        segment_component_source.component_json #>> '{source_basis_json,source_pay_ex_vat}',
        ''
      )), '') AS derived_amount_text
    FROM segment_component_source
  ), segment_component_amounts AS (
    SELECT
      segment_component_derived.*,
      CASE WHEN segment_component_derived.derived_units_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN segment_component_derived.derived_units_text::numeric ELSE NULL::numeric END AS derived_units,
      CASE WHEN segment_component_derived.derived_rate_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN segment_component_derived.derived_rate_text::numeric ELSE NULL::numeric END AS derived_rate,
      CASE
        WHEN segment_component_derived.derived_amount_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN segment_component_derived.derived_amount_text::numeric
        WHEN segment_component_derived.derived_units_text ~ '^-?[0-9]+(\.[0-9]+)?$'
         AND segment_component_derived.derived_rate_text ~ '^-?[0-9]+(\.[0-9]+)?$'
          THEN segment_component_derived.derived_units_text::numeric * segment_component_derived.derived_rate_text::numeric
        ELSE 0::numeric
      END AS derived_amount_ex_vat,
      ROUND(COALESCE(segment_component_derived.frozen_target_amount_vat, segment_component_derived.amount_vat, 0), 2) AS item_total_vat
    FROM segment_component_derived
  ), segment_component_prorated AS (
    SELECT
      segment_component_amounts.*,
      COUNT(*) OVER (PARTITION BY segment_component_amounts.id) AS component_count,
      ROW_NUMBER() OVER (PARTITION BY segment_component_amounts.id ORDER BY segment_component_amounts.component_ordinal, segment_component_amounts.id) AS component_position,
      SUM(ROUND(COALESCE(segment_component_amounts.derived_amount_ex_vat, 0), 2)) OVER (PARTITION BY segment_component_amounts.id) AS component_total_ex_vat,
      ROUND(COALESCE(
        segment_component_amounts.frozen_target_amount_ex_vat,
        segment_component_amounts.amount_ex_vat,
        0
      ), 2) AS item_total_ex_vat,
      ROUND(
        CASE
          WHEN SUM(ROUND(COALESCE(segment_component_amounts.derived_amount_ex_vat, 0), 2)) OVER (PARTITION BY segment_component_amounts.id) <> 0 THEN
            COALESCE(
              segment_component_amounts.frozen_target_amount_ex_vat,
              segment_component_amounts.amount_ex_vat,
              0
            )
            * ROUND(COALESCE(segment_component_amounts.derived_amount_ex_vat, 0), 2)
            / NULLIF(SUM(ROUND(COALESCE(segment_component_amounts.derived_amount_ex_vat, 0), 2)) OVER (PARTITION BY segment_component_amounts.id), 0)
          WHEN COUNT(*) OVER (PARTITION BY segment_component_amounts.id) > 0 THEN
            COALESCE(
              segment_component_amounts.frozen_target_amount_ex_vat,
              segment_component_amounts.amount_ex_vat,
              0
            ) / NULLIF(COUNT(*) OVER (PARTITION BY segment_component_amounts.id), 0)
          ELSE 0
        END,
        2
      ) AS provisional_component_ex_vat,
      ROUND(
        CASE
          WHEN SUM(ROUND(COALESCE(segment_component_amounts.derived_amount_ex_vat, 0), 2)) OVER (PARTITION BY segment_component_amounts.id) <> 0 THEN
            COALESCE(segment_component_amounts.item_total_vat, 0)
            * ROUND(COALESCE(segment_component_amounts.derived_amount_ex_vat, 0), 2)
            / NULLIF(SUM(ROUND(COALESCE(segment_component_amounts.derived_amount_ex_vat, 0), 2)) OVER (PARTITION BY segment_component_amounts.id), 0)
          WHEN COUNT(*) OVER (PARTITION BY segment_component_amounts.id) > 0 THEN
            COALESCE(segment_component_amounts.item_total_vat, 0) / NULLIF(COUNT(*) OVER (PARTITION BY segment_component_amounts.id), 0)
          ELSE 0
        END,
        2
      ) AS provisional_component_vat
    FROM segment_component_amounts
  ), segment_component_allocated AS (
    SELECT
      segment_component_prorated.*,
      CASE
        WHEN segment_component_prorated.component_position = segment_component_prorated.component_count THEN
          ROUND(
            COALESCE(segment_component_prorated.item_total_ex_vat, 0)
            - COALESCE(
                SUM(segment_component_prorated.provisional_component_ex_vat) OVER (
                  PARTITION BY segment_component_prorated.id
                  ORDER BY segment_component_prorated.component_ordinal, segment_component_prorated.id
                  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ),
                0
              ),
            2
          )
        ELSE segment_component_prorated.provisional_component_ex_vat
      END AS allocated_amount_ex_vat,
      CASE
        WHEN segment_component_prorated.component_position = segment_component_prorated.component_count THEN
          ROUND(
            COALESCE(segment_component_prorated.item_total_vat, 0)
            - COALESCE(
                SUM(segment_component_prorated.provisional_component_vat) OVER (
                  PARTITION BY segment_component_prorated.id
                  ORDER BY segment_component_prorated.component_ordinal, segment_component_prorated.id
                  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ),
                0
              ),
            2
          )
        ELSE segment_component_prorated.provisional_component_vat
      END AS derived_amount_vat
    FROM segment_component_prorated
  ), component_breakdown_rows AS (
    SELECT
      segment_component_allocated.id AS pay_batch_item_id,
      'TS_BUCKET'::text AS line_kind,
      segment_component_allocated.derived_bucket_code AS bucket_code,
      segment_component_allocated.derived_unit_name AS unit_name,
      segment_component_allocated.derived_units AS units,
      segment_component_allocated.derived_rate AS rate,
      ROUND(COALESCE(segment_component_allocated.allocated_amount_ex_vat, 0), 2) AS amount_ex_vat,
      ROUND(COALESCE(segment_component_allocated.derived_amount_vat, 0), 2) AS amount_vat,
      ROUND(COALESCE(segment_component_allocated.allocated_amount_ex_vat, 0) + COALESCE(segment_component_allocated.derived_amount_vat, 0), 2) AS amount_inc_vat,
      jsonb_strip_nulls(
        jsonb_build_object(
          'finance_case_id', CASE WHEN segment_component_allocated.finance_case_id IS NULL THEN NULL ELSE segment_component_allocated.finance_case_id::text END,
          'finance_component_id', CASE WHEN segment_component_allocated.finance_component_id IS NULL THEN NULL ELSE segment_component_allocated.finance_component_id::text END,
          'component_key_type', segment_component_allocated.frozen_component_key_type,
          'component_key_value', segment_component_allocated.frozen_component_key_value,
          'economic_key_type', segment_component_allocated.frozen_component_key_type,
          'economic_key_value', segment_component_allocated.frozen_component_key_value,
          'source_basis_json', segment_component_allocated.frozen_source_basis_json,
          'frozen_component_snapshot_json', segment_component_allocated.frozen_component_snapshot_json,
          'case_component_json', segment_component_allocated.component_json,
          'case_component_ordinal', segment_component_allocated.component_ordinal,
          'source_reservation_amount_ex_vat', public._pay_batch_item_source_reservation_amount_ex_vat(segment_component_allocated.id),
          'target_amount_ex_vat', ROUND(COALESCE(segment_component_allocated.allocated_amount_ex_vat, 0), 2),
          'full_resolved_target_amount_ex_vat', ROUND(COALESCE(segment_component_allocated.derived_amount_ex_vat, 0), 2),
          'resolution_mode', CASE WHEN segment_component_allocated.frozen_resolution_mode IS NULL THEN NULL ELSE segment_component_allocated.frozen_resolution_mode::text END,
          'resolution_payload_json', segment_component_allocated.frozen_resolution_payload_json,
          'resolution_result_json', segment_component_allocated.frozen_resolution_result_json
        )
      ) AS meta_json,
      p_operation_id::text || ':breakdown:' || segment_component_allocated.id::text || ':component:' || segment_component_allocated.component_ordinal::text AS operation_source_key
    FROM segment_component_allocated
  ), fallback_breakdown_rows AS (
    SELECT
      item_page.id AS pay_batch_item_id,
      CASE
        WHEN item_page.item_type = 'SEGMENT_DELTA' THEN 'TS_BUCKET'
        WHEN item_page.item_type = 'MILEAGE_DELTA' THEN 'MILEAGE'
        WHEN item_page.item_type = 'ADJUSTMENT_DELTA' THEN 'ADJUSTMENT'
        WHEN item_page.item_type IN (
          'OVERPAYMENT_RECOVERY',
          'LOAN_REPAYMENT',
          'MANUAL_DEBT_RECOVERY',
          'MANUAL_CREDIT_PAYOUT',
          'LOAN_PAYOUT',
          'UNDERPAYMENT_PAYMENT',
          'DEBT_CREATED'
        ) THEN item_page.item_type
        ELSE 'EXPENSE'
      END AS line_kind,
      NULLIF(UPPER(BTRIM(COALESCE(item_page.frozen_component_snapshot_json->>'bucket_code', item_page.frozen_source_basis_json->>'bucket_code', ''))), '') AS bucket_code,
      COALESCE(NULLIF(BTRIM(item_page.frozen_component_snapshot_json->>'label'), ''), NULLIF(BTRIM(item_page.description), ''), item_page.item_type) AS unit_name,
      CASE WHEN COALESCE(item_page.frozen_component_snapshot_json->>'target_units', item_page.frozen_source_basis_json->>'units', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN COALESCE(item_page.frozen_component_snapshot_json->>'target_units', item_page.frozen_source_basis_json->>'units')::numeric ELSE NULL::numeric END AS units,
      CASE WHEN COALESCE(item_page.frozen_component_snapshot_json->>'target_rate', item_page.frozen_source_basis_json->>'rate', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN COALESCE(item_page.frozen_component_snapshot_json->>'target_rate', item_page.frozen_source_basis_json->>'rate')::numeric ELSE NULL::numeric END AS rate,
      ROUND(COALESCE(item_page.frozen_target_amount_ex_vat, item_page.amount_ex_vat, 0), 2) AS amount_ex_vat,
      ROUND(COALESCE(item_page.frozen_target_amount_vat, item_page.amount_vat, 0), 2) AS amount_vat,
      ROUND(COALESCE(item_page.frozen_target_amount_inc_vat, item_page.amount_inc_vat, 0), 2) AS amount_inc_vat,
      jsonb_strip_nulls(
        jsonb_build_object(
          'finance_case_id', CASE WHEN item_page.finance_case_id IS NULL THEN NULL ELSE item_page.finance_case_id::text END,
          'finance_component_id', CASE WHEN item_page.finance_component_id IS NULL THEN NULL ELSE item_page.finance_component_id::text END,
          'component_key_type', item_page.frozen_component_key_type,
          'component_key_value', item_page.frozen_component_key_value,
          'economic_key_type', item_page.frozen_component_key_type,
          'economic_key_value', item_page.frozen_component_key_value,
          'source_basis_json', item_page.frozen_source_basis_json,
          'frozen_component_snapshot_json', item_page.frozen_component_snapshot_json,
          'source_reservation_amount_ex_vat', public._pay_batch_item_source_reservation_amount_ex_vat(item_page.id),
          'target_amount_ex_vat', COALESCE(item_page.frozen_target_amount_ex_vat, item_page.amount_ex_vat),
          'resolution_mode', CASE WHEN item_page.frozen_resolution_mode IS NULL THEN NULL ELSE item_page.frozen_resolution_mode::text END,
          'resolution_payload_json', item_page.frozen_resolution_payload_json,
          'resolution_result_json', item_page.frozen_resolution_result_json
        )
      ) AS meta_json,
      p_operation_id::text || ':breakdown:' || item_page.id::text AS operation_source_key
    FROM pg_temp.tmp_pay_batch_breakdown_item_page AS item_page
    WHERE item_page.item_type <> 'SEGMENT_DELTA'
       OR NOT EXISTS (
         SELECT 1
         FROM segment_component_source AS existing_component
         WHERE existing_component.id = item_page.id
       )
  ), breakdown_rows AS (
    SELECT * FROM component_breakdown_rows
    UNION ALL
    SELECT * FROM fallback_breakdown_rows
  ), inserted_breakdowns AS (
    INSERT INTO public.pay_batch_item_breakdowns(
      pay_batch_item_id,
      line_kind,
      bucket_code,
      unit_name,
      units,
      rate,
      amount_ex_vat,
      amount_vat,
      amount_inc_vat,
      meta_json,
      operation_source_key
    )
    SELECT
      breakdown_rows.pay_batch_item_id,
      breakdown_rows.line_kind,
      breakdown_rows.bucket_code,
      breakdown_rows.unit_name,
      breakdown_rows.units,
      breakdown_rows.rate,
      breakdown_rows.amount_ex_vat,
      breakdown_rows.amount_vat,
      breakdown_rows.amount_inc_vat,
      breakdown_rows.meta_json,
      breakdown_rows.operation_source_key
    FROM breakdown_rows
    ON CONFLICT (pay_batch_item_id, operation_source_key) WHERE operation_source_key IS NOT NULL DO NOTHING
    RETURNING public.pay_batch_item_breakdowns.id
  )
  SELECT COUNT(*)::integer INTO v_inserted_count FROM inserted_breakdowns;

  SELECT COUNT(*)::integer
  INTO v_reused_count
  FROM public.pay_batch_item_breakdowns AS existing_breakdown
  JOIN pg_temp.tmp_pay_batch_breakdown_item_page AS item_page
    ON item_page.id = existing_breakdown.pay_batch_item_id;

  v_reused_count := GREATEST(COALESCE(v_reused_count, 0) - COALESCE(v_inserted_count, 0), 0);

  PERFORM private.pay_workbench_draft_expected_effects_v1(
    p_operation_id,'BUILD_ITEM_BREAKDOWNS','ASSERT_COMPLETE','[]'::jsonb,
    jsonb_build_object('pay_batch_id',p_pay_batch_id)
  );

  RETURN jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'operation_id', p_operation_id::text,
    'candidate_scope_count', v_scope_id_count,
    'inserted_count', COALESCE(v_inserted_count, 0),
    'reused_count', COALESCE(v_reused_count, 0),
    'failed_count', 0,
    'inserted_breakdown_rows', COALESCE(v_inserted_count, 0),
    'has_more', EXISTS (
      SELECT 1
      FROM public.pay_batch_items AS remaining_item
      JOIN public.pay_batch_candidates AS remaining_candidate
        ON remaining_candidate.id = remaining_item.pay_batch_candidate_id
      JOIN public.banking_pay_operation_candidate_allocation_rows AS remaining_allocation
        ON remaining_allocation.pay_batch_item_id = remaining_item.id
       AND remaining_allocation.operation_id = p_operation_id
      WHERE remaining_candidate.pay_batch_id = p_pay_batch_id
        AND remaining_allocation.candidate_scope_id IN (
          SELECT (supplied_scope.scope_value #>> '{}')::uuid
          FROM jsonb_array_elements(v_scope_ids) AS supplied_scope(scope_value)
        )
        AND COALESCE(remaining_item.is_voided, false) = false
        AND NOT EXISTS (
          SELECT 1
          FROM public.pay_batch_item_breakdowns AS remaining_breakdown
          WHERE remaining_breakdown.pay_batch_item_id = remaining_item.id
        )
    )
  );
END;
$function$;

