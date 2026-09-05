-- Set-wise execution repair for the existing post-Draft economic-component owner.
-- Runtime authority is Miget TEST. The `supabase` directory name is historical only.
-- This replacement preserves every key/amount/Policy X decision and removes only
-- the per-item re-read performed inside the already set-oriented owner.
CREATE OR REPLACE FUNCTION public._pay_batch_item_economic_components(p_pay_batch_id uuid DEFAULT NULL::uuid, p_pay_batch_item_ids uuid[] DEFAULT NULL::uuid[])
 RETURNS TABLE(pay_batch_id uuid, pay_batch_item_id uuid, timesheet_id uuid, item_type text, key_type text, key_value text, source_amount_ex_vat numeric, source_amount_inc_vat numeric, target_amount_ex_vat numeric, key_resolution_source text, key_resolution_failure_reason text)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  IF p_pay_batch_id IS NULL
     AND COALESCE(array_length(p_pay_batch_item_ids, 1), 0) = 0 THEN
    RAISE EXCEPTION '_pay_batch_item_economic_components requires p_pay_batch_id or p_pay_batch_item_ids';
  END IF;

  RETURN QUERY
  WITH input_item_ids AS (
    SELECT DISTINCT input_ids.item_id
    FROM unnest(COALESCE(p_pay_batch_item_ids, ARRAY[]::uuid[])) AS input_ids(item_id)
    WHERE input_ids.item_id IS NOT NULL
  ),
  base_items AS (
    SELECT
      pay_batch_candidate_row.pay_batch_id AS batch_id,
      pay_batch_item_row.id AS batch_item_id,
      pay_batch_item_row.timesheet_id AS item_timesheet_id,
      UPPER(NULLIF(BTRIM(COALESCE(pay_batch_item_row.item_type, '')), '')) AS item_type_norm,
      pay_batch_item_row.item_type AS item_type_raw,
      pay_batch_item_row.segment_key AS item_segment_key,
      pay_batch_item_row.source_ref AS item_source_ref,
      pay_batch_item_row.amount_ex_vat AS item_amount_ex_vat,
      pay_batch_item_row.amount_inc_vat AS item_amount_inc_vat,
      pay_batch_item_row.finance_case_id AS item_finance_case_id,
      pay_batch_item_row.finance_component_id AS item_finance_component_id,
      pay_batch_item_row.frozen_target_amount_ex_vat AS item_frozen_target_amount_ex_vat,
      pay_batch_item_row.frozen_source_amount AS item_frozen_source_amount,
      pay_batch_item_row.frozen_resolution_mode AS item_frozen_resolution_mode,
      pay_batch_item_row.frozen_resolution_payload_json AS item_frozen_resolution_payload_json,
      pay_batch_item_row.frozen_resolution_result_json AS item_frozen_resolution_result_json,
      pay_batch_item_row.frozen_component_key_type AS item_frozen_key_type,
      pay_batch_item_row.frozen_component_key_value AS item_frozen_key_value,
      CASE
        WHEN jsonb_typeof(COALESCE(pay_batch_item_row.frozen_component_snapshot_json, '{}'::jsonb)) = 'object'
          THEN COALESCE(pay_batch_item_row.frozen_component_snapshot_json, '{}'::jsonb)
        ELSE '{}'::jsonb
      END AS item_frozen_component_snapshot_json,
      CASE
        WHEN jsonb_typeof(COALESCE(pay_batch_item_row.frozen_source_basis_json, '{}'::jsonb)) = 'object'
          THEN COALESCE(pay_batch_item_row.frozen_source_basis_json, '{}'::jsonb)
        ELSE '{}'::jsonb
      END AS item_frozen_source_basis_json
    FROM public.pay_batch_items AS pay_batch_item_row
    JOIN public.pay_batch_candidates AS pay_batch_candidate_row
      ON pay_batch_candidate_row.id = pay_batch_item_row.pay_batch_candidate_id
    WHERE (p_pay_batch_id IS NULL OR pay_batch_candidate_row.pay_batch_id = p_pay_batch_id)
      AND (
        COALESCE(array_length(p_pay_batch_item_ids, 1), 0) = 0
        OR EXISTS (
          SELECT 1
          FROM input_item_ids AS input_filter
          WHERE input_filter.item_id = pay_batch_item_row.id
        )
      )
  ),
  single_breakdown_meta AS (
    SELECT
      breakdown_row.pay_batch_item_id AS batch_item_id,
      COUNT(*)::integer AS breakdown_count,
      CASE
        WHEN COUNT(*) = 1 THEN (ARRAY_AGG(breakdown_row.meta_json ORDER BY breakdown_row.id))[1]
        ELSE '{}'::jsonb
      END AS single_meta_json
    FROM public.pay_batch_item_breakdowns AS breakdown_row
    JOIN base_items AS base_for_breakdown
      ON base_for_breakdown.batch_item_id = breakdown_row.pay_batch_item_id
    GROUP BY breakdown_row.pay_batch_item_id
  ),
  snapshot_choice AS (
    SELECT
      distinct_item_pairs.batch_id,
      distinct_item_pairs.item_timesheet_id,
      (
        SELECT snapshot_row.target_snapshot_json
        FROM public.pay_batch_timesheet_snapshots AS snapshot_row
        WHERE snapshot_row.pay_batch_id = distinct_item_pairs.batch_id
          AND snapshot_row.timesheet_id = distinct_item_pairs.item_timesheet_id
        ORDER BY snapshot_row.created_at_utc DESC, snapshot_row.id DESC
        LIMIT 1
      ) AS target_snapshot_json
    FROM (
      SELECT DISTINCT
        base_item_rows.batch_id,
        base_item_rows.item_timesheet_id
      FROM base_items AS base_item_rows
      WHERE base_item_rows.item_timesheet_id IS NOT NULL
    ) AS distinct_item_pairs
  ),
  prepared_items AS (
    SELECT
      base_item_rows.batch_id,
      base_item_rows.batch_item_id,
      base_item_rows.item_timesheet_id,
      base_item_rows.item_type_norm,
      base_item_rows.item_type_raw,
      base_item_rows.item_segment_key,
      base_item_rows.item_source_ref,
      base_item_rows.item_amount_ex_vat,
      base_item_rows.item_amount_inc_vat,
      base_item_rows.item_finance_case_id,
      base_item_rows.item_finance_component_id,
      base_item_rows.item_frozen_target_amount_ex_vat,
      base_item_rows.item_frozen_source_amount,
      base_item_rows.item_frozen_resolution_mode,
      base_item_rows.item_frozen_resolution_payload_json,
      base_item_rows.item_frozen_resolution_result_json,
      base_item_rows.item_frozen_key_type,
      base_item_rows.item_frozen_key_value,
      base_item_rows.item_frozen_component_snapshot_json,
      base_item_rows.item_frozen_source_basis_json,
      CASE
        WHEN COALESCE(breakdown_meta.breakdown_count, 0) = 1
         AND jsonb_typeof(COALESCE(breakdown_meta.single_meta_json, '{}'::jsonb)) = 'object'
          THEN COALESCE(breakdown_meta.single_meta_json, '{}'::jsonb)
        ELSE '{}'::jsonb
      END AS item_single_breakdown_meta_json,
      CASE
        WHEN jsonb_typeof(COALESCE(snapshot_choice_rows.target_snapshot_json, '{}'::jsonb)) = 'object'
          THEN COALESCE(snapshot_choice_rows.target_snapshot_json, '{}'::jsonb)
        ELSE '{}'::jsonb
      END AS target_snapshot_json
    FROM base_items AS base_item_rows
    LEFT JOIN single_breakdown_meta AS breakdown_meta
      ON breakdown_meta.batch_item_id = base_item_rows.batch_item_id
    LEFT JOIN snapshot_choice AS snapshot_choice_rows
      ON snapshot_choice_rows.batch_id = base_item_rows.batch_id
     AND snapshot_choice_rows.item_timesheet_id = base_item_rows.item_timesheet_id
  ),
  resolved_items AS (
    SELECT
      prepared_item_rows.batch_id,
      prepared_item_rows.batch_item_id,
      prepared_item_rows.item_timesheet_id,
      prepared_item_rows.item_type_norm,
      prepared_item_rows.item_type_raw,
      prepared_item_rows.item_amount_ex_vat,
      prepared_item_rows.item_amount_inc_vat,
      prepared_item_rows.item_finance_case_id,
      prepared_item_rows.item_finance_component_id,
      prepared_item_rows.item_frozen_target_amount_ex_vat,
      prepared_item_rows.item_frozen_source_amount,
      prepared_item_rows.item_frozen_resolution_mode,
      prepared_item_rows.item_frozen_resolution_payload_json,
      prepared_item_rows.item_frozen_resolution_result_json,
      prepared_item_rows.item_frozen_component_snapshot_json,
      prepared_item_rows.item_frozen_source_basis_json,
      prepared_item_rows.item_single_breakdown_meta_json,
      prepared_item_rows.target_snapshot_json,
      resolved_key_rows.key_type AS resolved_key_type,
      resolved_key_rows.key_value AS resolved_key_value,
      resolved_key_rows.key_resolution_source AS resolved_key_source,
      resolved_key_rows.key_resolution_failure_reason AS resolved_key_failure_reason
    FROM prepared_items AS prepared_item_rows
    LEFT JOIN LATERAL public._pay_policy_x_assert_economic_key(
      p_timesheet_id => prepared_item_rows.item_timesheet_id,
      p_key_type => prepared_item_rows.item_frozen_key_type,
      p_key_value => prepared_item_rows.item_frozen_key_value,
      p_context => 'POST_DRAFT_RESOLVE_FROZEN_ITEM_KEY',
      p_authority_scope => 'POST_DRAFT',
      p_resolution_source => 'FROZEN_ITEM_KEY',
      p_required => true,
      p_source_json => jsonb_build_object(
        'pay_batch_item_id',prepared_item_rows.batch_item_id::text,
        'pay_batch_id',prepared_item_rows.batch_id::text,
        'authority_scope','POST_DRAFT'
      )
    ) AS frozen_key_check
      ON prepared_item_rows.item_frozen_key_type IS NOT NULL
      OR prepared_item_rows.item_frozen_key_value IS NOT NULL
    JOIN LATERAL (
      SELECT frozen_key_check.key_type,
             frozen_key_check.key_value,
             'FROZEN_ITEM_KEY'::text AS key_resolution_source,
             NULL::text AS key_resolution_failure_reason
      WHERE COALESCE(frozen_key_check.ok,false)

      UNION ALL

      SELECT fallback_key.key_type,
             fallback_key.key_value,
             fallback_key.key_resolution_source,
             fallback_key.key_resolution_failure_reason
      FROM public._pay_policy_x_resolve_post_draft_economic_key(
        p_pay_batch_item_id => prepared_item_rows.batch_item_id,
        p_pay_batch_id => prepared_item_rows.batch_id,
        p_timesheet_id => prepared_item_rows.item_timesheet_id,
        p_item_type => prepared_item_rows.item_type_norm,
        p_frozen_key_type => prepared_item_rows.item_frozen_key_type,
        p_frozen_key_value => prepared_item_rows.item_frozen_key_value,
        p_frozen_component_snapshot_json => prepared_item_rows.item_frozen_component_snapshot_json,
        p_frozen_source_basis_json => prepared_item_rows.item_frozen_source_basis_json,
        p_breakdown_meta_json => prepared_item_rows.item_single_breakdown_meta_json,
        p_target_snapshot_json => prepared_item_rows.target_snapshot_json
      ) AS fallback_key
      WHERE NOT COALESCE(frozen_key_check.ok,false)
      LIMIT 1
    ) AS resolved_key_rows ON true
  ),
  amount_candidates AS (
    SELECT
      resolved_item_rows.batch_id,
      resolved_item_rows.batch_item_id,
      resolved_item_rows.item_timesheet_id,
      resolved_item_rows.item_type_norm,
      resolved_item_rows.item_type_raw,
      resolved_item_rows.item_amount_ex_vat,
      resolved_item_rows.item_amount_inc_vat,
      resolved_item_rows.resolved_key_type,
      resolved_item_rows.resolved_key_value,
      resolved_item_rows.resolved_key_source,
      resolved_item_rows.resolved_key_failure_reason,
      (
           resolved_item_rows.item_finance_case_id IS NOT NULL
        OR resolved_item_rows.item_finance_component_id IS NOT NULL
        OR resolved_item_rows.item_frozen_target_amount_ex_vat IS NOT NULL
        OR resolved_item_rows.item_frozen_resolution_mode IS NOT NULL
        OR resolved_item_rows.item_frozen_resolution_payload_json IS NOT NULL
        OR resolved_item_rows.item_frozen_resolution_result_json IS NOT NULL
        OR resolved_item_rows.item_frozen_component_snapshot_json ? 'target_amount_ex_vat'
        OR resolved_item_rows.item_frozen_component_snapshot_json ? 'target_pay_ex_vat'
        OR resolved_item_rows.item_frozen_component_snapshot_json ? 'target_pay_amount_ex_vat'
        OR resolved_item_rows.item_frozen_component_snapshot_json ? 'frozen_target_amount_ex_vat'
        OR resolved_item_rows.item_frozen_component_snapshot_json ? 'target_rate'
        OR resolved_item_rows.item_frozen_component_snapshot_json ? 'target_units'
        OR resolved_item_rows.item_frozen_component_snapshot_json ? 'resolution_mode'
        OR resolved_item_rows.item_frozen_component_snapshot_json ? 'saved_resolution_mode'
        OR resolved_item_rows.item_frozen_component_snapshot_json ? 'saved_resolution_payload_json'
        OR resolved_item_rows.item_frozen_component_snapshot_json ? 'saved_resolution_result_json'
        OR resolved_item_rows.item_single_breakdown_meta_json ? 'target_amount_ex_vat'
        OR resolved_item_rows.item_single_breakdown_meta_json ? 'target_pay_ex_vat'
        OR resolved_item_rows.item_single_breakdown_meta_json ? 'target_rate'
        OR resolved_item_rows.item_single_breakdown_meta_json ? 'resolution_mode'
      ) AS item_has_source_target_split,
      CASE
        -- The scalar owner first prefers a fresh resolved case-component amount,
        -- then the frozen source amount. When there is provably no eligible
        -- resolved component, use that same frozen value already loaded by this
        -- set-oriented owner instead of selecting the item, breakdown and key a
        -- second time. Every other shape retains the exact historical owner.
        WHEN resolved_item_rows.item_type_norm IN (
               'SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA'
             )
         AND resolved_item_rows.resolved_key_failure_reason IS NULL
         AND resolved_item_rows.resolved_key_type IS NOT NULL
         AND BTRIM(resolved_item_rows.resolved_key_type) <> ''
         AND resolved_item_rows.resolved_key_value IS NOT NULL
         AND BTRIM(resolved_item_rows.resolved_key_value) <> ''
         AND NOT (
           resolved_item_rows.resolved_key_type = 'TS_DAY'
           AND resolved_item_rows.resolved_key_value !~ '^\d{4}-\d{2}-\d{2}$'
         )
         AND resolved_item_rows.item_frozen_source_amount IS NOT NULL
         AND NOT EXISTS (
           SELECT 1
           FROM jsonb_array_elements(
             CASE
               WHEN jsonb_typeof(
                      resolved_item_rows.item_frozen_resolution_payload_json->'case_components'
                    ) = 'array'
                 THEN resolved_item_rows.item_frozen_resolution_payload_json->'case_components'
               ELSE '[]'::jsonb
             END
           ) AS frozen_resolution_component(component_json)
           WHERE UPPER(BTRIM(COALESCE(
                   frozen_resolution_component.component_json->>'component_key_type',''
                 ))) = resolved_item_rows.resolved_key_type
             AND BTRIM(COALESCE(
                   frozen_resolution_component.component_json->>'component_key_value',''
                 )) = resolved_item_rows.resolved_key_value
             AND LOWER(BTRIM(COALESCE(
                   frozen_resolution_component.component_json->>'is_resolution_stale','false'
                 ))) NOT IN ('true','t','1','yes','y','on')
             AND LOWER(BTRIM(COALESCE(
                   frozen_resolution_component.component_json->>'is_stale_saved_resolution','false'
                 ))) NOT IN ('true','t','1','yes','y','on')
             AND LOWER(BTRIM(COALESCE(
                   frozen_resolution_component.component_json->>'requires_resolution','true'
                 ))) IN ('false','f','0','no','n','off')
             AND NULLIF(BTRIM(COALESCE(
                   frozen_resolution_component.component_json->>'resolved_rate_resolution_id',''
                 )), '') IS NOT NULL
             AND COALESCE(
                   frozen_resolution_component.component_json->>'source_pay_ex_vat',''
                 ) ~ '^-?[0-9]+(\.[0-9]+)?$'
         )
          THEN ROUND(ABS(resolved_item_rows.item_frozen_source_amount),2)::numeric(12,2)
        ELSE public._pay_batch_item_source_reservation_amount_ex_vat(
               resolved_item_rows.batch_item_id
             )
      END AS entitlement_source_amount_ex_vat,
      (
        SELECT target_candidates.target_text_value::numeric
        FROM (
          VALUES
            (resolved_item_rows.item_frozen_target_amount_ex_vat::text),
            (resolved_item_rows.item_amount_ex_vat::text),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'frozen_target_amount_ex_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'target_amount_ex_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'target_pay_ex_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'target_pay_amount_ex_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'amount_ex_vat'),
            (resolved_item_rows.item_single_breakdown_meta_json->>'target_amount_ex_vat'),
            (resolved_item_rows.item_single_breakdown_meta_json->>'target_pay_ex_vat'),
            (resolved_item_rows.item_single_breakdown_meta_json->>'amount_ex_vat')
        ) AS target_candidates(target_text_value)
        WHERE target_candidates.target_text_value IS NOT NULL
          AND BTRIM(target_candidates.target_text_value) ~ '^-?[0-9]+(\.[0-9]+)?$'
        LIMIT 1
      ) AS raw_target_amount_ex_vat,
      (
        SELECT source_candidates.source_text_value::numeric
        FROM (
          VALUES
            (resolved_item_rows.item_frozen_source_amount::text),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'source_reservation_amount_ex_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'source_entitlement_amount_ex_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'source_amount_ex_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'source_pay_ex_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'source_pay_amount_ex_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'basis_source_amount_ex_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'reserved_source_amount'),
            (resolved_item_rows.item_frozen_component_snapshot_json#>>'{source_basis_json,source_reservation_amount_ex_vat}'),
            (resolved_item_rows.item_frozen_component_snapshot_json#>>'{source_basis_json,source_entitlement_amount_ex_vat}'),
            (resolved_item_rows.item_frozen_component_snapshot_json#>>'{source_basis_json,source_amount_ex_vat}'),
            (resolved_item_rows.item_frozen_component_snapshot_json#>>'{source_basis_json,source_pay_ex_vat}'),
            (resolved_item_rows.item_frozen_source_basis_json->>'source_reservation_amount_ex_vat'),
            (resolved_item_rows.item_frozen_source_basis_json->>'source_entitlement_amount_ex_vat'),
            (resolved_item_rows.item_frozen_source_basis_json->>'source_amount_ex_vat'),
            (resolved_item_rows.item_frozen_source_basis_json->>'source_pay_ex_vat'),
            (resolved_item_rows.item_frozen_source_basis_json->>'pay_ex_vat'),
            (resolved_item_rows.item_frozen_source_basis_json->>'amount_ex_vat'),
            (resolved_item_rows.item_frozen_resolution_payload_json->>'source_reservation_amount_ex_vat'),
            (resolved_item_rows.item_frozen_resolution_payload_json->>'source_entitlement_amount_ex_vat'),
            (resolved_item_rows.item_frozen_resolution_payload_json->>'source_amount_ex_vat'),
            (resolved_item_rows.item_frozen_resolution_payload_json->>'source_pay_ex_vat'),
            (resolved_item_rows.item_frozen_resolution_result_json->>'source_reservation_amount_ex_vat'),
            (resolved_item_rows.item_frozen_resolution_result_json->>'source_entitlement_amount_ex_vat'),
            (resolved_item_rows.item_frozen_resolution_result_json->>'source_amount_ex_vat'),
            (resolved_item_rows.item_frozen_resolution_result_json->>'source_pay_ex_vat'),
            (resolved_item_rows.item_single_breakdown_meta_json->>'source_reservation_amount_ex_vat'),
            (resolved_item_rows.item_single_breakdown_meta_json->>'source_entitlement_amount_ex_vat'),
            (resolved_item_rows.item_single_breakdown_meta_json->>'source_amount_ex_vat'),
            (resolved_item_rows.item_single_breakdown_meta_json->>'source_pay_ex_vat')
        ) AS source_candidates(source_text_value)
        WHERE source_candidates.source_text_value IS NOT NULL
          AND BTRIM(source_candidates.source_text_value) ~ '^-?[0-9]+(\.[0-9]+)?$'
        LIMIT 1
      ) AS raw_artifact_source_amount_ex_vat,
      (
        SELECT source_inc_candidates.source_inc_text_value::numeric
        FROM (
          VALUES
            (resolved_item_rows.item_frozen_source_basis_json->>'source_amount_inc_vat'),
            (resolved_item_rows.item_frozen_source_basis_json->>'source_pay_inc_vat'),
            (resolved_item_rows.item_frozen_source_basis_json->>'source_entitlement_amount_inc_vat'),
            (resolved_item_rows.item_frozen_source_basis_json->>'source_reservation_amount_inc_vat'),
            (resolved_item_rows.item_frozen_source_basis_json->>'pay_inc_vat'),
            (resolved_item_rows.item_frozen_source_basis_json->>'amount_inc_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'source_amount_inc_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'source_pay_inc_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'source_entitlement_amount_inc_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'source_reservation_amount_inc_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'basis_source_amount_inc_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'reserved_source_amount_inc_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json#>>'{source_basis_json,source_amount_inc_vat}'),
            (resolved_item_rows.item_frozen_component_snapshot_json#>>'{source_basis_json,source_pay_inc_vat}'),
            (resolved_item_rows.item_frozen_component_snapshot_json#>>'{source_basis_json,source_entitlement_amount_inc_vat}'),
            (resolved_item_rows.item_frozen_component_snapshot_json#>>'{source_basis_json,source_reservation_amount_inc_vat}'),
            (resolved_item_rows.item_frozen_component_snapshot_json#>>'{source_basis_json,pay_inc_vat}'),
            (resolved_item_rows.item_frozen_component_snapshot_json#>>'{source_basis_json,amount_inc_vat}'),
            (resolved_item_rows.item_frozen_resolution_payload_json->>'source_amount_inc_vat'),
            (resolved_item_rows.item_frozen_resolution_payload_json->>'source_pay_inc_vat'),
            (resolved_item_rows.item_frozen_resolution_payload_json->>'source_entitlement_amount_inc_vat'),
            (resolved_item_rows.item_frozen_resolution_payload_json->>'source_reservation_amount_inc_vat'),
            (resolved_item_rows.item_frozen_resolution_result_json->>'source_amount_inc_vat'),
            (resolved_item_rows.item_frozen_resolution_result_json->>'source_pay_inc_vat'),
            (resolved_item_rows.item_frozen_resolution_result_json->>'source_entitlement_amount_inc_vat'),
            (resolved_item_rows.item_frozen_resolution_result_json->>'source_reservation_amount_inc_vat'),
            (resolved_item_rows.item_single_breakdown_meta_json->>'source_amount_inc_vat'),
            (resolved_item_rows.item_single_breakdown_meta_json->>'source_pay_inc_vat'),
            (resolved_item_rows.item_single_breakdown_meta_json->>'source_entitlement_amount_inc_vat'),
            (resolved_item_rows.item_single_breakdown_meta_json->>'source_reservation_amount_inc_vat')
        ) AS source_inc_candidates(source_inc_text_value)
        WHERE source_inc_candidates.source_inc_text_value IS NOT NULL
          AND BTRIM(source_inc_candidates.source_inc_text_value) ~ '^-?[0-9]+(\.[0-9]+)?$'
        LIMIT 1
      ) AS raw_artifact_source_amount_inc_vat
    FROM resolved_items AS resolved_item_rows
  ),
  final_rows AS (
    SELECT
      amount_candidate_rows.batch_id,
      amount_candidate_rows.batch_item_id,
      amount_candidate_rows.item_timesheet_id,
      amount_candidate_rows.item_type_raw,
      amount_candidate_rows.resolved_key_type,
      amount_candidate_rows.resolved_key_value,
      amount_candidate_rows.resolved_key_source,
      amount_candidate_rows.resolved_key_failure_reason,
      ROUND(ABS(
        CASE
          WHEN amount_candidate_rows.item_type_norm IN ('SEGMENT_DELTA', 'EXPENSE_DELTA', 'ADJUSTMENT_DELTA', 'MILEAGE_DELTA')
            THEN COALESCE(
              amount_candidate_rows.entitlement_source_amount_ex_vat,
              amount_candidate_rows.raw_artifact_source_amount_ex_vat,
              CASE
                WHEN amount_candidate_rows.item_has_source_target_split THEN NULL::numeric
                ELSE COALESCE(amount_candidate_rows.raw_target_amount_ex_vat, amount_candidate_rows.item_amount_ex_vat)
              END
            )
          ELSE COALESCE(amount_candidate_rows.raw_artifact_source_amount_ex_vat, amount_candidate_rows.raw_target_amount_ex_vat, amount_candidate_rows.item_amount_ex_vat)
        END
      ), 2)::numeric AS final_source_amount_ex_vat,
      ROUND(ABS(COALESCE(
        amount_candidate_rows.raw_artifact_source_amount_inc_vat,
        CASE
          WHEN amount_candidate_rows.item_has_source_target_split THEN NULL::numeric
          ELSE amount_candidate_rows.item_amount_inc_vat
        END,
        CASE
          WHEN amount_candidate_rows.item_type_norm IN ('SEGMENT_DELTA', 'EXPENSE_DELTA', 'ADJUSTMENT_DELTA', 'MILEAGE_DELTA')
            THEN COALESCE(
              amount_candidate_rows.entitlement_source_amount_ex_vat,
              amount_candidate_rows.raw_artifact_source_amount_ex_vat,
              CASE
                WHEN amount_candidate_rows.item_has_source_target_split THEN NULL::numeric
                ELSE COALESCE(amount_candidate_rows.raw_target_amount_ex_vat, amount_candidate_rows.item_amount_ex_vat)
              END
            )
          ELSE COALESCE(amount_candidate_rows.raw_artifact_source_amount_ex_vat, amount_candidate_rows.raw_target_amount_ex_vat, amount_candidate_rows.item_amount_ex_vat)
        END
      )), 2)::numeric AS final_source_amount_inc_vat,
      ROUND(ABS(COALESCE(
        amount_candidate_rows.raw_target_amount_ex_vat,
        amount_candidate_rows.item_amount_ex_vat,
        CASE
          WHEN amount_candidate_rows.item_type_norm IN ('SEGMENT_DELTA', 'EXPENSE_DELTA', 'ADJUSTMENT_DELTA', 'MILEAGE_DELTA')
            THEN COALESCE(amount_candidate_rows.entitlement_source_amount_ex_vat, amount_candidate_rows.raw_artifact_source_amount_ex_vat)
          ELSE amount_candidate_rows.raw_artifact_source_amount_ex_vat
        END
      )), 2)::numeric AS final_target_amount_ex_vat,
      CASE
        WHEN amount_candidate_rows.resolved_key_failure_reason IS NOT NULL
          THEN amount_candidate_rows.resolved_key_failure_reason
        WHEN amount_candidate_rows.resolved_key_type IS NULL OR BTRIM(COALESCE(amount_candidate_rows.resolved_key_type, '')) = ''
          THEN 'POST_DRAFT_KEY_RESOLUTION_FAILED'
        WHEN amount_candidate_rows.resolved_key_value IS NULL OR BTRIM(COALESCE(amount_candidate_rows.resolved_key_value, '')) = ''
          THEN 'POST_DRAFT_KEY_RESOLUTION_FAILED'
        WHEN amount_candidate_rows.resolved_key_type = 'TS_DAY'
         AND amount_candidate_rows.resolved_key_value !~ '^\d{4}-\d{2}-\d{2}$'
          THEN 'TS_DAY_KEY_VALUE_NOT_DATE'
        WHEN ROUND(ABS(
          CASE
            WHEN amount_candidate_rows.item_type_norm IN ('SEGMENT_DELTA', 'EXPENSE_DELTA', 'ADJUSTMENT_DELTA', 'MILEAGE_DELTA')
              THEN COALESCE(
                amount_candidate_rows.entitlement_source_amount_ex_vat,
                amount_candidate_rows.raw_artifact_source_amount_ex_vat,
                CASE
                  WHEN amount_candidate_rows.item_has_source_target_split THEN NULL::numeric
                  ELSE COALESCE(amount_candidate_rows.raw_target_amount_ex_vat, amount_candidate_rows.item_amount_ex_vat)
                END
              )
            ELSE COALESCE(amount_candidate_rows.raw_artifact_source_amount_ex_vat, amount_candidate_rows.raw_target_amount_ex_vat, amount_candidate_rows.item_amount_ex_vat)
          END
        ), 2) IS NULL
          THEN 'SOURCE_AMOUNT_NOT_RESOLVED'
        WHEN ROUND(ABS(COALESCE(
          amount_candidate_rows.raw_target_amount_ex_vat,
          amount_candidate_rows.item_amount_ex_vat,
          CASE
            WHEN amount_candidate_rows.item_type_norm IN ('SEGMENT_DELTA', 'EXPENSE_DELTA', 'ADJUSTMENT_DELTA', 'MILEAGE_DELTA')
              THEN COALESCE(amount_candidate_rows.entitlement_source_amount_ex_vat, amount_candidate_rows.raw_artifact_source_amount_ex_vat)
            ELSE amount_candidate_rows.raw_artifact_source_amount_ex_vat
          END
        )), 2) IS NULL
          THEN 'TARGET_AMOUNT_NOT_RESOLVED'
        ELSE NULL::text
      END AS final_failure_reason
    FROM amount_candidates AS amount_candidate_rows
  )
  SELECT
    final_rows.batch_id AS pay_batch_id,
    final_rows.batch_item_id AS pay_batch_item_id,
    final_rows.item_timesheet_id AS timesheet_id,
    final_rows.item_type_raw AS item_type,
    CASE WHEN final_rows.final_failure_reason IS NULL THEN final_rows.resolved_key_type ELSE NULL::text END AS key_type,
    CASE WHEN final_rows.final_failure_reason IS NULL THEN final_rows.resolved_key_value ELSE NULL::text END AS key_value,
    CASE WHEN final_rows.final_source_amount_ex_vat IS NULL THEN NULL::numeric ELSE ROUND(final_rows.final_source_amount_ex_vat, 2)::numeric END AS source_amount_ex_vat,
    CASE WHEN final_rows.final_source_amount_inc_vat IS NULL THEN NULL::numeric ELSE ROUND(final_rows.final_source_amount_inc_vat, 2)::numeric END AS source_amount_inc_vat,
    CASE WHEN final_rows.final_target_amount_ex_vat IS NULL THEN NULL::numeric ELSE ROUND(final_rows.final_target_amount_ex_vat, 2)::numeric END AS target_amount_ex_vat,
    CASE WHEN final_rows.final_failure_reason IS NULL THEN final_rows.resolved_key_source ELSE 'KEY_RESOLUTION_FAILED' END AS key_resolution_source,
    final_rows.final_failure_reason AS key_resolution_failure_reason
  FROM final_rows
  ORDER BY final_rows.batch_id, final_rows.batch_item_id;
END;
$function$
;

ALTER FUNCTION public._pay_batch_item_economic_components(uuid, uuid[]) OWNER TO postgres;
REVOKE ALL ON FUNCTION public._pay_batch_item_economic_components(uuid, uuid[]) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public._pay_batch_item_economic_components(uuid, uuid[]) TO service_role;
