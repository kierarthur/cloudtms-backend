CREATE OR REPLACE FUNCTION public.pay_batch_validate_freshness(
  p_pay_batch_id uuid,
  p_actor_user_id uuid,
  p_allow_large_full_scan boolean DEFAULT false
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_base_result jsonb;
  v_base_diff jsonb := '[]'::jsonb;
  v_filtered_diff jsonb := '[]'::jsonb;
  v_reasons jsonb := '[]'::jsonb;
  v_fresh_chain_components jsonb := '[]'::jsonb;
  v_original_key_diff_count integer := 0;
  v_suppressed_key_diff_count integer := 0;
  v_remaining_key_diff_count integer := 0;
BEGIN
  v_base_result := public._pay_batch_validate_freshness_base_v1(
    p_pay_batch_id,
    p_actor_user_id,
    p_allow_large_full_scan
  );

  v_base_diff := COALESCE(v_base_result->'diff', '[]'::jsonb);
  v_reasons := COALESCE(v_base_result->'stale_reasons', '[]'::jsonb);

  /*
   * A correction-chain item freezes its complete residual evidence when the
   * draft is created. The base freshness validator intentionally uses the
   * ordinary timesheet entitlement comparison for all economic rows. That
   * ordinary comparison cannot distinguish a correction-chain residual from
   * its root timesheet and therefore sees this batch's own reservation as a
   * change.
   *
   * Reconstruct the pre-current-batch correction-chain residual through the
   * bounded Policy X helper. Suppress only the exact base diff when every
   * frozen component still equals the reconstructed component. Any helper
   * error or real component change fails closed and leaves the base stale
   * result untouched.
   */
  WITH correction_items AS (
    SELECT DISTINCT ON (
      correction_item.source_family_key,
      correction_item.candidate_id,
      correction_item.target_pay_method
    )
      correction_item.pay_batch_item_id,
      correction_item.root_timesheet_id,
      correction_item.candidate_id,
      correction_item.source_family_key,
      correction_item.target_pay_method,
      correction_item.frozen_residual
    FROM (
      SELECT
        pay_batch_item.id AS pay_batch_item_id,
        COALESCE(
          NULLIF(
            COALESCE(
              pay_batch_item.frozen_source_basis_json
                #>> '{correction_chain_residual,root_timesheet_id}',
              pay_batch_item.frozen_component_snapshot_json
                #>> '{correction_chain_residual,root_timesheet_id}'
            ),
            ''
          )::uuid,
          CASE
            WHEN COALESCE(
              pay_batch_item.frozen_source_basis_json->>'source_family_key',
              pay_batch_item.frozen_component_snapshot_json->>'source_family_key',
              ''
            ) ~ '^correction-chain:[0-9a-fA-F-]{36}$'
              THEN replace(
                COALESCE(
                  pay_batch_item.frozen_source_basis_json->>'source_family_key',
                  pay_batch_item.frozen_component_snapshot_json->>'source_family_key'
                ),
                'correction-chain:',
                ''
              )::uuid
            ELSE pay_batch_item.timesheet_id
          END
        ) AS root_timesheet_id,
        pay_batch_candidate.candidate_id,
        NULLIF(BTRIM(COALESCE(
          pay_batch_item.frozen_source_basis_json->>'source_family_key',
          pay_batch_item.frozen_component_snapshot_json->>'source_family_key',
          ''
        )), '') AS source_family_key,
        UPPER(NULLIF(BTRIM(COALESCE(
          pay_batch_item.frozen_target_pay_method,
          pay_batch_item.pay_channel,
          pay_batch.batch_kind_fixed,
          ''
        )), '')) AS target_pay_method,
        COALESCE(
          pay_batch_item.frozen_source_basis_json->'correction_chain_residual',
          pay_batch_item.frozen_component_snapshot_json->'correction_chain_residual'
        ) AS frozen_residual
      FROM public.pay_batch_items AS pay_batch_item
      JOIN public.pay_batch_candidates AS pay_batch_candidate
        ON pay_batch_candidate.id = pay_batch_item.pay_batch_candidate_id
      JOIN public.pay_batches AS pay_batch
        ON pay_batch.id = pay_batch_candidate.pay_batch_id
      WHERE pay_batch_candidate.pay_batch_id = p_pay_batch_id
        AND COALESCE(pay_batch_item.is_voided, false) = false
        AND COALESCE(
          NULLIF(BTRIM(COALESCE(
            pay_batch_item.frozen_source_basis_json->>'source_family_key',
            pay_batch_item.frozen_component_snapshot_json->>'source_family_key',
            ''
          )), ''),
          ''
        ) LIKE 'correction-chain:%'
        /*
         * Filter to the row that actually owns the frozen chain snapshot
         * before DISTINCT ON chooses one row for the family. A recovery item
         * can share the family key without carrying the snapshot; allowing it
         * into the ordering can hide the valid correction carrier entirely.
         */
        AND jsonb_typeof(
          COALESCE(
            pay_batch_item.frozen_source_basis_json->'correction_chain_residual',
            pay_batch_item.frozen_component_snapshot_json->'correction_chain_residual'
          )
        ) = 'object'
        AND NOT EXISTS (
          SELECT 1
          FROM public.pay_payment_correction_items AS correction_item_exclusion
          WHERE correction_item_exclusion.pay_batch_item_id = pay_batch_item.id
            AND correction_item_exclusion.status = 'APPLIED'
            AND correction_item_exclusion.correction_item_kind IN (
              'PRE_BANK_CANCEL',
              'NO_MONEY_UNWIND',
              'SETTLED_REVERSAL'
            )
        )
    ) AS correction_item
    WHERE correction_item.root_timesheet_id IS NOT NULL
      AND correction_item.candidate_id IS NOT NULL
      AND correction_item.target_pay_method IN ('PAYE', 'UMBRELLA')
      AND jsonb_typeof(correction_item.frozen_residual) = 'object'
    ORDER BY
      correction_item.source_family_key,
      correction_item.candidate_id,
      correction_item.target_pay_method,
      correction_item.pay_batch_item_id
  ),
  chain_results AS (
    SELECT
      correction_item.*,
      public.pay_correction_chain_residual_v1(
        correction_item.root_timesheet_id,
        correction_item.candidate_id,
        correction_item.target_pay_method,
        NULL::uuid,
        p_pay_batch_id,
        100
      ) AS live_residual
    FROM correction_items AS correction_item
  ),
  expected_components AS (
    SELECT
      chain_result.root_timesheet_id,
      chain_result.source_family_key,
      UPPER(NULLIF(BTRIM(component.component_json->>'component_key_type'), ''))
        AS key_type,
      NULLIF(BTRIM(component.component_json->>'component_key_value'), '')
        AS key_value,
      CASE
        WHEN COALESCE(
          component.component_json->>'effective_source_outstanding_ex_vat',
          ''
        ) ~ '^-?[0-9]+(\.[0-9]+)?$'
          THEN ROUND(
            (
              component.component_json
                ->>'effective_source_outstanding_ex_vat'
            )::numeric,
            2
          )::numeric(12,2)
        ELSE NULL::numeric(12,2)
      END AS amount_ex
    FROM chain_results AS chain_result
    CROSS JOIN LATERAL jsonb_array_elements(
      CASE
        WHEN jsonb_typeof(chain_result.frozen_residual->'components') = 'array'
          THEN chain_result.frozen_residual->'components'
        ELSE '[]'::jsonb
      END
    ) AS component(component_json)
  ),
  live_components AS (
    SELECT
      chain_result.root_timesheet_id,
      chain_result.source_family_key,
      UPPER(NULLIF(BTRIM(component.component_json->>'component_key_type'), ''))
        AS key_type,
      NULLIF(BTRIM(component.component_json->>'component_key_value'), '')
        AS key_value,
      CASE
        WHEN COALESCE(
          component.component_json->>'effective_source_outstanding_ex_vat',
          ''
        ) ~ '^-?[0-9]+(\.[0-9]+)?$'
          THEN ROUND(
            (
              component.component_json
                ->>'effective_source_outstanding_ex_vat'
            )::numeric,
            2
          )::numeric(12,2)
        ELSE NULL::numeric(12,2)
      END AS amount_ex
    FROM chain_results AS chain_result
    CROSS JOIN LATERAL jsonb_array_elements(
      CASE
        WHEN jsonb_typeof(chain_result.live_residual->'components') = 'array'
          THEN chain_result.live_residual->'components'
        ELSE '[]'::jsonb
      END
    ) AS component(component_json)
  ),
  chain_component_comparison AS (
    SELECT
      COALESCE(expected.root_timesheet_id, live.root_timesheet_id)
        AS root_timesheet_id,
      COALESCE(expected.source_family_key, live.source_family_key)
        AS source_family_key,
      COALESCE(expected.key_type, live.key_type) AS key_type,
      COALESCE(expected.key_value, live.key_value) AS key_value,
      expected.amount_ex AS expected_ex,
      live.amount_ex AS actual_ex,
      (
        expected.amount_ex IS NOT NULL
        AND live.amount_ex IS NOT NULL
        AND ABS(
          ROUND(expected.amount_ex, 2) - ROUND(live.amount_ex, 2)
        ) <= 0.01
      ) AS component_matches
    FROM expected_components AS expected
    FULL JOIN live_components AS live
      ON live.source_family_key = expected.source_family_key
     AND live.key_type = expected.key_type
     AND live.key_value = expected.key_value
  ),
  fresh_families AS (
    SELECT comparison.source_family_key
    FROM chain_component_comparison AS comparison
    GROUP BY comparison.source_family_key
    HAVING COUNT(*) > 0
       AND BOOL_AND(comparison.component_matches)
  ),
  fresh_components AS (
    SELECT comparison.*
    FROM chain_component_comparison AS comparison
    JOIN fresh_families AS fresh_family
      ON fresh_family.source_family_key = comparison.source_family_key
  )
  SELECT COALESCE(
    jsonb_agg(
      jsonb_build_object(
        'timesheet_id', fresh_component.root_timesheet_id::text,
        'key_type', fresh_component.key_type,
        'key_value', fresh_component.key_value,
        'expected_ex', fresh_component.expected_ex
      )
      ORDER BY
        fresh_component.root_timesheet_id,
        fresh_component.key_type,
        fresh_component.key_value
    ),
    '[]'::jsonb
  )
  INTO v_fresh_chain_components
  FROM fresh_components AS fresh_component;

  SELECT COALESCE(
    MAX(
      CASE
        WHEN diff_row.value->>'key_type' = 'INFO'
         AND diff_row.value->>'key_value' = 'COUNTS'
          THEN COALESCE(
            (diff_row.value#>>'{expected,key_diffs}')::integer,
            0
          )
        ELSE NULL
      END
    ),
    0
  )
  INTO v_original_key_diff_count
  FROM jsonb_array_elements(v_base_diff) AS diff_row(value);

  SELECT COUNT(*)::integer
  INTO v_suppressed_key_diff_count
  FROM jsonb_array_elements(v_base_diff) WITH ORDINALITY AS diff_row(value, ordinality)
  WHERE EXISTS (
    SELECT 1
    FROM jsonb_array_elements(v_fresh_chain_components)
      AS fresh_component(value)
    WHERE fresh_component.value->>'timesheet_id'
            = diff_row.value->>'timesheet_id'
      AND fresh_component.value->>'key_type'
            = diff_row.value->>'key_type'
      AND fresh_component.value->>'key_value'
            = diff_row.value->>'key_value'
      AND COALESCE(diff_row.value->>'expected', '')
            ~ '^-?[0-9]+(\.[0-9]+)?$'
      AND ABS(
        ROUND((diff_row.value->>'expected')::numeric, 2)
        - ROUND((fresh_component.value->>'expected_ex')::numeric, 2)
      ) <= 0.01
  );

  SELECT COALESCE(
    jsonb_agg(diff_row.value ORDER BY diff_row.ordinality),
    '[]'::jsonb
  )
  INTO v_filtered_diff
  FROM jsonb_array_elements(v_base_diff) WITH ORDINALITY AS diff_row(value, ordinality)
  WHERE NOT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(v_fresh_chain_components)
      AS fresh_component(value)
    WHERE fresh_component.value->>'timesheet_id'
            = diff_row.value->>'timesheet_id'
      AND fresh_component.value->>'key_type'
            = diff_row.value->>'key_type'
      AND fresh_component.value->>'key_value'
            = diff_row.value->>'key_value'
      AND COALESCE(diff_row.value->>'expected', '')
            ~ '^-?[0-9]+(\.[0-9]+)?$'
      AND ABS(
        ROUND((diff_row.value->>'expected')::numeric, 2)
        - ROUND((fresh_component.value->>'expected_ex')::numeric, 2)
      ) <= 0.01
  );

  v_remaining_key_diff_count := GREATEST(
    v_original_key_diff_count - v_suppressed_key_diff_count,
    0
  );

  SELECT COALESCE(
    jsonb_agg(
      CASE
        WHEN diff_row.value->>'key_type' = 'INFO'
         AND diff_row.value->>'key_value' = 'COUNTS'
          THEN jsonb_set(
            diff_row.value,
            '{expected,key_diffs}',
            to_jsonb(v_remaining_key_diff_count),
            true
          )
        ELSE diff_row.value
      END
      ORDER BY diff_row.ordinality
    ),
    '[]'::jsonb
  )
  INTO v_filtered_diff
  FROM jsonb_array_elements(v_filtered_diff)
    WITH ORDINALITY AS diff_row(value, ordinality);

  IF v_remaining_key_diff_count = 0 THEN
    SELECT COALESCE(jsonb_agg(reason.value ORDER BY reason.ordinality), '[]'::jsonb)
    INTO v_reasons
    FROM jsonb_array_elements(v_reasons)
      WITH ORDINALITY AS reason(value, ordinality)
    WHERE reason.value <> to_jsonb('RESERVATION_CHANGED'::text);
  END IF;

  RETURN jsonb_build_object(
    'is_stale', jsonb_array_length(v_reasons) > 0,
    'stale_reasons', v_reasons,
    'diff', v_filtered_diff
  );
END;
$function$;

COMMENT ON FUNCTION public.pay_batch_validate_freshness(
  uuid,
  uuid,
  boolean
) IS
'Validates frozen Banking Pay batch freshness and uses the bounded correction-chain residual authority to exclude the current batch reservation from correction-chain comparisons.';

REVOKE ALL ON FUNCTION public.pay_batch_validate_freshness(
  uuid,
  uuid,
  boolean
) FROM PUBLIC, anon, authenticated;

GRANT EXECUTE ON FUNCTION public.pay_batch_validate_freshness(
  uuid,
  uuid,
  boolean
) TO service_role;
