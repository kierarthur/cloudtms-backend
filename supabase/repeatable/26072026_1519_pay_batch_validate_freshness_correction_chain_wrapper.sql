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

  /*
   * Current canonical recovery items freeze one component per item rather
   * than embedding a second complete residual object. Accept that flat frozen
   * shape only when the chain and policy fingerprints still match and the
   * exact frozen component still equals the bounded live residual (with this
   * batch excluded). This also supplies the fresh anchor proof used by linked
   * resolved ordinary components below.
   */
  WITH flat_correction_items AS (
    SELECT
      pay_batch_item.id AS pay_batch_item_id,
      (
        REPLACE(
          COALESCE(
            pay_batch_item.frozen_source_basis_json->>'source_family_key',
            pay_batch_item.frozen_component_snapshot_json->>'source_family_key'
          ),
          'correction-chain:',
          ''
        )
      )::uuid AS root_timesheet_id,
      pay_batch_candidate.candidate_id,
      COALESCE(
        pay_batch_item.frozen_source_basis_json->>'source_family_key',
        pay_batch_item.frozen_component_snapshot_json->>'source_family_key'
      ) AS source_family_key,
      UPPER(BTRIM(COALESCE(
        pay_batch_item.frozen_target_pay_method,
        pay_batch_item.pay_channel,
        pay_batch.batch_kind_fixed,
        ''
      ))) AS target_pay_method,
      UPPER(BTRIM(COALESCE(
        pay_batch_item.frozen_component_key_type,
        pay_batch_item.frozen_component_snapshot_json->>'component_key_type',
        ''
      ))) AS key_type,
      BTRIM(COALESCE(
        pay_batch_item.frozen_component_key_value,
        pay_batch_item.frozen_component_snapshot_json->>'component_key_value',
        ''
      )) AS key_value,
      CASE
        WHEN pay_batch_item.frozen_source_amount IS NOT NULL
          THEN ROUND(
            pay_batch_item.frozen_source_amount,
            2
          )::numeric(12,2)
        WHEN COALESCE(
          pay_batch_item.frozen_resolution_result_json
            ->>'source_amount_ex_vat',
          pay_batch_item.frozen_component_snapshot_json
            ->>'source_amount_ex_vat',
          pay_batch_item.frozen_component_snapshot_json
            ->>'source_pay_ex_vat',
          pay_batch_item.frozen_component_snapshot_json
            ->>'effective_source_outstanding_ex_vat',
          ''
        ) ~ '^-?[0-9]+(\.[0-9]+)?$'
          THEN ROUND(
            COALESCE(
              pay_batch_item.frozen_resolution_result_json
                ->>'source_amount_ex_vat',
              pay_batch_item.frozen_component_snapshot_json
                ->>'source_amount_ex_vat',
              pay_batch_item.frozen_component_snapshot_json
                ->>'source_pay_ex_vat',
              pay_batch_item.frozen_component_snapshot_json
                ->>'effective_source_outstanding_ex_vat'
            )::numeric,
            2
          )::numeric(12,2)
        ELSE NULL::numeric(12,2)
      END AS expected_ex,
      CASE
        WHEN pay_batch_item.frozen_target_amount_ex_vat IS NOT NULL
          THEN ROUND(
            pay_batch_item.frozen_target_amount_ex_vat,
            2
          )::numeric(12,2)
        WHEN COALESCE(
          pay_batch_item.frozen_resolution_result_json
            ->>'target_amount_ex_vat',
          ''
        ) ~ '^-?[0-9]+(\.[0-9]+)?$'
          THEN ROUND(
            (
              pay_batch_item.frozen_resolution_result_json
                ->>'target_amount_ex_vat'
            )::numeric,
            2
          )::numeric(12,2)
        ELSE ROUND(
          pay_batch_item.amount_ex_vat,
          2
        )::numeric(12,2)
      END AS expected_target_ex,
      ROUND(
        pay_batch_item.amount_ex_vat,
        2
      )::numeric(12,2) AS physical_target_ex,
      CASE
        WHEN COALESCE(
          pay_batch_item.frozen_resolution_result_json
            ->>'source_amount_ex_vat',
          ''
        ) ~ '^-?[0-9]+(\.[0-9]+)?$'
          THEN ROUND(
            (
              pay_batch_item.frozen_resolution_result_json
                ->>'source_amount_ex_vat'
            )::numeric,
            2
          )::numeric(12,2)
        ELSE NULL::numeric(12,2)
      END AS result_source_ex,
      CASE
        WHEN COALESCE(
          pay_batch_item.frozen_resolution_result_json
            ->>'target_amount_ex_vat',
          ''
        ) ~ '^-?[0-9]+(\.[0-9]+)?$'
          THEN ROUND(
            (
              pay_batch_item.frozen_resolution_result_json
                ->>'target_amount_ex_vat'
            )::numeric,
            2
          )::numeric(12,2)
        ELSE NULL::numeric(12,2)
      END AS result_target_ex,
      CASE
        WHEN COALESCE(
          pay_batch_item.frozen_component_snapshot_json
            ->>'source_amount_ex_vat',
          pay_batch_item.frozen_component_snapshot_json
            ->>'source_pay_ex_vat',
          ''
        ) ~ '^-?[0-9]+(\.[0-9]+)?$'
          THEN ROUND(
            COALESCE(
              pay_batch_item.frozen_component_snapshot_json
                ->>'source_amount_ex_vat',
              pay_batch_item.frozen_component_snapshot_json
                ->>'source_pay_ex_vat'
            )::numeric,
            2
          )::numeric(12,2)
        ELSE NULL::numeric(12,2)
      END AS snapshot_source_ex,
      NULLIF(BTRIM(COALESCE(
        pay_batch_item.frozen_source_basis_json
          ->>'correction_chain_fingerprint',
        pay_batch_item.frozen_component_snapshot_json
          ->>'correction_chain_fingerprint',
        ''
      )), '') AS frozen_chain_fingerprint,
      NULLIF(BTRIM(COALESCE(
        pay_batch_item.frozen_source_basis_json
          ->>'correction_financials_policy_envelope_fingerprint',
        pay_batch_item.frozen_component_snapshot_json
          ->>'correction_financials_policy_envelope_fingerprint',
        ''
      )), '') AS frozen_policy_fingerprint
    FROM public.pay_batch_items AS pay_batch_item
    JOIN public.pay_batch_candidates AS pay_batch_candidate
      ON pay_batch_candidate.id = pay_batch_item.pay_batch_candidate_id
    JOIN public.pay_batches AS pay_batch
      ON pay_batch.id = pay_batch_candidate.pay_batch_id
    WHERE pay_batch_candidate.pay_batch_id = p_pay_batch_id
      AND COALESCE(pay_batch_item.is_voided, false) = false
      AND COALESCE(
        pay_batch_item.frozen_source_basis_json->>'source_family_key',
        pay_batch_item.frozen_component_snapshot_json->>'source_family_key',
        ''
      ) ~ '^correction-chain:[0-9a-fA-F-]{36}$'
      AND jsonb_typeof(
        pay_batch_item.frozen_component_snapshot_json
      ) = 'object'
      AND pay_batch_item.frozen_source_amount IS NOT NULL
      AND pay_batch_item.frozen_target_amount_ex_vat IS NOT NULL
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
  ),
  flat_chain_groups AS (
    SELECT DISTINCT
      flat_item.root_timesheet_id,
      flat_item.candidate_id,
      flat_item.source_family_key,
      flat_item.target_pay_method
    FROM flat_correction_items AS flat_item
    WHERE flat_item.root_timesheet_id IS NOT NULL
      AND flat_item.candidate_id IS NOT NULL
      AND flat_item.target_pay_method IN ('PAYE', 'UMBRELLA')
  ),
  flat_chain_results AS (
    SELECT
      flat_group.*,
      public.pay_correction_chain_residual_v1(
        flat_group.root_timesheet_id,
        flat_group.candidate_id,
        flat_group.target_pay_method,
        NULL::uuid,
        p_pay_batch_id,
        100
      ) AS live_residual
    FROM flat_chain_groups AS flat_group
  ),
  flat_fresh_components AS (
    SELECT
      flat_item.root_timesheet_id,
      flat_item.key_type,
      flat_item.key_value,
      flat_item.expected_ex
    FROM flat_correction_items AS flat_item
    JOIN flat_chain_results AS flat_result
      ON flat_result.root_timesheet_id = flat_item.root_timesheet_id
     AND flat_result.candidate_id = flat_item.candidate_id
     AND flat_result.source_family_key = flat_item.source_family_key
     AND flat_result.target_pay_method = flat_item.target_pay_method
    JOIN LATERAL (
      SELECT live_component.component_json
      FROM jsonb_array_elements(
        CASE
          WHEN jsonb_typeof(flat_result.live_residual->'components') = 'array'
            THEN flat_result.live_residual->'components'
          ELSE '[]'::jsonb
        END
      ) AS live_component(component_json)
      WHERE UPPER(BTRIM(COALESCE(
              live_component.component_json->>'component_key_type',
              ''
            ))) = flat_item.key_type
        AND BTRIM(COALESCE(
              live_component.component_json->>'component_key_value',
              ''
            )) = flat_item.key_value
      LIMIT 1
    ) AS matched_live_component
      ON true
    WHERE flat_item.key_type IN (
        'TS_DAY',
        'TS_TOTAL',
        'ADDITIONAL_CODE',
        'ADJUSTMENT_CODE',
        'EXPENSE_CODE'
      )
      AND flat_item.key_value <> ''
      AND flat_item.expected_ex IS NOT NULL
      AND flat_item.expected_target_ex IS NOT NULL
      AND flat_item.result_source_ex IS NOT NULL
      AND flat_item.result_target_ex IS NOT NULL
      AND flat_item.snapshot_source_ex IS NOT NULL
      AND flat_item.frozen_chain_fingerprint IS NOT NULL
      AND flat_item.frozen_policy_fingerprint IS NOT NULL
      AND flat_item.frozen_chain_fingerprint
            = flat_result.live_residual->>'chain_fingerprint'
      AND flat_item.frozen_policy_fingerprint
            = flat_result.live_residual
                ->>'correction_financials_policy_envelope_fingerprint'
      AND COALESCE(
        matched_live_component.component_json
          ->>'target_outstanding_ex_vat',
        matched_live_component.component_json
          ->>'effective_source_outstanding_ex_vat',
        ''
      ) ~ '^-?[0-9]+(\.[0-9]+)?$'
      AND ABS(
        ROUND(flat_item.expected_target_ex, 2)
        - ROUND(
            COALESCE(
              matched_live_component.component_json
                ->>'target_outstanding_ex_vat',
              matched_live_component.component_json
                ->>'effective_source_outstanding_ex_vat'
            )::numeric,
            2
          )
      ) <= 0.01
      AND ABS(
        ROUND(flat_item.result_source_ex, 2)
        - ROUND(flat_item.expected_ex, 2)
      ) <= 0.01
      AND ABS(
        ROUND(flat_item.snapshot_source_ex, 2)
        - ROUND(flat_item.expected_ex, 2)
      ) <= 0.01
      AND ABS(
        ROUND(flat_item.result_target_ex, 2)
        - ROUND(flat_item.expected_target_ex, 2)
      ) <= 0.01
      AND ABS(
        ROUND(flat_item.physical_target_ex, 2)
        - ROUND(flat_item.expected_target_ex, 2)
      ) <= 0.01
  )
  SELECT
    v_fresh_chain_components
    || COALESCE(
      jsonb_agg(
        jsonb_build_object(
          'timesheet_id', flat_component.root_timesheet_id::text,
          'key_type', flat_component.key_type,
          'key_value', flat_component.key_value,
          'expected_ex', flat_component.expected_ex
        )
        ORDER BY
          flat_component.root_timesheet_id,
          flat_component.key_type,
          flat_component.key_value
      ),
      '[]'::jsonb
    )
  INTO v_fresh_chain_components
  FROM flat_fresh_components AS flat_component;

  /*
   * A resolved ordinary timesheet component can also carry an approved target
   * rate. Its frozen source entitlement is the full original component, while
   * the item pays only the target-side remainder after the already-settled
   * baseline. The ordinary base comparison therefore reports the expected
   * source entitlement against zero even when:
   *
   *   - the live source component is unchanged;
   *   - no other active batch reserves the component;
   *   - the exact target remainder was frozen into this batch; and
   *   - its exact local-timesheet anchor remains valid, or its linked
   *     correction-chain anchor remains fresh.
   *
   * Suppress only that exact self-reservation diff. This is deliberately
   * stricter than trusting the summary flag: frozen component evidence,
   * physical/result parity, every frozen member of the economic component,
   * current source truth, settled baseline, absence of another reservation,
   * and the applicable frozen anchor must all agree. Any missing, partial or
   * contradictory evidence leaves the base stale result intact.
   */
  WITH linked_resolution_items AS (
    SELECT
      pay_batch_item.id AS pay_batch_item_id,
      pay_batch_item.timesheet_id,
      UPPER(BTRIM(COALESCE(pay_batch_item.frozen_component_key_type, '')))
        AS key_type,
      BTRIM(COALESCE(pay_batch_item.frozen_component_key_value, ''))
        AS key_value,
      pay_batch_item.amount_ex_vat,
      pay_batch_item.frozen_source_amount,
      pay_batch_item.frozen_target_amount_ex_vat,
      pay_batch_item.frozen_resolution_result_json,
      pay_batch_item.frozen_resolution_payload_json
        ->'case_resolution_summary' AS resolution_summary,
      resolved_component.component_count,
      resolved_component.source_numeric_count,
      resolved_component.target_numeric_count,
      resolved_component.resolved_source_ex_vat,
      resolved_component.resolved_target_ex_vat,
      resolved_component.all_components_resolved,
      resolved_component.all_component_fingerprints_present,
      resolved_component.all_source_basis_fingerprints_present,
      resolved_component.all_components_current,
      resolved_component.all_saved_results_current,
      resolved_component.all_components_do_not_require_resolution,
      resolved_component.all_saved_anchor_modes_match,
      resolved_component.all_saved_anchor_case_keys_match,
      resolved_component.all_saved_anchor_timesheets_match,
      LOWER(BTRIM(COALESCE(
        pay_batch_item.frozen_resolution_payload_json
          #>> '{case_resolution_summary,resolved_rate_applied_via_linked_scope}',
        'false'
      ))) IN ('true','t','1','yes','y','on')
        AS applied_via_linked_scope,
      CASE
        WHEN COALESCE(
          pay_batch_item.frozen_resolution_payload_json
            #>> '{case_resolution_summary,resolved_rate_source_anchor_timesheet_id}',
          ''
        ) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN (
            pay_batch_item.frozen_resolution_payload_json
              #>> '{case_resolution_summary,resolved_rate_source_anchor_timesheet_id}'
          )::uuid
        ELSE NULL::uuid
      END AS anchor_timesheet_id,
      NULLIF(BTRIM(COALESCE(
        pay_batch_item.frozen_resolution_payload_json
          #>> '{case_resolution_summary,resolved_rate_source_anchor_case_key}',
        ''
      )), '') AS anchor_case_key
    FROM public.pay_batch_items AS pay_batch_item
    JOIN public.pay_batch_candidates AS pay_batch_candidate
      ON pay_batch_candidate.id = pay_batch_item.pay_batch_candidate_id
    LEFT JOIN LATERAL (
      SELECT
        COUNT(*)::integer AS component_count,
        COUNT(*) FILTER (
          WHERE COALESCE(
            frozen_component.component_json->>'source_pay_ex_vat',
            ''
          ) ~ '^-?[0-9]+(\.[0-9]+)?$'
        )::integer AS source_numeric_count,
        COUNT(*) FILTER (
          WHERE COALESCE(
            frozen_component.component_json->>'target_pay_ex_vat',
            ''
          ) ~ '^-?[0-9]+(\.[0-9]+)?$'
        )::integer AS target_numeric_count,
        ROUND(SUM(
          CASE
            WHEN COALESCE(
              frozen_component.component_json->>'source_pay_ex_vat',
              ''
            ) ~ '^-?[0-9]+(\.[0-9]+)?$'
              THEN (
                frozen_component.component_json->>'source_pay_ex_vat'
              )::numeric
            ELSE 0::numeric
          END
        ), 2)::numeric(12,2) AS resolved_source_ex_vat,
        ROUND(SUM(
          CASE
            WHEN COALESCE(
              frozen_component.component_json->>'target_pay_ex_vat',
              ''
            ) ~ '^-?[0-9]+(\.[0-9]+)?$'
              THEN (
                frozen_component.component_json->>'target_pay_ex_vat'
              )::numeric
            ELSE 0::numeric
          END
        ), 2)::numeric(12,2) AS resolved_target_ex_vat,
        BOOL_AND(NULLIF(BTRIM(COALESCE(
          frozen_component.component_json->>'resolved_rate_resolution_id',
          ''
        )), '') IS NOT NULL) AS all_components_resolved,
        BOOL_AND(NULLIF(BTRIM(COALESCE(
          frozen_component.component_json->>'component_fingerprint',
          ''
        )), '') IS NOT NULL) AS all_component_fingerprints_present,
        BOOL_AND(NULLIF(BTRIM(COALESCE(
          frozen_component.component_json->>'source_basis_fingerprint',
          ''
        )), '') IS NOT NULL) AS all_source_basis_fingerprints_present,
        BOOL_AND(LOWER(BTRIM(COALESCE(
          frozen_component.component_json->>'is_resolution_stale',
          'false'
        ))) NOT IN ('true','t','1','yes','y','on'))
          AS all_components_current,
        BOOL_AND(LOWER(BTRIM(COALESCE(
          frozen_component.component_json->>'is_stale_saved_resolution',
          'false'
        ))) NOT IN ('true','t','1','yes','y','on'))
          AS all_saved_results_current,
        BOOL_AND(LOWER(BTRIM(COALESCE(
          frozen_component.component_json->>'requires_resolution',
          'true'
        ))) IN ('false','f','0','no','n','off'))
          AS all_components_do_not_require_resolution,
        BOOL_AND(
          (
            LOWER(BTRIM(COALESCE(
              frozen_component.component_json
                #>> '{saved_resolution_result_json,applied_via_linked_scope}',
              'false'
            ))) IN ('true','t','1','yes','y','on')
          ) = (
            LOWER(BTRIM(COALESCE(
              pay_batch_item.frozen_resolution_payload_json
                #>> '{case_resolution_summary,resolved_rate_applied_via_linked_scope}',
              'false'
            ))) IN ('true','t','1','yes','y','on')
          )
        ) AS all_saved_anchor_modes_match,
        BOOL_AND(
          frozen_component.component_json
            #>> '{saved_resolution_result_json,source_anchor_case_key}'
          = pay_batch_item.frozen_resolution_payload_json
              #>> '{case_resolution_summary,resolved_rate_source_anchor_case_key}'
        ) AS all_saved_anchor_case_keys_match,
        BOOL_AND(
          frozen_component.component_json
            #>> '{saved_resolution_result_json,source_anchor_timesheet_id}'
          = pay_batch_item.frozen_resolution_payload_json
              #>> '{case_resolution_summary,resolved_rate_source_anchor_timesheet_id}'
        ) AS all_saved_anchor_timesheets_match
      FROM jsonb_array_elements(
        CASE
          WHEN jsonb_typeof(
            pay_batch_item.frozen_resolution_payload_json->'case_components'
          ) = 'array'
            THEN pay_batch_item.frozen_resolution_payload_json->'case_components'
          ELSE '[]'::jsonb
        END
      ) AS frozen_component(component_json)
      WHERE UPPER(BTRIM(COALESCE(
              frozen_component.component_json->>'component_key_type',
              ''
            ))) = UPPER(BTRIM(COALESCE(
              pay_batch_item.frozen_component_key_type,
              ''
            )))
        AND BTRIM(COALESCE(
              frozen_component.component_json->>'component_key_value',
              ''
            )) = BTRIM(COALESCE(
              pay_batch_item.frozen_component_key_value,
              ''
            ))
    ) AS resolved_component
      ON true
    WHERE pay_batch_candidate.pay_batch_id = p_pay_batch_id
      AND COALESCE(pay_batch_item.is_voided, false) = false
      AND pay_batch_item.timesheet_id IS NOT NULL
      AND UPPER(BTRIM(COALESCE(pay_batch_item.item_type, ''))) IN (
        'SEGMENT_DELTA',
        'ADJUSTMENT_DELTA',
        'MILEAGE_DELTA'
      )
      AND UPPER(BTRIM(COALESCE(
        pay_batch_item.frozen_component_key_type,
        ''
      ))) IN (
        'TS_DAY',
        'TS_TOTAL',
        'ADDITIONAL_CODE',
        'ADJUSTMENT_CODE'
      )
      AND BTRIM(COALESCE(
        pay_batch_item.frozen_component_key_value,
        ''
      )) <> ''
      AND jsonb_typeof(
        pay_batch_item.frozen_resolution_payload_json
          ->'case_resolution_summary'
      ) = 'object'
      AND COALESCE(resolved_component.component_count, 0) > 0
  ),
  linked_timesheet_ids AS (
    SELECT COALESCE(
      ARRAY_AGG(
        DISTINCT linked_resolution_item.timesheet_id
        ORDER BY linked_resolution_item.timesheet_id
      ),
      ARRAY[]::uuid[]
    ) AS timesheet_ids
    FROM linked_resolution_items AS linked_resolution_item
  ),
  linked_outstanding AS (
    SELECT
      outstanding_component.timesheet_id,
      UPPER(BTRIM(COALESCE(outstanding_component.key_type, '')))
        AS key_type,
      BTRIM(COALESCE(outstanding_component.key_value, '')) AS key_value,
      ROUND(COALESCE(outstanding_component.truth_ex_vat, 0), 2)
        AS truth_ex_vat,
      ROUND(COALESCE(outstanding_component.baseline_ex_vat, 0), 2)
        AS baseline_ex_vat,
      ROUND(COALESCE(outstanding_component.reserved_ex_vat, 0), 2)
        AS other_reserved_ex_vat,
      ROUND(COALESCE(outstanding_component.outstanding_ex_vat, 0), 2)
        AS outstanding_ex_vat
    FROM linked_timesheet_ids AS linked_scope
    JOIN public._pay_outstanding_components(
      linked_scope.timesheet_ids,
      p_pay_batch_id
    ) AS outstanding_component
      ON true
  ),
  fresh_linked_resolution_components AS (
    SELECT
      linked_resolution_item.timesheet_id,
      linked_resolution_item.key_type,
      linked_resolution_item.key_value,
      linked_resolution_item.resolved_source_ex_vat AS expected_ex
    FROM linked_resolution_items AS linked_resolution_item
    JOIN linked_outstanding AS outstanding_component
      ON outstanding_component.timesheet_id
           = linked_resolution_item.timesheet_id
     AND outstanding_component.key_type = linked_resolution_item.key_type
     AND outstanding_component.key_value = linked_resolution_item.key_value
    WHERE linked_resolution_item.resolved_source_ex_vat IS NOT NULL
      AND linked_resolution_item.resolved_target_ex_vat IS NOT NULL
      AND linked_resolution_item.anchor_timesheet_id IS NOT NULL
      AND linked_resolution_item.anchor_case_key IS NOT NULL
      AND LOWER(BTRIM(COALESCE(
        linked_resolution_item.resolution_summary->>'has_resolved_rate',
        'false'
      ))) IN ('true','t','1','yes','y','on')
      AND LOWER(BTRIM(COALESCE(
        linked_resolution_item.resolution_summary
          ->>'case_resolution_satisfied_now',
        'false'
      ))) IN ('true','t','1','yes','y','on')
      AND linked_resolution_item.resolution_summary
            ->>'resolved_rate_timesheet_id'
          = linked_resolution_item.timesheet_id::text
      AND linked_resolution_item.component_count
            = linked_resolution_item.source_numeric_count
      AND linked_resolution_item.component_count
            = linked_resolution_item.target_numeric_count
      AND linked_resolution_item.all_components_resolved
      AND linked_resolution_item.all_component_fingerprints_present
      AND linked_resolution_item.all_source_basis_fingerprints_present
      AND linked_resolution_item.all_components_current
      AND linked_resolution_item.all_saved_results_current
      AND linked_resolution_item.all_components_do_not_require_resolution
      AND linked_resolution_item.all_saved_anchor_modes_match
      AND linked_resolution_item.all_saved_anchor_case_keys_match
      AND linked_resolution_item.all_saved_anchor_timesheets_match
      AND (
        (
          linked_resolution_item.applied_via_linked_scope
          AND EXISTS (
            SELECT 1
            FROM jsonb_array_elements(v_fresh_chain_components)
              AS fresh_anchor(value)
            WHERE fresh_anchor.value->>'timesheet_id'
                    = linked_resolution_item.anchor_timesheet_id::text
              AND LOWER(linked_resolution_item.anchor_case_key)
                    = LOWER(
                        'correction-chain:'
                        || linked_resolution_item.anchor_timesheet_id::text
                        || ':'
                        || COALESCE(fresh_anchor.value->>'key_type', '')
                        || ':'
                        || COALESCE(fresh_anchor.value->>'key_value', '')
                      )
          )
        )
        OR
        (
          NOT linked_resolution_item.applied_via_linked_scope
          AND linked_resolution_item.anchor_timesheet_id
                = linked_resolution_item.timesheet_id
          AND LOWER(linked_resolution_item.anchor_case_key)
                = LOWER(
                    'timesheet:'
                    || linked_resolution_item.timesheet_id::text
                  )
        )
      )
      AND ABS(
        ROUND(outstanding_component.truth_ex_vat, 2)
        - ROUND(linked_resolution_item.resolved_source_ex_vat, 2)
      ) <= 0.01
      AND ABS(
        ROUND(outstanding_component.baseline_ex_vat, 2)
        - ROUND(linked_resolution_item.resolved_source_ex_vat, 2)
      ) <= 0.01
      AND ABS(ROUND(outstanding_component.other_reserved_ex_vat, 2)) <= 0.01
      AND ABS(
        ROUND(linked_resolution_item.frozen_target_amount_ex_vat, 2)
        - ROUND(linked_resolution_item.amount_ex_vat, 2)
      ) <= 0.01
      AND COALESCE(
        linked_resolution_item.frozen_resolution_result_json
          ->>'target_amount_ex_vat',
        ''
      ) ~ '^-?[0-9]+(\.[0-9]+)?$'
      AND ABS(
        ROUND(
          (
            linked_resolution_item.frozen_resolution_result_json
              ->>'target_amount_ex_vat'
          )::numeric,
          2
        )
        - ROUND(linked_resolution_item.amount_ex_vat, 2)
      ) <= 0.01
      AND linked_resolution_item.frozen_source_amount IS NOT NULL
      AND ABS(
        ROUND(linked_resolution_item.frozen_source_amount, 2)
        - ROUND(linked_resolution_item.amount_ex_vat, 2)
      ) <= 0.01
      AND ABS(
        ROUND(linked_resolution_item.amount_ex_vat, 2)
        - ROUND(
            GREATEST(
              linked_resolution_item.resolved_target_ex_vat
                - outstanding_component.baseline_ex_vat,
              0
            ),
            2
          )
      ) <= 0.01
  )
  SELECT
    v_fresh_chain_components
    || COALESCE(
      jsonb_agg(
        jsonb_build_object(
          'timesheet_id', linked_component.timesheet_id::text,
          'key_type', linked_component.key_type,
          'key_value', linked_component.key_value,
          'expected_ex', linked_component.expected_ex
        )
        ORDER BY
          linked_component.timesheet_id,
          linked_component.key_type,
          linked_component.key_value
      ),
      '[]'::jsonb
    )
  INTO v_fresh_chain_components
  FROM fresh_linked_resolution_components AS linked_component;

  /*
   * A completed semantic-V3 source can retain a settled entitlement baseline
   * that the legacy live helper no longer exposes.  In that exact case the
   * base validator compares the frozen source amount with live truth alone
   * and mistakes this batch's own reservation for external drift.
   *
   * Recover the baseline only from the one current, parity-complete V3
   * publication that is fenced to the candidate registry's current sequence
   * and generation.  Live truth must still equal the certified build truth,
   * all other reservations are read live with this batch excluded, and the
   * complete frozen source/target evidence must reconcile exactly.  Any
   * missing, mixed or stale authority leaves the base stale result untouched.
   */
  WITH frozen_ordinary_items AS (
    SELECT
      pay_batch_item.id AS pay_batch_item_id,
      pay_batch_candidate.candidate_id,
      pay_batch_item.timesheet_id,
      UPPER(BTRIM(pay_batch_item.frozen_component_key_type)) AS key_type,
      BTRIM(pay_batch_item.frozen_component_key_value) AS key_value,
      ROUND(pay_batch_item.frozen_source_amount, 2)::numeric(12,2)
        AS frozen_source_ex,
      ROUND(pay_batch_item.frozen_target_amount_ex_vat, 2)::numeric(12,2)
        AS frozen_target_ex,
      ROUND(pay_batch_item.amount_ex_vat, 2)::numeric(12,2)
        AS physical_target_ex
    FROM public.pay_batch_items AS pay_batch_item
    JOIN public.pay_batch_candidates AS pay_batch_candidate
      ON pay_batch_candidate.id = pay_batch_item.pay_batch_candidate_id
    WHERE pay_batch_candidate.pay_batch_id = p_pay_batch_id
      AND COALESCE(pay_batch_item.is_voided, false) = false
      AND pay_batch_item.timesheet_id IS NOT NULL
      AND UPPER(BTRIM(COALESCE(pay_batch_item.item_type, ''))) IN (
        'SEGMENT_DELTA',
        'ADJUSTMENT_DELTA',
        'MILEAGE_DELTA'
      )
      AND UPPER(BTRIM(COALESCE(
        pay_batch_item.frozen_component_key_type,
        ''
      ))) IN (
        'TS_DAY',
        'TS_TOTAL',
        'ADDITIONAL_CODE',
        'ADJUSTMENT_CODE',
        'EXPENSE_CODE'
      )
      AND BTRIM(COALESCE(
        pay_batch_item.frozen_component_key_value,
        ''
      )) <> ''
      AND pay_batch_item.frozen_source_amount IS NOT NULL
      AND pay_batch_item.frozen_target_amount_ex_vat IS NOT NULL
      AND ABS(
        ROUND(pay_batch_item.frozen_target_amount_ex_vat, 2)
        - ROUND(pay_batch_item.amount_ex_vat, 2)
      ) <= 0.01
      AND COALESCE(
        pay_batch_item.frozen_source_basis_json->>'source_family_key',
        pay_batch_item.frozen_component_snapshot_json->>'source_family_key',
        ''
      ) NOT LIKE 'correction-chain:%'
      AND jsonb_typeof(
        pay_batch_item.frozen_resolution_payload_json
          ->'case_resolution_summary'
      ) IS DISTINCT FROM 'object'
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
  ), certified_authority_candidates AS (
    SELECT
      registry.candidate_id,
      scope_row.session_id,
      scope_row.certified_preview_publication_source_build_run_id
        AS source_build_run_id,
      scope_row.certified_preview_publication_source_publication_id
        AS source_publication_id,
      economic_build.id AS build_id,
      registry.current_source_change_seq,
      registry.dirty_generation
    FROM private.banking_pay_workbench_candidate_scope_registry AS registry
    JOIN public.banking_pay_workbench_session_scope AS scope_row
      ON scope_row.candidate_id = registry.candidate_id
     AND scope_row.status = 'MATERIALISED'
     AND COALESCE(scope_row.dirty, false) = false
     AND COALESCE(
           scope_row.certified_preview_publication_required,
           false
         )
     AND COALESCE(
           scope_row.certified_preview_publication_parity_ok,
           false
         )
     AND scope_row.certified_preview_publication_source_change_seq
           = registry.current_source_change_seq
     AND scope_row.certified_preview_publication_source_build_run_id
           IS NOT NULL
     AND scope_row.certified_preview_publication_source_publication_id
           IS NOT NULL
    JOIN private.banking_pay_workbench_economic_builds AS economic_build
      ON economic_build.id = CASE
           WHEN COALESCE(
                  scope_row.certified_preview_publication_attestation_json
                    ->>'economic_build_id',
                  ''
                ) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
             THEN (
               scope_row.certified_preview_publication_attestation_json
                 ->>'economic_build_id'
             )::uuid
           ELSE NULL::uuid
         END
     AND economic_build.candidate_id = registry.candidate_id
     AND economic_build.session_id = scope_row.session_id
     AND economic_build.source_build_run_id
           = scope_row.certified_preview_publication_source_build_run_id
     AND economic_build.source_change_seq
           = registry.current_source_change_seq
     AND economic_build.captured_candidate_generation
           = registry.evaluated_generation
     AND economic_build.status = 'COMPLETE'
    WHERE registry.dirty_generation = registry.evaluated_generation
      AND scope_row.certified_preview_publication_attestation_json
            ->>'attestation_version'
          = 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3'
      AND LOWER(BTRIM(COALESCE(
            scope_row.certified_preview_publication_attestation_json
              ->>'parity_complete',
            'false'
          ))) IN ('true','t','1','yes','y','on')
      AND LOWER(BTRIM(COALESCE(
            scope_row.certified_preview_publication_attestation_json
              ->>'semantic_ready',
            'false'
          ))) IN ('true','t','1','yes','y','on')
      AND COALESCE(
            scope_row.certified_preview_publication_attestation_json
              ->>'invalid_selectable_row_count',
            ''
          ) ~ '^[0-9]+$'
      AND (
            scope_row.certified_preview_publication_attestation_json
              ->>'invalid_selectable_row_count'
          )::integer = 0
      AND scope_row.certified_preview_publication_attestation_json
            ->>'source_build_run_id'
          = scope_row.certified_preview_publication_source_build_run_id::text
      AND scope_row.certified_preview_publication_attestation_json
            ->>'source_publication_id'
          = scope_row.certified_preview_publication_source_publication_id::text
      AND COALESCE(
            scope_row.certified_preview_publication_attestation_json
              ->>'source_row_count',
            ''
          ) ~ '^[0-9]+$'
      AND (
        SELECT COUNT(*)::integer
        FROM public.banking_pay_workbench_candidate_source_lines
          AS current_source
        WHERE current_source.session_id = scope_row.session_id
          AND current_source.candidate_id = registry.candidate_id
          AND current_source.session_version
                = scope_row.certified_preview_publication_session_version
          AND current_source.source_change_seq
                = registry.current_source_change_seq
          AND current_source.source_build_run_id
                = scope_row.certified_preview_publication_source_build_run_id
          AND current_source.source_publication_id
                = scope_row.certified_preview_publication_source_publication_id
          AND current_source.status = 'CURRENT'
      ) = (
        scope_row.certified_preview_publication_attestation_json
          ->>'source_row_count'
      )::integer
      AND NOT EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_candidate_source_lines
          AS conflicting_source
        WHERE conflicting_source.session_id = scope_row.session_id
          AND conflicting_source.candidate_id = registry.candidate_id
          AND conflicting_source.status = 'CURRENT'
          AND (
            conflicting_source.session_version
              IS DISTINCT FROM
                scope_row.certified_preview_publication_session_version
            OR conflicting_source.source_change_seq
              IS DISTINCT FROM registry.current_source_change_seq
            OR conflicting_source.source_build_run_id
              IS DISTINCT FROM
                scope_row.certified_preview_publication_source_build_run_id
            OR conflicting_source.source_publication_id
              IS DISTINCT FROM
                scope_row.certified_preview_publication_source_publication_id
          )
      )
  ), certified_current_authorities AS (
    SELECT authority.*
    FROM certified_authority_candidates AS authority
    WHERE 1 = (
      SELECT COUNT(*)
      FROM certified_authority_candidates AS candidate_authority
      WHERE candidate_authority.candidate_id = authority.candidate_id
    )
  ), certified_fresh_components AS (
    SELECT
      frozen_item.timesheet_id,
      frozen_item.key_type,
      frozen_item.key_value,
      frozen_item.frozen_source_ex AS expected_ex
    FROM frozen_ordinary_items AS frozen_item
    JOIN certified_current_authorities AS certified_authority
      ON certified_authority.candidate_id = frozen_item.candidate_id
    JOIN private.pay_current_timesheet_entitlement_components_from_build_v1(
      certified_authority.build_id,
      NULL::text
    ) AS certified_component
      ON certified_component.timesheet_id = frozen_item.timesheet_id
     AND UPPER(BTRIM(certified_component.key_type))
           = frozen_item.key_type
     AND BTRIM(certified_component.key_value) = frozen_item.key_value
    JOIN public._pay_current_timesheet_entitlement_components(
      ARRAY[frozen_item.timesheet_id]::uuid[]
    ) AS live_truth_component
      ON live_truth_component.timesheet_id = frozen_item.timesheet_id
     AND UPPER(BTRIM(live_truth_component.key_type))
           = frozen_item.key_type
     AND BTRIM(live_truth_component.key_value) = frozen_item.key_value
    JOIN LATERAL public._pay_outstanding_components(
      ARRAY[frozen_item.timesheet_id]::uuid[],
      p_pay_batch_id
    ) AS live_component
      ON live_component.timesheet_id = frozen_item.timesheet_id
     AND UPPER(BTRIM(live_component.key_type)) = frozen_item.key_type
     AND BTRIM(live_component.key_value) = frozen_item.key_value
    WHERE ABS(
        ROUND(COALESCE(live_truth_component.truth_ex_vat, 0), 2)
        - ROUND(COALESCE(certified_component.truth_ex_vat, 0), 2)
      ) <= 0.01
      AND ABS(
        ROUND(COALESCE(live_truth_component.baseline_ex_vat, 0), 2)
        - ROUND(COALESCE(certified_component.baseline_ex_vat, 0), 2)
      ) > 0.01
      AND ABS(
        ROUND(COALESCE(certified_component.baseline_ex_vat, 0), 2)
      ) > 0.01
      AND ABS(
        ROUND(
          COALESCE(certified_component.truth_ex_vat, 0)
          - COALESCE(certified_component.baseline_ex_vat, 0)
          - COALESCE(live_component.reserved_ex_vat, 0),
          2
        )
        - frozen_item.frozen_source_ex
      ) <= 0.01
      AND ABS(
        frozen_item.frozen_target_ex - frozen_item.physical_target_ex
      ) <= 0.01
  )
  SELECT
    v_fresh_chain_components
    || COALESCE(
      jsonb_agg(
        jsonb_build_object(
          'timesheet_id', certified_component.timesheet_id::text,
          'key_type', certified_component.key_type,
          'key_value', certified_component.key_value,
          'expected_ex', certified_component.expected_ex
        )
        ORDER BY
          certified_component.timesheet_id,
          certified_component.key_type,
          certified_component.key_value
      ),
      '[]'::jsonb
    )
  INTO v_fresh_chain_components
  FROM certified_fresh_components AS certified_component;

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
