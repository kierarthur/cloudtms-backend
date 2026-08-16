-- ============================================================================
-- 03_pay_correction_chain_residual_v1.txt
--
-- Narrow pre-draft changed-hours correction-chain residual helper.
--
-- Scope:
--   - only TS_DAY hours components;
--   - only one validated correction chain;
--   - no expenses, mileage, additional codes, loans, advances, or unrelated
--     overpayments;
--   - no post-draft authority and no permanent writes.
--
-- The helper sums existing current truth, settled baseline, and active
-- reservations using the existing Policy X component authorities. It then
-- accounts for settled/reserved finance-case movements already linked to the
-- correction-chain source family.
--
-- Cross-channel PAYE/umbrella conversion is not guessed. Existing Workbench
-- case-resolution rows are surfaced and the result remains non-draftable until
-- every required target amount is present and fresh.
--
-- Limits:
--   6 arguments.
--   Maximum chain members: 100.
--   Maximum TS_DAY components: 100.
-- ============================================================================

CREATE OR REPLACE FUNCTION public.pay_correction_chain_residual_v1(
  p_timesheet_id uuid,
  p_candidate_id uuid,
  p_target_pay_method text,
  p_workbench_session_id uuid DEFAULT NULL::uuid,
  p_exclude_pay_batch_id uuid DEFAULT NULL::uuid,
  p_max_components integer DEFAULT 100
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'extensions', 'pg_temp'
SET plpgsql_check.mode TO 'disabled'
AS $function$
DECLARE
  v_target_pay_method text :=
    UPPER(BTRIM(COALESCE(p_target_pay_method, '')));

  v_chain jsonb;
  v_root_timesheet_id uuid;
  v_correction_financials_policy_envelope jsonb;
  v_correction_financials_policy_envelope_fingerprint text;
  v_latest_positive_timesheet_id uuid;
  v_member_ids uuid[] := ARRAY[]::uuid[];
  v_member_count integer := 0;

  v_chain_candidate_id uuid;
  v_chain_client_id uuid;
  v_source_family_key text;
  v_source_pay_methods jsonb := '[]'::jsonb;
  v_distinct_candidate_count integer := 0;
  v_distinct_client_count integer := 0;
  v_source_pay_method_count integer := 0;
  v_mismatched_source_method_count integer := 0;
  v_anchor_mismatch_count integer := 0;

  v_component_count integer := 0;
  v_components jsonb := '[]'::jsonb;
  v_total_raw_ex numeric := 0;
  v_total_effective_ex numeric := 0;
  v_total_raw_inc numeric := 0;
  v_total_effective_inc numeric := 0;
  v_resolution_required_count integer := 0;
  v_unresolved_count integer := 0;
  v_reservation_overrun_count integer := 0;
  v_draftable boolean := false;
  v_residual_fingerprint text;
BEGIN
  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION 'CORRECTION_RESIDUAL_TIMESHEET_ID_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  IF p_candidate_id IS NULL THEN
    RAISE EXCEPTION 'CORRECTION_RESIDUAL_CANDIDATE_ID_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  IF v_target_pay_method NOT IN ('PAYE', 'UMBRELLA') THEN
    RAISE EXCEPTION 'CORRECTION_RESIDUAL_TARGET_PAY_METHOD_INVALID'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'target_pay_method', v_target_pay_method,
              'allowed', jsonb_build_array('PAYE', 'UMBRELLA')
            )::text;
  END IF;

  IF p_max_components < 1 OR p_max_components > 100 THEN
    RAISE EXCEPTION 'CORRECTION_RESIDUAL_MAX_COMPONENTS_OUT_OF_RANGE'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'min', 1,
              'max', 100,
              'supplied', p_max_components
            )::text;
  END IF;

  v_chain := public.timesheet_correction_chain_scope_v1(
    p_timesheet_id,
    false,
    32,
    100
  );

  IF COALESCE((v_chain ->> 'valid')::boolean, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'CORRECTION_RESIDUAL_CHAIN_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'chain', v_chain
            )::text;
  END IF;

  v_root_timesheet_id :=
    NULLIF(v_chain ->> 'root_timesheet_id', '')::uuid;


  -- A residual can span more than one correction operation. Each operation
  -- keeps its own two-leg envelope; the chain fingerprint freezes the ordered
  -- catalogue without forcing later corrections to reuse an earlier policy.
  v_correction_financials_policy_envelope := jsonb_build_object(
    'correction_units', COALESCE(v_chain -> 'correction_units', '[]'::jsonb),
    'chain_fingerprint', v_chain ->> 'chain_fingerprint'
  );
  v_correction_financials_policy_envelope_fingerprint :=
    NULLIF(v_chain ->> 'chain_fingerprint', '');

  IF jsonb_array_length(COALESCE(v_chain -> 'correction_units', '[]'::jsonb)) < 1
     OR v_correction_financials_policy_envelope_fingerprint IS NULL THEN
    RAISE EXCEPTION 'CORRECTION_RESIDUAL_CORRECTION_FINANCIALS_POLICY_ENVELOPE_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'root_timesheet_id', v_root_timesheet_id::text,
              'chain_errors', COALESCE(v_chain -> 'errors', '[]'::jsonb)
            )::text;
  END IF;

  v_latest_positive_timesheet_id :=
    NULLIF(v_chain ->> 'latest_positive_timesheet_id', '')::uuid;

  SELECT COALESCE(
    array_agg(member_text::uuid ORDER BY member_text::uuid),
    ARRAY[]::uuid[]
  )
  INTO v_member_ids
  FROM jsonb_array_elements_text(
    COALESCE(v_chain -> 'member_timesheet_ids', '[]'::jsonb)
  ) AS chain_member(member_text);

  v_member_count := cardinality(v_member_ids);

  IF v_member_count < 1 OR v_member_count > 100 THEN
    RAISE EXCEPTION 'CORRECTION_RESIDUAL_MEMBER_COUNT_OUT_OF_RANGE'
      USING ERRCODE = '54001',
            DETAIL = jsonb_build_object(
              'member_count', v_member_count,
              'max', 100
            )::text;
  END IF;

  SELECT
    min(financial_row.candidate_id::text)::uuid,
    min(financial_row.client_id::text)::uuid,
    count(DISTINCT financial_row.candidate_id)
      FILTER (WHERE financial_row.candidate_id IS NOT NULL)::integer,
    count(DISTINCT financial_row.client_id)
      FILTER (WHERE financial_row.client_id IS NOT NULL)::integer,
    COALESCE(
      jsonb_agg(
        DISTINCT UPPER(BTRIM(financial_row.pay_method))
        ORDER BY UPPER(BTRIM(financial_row.pay_method))
      ) FILTER (
        WHERE NULLIF(BTRIM(COALESCE(financial_row.pay_method, '')), '')
          IS NOT NULL
      ),
      '[]'::jsonb
    ),
    count(DISTINCT UPPER(BTRIM(financial_row.pay_method)))
      FILTER (
        WHERE NULLIF(BTRIM(COALESCE(financial_row.pay_method, '')), '')
          IS NOT NULL
      )::integer,
    count(DISTINCT UPPER(BTRIM(financial_row.pay_method)))
      FILTER (
        WHERE NULLIF(BTRIM(COALESCE(financial_row.pay_method, '')), '')
          IS NOT NULL
          AND UPPER(BTRIM(financial_row.pay_method))
              IS DISTINCT FROM v_target_pay_method
      )::integer
  INTO
    v_chain_candidate_id,
    v_chain_client_id,
    v_distinct_candidate_count,
    v_distinct_client_count,
    v_source_pay_methods,
    v_source_pay_method_count,
    v_mismatched_source_method_count
  FROM public.timesheets_financials AS financial_row
  WHERE financial_row.timesheet_id = ANY(v_member_ids)
    AND financial_row.is_current = true;

  IF v_distinct_candidate_count <> 1
     OR v_distinct_client_count <> 1 THEN
    RAISE EXCEPTION 'CORRECTION_RESIDUAL_CHAIN_IDENTITY_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'distinct_candidate_count', v_distinct_candidate_count,
              'distinct_client_count', v_distinct_client_count
            )::text;
  END IF;

  IF v_chain_candidate_id IS DISTINCT FROM p_candidate_id THEN
    RAISE EXCEPTION 'CORRECTION_RESIDUAL_CANDIDATE_MISMATCH'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'expected_candidate_id', p_candidate_id::text,
              'actual_candidate_id', CASE
                WHEN v_chain_candidate_id IS NULL THEN NULL
                ELSE v_chain_candidate_id::text
              END
            )::text;
  END IF;

  IF v_source_pay_method_count < 1 THEN
    RAISE EXCEPTION 'CORRECTION_RESIDUAL_SOURCE_PAY_METHOD_MISSING'
      USING ERRCODE = 'P0001';
  END IF;


  SELECT count(*)::integer
  INTO v_anchor_mismatch_count
  FROM public.timesheets AS correction_member
  LEFT JOIN LATERAL (
    SELECT financial_row.*
    FROM public.timesheets_financials AS financial_row
    WHERE financial_row.timesheet_id = correction_member.timesheet_id
      AND financial_row.is_current = true
    ORDER BY financial_row.computed_at_utc DESC, financial_row.id DESC
    LIMIT 1
  ) AS current_financial
    ON true
  LEFT JOIN LATERAL (
    SELECT public._ctms_correction_policy_leg_read_v1(
      correction_member.timesheet_id
    ) AS expected_leg
  ) AS policy_leg ON true
  WHERE correction_member.timesheet_id = ANY(v_member_ids)
    AND UPPER(BTRIM(COALESCE(correction_member.correction_kind, '')))
        IN ('CHANGED_HOURS_REVERSAL', 'CHANGED_HOURS_REPLACEMENT',
            'CANCELLATION_REVERSAL', 'CANCELLATION_REPLACEMENT')
    AND (
      current_financial.id IS NULL
      OR COALESCE(
           current_financial.policy_snapshot_json ->>
             'correction_financials_policy_envelope_fingerprint',
           current_financial.policy_snapshot_json #>>
             '{correction_financials_policy_envelope,envelope_fingerprint}',
           current_financial.rate_source_refs_json ->>
             'correction_financials_policy_envelope_fingerprint',
           current_financial.rate_source_refs_json #>>
             '{correction_financials_policy_envelope,envelope_fingerprint}'
         ) IS DISTINCT FROM policy_leg.expected_leg ->> 'envelope_fingerprint'
      OR COALESCE(
           current_financial.policy_snapshot_json ->> 'correction_leg_fingerprint',
           current_financial.rate_source_refs_json ->> 'correction_leg_fingerprint'
         ) IS DISTINCT FROM policy_leg.expected_leg ->> 'leg_fingerprint'
      OR COALESCE(
           current_financial.policy_snapshot_json ->> 'correction_tsfin_policy_fingerprint',
           current_financial.rate_source_refs_json ->> 'correction_tsfin_policy_fingerprint'
         ) IS DISTINCT FROM policy_leg.expected_leg #>>
           '{tsfin_policy,tsfin_policy_fingerprint}'
      OR COALESCE(
           current_financial.policy_snapshot_json ->> 'correction_invoice_policy_fingerprint',
           current_financial.rate_source_refs_json ->> 'correction_invoice_policy_fingerprint'
         ) IS DISTINCT FROM policy_leg.expected_leg #>>
           '{invoice_policy,invoice_policy_fingerprint}'
      OR CASE
           WHEN COALESCE(
                  current_financial.policy_snapshot_json ->> 'erni_pct',
                  ''
                ) ~ '^-?[0-9]+([.][0-9]+)?$'
            AND COALESCE(
                  policy_leg.expected_leg #>> '{tsfin_policy,erni_pct}',
                  ''
                ) ~ '^-?[0-9]+([.][0-9]+)?$'
             THEN (current_financial.policy_snapshot_json ->> 'erni_pct')::numeric
                  IS DISTINCT FROM
                   (policy_leg.expected_leg #>> '{tsfin_policy,erni_pct}')::numeric
           ELSE true
         END
      OR NULLIF(UPPER(BTRIM(COALESCE(
           current_financial.policy_snapshot_json ->> 'apply_erni_to',
           ''
         ))), '') IS DISTINCT FROM
         NULLIF(UPPER(BTRIM(COALESCE(
           policy_leg.expected_leg #>> '{tsfin_policy,apply_erni_to}',
           ''
         ))), '')
      OR COALESCE(
           current_financial.pay_vat_rate_pct_snapshot,
           CASE
             WHEN COALESCE(
                    current_financial.policy_snapshot_json ->> 'vat_rate_pct',
                    ''
                  ) ~ '^-?[0-9]+([.][0-9]+)?$'
               THEN (current_financial.policy_snapshot_json ->>
                       'vat_rate_pct')::numeric
             ELSE NULL::numeric
           END
         ) IS DISTINCT FROM
         CASE
           WHEN COALESCE(
                   policy_leg.expected_leg #>> '{tsfin_policy,applied_pay_vat_rate_pct}',
                  ''
                ) ~ '^-?[0-9]+([.][0-9]+)?$'
             THEN (policy_leg.expected_leg #>>
                     '{tsfin_policy,applied_pay_vat_rate_pct}')::numeric
           ELSE NULL::numeric
         END
    );

  IF v_anchor_mismatch_count > 0 THEN
    RAISE EXCEPTION 'CORRECTION_RESIDUAL_TSFIN_ANCHOR_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'root_timesheet_id', v_root_timesheet_id::text,
              'expected_chain_policy_fingerprint',
                v_correction_financials_policy_envelope_fingerprint,
              'mismatching_correction_member_count',
                v_anchor_mismatch_count
            )::text;
  END IF;

  v_source_family_key :=
    'correction-chain:' || v_root_timesheet_id::text;

  WITH
  -- Correction-chain source truth must come from the timesheet entitlement
  -- and timesheet reservation authorities only. _pay_outstanding_components
  -- also projects open finance cases, including a correction finance case
  -- created by the synchronisation that consumes this helper. Reusing that
  -- mixed projection here would feed the new case back into its own source
  -- truth and double the residual on the post-sync recheck.
  live_entitlement_components AS (
    SELECT
      entitlement_row.timesheet_id,
      UPPER(BTRIM(entitlement_row.key_type)) AS key_type,
      BTRIM(entitlement_row.key_value) AS key_value,
      entitlement_row.truth_ex_vat,
      entitlement_row.baseline_ex_vat,
      entitlement_row.truth_inc_vat,
      entitlement_row.baseline_inc_vat
    FROM private.pay_workbench_current_entitlement_components_cached_v1(
      v_member_ids
    ) AS entitlement_row
    WHERE UPPER(BTRIM(entitlement_row.key_type)) = 'TS_DAY'
      AND NULLIF(BTRIM(entitlement_row.key_value), '') IS NOT NULL
  ),
  active_entitlement_reservations AS (
    SELECT
      reservation_row.timesheet_id,
      UPPER(BTRIM(reservation_row.key_type)) AS key_type,
      BTRIM(reservation_row.key_value) AS key_value,
      reservation_row.amount_ex_vat,
      reservation_row.amount_inc_vat
    FROM public._pay_reserved_components(
      v_member_ids,
      p_exclude_pay_batch_id
    ) AS reservation_row
    WHERE UPPER(BTRIM(reservation_row.key_type)) = 'TS_DAY'
      AND NULLIF(BTRIM(reservation_row.key_value), '') IS NOT NULL
  ),
  entitlement_keys AS (
    SELECT DISTINCT
      entitlement_row.timesheet_id,
      entitlement_row.key_type,
      entitlement_row.key_value
    FROM live_entitlement_components AS entitlement_row

    UNION

    SELECT DISTINCT
      reservation_row.timesheet_id,
      reservation_row.key_type,
      reservation_row.key_value
    FROM active_entitlement_reservations AS reservation_row
  ),
  entitlement_joined AS (
    SELECT
      component_key.timesheet_id,
      component_key.key_type,
      component_key.key_value,
      COALESCE(entitlement_row.truth_ex_vat, 0) AS truth_ex_vat,
      COALESCE(entitlement_row.baseline_ex_vat, 0) AS baseline_ex_vat,
      COALESCE(reservation_row.amount_ex_vat, 0) AS reserved_ex_vat,
      COALESCE(entitlement_row.truth_inc_vat, 0) AS truth_inc_vat,
      COALESCE(entitlement_row.baseline_inc_vat, 0)
        AS baseline_inc_vat,
      COALESCE(reservation_row.amount_inc_vat, 0) AS reserved_inc_vat
    FROM entitlement_keys AS component_key
    LEFT JOIN live_entitlement_components AS entitlement_row
      ON entitlement_row.timesheet_id = component_key.timesheet_id
     AND entitlement_row.key_type = component_key.key_type
     AND entitlement_row.key_value = component_key.key_value
    LEFT JOIN active_entitlement_reservations AS reservation_row
      ON reservation_row.timesheet_id = component_key.timesheet_id
     AND reservation_row.key_type = component_key.key_type
     AND reservation_row.key_value = component_key.key_value
  ),
  raw_component_totals AS (
    SELECT
      component_row.key_type,
      component_row.key_value,
      round(sum(component_row.truth_ex_vat), 2)::numeric(18,2)
        AS truth_ex_vat,
      round(sum(component_row.baseline_ex_vat), 2)::numeric(18,2)
        AS baseline_ex_vat,
      round(sum(component_row.reserved_ex_vat), 2)::numeric(18,2)
        AS reserved_ex_vat,
      round(sum(component_row.truth_inc_vat), 2)::numeric(18,2)
        AS truth_inc_vat,
      round(sum(component_row.baseline_inc_vat), 2)::numeric(18,2)
        AS baseline_inc_vat,
      round(sum(component_row.reserved_inc_vat), 2)::numeric(18,2)
        AS reserved_inc_vat
    FROM entitlement_joined AS component_row
    GROUP BY
      component_row.key_type,
      component_row.key_value
  ),
  raw_outstanding AS (
    SELECT
      component_total.key_type,
      component_total.key_value,
      component_total.truth_ex_vat,
      component_total.baseline_ex_vat,
      component_total.reserved_ex_vat,
      round(
        component_total.truth_ex_vat
        - component_total.baseline_ex_vat
        - component_total.reserved_ex_vat,
        2
      )::numeric(18,2) AS raw_outstanding_ex_vat,
      component_total.truth_inc_vat,
      component_total.baseline_inc_vat,
      component_total.reserved_inc_vat,
      round(
        component_total.truth_inc_vat
        - component_total.baseline_inc_vat
        - component_total.reserved_inc_vat,
        2
      )::numeric(18,2) AS raw_outstanding_inc_vat,
      round(abs(component_total.reserved_ex_vat), 2)
        > round(
            abs(GREATEST(
              component_total.truth_ex_vat
              - component_total.baseline_ex_vat,
              0
            )),
            2
          ) AS reservation_overrun_detected
    FROM raw_component_totals AS component_total
  ),
  family_components AS (
    SELECT
      finance_component.id AS finance_component_id,
      finance_component.component_key_type,
      finance_component.component_key_value
    FROM public.pay_finance_case_components AS finance_component
    WHERE finance_component.candidate_id = p_candidate_id
      AND finance_component.source_family_key = v_source_family_key
      AND UPPER(BTRIM(finance_component.component_key_type)) = 'TS_DAY'
  ),
  settled_movements AS (
    SELECT
      UPPER(BTRIM(family_component.component_key_type)) AS key_type,
      BTRIM(family_component.component_key_value) AS key_value,
      round(sum(
        CASE
          WHEN UPPER(BTRIM(COALESCE(batch_item.item_type, '')))
               = 'OVERPAYMENT_RECOVERY'
            THEN abs(COALESCE(
              reservation_row.reserved_source_amount,
              batch_item.frozen_source_amount,
              batch_item.amount_ex_vat,
              0
            ))
          ELSE 0
        END
      ), 2)::numeric(18,2) AS settled_recovery_ex,
      round(sum(
        CASE
          WHEN UPPER(BTRIM(COALESCE(batch_item.item_type, '')))
               = 'UNDERPAYMENT_PAYMENT'
            THEN abs(COALESCE(
              reservation_row.reserved_source_amount,
              batch_item.frozen_source_amount,
              batch_item.amount_ex_vat,
              0
            ))
          ELSE 0
        END
      ), 2)::numeric(18,2) AS settled_underpayment_ex
    FROM family_components AS family_component
    JOIN public.pay_batch_items AS batch_item
      ON batch_item.finance_component_id =
         family_component.finance_component_id
    JOIN public.pay_batch_candidates AS batch_candidate
      ON batch_candidate.id = batch_item.pay_batch_candidate_id
    LEFT JOIN public.pay_bank_transfers AS bank_transfer
      ON bank_transfer.id = batch_item.pay_bank_transfer_id
    LEFT JOIN public.pay_advance_reservations AS reservation_row
      ON reservation_row.pay_batch_item_id = batch_item.id
    WHERE COALESCE(batch_item.is_voided, false) = false
      AND (
        UPPER(BTRIM(COALESCE(
          batch_candidate.settlement_status,
          ''
        ))) = 'SETTLED'
        OR batch_candidate.settled_at_utc IS NOT NULL
        OR UPPER(BTRIM(COALESCE(bank_transfer.status, '')))
             = 'COMPLETED'
        OR bank_transfer.completed_at_utc IS NOT NULL
        OR UPPER(BTRIM(COALESCE(reservation_row.status, '')))
             = 'SETTLED'
        OR reservation_row.settled_at_utc IS NOT NULL
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_payment_correction_items AS applied_correction
        WHERE applied_correction.pay_batch_item_id = batch_item.id
          AND applied_correction.status = 'APPLIED'
          AND applied_correction.correction_item_kind IN (
            'PRE_BANK_CANCEL',
            'NO_MONEY_UNWIND',
            'SETTLED_REVERSAL'
          )
      )
    GROUP BY
      UPPER(BTRIM(family_component.component_key_type)),
      BTRIM(family_component.component_key_value)
  ),
  active_case_reservations AS (
    SELECT
      UPPER(BTRIM(family_component.component_key_type)) AS key_type,
      BTRIM(family_component.component_key_value) AS key_value,
      round(sum(
        CASE
          WHEN UPPER(BTRIM(COALESCE(batch_item.item_type, '')))
               = 'OVERPAYMENT_RECOVERY'
            THEN abs(COALESCE(
              reservation_row.reserved_source_amount,
              reservation_row.reserved_amount,
              0
            ))
          ELSE 0
        END
      ), 2)::numeric(18,2) AS reserved_recovery_ex,
      round(sum(
        CASE
          WHEN UPPER(BTRIM(COALESCE(batch_item.item_type, '')))
               = 'UNDERPAYMENT_PAYMENT'
            THEN abs(COALESCE(
              reservation_row.reserved_source_amount,
              reservation_row.reserved_amount,
              0
            ))
          ELSE 0
        END
      ), 2)::numeric(18,2) AS reserved_underpayment_ex
    FROM family_components AS family_component
    JOIN public.pay_advance_reservations AS reservation_row
      ON reservation_row.finance_component_id =
         family_component.finance_component_id
    LEFT JOIN public.pay_batch_items AS batch_item
      ON batch_item.id = reservation_row.pay_batch_item_id
    WHERE UPPER(BTRIM(COALESCE(reservation_row.status, '')))
          IN ('RESERVED', 'COMMITTED')
      AND reservation_row.released_at_utc IS NULL
      AND reservation_row.settled_at_utc IS NULL
      AND (
        p_exclude_pay_batch_id IS NULL
        OR reservation_row.pay_batch_id <> p_exclude_pay_batch_id
      )
    GROUP BY
      UPPER(BTRIM(family_component.component_key_type)),
      BTRIM(family_component.component_key_value)
  ),
  -- Settled and actively reserved recovery/payment movements are part of the
  -- pre-draft source basis.  Including them in the fingerprint makes a saved
  -- PAYE/umbrella resolution stale whenever the net economic balance changes.
  source_basis AS (
    SELECT
      raw_component.*,
      COALESCE(settled_movement.settled_recovery_ex, 0)
        AS settled_recovery_ex,
      COALESCE(settled_movement.settled_underpayment_ex, 0)
        AS settled_underpayment_ex,
      COALESCE(active_reservation.reserved_recovery_ex, 0)
        AS reserved_recovery_ex,
      COALESCE(active_reservation.reserved_underpayment_ex, 0)
        AS reserved_underpayment_ex,
      encode(
        extensions.digest(
          convert_to(
            jsonb_build_object(
              'source_family_key', v_source_family_key,
              'root_timesheet_id', v_root_timesheet_id::text,
              'correction_financials_policy_envelope_fingerprint',
                v_correction_financials_policy_envelope_fingerprint,
              'candidate_id', p_candidate_id::text,
              'source_pay_methods', v_source_pay_methods,
              'target_pay_method', v_target_pay_method,
              'component_key_type', raw_component.key_type,
              'component_key_value', raw_component.key_value,
              'truth_ex_vat', raw_component.truth_ex_vat,
              'baseline_ex_vat', raw_component.baseline_ex_vat,
              'reserved_ex_vat', raw_component.reserved_ex_vat,
              'raw_outstanding_ex_vat',
                raw_component.raw_outstanding_ex_vat,
              'truth_inc_vat', raw_component.truth_inc_vat,
              'baseline_inc_vat', raw_component.baseline_inc_vat,
              'reserved_inc_vat', raw_component.reserved_inc_vat,
              'raw_outstanding_inc_vat',
                raw_component.raw_outstanding_inc_vat,
              'settled_recovery_ex',
                COALESCE(settled_movement.settled_recovery_ex, 0),
              'settled_underpayment_ex',
                COALESCE(settled_movement.settled_underpayment_ex, 0),
              'reserved_recovery_ex',
                COALESCE(active_reservation.reserved_recovery_ex, 0),
              'reserved_underpayment_ex',
                COALESCE(active_reservation.reserved_underpayment_ex, 0)
            )::text,
            'UTF8'
          ),
          'sha256'
        ),
        'hex'
      ) AS source_basis_fingerprint
    FROM raw_outstanding AS raw_component
    LEFT JOIN settled_movements AS settled_movement
      ON settled_movement.key_type = raw_component.key_type
     AND settled_movement.key_value = raw_component.key_value
    LEFT JOIN active_case_reservations AS active_reservation
      ON active_reservation.key_type = raw_component.key_type
     AND active_reservation.key_value = raw_component.key_value
  ),
  latest_session_resolution AS (
    SELECT ranked.*
    FROM (
      SELECT resolution_source.*,
        row_number() over (
          partition by
            upper(btrim(resolution_source.component_key_type)),
            btrim(resolution_source.component_key_value)
          order by resolution_source.updated_at_utc desc,
                   resolution_source.created_at_utc desc,
                   resolution_source.id desc
        ) AS resolution_rank
      FROM public.banking_pay_workbench_session_case_resolutions
        AS resolution_source
      WHERE p_workbench_session_id IS NOT NULL
        AND resolution_source.session_id = p_workbench_session_id
        AND resolution_source.candidate_id = p_candidate_id
        AND resolution_source.source_family_key = v_source_family_key
        AND upper(btrim(coalesce(resolution_source.component_key_type,''))) = 'TS_DAY'
        AND nullif(btrim(coalesce(resolution_source.component_key_value,'')),'') IS NOT NULL
        AND resolution_source.payload_json->>
              'correction_resolution_aggregate_version'
            = 'CORRECTION_CHAIN_BUCKET_SET_V1'
    ) AS ranked
    WHERE ranked.resolution_rank = 1
  ),
  session_resolution_rows AS (
    SELECT
      UPPER(BTRIM(resolution_row.component_key_type)) AS key_type,
      BTRIM(resolution_row.component_key_value) AS key_value,
      count(*)::integer AS resolution_row_count,
      min(source_component.source_basis_fingerprint)
        AS expected_source_basis_fingerprint,
      bool_and(
        resolution_row.source_basis_fingerprint
          IS NOT DISTINCT FROM source_component.source_basis_fingerprint
      ) AS source_basis_matches,
      bool_and(
        COALESCE(
          resolution_row.payload_json ->>
            'correction_financials_policy_envelope_fingerprint',
          resolution_row.payload_json #>>
            '{source_basis,correction_financials_policy_envelope_fingerprint}',
          resolution_row.payload_json #>>
            '{saved_resolution_payload_json,correction_financials_policy_envelope_fingerprint}',
          resolution_row.payload_json #>>
            '{component_state_json,correction_financials_policy_envelope_fingerprint}'
        ) IS NOT DISTINCT FROM v_correction_financials_policy_envelope_fingerprint
      ) AS historical_anchor_matches,
      bool_and(
        resolution_row.payload_json->>
          'correction_resolution_aggregate_version'
            = 'CORRECTION_CHAIN_BUCKET_SET_V1'
        AND jsonb_typeof(
              resolution_row.payload_json->'bucket_resolutions'
            ) = 'array'
        AND COALESCE(
              NULLIF(resolution_row.payload_json->>'physical_decision_count','')::integer,
              0
            ) = jsonb_array_length(
                  resolution_row.payload_json->'bucket_resolutions'
                )
        AND COALESCE(
              NULLIF(
                resolution_row.payload_json->>'distinct_projected_target_count',
                ''
              )::integer,
              0
            ) = 1
        AND COALESCE(
              resolution_row.payload_json->>'physical_decision_set_digest',
              ''
            ) = encode(
                  extensions.digest(
                    convert_to(
                      (resolution_row.payload_json->'bucket_resolutions')::text,
                      'UTF8'
                    ),
                    'sha256'
                  ),
                  'hex'
                )
      ) AS aggregate_proof_valid,
      min(COALESCE(
        NULLIF(resolution_row.payload_json->>'physical_decision_count','')::integer,
        0
      )) AS physical_decision_count,
      min(nullif(btrim(coalesce(
        resolution_row.payload_json ->> 'resolution_economic_fingerprint',
        resolution_row.payload_json #>>
          '{saved_resolution_payload_json,resolution_economic_fingerprint}',
        resolution_row.payload_json #>>
          '{bucket_resolutions,0,resolution_economic_fingerprint}',
        resolution_row.payload_json #>>
          '{bucket_resolutions,0,saved_resolution_payload_json,resolution_economic_fingerprint}',
        ''
      )), '')) AS saved_resolution_economic_fingerprint,
      count(*) FILTER (
        WHERE COALESCE(
          resolution_row.payload_json #>>
            '{resolution_result,target_amount_ex_vat}',
          resolution_row.payload_json #>>
            '{saved_resolution_result_json,target_amount_ex_vat}',
          resolution_row.payload_json #>>
            '{result,target_amount_ex_vat}',
          resolution_row.payload_json #>>
            '{bucket_resolutions,0,resolution_result,target_amount_ex_vat}',
          resolution_row.payload_json #>>
            '{bucket_resolutions,0,saved_resolution_result_json,target_amount_ex_vat}',
          resolution_row.payload_json #>>
            '{bucket_resolutions,0,result,target_amount_ex_vat}',
          resolution_row.payload_json #>>
            '{bucket_resolutions,0,target_amount_ex_vat}',
          resolution_row.payload_json ->>
            'target_amount_ex_vat',
          ''
        ) ~ '^-?[0-9]+([.][0-9]+)?$'
      )::integer AS valid_target_amount_count,
      round(sum(
        CASE
          WHEN COALESCE(
            resolution_row.payload_json #>>
              '{resolution_result,target_amount_ex_vat}',
            resolution_row.payload_json #>>
              '{saved_resolution_result_json,target_amount_ex_vat}',
            resolution_row.payload_json #>>
              '{result,target_amount_ex_vat}',
            resolution_row.payload_json #>>
              '{bucket_resolutions,0,resolution_result,target_amount_ex_vat}',
            resolution_row.payload_json #>>
              '{bucket_resolutions,0,saved_resolution_result_json,target_amount_ex_vat}',
            resolution_row.payload_json #>>
              '{bucket_resolutions,0,result,target_amount_ex_vat}',
            resolution_row.payload_json #>>
              '{bucket_resolutions,0,target_amount_ex_vat}',
            resolution_row.payload_json ->>
              'target_amount_ex_vat',
            ''
          ) ~ '^-?[0-9]+([.][0-9]+)?$'
          THEN COALESCE(
            resolution_row.payload_json #>>
              '{resolution_result,target_amount_ex_vat}',
            resolution_row.payload_json #>>
              '{saved_resolution_result_json,target_amount_ex_vat}',
            resolution_row.payload_json #>>
              '{result,target_amount_ex_vat}',
            resolution_row.payload_json #>>
              '{bucket_resolutions,0,resolution_result,target_amount_ex_vat}',
            resolution_row.payload_json #>>
              '{bucket_resolutions,0,saved_resolution_result_json,target_amount_ex_vat}',
            resolution_row.payload_json #>>
              '{bucket_resolutions,0,result,target_amount_ex_vat}',
            resolution_row.payload_json #>>
              '{bucket_resolutions,0,target_amount_ex_vat}',
            resolution_row.payload_json ->>
              'target_amount_ex_vat'
          )::numeric
          ELSE 0
        END
      ), 2)::numeric(18,2) AS resolved_target_amount_ex_vat,
      bool_and(
        UPPER(BTRIM(COALESCE(
          resolution_row.payload_json ->> 'target_pay_method',
          resolution_row.payload_json #>>
            '{saved_resolution_payload_json,target_pay_method}',
          resolution_row.payload_json #>>
            '{bucket_resolutions,0,saved_resolution_payload_json,target_pay_method}',
          resolution_row.payload_json #>>
            '{bucket_resolutions,0,target_pay_method}',
          ''
        ))) = v_target_pay_method
      ) AS target_method_matches,
      bool_and(
        LOWER(BTRIM(COALESCE(
          resolution_row.payload_json ->> 'is_resolution_stale',
          resolution_row.payload_json #>>
            '{component_state_json,is_resolution_stale}',
          resolution_row.payload_json #>>
            '{bucket_resolutions,0,component_state_json,is_resolution_stale}',
          'false'
        ))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
      ) AS resolution_not_stale,
      jsonb_agg(
        jsonb_build_object(
          'resolution_id', resolution_row.id::text,
          'timesheet_id', CASE
            WHEN resolution_row.timesheet_id IS NULL THEN NULL
            ELSE resolution_row.timesheet_id::text
          END,
          'source_basis_fingerprint',
            resolution_row.source_basis_fingerprint,
          'resolution_identity_key',
            resolution_row.resolution_identity_key,
          'payload_json', resolution_row.payload_json
        )
        ORDER BY resolution_row.resolution_identity_key,
                 resolution_row.id
      ) AS resolution_rows
    FROM latest_session_resolution AS resolution_row
    JOIN source_basis AS source_component
      ON source_component.key_type =
           UPPER(BTRIM(resolution_row.component_key_type))
     AND source_component.key_value =
           BTRIM(resolution_row.component_key_value)
    WHERE p_workbench_session_id IS NOT NULL
      AND resolution_row.session_id = p_workbench_session_id
      AND resolution_row.candidate_id = p_candidate_id
      AND resolution_row.source_family_key = v_source_family_key
      AND UPPER(BTRIM(COALESCE(
        resolution_row.component_key_type,
        ''
      ))) = 'TS_DAY'
      AND NULLIF(BTRIM(COALESCE(
        resolution_row.component_key_value,
        ''
      )), '') IS NOT NULL
    GROUP BY
      UPPER(BTRIM(resolution_row.component_key_type)),
      BTRIM(resolution_row.component_key_value)
  ),
  component_result AS (
    SELECT
      raw_component.*,
      -- Recovery and underpayment movements form one signed ledger.  Do not
      -- clamp by the current raw sign: a later correction can legitimately
      -- cross zero, for example repaying money that was recovered in an
      -- earlier settled Banking Pay batch.
      round(
        raw_component.raw_outstanding_ex_vat
        + raw_component.settled_recovery_ex
        + raw_component.reserved_recovery_ex
        - raw_component.settled_underpayment_ex
        - raw_component.reserved_underpayment_ex,
        2
      )::numeric(18,2) AS effective_outstanding_ex_vat,

      (
        v_mismatched_source_method_count > 0
        AND round(
          raw_component.raw_outstanding_ex_vat
          + raw_component.settled_recovery_ex
          + raw_component.reserved_recovery_ex
          - raw_component.settled_underpayment_ex
          - raw_component.reserved_underpayment_ex,
          2
        ) <> 0
      ) AS resolution_required,

      COALESCE(session_resolution.resolution_row_count, 0)
        AS resolution_row_count,
      session_resolution.expected_source_basis_fingerprint,
      COALESCE(session_resolution.source_basis_matches, false)
        AS source_basis_matches,
      COALESCE(session_resolution.historical_anchor_matches, false)
        AS historical_anchor_matches,
      COALESCE(session_resolution.aggregate_proof_valid, false)
        AS aggregate_proof_valid,
      COALESCE(session_resolution.physical_decision_count, 0)
        AS physical_decision_count,
      session_resolution.saved_resolution_economic_fingerprint,
      COALESCE(session_resolution.valid_target_amount_count, 0)
        AS valid_target_amount_count,
      session_resolution.resolved_target_amount_ex_vat,
      COALESCE(session_resolution.target_method_matches, false)
        AS target_method_matches,
      COALESCE(session_resolution.resolution_not_stale, false)
        AS resolution_not_stale,
      COALESCE(session_resolution.resolution_rows, '[]'::jsonb)
        AS resolution_rows,

      CASE
        WHEN v_mismatched_source_method_count = 0 THEN true
        WHEN round(
          raw_component.raw_outstanding_ex_vat
          + raw_component.settled_recovery_ex
          + raw_component.reserved_recovery_ex
          - raw_component.settled_underpayment_ex
          - raw_component.reserved_underpayment_ex,
          2
        ) = 0 THEN true
        WHEN COALESCE(session_resolution.resolution_row_count, 0) = 1
         AND COALESCE(session_resolution.valid_target_amount_count, 0) = 1
         AND COALESCE(session_resolution.physical_decision_count, 0)
             >= GREATEST(v_mismatched_source_method_count, 1)
         AND COALESCE(session_resolution.aggregate_proof_valid, false)
         AND COALESCE(session_resolution.target_method_matches, false)
         AND COALESCE(session_resolution.resolution_not_stale, false)
         AND COALESCE(session_resolution.source_basis_matches, false)
         AND COALESCE(session_resolution.historical_anchor_matches, false)
          THEN true
        ELSE false
      END AS resolution_complete
    FROM source_basis AS raw_component
    LEFT JOIN session_resolution_rows AS session_resolution
      ON session_resolution.key_type = raw_component.key_type
     AND session_resolution.key_value = raw_component.key_value
  ),
  component_balanced AS (
    SELECT
      component_row.*,
      CASE
        WHEN component_row.raw_outstanding_ex_vat <> 0 THEN round(
          component_row.raw_outstanding_inc_vat
          * (
              component_row.effective_outstanding_ex_vat
              / component_row.raw_outstanding_ex_vat
            ),
          2
        )
        WHEN component_row.effective_outstanding_ex_vat = 0 THEN 0
        WHEN component_row.truth_ex_vat <> 0 THEN round(
          component_row.effective_outstanding_ex_vat
          * (component_row.truth_inc_vat / component_row.truth_ex_vat),
          2
        )
        WHEN component_row.baseline_ex_vat <> 0 THEN round(
          component_row.effective_outstanding_ex_vat
          * (component_row.baseline_inc_vat / component_row.baseline_ex_vat),
          2
        )
        ELSE component_row.effective_outstanding_ex_vat
      END::numeric(18,2) AS effective_outstanding_inc_vat,
      CASE
        WHEN component_row.resolution_required
         AND component_row.resolution_complete
          THEN round(
            CASE
              WHEN component_row.effective_outstanding_ex_vat = 0
                THEN 0
              ELSE
                sign(component_row.effective_outstanding_ex_vat)
                * abs(COALESCE(
                    component_row.resolved_target_amount_ex_vat,
                    0
                  ))
            END,
            2
          )
        ELSE component_row.effective_outstanding_ex_vat
      END::numeric(18,2) AS target_outstanding_ex_vat
    FROM component_result AS component_row
  ),
  component_fingerprinted AS (
    SELECT
      component_row.*,
      encode(
        extensions.digest(
          convert_to(
            jsonb_build_object(
              'canonical_correction_key',
                concat_ws(
                  '|',
                  'CORRECTION_CHAIN_V1',
                  p_candidate_id::text,
                  v_root_timesheet_id::text,
                  upper(btrim(component_row.key_type)),
                  CASE
                    WHEN upper(btrim(component_row.key_type)) = 'TS_DAY'
                      THEN (component_row.key_value::date)::text
                    ELSE btrim(component_row.key_value)
                  END
                ),
              'ordered_member_timesheet_ids', to_jsonb(v_member_ids),
              'chain_fingerprint', v_chain ->> 'chain_fingerprint',
              'source_basis_fingerprint',
                component_row.source_basis_fingerprint,
              'correction_financials_policy_envelope_fingerprint',
                v_correction_financials_policy_envelope_fingerprint,
              'classification', 'TAXABLE_CHANNEL_SENSITIVE',
              'source_pay_methods', v_source_pay_methods,
              'target_pay_method', v_target_pay_method,
              'truth_ex_vat', component_row.truth_ex_vat,
              'baseline_ex_vat', component_row.baseline_ex_vat,
              'reserved_ex_vat', component_row.reserved_ex_vat,
              'raw_outstanding_ex_vat',
                component_row.raw_outstanding_ex_vat,
              'effective_source_outstanding_ex_vat',
                component_row.effective_outstanding_ex_vat,
              'raw_outstanding_inc_vat',
                component_row.raw_outstanding_inc_vat,
              'effective_source_outstanding_inc_vat',
                component_row.effective_outstanding_inc_vat,
              'resolution_required', component_row.resolution_required
            )::text,
            'UTF8'
          ),
          'sha256'
        ),
        'hex'
      ) AS current_resolution_economic_fingerprint
    FROM component_balanced AS component_row
  ),
  component_verified AS (
    SELECT
      component_row.*,
      (
        component_row.resolution_not_stale
        AND (
          NOT component_row.resolution_required
          OR component_row.saved_resolution_economic_fingerprint
            IS NOT DISTINCT FROM
              component_row.current_resolution_economic_fingerprint
        )
      ) AS verified_resolution_not_stale,
      (
        component_row.resolution_complete
        AND (
          NOT component_row.resolution_required
          OR component_row.saved_resolution_economic_fingerprint
            IS NOT DISTINCT FROM
              component_row.current_resolution_economic_fingerprint
        )
      ) AS verified_resolution_complete,
      CASE
        WHEN component_row.resolution_required
         AND component_row.saved_resolution_economic_fingerprint
           IS DISTINCT FROM
             component_row.current_resolution_economic_fingerprint
          THEN component_row.effective_outstanding_ex_vat
        ELSE component_row.target_outstanding_ex_vat
      END::numeric(18,2) AS verified_target_outstanding_ex_vat
    FROM component_fingerprinted AS component_row
  )
  SELECT
    count(*)::integer,
    COALESCE(
      jsonb_agg(
        jsonb_build_object(
          'canonical_correction_key',
            concat_ws(
              '|',
              'CORRECTION_CHAIN_V1',
              p_candidate_id::text,
              v_root_timesheet_id::text,
              upper(btrim(component_row.key_type)),
              CASE
                WHEN upper(btrim(component_row.key_type)) = 'TS_DAY'
                  THEN (component_row.key_value::date)::text
                ELSE btrim(component_row.key_value)
              END
            ),
          'correction_identity_version', 'CORRECTION_CHAIN_V1',
          'correction_root_id', v_root_timesheet_id::text,
          'ordered_member_timesheet_ids', to_jsonb(v_member_ids),
          'component_lineage_fingerprint',
            encode(
              extensions.digest(
                convert_to(
                  jsonb_build_object(
                    'identity_version', 'CORRECTION_CHAIN_V1',
                    'candidate_id', p_candidate_id::text,
                    'correction_root_id', v_root_timesheet_id::text,
                    'component_key_type', upper(btrim(component_row.key_type)),
                    'component_key_value', CASE
                      WHEN upper(btrim(component_row.key_type)) = 'TS_DAY'
                        THEN (component_row.key_value::date)::text
                      ELSE btrim(component_row.key_value)
                    END,
                    'ordered_member_timesheet_ids', to_jsonb(v_member_ids),
                    'chain_fingerprint', v_chain ->> 'chain_fingerprint'
                  )::text,
                  'UTF8'
                ),
                'sha256'
              ),
              'hex'
            ),
          'resolution_economic_fingerprint',
            component_row.current_resolution_economic_fingerprint,
          'carrier_timesheet_id',
            v_latest_positive_timesheet_id::text,
          'source_family_key', v_source_family_key,
          'source_basis_fingerprint',
            component_row.source_basis_fingerprint,
          'correction_financials_policy_envelope_fingerprint',
            v_correction_financials_policy_envelope_fingerprint,
          'correction_financials_policy_envelope',
            v_correction_financials_policy_envelope,
          'component_key_type', component_row.key_type,
          'component_key_value', component_row.key_value,
          'classification', 'TAXABLE_CHANNEL_SENSITIVE',
          'source_pay_methods', v_source_pay_methods,
          'target_pay_method', v_target_pay_method,
          'truth_ex_vat', component_row.truth_ex_vat,
          'baseline_ex_vat', component_row.baseline_ex_vat,
          'reserved_ex_vat', component_row.reserved_ex_vat,
          'raw_outstanding_ex_vat',
            component_row.raw_outstanding_ex_vat,
          'settled_recovery_ex',
            component_row.settled_recovery_ex,
          'reserved_recovery_ex',
            component_row.reserved_recovery_ex,
          'settled_underpayment_ex',
            component_row.settled_underpayment_ex,
          'reserved_underpayment_ex',
            component_row.reserved_underpayment_ex,
          'effective_source_outstanding_ex_vat',
            component_row.effective_outstanding_ex_vat,
          'target_outstanding_ex_vat',
            component_row.verified_target_outstanding_ex_vat,
          'raw_outstanding_inc_vat',
            component_row.raw_outstanding_inc_vat,
          'effective_source_outstanding_inc_vat',
            component_row.effective_outstanding_inc_vat,
          'reservation_overrun_detected',
            component_row.reservation_overrun_detected,
          'resolution_required',
            component_row.resolution_required,
          'resolution_complete',
            component_row.verified_resolution_complete,
          'resolution_row_count',
            component_row.resolution_row_count,
          'resolution_not_stale',
            component_row.verified_resolution_not_stale,
          'saved_resolution_economic_fingerprint',
            component_row.saved_resolution_economic_fingerprint,
          'expected_source_basis_fingerprint',
            component_row.expected_source_basis_fingerprint,
          'source_basis_matches',
            component_row.source_basis_matches,
          'historical_anchor_matches',
            component_row.historical_anchor_matches,
          'aggregate_proof_valid',
            component_row.aggregate_proof_valid,
          'physical_decision_count',
            component_row.physical_decision_count,
          'resolved_target_amount_ex_vat',
            component_row.resolved_target_amount_ex_vat,
          'resolution_rows',
            component_row.resolution_rows
        )
        ORDER BY component_row.key_type, component_row.key_value
      ),
      '[]'::jsonb
    ),
    round(COALESCE(sum(component_row.raw_outstanding_ex_vat), 0), 2),
    round(COALESCE(sum(component_row.verified_target_outstanding_ex_vat), 0), 2),
    round(COALESCE(sum(component_row.raw_outstanding_inc_vat), 0), 2),
    round(COALESCE(sum(component_row.effective_outstanding_inc_vat), 0), 2),
    count(*) FILTER (
      WHERE component_row.resolution_required
    )::integer,
    count(*) FILTER (
      WHERE component_row.resolution_required
        AND NOT component_row.verified_resolution_complete
    )::integer,
    count(*) FILTER (
      WHERE component_row.reservation_overrun_detected
    )::integer
  INTO
    v_component_count,
    v_components,
    v_total_raw_ex,
    v_total_effective_ex,
    v_total_raw_inc,
    v_total_effective_inc,
    v_resolution_required_count,
    v_unresolved_count,
    v_reservation_overrun_count
  FROM component_verified AS component_row;

  IF v_component_count > p_max_components THEN
    RAISE EXCEPTION 'CORRECTION_RESIDUAL_COMPONENT_LIMIT_EXCEEDED'
      USING ERRCODE = '54001',
            DETAIL = jsonb_build_object(
              'component_count', v_component_count,
              'max_components', p_max_components
            )::text;
  END IF;

  v_draftable :=
    v_component_count > 0
    AND v_unresolved_count = 0
    AND v_reservation_overrun_count = 0;

  IF v_unresolved_count > 0 THEN
    v_total_effective_ex := NULL::numeric;
  END IF;

  v_residual_fingerprint := encode(
    extensions.digest(
      convert_to(
        jsonb_build_object(
          'chain_fingerprint', v_chain ->> 'chain_fingerprint',
          'correction_financials_policy_envelope_fingerprint',
            v_correction_financials_policy_envelope_fingerprint,
          'candidate_id', p_candidate_id::text,
          'target_pay_method', v_target_pay_method,
          'source_family_key', v_source_family_key,
          'components', v_components,
          'exclude_pay_batch_id', CASE
            WHEN p_exclude_pay_batch_id IS NULL THEN NULL
            ELSE p_exclude_pay_batch_id::text
          END
        )::text,
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  );

  RETURN jsonb_build_object(
    'ok', true,
    'draftable', v_draftable,
    'root_timesheet_id', v_root_timesheet_id::text,
    'correction_identity_version', 'CORRECTION_CHAIN_V1',
    'ordered_member_timesheet_ids', to_jsonb(v_member_ids),
    'latest_positive_timesheet_id',
      v_latest_positive_timesheet_id::text,
    'member_timesheet_ids', to_jsonb(v_member_ids),
    'member_count', v_member_count,
    'candidate_id', p_candidate_id::text,
    'client_id', CASE
      WHEN v_chain_client_id IS NULL THEN NULL
      ELSE v_chain_client_id::text
    END,
    'source_family_key', v_source_family_key,
    'correction_financials_policy_envelope', v_correction_financials_policy_envelope,
    'correction_financials_policy_envelope_fingerprint',
      v_correction_financials_policy_envelope_fingerprint,
    'source_pay_methods', v_source_pay_methods,
    'source_pay_method_count', v_source_pay_method_count,
    'target_pay_method', v_target_pay_method,
    'component_count', v_component_count,
    'components', v_components,
    'total_raw_outstanding_ex_vat', v_total_raw_ex,
    'total_target_outstanding_ex_vat', v_total_effective_ex,
    'total_raw_outstanding_inc_vat', v_total_raw_inc,
    'total_effective_source_outstanding_inc_vat',
      v_total_effective_inc,
    'resolution_required_count', v_resolution_required_count,
    'unresolved_count', v_unresolved_count,
    'reservation_overrun_count', v_reservation_overrun_count,
    'chain_fingerprint', v_chain ->> 'chain_fingerprint',
    'residual_fingerprint', v_residual_fingerprint,
    'block_code', CASE
      WHEN v_reservation_overrun_count > 0
        THEN 'CORRECTION_CHAIN_RESERVATION_OVERRUN'
      WHEN v_unresolved_count > 0
        THEN 'CORRECTION_CHAIN_PAY_METHOD_RESOLUTION_REQUIRED'
      WHEN v_component_count = 0
        THEN 'CORRECTION_CHAIN_NO_TS_DAY_COMPONENTS'
      ELSE NULL
    END
  );
END;
$function$;

COMMENT ON FUNCTION public.pay_correction_chain_residual_v1(
  uuid,
  uuid,
  text,
  uuid,
  uuid,
  integer
)
IS 'Computes one bounded pre-draft TS_DAY correction-chain residual using existing Policy X authorities and the ordered frozen per-correction policy envelopes in every source-basis and resolution fingerprint.';

REVOKE ALL ON FUNCTION public.pay_correction_chain_residual_v1(
  uuid,
  uuid,
  text,
  uuid,
  uuid,
  integer
) FROM PUBLIC, anon, authenticated;

GRANT EXECUTE ON FUNCTION public.pay_correction_chain_residual_v1(
  uuid,
  uuid,
  text,
  uuid,
  uuid,
  integer
) TO service_role;
