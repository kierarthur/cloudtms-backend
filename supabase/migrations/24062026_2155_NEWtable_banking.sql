-- CloudTMS targeted Timesheets Summary payment badge/cache correctness fix
-- Policy X: display/cache-only. No payment, reservation, settlement, VAT,
-- economic-key, remittance, provider, or frozen-authority economics are changed.

BEGIN;

-- ============================================================================
-- 001_timesheet_summary_pay_state_cache.sql
-- ============================================================================
CREATE TABLE IF NOT EXISTS public.timesheet_summary_pay_state_cache (
  timesheet_id uuid PRIMARY KEY
    REFERENCES public.timesheets(timesheet_id) ON DELETE CASCADE,
  paid_to_date_ex_vat numeric NOT NULL DEFAULT 0,
  last_paid_at_utc timestamptz NULL,
  reserved_ex_vat numeric NOT NULL DEFAULT 0,
  outstanding_ex_vat numeric NOT NULL DEFAULT 0,
  net_delta_ex_vat numeric NOT NULL DEFAULT 0,
  active_advance boolean NOT NULL DEFAULT false,
  active_processing boolean NOT NULL DEFAULT false,
  summary_state_applies boolean NOT NULL DEFAULT false,
  advance_override_created_at_utc timestamptz NULL,
  advance_authorisation_consumed_at_utc timestamptz NULL,
  summary_pay_status_code text NOT NULL DEFAULT 'UNPAID',
  summary_pay_icon_code text NOT NULL DEFAULT 'NONE',
  summary_badge_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
  refreshed_at_utc timestamptz NOT NULL DEFAULT now(),
  refreshed_by_user_id uuid NULL,
  CONSTRAINT timesheet_summary_pay_state_cache_status_chk CHECK (
    summary_pay_status_code = ANY (
      ARRAY['PAID','PARTIALLY_PAID','PROCESSING','OVERPAID','UNPAID']::text[]
    )
  ),
  CONSTRAINT timesheet_summary_pay_state_cache_icon_chk CHECK (
    summary_pay_icon_code = ANY (
      ARRAY['COIN','HALF_COIN','CLOCK','RED_COIN','NONE']::text[]
    )
  ),
  CONSTRAINT timesheet_summary_pay_state_cache_badges_chk CHECK (
    summary_badge_codes <@ ARRAY[
      '__PAY_BADGE_ADV__',
      '__PAY_BADGE_OVERPAID__',
      '__PAY_BADGE_PROCESSING__'
    ]::text[]
  )
);

-- Idempotent support for an interrupted/partially-applied deployment.
ALTER TABLE public.timesheet_summary_pay_state_cache
  ADD COLUMN IF NOT EXISTS summary_state_applies boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS advance_override_created_at_utc timestamptz NULL,
  ADD COLUMN IF NOT EXISTS advance_authorisation_consumed_at_utc timestamptz NULL;

COMMENT ON TABLE public.timesheet_summary_pay_state_cache IS
  'Mutation-side cache for Timesheets Summary payment display state. It is not a settlement ledger and must not be used as payment authority.';

COMMENT ON COLUMN public.timesheet_summary_pay_state_cache.summary_state_applies IS
  'True only when the display cache should override legacy Timesheets Summary payment fallbacks.';

COMMENT ON COLUMN public.timesheet_summary_pay_state_cache.advance_authorisation_consumed_at_utc IS
  'Display-only sticky marker: an Advance Pay badge was consumed by normal authorisation. It does not mutate the source override or any frozen batch artefact.';

COMMENT ON COLUMN public.timesheet_summary_pay_state_cache.summary_badge_codes IS
  'Internal UI overlay tokens. Issue filtering must ignore these tokens.';

-- ============================================================================
-- 002_pay_timesheet_summary_pay_state_refresh.sql
-- ============================================================================
CREATE OR REPLACE FUNCTION public.pay_timesheet_summary_pay_state_refresh(
  p_timesheet_ids uuid[],
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_requested_ids uuid[] := ARRAY[]::uuid[];
  v_target_ids uuid[] := ARRAY[]::uuid[];
  v_refreshed_count integer := 0;
  v_legacy_state_rows_updated integer := 0;
BEGIN
  SELECT COALESCE(
    ARRAY_AGG(DISTINCT input_rows.timesheet_id ORDER BY input_rows.timesheet_id),
    ARRAY[]::uuid[]
  )
  INTO v_requested_ids
  FROM UNNEST(COALESCE(p_timesheet_ids, ARRAY[]::uuid[])) AS input_rows(timesheet_id)
  WHERE input_rows.timesheet_id IS NOT NULL;

  IF COALESCE(CARDINALITY(v_requested_ids), 0) = 0 THEN
    RETURN jsonb_build_object(
      'ok', true,
      'requested_count', 0,
      'target_count', 0,
      'refreshed_count', 0,
      'legacy_state_rows_updated', 0
    );
  END IF;

  IF CARDINALITY(v_requested_ids) > 100 THEN
    RAISE EXCEPTION 'pay_timesheet_summary_pay_state_refresh accepts at most 100 distinct timesheet ids per call';
  END IF;

  SELECT COALESCE(
    ARRAY_AGG(DISTINCT resolved_rows.target_timesheet_id ORDER BY resolved_rows.target_timesheet_id),
    ARRAY[]::uuid[]
  )
  INTO v_target_ids
  FROM (
    SELECT
      COALESCE(
        rotation_rows.canonical_timesheet_id,
        rotation_rows.requested_timesheet_id
      ) AS target_timesheet_id
    FROM public._pay_timesheet_rotation_scope(v_requested_ids) AS rotation_rows
  ) AS resolved_rows
  JOIN public.timesheets AS target_timesheet
    ON target_timesheet.timesheet_id = resolved_rows.target_timesheet_id
  WHERE resolved_rows.target_timesheet_id IS NOT NULL;

  IF COALESCE(CARDINALITY(v_target_ids), 0) = 0 THEN
    RETURN jsonb_build_object(
      'ok', true,
      'requested_count', CARDINALITY(v_requested_ids),
      'target_count', 0,
      'refreshed_count', 0,
      'legacy_state_rows_updated', 0
    );
  END IF;

  WITH
  target_ids AS MATERIALIZED (
    SELECT target_values.timesheet_id
    FROM UNNEST(v_target_ids) AS target_values(timesheet_id)
  ),
  target_id_array AS MATERIALIZED (
    SELECT ARRAY_AGG(target_ids.timesheet_id ORDER BY target_ids.timesheet_id) AS timesheet_ids
    FROM target_ids
  ),
  family_scope AS MATERIALIZED (
    SELECT DISTINCT
      rotation_rows.family_timesheet_id,
      COALESCE(
        rotation_rows.canonical_timesheet_id,
        rotation_rows.requested_timesheet_id
      ) AS projected_timesheet_id
    FROM target_id_array
    CROSS JOIN LATERAL public._pay_timesheet_rotation_scope(
      target_id_array.timesheet_ids
    ) AS rotation_rows
    WHERE rotation_rows.family_timesheet_id IS NOT NULL
      AND COALESCE(
        rotation_rows.canonical_timesheet_id,
        rotation_rows.requested_timesheet_id
      ) IS NOT NULL
  ),
  outstanding_components AS MATERIALIZED (
    SELECT
      outstanding_rows.timesheet_id,
      outstanding_rows.truth_ex_vat,
      outstanding_rows.baseline_ex_vat,
      outstanding_rows.reserved_ex_vat,
      outstanding_rows.outstanding_ex_vat
    FROM target_id_array
    CROSS JOIN LATERAL public._pay_outstanding_components(
      target_id_array.timesheet_ids
    ) AS outstanding_rows
  ),
  component_totals AS MATERIALIZED (
    SELECT
      target_ids.timesheet_id,
      ROUND(
        COALESCE(SUM(COALESCE(outstanding_components.baseline_ex_vat, 0)), 0),
        2
      )::numeric AS paid_to_date_ex_vat,
      ROUND(
        COALESCE(
          SUM(
            COALESCE(outstanding_components.truth_ex_vat, 0)
            - COALESCE(outstanding_components.baseline_ex_vat, 0)
          ),
          0
        ),
        2
      )::numeric AS net_delta_ex_vat,
      ROUND(
        COALESCE(SUM(COALESCE(outstanding_components.reserved_ex_vat, 0)), 0),
        2
      )::numeric AS reserved_ex_vat,
      ROUND(
        COALESCE(SUM(COALESCE(outstanding_components.outstanding_ex_vat, 0)), 0),
        2
      )::numeric AS outstanding_ex_vat
    FROM target_ids
    LEFT JOIN outstanding_components
      ON outstanding_components.timesheet_id = target_ids.timesheet_id
    GROUP BY target_ids.timesheet_id
  ),
  legacy_state_presence AS MATERIALIZED (
    SELECT
      target_ids.timesheet_id,
      COALESCE(BOOL_OR(legacy_state.timesheet_id IS NOT NULL), false) AS has_legacy_pay_state
    FROM target_ids
    LEFT JOIN family_scope
      ON family_scope.projected_timesheet_id = target_ids.timesheet_id
    LEFT JOIN public.timesheet_pay_state AS legacy_state
      ON legacy_state.timesheet_id = family_scope.family_timesheet_id
    GROUP BY target_ids.timesheet_id
  ),
  actual_last_paid_totals AS MATERIALIZED (
    SELECT
      family_scope.projected_timesheet_id AS timesheet_id,
      MAX(
        GREATEST(
          paid_candidate.settled_at_utc,
          paid_transfer.completed_at_utc,
          paid_history.settled_at_utc,
          paid_batch.completed_at_utc
        )
      ) AS last_paid_at_utc
    FROM family_scope
    JOIN public.pay_batch_items AS paid_item
      ON paid_item.timesheet_id = family_scope.family_timesheet_id
    JOIN public.pay_batch_candidates AS paid_candidate
      ON paid_candidate.id = paid_item.pay_batch_candidate_id
    JOIN public.pay_batches AS paid_batch
      ON paid_batch.id = paid_candidate.pay_batch_id
    LEFT JOIN public.pay_bank_transfers AS paid_transfer
      ON paid_transfer.id = paid_item.pay_bank_transfer_id
     AND paid_transfer.pay_batch_id = paid_batch.id
    LEFT JOIN public.timesheet_pay_state_history AS paid_history
      ON paid_history.pay_batch_id = paid_candidate.pay_batch_id
     AND paid_history.timesheet_id = paid_item.timesheet_id
    WHERE COALESCE(paid_item.is_voided, false) = false
      AND UPPER(BTRIM(COALESCE(paid_item.item_type, ''))) IN (
        'SEGMENT_DELTA',
        'EXPENSE_DELTA',
        'ADJUSTMENT_DELTA',
        'MILEAGE_DELTA'
      )
      AND (
        UPPER(BTRIM(COALESCE(paid_candidate.settlement_status, ''))) = 'SETTLED'
        OR paid_candidate.settled_at_utc IS NOT NULL
        OR UPPER(BTRIM(COALESCE(paid_transfer.status, ''))) = 'COMPLETED'
        OR paid_transfer.completed_at_utc IS NOT NULL
        OR paid_history.id IS NOT NULL
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_payment_correction_items AS applied_correction
        WHERE applied_correction.pay_batch_item_id = paid_item.id
          AND applied_correction.status = 'APPLIED'
          AND applied_correction.correction_item_kind IN (
            'PRE_BANK_CANCEL',
            'NO_MONEY_UNWIND',
            'SETTLED_REVERSAL'
          )
      )
    GROUP BY family_scope.projected_timesheet_id
  ),
  legacy_last_paid_totals AS MATERIALIZED (
    SELECT
      target_ids.timesheet_id,
      MAX(
        COALESCE(
          legacy_state.last_settled_at_utc,
          legacy_state.summary_pay_paid_at_utc
        )
      ) AS last_paid_at_utc
    FROM target_ids
    LEFT JOIN family_scope
      ON family_scope.projected_timesheet_id = target_ids.timesheet_id
    LEFT JOIN public.timesheet_pay_state AS legacy_state
      ON legacy_state.timesheet_id = family_scope.family_timesheet_id
    GROUP BY target_ids.timesheet_id
  ),
  last_paid_totals AS MATERIALIZED (
    SELECT
      target_ids.timesheet_id,
      COALESCE(
        actual_last_paid_totals.last_paid_at_utc,
        legacy_last_paid_totals.last_paid_at_utc
      ) AS last_paid_at_utc
    FROM target_ids
    LEFT JOIN actual_last_paid_totals
      ON actual_last_paid_totals.timesheet_id = target_ids.timesheet_id
    LEFT JOIN legacy_last_paid_totals
      ON legacy_last_paid_totals.timesheet_id = target_ids.timesheet_id
  ),
  latest_advance_override AS MATERIALIZED (
    SELECT DISTINCT ON (family_scope.projected_timesheet_id)
      family_scope.projected_timesheet_id AS timesheet_id,
      payment_override.id AS override_id,
      payment_override.created_at_utc AS override_created_at_utc
    FROM family_scope
    JOIN public.timesheet_payment_overrides AS payment_override
      ON payment_override.timesheet_id = family_scope.family_timesheet_id
    WHERE payment_override.cleared_at_utc IS NULL
      AND UPPER(BTRIM(COALESCE(payment_override.override_type, ''))) = 'ADVANCE_THIS_PAYMENT'
    ORDER BY
      family_scope.projected_timesheet_id,
      payment_override.created_at_utc DESC,
      payment_override.id DESC
  ),
  current_authorisation_state AS MATERIALIZED (
    SELECT
      target_ids.timesheet_id,
      GREATEST(
        MAX(family_timesheet.authorised_at_server),
        MAX(family_timesheet.revoked_at),
        MAX(family_financial.authorised_at_utc)
      ) AS authorised_at_utc
    FROM target_ids
    LEFT JOIN family_scope
      ON family_scope.projected_timesheet_id = target_ids.timesheet_id
    LEFT JOIN public.timesheets AS family_timesheet
      ON family_timesheet.timesheet_id = family_scope.family_timesheet_id
    LEFT JOIN public.timesheets_financials AS family_financial
      ON family_financial.timesheet_id = family_scope.family_timesheet_id
    GROUP BY target_ids.timesheet_id
  ),
  existing_display_cache AS MATERIALIZED (
    SELECT
      target_ids.timesheet_id,
      MAX(existing_cache.advance_authorisation_consumed_at_utc) AS advance_authorisation_consumed_at_utc
    FROM target_ids
    LEFT JOIN family_scope
      ON family_scope.projected_timesheet_id = target_ids.timesheet_id
    LEFT JOIN public.timesheet_summary_pay_state_cache AS existing_cache
      ON existing_cache.timesheet_id = family_scope.family_timesheet_id
    GROUP BY target_ids.timesheet_id
  ),
  advance_state_prepared AS MATERIALIZED (
    SELECT
      target_ids.timesheet_id,
      latest_advance_override.override_created_at_utc,
      current_authorisation_state.authorised_at_utc,
      CASE
        WHEN latest_advance_override.override_created_at_utc IS NULL
          THEN existing_display_cache.advance_authorisation_consumed_at_utc
        WHEN existing_display_cache.advance_authorisation_consumed_at_utc
               >= latest_advance_override.override_created_at_utc
          THEN existing_display_cache.advance_authorisation_consumed_at_utc
        WHEN current_authorisation_state.authorised_at_utc
               >= latest_advance_override.override_created_at_utc
          THEN current_authorisation_state.authorised_at_utc
        ELSE NULL::timestamptz
      END AS advance_authorisation_consumed_at_utc
    FROM target_ids
    LEFT JOIN latest_advance_override
      ON latest_advance_override.timesheet_id = target_ids.timesheet_id
    LEFT JOIN current_authorisation_state
      ON current_authorisation_state.timesheet_id = target_ids.timesheet_id
    LEFT JOIN existing_display_cache
      ON existing_display_cache.timesheet_id = target_ids.timesheet_id
  ),
  advance_state AS MATERIALIZED (
    SELECT
      advance_state_prepared.timesheet_id,
      advance_state_prepared.override_created_at_utc,
      advance_state_prepared.advance_authorisation_consumed_at_utc,
      (
        advance_state_prepared.override_created_at_utc IS NOT NULL
        AND (
          advance_state_prepared.advance_authorisation_consumed_at_utc IS NULL
          OR advance_state_prepared.advance_authorisation_consumed_at_utc
               < advance_state_prepared.override_created_at_utc
        )
      ) AS active_advance
    FROM advance_state_prepared
  ),
  active_batch_processing AS MATERIALIZED (
    SELECT DISTINCT
      family_scope.projected_timesheet_id AS timesheet_id
    FROM family_scope
    JOIN public.pay_batch_items AS batch_item
      ON batch_item.timesheet_id = family_scope.family_timesheet_id
    JOIN public.pay_batch_candidates AS batch_candidate
      ON batch_candidate.id = batch_item.pay_batch_candidate_id
    JOIN public.pay_batches AS pay_batch
      ON pay_batch.id = batch_candidate.pay_batch_id
    LEFT JOIN public.pay_bank_transfers AS bank_transfer
      ON bank_transfer.id = batch_item.pay_bank_transfer_id
     AND bank_transfer.pay_batch_id = pay_batch.id
    LEFT JOIN LATERAL public._pay_rail_state_money_movement_classify(
      bank_transfer.status,
      bank_transfer.rail_state,
      COALESCE(bank_transfer.rail_meta_json, '{}'::jsonb),
      COALESCE(bank_transfer.rail_meta_json, '{}'::jsonb)
    ) AS transfer_classifier
      ON bank_transfer.id IS NOT NULL
    WHERE COALESCE(batch_item.is_voided, false) = false
      AND UPPER(BTRIM(COALESCE(batch_item.item_type, ''))) IN (
        'SEGMENT_DELTA',
        'EXPENSE_DELTA',
        'ADJUSTMENT_DELTA',
        'MILEAGE_DELTA'
      )
      AND public._pay_batch_status_is_active_reservation(pay_batch.status)
      AND UPPER(BTRIM(COALESCE(batch_candidate.settlement_status, ''))) NOT IN (
        'SETTLED',
        'PAID',
        'CONFIRMED'
      )
      AND batch_candidate.settled_at_utc IS NULL
      AND COALESCE(transfer_classifier.is_final_money_moved, false) = false
      AND COALESCE(transfer_classifier.is_terminal_no_money, false) = false
      AND bank_transfer.completed_at_utc IS NULL
  ),
  active_correction_processing AS MATERIALIZED (
    SELECT DISTINCT
      family_scope.projected_timesheet_id AS timesheet_id
    FROM public.pay_payment_correction_items AS correction_item
    LEFT JOIN public.pay_batch_items AS correction_batch_item
      ON correction_batch_item.id = correction_item.pay_batch_item_id
    JOIN family_scope
      ON family_scope.family_timesheet_id = COALESCE(
        correction_item.timesheet_id,
        correction_batch_item.timesheet_id
      )
    JOIN public.pay_payment_correction_requests AS correction_request
      ON correction_request.id = correction_item.correction_request_id
    WHERE correction_request.status IN (
      'REQUESTED',
      'AWAITING_AUTHORISATION',
      'AUTHORISED',
      'EXPANDED',
      'PROCESSING',
      'BLOCKED'
    )
      AND correction_item.status NOT IN ('APPLIED', 'SKIPPED')
  ),
  calculated_state AS MATERIALIZED (
    SELECT
      target_ids.timesheet_id,
      COALESCE(component_totals.paid_to_date_ex_vat, 0)::numeric AS paid_to_date_ex_vat,
      last_paid_totals.last_paid_at_utc,
      COALESCE(component_totals.reserved_ex_vat, 0)::numeric AS reserved_ex_vat,
      COALESCE(component_totals.outstanding_ex_vat, 0)::numeric AS outstanding_ex_vat,
      COALESCE(component_totals.net_delta_ex_vat, 0)::numeric AS net_delta_ex_vat,
      COALESCE(advance_state.active_advance, false) AS active_advance,
      advance_state.override_created_at_utc AS advance_override_created_at_utc,
      advance_state.advance_authorisation_consumed_at_utc,
      (
        ABS(COALESCE(component_totals.reserved_ex_vat, 0)) > 0.01
        OR active_batch_processing.timesheet_id IS NOT NULL
        OR active_correction_processing.timesheet_id IS NOT NULL
      ) AS active_processing,
      (
        COALESCE(component_totals.net_delta_ex_vat, 0) < -0.01
        OR COALESCE(component_totals.outstanding_ex_vat, 0) < -0.01
      ) AS is_overpaid,
      COALESCE(legacy_state_presence.has_legacy_pay_state, false) AS has_legacy_pay_state
    FROM target_ids
    LEFT JOIN component_totals
      ON component_totals.timesheet_id = target_ids.timesheet_id
    LEFT JOIN last_paid_totals
      ON last_paid_totals.timesheet_id = target_ids.timesheet_id
    LEFT JOIN advance_state
      ON advance_state.timesheet_id = target_ids.timesheet_id
    LEFT JOIN active_batch_processing
      ON active_batch_processing.timesheet_id = target_ids.timesheet_id
    LEFT JOIN active_correction_processing
      ON active_correction_processing.timesheet_id = target_ids.timesheet_id
    LEFT JOIN legacy_state_presence
      ON legacy_state_presence.timesheet_id = target_ids.timesheet_id
  ),
  display_state_base AS MATERIALIZED (
    SELECT
      calculated_state.*,
      CASE
        WHEN calculated_state.is_overpaid THEN 'OVERPAID'
        WHEN calculated_state.paid_to_date_ex_vat > 0.01
         AND calculated_state.net_delta_ex_vat > 0.01 THEN 'PARTIALLY_PAID'
        WHEN calculated_state.paid_to_date_ex_vat > 0.01 THEN 'PAID'
        WHEN calculated_state.active_processing THEN 'PROCESSING'
        ELSE 'UNPAID'
      END::text AS summary_pay_status_code,
      (
        calculated_state.has_legacy_pay_state
        OR ABS(calculated_state.paid_to_date_ex_vat) > 0.01
        OR calculated_state.last_paid_at_utc IS NOT NULL
        OR ABS(calculated_state.reserved_ex_vat) > 0.01
        OR calculated_state.active_advance
        OR calculated_state.active_processing
        OR calculated_state.is_overpaid
      ) AS summary_state_applies
    FROM calculated_state
  ),
  display_state AS MATERIALIZED (
    SELECT
      display_state_base.timesheet_id,
      display_state_base.paid_to_date_ex_vat,
      display_state_base.last_paid_at_utc,
      display_state_base.reserved_ex_vat,
      display_state_base.outstanding_ex_vat,
      display_state_base.net_delta_ex_vat,
      display_state_base.active_advance,
      display_state_base.active_processing,
      display_state_base.summary_state_applies,
      display_state_base.advance_override_created_at_utc,
      display_state_base.advance_authorisation_consumed_at_utc,
      display_state_base.summary_pay_status_code,
      CASE display_state_base.summary_pay_status_code
        WHEN 'PARTIALLY_PAID' THEN 'HALF_COIN'
        WHEN 'PAID' THEN 'COIN'
        WHEN 'PROCESSING' THEN 'CLOCK'
        ELSE 'NONE'
      END::text AS summary_pay_icon_code,
      ARRAY_REMOVE(
        ARRAY[
          CASE
            WHEN display_state_base.active_advance THEN '__PAY_BADGE_ADV__'
            ELSE NULL::text
          END,
          CASE
            WHEN display_state_base.summary_pay_status_code = 'OVERPAID'
              THEN '__PAY_BADGE_OVERPAID__'
            ELSE NULL::text
          END,
          CASE
            WHEN display_state_base.active_processing
             AND display_state_base.summary_pay_status_code <> 'PROCESSING'
              THEN '__PAY_BADGE_PROCESSING__'
            ELSE NULL::text
          END
        ]::text[],
        NULL::text
      ) AS summary_badge_codes
    FROM display_state_base
  )
  INSERT INTO public.timesheet_summary_pay_state_cache (
    timesheet_id,
    paid_to_date_ex_vat,
    last_paid_at_utc,
    reserved_ex_vat,
    outstanding_ex_vat,
    net_delta_ex_vat,
    active_advance,
    active_processing,
    summary_state_applies,
    advance_override_created_at_utc,
    advance_authorisation_consumed_at_utc,
    summary_pay_status_code,
    summary_pay_icon_code,
    summary_badge_codes,
    refreshed_at_utc,
    refreshed_by_user_id
  )
  SELECT
    display_state.timesheet_id,
    display_state.paid_to_date_ex_vat,
    display_state.last_paid_at_utc,
    display_state.reserved_ex_vat,
    display_state.outstanding_ex_vat,
    display_state.net_delta_ex_vat,
    display_state.active_advance,
    display_state.active_processing,
    display_state.summary_state_applies,
    display_state.advance_override_created_at_utc,
    display_state.advance_authorisation_consumed_at_utc,
    display_state.summary_pay_status_code,
    display_state.summary_pay_icon_code,
    display_state.summary_badge_codes,
    now(),
    p_actor_user_id
  FROM display_state
  ORDER BY display_state.timesheet_id
  ON CONFLICT (timesheet_id) DO UPDATE
  SET
    paid_to_date_ex_vat = EXCLUDED.paid_to_date_ex_vat,
    last_paid_at_utc = EXCLUDED.last_paid_at_utc,
    reserved_ex_vat = EXCLUDED.reserved_ex_vat,
    outstanding_ex_vat = EXCLUDED.outstanding_ex_vat,
    net_delta_ex_vat = EXCLUDED.net_delta_ex_vat,
    active_advance = EXCLUDED.active_advance,
    active_processing = EXCLUDED.active_processing,
    summary_state_applies = EXCLUDED.summary_state_applies,
    advance_override_created_at_utc = EXCLUDED.advance_override_created_at_utc,
    advance_authorisation_consumed_at_utc = EXCLUDED.advance_authorisation_consumed_at_utc,
    summary_pay_status_code = EXCLUDED.summary_pay_status_code,
    summary_pay_icon_code = EXCLUDED.summary_pay_icon_code,
    summary_badge_codes = EXCLUDED.summary_badge_codes,
    refreshed_at_utc = EXCLUDED.refreshed_at_utc,
    refreshed_by_user_id = EXCLUDED.refreshed_by_user_id;

  GET DIAGNOSTICS v_refreshed_count = ROW_COUNT;

  WITH legacy_projection AS MATERIALIZED (
    SELECT DISTINCT
      rotation_rows.family_timesheet_id,
      COALESCE(
        rotation_rows.canonical_timesheet_id,
        rotation_rows.requested_timesheet_id
      ) AS projected_timesheet_id
    FROM public._pay_timesheet_rotation_scope(v_target_ids) AS rotation_rows
    WHERE rotation_rows.family_timesheet_id IS NOT NULL
      AND COALESCE(
        rotation_rows.canonical_timesheet_id,
        rotation_rows.requested_timesheet_id
      ) IS NOT NULL
  )
  UPDATE public.timesheet_pay_state AS legacy_state
  SET
    summary_pay_status_code = CASE
      WHEN summary_cache.summary_pay_status_code = 'OVERPAID' THEN 'PAID'
      ELSE summary_cache.summary_pay_status_code
    END,
    summary_pay_icon_code = CASE
      WHEN summary_cache.summary_pay_status_code = 'OVERPAID' THEN 'RED_COIN'
      ELSE summary_cache.summary_pay_icon_code
    END,
    summary_pay_paid_at_utc = summary_cache.last_paid_at_utc,
    summary_net_delta_ex_vat = summary_cache.net_delta_ex_vat
  FROM legacy_projection
  JOIN public.timesheet_summary_pay_state_cache AS summary_cache
    ON summary_cache.timesheet_id = legacy_projection.projected_timesheet_id
  WHERE legacy_state.timesheet_id = legacy_projection.family_timesheet_id
    AND summary_cache.timesheet_id = ANY (v_target_ids)
    AND (
      legacy_state.summary_pay_status_code IS DISTINCT FROM CASE
        WHEN summary_cache.summary_pay_status_code = 'OVERPAID' THEN 'PAID'
        ELSE summary_cache.summary_pay_status_code
      END
      OR legacy_state.summary_pay_icon_code IS DISTINCT FROM CASE
        WHEN summary_cache.summary_pay_status_code = 'OVERPAID' THEN 'RED_COIN'
        ELSE summary_cache.summary_pay_icon_code
      END
      OR legacy_state.summary_pay_paid_at_utc IS DISTINCT FROM summary_cache.last_paid_at_utc
      OR legacy_state.summary_net_delta_ex_vat IS DISTINCT FROM summary_cache.net_delta_ex_vat
    );

  GET DIAGNOSTICS v_legacy_state_rows_updated = ROW_COUNT;

  RETURN jsonb_build_object(
    'ok', true,
    'requested_count', CARDINALITY(v_requested_ids),
    'target_count', CARDINALITY(v_target_ids),
    'refreshed_count', v_refreshed_count,
    'legacy_state_rows_updated', v_legacy_state_rows_updated
  );
END;
$function$;

-- ============================================================================
-- 003_pay_timesheet_summary_pay_state_refresh_trigger.sql
-- ============================================================================
CREATE OR REPLACE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_chunk_ids uuid[] := ARRAY[]::uuid[];
  v_offset integer := 1;
  v_total integer := 0;
BEGIN
  IF TG_TABLE_SCHEMA <> 'public' THEN
    RETURN NULL;
  END IF;

  IF TG_TABLE_NAME = 'timesheets_financials' THEN
    IF TG_OP = 'INSERT' THEN
      SELECT COALESCE(ARRAY_AGG(DISTINCT new_rows.timesheet_id), ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM new_rows
      WHERE new_rows.timesheet_id IS NOT NULL
        AND COALESCE(new_rows.is_current, false) = true;
    ELSIF TG_OP = 'UPDATE' THEN
      WITH changed_rows AS MATERIALIZED (
        SELECT
          new_rows.timesheet_id AS new_timesheet_id,
          old_rows.timesheet_id AS old_timesheet_id,
          COALESCE(new_rows.is_current, false) AS new_is_current,
          COALESCE(old_rows.is_current, false) AS old_is_current
        FROM new_rows
        JOIN old_rows
          ON old_rows.id = new_rows.id
        WHERE new_rows.timesheet_id IS DISTINCT FROM old_rows.timesheet_id
           OR new_rows.is_current IS DISTINCT FROM old_rows.is_current
           OR new_rows.basis IS DISTINCT FROM old_rows.basis
           OR new_rows.candidate_id IS DISTINCT FROM old_rows.candidate_id
           OR new_rows.client_id IS DISTINCT FROM old_rows.client_id
           OR new_rows.pay_method IS DISTINCT FROM old_rows.pay_method
           OR new_rows.policy_snapshot_json IS DISTINCT FROM old_rows.policy_snapshot_json
           OR new_rows.rate_source_refs_json IS DISTINCT FROM old_rows.rate_source_refs_json
           OR new_rows.hours_day IS DISTINCT FROM old_rows.hours_day
           OR new_rows.hours_night IS DISTINCT FROM old_rows.hours_night
           OR new_rows.hours_sat IS DISTINCT FROM old_rows.hours_sat
           OR new_rows.hours_sun IS DISTINCT FROM old_rows.hours_sun
           OR new_rows.hours_bh IS DISTINCT FROM old_rows.hours_bh
           OR new_rows.pay_day IS DISTINCT FROM old_rows.pay_day
           OR new_rows.pay_night IS DISTINCT FROM old_rows.pay_night
           OR new_rows.pay_sat IS DISTINCT FROM old_rows.pay_sat
           OR new_rows.pay_sun IS DISTINCT FROM old_rows.pay_sun
           OR new_rows.pay_bh IS DISTINCT FROM old_rows.pay_bh
           OR new_rows.total_pay_ex_vat IS DISTINCT FROM old_rows.total_pay_ex_vat
           OR new_rows.invoice_breakdown_json IS DISTINCT FROM old_rows.invoice_breakdown_json
           OR new_rows.additional_units_json IS DISTINCT FROM old_rows.additional_units_json
           OR new_rows.additional_pay_ex_vat IS DISTINCT FROM old_rows.additional_pay_ex_vat
           OR new_rows.expenses_pay_ex_vat IS DISTINCT FROM old_rows.expenses_pay_ex_vat
           OR new_rows.travel_pay_ex_vat IS DISTINCT FROM old_rows.travel_pay_ex_vat
           OR new_rows.accommodation_pay_ex_vat IS DISTINCT FROM old_rows.accommodation_pay_ex_vat
           OR new_rows.other_pay_ex_vat IS DISTINCT FROM old_rows.other_pay_ex_vat
           OR new_rows.mileage_pay_ex_vat IS DISTINCT FROM old_rows.mileage_pay_ex_vat
           OR new_rows.worked_start_iso IS DISTINCT FROM old_rows.worked_start_iso
           OR new_rows.worked_end_iso IS DISTINCT FROM old_rows.worked_end_iso
           OR new_rows.break_start_iso IS DISTINCT FROM old_rows.break_start_iso
           OR new_rows.break_end_iso IS DISTINCT FROM old_rows.break_end_iso
           OR new_rows.break_minutes IS DISTINCT FROM old_rows.break_minutes
           OR new_rows.actual_schedule_json IS DISTINCT FROM old_rows.actual_schedule_json
           OR new_rows.pay_on_hold IS DISTINCT FROM old_rows.pay_on_hold
           OR new_rows.paid_at_utc IS DISTINCT FROM old_rows.paid_at_utc
           OR new_rows.authorised_at_utc IS DISTINCT FROM old_rows.authorised_at_utc
      ),
      affected_rows AS (
        SELECT changed_rows.new_timesheet_id AS timesheet_id
        FROM changed_rows
        WHERE changed_rows.new_timesheet_id IS NOT NULL
          AND changed_rows.new_is_current = true

        UNION

        SELECT changed_rows.old_timesheet_id AS timesheet_id
        FROM changed_rows
        WHERE changed_rows.old_timesheet_id IS NOT NULL
          AND changed_rows.old_is_current = true
      )
      SELECT COALESCE(ARRAY_AGG(DISTINCT affected_rows.timesheet_id), ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM affected_rows;
    ELSE
      SELECT COALESCE(ARRAY_AGG(DISTINCT old_rows.timesheet_id), ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM old_rows
      WHERE old_rows.timesheet_id IS NOT NULL
        AND COALESCE(old_rows.is_current, false) = true;
    END IF;

  ELSIF TG_TABLE_NAME = 'timesheets' THEN
    SELECT COALESCE(ARRAY_AGG(DISTINCT new_rows.timesheet_id), ARRAY[]::uuid[])
    INTO v_timesheet_ids
    FROM new_rows
    JOIN old_rows
      ON old_rows.timesheet_id = new_rows.timesheet_id
    WHERE new_rows.timesheet_id IS NOT NULL
      AND (
        new_rows.is_current IS DISTINCT FROM old_rows.is_current
        OR new_rows.version IS DISTINCT FROM old_rows.version
        OR new_rows.authorised_at_server IS DISTINCT FROM old_rows.authorised_at_server
        OR new_rows.revoked_at IS DISTINCT FROM old_rows.revoked_at
        OR new_rows.status IS DISTINCT FROM old_rows.status
        OR new_rows.booking_id IS DISTINCT FROM old_rows.booking_id
        OR new_rows.contract_id IS DISTINCT FROM old_rows.contract_id
        OR new_rows.sheet_scope IS DISTINCT FROM old_rows.sheet_scope
        OR new_rows.submission_mode IS DISTINCT FROM old_rows.submission_mode
        OR new_rows.line_type IS DISTINCT FROM old_rows.line_type
        OR new_rows.week_ending_date IS DISTINCT FROM old_rows.week_ending_date
        OR new_rows.reference_number IS DISTINCT FROM old_rows.reference_number
        OR new_rows.scheduled_start_iso IS DISTINCT FROM old_rows.scheduled_start_iso
        OR new_rows.scheduled_end_iso IS DISTINCT FROM old_rows.scheduled_end_iso
        OR new_rows.worked_start_iso IS DISTINCT FROM old_rows.worked_start_iso
        OR new_rows.worked_end_iso IS DISTINCT FROM old_rows.worked_end_iso
        OR new_rows.break_start_iso IS DISTINCT FROM old_rows.break_start_iso
        OR new_rows.break_end_iso IS DISTINCT FROM old_rows.break_end_iso
        OR new_rows.break_minutes IS DISTINCT FROM old_rows.break_minutes
        OR new_rows.actual_schedule_json IS DISTINCT FROM old_rows.actual_schedule_json
        OR new_rows.additional_units_week IS DISTINCT FROM old_rows.additional_units_week
        OR new_rows.additional_units_per_day IS DISTINCT FROM old_rows.additional_units_per_day
        OR new_rows.is_adjustment IS DISTINCT FROM old_rows.is_adjustment
        OR new_rows.parent_timesheet_id IS DISTINCT FROM old_rows.parent_timesheet_id
        OR new_rows.correction_id IS DISTINCT FROM old_rows.correction_id
        OR new_rows.correction_kind IS DISTINCT FROM old_rows.correction_kind
        OR new_rows.adjustment_origin IS DISTINCT FROM old_rows.adjustment_origin
      );

  ELSIF TG_TABLE_NAME = 'timesheet_payment_overrides' THEN
    IF TG_OP = 'INSERT' THEN
      SELECT COALESCE(ARRAY_AGG(DISTINCT new_rows.timesheet_id), ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM new_rows
      WHERE new_rows.timesheet_id IS NOT NULL;
    ELSIF TG_OP = 'UPDATE' THEN
      WITH changed_overrides AS (
        SELECT
          new_rows.timesheet_id AS new_timesheet_id,
          old_rows.timesheet_id AS old_timesheet_id
        FROM new_rows
        JOIN old_rows ON old_rows.id = new_rows.id
        WHERE new_rows.timesheet_id IS DISTINCT FROM old_rows.timesheet_id
           OR new_rows.override_type IS DISTINCT FROM old_rows.override_type
           OR new_rows.created_at_utc IS DISTINCT FROM old_rows.created_at_utc
           OR new_rows.consumed_by_pay_batch_id IS DISTINCT FROM old_rows.consumed_by_pay_batch_id
           OR new_rows.consumed_at_utc IS DISTINCT FROM old_rows.consumed_at_utc
           OR new_rows.cleared_at_utc IS DISTINCT FROM old_rows.cleared_at_utc
      )
      SELECT COALESCE(ARRAY_AGG(DISTINCT affected_rows.timesheet_id), ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM (
        SELECT changed_overrides.new_timesheet_id AS timesheet_id
        FROM changed_overrides
        WHERE changed_overrides.new_timesheet_id IS NOT NULL

        UNION

        SELECT changed_overrides.old_timesheet_id AS timesheet_id
        FROM changed_overrides
        WHERE changed_overrides.old_timesheet_id IS NOT NULL
      ) AS affected_rows;
    ELSE
      SELECT COALESCE(ARRAY_AGG(DISTINCT old_rows.timesheet_id), ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM old_rows
      WHERE old_rows.timesheet_id IS NOT NULL;
    END IF;

  ELSIF TG_TABLE_NAME = 'timesheet_pay_state' THEN
    IF TG_OP = 'INSERT' THEN
      SELECT COALESCE(ARRAY_AGG(DISTINCT new_rows.timesheet_id), ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM new_rows
      WHERE new_rows.timesheet_id IS NOT NULL;
    ELSIF TG_OP = 'UPDATE' THEN
      SELECT COALESCE(ARRAY_AGG(DISTINCT new_rows.timesheet_id), ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM new_rows
      JOIN old_rows
        ON old_rows.timesheet_id = new_rows.timesheet_id
      WHERE new_rows.timesheet_id IS NOT NULL
        AND (
          new_rows.last_settled_snapshot_json IS DISTINCT FROM old_rows.last_settled_snapshot_json
          OR new_rows.last_settled_signature IS DISTINCT FROM old_rows.last_settled_signature
          OR new_rows.last_settled_pay_batch_id IS DISTINCT FROM old_rows.last_settled_pay_batch_id
          OR new_rows.last_settled_at_utc IS DISTINCT FROM old_rows.last_settled_at_utc
        );
    ELSE
      SELECT COALESCE(ARRAY_AGG(DISTINCT old_rows.timesheet_id), ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM old_rows
      WHERE old_rows.timesheet_id IS NOT NULL;
    END IF;

  ELSIF TG_TABLE_NAME = 'timesheet_pay_state_history' THEN
    IF TG_OP = 'INSERT' THEN
      SELECT COALESCE(ARRAY_AGG(DISTINCT new_rows.timesheet_id), ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM new_rows
      WHERE new_rows.timesheet_id IS NOT NULL;
    ELSIF TG_OP = 'UPDATE' THEN
      WITH changed_history AS (
        SELECT
          new_rows.timesheet_id AS new_timesheet_id,
          old_rows.timesheet_id AS old_timesheet_id
        FROM new_rows
        JOIN old_rows ON old_rows.id = new_rows.id
        WHERE new_rows.timesheet_id IS DISTINCT FROM old_rows.timesheet_id
           OR new_rows.pay_batch_id IS DISTINCT FROM old_rows.pay_batch_id
           OR new_rows.settled_at_utc IS DISTINCT FROM old_rows.settled_at_utc
           OR new_rows.snapshot_json IS DISTINCT FROM old_rows.snapshot_json
           OR new_rows.signature IS DISTINCT FROM old_rows.signature
      )
      SELECT COALESCE(ARRAY_AGG(DISTINCT affected_rows.timesheet_id), ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM (
        SELECT changed_history.new_timesheet_id AS timesheet_id
        FROM changed_history
        WHERE changed_history.new_timesheet_id IS NOT NULL

        UNION

        SELECT changed_history.old_timesheet_id AS timesheet_id
        FROM changed_history
        WHERE changed_history.old_timesheet_id IS NOT NULL
      ) AS affected_rows;
    ELSE
      SELECT COALESCE(ARRAY_AGG(DISTINCT old_rows.timesheet_id), ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM old_rows
      WHERE old_rows.timesheet_id IS NOT NULL;
    END IF;

  ELSIF TG_TABLE_NAME = 'pay_batch_items' THEN
    IF TG_OP = 'UPDATE' THEN
      SELECT COALESCE(ARRAY_AGG(DISTINCT affected_rows.timesheet_id), ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM (
        SELECT new_rows.timesheet_id
        FROM new_rows
        JOIN old_rows ON old_rows.id = new_rows.id
        WHERE new_rows.timesheet_id IS NOT NULL
          AND (
            new_rows.timesheet_id IS DISTINCT FROM old_rows.timesheet_id
            OR new_rows.is_voided IS DISTINCT FROM old_rows.is_voided
          )

        UNION

        SELECT old_rows.timesheet_id
        FROM old_rows
        JOIN new_rows ON new_rows.id = old_rows.id
        WHERE old_rows.timesheet_id IS NOT NULL
          AND old_rows.timesheet_id IS DISTINCT FROM new_rows.timesheet_id
      ) AS affected_rows;
    ELSE
      SELECT COALESCE(ARRAY_AGG(DISTINCT old_rows.timesheet_id), ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM old_rows
      WHERE old_rows.timesheet_id IS NOT NULL;
    END IF;

  ELSIF TG_TABLE_NAME = 'pay_batch_candidates' THEN
    WITH changed_candidates AS (
      SELECT new_rows.id
      FROM new_rows
      JOIN old_rows ON old_rows.id = new_rows.id
      WHERE new_rows.settlement_status IS DISTINCT FROM old_rows.settlement_status
         OR new_rows.settled_at_utc IS DISTINCT FROM old_rows.settled_at_utc
         OR new_rows.pay_batch_id IS DISTINCT FROM old_rows.pay_batch_id
    )
    SELECT COALESCE(ARRAY_AGG(DISTINCT batch_item.timesheet_id), ARRAY[]::uuid[])
    INTO v_timesheet_ids
    FROM changed_candidates
    JOIN public.pay_batch_items AS batch_item
      ON batch_item.pay_batch_candidate_id = changed_candidates.id
    WHERE batch_item.timesheet_id IS NOT NULL;

  ELSIF TG_TABLE_NAME = 'pay_batches' THEN
    WITH changed_batches AS (
      SELECT new_rows.id
      FROM new_rows
      JOIN old_rows ON old_rows.id = new_rows.id
      WHERE public._pay_batch_status_is_active_reservation(old_rows.status)
            IS DISTINCT FROM
            public._pay_batch_status_is_active_reservation(new_rows.status)
         OR new_rows.cancelled_at_utc IS DISTINCT FROM old_rows.cancelled_at_utc
         OR new_rows.completed_at_utc IS DISTINCT FROM old_rows.completed_at_utc
    )
    SELECT COALESCE(ARRAY_AGG(DISTINCT batch_item.timesheet_id), ARRAY[]::uuid[])
    INTO v_timesheet_ids
    FROM changed_batches
    JOIN public.pay_batch_candidates AS batch_candidate
      ON batch_candidate.pay_batch_id = changed_batches.id
    JOIN public.pay_batch_items AS batch_item
      ON batch_item.pay_batch_candidate_id = batch_candidate.id
    WHERE batch_item.timesheet_id IS NOT NULL;

  ELSIF TG_TABLE_NAME = 'pay_bank_transfers' THEN
    WITH changed_transfers AS (
      SELECT new_rows.id
      FROM new_rows
      JOIN old_rows ON old_rows.id = new_rows.id
      WHERE new_rows.status IS DISTINCT FROM old_rows.status
         OR new_rows.rail_state IS DISTINCT FROM old_rows.rail_state
         OR new_rows.rail_meta_json IS DISTINCT FROM old_rows.rail_meta_json
         OR new_rows.completed_at_utc IS DISTINCT FROM old_rows.completed_at_utc
    )
    SELECT COALESCE(ARRAY_AGG(DISTINCT batch_item.timesheet_id), ARRAY[]::uuid[])
    INTO v_timesheet_ids
    FROM changed_transfers
    JOIN public.pay_batch_items AS batch_item
      ON batch_item.pay_bank_transfer_id = changed_transfers.id
    WHERE batch_item.timesheet_id IS NOT NULL;

  ELSIF TG_TABLE_NAME = 'pay_payment_correction_items' THEN
    IF TG_OP = 'INSERT' THEN
      SELECT COALESCE(ARRAY_AGG(DISTINCT COALESCE(new_rows.timesheet_id, batch_item.timesheet_id)), ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM new_rows
      LEFT JOIN public.pay_batch_items AS batch_item
        ON batch_item.id = new_rows.pay_batch_item_id
      WHERE COALESCE(new_rows.timesheet_id, batch_item.timesheet_id) IS NOT NULL;
    ELSIF TG_OP = 'UPDATE' THEN
      WITH changed_correction_rows AS (
        SELECT
          new_rows.timesheet_id AS new_timesheet_id,
          new_rows.pay_batch_item_id AS new_pay_batch_item_id,
          old_rows.timesheet_id AS old_timesheet_id,
          old_rows.pay_batch_item_id AS old_pay_batch_item_id
        FROM new_rows
        JOIN old_rows ON old_rows.id = new_rows.id
        WHERE new_rows.timesheet_id IS DISTINCT FROM old_rows.timesheet_id
           OR new_rows.pay_batch_item_id IS DISTINCT FROM old_rows.pay_batch_item_id
           OR new_rows.correction_request_id IS DISTINCT FROM old_rows.correction_request_id
           OR new_rows.status IS DISTINCT FROM old_rows.status
           OR new_rows.correction_item_kind IS DISTINCT FROM old_rows.correction_item_kind
      ),
      correction_rows AS (
        SELECT
          changed_correction_rows.new_timesheet_id AS timesheet_id,
          changed_correction_rows.new_pay_batch_item_id AS pay_batch_item_id
        FROM changed_correction_rows

        UNION ALL

        SELECT
          changed_correction_rows.old_timesheet_id AS timesheet_id,
          changed_correction_rows.old_pay_batch_item_id AS pay_batch_item_id
        FROM changed_correction_rows
      )
      SELECT COALESCE(ARRAY_AGG(DISTINCT COALESCE(correction_rows.timesheet_id, batch_item.timesheet_id)), ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM correction_rows
      LEFT JOIN public.pay_batch_items AS batch_item
        ON batch_item.id = correction_rows.pay_batch_item_id
      WHERE COALESCE(correction_rows.timesheet_id, batch_item.timesheet_id) IS NOT NULL;
    ELSE
      SELECT COALESCE(ARRAY_AGG(DISTINCT COALESCE(old_rows.timesheet_id, batch_item.timesheet_id)), ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM old_rows
      LEFT JOIN public.pay_batch_items AS batch_item
        ON batch_item.id = old_rows.pay_batch_item_id
      WHERE COALESCE(old_rows.timesheet_id, batch_item.timesheet_id) IS NOT NULL;
    END IF;

  ELSIF TG_TABLE_NAME = 'pay_payment_correction_requests' THEN
    IF TG_OP = 'INSERT' THEN
      WITH changed_batches AS (
        SELECT new_rows.pay_batch_id
        FROM new_rows
        WHERE new_rows.pay_batch_id IS NOT NULL
      )
      SELECT COALESCE(ARRAY_AGG(DISTINCT batch_item.timesheet_id), ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM changed_batches
      JOIN public.pay_batch_candidates AS batch_candidate
        ON batch_candidate.pay_batch_id = changed_batches.pay_batch_id
      JOIN public.pay_batch_items AS batch_item
        ON batch_item.pay_batch_candidate_id = batch_candidate.id
      WHERE batch_item.timesheet_id IS NOT NULL;
    ELSIF TG_OP = 'UPDATE' THEN
      WITH changed_batches AS (
        SELECT new_rows.pay_batch_id
        FROM new_rows
        JOIN old_rows ON old_rows.id = new_rows.id
        WHERE new_rows.pay_batch_id IS DISTINCT FROM old_rows.pay_batch_id
           OR new_rows.status IS DISTINCT FROM old_rows.status
        UNION
        SELECT old_rows.pay_batch_id
        FROM old_rows
        JOIN new_rows ON new_rows.id = old_rows.id
        WHERE new_rows.pay_batch_id IS DISTINCT FROM old_rows.pay_batch_id
           OR new_rows.status IS DISTINCT FROM old_rows.status
      )
      SELECT COALESCE(ARRAY_AGG(DISTINCT batch_item.timesheet_id), ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM changed_batches
      JOIN public.pay_batch_candidates AS batch_candidate
        ON batch_candidate.pay_batch_id = changed_batches.pay_batch_id
      JOIN public.pay_batch_items AS batch_item
        ON batch_item.pay_batch_candidate_id = batch_candidate.id
      WHERE batch_item.timesheet_id IS NOT NULL;
    ELSE
      WITH changed_batches AS (
        SELECT old_rows.pay_batch_id
        FROM old_rows
        WHERE old_rows.pay_batch_id IS NOT NULL
      )
      SELECT COALESCE(ARRAY_AGG(DISTINCT batch_item.timesheet_id), ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM changed_batches
      JOIN public.pay_batch_candidates AS batch_candidate
        ON batch_candidate.pay_batch_id = changed_batches.pay_batch_id
      JOIN public.pay_batch_items AS batch_item
        ON batch_item.pay_batch_candidate_id = batch_candidate.id
      WHERE batch_item.timesheet_id IS NOT NULL;
    END IF;

  ELSIF TG_TABLE_NAME = 'pay_finance_case_components' THEN
    IF TG_OP = 'INSERT' THEN
      SELECT COALESCE(ARRAY_AGG(DISTINCT new_rows.linked_timesheet_id), ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM new_rows
      WHERE new_rows.linked_timesheet_id IS NOT NULL;
    ELSIF TG_OP = 'UPDATE' THEN
      WITH changed_components AS (
        SELECT
          new_rows.linked_timesheet_id AS new_timesheet_id,
          old_rows.linked_timesheet_id AS old_timesheet_id
        FROM new_rows
        JOIN old_rows ON old_rows.id = new_rows.id
        WHERE new_rows.linked_timesheet_id IS DISTINCT FROM old_rows.linked_timesheet_id
           OR new_rows.finance_case_id IS DISTINCT FROM old_rows.finance_case_id
           OR new_rows.component_key_type IS DISTINCT FROM old_rows.component_key_type
           OR new_rows.component_key_value IS DISTINCT FROM old_rows.component_key_value
           OR new_rows.source_basis_json IS DISTINCT FROM old_rows.source_basis_json
           OR new_rows.remaining_source_amount IS DISTINCT FROM old_rows.remaining_source_amount
           OR new_rows.closed_at_utc IS DISTINCT FROM old_rows.closed_at_utc
      )
      SELECT COALESCE(ARRAY_AGG(DISTINCT affected_rows.timesheet_id), ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM (
        SELECT changed_components.new_timesheet_id AS timesheet_id
        FROM changed_components
        WHERE changed_components.new_timesheet_id IS NOT NULL

        UNION

        SELECT changed_components.old_timesheet_id AS timesheet_id
        FROM changed_components
        WHERE changed_components.old_timesheet_id IS NOT NULL
      ) AS affected_rows;
    ELSE
      SELECT COALESCE(ARRAY_AGG(DISTINCT old_rows.linked_timesheet_id), ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM old_rows
      WHERE old_rows.linked_timesheet_id IS NOT NULL;
    END IF;

  ELSIF TG_TABLE_NAME = 'pay_advances' THEN
    WITH changed_advances AS (
      SELECT
        new_rows.id AS new_id,
        new_rows.linked_timesheet_id AS new_linked_timesheet_id,
        old_rows.id AS old_id,
        old_rows.linked_timesheet_id AS old_linked_timesheet_id
      FROM new_rows
      JOIN old_rows ON old_rows.id = new_rows.id
      WHERE new_rows.linked_timesheet_id IS DISTINCT FROM old_rows.linked_timesheet_id
         OR new_rows.case_type IS DISTINCT FROM old_rows.case_type
    ),
    advance_ids AS (
      SELECT
        changed_advances.new_id AS id,
        changed_advances.new_linked_timesheet_id AS linked_timesheet_id
      FROM changed_advances

      UNION

      SELECT
        changed_advances.old_id AS id,
        changed_advances.old_linked_timesheet_id AS linked_timesheet_id
      FROM changed_advances
    )
    SELECT COALESCE(ARRAY_AGG(DISTINCT affected_rows.timesheet_id), ARRAY[]::uuid[])
    INTO v_timesheet_ids
    FROM (
      SELECT advance_ids.linked_timesheet_id AS timesheet_id
      FROM advance_ids
      WHERE advance_ids.linked_timesheet_id IS NOT NULL

      UNION

      SELECT finance_component.linked_timesheet_id AS timesheet_id
      FROM advance_ids
      JOIN public.pay_finance_case_components AS finance_component
        ON finance_component.finance_case_id = advance_ids.id
      WHERE finance_component.linked_timesheet_id IS NOT NULL
    ) AS affected_rows;

  ELSIF TG_TABLE_NAME = 'ts_pay_adjustments' THEN
    IF TG_OP = 'INSERT' THEN
      SELECT COALESCE(ARRAY_AGG(DISTINCT new_rows.timesheet_id), ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM new_rows
      WHERE new_rows.timesheet_id IS NOT NULL;
    ELSIF TG_OP = 'UPDATE' THEN
      WITH changed_adjustments AS (
        SELECT
          new_rows.timesheet_id AS new_timesheet_id,
          old_rows.timesheet_id AS old_timesheet_id
        FROM new_rows
        JOIN old_rows ON old_rows.id = new_rows.id
        WHERE new_rows.timesheet_id IS DISTINCT FROM old_rows.timesheet_id
           OR new_rows.delta_pay_ex_vat IS DISTINCT FROM old_rows.delta_pay_ex_vat
           OR new_rows.as_advance IS DISTINCT FROM old_rows.as_advance
      )
      SELECT COALESCE(ARRAY_AGG(DISTINCT affected_rows.timesheet_id), ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM (
        SELECT changed_adjustments.new_timesheet_id AS timesheet_id
        FROM changed_adjustments
        WHERE changed_adjustments.new_timesheet_id IS NOT NULL

        UNION

        SELECT changed_adjustments.old_timesheet_id AS timesheet_id
        FROM changed_adjustments
        WHERE changed_adjustments.old_timesheet_id IS NOT NULL
      ) AS affected_rows;
    ELSE
      SELECT COALESCE(ARRAY_AGG(DISTINCT old_rows.timesheet_id), ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM old_rows
      WHERE old_rows.timesheet_id IS NOT NULL;
    END IF;
  END IF;

  SELECT COALESCE(
    ARRAY_AGG(DISTINCT input_ids.timesheet_id ORDER BY input_ids.timesheet_id),
    ARRAY[]::uuid[]
  )
  INTO v_timesheet_ids
  FROM UNNEST(COALESCE(v_timesheet_ids, ARRAY[]::uuid[])) AS input_ids(timesheet_id)
  WHERE input_ids.timesheet_id IS NOT NULL;

  v_total := COALESCE(CARDINALITY(v_timesheet_ids), 0);
  v_offset := 1;

  WHILE v_offset <= v_total LOOP
    SELECT COALESCE(
      ARRAY_AGG(chunk_rows.timesheet_id ORDER BY chunk_rows.ordinality),
      ARRAY[]::uuid[]
    )
    INTO v_chunk_ids
    FROM UNNEST(v_timesheet_ids) WITH ORDINALITY AS chunk_rows(timesheet_id, ordinality)
    WHERE chunk_rows.ordinality BETWEEN v_offset AND v_offset + 99;

    PERFORM public.pay_timesheet_summary_pay_state_refresh(
      p_timesheet_ids => v_chunk_ids,
      p_actor_user_id => NULL::uuid
    );

    v_offset := v_offset + 100;
  END LOOP;

  RETURN NULL;
END;
$function$;

-- Current financial truth / save / recompute.
DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_tsfin_ai ON public.timesheets_financials;
CREATE TRIGGER trg_ts_summary_pay_cache_tsfin_ai
AFTER INSERT ON public.timesheets_financials
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_tsfin_au ON public.timesheets_financials;
CREATE TRIGGER trg_ts_summary_pay_cache_tsfin_au
AFTER UPDATE ON public.timesheets_financials
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_tsfin_ad ON public.timesheets_financials;
CREATE TRIGGER trg_ts_summary_pay_cache_tsfin_ad
AFTER DELETE ON public.timesheets_financials
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

-- Authorise / unauthorise / version rotation / current timesheet truth.
DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_timesheets_au ON public.timesheets;
CREATE TRIGGER trg_ts_summary_pay_cache_timesheets_au
AFTER UPDATE ON public.timesheets
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

-- Advance Pay lifecycle.
DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_override_ai ON public.timesheet_payment_overrides;
CREATE TRIGGER trg_ts_summary_pay_cache_override_ai
AFTER INSERT ON public.timesheet_payment_overrides
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_override_au ON public.timesheet_payment_overrides;
CREATE TRIGGER trg_ts_summary_pay_cache_override_au
AFTER UPDATE ON public.timesheet_payment_overrides
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_override_ad ON public.timesheet_payment_overrides;
CREATE TRIGGER trg_ts_summary_pay_cache_override_ad
AFTER DELETE ON public.timesheet_payment_overrides
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

-- Settlement snapshot lifecycle. Summary-only updates are ignored by the trigger function.
DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_state_ai ON public.timesheet_pay_state;
CREATE TRIGGER trg_ts_summary_pay_cache_state_ai
AFTER INSERT ON public.timesheet_pay_state
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_state_au ON public.timesheet_pay_state;
CREATE TRIGGER trg_ts_summary_pay_cache_state_au
AFTER UPDATE ON public.timesheet_pay_state
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_state_ad ON public.timesheet_pay_state;
CREATE TRIGGER trg_ts_summary_pay_cache_state_ad
AFTER DELETE ON public.timesheet_pay_state
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

-- Reservation release / item void / item deletion. Draft activation is refreshed explicitly by the finaliser RPC.
DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_items_au ON public.pay_batch_items;
CREATE TRIGGER trg_ts_summary_pay_cache_items_au
AFTER UPDATE ON public.pay_batch_items
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_items_ad ON public.pay_batch_items;
CREATE TRIGGER trg_ts_summary_pay_cache_items_ad
AFTER DELETE ON public.pay_batch_items
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

-- Partial/final candidate and rail state transitions.
DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_candidates_au ON public.pay_batch_candidates;
CREATE TRIGGER trg_ts_summary_pay_cache_candidates_au
AFTER UPDATE ON public.pay_batch_candidates
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_batches_au ON public.pay_batches;
CREATE TRIGGER trg_ts_summary_pay_cache_batches_au
AFTER UPDATE ON public.pay_batches
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_transfers_au ON public.pay_bank_transfers;
CREATE TRIGGER trg_ts_summary_pay_cache_transfers_au
AFTER UPDATE ON public.pay_bank_transfers
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

-- Direct history evidence used by the authoritative settled baseline.
DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_history_ai ON public.timesheet_pay_state_history;
CREATE TRIGGER trg_ts_summary_pay_cache_history_ai
AFTER INSERT ON public.timesheet_pay_state_history
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_history_au ON public.timesheet_pay_state_history;
CREATE TRIGGER trg_ts_summary_pay_cache_history_au
AFTER UPDATE ON public.timesheet_pay_state_history
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_history_ad ON public.timesheet_pay_state_history;
CREATE TRIGGER trg_ts_summary_pay_cache_history_ad
AFTER DELETE ON public.timesheet_pay_state_history
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

-- Remove earlier draft trigger names if an interrupted deployment created them.
DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_transfer_events_ai ON public.pay_bank_transfer_events;
DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_transfer_events_au ON public.pay_bank_transfer_events;
DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_transfer_events_ad ON public.pay_bank_transfer_events;

-- Correction / unwind / recovery truth.
DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_correction_requests_ai ON public.pay_payment_correction_requests;
CREATE TRIGGER trg_ts_summary_pay_cache_correction_requests_ai
AFTER INSERT ON public.pay_payment_correction_requests
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_correction_requests_au ON public.pay_payment_correction_requests;
CREATE TRIGGER trg_ts_summary_pay_cache_correction_requests_au
AFTER UPDATE ON public.pay_payment_correction_requests
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_correction_requests_ad ON public.pay_payment_correction_requests;
CREATE TRIGGER trg_ts_summary_pay_cache_correction_requests_ad
AFTER DELETE ON public.pay_payment_correction_requests
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_correction_items_ai ON public.pay_payment_correction_items;
CREATE TRIGGER trg_ts_summary_pay_cache_correction_items_ai
AFTER INSERT ON public.pay_payment_correction_items
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_correction_items_au ON public.pay_payment_correction_items;
CREATE TRIGGER trg_ts_summary_pay_cache_correction_items_au
AFTER UPDATE ON public.pay_payment_correction_items
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_correction_items_ad ON public.pay_payment_correction_items;
CREATE TRIGGER trg_ts_summary_pay_cache_correction_items_ad
AFTER DELETE ON public.pay_payment_correction_items
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_finance_components_ai ON public.pay_finance_case_components;
CREATE TRIGGER trg_ts_summary_pay_cache_finance_components_ai
AFTER INSERT ON public.pay_finance_case_components
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_finance_components_au ON public.pay_finance_case_components;
CREATE TRIGGER trg_ts_summary_pay_cache_finance_components_au
AFTER UPDATE ON public.pay_finance_case_components
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_finance_components_ad ON public.pay_finance_case_components;
CREATE TRIGGER trg_ts_summary_pay_cache_finance_components_ad
AFTER DELETE ON public.pay_finance_case_components
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

-- pay_advances.case_type participates in under/overpayment component truth.
-- Finance component insert/delete triggers cover creation/removal of linked component rows.
DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_advances_ai ON public.pay_advances;
DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_advances_ad ON public.pay_advances;
DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_advances_au ON public.pay_advances;
CREATE TRIGGER trg_ts_summary_pay_cache_advances_au
AFTER UPDATE ON public.pay_advances
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_adjustments_ai ON public.ts_pay_adjustments;
CREATE TRIGGER trg_ts_summary_pay_cache_adjustments_ai
AFTER INSERT ON public.ts_pay_adjustments
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_adjustments_au ON public.ts_pay_adjustments;
CREATE TRIGGER trg_ts_summary_pay_cache_adjustments_au
AFTER UPDATE ON public.ts_pay_adjustments
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

DROP TRIGGER IF EXISTS trg_ts_summary_pay_cache_adjustments_ad ON public.ts_pay_adjustments;
CREATE TRIGGER trg_ts_summary_pay_cache_adjustments_ad
AFTER DELETE ON public.ts_pay_adjustments
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger();

-- ============================================================================
-- 004_pay_batch_finalize_reservations_and_markers.sql
-- ============================================================================
CREATE OR REPLACE FUNCTION public.pay_batch_finalize_reservations_and_markers(p_pay_batch_id uuid, p_pay_channel_scope text, p_actor_user_id uuid DEFAULT NULL::uuid, p_pay_date date DEFAULT NULL::date, p_week_start date DEFAULT NULL::date, p_operation_id uuid DEFAULT NULL::uuid, p_candidate_scope_ids jsonb DEFAULT NULL::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_scope text := UPPER(BTRIM(COALESCE(p_pay_channel_scope, '')));
  v_scope_ids jsonb := COALESCE(p_candidate_scope_ids, '[]'::jsonb);
  v_scope_id_count integer := 0;
  v_week_start date := p_week_start;
  v_candidate_rows_before_empty_delete integer := 0;
  v_candidate_rows_after_empty_delete integer := 0;
  v_deleted_candidate_rows integer := 0;
  v_awaiting_net_rows_updated integer := 0;
  v_reservations_created integer := 0;
  v_reservations_reused integer := 0;
  v_invalid_item_sample jsonb := '[]'::jsonb;
  v_overrun_sample jsonb := '[]'::jsonb;
  v_reservation_check_component_count integer := 0;
  v_reservation_requested_amount_ex_vat numeric := 0;
  v_reservation_outstanding_before_batch_ex_vat numeric := 0;
  v_summary_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_summary_chunk_ids uuid[] := ARRAY[]::uuid[];
  v_summary_offset integer := 1;
  v_summary_total integer := 0;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'pay_batch_id is required';
  END IF;

  IF v_scope NOT IN ('PAYE', 'UMBRELLA') THEN
    RAISE EXCEPTION 'pay_channel_scope must be PAYE or UMBRELLA';
  END IF;

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'p_operation_id is required for row-backed draft finalisation';
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'p_actor_user_id is required for row-backed draft finalisation';
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

  IF v_week_start IS NULL THEN
    v_week_start := public._pay_week_start_monday(COALESCE(p_pay_date, (SELECT batch_row.pay_date FROM public.pay_batches AS batch_row WHERE batch_row.id = p_pay_batch_id)));
  END IF;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_batch_finalize_scope;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_batch_finalize_scope ON COMMIT DROP AS
  SELECT scope_row.*
  FROM public.banking_pay_operation_candidate_scope AS scope_row
  WHERE scope_row.operation_id = p_operation_id
    AND scope_row.pay_batch_id = p_pay_batch_id
    AND scope_row.pay_channel = v_scope
    AND scope_row.id IN (
      SELECT (supplied_scope.scope_value #>> '{}')::uuid
      FROM jsonb_array_elements(v_scope_ids) AS supplied_scope(scope_value)
    )
  ORDER BY scope_row.chunk_sequence NULLS LAST, scope_row.candidate_id, scope_row.id;

  IF (SELECT COUNT(*)::integer FROM pg_temp.tmp_pay_batch_finalize_scope) <> v_scope_id_count THEN
    RAISE EXCEPTION 'one or more candidate scope ids do not belong to operation %, batch %, and scope %', p_operation_id, p_pay_batch_id, v_scope;
  END IF;

  SELECT COUNT(*)::integer
  INTO v_candidate_rows_before_empty_delete
  FROM public.pay_batch_candidates AS candidate_before
  WHERE candidate_before.pay_batch_id = p_pay_batch_id
    AND EXISTS (
      SELECT 1
      FROM pg_temp.tmp_pay_batch_finalize_scope AS scope_row
      WHERE scope_row.candidate_id = candidate_before.candidate_id
    );

  DELETE FROM public.pay_batch_candidates AS candidate_delete
  WHERE candidate_delete.pay_batch_id = p_pay_batch_id
    AND EXISTS (
      SELECT 1
      FROM pg_temp.tmp_pay_batch_finalize_scope AS scope_row
      WHERE scope_row.candidate_id = candidate_delete.candidate_id
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_batch_items AS item_check
      WHERE item_check.pay_batch_candidate_id = candidate_delete.id
        AND COALESCE(item_check.is_voided, false) = false
        AND item_check.item_type <> 'DEBT_CREATED'
    );
  GET DIAGNOSTICS v_deleted_candidate_rows = ROW_COUNT;

  SELECT COUNT(*)::integer
  INTO v_candidate_rows_after_empty_delete
  FROM public.pay_batch_candidates AS candidate_after
  WHERE candidate_after.pay_batch_id = p_pay_batch_id
    AND EXISTS (
      SELECT 1
      FROM pg_temp.tmp_pay_batch_finalize_scope AS scope_row
      WHERE scope_row.candidate_id = candidate_after.candidate_id
    );

  IF COALESCE(v_candidate_rows_after_empty_delete, 0) = 0 THEN
    RETURN jsonb_build_object(
      'ok', true,
      'pay_batch_id', p_pay_batch_id::text,
      'operation_id', p_operation_id::text,
      'nothing_payable_after_reservation_exclusion', true,
      'message', 'Nothing to pay for this bounded scope after blockers and reservation availability checks.',
      'candidate_rows_before_empty_delete', COALESCE(v_candidate_rows_before_empty_delete, 0),
      'deleted_candidate_rows', COALESCE(v_deleted_candidate_rows, 0),
      'candidate_rows_after_empty_delete', COALESCE(v_candidate_rows_after_empty_delete, 0)
    );
  END IF;

  WITH scoped_items AS (
    SELECT pay_batch_item.id AS pay_batch_item_id,
           pay_batch_item.timesheet_id,
           pay_batch_item.item_type,
           economic_component.key_type,
           economic_component.key_value,
           economic_component.source_amount_ex_vat,
           economic_component.key_resolution_failure_reason
    FROM public.pay_batch_items AS pay_batch_item
    JOIN public.pay_batch_candidates AS pay_batch_candidate
      ON pay_batch_candidate.id = pay_batch_item.pay_batch_candidate_id
    JOIN public.banking_pay_operation_candidate_allocation_rows AS allocation_row
      ON allocation_row.pay_batch_item_id = pay_batch_item.id
     AND allocation_row.operation_id = p_operation_id
    JOIN LATERAL public._pay_batch_item_economic_components(NULL::uuid, ARRAY[pay_batch_item.id]::uuid[]) AS economic_component
      ON true
    WHERE pay_batch_candidate.pay_batch_id = p_pay_batch_id
      AND allocation_row.candidate_scope_id IN (SELECT finalize_scope.id FROM pg_temp.tmp_pay_batch_finalize_scope AS finalize_scope)
      AND pay_batch_item.timesheet_id IS NOT NULL
      AND COALESCE(pay_batch_item.is_voided, false) = false
      AND pay_batch_item.item_type IN ('SEGMENT_DELTA', 'EXPENSE_DELTA', 'ADJUSTMENT_DELTA', 'MILEAGE_DELTA')
  ), invalid_items AS (
    SELECT scoped_items.*,
           CASE
             WHEN scoped_items.key_resolution_failure_reason IS NOT NULL THEN scoped_items.key_resolution_failure_reason
             WHEN scoped_items.key_type IS NULL OR BTRIM(COALESCE(scoped_items.key_type, '')) = '' THEN 'MISSING_ECONOMIC_KEY_TYPE'
             WHEN scoped_items.key_value IS NULL OR BTRIM(COALESCE(scoped_items.key_value, '')) = '' THEN 'MISSING_ECONOMIC_KEY_VALUE'
             WHEN scoped_items.key_type = 'TS_DAY' AND scoped_items.key_value !~ '^\d{4}-\d{2}-\d{2}$' THEN 'INVALID_TS_DAY_KEY_VALUE'
             WHEN scoped_items.source_amount_ex_vat IS NULL THEN 'MISSING_SOURCE_RESERVATION_AMOUNT'
             ELSE NULL::text
           END AS failure_reason
    FROM scoped_items
  )
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
           'pay_batch_item_id', invalid_items.pay_batch_item_id::text,
           'timesheet_id', invalid_items.timesheet_id::text,
           'item_type', invalid_items.item_type,
           'key_type', invalid_items.key_type,
           'key_value', invalid_items.key_value,
           'source_amount_ex_vat', invalid_items.source_amount_ex_vat,
           'failure_reason', invalid_items.failure_reason
         ) ORDER BY invalid_items.pay_batch_item_id::text), '[]'::jsonb)
  INTO v_invalid_item_sample
  FROM invalid_items
  WHERE invalid_items.failure_reason IS NOT NULL
  LIMIT 25;

  IF jsonb_array_length(COALESCE(v_invalid_item_sample, '[]'::jsonb)) > 0 THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_BATCH_ITEM_ECONOMIC_KEY_OR_SOURCE_RESOLUTION_FAILED',
      'pay_batch_id', p_pay_batch_id::text,
      'items', v_invalid_item_sample
    )::text;
  END IF;

  WITH scoped_components AS (
    SELECT economic_component.timesheet_id,
           economic_component.key_type,
           economic_component.key_value,
           SUM(ROUND(COALESCE(economic_component.source_amount_ex_vat, 0), 2)) AS requested_source_amount_ex_vat
    FROM public.pay_batch_items AS pay_batch_item
    JOIN public.pay_batch_candidates AS pay_batch_candidate
      ON pay_batch_candidate.id = pay_batch_item.pay_batch_candidate_id
    JOIN public.banking_pay_operation_candidate_allocation_rows AS allocation_row
      ON allocation_row.pay_batch_item_id = pay_batch_item.id
     AND allocation_row.operation_id = p_operation_id
    JOIN LATERAL public._pay_batch_item_economic_components(NULL::uuid, ARRAY[pay_batch_item.id]::uuid[]) AS economic_component
      ON true
    WHERE pay_batch_candidate.pay_batch_id = p_pay_batch_id
      AND allocation_row.candidate_scope_id IN (SELECT finalize_scope.id FROM pg_temp.tmp_pay_batch_finalize_scope AS finalize_scope)
      AND economic_component.timesheet_id IS NOT NULL
      AND economic_component.key_type IN ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE')
      AND economic_component.key_value IS NOT NULL
      AND NOT (economic_component.key_type = 'TS_DAY' AND economic_component.key_value !~ '^\d{4}-\d{2}-\d{2}$')
      AND economic_component.source_amount_ex_vat IS NOT NULL
    GROUP BY economic_component.timesheet_id, economic_component.key_type, economic_component.key_value
  ), timesheet_ids AS (
    SELECT COALESCE(array_agg(DISTINCT scoped_component_rows.timesheet_id), ARRAY[]::uuid[]) AS timesheet_id_array
    FROM scoped_components AS scoped_component_rows
  ), outstanding_components AS (
    SELECT outstanding_component.timesheet_id,
           UPPER(BTRIM(COALESCE(outstanding_component.key_type, ''))) AS key_type,
           BTRIM(COALESCE(outstanding_component.key_value, '')) AS key_value,
           ROUND(COALESCE(outstanding_component.outstanding_ex_vat, 0), 2) AS outstanding_ex_vat
    FROM public._pay_outstanding_components(
      p_timesheet_ids => (SELECT timesheet_ids.timesheet_id_array FROM timesheet_ids),
      p_exclude_pay_batch_id => p_pay_batch_id
    ) AS outstanding_component
  ), joined_components AS (
    SELECT scoped_component_rows.timesheet_id,
           scoped_component_rows.key_type,
           scoped_component_rows.key_value,
           ROUND(scoped_component_rows.requested_source_amount_ex_vat, 2) AS requested_source_amount_ex_vat,
           ROUND(COALESCE(outstanding_component_rows.outstanding_ex_vat, 0), 2) AS outstanding_ex_vat
    FROM scoped_components AS scoped_component_rows
    LEFT JOIN outstanding_components AS outstanding_component_rows
      ON outstanding_component_rows.timesheet_id = scoped_component_rows.timesheet_id
     AND outstanding_component_rows.key_type = scoped_component_rows.key_type
     AND outstanding_component_rows.key_value = scoped_component_rows.key_value
  ), overruns AS (
    SELECT joined_component_rows.timesheet_id,
           joined_component_rows.key_type,
           joined_component_rows.key_value,
           joined_component_rows.requested_source_amount_ex_vat,
           joined_component_rows.outstanding_ex_vat
    FROM joined_components AS joined_component_rows
    WHERE joined_component_rows.requested_source_amount_ex_vat > joined_component_rows.outstanding_ex_vat + 0.01
  )
  SELECT COALESCE(
           (
             SELECT jsonb_agg(
                      jsonb_build_object(
                        'timesheet_id', overrun_sample_rows.timesheet_id::text,
                        'key_type', overrun_sample_rows.key_type,
                        'key_value', overrun_sample_rows.key_value,
                        'requested_source_amount_ex_vat', overrun_sample_rows.requested_source_amount_ex_vat,
                        'outstanding_ex_vat', overrun_sample_rows.outstanding_ex_vat
                      )
                      ORDER BY overrun_sample_rows.timesheet_id::text,
                               overrun_sample_rows.key_type,
                               overrun_sample_rows.key_value
                    )
             FROM (
               SELECT overrun_rows.*
               FROM overruns AS overrun_rows
               ORDER BY overrun_rows.timesheet_id::text, overrun_rows.key_type, overrun_rows.key_value
               LIMIT 25
             ) AS overrun_sample_rows
           ),
           '[]'::jsonb
         ),
         COALESCE((SELECT COUNT(*)::integer FROM scoped_components AS scoped_count_rows), 0),
         ROUND(COALESCE((SELECT SUM(scoped_sum_rows.requested_source_amount_ex_vat) FROM scoped_components AS scoped_sum_rows), 0), 2),
         ROUND(COALESCE((SELECT SUM(joined_sum_rows.outstanding_ex_vat) FROM joined_components AS joined_sum_rows), 0), 2)
  INTO v_overrun_sample,
       v_reservation_check_component_count,
       v_reservation_requested_amount_ex_vat,
       v_reservation_outstanding_before_batch_ex_vat;

  IF jsonb_array_length(COALESCE(v_overrun_sample, '[]'::jsonb)) > 0 THEN
    RAISE EXCEPTION '%', (
      jsonb_build_object(
        'error', 'PAY_BATCH_RESERVATION_OVERRUN',
        'pay_batch_id', p_pay_batch_id::text,
        'overruns', v_overrun_sample
      )
      || jsonb_build_object(
        'reservation_check_excluded_pay_batch_id', p_pay_batch_id::text,
        'reservation_component_count', COALESCE(v_reservation_check_component_count, 0),
        'reservation_requested_amount_ex_vat', COALESCE(v_reservation_requested_amount_ex_vat, 0),
        'reservation_outstanding_before_batch_ex_vat', COALESCE(v_reservation_outstanding_before_batch_ex_vat, 0)
      )
    )::text;
  END IF;

  UPDATE public.pay_batch_candidates AS pay_batch_candidate
  SET awaiting_net_amount = (
        v_scope = 'PAYE'
        AND NOT EXISTS (
          SELECT 1
          FROM public.pay_batch_paye_net_inputs AS net_input
          WHERE net_input.pay_batch_candidate_id = pay_batch_candidate.id
        )
      ),
      updated_at = v_now
  WHERE pay_batch_candidate.pay_batch_id = p_pay_batch_id
    AND EXISTS (
      SELECT 1
      FROM pg_temp.tmp_pay_batch_finalize_scope AS scope_row
      WHERE scope_row.candidate_id = pay_batch_candidate.candidate_id
    );
  GET DIAGNOSTICS v_awaiting_net_rows_updated = ROW_COUNT;

  WITH scoped_finance_items AS (
    SELECT pay_batch_item.id AS pay_batch_item_id,
           pay_batch_item.reservation_id AS current_reservation_id,
           pay_batch_item.finance_case_id,
           pay_batch_item.finance_component_id,
           pay_batch_item.pay_batch_candidate_id,
           pay_batch_item.repayment_week_start,
           pay_batch_item.frozen_component_snapshot_json,
           pay_batch_item.frozen_component_key_type,
           pay_batch_item.frozen_component_key_value,
           pay_batch_item.frozen_component_classification,
           pay_batch_item.frozen_source_basis_json,
           pay_batch_item.frozen_source_pay_method,
           pay_batch_item.frozen_target_pay_method,
           pay_batch_item.frozen_resolution_mode,
           pay_batch_item.frozen_resolution_payload_json,
           pay_batch_item.frozen_resolution_result_json,
           ROUND(ABS(COALESCE(public._pay_batch_item_source_reservation_amount_ex_vat(pay_batch_item.id), pay_batch_item.frozen_source_amount, pay_batch_item.amount_ex_vat, 0)), 2) AS reserved_source_amount,
           ROUND(ABS(COALESCE(pay_batch_item.frozen_target_amount_ex_vat, pay_batch_item.amount_ex_vat, 0)), 2) AS frozen_rounded_target_amount,
           (
             substr(md5(p_pay_batch_id::text || ':' || pay_batch_item.id::text || ':' || COALESCE(pay_batch_item.finance_case_id::text, 'no_case') || ':' || COALESCE(pay_batch_item.finance_component_id::text, 'no_component')), 1, 8) || '-' ||
             substr(md5(p_pay_batch_id::text || ':' || pay_batch_item.id::text || ':' || COALESCE(pay_batch_item.finance_case_id::text, 'no_case') || ':' || COALESCE(pay_batch_item.finance_component_id::text, 'no_component')), 9, 4) || '-' ||
             substr(md5(p_pay_batch_id::text || ':' || pay_batch_item.id::text || ':' || COALESCE(pay_batch_item.finance_case_id::text, 'no_case') || ':' || COALESCE(pay_batch_item.finance_component_id::text, 'no_component')), 13, 4) || '-' ||
             substr(md5(p_pay_batch_id::text || ':' || pay_batch_item.id::text || ':' || COALESCE(pay_batch_item.finance_case_id::text, 'no_case') || ':' || COALESCE(pay_batch_item.finance_component_id::text, 'no_component')), 17, 4) || '-' ||
             substr(md5(p_pay_batch_id::text || ':' || pay_batch_item.id::text || ':' || COALESCE(pay_batch_item.finance_case_id::text, 'no_case') || ':' || COALESCE(pay_batch_item.finance_component_id::text, 'no_component')), 21, 12)
           )::uuid AS deterministic_reservation_id
    FROM public.pay_batch_items AS pay_batch_item
    JOIN public.pay_batch_candidates AS pay_batch_candidate
      ON pay_batch_candidate.id = pay_batch_item.pay_batch_candidate_id
    JOIN public.banking_pay_operation_candidate_allocation_rows AS allocation_row
      ON allocation_row.pay_batch_item_id = pay_batch_item.id
     AND allocation_row.operation_id = p_operation_id
    WHERE pay_batch_candidate.pay_batch_id = p_pay_batch_id
      AND allocation_row.candidate_scope_id IN (SELECT scope_row.id FROM pg_temp.tmp_pay_batch_finalize_scope AS scope_row)
      AND COALESCE(pay_batch_item.is_voided, false) = false
      AND pay_batch_item.finance_case_id IS NOT NULL
      AND pay_batch_item.reservation_id IS NULL
    ORDER BY allocation_row.candidate_scope_id, allocation_row.sort_order, pay_batch_item.id
    LIMIT 100
  ), inserted_reservations AS (
    INSERT INTO public.pay_advance_reservations (
      id,
      finance_case_id,
      finance_component_id,
      pay_batch_id,
      pay_batch_candidate_id,
      pay_batch_item_id,
      reserved_amount,
      repayment_week_start,
      status,
      created_at_utc,
      committed_at_utc,
      settled_at_utc,
      released_at_utc,
      released_reason,
      created_by_user_id,
      updated_by_user_id,
      frozen_component_snapshot_json,
      frozen_component_key_type,
      frozen_component_key_value,
      frozen_component_classification,
      frozen_source_basis_json,
      frozen_source_pay_method,
      frozen_target_pay_method,
      frozen_resolution_mode,
      frozen_resolution_payload_json,
      frozen_resolution_result_json,
      reserved_source_amount,
      frozen_rounded_target_amount
    )
    SELECT
      scoped_finance_items.deterministic_reservation_id,
      scoped_finance_items.finance_case_id,
      scoped_finance_items.finance_component_id,
      p_pay_batch_id,
      scoped_finance_items.pay_batch_candidate_id,
      scoped_finance_items.pay_batch_item_id,
      scoped_finance_items.frozen_rounded_target_amount,
      scoped_finance_items.repayment_week_start,
      'RESERVED',
      v_now,
      NULL::timestamptz,
      NULL::timestamptz,
      NULL::timestamptz,
      NULL::text,
      p_actor_user_id,
      p_actor_user_id,
      scoped_finance_items.frozen_component_snapshot_json,
      scoped_finance_items.frozen_component_key_type,
      scoped_finance_items.frozen_component_key_value,
      scoped_finance_items.frozen_component_classification,
      scoped_finance_items.frozen_source_basis_json,
      scoped_finance_items.frozen_source_pay_method,
      scoped_finance_items.frozen_target_pay_method,
      scoped_finance_items.frozen_resolution_mode,
      scoped_finance_items.frozen_resolution_payload_json,
      scoped_finance_items.frozen_resolution_result_json,
      scoped_finance_items.reserved_source_amount,
      scoped_finance_items.frozen_rounded_target_amount
    FROM scoped_finance_items
    ON CONFLICT (id) DO UPDATE
    SET pay_batch_id = EXCLUDED.pay_batch_id,
        pay_batch_candidate_id = EXCLUDED.pay_batch_candidate_id,
        pay_batch_item_id = EXCLUDED.pay_batch_item_id,
        updated_by_user_id = EXCLUDED.updated_by_user_id
    RETURNING public.pay_advance_reservations.id, public.pay_advance_reservations.pay_batch_item_id, xmax = 0 AS inserted_flag
  ), linked_items AS (
    UPDATE public.pay_batch_items AS item_update
    SET reservation_id = inserted_reservations.id,
        updated_at = v_now
    FROM inserted_reservations
    WHERE item_update.id = inserted_reservations.pay_batch_item_id
    RETURNING inserted_reservations.inserted_flag
  )
  SELECT COUNT(*) FILTER (WHERE linked_items.inserted_flag)::integer,
         COUNT(*) FILTER (WHERE NOT linked_items.inserted_flag)::integer
  INTO v_reservations_created, v_reservations_reused
  FROM linked_items;

  UPDATE public.banking_pay_operation_candidate_scope AS scope_update
  SET status = CASE
        WHEN NOT EXISTS (
          SELECT 1
          FROM public.banking_pay_operation_candidate_allocation_rows AS allocation_remaining
          JOIN public.pay_batch_items AS item_remaining
            ON item_remaining.id = allocation_remaining.pay_batch_item_id
          WHERE allocation_remaining.candidate_scope_id = scope_update.id
            AND COALESCE(item_remaining.is_voided, false) = false
            AND item_remaining.finance_case_id IS NOT NULL
            AND item_remaining.reservation_id IS NULL
        ) THEN 'DRAFTED'
        ELSE scope_update.status
      END,
      updated_at_utc = v_now
  WHERE scope_update.operation_id = p_operation_id
    AND EXISTS (
      SELECT 1
      FROM pg_temp.tmp_pay_batch_finalize_scope AS scope_row
      WHERE scope_row.id = scope_update.id
    );

  SELECT COALESCE(
    ARRAY_AGG(DISTINCT batch_item.timesheet_id ORDER BY batch_item.timesheet_id),
    ARRAY[]::uuid[]
  )
  INTO v_summary_timesheet_ids
  FROM public.pay_batch_items AS batch_item
  JOIN public.pay_batch_candidates AS batch_candidate
    ON batch_candidate.id = batch_item.pay_batch_candidate_id
  JOIN public.banking_pay_operation_candidate_allocation_rows AS allocation_row
    ON allocation_row.pay_batch_item_id = batch_item.id
   AND allocation_row.operation_id = p_operation_id
  WHERE batch_candidate.pay_batch_id = p_pay_batch_id
    AND allocation_row.candidate_scope_id IN (
      SELECT scope_row.id
      FROM pg_temp.tmp_pay_batch_finalize_scope AS scope_row
    )
    AND batch_item.timesheet_id IS NOT NULL
    AND COALESCE(batch_item.is_voided, false) = false
    AND UPPER(BTRIM(COALESCE(batch_item.item_type, ''))) IN (
      'SEGMENT_DELTA',
      'EXPENSE_DELTA',
      'ADJUSTMENT_DELTA',
      'MILEAGE_DELTA'
    );

  v_summary_total := COALESCE(CARDINALITY(v_summary_timesheet_ids), 0);
  v_summary_offset := 1;

  WHILE v_summary_offset <= v_summary_total LOOP
    SELECT COALESCE(
      ARRAY_AGG(chunk_rows.timesheet_id ORDER BY chunk_rows.ordinality),
      ARRAY[]::uuid[]
    )
    INTO v_summary_chunk_ids
    FROM UNNEST(v_summary_timesheet_ids) WITH ORDINALITY AS chunk_rows(timesheet_id, ordinality)
    WHERE chunk_rows.ordinality BETWEEN v_summary_offset AND v_summary_offset + 99;

    PERFORM public.pay_timesheet_summary_pay_state_refresh(
      p_timesheet_ids => v_summary_chunk_ids,
      p_actor_user_id => p_actor_user_id
    );

    v_summary_offset := v_summary_offset + 100;
  END LOOP;

  PERFORM public.pay_batch_display_summary_touch(p_pay_batch_id);

  PERFORM public.banking_pay_batch_signal_touch(
    p_pay_batch_id => p_pay_batch_id,
    p_change_reason => 'DRAFT_ARTIFACTS_FINALISED',
    p_change_source => 'pay_batch_finalize_reservations_and_markers',
    p_change_scope_json => jsonb_build_object(
      'operation_id', p_operation_id::text,
      'candidate_scope_count', v_scope_id_count,
      'pay_channel_scope', v_scope
    ) || jsonb_build_object(
      'reservation_check_excluded_pay_batch_id', p_pay_batch_id::text,
      'reservation_component_count', COALESCE(v_reservation_check_component_count, 0),
      'reservation_requested_amount_ex_vat', COALESCE(v_reservation_requested_amount_ex_vat, 0),
      'reservation_outstanding_before_batch_ex_vat', COALESCE(v_reservation_outstanding_before_batch_ex_vat, 0)
    ),
    p_touch_payment_status => false,
    p_touch_correction_progress => false,
    p_touch_alerts => false,
    p_touch_overview => true
  );

  RETURN jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'operation_id', p_operation_id::text,
    'candidate_scope_count', v_scope_id_count,
    'reservations_created', COALESCE(v_reservations_created, 0),
    'reservations_reused', COALESCE(v_reservations_reused, 0),
    'markers_updated', COALESCE(v_awaiting_net_rows_updated, 0),
    'failed_count', 0,
    'candidate_rows_before_empty_delete', COALESCE(v_candidate_rows_before_empty_delete, 0),
    'deleted_candidate_rows', COALESCE(v_deleted_candidate_rows, 0),
    'candidate_rows_after_empty_delete', COALESCE(v_candidate_rows_after_empty_delete, 0),
    'awaiting_net_rows_updated', COALESCE(v_awaiting_net_rows_updated, 0),
    'reservation_check_excluded_pay_batch_id', p_pay_batch_id::text,
    'reservation_component_count', COALESCE(v_reservation_check_component_count, 0),
    'reservation_requested_amount_ex_vat', COALESCE(v_reservation_requested_amount_ex_vat, 0),
    'reservation_outstanding_before_batch_ex_vat', COALESCE(v_reservation_outstanding_before_batch_ex_vat, 0),
    'has_more', EXISTS (
      SELECT 1
      FROM public.pay_batch_items AS remaining_item
      JOIN public.pay_batch_candidates AS remaining_candidate
        ON remaining_candidate.id = remaining_item.pay_batch_candidate_id
      JOIN public.banking_pay_operation_candidate_allocation_rows AS remaining_allocation
        ON remaining_allocation.pay_batch_item_id = remaining_item.id
       AND remaining_allocation.operation_id = p_operation_id
      WHERE remaining_candidate.pay_batch_id = p_pay_batch_id
        AND remaining_allocation.candidate_scope_id IN (SELECT scope_row.id FROM pg_temp.tmp_pay_batch_finalize_scope AS scope_row)
        AND COALESCE(remaining_item.is_voided, false) = false
        AND remaining_item.finance_case_id IS NOT NULL
        AND remaining_item.reservation_id IS NULL
    )
  );
END;
$function$;

-- ============================================================================
-- 005_bulk_timesheet_workbench_row_source_v1.sql
-- ============================================================================
CREATE OR REPLACE FUNCTION public.bulk_timesheet_workbench_row_source_v1(p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS TABLE(timesheet_id uuid, timesheet_status timesheet_status_enum, week_ending_date date, booking_id text, occupant_key_norm text, hospital_norm text, sheet_scope timesheet_scope_enum, submission_mode submission_mode_enum, authorised_at_server timestamp with time zone, candidate_id uuid, client_id uuid, pay_method text, processing_status ts_fin_processing_status_enum, basis timesheet_fin_basis_enum, total_hours numeric, total_pay_ex_vat numeric, total_charge_ex_vat numeric, margin_ex_vat numeric, paid_at_utc timestamp with time zone, pay_on_hold boolean, ready_to_pay boolean, locked_by_invoice_id uuid, candidate_name text, client_name text, nhsp_shift_count integer, nhsp_shift_included_count integer, nhsp_shift_deferred_count integer, validation_status validation_status_enum, summary_stage text, route_type text, contract_week_id uuid, contract_week_ending_date date, contract_week_status contract_week_status_enum, additional_seq integer, is_adjustment boolean, qr_status timesheet_qr_status_enum, pay_adjustment_count integer, has_pay_adjustments boolean, is_adjusted boolean, is_qr boolean, needs_attention boolean, client_autoprocess_hr boolean, has_rate_issue boolean, has_pay_channel_issue boolean, hr_crosscheck_status text, hr_crosscheck_issues text[], external_source_rows_json jsonb, issue_codes text[], client_requires_hr boolean, client_no_timesheet_required boolean, client_is_nhsp boolean, client_pay_reference_required boolean, client_invoice_reference_required boolean, client_hr_validation_required boolean, client_ts_reference_required boolean, require_reference_to_pay boolean, require_reference_to_invoice boolean, qr_token text, qr_generated_at timestamp with time zone, qr_scanned_at timestamp with time zone, candidate_hint_text jsonb, expenses_pay_ex_vat numeric, expenses_description text, mileage_units numeric, mileage_pay_rate numeric, mileage_charge_rate numeric, mileage_pay_ex_vat numeric, travel_pay_ex_vat numeric, travel_charge_ex_vat numeric, accommodation_pay_ex_vat numeric, accommodation_charge_ex_vat numeric, other_pay_ex_vat numeric, other_charge_ex_vat numeric, hr_validation_required_for_invoice boolean, invoice_segments_total integer, invoice_segments_locked integer, invoice_segments_unlocked integer, invoice_segment_stage text, tools_stage text, processing_status_display text, invoice_is_paid boolean, refs_block_invoicing boolean, refs_block_issuing_invoices boolean, refs_block_invoice_and_issuing boolean, pay_icon_code text, pay_status_code text, pay_paid_at_utc timestamp with time zone, net_delta_ex_vat numeric)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_candidate_id_text text := NULL;
  v_client_id_text text := NULL;
  v_candidate_id uuid := NULL;
  v_client_id uuid := NULL;
  v_timesheet_ids uuid[] := NULL;
  v_contract_week_ids uuid[] := NULL;
  v_row_keys text[] := NULL;
  v_row_key_timesheet_ids uuid[] := NULL;
  v_row_key_contract_week_ids uuid[] := NULL;
  v_has_contract_week_row_key boolean := FALSE;
  v_dataset_mode text := NULL;
  v_period_filter text := NULL;
  v_date_from_text text := NULL;
  v_date_to_text text := NULL;
  v_week_ending_text text := NULL;
  v_week_ending_from_text text := NULL;
  v_week_ending_to_text text := NULL;
  v_date_from date := NULL;
  v_date_to date := NULL;
  v_week_ending_date date := NULL;
  v_week_ending_from date := NULL;
  v_week_ending_to date := NULL;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';
  v_numeric_re text := E'^[-+]?[0-9]+(\\.[0-9]+)?$';
BEGIN
  v_candidate_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), '');
  BEGIN
    IF v_candidate_id_text IS NOT NULL THEN
      v_candidate_id := v_candidate_id_text::uuid;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_candidate_id := NULL;
  END;

  v_client_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), '');
  BEGIN
    IF v_client_id_text IS NOT NULL THEN
      v_client_id := v_client_id_text::uuid;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_client_id := NULL;
  END;

  IF v_filters ? 'timesheet_ids' AND jsonb_typeof(v_filters->'timesheet_ids') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_timesheet_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'timesheet_ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'timesheetIds' AND jsonb_typeof(v_filters->'timesheetIds') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_timesheet_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'timesheetIds') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'timesheet_id' AND NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_id', '')), '') IS NOT NULL AND (v_filters->>'timesheet_id') ~* v_uuid_re THEN
    v_timesheet_ids := ARRAY[(v_filters->>'timesheet_id')::uuid];
  ELSIF v_filters ? 'timesheetId' AND NULLIF(BTRIM(COALESCE(v_filters->>'timesheetId', '')), '') IS NOT NULL AND (v_filters->>'timesheetId') ~* v_uuid_re THEN
    v_timesheet_ids := ARRAY[(v_filters->>'timesheetId')::uuid];
  END IF;

  IF v_filters ? 'contract_week_ids' AND jsonb_typeof(v_filters->'contract_week_ids') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_contract_week_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'contract_week_ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'contractWeekIds' AND jsonb_typeof(v_filters->'contractWeekIds') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_contract_week_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'contractWeekIds') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'contract_week_id' AND NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_id', '')), '') IS NOT NULL AND (v_filters->>'contract_week_id') ~* v_uuid_re THEN
    v_contract_week_ids := ARRAY[(v_filters->>'contract_week_id')::uuid];
  ELSIF v_filters ? 'contractWeekId' AND NULLIF(BTRIM(COALESCE(v_filters->>'contractWeekId', '')), '') IS NOT NULL AND (v_filters->>'contractWeekId') ~* v_uuid_re THEN
    v_contract_week_ids := ARRAY[(v_filters->>'contractWeekId')::uuid];
  END IF;

  IF v_filters ? 'row_keys' AND jsonb_typeof(v_filters->'row_keys') = 'array' THEN
    SELECT ARRAY_AGG(row_key_values.row_key_value)
      INTO v_row_keys
    FROM (
      SELECT DISTINCT NULLIF(BTRIM(input_values.value), '') AS row_key_value
      FROM jsonb_array_elements_text(v_filters->'row_keys') AS input_values(value)
    ) AS row_key_values
    WHERE row_key_values.row_key_value IS NOT NULL;
  ELSIF v_filters ? 'rowKeys' AND jsonb_typeof(v_filters->'rowKeys') = 'array' THEN
    SELECT ARRAY_AGG(row_key_values.row_key_value)
      INTO v_row_keys
    FROM (
      SELECT DISTINCT NULLIF(BTRIM(input_values.value), '') AS row_key_value
      FROM jsonb_array_elements_text(v_filters->'rowKeys') AS input_values(value)
    ) AS row_key_values
    WHERE row_key_values.row_key_value IS NOT NULL;
  ELSIF v_filters ? 'row_key' AND NULLIF(BTRIM(COALESCE(v_filters->>'row_key', '')), '') IS NOT NULL THEN
    v_row_keys := ARRAY[NULLIF(BTRIM(v_filters->>'row_key'), '')];
  ELSIF v_filters ? 'rowKey' AND NULLIF(BTRIM(COALESCE(v_filters->>'rowKey', '')), '') IS NOT NULL THEN
    v_row_keys := ARRAY[NULLIF(BTRIM(v_filters->>'rowKey'), '')];
  END IF;


  IF v_row_keys IS NOT NULL THEN
    SELECT ARRAY_AGG(parsed_values.uuid_value)
      INTO v_row_key_timesheet_ids
    FROM (
      SELECT DISTINCT SUBSTRING(row_key_values.row_key_value FROM 11)::uuid AS uuid_value
      FROM UNNEST(v_row_keys) AS row_key_values(row_key_value)
      WHERE LOWER(LEFT(row_key_values.row_key_value, 10)) = 'timesheet:'
        AND SUBSTRING(row_key_values.row_key_value FROM 11) ~* v_uuid_re
    ) AS parsed_values;

    SELECT ARRAY_AGG(parsed_values.uuid_value)
      INTO v_row_key_contract_week_ids
    FROM (
      SELECT DISTINCT SUBSTRING(row_key_values.row_key_value FROM 15)::uuid AS uuid_value
      FROM UNNEST(v_row_keys) AS row_key_values(row_key_value)
      WHERE LOWER(LEFT(row_key_values.row_key_value, 14)) = 'contract_week:'
        AND SUBSTRING(row_key_values.row_key_value FROM 15) ~* v_uuid_re
    ) AS parsed_values;

    v_has_contract_week_row_key := COALESCE(ARRAY_LENGTH(v_row_key_contract_week_ids, 1), 0) > 0;

    IF v_row_key_timesheet_ids IS NOT NULL THEN
      SELECT ARRAY_AGG(merged_ids.uuid_value)
        INTO v_timesheet_ids
      FROM (
        SELECT DISTINCT existing_ids.uuid_value
        FROM UNNEST(COALESCE(v_timesheet_ids, ARRAY[]::uuid[])) AS existing_ids(uuid_value)
        UNION
        SELECT DISTINCT row_key_ids.uuid_value
        FROM UNNEST(v_row_key_timesheet_ids) AS row_key_ids(uuid_value)
      ) AS merged_ids;
    END IF;

    IF v_row_key_contract_week_ids IS NOT NULL THEN
      SELECT ARRAY_AGG(merged_ids.uuid_value)
        INTO v_contract_week_ids
      FROM (
        SELECT DISTINCT existing_ids.uuid_value
        FROM UNNEST(COALESCE(v_contract_week_ids, ARRAY[]::uuid[])) AS existing_ids(uuid_value)
        UNION
        SELECT DISTINCT row_key_ids.uuid_value
        FROM UNNEST(v_row_key_contract_week_ids) AS row_key_ids(uuid_value)
      ) AS merged_ids;
    END IF;
  END IF;

  v_dataset_mode := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'dataset_mode', v_filters->>'datasetMode', '')), ''));
  IF v_dataset_mode NOT IN ('PROCESS', 'AUTHORISE', 'ROW_CONTEXT') THEN
    v_dataset_mode := NULL;
  END IF;

  v_period_filter := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'period_type', v_filters->>'periodType', v_filters->>'sheet_scope', v_filters->>'sheetScope', '')), ''));
  IF v_period_filter NOT IN ('DAILY', 'WEEKLY') THEN
    v_period_filter := NULL;
  END IF;

  v_date_from_text := NULLIF(BTRIM(COALESCE(v_filters->>'date_from', v_filters->>'dateFrom', v_filters->>'from_date', v_filters->>'fromDate', '')), '');
  v_date_to_text := NULLIF(BTRIM(COALESCE(v_filters->>'date_to', v_filters->>'dateTo', v_filters->>'to_date', v_filters->>'toDate', '')), '');
  v_week_ending_text := NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_date', v_filters->>'weekEndingDate', v_filters->>'week_ending', v_filters->>'weekEnding', '')), '');
  v_week_ending_from_text := NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_from', v_filters->>'weekEndingFrom', '')), '');
  v_week_ending_to_text := NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_to', v_filters->>'weekEndingTo', '')), '');

  BEGIN
    IF v_date_from_text IS NOT NULL THEN
      v_date_from := v_date_from_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_date_from := NULL;
  END;

  BEGIN
    IF v_date_to_text IS NOT NULL THEN
      v_date_to := v_date_to_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_date_to := NULL;
  END;

  BEGIN
    IF v_week_ending_text IS NOT NULL THEN
      v_week_ending_date := v_week_ending_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_week_ending_date := NULL;
  END;

  BEGIN
    IF v_week_ending_from_text IS NOT NULL THEN
      v_week_ending_from := v_week_ending_from_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_week_ending_from := NULL;
  END;

  BEGIN
    IF v_week_ending_to_text IS NOT NULL THEN
      v_week_ending_to := v_week_ending_to_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_week_ending_to := NULL;
  END;

  RETURN QUERY
  WITH client_hr AS MATERIALIZED (
    SELECT
      cs0.client_id,
      BOOL_OR(cs0.autoprocess_hr) AS autoprocess_hr,
      BOOL_OR(cs0.requires_hr) AS requires_hr,
      BOOL_OR(cs0.no_timesheet_required) AS no_timesheet_required,
      BOOL_OR(cs0.pay_reference_required) AS pay_reference_required,
      BOOL_OR(cs0.invoice_reference_required) AS invoice_reference_required,
      BOOL_OR(cs0.reference_number_required_to_issue_invoice) AS reference_number_required_to_issue_invoice,
      BOOL_OR(cs0.hr_validation_required) AS hr_validation_required,
      BOOL_OR(cs0.ts_reference_required) AS ts_reference_required,
      BOOL_OR(cs0.is_nhsp) AS is_nhsp
    FROM public.client_settings AS cs0
    GROUP BY cs0.client_id
  ),
  timesheet_scope_rows AS MATERIALIZED (
    SELECT
      ts0.timesheet_id,
      cw0.id AS contract_week_id,
      COALESCE(cw0.week_ending_date, ts0.week_ending_date) AS effective_week_ending_date,
      ts0.sheet_scope AS effective_sheet_scope,
      COALESCE(ts0.contract_id, cw0.contract_id) AS effective_contract_id
    FROM public.timesheets AS ts0
    LEFT JOIN public.contract_weeks AS cw0
      ON cw0.timesheet_id = ts0.timesheet_id
    WHERE ts0.is_current = TRUE
      AND (v_timesheet_ids IS NULL OR ts0.timesheet_id = ANY(v_timesheet_ids))
      AND (
        v_contract_week_ids IS NULL
        OR cw0.id = ANY(v_contract_week_ids)
        OR v_timesheet_ids IS NOT NULL
        OR (v_row_key_timesheet_ids IS NOT NULL AND ts0.timesheet_id = ANY(v_row_key_timesheet_ids))
      )
      AND (
        v_row_keys IS NULL
        OR ('timesheet:' || ts0.timesheet_id::text) = ANY(v_row_keys)
        OR (v_row_key_timesheet_ids IS NOT NULL AND ts0.timesheet_id = ANY(v_row_key_timesheet_ids))
      )
      AND (
        v_week_ending_date IS NULL
        OR COALESCE(cw0.week_ending_date, ts0.week_ending_date) = v_week_ending_date
      )
      AND (
        v_week_ending_from IS NULL
        OR COALESCE(cw0.week_ending_date, ts0.week_ending_date) >= v_week_ending_from
      )
      AND (
        v_week_ending_to IS NULL
        OR COALESCE(cw0.week_ending_date, ts0.week_ending_date) <= v_week_ending_to
      )
      AND (
        v_date_from IS NULL
        OR (
          CASE
            WHEN ts0.sheet_scope = 'DAILY'::public.timesheet_scope_enum THEN COALESCE(ts0.worked_start_iso::date, ts0.scheduled_start_iso::date, ts0.week_ending_date)
            ELSE COALESCE(cw0.week_ending_date, ts0.week_ending_date)
          END
        ) >= v_date_from
      )
      AND (
        v_date_to IS NULL
        OR (
          CASE
            WHEN ts0.sheet_scope = 'DAILY'::public.timesheet_scope_enum THEN COALESCE(ts0.worked_start_iso::date, ts0.scheduled_start_iso::date, ts0.week_ending_date)
            ELSE COALESCE(cw0.week_ending_date, ts0.week_ending_date)
          END
        ) <= v_date_to
      )
      AND (
        v_period_filter IS NULL
        OR UPPER(ts0.sheet_scope::text) = v_period_filter
      )
  ),
  contract_week_scope_rows AS MATERIALIZED (
    SELECT
      cw0.id AS contract_week_id,
      cw0.contract_id,
      cw0.week_ending_date AS effective_week_ending_date
    FROM public.contract_weeks AS cw0
    JOIN public.contracts AS ct_scope
      ON ct_scope.id = cw0.contract_id
    WHERE cw0.timesheet_id IS NULL
      AND (
        v_timesheet_ids IS NULL
        OR v_contract_week_ids IS NOT NULL
        OR v_has_contract_week_row_key = TRUE
      )
      AND (
        COALESCE(v_dataset_mode, 'PROCESS') <> 'AUTHORISE'
        OR v_has_contract_week_row_key = TRUE
      )
      AND (v_candidate_id IS NULL OR ct_scope.candidate_id = v_candidate_id)
      AND (v_client_id IS NULL OR ct_scope.client_id = v_client_id)
      AND (v_contract_week_ids IS NULL OR cw0.id = ANY(v_contract_week_ids))
      AND (
        v_row_keys IS NULL
        OR ('contract_week:' || cw0.id::text) = ANY(v_row_keys)
      )
      AND (
        v_week_ending_date IS NULL
        OR cw0.week_ending_date = v_week_ending_date
      )
      AND (
        v_week_ending_from IS NULL
        OR cw0.week_ending_date >= v_week_ending_from
      )
      AND (
        v_week_ending_to IS NULL
        OR cw0.week_ending_date <= v_week_ending_to
      )
      AND (
        v_date_from IS NULL
        OR cw0.week_ending_date >= v_date_from
      )
      AND (
        v_date_to IS NULL
        OR cw0.week_ending_date <= v_date_to
      )
      AND (
        v_period_filter IS NULL
        OR v_period_filter = 'WEEKLY'
      )
  ),
  tf_ranked AS MATERIALIZED (
    SELECT
      tf0.id,
      tf0.timesheet_id,
      tf0.candidate_id,
      tf0.client_id,
      tf0.pay_method,
      tf0.processing_status,
      tf0.basis,
      tf0.total_hours,
      tf0.total_pay_ex_vat,
      tf0.total_charge_ex_vat,
      tf0.margin_ex_vat,
      tf0.paid_at_utc,
      tf0.pay_on_hold,
      tf0.locked_by_invoice_id,
      tf0.has_rate_issue,
      tf0.has_pay_channel_issue,
      tf0.hr_crosscheck_status,
      tf0.hr_crosscheck_issues,
      tf0.external_source_rows_json,
      tf0.invoice_breakdown_json,
      tf0.expenses_pay_ex_vat,
      tf0.expenses_description,
      tf0.mileage_units,
      tf0.mileage_pay_rate,
      tf0.mileage_charge_rate,
      tf0.mileage_pay_ex_vat,
      tf0.mileage_charge_ex_vat,
      tf0.travel_pay_ex_vat,
      tf0.travel_charge_ex_vat,
      tf0.accommodation_pay_ex_vat,
      tf0.accommodation_charge_ex_vat,
      tf0.other_pay_ex_vat,
      tf0.other_charge_ex_vat,
      tf0.created_at,
      tf0.updated_at,
      ROW_NUMBER() OVER (
        PARTITION BY tf0.timesheet_id
        ORDER BY tf0.created_at DESC NULLS LAST, tf0.updated_at DESC NULLS LAST, tf0.id DESC
      ) AS rn
    FROM public.timesheets_financials AS tf0
    WHERE tf0.is_current = TRUE
      AND EXISTS (
        SELECT 1
        FROM timesheet_scope_rows AS ts_scope_tf
        WHERE ts_scope_tf.timesheet_id = tf0.timesheet_id
      )
  ),
  tf_latest AS MATERIALIZED (
    SELECT
      tf1.id,
      tf1.timesheet_id,
      tf1.candidate_id,
      tf1.client_id,
      tf1.pay_method,
      tf1.processing_status,
      tf1.basis,
      tf1.total_hours,
      tf1.total_pay_ex_vat,
      tf1.total_charge_ex_vat,
      tf1.margin_ex_vat,
      tf1.paid_at_utc,
      tf1.pay_on_hold,
      tf1.locked_by_invoice_id,
      tf1.has_rate_issue,
      tf1.has_pay_channel_issue,
      tf1.hr_crosscheck_status,
      tf1.hr_crosscheck_issues,
      tf1.external_source_rows_json,
      tf1.invoice_breakdown_json,
      tf1.expenses_pay_ex_vat,
      tf1.expenses_description,
      tf1.mileage_units,
      tf1.mileage_pay_rate,
      tf1.mileage_charge_rate,
      tf1.mileage_pay_ex_vat,
      tf1.mileage_charge_ex_vat,
      tf1.travel_pay_ex_vat,
      tf1.travel_charge_ex_vat,
      tf1.accommodation_pay_ex_vat,
      tf1.accommodation_charge_ex_vat,
      tf1.other_pay_ex_vat,
      tf1.other_charge_ex_vat
    FROM tf_ranked AS tf1
    WHERE tf1.rn = 1
  ),
  tv_ranked AS MATERIALIZED (
    SELECT
      tv0.id,
      tv0.timesheet_id,
      tv0.status,
      tv0.reason_code,
      tv0.created_at,
      tv0.updated_at,
      ROW_NUMBER() OVER (
        PARTITION BY tv0.timesheet_id
        ORDER BY tv0.updated_at DESC NULLS LAST, tv0.created_at DESC NULLS LAST, tv0.id DESC
      ) AS rn
    FROM public.timesheet_validations AS tv0
    WHERE tv0.timesheet_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM timesheet_scope_rows AS ts_scope_tv
        WHERE ts_scope_tv.timesheet_id = tv0.timesheet_id
      )
  ),
  tv_latest AS MATERIALIZED (
    SELECT
      tv1.timesheet_id,
      tv1.status
    FROM tv_ranked AS tv1
    WHERE tv1.rn = 1
  ),
  evidence_agg AS MATERIALIZED (
    SELECT
      te0.timesheet_id,
      COUNT(te0.id)::integer AS evidence_count,
      COALESCE(BOOL_OR(UPPER(COALESCE(te0.kind, '')) = 'TIMESHEET'), FALSE) AS has_timesheet_evidence,
      COALESCE(BOOL_OR(UPPER(COALESCE(te0.kind, '')) = 'MILEAGE'), FALSE) AS has_mileage_evidence,
      COALESCE(BOOL_OR(UPPER(COALESCE(te0.kind, '')) = 'TRAVEL'), FALSE) AS has_travel_evidence,
      COALESCE(BOOL_OR(UPPER(COALESCE(te0.kind, '')) = 'ACCOMMODATION'), FALSE) AS has_accommodation_evidence,
      COALESCE(BOOL_OR(UPPER(COALESCE(te0.kind, '')) = 'OTHER'), FALSE) AS has_other_evidence
    FROM public.timesheet_evidence AS te0
    WHERE te0.timesheet_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM timesheet_scope_rows AS ts_scope_te
        WHERE ts_scope_te.timesheet_id = te0.timesheet_id
      )
    GROUP BY te0.timesheet_id
  ),
  nhsp_agg AS MATERIALIZED (
    SELECT
      ns0.timesheet_id,
      COUNT(ns0.id)::integer AS nhsp_shift_count,
      (COUNT(ns0.id) FILTER (WHERE ns0.invoice_status = 'INCLUDED'))::integer AS nhsp_shift_included_count,
      (COUNT(ns0.id) FILTER (WHERE ns0.invoice_status = 'DEFERRED'))::integer AS nhsp_shift_deferred_count
    FROM public.nhsp_shifts AS ns0
    WHERE ns0.timesheet_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM timesheet_scope_rows AS ts_scope_ns
        WHERE ts_scope_ns.timesheet_id = ns0.timesheet_id
      )
    GROUP BY ns0.timesheet_id
  ),
  pay_adjustments_agg AS MATERIALIZED (
    SELECT
      pa0.timesheet_id,
      COUNT(pa0.id)::integer AS pay_adjustment_count
    FROM public.ts_pay_adjustments AS pa0
    WHERE pa0.timesheet_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM timesheet_scope_rows AS ts_scope_pa
        WHERE ts_scope_pa.timesheet_id = pa0.timesheet_id
      )
    GROUP BY pa0.timesheet_id
  ),
  timesheet_rows AS MATERIALIZED (
    SELECT
      ts0.timesheet_id AS timesheet_id,
      ts0.status AS timesheet_status,
      ts0.week_ending_date AS week_ending_date,
      ts0.booking_id AS booking_id,
      ts0.occupant_key_norm AS occupant_key_norm,
      ts0.hospital_norm AS hospital_norm,
      ts0.sheet_scope AS sheet_scope,
      ts0.submission_mode AS submission_mode,
      ts0.authorised_at_server AS authorised_at_server,
      COALESCE(tf2.candidate_id, ct0.candidate_id) AS candidate_id,
      COALESCE(tf2.client_id, ct0.client_id) AS client_id,
      tf2.pay_method AS pay_method,
      tf2.processing_status AS processing_status,
      tf2.basis AS basis,
      tf2.total_hours AS total_hours,
      tf2.total_pay_ex_vat AS total_pay_ex_vat,
      tf2.total_charge_ex_vat AS total_charge_ex_vat,
      tf2.margin_ex_vat AS margin_ex_vat,
      tf2.paid_at_utc AS paid_at_utc,
      tf2.pay_on_hold AS pay_on_hold,
      tf2.locked_by_invoice_id AS locked_by_invoice_id,
      CASE
        WHEN COALESCE(tf2.candidate_id, ct0.candidate_id) IS NULL
         AND ts0.candidate_hint_text IS NOT NULL
         AND jsonb_typeof(ts0.candidate_hint_text) = 'object'
         AND (
           NULLIF(BTRIM(CONCAT_WS(' ', NULLIF(BTRIM(ts0.candidate_hint_text->>'first_name'), ''), NULLIF(BTRIM(ts0.candidate_hint_text->>'surname'), ''))), '') IS NOT NULL
           OR NULLIF(BTRIM(ts0.candidate_hint_text->>'display_name'), '') IS NOT NULL
           OR NULLIF(BTRIM(ts0.candidate_hint_text->>'email'), '') IS NOT NULL
         ) THEN
          'Unresolved Timesheet - '
          || COALESCE(
            NULLIF(BTRIM(CONCAT_WS(' ', NULLIF(BTRIM(ts0.candidate_hint_text->>'first_name'), ''), NULLIF(BTRIM(ts0.candidate_hint_text->>'surname'), ''))), ''),
            NULLIF(BTRIM(ts0.candidate_hint_text->>'display_name'), ''),
            'Candidate'
          )
          || CASE
               WHEN NULLIF(BTRIM(ts0.candidate_hint_text->>'email'), '') IS NOT NULL THEN ', Email - ' || BTRIM(ts0.candidate_hint_text->>'email')
               ELSE ''
             END
        ELSE COALESCE(cand0.display_name, ts0.occupant_key_norm)
      END AS candidate_name,
      cli0.name AS client_name,
      COALESCE(ns1.nhsp_shift_count, 0) AS nhsp_shift_count,
      COALESCE(ns1.nhsp_shift_included_count, 0) AS nhsp_shift_included_count,
      COALESCE(ns1.nhsp_shift_deferred_count, 0) AS nhsp_shift_deferred_count,
      tv2.status AS validation_status,
      cw0.id AS contract_week_id,
      cw0.week_ending_date AS contract_week_ending_date,
      cw0.status AS contract_week_status,
      cw0.additional_seq AS additional_seq,
      COALESCE(ts0.is_adjustment, cw0.is_adjustment, FALSE) AS is_adjustment,
      ts0.qr_status AS qr_status,
      ts0.qr_token AS qr_token,
      ts0.qr_generated_at AS qr_generated_at,
      ts0.qr_scanned_at AS qr_scanned_at,
      ts0.qr_last_sent_hash AS qr_last_sent_hash,
      COALESCE(pa1.pay_adjustment_count, 0) AS pay_adjustment_count,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.autoprocess_hr ELSE NULL::boolean END,
        ch0.autoprocess_hr,
        FALSE
      ) AS client_autoprocess_hr,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.requires_hr ELSE NULL::boolean END,
        ch0.requires_hr,
        FALSE
      ) AS client_requires_hr,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.no_timesheet_required ELSE NULL::boolean END,
        ch0.no_timesheet_required,
        FALSE
      ) AS client_no_timesheet_required,
      COALESCE(ch0.pay_reference_required, FALSE) AS client_pay_reference_required,
      COALESCE(ch0.invoice_reference_required, FALSE) AS client_invoice_reference_required,
      COALESCE(ch0.hr_validation_required, FALSE) AS client_hr_validation_required,
      COALESCE(ch0.ts_reference_required, FALSE) AS client_ts_reference_required,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.is_nhsp ELSE NULL::boolean END,
        ch0.is_nhsp,
        FALSE
      ) AS client_is_nhsp,
      COALESCE(tf2.has_rate_issue, FALSE) AS has_rate_issue,
      COALESCE(tf2.has_pay_channel_issue, FALSE) AS has_pay_channel_issue,
      tf2.hr_crosscheck_status AS hr_crosscheck_status,
      tf2.hr_crosscheck_issues AS hr_crosscheck_issues,
      tf2.external_source_rows_json AS external_source_rows_json,
      ts0.reference_number AS reference_number,
      ts0.day_references_json AS day_references_json,
      ts0.actual_schedule_json AS actual_schedule_json,
      ts0.r2_nurse_key AS r2_nurse_key,
      ts0.r2_auth_key AS r2_auth_key,
      ts0.manual_pdf_r2_key AS manual_pdf_r2_key,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.require_reference_to_pay ELSE NULL::boolean END,
        ch0.pay_reference_required,
        FALSE
      ) AS require_reference_to_pay,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.require_reference_to_invoice ELSE NULL::boolean END,
        ch0.invoice_reference_required,
        FALSE
      ) AS require_reference_to_invoice,
      COALESCE(ev1.evidence_count, 0) AS evidence_count,
      COALESCE(ev1.has_timesheet_evidence, FALSE) AS has_timesheet_evidence,
      COALESCE(ev1.has_mileage_evidence, FALSE) AS has_mileage_evidence,
      COALESCE(ev1.has_travel_evidence, FALSE) AS has_travel_evidence,
      COALESCE(ev1.has_accommodation_evidence, FALSE) AS has_accommodation_evidence,
      COALESCE(ev1.has_other_evidence, FALSE) AS has_other_evidence,
      ts0.candidate_hint_text AS candidate_hint_text,
      tf2.expenses_pay_ex_vat AS expenses_pay_ex_vat,
      tf2.expenses_description AS expenses_description,
      tf2.mileage_units AS mileage_units,
      tf2.mileage_pay_rate AS mileage_pay_rate,
      tf2.mileage_charge_rate AS mileage_charge_rate,
      tf2.mileage_pay_ex_vat AS mileage_pay_ex_vat,
      tf2.mileage_charge_ex_vat AS mileage_charge_ex_vat,
      tf2.travel_pay_ex_vat AS travel_pay_ex_vat,
      tf2.travel_charge_ex_vat AS travel_charge_ex_vat,
      tf2.accommodation_pay_ex_vat AS accommodation_pay_ex_vat,
      tf2.accommodation_charge_ex_vat AS accommodation_charge_ex_vat,
      tf2.other_pay_ex_vat AS other_pay_ex_vat,
      tf2.other_charge_ex_vat AS other_charge_ex_vat,
      tf2.invoice_breakdown_json AS invoice_breakdown_json
    FROM timesheet_scope_rows AS ts_scope
    JOIN public.timesheets AS ts0
      ON ts0.timesheet_id = ts_scope.timesheet_id
     AND ts0.is_current = TRUE
    LEFT JOIN public.contract_weeks AS cw0
      ON cw0.id = ts_scope.contract_week_id
    LEFT JOIN public.contracts AS ct0
      ON ct0.id = COALESCE(ts0.contract_id, cw0.contract_id)
    LEFT JOIN tf_latest AS tf2
      ON tf2.timesheet_id = ts0.timesheet_id
    LEFT JOIN tv_latest AS tv2
      ON tv2.timesheet_id = ts0.timesheet_id
    LEFT JOIN evidence_agg AS ev1
      ON ev1.timesheet_id = ts0.timesheet_id
    LEFT JOIN nhsp_agg AS ns1
      ON ns1.timesheet_id = ts0.timesheet_id
    LEFT JOIN pay_adjustments_agg AS pa1
      ON pa1.timesheet_id = ts0.timesheet_id
    LEFT JOIN public.candidates AS cand0
      ON cand0.id = COALESCE(tf2.candidate_id, ct0.candidate_id)
    LEFT JOIN public.clients AS cli0
      ON cli0.id = COALESCE(tf2.client_id, ct0.client_id)
    LEFT JOIN client_hr AS ch0
      ON ch0.client_id = COALESCE(tf2.client_id, ct0.client_id)
  ),
  planned_week_rows AS MATERIALIZED (
    SELECT
      NULL::uuid AS timesheet_id,
      NULL::public.timesheet_status_enum AS timesheet_status,
      cw0.week_ending_date AS week_ending_date,
      NULL::text AS booking_id,
      NULL::text AS occupant_key_norm,
      NULL::text AS hospital_norm,
      'WEEKLY'::public.timesheet_scope_enum AS sheet_scope,
      cw0.submission_mode_snapshot AS submission_mode,
      NULL::timestamp with time zone AS authorised_at_server,
      ct0.candidate_id AS candidate_id,
      ct0.client_id AS client_id,
      NULL::text AS pay_method,
      NULL::public.ts_fin_processing_status_enum AS processing_status,
      NULL::public.timesheet_fin_basis_enum AS basis,
      ROUND(
        (
          CASE WHEN NULLIF(BTRIM(COALESCE(cw0.totals_json #>> '{hours,day}', '')), '') ~ v_numeric_re THEN (cw0.totals_json #>> '{hours,day}')::numeric ELSE 0::numeric END
          + CASE WHEN NULLIF(BTRIM(COALESCE(cw0.totals_json #>> '{hours,night}', '')), '') ~ v_numeric_re THEN (cw0.totals_json #>> '{hours,night}')::numeric ELSE 0::numeric END
          + CASE WHEN NULLIF(BTRIM(COALESCE(cw0.totals_json #>> '{hours,sat}', '')), '') ~ v_numeric_re THEN (cw0.totals_json #>> '{hours,sat}')::numeric ELSE 0::numeric END
          + CASE WHEN NULLIF(BTRIM(COALESCE(cw0.totals_json #>> '{hours,sun}', '')), '') ~ v_numeric_re THEN (cw0.totals_json #>> '{hours,sun}')::numeric ELSE 0::numeric END
          + CASE WHEN NULLIF(BTRIM(COALESCE(cw0.totals_json #>> '{hours,bh}', '')), '') ~ v_numeric_re THEN (cw0.totals_json #>> '{hours,bh}')::numeric ELSE 0::numeric END
        ),
        2
      ) AS total_hours,
      NULL::numeric AS total_pay_ex_vat,
      NULL::numeric AS total_charge_ex_vat,
      NULL::numeric AS margin_ex_vat,
      NULL::timestamp with time zone AS paid_at_utc,
      FALSE AS pay_on_hold,
      NULL::uuid AS locked_by_invoice_id,
      cand0.display_name AS candidate_name,
      cli0.name AS client_name,
      0::integer AS nhsp_shift_count,
      0::integer AS nhsp_shift_included_count,
      0::integer AS nhsp_shift_deferred_count,
      NULL::public.validation_status_enum AS validation_status,
      cw0.id AS contract_week_id,
      cw0.week_ending_date AS contract_week_ending_date,
      cw0.status AS contract_week_status,
      cw0.additional_seq AS additional_seq,
      cw0.is_adjustment AS is_adjustment,
      NULL::public.timesheet_qr_status_enum AS qr_status,
      NULL::text AS qr_token,
      NULL::timestamp with time zone AS qr_generated_at,
      NULL::timestamp with time zone AS qr_scanned_at,
      NULL::text AS qr_last_sent_hash,
      0::integer AS pay_adjustment_count,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.autoprocess_hr ELSE NULL::boolean END,
        ch0.autoprocess_hr,
        FALSE
      ) AS client_autoprocess_hr,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.requires_hr ELSE NULL::boolean END,
        ch0.requires_hr,
        FALSE
      ) AS client_requires_hr,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.no_timesheet_required ELSE NULL::boolean END,
        ch0.no_timesheet_required,
        FALSE
      ) AS client_no_timesheet_required,
      COALESCE(ch0.pay_reference_required, FALSE) AS client_pay_reference_required,
      COALESCE(ch0.invoice_reference_required, FALSE) AS client_invoice_reference_required,
      COALESCE(ch0.hr_validation_required, FALSE) AS client_hr_validation_required,
      COALESCE(ch0.ts_reference_required, FALSE) AS client_ts_reference_required,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.is_nhsp ELSE NULL::boolean END,
        ch0.is_nhsp,
        FALSE
      ) AS client_is_nhsp,
      FALSE AS has_rate_issue,
      FALSE AS has_pay_channel_issue,
      NULL::text AS hr_crosscheck_status,
      NULL::text[] AS hr_crosscheck_issues,
      NULL::jsonb AS external_source_rows_json,
      NULL::text AS reference_number,
      NULL::jsonb AS day_references_json,
      NULL::jsonb AS actual_schedule_json,
      NULL::text AS r2_nurse_key,
      NULL::text AS r2_auth_key,
      NULL::text AS manual_pdf_r2_key,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.require_reference_to_pay ELSE NULL::boolean END,
        ch0.pay_reference_required,
        FALSE
      ) AS require_reference_to_pay,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.require_reference_to_invoice ELSE NULL::boolean END,
        ch0.invoice_reference_required,
        FALSE
      ) AS require_reference_to_invoice,
      0::integer AS evidence_count,
      FALSE AS has_timesheet_evidence,
      FALSE AS has_mileage_evidence,
      FALSE AS has_travel_evidence,
      FALSE AS has_accommodation_evidence,
      FALSE AS has_other_evidence,
      NULL::jsonb AS candidate_hint_text,
      NULL::numeric AS expenses_pay_ex_vat,
      NULL::text AS expenses_description,
      NULL::numeric AS mileage_units,
      NULL::numeric AS mileage_pay_rate,
      NULL::numeric AS mileage_charge_rate,
      NULL::numeric AS mileage_pay_ex_vat,
      NULL::numeric AS mileage_charge_ex_vat,
      NULL::numeric AS travel_pay_ex_vat,
      NULL::numeric AS travel_charge_ex_vat,
      NULL::numeric AS accommodation_pay_ex_vat,
      NULL::numeric AS accommodation_charge_ex_vat,
      NULL::numeric AS other_pay_ex_vat,
      NULL::numeric AS other_charge_ex_vat,
      NULL::jsonb AS invoice_breakdown_json
    FROM contract_week_scope_rows AS cw_scope
    JOIN public.contract_weeks AS cw0
      ON cw0.id = cw_scope.contract_week_id
    JOIN public.contracts AS ct0
      ON ct0.id = cw0.contract_id
    LEFT JOIN public.candidates AS cand0
      ON cand0.id = ct0.candidate_id
    LEFT JOIN public.clients AS cli0
      ON cli0.id = ct0.client_id
    LEFT JOIN client_hr AS ch0
      ON ch0.client_id = ct0.client_id
  ),
  base_union AS MATERIALIZED (
    SELECT
      tr0.timesheet_id,
      tr0.timesheet_status,
      tr0.week_ending_date,
      tr0.booking_id,
      tr0.occupant_key_norm,
      tr0.hospital_norm,
      tr0.sheet_scope,
      tr0.submission_mode,
      tr0.authorised_at_server,
      tr0.candidate_id,
      tr0.client_id,
      tr0.pay_method,
      tr0.processing_status,
      tr0.basis,
      tr0.total_hours,
      tr0.total_pay_ex_vat,
      tr0.total_charge_ex_vat,
      tr0.margin_ex_vat,
      tr0.paid_at_utc,
      tr0.pay_on_hold,
      tr0.locked_by_invoice_id,
      tr0.candidate_name,
      tr0.client_name,
      tr0.nhsp_shift_count,
      tr0.nhsp_shift_included_count,
      tr0.nhsp_shift_deferred_count,
      tr0.validation_status,
      tr0.contract_week_id,
      tr0.contract_week_ending_date,
      tr0.contract_week_status,
      tr0.additional_seq,
      tr0.is_adjustment,
      tr0.qr_status,
      tr0.qr_token,
      tr0.qr_generated_at,
      tr0.qr_scanned_at,
      tr0.qr_last_sent_hash,
      tr0.pay_adjustment_count,
      tr0.client_autoprocess_hr,
      tr0.client_requires_hr,
      tr0.client_no_timesheet_required,
      tr0.client_pay_reference_required,
      tr0.client_invoice_reference_required,
      tr0.client_hr_validation_required,
      tr0.client_ts_reference_required,
      tr0.client_is_nhsp,
      tr0.has_rate_issue,
      tr0.has_pay_channel_issue,
      tr0.hr_crosscheck_status,
      tr0.hr_crosscheck_issues,
      tr0.external_source_rows_json,
      tr0.reference_number,
      tr0.day_references_json,
      tr0.actual_schedule_json,
      tr0.r2_nurse_key,
      tr0.r2_auth_key,
      tr0.manual_pdf_r2_key,
      tr0.require_reference_to_pay,
      tr0.require_reference_to_invoice,
      tr0.evidence_count,
      tr0.has_timesheet_evidence,
      tr0.has_mileage_evidence,
      tr0.has_travel_evidence,
      tr0.has_accommodation_evidence,
      tr0.has_other_evidence,
      tr0.candidate_hint_text,
      tr0.expenses_pay_ex_vat,
      tr0.expenses_description,
      tr0.mileage_units,
      tr0.mileage_pay_rate,
      tr0.mileage_charge_rate,
      tr0.mileage_pay_ex_vat,
      tr0.mileage_charge_ex_vat,
      tr0.travel_pay_ex_vat,
      tr0.travel_charge_ex_vat,
      tr0.accommodation_pay_ex_vat,
      tr0.accommodation_charge_ex_vat,
      tr0.other_pay_ex_vat,
      tr0.other_charge_ex_vat,
      tr0.invoice_breakdown_json
    FROM timesheet_rows AS tr0
    UNION ALL
    SELECT
      pwr0.timesheet_id,
      pwr0.timesheet_status,
      pwr0.week_ending_date,
      pwr0.booking_id,
      pwr0.occupant_key_norm,
      pwr0.hospital_norm,
      pwr0.sheet_scope,
      pwr0.submission_mode,
      pwr0.authorised_at_server,
      pwr0.candidate_id,
      pwr0.client_id,
      pwr0.pay_method,
      pwr0.processing_status,
      pwr0.basis,
      pwr0.total_hours,
      pwr0.total_pay_ex_vat,
      pwr0.total_charge_ex_vat,
      pwr0.margin_ex_vat,
      pwr0.paid_at_utc,
      pwr0.pay_on_hold,
      pwr0.locked_by_invoice_id,
      pwr0.candidate_name,
      pwr0.client_name,
      pwr0.nhsp_shift_count,
      pwr0.nhsp_shift_included_count,
      pwr0.nhsp_shift_deferred_count,
      pwr0.validation_status,
      pwr0.contract_week_id,
      pwr0.contract_week_ending_date,
      pwr0.contract_week_status,
      pwr0.additional_seq,
      pwr0.is_adjustment,
      pwr0.qr_status,
      pwr0.qr_token,
      pwr0.qr_generated_at,
      pwr0.qr_scanned_at,
      pwr0.qr_last_sent_hash,
      pwr0.pay_adjustment_count,
      pwr0.client_autoprocess_hr,
      pwr0.client_requires_hr,
      pwr0.client_no_timesheet_required,
      pwr0.client_pay_reference_required,
      pwr0.client_invoice_reference_required,
      pwr0.client_hr_validation_required,
      pwr0.client_ts_reference_required,
      pwr0.client_is_nhsp,
      pwr0.has_rate_issue,
      pwr0.has_pay_channel_issue,
      pwr0.hr_crosscheck_status,
      pwr0.hr_crosscheck_issues,
      pwr0.external_source_rows_json,
      pwr0.reference_number,
      pwr0.day_references_json,
      pwr0.actual_schedule_json,
      pwr0.r2_nurse_key,
      pwr0.r2_auth_key,
      pwr0.manual_pdf_r2_key,
      pwr0.require_reference_to_pay,
      pwr0.require_reference_to_invoice,
      pwr0.evidence_count,
      pwr0.has_timesheet_evidence,
      pwr0.has_mileage_evidence,
      pwr0.has_travel_evidence,
      pwr0.has_accommodation_evidence,
      pwr0.has_other_evidence,
      pwr0.candidate_hint_text,
      pwr0.expenses_pay_ex_vat,
      pwr0.expenses_description,
      pwr0.mileage_units,
      pwr0.mileage_pay_rate,
      pwr0.mileage_charge_rate,
      pwr0.mileage_pay_ex_vat,
      pwr0.mileage_charge_ex_vat,
      pwr0.travel_pay_ex_vat,
      pwr0.travel_charge_ex_vat,
      pwr0.accommodation_pay_ex_vat,
      pwr0.accommodation_charge_ex_vat,
      pwr0.other_pay_ex_vat,
      pwr0.other_charge_ex_vat,
      pwr0.invoice_breakdown_json
    FROM planned_week_rows AS pwr0
  ),
  issue_rows AS MATERIALIZED (
    SELECT
      bu0.*,
      (
        ARRAY[]::text[]
        || CASE
             WHEN COALESCE(bu0.has_rate_issue, FALSE) = TRUE OR bu0.processing_status = 'RATE_MISSING'::public.ts_fin_processing_status_enum THEN ARRAY['Rate'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN COALESCE(bu0.has_pay_channel_issue, FALSE) = TRUE OR bu0.processing_status = 'PAY_CHANNEL_MISSING'::public.ts_fin_processing_status_enum THEN ARRAY['Pay channel'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN bu0.processing_status = 'UNASSIGNED'::public.ts_fin_processing_status_enum THEN ARRAY['Candidate ID'::text]
             WHEN bu0.processing_status = 'CLIENT_UNRESOLVED'::public.ts_fin_processing_status_enum THEN ARRAY['Client ID'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN COALESCE(bu0.pay_on_hold, FALSE) = TRUE THEN ARRAY['On hold'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN NOT (
                    bu0.timesheet_id IS NOT NULL
                    AND COALESCE(bu0.client_hr_validation_required, FALSE) = TRUE
                    AND COALESCE(bu0.client_no_timesheet_required, FALSE) = FALSE
                    AND COALESCE(bu0.total_hours, 0::numeric) > 0::numeric
                  )
              AND (
                    bu0.hr_crosscheck_status = 'HOURS_MISMATCH_HR'
                    OR COALESCE(bu0.hr_crosscheck_issues, ARRAY[]::text[]) && ARRAY['HOURS_MISMATCH_HR'::text]
                  ) THEN ARRAY['Hours mismatch HR'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN NOT (
                    bu0.timesheet_id IS NOT NULL
                    AND COALESCE(bu0.client_hr_validation_required, FALSE) = TRUE
                    AND COALESCE(bu0.client_no_timesheet_required, FALSE) = FALSE
                    AND COALESCE(bu0.total_hours, 0::numeric) > 0::numeric
                  )
              AND COALESCE(bu0.hr_crosscheck_issues, ARRAY[]::text[]) && ARRAY['HR_HOURS_MISSING'::text] THEN ARRAY['HR hours missing'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN COALESCE(bu0.hr_crosscheck_issues, ARRAY[]::text[]) && ARRAY['DUPLICATE_CONTRACTS'::text] THEN ARRAY['Duplicate contracts'::text]
             ELSE ARRAY[]::text[]
           END

        || CASE
             WHEN bu0.timesheet_id IS NOT NULL
              AND (
                    COALESCE(bu0.travel_charge_ex_vat, 0::numeric) > 0::numeric
                    OR COALESCE(bu0.travel_pay_ex_vat, 0::numeric) > 0::numeric
                    OR COALESCE(bu0.accommodation_charge_ex_vat, 0::numeric) > 0::numeric
                    OR COALESCE(bu0.accommodation_pay_ex_vat, 0::numeric) > 0::numeric
                    OR COALESCE(bu0.other_charge_ex_vat, 0::numeric) > 0::numeric
                    OR COALESCE(bu0.other_pay_ex_vat, 0::numeric) > 0::numeric
                  )
              AND (
                    ((COALESCE(bu0.travel_charge_ex_vat, 0::numeric) > 0::numeric OR COALESCE(bu0.travel_pay_ex_vat, 0::numeric) > 0::numeric) AND COALESCE(bu0.has_travel_evidence, FALSE) = FALSE)
                    OR ((COALESCE(bu0.accommodation_charge_ex_vat, 0::numeric) > 0::numeric OR COALESCE(bu0.accommodation_pay_ex_vat, 0::numeric) > 0::numeric) AND COALESCE(bu0.has_accommodation_evidence, FALSE) = FALSE)
                    OR ((COALESCE(bu0.other_charge_ex_vat, 0::numeric) > 0::numeric OR COALESCE(bu0.other_pay_ex_vat, 0::numeric) > 0::numeric) AND COALESCE(bu0.has_other_evidence, FALSE) = FALSE)
                  ) THEN ARRAY['Expenses evidence'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN bu0.timesheet_id IS NOT NULL
              AND (
                    COALESCE(bu0.mileage_units, 0::numeric) > 0::numeric
                    OR COALESCE(bu0.mileage_charge_ex_vat, 0::numeric) > 0::numeric
                    OR COALESCE(bu0.mileage_pay_ex_vat, 0::numeric) > 0::numeric
                  )
              AND COALESCE(bu0.has_mileage_evidence, FALSE) = FALSE THEN ARRAY['Mileage evidence'::text]
             ELSE ARRAY[]::text[]
           END


        || CASE
             WHEN bu0.timesheet_id IS NOT NULL
              AND COALESCE(bu0.client_hr_validation_required, FALSE) = TRUE
              AND COALESCE(bu0.client_no_timesheet_required, FALSE) = FALSE
              AND COALESCE(bu0.total_hours, 0::numeric) > 0::numeric
              AND (bu0.validation_status IS NULL OR bu0.validation_status = 'PENDING'::public.validation_status_enum) THEN ARRAY['Awaiting validation'::text]
             WHEN bu0.timesheet_id IS NOT NULL
              AND COALESCE(bu0.client_hr_validation_required, FALSE) = TRUE
              AND COALESCE(bu0.client_no_timesheet_required, FALSE) = FALSE
              AND COALESCE(bu0.total_hours, 0::numeric) > 0::numeric
              AND bu0.validation_status IS NOT NULL
              AND bu0.validation_status <> ALL (ARRAY['VALIDATION_OK'::public.validation_status_enum, 'OVERRIDDEN'::public.validation_status_enum, 'PENDING'::public.validation_status_enum]) THEN ARRAY['Validation failed'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN bu0.timesheet_id IS NOT NULL
              AND COALESCE(bu0.client_requires_hr, FALSE) = TRUE
              AND COALESCE(bu0.client_autoprocess_hr, FALSE) = FALSE
              AND bu0.authorised_at_server IS NULL THEN ARRAY['Authorisation'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN bu0.timesheet_id IS NOT NULL
              AND bu0.qr_status = 'PENDING'::public.timesheet_qr_status_enum
              AND (
                    (NULLIF(BTRIM(COALESCE(bu0.qr_token, '')), '') IS NOT NULL AND bu0.qr_generated_at IS NOT NULL)
                    OR NULLIF(BTRIM(COALESCE(bu0.qr_last_sent_hash, '')), '') IS NOT NULL
                  )
              AND bu0.qr_scanned_at IS NULL
              AND COALESCE(bu0.total_hours, 0::numeric) > 0::numeric THEN ARRAY['Awaiting signed QR timesheet'::text]
             ELSE ARRAY[]::text[]
           END
      ) AS lightweight_issue_codes
    FROM base_union AS bu0
  ),
  segment_rows AS MATERIALIZED (
    SELECT
      ir0.*,
      segment_stats.seg_total AS invoice_segments_total_calc,
      segment_stats.seg_locked AS invoice_segments_locked_calc,
      COALESCE(invoice_paid_stats.invoice_paid_any, FALSE) AS invoice_is_paid_calc
    FROM issue_rows AS ir0
    LEFT JOIN LATERAL (
      SELECT
        CASE
          WHEN ir0.timesheet_id IS NULL THEN NULL::integer
          WHEN ir0.invoice_breakdown_json IS NOT NULL
           AND jsonb_typeof(ir0.invoice_breakdown_json) = 'object'
           AND UPPER(COALESCE(ir0.invoice_breakdown_json->>'mode', '')) = 'SEGMENTS'
           AND jsonb_typeof(ir0.invoice_breakdown_json->'segments') = 'array' THEN jsonb_array_length(ir0.invoice_breakdown_json->'segments')::integer
          ELSE 1::integer
        END AS seg_total,
        CASE
          WHEN ir0.timesheet_id IS NULL THEN NULL::integer
          WHEN ir0.invoice_breakdown_json IS NOT NULL
           AND jsonb_typeof(ir0.invoice_breakdown_json) = 'object'
           AND UPPER(COALESCE(ir0.invoice_breakdown_json->>'mode', '')) = 'SEGMENTS'
           AND jsonb_typeof(ir0.invoice_breakdown_json->'segments') = 'array' THEN (
             SELECT COUNT(*)::integer
             FROM jsonb_array_elements(ir0.invoice_breakdown_json->'segments') AS lock_segment(segment_value)
             WHERE NULLIF(BTRIM(COALESCE(lock_segment.segment_value->>'invoice_locked_invoice_id', '')), '') IS NOT NULL
           )
          ELSE CASE WHEN ir0.locked_by_invoice_id IS NULL THEN 0::integer ELSE 1::integer END
        END AS seg_locked
    ) AS segment_stats
      ON TRUE
    LEFT JOIN LATERAL (
      SELECT
        CASE
          WHEN ir0.timesheet_id IS NULL THEN FALSE
          WHEN ir0.invoice_breakdown_json IS NOT NULL
           AND jsonb_typeof(ir0.invoice_breakdown_json) = 'object'
           AND UPPER(COALESCE(ir0.invoice_breakdown_json->>'mode', '')) = 'SEGMENTS'
           AND jsonb_typeof(ir0.invoice_breakdown_json->'segments') = 'array' THEN EXISTS (
             SELECT 1
             FROM jsonb_array_elements(ir0.invoice_breakdown_json->'segments') AS paid_segment(segment_value)
             JOIN public.invoices AS inv2
               ON inv2.id = CASE
                 WHEN NULLIF(BTRIM(COALESCE(paid_segment.segment_value->>'invoice_locked_invoice_id', '')), '') ~* v_uuid_re THEN (paid_segment.segment_value->>'invoice_locked_invoice_id')::uuid
                 ELSE NULL::uuid
               END
             WHERE inv2.status = 'PAID'::public.invoice_status_enum
                OR inv2.paid_at_utc IS NOT NULL
           )
          ELSE EXISTS (
             SELECT 1
             FROM public.invoices AS inv3
             WHERE inv3.id = ir0.locked_by_invoice_id
               AND (inv3.status = 'PAID'::public.invoice_status_enum OR inv3.paid_at_utc IS NOT NULL)
           )
        END AS invoice_paid_any
    ) AS invoice_paid_stats
      ON TRUE
  ),
  issue_normalised_rows AS MATERIALIZED (
    SELECT
      sr0.*,
      COALESCE(sr0.lightweight_issue_codes, ARRAY[]::text[]) AS workbench_issue_codes
    FROM segment_rows AS sr0
  ),
  result_rows AS MATERIALIZED (
    SELECT
      sr0.timesheet_id,
      sr0.timesheet_status,
      sr0.week_ending_date,
      sr0.booking_id,
      sr0.occupant_key_norm,
      sr0.hospital_norm,
      sr0.sheet_scope,
      sr0.submission_mode,
      sr0.authorised_at_server,
      sr0.candidate_id,
      sr0.client_id,
      sr0.pay_method,
      sr0.processing_status,
      sr0.basis,
      sr0.total_hours,
      sr0.total_pay_ex_vat,
      sr0.total_charge_ex_vat,
      sr0.margin_ex_vat,
      sr0.paid_at_utc,
      sr0.pay_on_hold,
      FALSE AS ready_to_pay,
      sr0.locked_by_invoice_id,
      sr0.candidate_name,
      sr0.client_name,
      sr0.nhsp_shift_count,
      sr0.nhsp_shift_included_count,
      sr0.nhsp_shift_deferred_count,
      sr0.validation_status,
      CASE
        WHEN sr0.timesheet_id IS NULL THEN CASE sr0.contract_week_status
          WHEN 'PLANNED'::public.contract_week_status_enum THEN 'PLANNED'
          WHEN 'OPEN'::public.contract_week_status_enum THEN 'PLANNED'
          WHEN 'SUBMITTED'::public.contract_week_status_enum THEN 'PENDING_AUTH'
          WHEN 'AUTHORISED'::public.contract_week_status_enum THEN 'READY_FOR_INVOICE'
          WHEN 'INVOICED'::public.contract_week_status_enum THEN 'INVOICED'
          WHEN 'CANCELLED'::public.contract_week_status_enum THEN 'NEEDS_ATTENTION'
          ELSE 'UNKNOWN'
        END
        WHEN sr0.paid_at_utc IS NOT NULL THEN 'PAID'
        WHEN sr0.locked_by_invoice_id IS NOT NULL
          OR (
            sr0.invoice_segments_total_calc IS NOT NULL
            AND sr0.invoice_segments_total_calc > 0
            AND COALESCE(sr0.invoice_segments_locked_calc, 0) >= sr0.invoice_segments_total_calc
          ) THEN 'INVOICED'
        WHEN sr0.timesheet_id IS NOT NULL
         AND sr0.qr_status = 'PENDING'::public.timesheet_qr_status_enum
         AND NULLIF(BTRIM(COALESCE(sr0.qr_token, '')), '') IS NULL
         AND sr0.qr_generated_at IS NULL THEN 'QR_NOT_ISSUED'
        WHEN sr0.timesheet_id IS NOT NULL
         AND sr0.qr_status = 'PENDING'::public.timesheet_qr_status_enum
         AND NULLIF(BTRIM(COALESCE(sr0.qr_token, '')), '') IS NOT NULL
         AND sr0.qr_generated_at IS NOT NULL
         AND sr0.qr_scanned_at IS NULL THEN 'QR_ISSUED_AWAITING_SIGNATURE'
        WHEN sr0.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum THEN 'READY_FOR_INVOICE'
        WHEN sr0.processing_status = 'READY_FOR_HR'::public.ts_fin_processing_status_enum THEN 'READY_FOR_HR'
        WHEN sr0.processing_status = 'PENDING_AUTH'::public.ts_fin_processing_status_enum THEN 'PENDING_AUTH'
        WHEN sr0.processing_status = 'UNPROCESSED'::public.ts_fin_processing_status_enum THEN 'UNPROCESSED'
        WHEN sr0.processing_status = ANY (ARRAY['UNASSIGNED'::public.ts_fin_processing_status_enum, 'CLIENT_UNRESOLVED'::public.ts_fin_processing_status_enum, 'RATE_MISSING'::public.ts_fin_processing_status_enum, 'PAY_CHANNEL_MISSING'::public.ts_fin_processing_status_enum]) THEN 'NEEDS_ATTENTION'
        ELSE 'UNKNOWN'
      END AS summary_stage,
      CASE
        WHEN sr0.sheet_scope = 'DAILY'::public.timesheet_scope_enum AND sr0.submission_mode = 'ELECTRONIC'::public.submission_mode_enum THEN 'DAILY_ELECTRONIC'
        WHEN sr0.sheet_scope = 'DAILY'::public.timesheet_scope_enum AND sr0.submission_mode = 'MANUAL'::public.submission_mode_enum THEN 'DAILY_MANUAL'
        WHEN sr0.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
         AND sr0.submission_mode = 'MANUAL'::public.submission_mode_enum
         AND (
           COALESCE(sr0.is_adjustment, FALSE) = TRUE
           OR COALESCE(sr0.additional_seq, 0) > 0
           OR COALESCE(sr0.pay_adjustment_count, 0) > 0
           OR UPPER(COALESCE(sr0.basis::text, '')) IN ('NHSP_ADJUSTMENT', 'HEALTHROSTER_ADJUSTMENT', 'MANUAL_ADJUSTMENT')
         )
         AND (
           COALESCE(sr0.client_is_nhsp, FALSE) = TRUE
           OR UPPER(COALESCE(sr0.basis::text, '')) IN ('NHSP', 'NHSP_ADJUSTMENT')
         ) THEN 'WEEKLY_NHSP_ADJUSTMENT'
        WHEN sr0.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
         AND sr0.submission_mode = 'MANUAL'::public.submission_mode_enum
         AND (
           COALESCE(sr0.is_adjustment, FALSE) = TRUE
           OR COALESCE(sr0.additional_seq, 0) > 0
           OR COALESCE(sr0.pay_adjustment_count, 0) > 0
           OR UPPER(COALESCE(sr0.basis::text, '')) IN ('NHSP_ADJUSTMENT', 'HEALTHROSTER_ADJUSTMENT', 'MANUAL_ADJUSTMENT')
         )
         AND (
           COALESCE(sr0.client_autoprocess_hr, FALSE) = TRUE
           OR UPPER(COALESCE(sr0.basis::text, '')) IN ('HEALTHROSTER_ADJUSTMENT', 'HEALTHROSTER_SELF_BILL')
         ) THEN 'WEEKLY_HEALTHROSTER_ADJUSTMENT'
        WHEN sr0.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
         AND sr0.submission_mode = 'MANUAL'::public.submission_mode_enum
         AND (
           COALESCE(sr0.is_adjustment, FALSE) = TRUE
           OR COALESCE(sr0.additional_seq, 0) > 0
           OR COALESCE(sr0.pay_adjustment_count, 0) > 0
           OR UPPER(COALESCE(sr0.basis::text, '')) IN ('NHSP_ADJUSTMENT', 'HEALTHROSTER_ADJUSTMENT', 'MANUAL_ADJUSTMENT')
         ) THEN 'WEEKLY_MANUAL_ADJUSTMENT'
        WHEN sr0.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND COALESCE(sr0.client_autoprocess_hr, FALSE) = TRUE THEN 'WEEKLY_HEALTHROSTER'
        WHEN sr0.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND sr0.basis = 'NHSP_ADJUSTMENT'::public.timesheet_fin_basis_enum THEN 'WEEKLY_NHSP_ADJUSTMENT'
        WHEN sr0.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND sr0.basis = 'NHSP'::public.timesheet_fin_basis_enum THEN 'WEEKLY_NHSP'
        WHEN sr0.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND COALESCE(sr0.client_is_nhsp, FALSE) = TRUE THEN 'WEEKLY_NHSP'
        WHEN sr0.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND sr0.submission_mode = 'ELECTRONIC'::public.submission_mode_enum THEN 'WEEKLY_ELECTRONIC'
        WHEN sr0.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND sr0.submission_mode = 'MANUAL'::public.submission_mode_enum THEN 'WEEKLY_MANUAL'
        ELSE 'UNKNOWN'
      END AS route_type,
      sr0.contract_week_id,
      sr0.contract_week_ending_date,
      sr0.contract_week_status,
      sr0.additional_seq,
      sr0.is_adjustment,
      sr0.qr_status,
      sr0.pay_adjustment_count,
      (COALESCE(sr0.pay_adjustment_count, 0) > 0) AS has_pay_adjustments,
      (COALESCE(sr0.is_adjustment, FALSE) OR COALESCE(sr0.pay_adjustment_count, 0) > 0) AS is_adjusted,
      (sr0.qr_status IS NOT NULL) AS is_qr,
      (
        sr0.processing_status = ANY (ARRAY['UNASSIGNED'::public.ts_fin_processing_status_enum, 'CLIENT_UNRESOLVED'::public.ts_fin_processing_status_enum, 'RATE_MISSING'::public.ts_fin_processing_status_enum, 'PAY_CHANNEL_MISSING'::public.ts_fin_processing_status_enum])
        OR (
          NOT (
            sr0.timesheet_id IS NOT NULL
            AND COALESCE(sr0.client_hr_validation_required, FALSE) = TRUE
            AND COALESCE(sr0.client_no_timesheet_required, FALSE) = FALSE
            AND COALESCE(sr0.total_hours, 0::numeric) > 0::numeric
          )
          AND sr0.hr_crosscheck_status IS NOT NULL
          AND sr0.hr_crosscheck_status <> 'OK'
        )
        OR (
          NOT (
            sr0.timesheet_id IS NOT NULL
            AND COALESCE(sr0.client_hr_validation_required, FALSE) = TRUE
            AND COALESCE(sr0.client_no_timesheet_required, FALSE) = FALSE
            AND COALESCE(sr0.total_hours, 0::numeric) > 0::numeric
          )
          AND COALESCE(sr0.hr_crosscheck_issues, ARRAY[]::text[]) && ARRAY['DUPLICATE_CONTRACTS'::text]
        )
        OR COALESCE(array_length(sr0.workbench_issue_codes, 1), 0) > 0
      ) AS needs_attention,
      sr0.client_autoprocess_hr,
      sr0.has_rate_issue,
      sr0.has_pay_channel_issue,
      sr0.hr_crosscheck_status,
      sr0.hr_crosscheck_issues,
      sr0.external_source_rows_json,
      COALESCE(sr0.workbench_issue_codes, ARRAY[]::text[]) AS issue_codes,
      sr0.client_requires_hr,
      sr0.client_no_timesheet_required,
      sr0.client_is_nhsp,
      sr0.client_pay_reference_required,
      sr0.client_invoice_reference_required,
      sr0.client_hr_validation_required,
      sr0.client_ts_reference_required,
      sr0.require_reference_to_pay,
      sr0.require_reference_to_invoice,
      sr0.qr_token,
      sr0.qr_generated_at,
      sr0.qr_scanned_at,
      sr0.candidate_hint_text,
      sr0.expenses_pay_ex_vat,
      sr0.expenses_description,
      sr0.mileage_units,
      sr0.mileage_pay_rate,
      sr0.mileage_charge_rate,
      sr0.mileage_pay_ex_vat,
      sr0.travel_pay_ex_vat,
      sr0.travel_charge_ex_vat,
      sr0.accommodation_pay_ex_vat,
      sr0.accommodation_charge_ex_vat,
      sr0.other_pay_ex_vat,
      sr0.other_charge_ex_vat,
      (
        sr0.timesheet_id IS NOT NULL
        AND COALESCE(sr0.client_hr_validation_required, FALSE) = TRUE
        AND COALESCE(sr0.client_no_timesheet_required, FALSE) = FALSE
        AND COALESCE(sr0.total_hours, 0::numeric) > 0::numeric
      ) AS hr_validation_required_for_invoice,
      sr0.invoice_segments_total_calc AS invoice_segments_total,
      sr0.invoice_segments_locked_calc AS invoice_segments_locked,
      CASE
        WHEN sr0.invoice_segments_total_calc IS NULL THEN NULL::integer
        ELSE GREATEST(sr0.invoice_segments_total_calc - COALESCE(sr0.invoice_segments_locked_calc, 0), 0)
      END AS invoice_segments_unlocked,
      CASE
        WHEN sr0.invoice_segments_total_calc IS NULL THEN NULL::text
        WHEN COALESCE(sr0.invoice_segments_locked_calc, 0) = 0 THEN 'NOT_INVOICED'
        WHEN COALESCE(sr0.invoice_segments_locked_calc, 0) >= sr0.invoice_segments_total_calc THEN 'FULLY_INVOICED'
        ELSE 'PARTIALLY_INVOICED'
      END AS invoice_segment_stage,
      CASE
        WHEN sr0.timesheet_id IS NULL THEN 'UNPROCESSED'
        WHEN sr0.processing_status = 'UNPROCESSED'::public.ts_fin_processing_status_enum THEN 'UNPROCESSED'
        WHEN sr0.locked_by_invoice_id IS NOT NULL OR COALESCE(sr0.invoice_segments_locked_calc, 0) > 0 THEN 'INVOICED'
        WHEN sr0.timesheet_id IS NOT NULL
         AND sr0.authorised_at_server IS NULL
         AND (
           sr0.processing_status = 'PENDING_AUTH'::public.ts_fin_processing_status_enum
           OR (
             COALESCE(sr0.client_requires_hr, FALSE) = TRUE
             AND COALESCE(sr0.client_autoprocess_hr, FALSE) = FALSE
             AND COALESCE(array_length(sr0.workbench_issue_codes, 1), 0) = 1
             AND sr0.workbench_issue_codes @> ARRAY['Authorisation'::text]
           )
         ) THEN 'AWAITING_AUTHORISATION'
        WHEN sr0.timesheet_id IS NOT NULL
         AND sr0.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum THEN 'AUTHORISED_FOR_INVOICING'
        ELSE 'PROCESSING_DELAYED'
      END AS tools_stage,
      CASE
        WHEN sr0.timesheet_id IS NULL THEN 'Unprocessed'
        WHEN sr0.processing_status = 'UNPROCESSED'::public.ts_fin_processing_status_enum THEN 'Unprocessed'
        WHEN sr0.locked_by_invoice_id IS NOT NULL OR COALESCE(sr0.invoice_segments_locked_calc, 0) > 0 THEN
          CASE
            WHEN sr0.invoice_segments_total_calc IS NOT NULL
             AND COALESCE(sr0.invoice_segments_locked_calc, 0) > 0
             AND COALESCE(sr0.invoice_segments_locked_calc, 0) < sr0.invoice_segments_total_calc THEN 'Partially Invoiced'
            ELSE 'Invoiced'
          END
        WHEN sr0.timesheet_id IS NOT NULL
         AND sr0.workbench_issue_codes @> ARRAY['Awaiting signed QR timesheet'::text] THEN 'Awaiting signed QR timesheet'
        WHEN sr0.timesheet_id IS NOT NULL
         AND sr0.authorised_at_server IS NULL
         AND (
           sr0.processing_status = 'PENDING_AUTH'::public.ts_fin_processing_status_enum
           OR (
             COALESCE(sr0.client_requires_hr, FALSE) = TRUE
             AND COALESCE(sr0.client_autoprocess_hr, FALSE) = FALSE
             AND COALESCE(array_length(sr0.workbench_issue_codes, 1), 0) = 1
             AND sr0.workbench_issue_codes @> ARRAY['Authorisation'::text]
           )
         ) THEN 'Awaiting Authorisation'
        WHEN sr0.timesheet_id IS NOT NULL
         AND sr0.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum THEN 'Authorised for Invoicing'
        ELSE 'Processing Delayed'
      END AS processing_status_display,
      COALESCE(sr0.invoice_is_paid_calc, FALSE) AS invoice_is_paid,
      FALSE AS refs_block_invoicing,
      FALSE AS refs_block_issuing_invoices,
      FALSE AS refs_block_invoice_and_issuing,
      CASE
        WHEN COALESCE(summary_pay_cache.summary_state_applies, FALSE)
          THEN summary_pay_cache.summary_pay_icon_code
        WHEN tps.summary_pay_icon_code IS NOT NULL THEN tps.summary_pay_icon_code
        WHEN tps.summary_pay_status_code = 'PAID' THEN 'COIN'
        WHEN tps.summary_pay_status_code = 'PARTIALLY_PAID' THEN 'HALF_COIN'
        WHEN tps.summary_pay_status_code IN ('PROCESSING','ADVANCED') THEN 'CLOCK'
        WHEN tps.summary_pay_status_code = 'UNPAID' THEN 'NONE'
        WHEN tps.last_settled_at_utc IS NOT NULL OR sr0.paid_at_utc IS NOT NULL THEN 'COIN'
        ELSE 'NONE'
      END::text AS pay_icon_code,
      COALESCE(
        CASE
          WHEN COALESCE(summary_pay_cache.summary_state_applies, FALSE)
            THEN summary_pay_cache.summary_pay_status_code
          ELSE NULL
        END,
        tps.summary_pay_status_code,
        CASE
          WHEN tps.last_settled_at_utc IS NOT NULL OR sr0.paid_at_utc IS NOT NULL THEN 'PAID'
          ELSE 'UNPAID'
        END
      )::text AS pay_status_code,
      CASE
        WHEN COALESCE(summary_pay_cache.summary_state_applies, FALSE)
          THEN summary_pay_cache.last_paid_at_utc
        WHEN tps.summary_pay_status_code IS NOT NULL OR tps.summary_pay_icon_code IS NOT NULL THEN tps.summary_pay_paid_at_utc
        ELSE COALESCE(tps.last_settled_at_utc, sr0.paid_at_utc)
      END AS pay_paid_at_utc,
      COALESCE(
        CASE
          WHEN COALESCE(summary_pay_cache.summary_state_applies, FALSE)
            THEN summary_pay_cache.net_delta_ex_vat
          ELSE NULL
        END,
        tps.summary_net_delta_ex_vat,
        0
      )::numeric AS net_delta_ex_vat
    FROM issue_normalised_rows AS sr0
    LEFT JOIN public.timesheet_summary_pay_state_cache AS summary_pay_cache
      ON summary_pay_cache.timesheet_id = sr0.timesheet_id
    LEFT JOIN public.timesheet_pay_state AS tps
      ON tps.timesheet_id = sr0.timesheet_id
  )
  SELECT
    rr0.timesheet_id,
    rr0.timesheet_status,
    rr0.week_ending_date,
    rr0.booking_id,
    rr0.occupant_key_norm,
    rr0.hospital_norm,
    rr0.sheet_scope,
    rr0.submission_mode,
    rr0.authorised_at_server,
    rr0.candidate_id,
    rr0.client_id,
    rr0.pay_method,
    rr0.processing_status,
    rr0.basis,
    rr0.total_hours,
    rr0.total_pay_ex_vat,
    rr0.total_charge_ex_vat,
    rr0.margin_ex_vat,
    rr0.paid_at_utc,
    rr0.pay_on_hold,
    rr0.ready_to_pay,
    rr0.locked_by_invoice_id,
    rr0.candidate_name,
    rr0.client_name,
    rr0.nhsp_shift_count,
    rr0.nhsp_shift_included_count,
    rr0.nhsp_shift_deferred_count,
    rr0.validation_status,
    rr0.summary_stage,
    rr0.route_type,
    rr0.contract_week_id,
    rr0.contract_week_ending_date,
    rr0.contract_week_status,
    rr0.additional_seq,
    rr0.is_adjustment,
    rr0.qr_status,
    rr0.pay_adjustment_count,
    rr0.has_pay_adjustments,
    rr0.is_adjusted,
    rr0.is_qr,
    rr0.needs_attention,
    rr0.client_autoprocess_hr,
    rr0.has_rate_issue,
    rr0.has_pay_channel_issue,
    rr0.hr_crosscheck_status,
    rr0.hr_crosscheck_issues,
    rr0.external_source_rows_json,
    rr0.issue_codes,
    rr0.client_requires_hr,
    rr0.client_no_timesheet_required,
    rr0.client_is_nhsp,
    rr0.client_pay_reference_required,
    rr0.client_invoice_reference_required,
    rr0.client_hr_validation_required,
    rr0.client_ts_reference_required,
    rr0.require_reference_to_pay,
    rr0.require_reference_to_invoice,
    rr0.qr_token,
    rr0.qr_generated_at,
    rr0.qr_scanned_at,
    rr0.candidate_hint_text,
    rr0.expenses_pay_ex_vat,
    rr0.expenses_description,
    rr0.mileage_units,
    rr0.mileage_pay_rate,
    rr0.mileage_charge_rate,
    rr0.mileage_pay_ex_vat,
    rr0.travel_pay_ex_vat,
    rr0.travel_charge_ex_vat,
    rr0.accommodation_pay_ex_vat,
    rr0.accommodation_charge_ex_vat,
    rr0.other_pay_ex_vat,
    rr0.other_charge_ex_vat,
    rr0.hr_validation_required_for_invoice,
    rr0.invoice_segments_total,
    rr0.invoice_segments_locked,
    rr0.invoice_segments_unlocked,
    rr0.invoice_segment_stage,
    rr0.tools_stage,
    rr0.processing_status_display,
    rr0.invoice_is_paid,
    rr0.refs_block_invoicing,
    rr0.refs_block_issuing_invoices,
    rr0.refs_block_invoice_and_issuing,
    rr0.pay_icon_code,
    rr0.pay_status_code,
    rr0.pay_paid_at_utc,
    rr0.net_delta_ex_vat
  FROM result_rows AS rr0
  WHERE (v_candidate_id IS NULL OR rr0.candidate_id = v_candidate_id)
    AND (v_client_id IS NULL OR rr0.client_id = v_client_id);
END;
$function$;

-- ============================================================================
-- 006_timesheet_summary_lightweight_rows_v1.sql
-- ============================================================================
CREATE OR REPLACE FUNCTION public.timesheet_summary_lightweight_rows_v1(p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS TABLE(timesheet_id uuid, contract_week_id uuid, contract_id uuid, candidate_id uuid, candidate_name text, candidate_display_name text, client_id uuid, client_name text, booking_id text, occupant_key_norm text, hospital_norm text, candidate_hint_text jsonb, week_ending_date date, work_date date, sheet_scope text, submission_mode text, submission_mode_snapshot text, basis text, route_type text, route_display text, route_family text, route_subfamily text, underlying_channel_family text, summary_stage text, tools_stage text, processing_status text, processing_status_display text, authorised_at_utc timestamp with time zone, authorised_at_server timestamp with time zone, processed_at_utc timestamp with time zone, is_authorised boolean, total_hours numeric, total_pay_ex_vat numeric, total_charge_ex_vat numeric, margin_ex_vat numeric, net_delta_ex_vat numeric, paid_at_utc timestamp with time zone, pay_icon_code text, pay_status_code text, pay_paid_at_utc timestamp with time zone, invoice_is_paid boolean, invoice_issue_stage text, invoice_segment_stage text, invoice_segments_total integer, invoice_segments_locked integer, invoice_segments_unlocked integer, issue_codes text[], validation_status text, validation_summary text, hr_crosscheck_status text, hr_crosscheck_issues text[], qr_status text, is_qr boolean, is_adjusted boolean, needs_attention boolean, has_rate_issue boolean, has_pay_channel_issue boolean, client_no_timesheet_required boolean, client_autoprocess_hr boolean, client_is_nhsp boolean, has_any_evidence boolean, attached_evidence_count integer, primary_artifact_storage_key text, primary_artifact_display_name text, primary_artifact_preview_mode text)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_source_filters jsonb := COALESCE(p_filters, '{}'::jsonb);

  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';

  v_id_text text := NULL;
  v_lookup_ids uuid[] := NULL;
  v_has_lookup_filter boolean := FALSE;

  v_timesheet_ids_filter uuid[] := NULL;
  v_contract_week_ids_filter uuid[] := NULL;
  v_has_timesheet_filter boolean := FALSE;
  v_has_contract_week_filter boolean := FALSE;

  v_candidate_id_text text := NULL;
  v_client_id_text text := NULL;
  v_candidate_id_filter uuid := NULL;
  v_client_id_filter uuid := NULL;
  v_has_candidate_filter boolean := FALSE;
  v_has_client_filter boolean := FALSE;

  v_q text := NULL;
  v_tools_stage text := NULL;
  v_route_type text := NULL;
  v_sheet_scope text := NULL;
  v_qr_status text := NULL;
  v_status_code text := NULL;
  v_issues_filter text := NULL;

  v_candidate_paid boolean := NULL;
  v_is_adjusted boolean := NULL;
  v_is_qr boolean := NULL;
  v_hr_issue boolean := NULL;
  v_hr_issue_token text := NULL;

  v_week_ending_from_text text := NULL;
  v_week_ending_to_text text := NULL;
  v_week_ending_from date := NULL;
  v_week_ending_to date := NULL;

  v_order_by text := 'candidate_name';
  v_order_dir text := 'asc';
  v_limit integer := 100;
  v_offset integer := 0;
  v_disable_paging boolean := FALSE;
BEGIN
  v_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'id', '')), '');
  v_has_lookup_filter := (v_id_text IS NOT NULL) OR (v_filters ? 'ids');

  IF v_id_text IS NOT NULL AND v_id_text ~* v_uuid_re THEN
    v_lookup_ids := ARRAY[v_id_text::uuid];
  ELSIF v_filters ? 'ids' AND jsonb_typeof(v_filters->'ids') = 'array' THEN
    SELECT ARRAY_AGG(id_values.id_value)
      INTO v_lookup_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS id_value
      FROM jsonb_array_elements_text(v_filters->'ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS id_values;
  ELSIF v_filters ? 'ids'
        AND NULLIF(BTRIM(COALESCE(v_filters->>'ids', '')), '') IS NOT NULL THEN
    SELECT ARRAY_AGG(id_values.id_value)
      INTO v_lookup_ids
    FROM (
      SELECT DISTINCT split_values.value::uuid AS id_value
      FROM UNNEST(regexp_split_to_array(v_filters->>'ids', '\s*,\s*')) AS split_values(value)
      WHERE split_values.value ~* v_uuid_re
    ) AS id_values;
  END IF;

  IF v_has_lookup_filter AND COALESCE(ARRAY_LENGTH(v_lookup_ids, 1), 0) = 0 THEN
    RETURN;
  END IF;

  IF COALESCE(ARRAY_LENGTH(v_lookup_ids, 1), 0) > 0 THEN
    v_source_filters :=
      v_source_filters
      || jsonb_build_object(
           'timesheet_ids', to_jsonb(v_lookup_ids),
           'contract_week_ids', to_jsonb(v_lookup_ids)
         );
  END IF;

  v_has_timesheet_filter :=
    (v_filters ? 'timesheet_id')
    OR (v_filters ? 'timesheetId')
    OR (v_filters ? 'timesheet_ids')
    OR (v_filters ? 'timesheetIds');

  IF v_filters ? 'timesheet_ids' AND jsonb_typeof(v_filters->'timesheet_ids') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_timesheet_ids_filter
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'timesheet_ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'timesheet_ids'
        AND NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_ids', '')), '') IS NOT NULL THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_timesheet_ids_filter
    FROM (
      SELECT DISTINCT split_values.value::uuid AS uuid_value
      FROM UNNEST(regexp_split_to_array(v_filters->>'timesheet_ids', '\s*,\s*')) AS split_values(value)
      WHERE split_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'timesheetIds' AND jsonb_typeof(v_filters->'timesheetIds') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_timesheet_ids_filter
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'timesheetIds') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'timesheetIds'
        AND NULLIF(BTRIM(COALESCE(v_filters->>'timesheetIds', '')), '') IS NOT NULL THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_timesheet_ids_filter
    FROM (
      SELECT DISTINCT split_values.value::uuid AS uuid_value
      FROM UNNEST(regexp_split_to_array(v_filters->>'timesheetIds', '\s*,\s*')) AS split_values(value)
      WHERE split_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'timesheet_id'
        AND NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_id', '')), '') IS NOT NULL
        AND (v_filters->>'timesheet_id') ~* v_uuid_re THEN
    v_timesheet_ids_filter := ARRAY[(v_filters->>'timesheet_id')::uuid];
  ELSIF v_filters ? 'timesheetId'
        AND NULLIF(BTRIM(COALESCE(v_filters->>'timesheetId', '')), '') IS NOT NULL
        AND (v_filters->>'timesheetId') ~* v_uuid_re THEN
    v_timesheet_ids_filter := ARRAY[(v_filters->>'timesheetId')::uuid];
  END IF;

  IF v_has_timesheet_filter AND COALESCE(ARRAY_LENGTH(v_timesheet_ids_filter, 1), 0) = 0 THEN
    RETURN;
  END IF;

  IF COALESCE(ARRAY_LENGTH(v_timesheet_ids_filter, 1), 0) > 0 THEN
    v_source_filters :=
      v_source_filters
      || jsonb_build_object('timesheet_ids', to_jsonb(v_timesheet_ids_filter));
  END IF;

  v_has_contract_week_filter :=
    (v_filters ? 'contract_week_id')
    OR (v_filters ? 'contractWeekId')
    OR (v_filters ? 'contract_week_ids')
    OR (v_filters ? 'contractWeekIds');

  IF v_filters ? 'contract_week_ids' AND jsonb_typeof(v_filters->'contract_week_ids') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_contract_week_ids_filter
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'contract_week_ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'contract_week_ids'
        AND NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_ids', '')), '') IS NOT NULL THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_contract_week_ids_filter
    FROM (
      SELECT DISTINCT split_values.value::uuid AS uuid_value
      FROM UNNEST(regexp_split_to_array(v_filters->>'contract_week_ids', '\s*,\s*')) AS split_values(value)
      WHERE split_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'contractWeekIds' AND jsonb_typeof(v_filters->'contractWeekIds') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_contract_week_ids_filter
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'contractWeekIds') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'contractWeekIds'
        AND NULLIF(BTRIM(COALESCE(v_filters->>'contractWeekIds', '')), '') IS NOT NULL THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_contract_week_ids_filter
    FROM (
      SELECT DISTINCT split_values.value::uuid AS uuid_value
      FROM UNNEST(regexp_split_to_array(v_filters->>'contractWeekIds', '\s*,\s*')) AS split_values(value)
      WHERE split_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'contract_week_id'
        AND NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_id', '')), '') IS NOT NULL
        AND (v_filters->>'contract_week_id') ~* v_uuid_re THEN
    v_contract_week_ids_filter := ARRAY[(v_filters->>'contract_week_id')::uuid];
  ELSIF v_filters ? 'contractWeekId'
        AND NULLIF(BTRIM(COALESCE(v_filters->>'contractWeekId', '')), '') IS NOT NULL
        AND (v_filters->>'contractWeekId') ~* v_uuid_re THEN
    v_contract_week_ids_filter := ARRAY[(v_filters->>'contractWeekId')::uuid];
  END IF;

  IF v_has_contract_week_filter AND COALESCE(ARRAY_LENGTH(v_contract_week_ids_filter, 1), 0) = 0 THEN
    RETURN;
  END IF;

  IF COALESCE(ARRAY_LENGTH(v_contract_week_ids_filter, 1), 0) > 0 THEN
    v_source_filters :=
      v_source_filters
      || jsonb_build_object('contract_week_ids', to_jsonb(v_contract_week_ids_filter));
  END IF;

  v_candidate_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), '');
  v_has_candidate_filter := v_candidate_id_text IS NOT NULL;
  BEGIN
    IF v_candidate_id_text IS NOT NULL AND v_candidate_id_text ~* v_uuid_re THEN
      v_candidate_id_filter := v_candidate_id_text::uuid;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_candidate_id_filter := NULL;
  END;

  IF v_has_candidate_filter AND v_candidate_id_filter IS NULL THEN
    RETURN;
  END IF;

  v_client_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), '');
  v_has_client_filter := v_client_id_text IS NOT NULL;
  BEGIN
    IF v_client_id_text IS NOT NULL AND v_client_id_text ~* v_uuid_re THEN
      v_client_id_filter := v_client_id_text::uuid;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_client_id_filter := NULL;
  END;

  IF v_has_client_filter AND v_client_id_filter IS NULL THEN
    RETURN;
  END IF;

  v_q := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'query', v_filters->>'name', '')), ''));
  v_tools_stage := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'tools_stage', v_filters->>'toolsStage', '')), ''));
  v_route_type := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'route_type', v_filters->>'routeType', '')), ''));
  v_sheet_scope := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'sheet_scope', v_filters->>'sheetScope', '')), ''));
  v_qr_status := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'qr_status', v_filters->>'qrStatus', '')), ''));
  v_status_code := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'status_code', v_filters->>'statusCode', '')), ''));
  v_issues_filter := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'issues_filter', v_filters->>'issuesFilter', '')), ''));

  IF LOWER(COALESCE(v_filters->>'candidate_paid', v_filters->>'candidatePaid', '')) IN ('true','t','yes','y','1') THEN
    v_candidate_paid := TRUE;
  ELSIF LOWER(COALESCE(v_filters->>'candidate_paid', v_filters->>'candidatePaid', '')) IN ('false','f','no','n','0') THEN
    v_candidate_paid := FALSE;
  END IF;

  IF LOWER(COALESCE(v_filters->>'is_adjusted', v_filters->>'isAdjusted', '')) IN ('true','t','yes','y','1') THEN
    v_is_adjusted := TRUE;
  ELSIF LOWER(COALESCE(v_filters->>'is_adjusted', v_filters->>'isAdjusted', '')) IN ('false','f','no','n','0') THEN
    v_is_adjusted := FALSE;
  END IF;

  IF LOWER(COALESCE(v_filters->>'is_qr', v_filters->>'isQr', '')) IN ('true','t','yes','y','1') THEN
    v_is_qr := TRUE;
  ELSIF LOWER(COALESCE(v_filters->>'is_qr', v_filters->>'isQr', '')) IN ('false','f','no','n','0') THEN
    v_is_qr := FALSE;
  END IF;

  IF LOWER(COALESCE(v_filters->>'hr_issue', v_filters->>'hrIssue', '')) IN ('true','t','yes','y','1') THEN
    v_hr_issue := TRUE;
  ELSIF LOWER(COALESCE(v_filters->>'hr_issue', v_filters->>'hrIssue', '')) IN ('false','f','no','n','0') THEN
    v_hr_issue := FALSE;
  ELSE
    v_hr_issue_token := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'hr_issue', v_filters->>'hrIssue', '')), ''));
    IF v_hr_issue_token = 'ALL' THEN
      v_hr_issue_token := NULL;
    END IF;
  END IF;

  v_week_ending_from_text := NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_from', v_filters->>'weekEndingFrom', '')), '');
  v_week_ending_to_text := NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_to', v_filters->>'weekEndingTo', '')), '');

  BEGIN
    IF v_week_ending_from_text IS NOT NULL THEN
      v_week_ending_from := v_week_ending_from_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_week_ending_from := NULL;
  END;

  BEGIN
    IF v_week_ending_to_text IS NOT NULL THEN
      v_week_ending_to := v_week_ending_to_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_week_ending_to := NULL;
  END;

  v_order_by := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'order_by', v_filters->>'orderBy', 'candidate_name')), ''));
  v_order_dir := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'order_dir', v_filters->>'orderDir', 'asc')), ''));

  IF v_order_dir NOT IN ('asc', 'desc') THEN
    v_order_dir := 'asc';
  END IF;

  v_disable_paging :=
    LOWER(COALESCE(v_filters->>'disable_paging', v_filters->>'disablePaging', v_filters->>'no_paging', v_filters->>'noPaging', '')) IN ('true','t','yes','y','1')
    OR LOWER(COALESCE(v_filters->>'apply_paging', v_filters->>'applyPaging', '')) IN ('false','f','no','n','0')
    OR LOWER(COALESCE(v_filters->>'purpose', '')) IN ('membership','memberships','ids','totals','total','count','counts');

  IF v_disable_paging THEN
    v_limit := NULL;
    v_offset := 0;
  ELSE
    IF COALESCE(v_filters->>'limit', '') ~ '^[0-9]+$' THEN
      v_limit := LEAST(GREATEST((v_filters->>'limit')::integer, 1), 5000);
    ELSIF COALESCE(v_filters->>'page_size', v_filters->>'pageSize', '') ~ '^[0-9]+$' THEN
      v_limit := LEAST(GREATEST(COALESCE(v_filters->>'page_size', v_filters->>'pageSize')::integer, 1), 5000);
    END IF;

    IF COALESCE(v_filters->>'offset', '') ~ '^[0-9]+$' THEN
      v_offset := GREATEST((v_filters->>'offset')::integer, 0);
    ELSIF COALESCE(v_filters->>'page', '') ~ '^[0-9]+$'
          AND COALESCE(v_filters->>'page_size', v_filters->>'pageSize', '') ~ '^[0-9]+$' THEN
      v_offset :=
        GREATEST(((v_filters->>'page')::integer - 1), 0)
        * LEAST(GREATEST(COALESCE(v_filters->>'page_size', v_filters->>'pageSize')::integer, 1), 5000);
    END IF;
  END IF;

  RETURN QUERY
  WITH source_rows AS MATERIALIZED (
    SELECT
      source_row.*
    FROM public.bulk_timesheet_workbench_row_source_v1(v_source_filters) AS source_row
  ),
  enriched AS MATERIALIZED (
    SELECT
      source_rows.timesheet_id,
      source_rows.contract_week_id,
      COALESCE(timesheet_row.contract_id, contract_week_row.contract_id) AS contract_id,

      source_rows.candidate_id,
      source_rows.candidate_name,
      source_rows.candidate_name AS candidate_display_name,
      source_rows.client_id,
      source_rows.client_name,

      source_rows.booking_id,
      source_rows.occupant_key_norm,
      source_rows.hospital_norm,
      source_rows.candidate_hint_text,

      COALESCE(source_rows.contract_week_ending_date, source_rows.week_ending_date) AS week_ending_date,

      CASE
        WHEN source_rows.sheet_scope = 'DAILY'::public.timesheet_scope_enum THEN
          COALESCE(timesheet_row.worked_start_iso::date, timesheet_row.scheduled_start_iso::date, source_rows.week_ending_date)
        ELSE NULL::date
      END AS work_date,

      source_rows.sheet_scope::text AS sheet_scope,
      source_rows.submission_mode::text AS submission_mode,
      COALESCE(contract_week_row.submission_mode_snapshot::text, source_rows.submission_mode::text) AS submission_mode_snapshot,
      source_rows.basis::text AS basis,

      source_rows.route_type,
      CASE
        WHEN (
          source_rows.submission_mode = 'MANUAL'::public.submission_mode_enum
          AND (
            COALESCE(source_rows.is_adjusted, FALSE) = TRUE
            OR COALESCE(source_rows.is_adjustment, FALSE) = TRUE
            OR COALESCE(source_rows.additional_seq, 0) > 0
            OR UPPER(COALESCE(source_rows.basis::text, '')) IN ('NHSP_ADJUSTMENT', 'MANUAL_ADJUSTMENT')
          )
        )
         AND COALESCE(source_rows.client_is_nhsp, FALSE) = TRUE THEN 'NHSP Manual Adjustment'
        WHEN (
          source_rows.submission_mode = 'MANUAL'::public.submission_mode_enum
          AND (
            COALESCE(source_rows.is_adjusted, FALSE) = TRUE
            OR COALESCE(source_rows.is_adjustment, FALSE) = TRUE
            OR COALESCE(source_rows.additional_seq, 0) > 0
            OR UPPER(COALESCE(source_rows.basis::text, '')) IN ('NHSP_ADJUSTMENT', 'MANUAL_ADJUSTMENT')
          )
        )
         AND COALESCE(source_rows.client_autoprocess_hr, FALSE) = TRUE THEN 'HealthRoster Manual Adjustment'
        WHEN (
          source_rows.submission_mode = 'MANUAL'::public.submission_mode_enum
          AND (
            COALESCE(source_rows.is_adjusted, FALSE) = TRUE
            OR COALESCE(source_rows.is_adjustment, FALSE) = TRUE
            OR COALESCE(source_rows.additional_seq, 0) > 0
            OR UPPER(COALESCE(source_rows.basis::text, '')) IN ('NHSP_ADJUSTMENT', 'MANUAL_ADJUSTMENT')
          )
        ) THEN 'Manual Adjustment'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) = 'NHSP' THEN 'NHSP'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) = 'HEALTHROSTER' THEN 'HealthRoster'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) = 'HEALTHROSTER_DAILY' THEN 'HealthRoster Daily'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) = 'QR' THEN 'QR'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) = 'NO_TIMESHEET_REQUIRED' THEN 'No timesheet required'
        WHEN COALESCE(source_rows.route_type, '') <> '' THEN INITCAP(REPLACE(source_rows.route_type, '_', ' '))
        ELSE 'Manual'
      END AS route_display,

      CASE
        WHEN (
          source_rows.submission_mode = 'MANUAL'::public.submission_mode_enum
          AND (
            COALESCE(source_rows.is_adjusted, FALSE) = TRUE
            OR COALESCE(source_rows.is_adjustment, FALSE) = TRUE
            OR COALESCE(source_rows.additional_seq, 0) > 0
            OR UPPER(COALESCE(source_rows.basis::text, '')) IN ('NHSP_ADJUSTMENT', 'MANUAL_ADJUSTMENT')
          )
        ) THEN 'MANUAL'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) LIKE '%NHSP%' THEN 'NHSP'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) LIKE '%HEALTHROSTER%' THEN 'HEALTHROSTER'
        WHEN COALESCE(source_rows.is_qr, FALSE) THEN 'QR'
        WHEN COALESCE(source_rows.client_no_timesheet_required, FALSE) THEN 'NO_TIMESHEET_REQUIRED'
        ELSE 'MANUAL'
      END AS route_family,

      CASE
        WHEN (
          source_rows.submission_mode = 'MANUAL'::public.submission_mode_enum
          AND (
            COALESCE(source_rows.is_adjusted, FALSE) = TRUE
            OR COALESCE(source_rows.is_adjustment, FALSE) = TRUE
            OR COALESCE(source_rows.additional_seq, 0) > 0
            OR UPPER(COALESCE(source_rows.basis::text, '')) IN ('NHSP_ADJUSTMENT', 'MANUAL_ADJUSTMENT')
          )
        ) THEN 'MANUAL_ADJUSTMENT'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) = 'HEALTHROSTER_DAILY' THEN 'DAILY'
        WHEN source_rows.sheet_scope = 'DAILY'::public.timesheet_scope_enum THEN 'DAILY'
        ELSE 'WEEKLY'
      END AS route_subfamily,

      CASE
        WHEN (
          source_rows.submission_mode = 'MANUAL'::public.submission_mode_enum
          AND (
            COALESCE(source_rows.is_adjusted, FALSE) = TRUE
            OR COALESCE(source_rows.is_adjustment, FALSE) = TRUE
            OR COALESCE(source_rows.additional_seq, 0) > 0
            OR UPPER(COALESCE(source_rows.basis::text, '')) IN ('NHSP_ADJUSTMENT', 'MANUAL_ADJUSTMENT')
          )
        ) THEN 'MANUAL'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) LIKE '%NHSP%' THEN 'IMPORT'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) LIKE '%HEALTHROSTER%' THEN 'IMPORT'
        WHEN COALESCE(source_rows.is_qr, FALSE) THEN 'QR'
        ELSE 'MANUAL'
      END AS underlying_channel_family,

      source_rows.summary_stage,
      source_rows.tools_stage,
      source_rows.processing_status::text AS processing_status,
      source_rows.processing_status_display,

      COALESCE(financial_row.authorised_at_utc, source_rows.authorised_at_server) AS authorised_at_utc,
      source_rows.authorised_at_server,
      financial_row.processed_at_utc,

      (COALESCE(financial_row.authorised_at_utc, source_rows.authorised_at_server) IS NOT NULL) AS is_authorised,

      COALESCE(source_rows.total_hours, 0::numeric) AS total_hours,
      COALESCE(source_rows.total_pay_ex_vat, 0::numeric) AS total_pay_ex_vat,
      COALESCE(source_rows.total_charge_ex_vat, 0::numeric) AS total_charge_ex_vat,
      COALESCE(source_rows.margin_ex_vat, 0::numeric) AS margin_ex_vat,
      COALESCE(source_rows.net_delta_ex_vat, COALESCE(source_rows.total_charge_ex_vat, 0::numeric) - COALESCE(source_rows.total_pay_ex_vat, 0::numeric)) AS net_delta_ex_vat,

      source_rows.paid_at_utc,
      source_rows.pay_icon_code,
      source_rows.pay_status_code,
      source_rows.pay_paid_at_utc,

      COALESCE(source_rows.invoice_is_paid, FALSE) AS invoice_is_paid,

      CASE
        WHEN COALESCE(source_rows.invoice_is_paid, FALSE) THEN 'PAID'
        WHEN COALESCE(source_rows.invoice_segments_locked, 0) > 0 THEN 'LOCKED'
        WHEN COALESCE(source_rows.invoice_segments_total, 0) > 0 THEN 'DRAFT'
        ELSE NULL::text
      END AS invoice_issue_stage,

      source_rows.invoice_segment_stage,
      COALESCE(source_rows.invoice_segments_total, 0)::integer AS invoice_segments_total,
      COALESCE(source_rows.invoice_segments_locked, 0)::integer AS invoice_segments_locked,
      COALESCE(source_rows.invoice_segments_unlocked, 0)::integer AS invoice_segments_unlocked,

      COALESCE(
        ARRAY(
          SELECT issue_values.issue_code
          FROM UNNEST(COALESCE(source_rows.issue_codes, ARRAY[]::text[]))
            WITH ORDINALITY AS issue_values(issue_code, issue_ordinality)
          WHERE issue_values.issue_code NOT IN (
            '__PAY_BADGE_ADV__',
            '__PAY_BADGE_OVERPAID__',
            '__PAY_BADGE_PROCESSING__'
          )
          ORDER BY issue_values.issue_ordinality
        ),
        ARRAY[]::text[]
      ) AS business_issue_codes,
      (
        COALESCE(
          ARRAY(
            SELECT issue_values.issue_code
            FROM UNNEST(COALESCE(source_rows.issue_codes, ARRAY[]::text[]))
              WITH ORDINALITY AS issue_values(issue_code, issue_ordinality)
            WHERE issue_values.issue_code NOT IN (
              '__PAY_BADGE_ADV__',
              '__PAY_BADGE_OVERPAID__',
              '__PAY_BADGE_PROCESSING__'
            )
            ORDER BY issue_values.issue_ordinality
          ),
          ARRAY[]::text[]
        )
        || COALESCE(summary_pay_cache.summary_badge_codes, ARRAY[]::text[])
      ) AS issue_codes,
      source_rows.validation_status::text AS validation_status,

      CASE
        WHEN source_rows.validation_status IS NULL THEN NULL::text
        ELSE source_rows.validation_status::text
      END AS validation_summary,

      source_rows.hr_crosscheck_status,
      COALESCE(source_rows.hr_crosscheck_issues, ARRAY[]::text[]) AS hr_crosscheck_issues,

      source_rows.qr_status::text AS qr_status,
      timesheet_row.qr_token AS qr_token,
      timesheet_row.qr_generated_at AS qr_generated_at,
      timesheet_row.qr_scanned_at AS qr_scanned_at,
      COALESCE(source_rows.is_qr, FALSE) AS is_qr,
      COALESCE(source_rows.is_adjusted, FALSE) AS is_adjusted,
      COALESCE(source_rows.needs_attention, FALSE) AS needs_attention,
      COALESCE(source_rows.has_rate_issue, FALSE) AS has_rate_issue,
      COALESCE(source_rows.has_pay_channel_issue, FALSE) AS has_pay_channel_issue,

      COALESCE(source_rows.client_no_timesheet_required, FALSE) AS client_no_timesheet_required,
      COALESCE(source_rows.client_autoprocess_hr, FALSE) AS client_autoprocess_hr,
      COALESCE(source_rows.client_is_nhsp, FALSE) AS client_is_nhsp,

      (
        COALESCE(evidence_summary.attached_evidence_count, 0) > 0
        OR NULLIF(timesheet_row.manual_pdf_r2_key, '') IS NOT NULL
        OR NULLIF(timesheet_row.qr_r2_key, '') IS NOT NULL
        OR (source_rows.timesheet_id IS NULL AND NULLIF(contract_week_row.uploaded_pdf_r2_key, '') IS NOT NULL)
      ) AS has_any_evidence,

      COALESCE(evidence_summary.attached_evidence_count, 0)::integer AS attached_evidence_count,

      COALESCE(
        evidence_summary.primary_storage_key,
        NULLIF(timesheet_row.manual_pdf_r2_key, ''),
        NULLIF(timesheet_row.qr_r2_key, ''),
        CASE WHEN source_rows.timesheet_id IS NULL THEN NULLIF(contract_week_row.uploaded_pdf_r2_key, '') END
      ) AS primary_artifact_storage_key,

      COALESCE(
        evidence_summary.primary_display_name,
        CASE WHEN NULLIF(timesheet_row.manual_pdf_r2_key, '') IS NOT NULL THEN 'Manual timesheet PDF' END,
        CASE WHEN NULLIF(timesheet_row.qr_r2_key, '') IS NOT NULL THEN 'QR timesheet' END,
        CASE WHEN source_rows.timesheet_id IS NULL AND NULLIF(contract_week_row.uploaded_pdf_r2_key, '') IS NOT NULL THEN 'Uploaded weekly PDF' END
      ) AS primary_artifact_display_name,

      CASE
        WHEN COALESCE(evidence_summary.primary_storage_key, NULLIF(timesheet_row.manual_pdf_r2_key, ''), NULLIF(timesheet_row.qr_r2_key, ''), CASE WHEN source_rows.timesheet_id IS NULL THEN NULLIF(contract_week_row.uploaded_pdf_r2_key, '') END) IS NOT NULL THEN 'document'
        ELSE NULL::text
      END AS primary_artifact_preview_mode

    FROM source_rows
    LEFT JOIN public.timesheet_summary_pay_state_cache AS summary_pay_cache
      ON summary_pay_cache.timesheet_id = source_rows.timesheet_id
    LEFT JOIN public.timesheets AS timesheet_row
      ON timesheet_row.timesheet_id = source_rows.timesheet_id
     AND timesheet_row.is_current = TRUE
    LEFT JOIN public.contract_weeks AS contract_week_row
      ON contract_week_row.id = source_rows.contract_week_id
    LEFT JOIN public.timesheets_financials AS financial_row
      ON financial_row.timesheet_id = source_rows.timesheet_id
     AND financial_row.is_current = TRUE
    LEFT JOIN LATERAL (
      SELECT
        COUNT(timesheet_evidence_row.id)::integer AS attached_evidence_count,
        (ARRAY_AGG(
          timesheet_evidence_row.storage_key
          ORDER BY
            (UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'TIMESHEET') DESC,
            timesheet_evidence_row.created_at DESC,
            timesheet_evidence_row.id DESC
        ))[1] AS primary_storage_key,
        (ARRAY_AGG(
          COALESCE(NULLIF(timesheet_evidence_row.display_name, ''), timesheet_evidence_row.kind, 'Evidence')
          ORDER BY
            (UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'TIMESHEET') DESC,
            timesheet_evidence_row.created_at DESC,
            timesheet_evidence_row.id DESC
        ))[1] AS primary_display_name
      FROM public.timesheet_evidence AS timesheet_evidence_row
      WHERE timesheet_evidence_row.timesheet_id = source_rows.timesheet_id
    ) AS evidence_summary ON TRUE
  ),
  filtered AS MATERIALIZED (
    SELECT enriched_row.*
    FROM enriched AS enriched_row
    WHERE
      (
        NOT v_has_lookup_filter
        OR (
          v_lookup_ids IS NOT NULL
          AND (
            enriched_row.timesheet_id = ANY(v_lookup_ids)
            OR enriched_row.contract_week_id = ANY(v_lookup_ids)
          )
        )
      )
      AND (
        NOT v_has_timesheet_filter
        OR (
          v_timesheet_ids_filter IS NOT NULL
          AND enriched_row.timesheet_id = ANY(v_timesheet_ids_filter)
        )
      )
      AND (
        NOT v_has_contract_week_filter
        OR (
          v_contract_week_ids_filter IS NOT NULL
          AND enriched_row.contract_week_id = ANY(v_contract_week_ids_filter)
        )
      )
      AND (
        NOT v_has_candidate_filter
        OR enriched_row.candidate_id = v_candidate_id_filter
      )
      AND (
        NOT v_has_client_filter
        OR enriched_row.client_id = v_client_id_filter
      )
      AND (
        v_week_ending_from IS NULL
        OR enriched_row.week_ending_date >= v_week_ending_from
      )
      AND (
        v_week_ending_to IS NULL
        OR enriched_row.week_ending_date <= v_week_ending_to
      )
      AND (
        v_q IS NULL
        OR LOWER(COALESCE(enriched_row.candidate_name, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(enriched_row.candidate_display_name, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(enriched_row.client_name, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(enriched_row.booking_id, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(enriched_row.occupant_key_norm, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(enriched_row.hospital_norm, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(enriched_row.route_display, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(enriched_row.processing_status_display, '')) LIKE '%' || v_q || '%'
      )
      AND (
        v_tools_stage IS NULL
        OR LOWER(COALESCE(enriched_row.tools_stage, '')) = v_tools_stage
      )
      AND (
        v_route_type IS NULL
        OR (
          v_route_type = 'electronic'
          AND UPPER(COALESCE(enriched_row.route_type, '')) IN ('DAILY_ELECTRONIC', 'WEEKLY_ELECTRONIC')
        )
        OR (
          v_route_type = 'manual'
          AND (
            UPPER(COALESCE(enriched_row.route_type, '')) IN ('DAILY_MANUAL', 'WEEKLY_MANUAL')
            OR UPPER(COALESCE(enriched_row.route_family, '')) = 'MANUAL'
          )
        )
        OR (
          v_route_type = 'nhsp'
          AND (
            UPPER(COALESCE(enriched_row.route_type, '')) IN ('WEEKLY_NHSP', 'NHSP')
            OR UPPER(COALESCE(enriched_row.route_family, '')) = 'NHSP'
          )
        )
        OR (
          v_route_type = 'healthroster'
          AND (
            UPPER(COALESCE(enriched_row.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
            OR UPPER(COALESCE(enriched_row.route_family, '')) = 'HEALTHROSTER'
          )
        )
        OR (
          v_route_type = 'qr'
          AND COALESCE(enriched_row.is_qr, FALSE) = TRUE
        )
        OR (
          v_route_type IN ('no_timesheet_required', 'no-timesheet-required')
          AND (
            UPPER(COALESCE(enriched_row.route_type, '')) = 'NO_TIMESHEET_REQUIRED'
            OR UPPER(COALESCE(enriched_row.route_family, '')) = 'NO_TIMESHEET_REQUIRED'
            OR COALESCE(enriched_row.client_no_timesheet_required, FALSE) = TRUE
          )
        )
        OR LOWER(COALESCE(enriched_row.route_type, '')) = v_route_type
        OR LOWER(COALESCE(enriched_row.route_family, '')) = v_route_type
      )
      AND (
        v_sheet_scope IS NULL
        OR LOWER(COALESCE(enriched_row.sheet_scope, '')) = v_sheet_scope
      )
      AND (
        v_qr_status IS NULL
        OR LOWER(COALESCE(enriched_row.qr_status, '')) = v_qr_status
      )
      AND (
        v_status_code IS NULL
        OR (
          v_status_code = 'no_match_id'
          AND (enriched_row.candidate_id IS NULL OR enriched_row.client_id IS NULL)
        )
        OR (
          v_status_code = 'rate_missing'
          AND enriched_row.has_rate_issue
        )
        OR (
          v_status_code IN ('pay_chan_miss', 'pay_channel_missing')
          AND enriched_row.has_pay_channel_issue
        )
        OR (
          v_status_code = 'ready_for_hr'
          AND UPPER(COALESCE(enriched_row.processing_status, '')) = 'READY_FOR_HR'
        )
        OR (
          v_status_code = 'ready_for_inv'
          AND UPPER(COALESCE(enriched_row.processing_status, '')) = 'READY_FOR_INVOICE'
        )
        OR LOWER(COALESCE(enriched_row.processing_status, '')) = v_status_code
        OR LOWER(COALESCE(enriched_row.summary_stage, '')) = v_status_code
        OR LOWER(COALESCE(enriched_row.tools_stage, '')) = v_status_code
      )
      AND (
        v_candidate_paid IS NULL
        OR (
          (
            UPPER(COALESCE(enriched_row.pay_status_code, '')) IN ('PAID','PARTIALLY_PAID','OVERPAID')
            OR enriched_row.pay_paid_at_utc IS NOT NULL
            OR enriched_row.paid_at_utc IS NOT NULL
          ) = v_candidate_paid
        )
      )
      AND (
        v_is_adjusted IS NULL
        OR enriched_row.is_adjusted = v_is_adjusted
      )
      AND (
        v_is_qr IS NULL
        OR enriched_row.is_qr = v_is_qr
      )
      AND (
        v_hr_issue IS NULL
        OR (
          (
            COALESCE(ARRAY_LENGTH(enriched_row.hr_crosscheck_issues, 1), 0) > 0
            OR (
              enriched_row.hr_crosscheck_status IS NOT NULL
              AND UPPER(enriched_row.hr_crosscheck_status) NOT IN ('OK', 'MATCHED', 'MATCH', 'VALID', 'PASSED', 'CLEAR')
            )
          ) = v_hr_issue
        )
      )
      AND (
        v_hr_issue_token IS NULL
        OR EXISTS (
          SELECT 1
          FROM UNNEST(COALESCE(enriched_row.hr_crosscheck_issues, ARRAY[]::text[])) AS hr_issue_value(issue_code)
          WHERE UPPER(COALESCE(hr_issue_value.issue_code, '')) = v_hr_issue_token
        )
      )
      AND (
        v_issues_filter IS NULL
        OR (
          v_issues_filter IN ('all', 'any')
          AND (enriched_row.needs_attention OR COALESCE(ARRAY_LENGTH(enriched_row.business_issue_codes, 1), 0) > 0)
        )
        OR (
          v_issues_filter IN ('none', 'clear')
          AND (NOT enriched_row.needs_attention AND COALESCE(ARRAY_LENGTH(enriched_row.business_issue_codes, 1), 0) = 0)
        )
        OR (
          v_issues_filter IN ('rate', 'rates', 'rate_missing')
          AND (
            enriched_row.has_rate_issue
            OR EXISTS (
              SELECT 1
              FROM UNNEST(COALESCE(enriched_row.business_issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
              WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('RATE', 'RATE MISSING')
            )
          )
        )
        OR (
          v_issues_filter IN ('pay', 'pay_channel', 'pay-channel', 'pay_chan_miss', 'pay_channel_missing')
          AND (
            enriched_row.has_pay_channel_issue
            OR EXISTS (
              SELECT 1
              FROM UNNEST(COALESCE(enriched_row.business_issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
              WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('PAY CHANNEL', 'PAY CHANNEL MISSING')
            )
          )
        )
        OR (
          v_issues_filter IN ('hr', 'hr_issue', 'hr-issue', 'awaiting_hr_validation', 'awaiting_hr_validation_required')
          AND (
            COALESCE(ARRAY_LENGTH(enriched_row.hr_crosscheck_issues, 1), 0) > 0
            OR UPPER(COALESCE(enriched_row.tools_stage, '')) = 'AWAITING_HR_VALIDATION'
            OR EXISTS (
              SELECT 1
              FROM UNNEST(COALESCE(enriched_row.business_issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
              WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('HR VALIDATION', 'AWAITING HR VALIDATION')
            )
            OR (
              enriched_row.hr_crosscheck_status IS NOT NULL
              AND UPPER(enriched_row.hr_crosscheck_status) NOT IN ('OK', 'MATCHED', 'MATCH', 'VALID', 'PASSED', 'CLEAR')
            )
          )
        )
        OR (
          v_issues_filter IN ('hr_hours_mismatch', 'hours_mismatch_hr')
          AND EXISTS (
            SELECT 1
            FROM UNNEST(COALESCE(enriched_row.business_issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) IN ('HOURS MISMATCH HR', 'HOURS MISMATCH (HEALTHROSTER)')
          )
        )
        OR (
          v_issues_filter = 'hr_hours_missing'
          AND EXISTS (
            SELECT 1
            FROM UNNEST(COALESCE(enriched_row.business_issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'HR HOURS MISSING'
          )
        )
        OR (
          v_issues_filter = 'duplicate_contracts'
          AND EXISTS (
            SELECT 1
            FROM UNNEST(COALESCE(enriched_row.business_issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE UPPER(COALESCE(issue_value.issue_code, '')) = 'DUPLICATE CONTRACTS'
          )
        )
        OR (
          v_issues_filter IN ('timesheet_evidence', 'expenses_evidence', 'mileage_evidence', 'reference_missing', 'validation', 'authorisation', 'on_hold', 'refs_pdf_invalid')
          AND EXISTS (
            SELECT 1
            FROM UNNEST(COALESCE(enriched_row.business_issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
            WHERE
              (v_issues_filter = 'timesheet_evidence' AND UPPER(COALESCE(issue_value.issue_code, '')) = 'TIMESHEET EVIDENCE MISSING')
              OR (v_issues_filter = 'expenses_evidence' AND UPPER(COALESCE(issue_value.issue_code, '')) = 'EXPENSES EVIDENCE MISSING')
              OR (v_issues_filter = 'mileage_evidence' AND UPPER(COALESCE(issue_value.issue_code, '')) = 'MILEAGE EVIDENCE MISSING')
              OR (v_issues_filter = 'reference_missing' AND UPPER(COALESCE(issue_value.issue_code, '')) IN ('REFERENCE', 'REFERENCE MISSING'))
              OR (v_issues_filter = 'validation' AND UPPER(COALESCE(issue_value.issue_code, '')) = 'VALIDATION')
              OR (v_issues_filter = 'authorisation' AND UPPER(COALESCE(issue_value.issue_code, '')) IN ('AUTHORISATION', 'AWAITING AUTHORISATION'))
              OR (v_issues_filter = 'on_hold' AND UPPER(COALESCE(issue_value.issue_code, '')) = 'ON HOLD')
              OR (v_issues_filter = 'refs_pdf_invalid' AND UPPER(COALESCE(issue_value.issue_code, '')) = 'REFS - TIMESHEET PDF INVALID')
          )
        )
        OR (
          v_issues_filter = 'qr_not_issued'
          AND enriched_row.timesheet_id IS NOT NULL
          AND UPPER(COALESCE(enriched_row.qr_status, '')) = 'PENDING'
          AND COALESCE(enriched_row.qr_token, '') = ''
          AND enriched_row.qr_generated_at IS NULL
        )
        OR (
          v_issues_filter IN ('qr_awaiting_signature', 'qr_issued_awaiting_signature')
          AND enriched_row.timesheet_id IS NOT NULL
          AND UPPER(COALESCE(enriched_row.qr_status, '')) = 'PENDING'
          AND COALESCE(enriched_row.qr_token, '') <> ''
          AND enriched_row.qr_generated_at IS NOT NULL
          AND enriched_row.qr_scanned_at IS NULL
        )
        OR EXISTS (
          SELECT 1
          FROM UNNEST(COALESCE(enriched_row.business_issue_codes, ARRAY[]::text[])) AS issue_value(issue_code)
          WHERE LOWER(issue_value.issue_code) = v_issues_filter
        )
      )
  )
  SELECT
    filtered_row.timesheet_id,
    filtered_row.contract_week_id,
    filtered_row.contract_id,
    filtered_row.candidate_id,
    filtered_row.candidate_name,
    filtered_row.candidate_display_name,
    filtered_row.client_id,
    filtered_row.client_name,
    filtered_row.booking_id,
    filtered_row.occupant_key_norm,
    filtered_row.hospital_norm,
    filtered_row.candidate_hint_text,
    filtered_row.week_ending_date,
    filtered_row.work_date,
    filtered_row.sheet_scope,
    filtered_row.submission_mode,
    filtered_row.submission_mode_snapshot,
    filtered_row.basis,
    filtered_row.route_type,
    filtered_row.route_display,
    filtered_row.route_family,
    filtered_row.route_subfamily,
    filtered_row.underlying_channel_family,
    filtered_row.summary_stage,
    filtered_row.tools_stage,
    filtered_row.processing_status,
    filtered_row.processing_status_display,
    filtered_row.authorised_at_utc,
    filtered_row.authorised_at_server,
    filtered_row.processed_at_utc,
    filtered_row.is_authorised,
    filtered_row.total_hours,
    filtered_row.total_pay_ex_vat,
    filtered_row.total_charge_ex_vat,
    filtered_row.margin_ex_vat,
    filtered_row.net_delta_ex_vat,
    filtered_row.paid_at_utc,
    filtered_row.pay_icon_code,
    filtered_row.pay_status_code,
    filtered_row.pay_paid_at_utc,
    filtered_row.invoice_is_paid,
    filtered_row.invoice_issue_stage,
    filtered_row.invoice_segment_stage,
    filtered_row.invoice_segments_total,
    filtered_row.invoice_segments_locked,
    filtered_row.invoice_segments_unlocked,
    filtered_row.issue_codes,
    filtered_row.validation_status,
    filtered_row.validation_summary,
    filtered_row.hr_crosscheck_status,
    filtered_row.hr_crosscheck_issues,
    filtered_row.qr_status,
    filtered_row.is_qr,
    filtered_row.is_adjusted,
    filtered_row.needs_attention,
    filtered_row.has_rate_issue,
    filtered_row.has_pay_channel_issue,
    filtered_row.client_no_timesheet_required,
    filtered_row.client_autoprocess_hr,
    filtered_row.client_is_nhsp,
    filtered_row.has_any_evidence,
    filtered_row.attached_evidence_count,
    filtered_row.primary_artifact_storage_key,
    filtered_row.primary_artifact_display_name,
    filtered_row.primary_artifact_preview_mode
  FROM filtered AS filtered_row
  ORDER BY
    CASE WHEN v_order_by IN ('candidate_name', 'candidate') AND v_order_dir = 'asc' THEN filtered_row.candidate_name END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('candidate_name', 'candidate') AND v_order_dir = 'desc' THEN filtered_row.candidate_name END DESC NULLS LAST,

    CASE WHEN v_order_by IN ('client_name', 'client') AND v_order_dir = 'asc' THEN filtered_row.client_name END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('client_name', 'client') AND v_order_dir = 'desc' THEN filtered_row.client_name END DESC NULLS LAST,

    CASE WHEN v_order_by IN ('week_ending_date', 'week_ending', 'date') AND v_order_dir = 'asc' THEN filtered_row.week_ending_date END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('week_ending_date', 'week_ending', 'date') AND v_order_dir = 'desc' THEN filtered_row.week_ending_date END DESC NULLS LAST,

    CASE WHEN v_order_by = 'work_date' AND v_order_dir = 'asc' THEN filtered_row.work_date END ASC NULLS LAST,
    CASE WHEN v_order_by = 'work_date' AND v_order_dir = 'desc' THEN filtered_row.work_date END DESC NULLS LAST,

    CASE WHEN v_order_by IN ('processing_status', 'status') AND v_order_dir = 'asc' THEN filtered_row.processing_status END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('processing_status', 'status') AND v_order_dir = 'desc' THEN filtered_row.processing_status END DESC NULLS LAST,

    CASE WHEN v_order_by = 'tools_stage' AND v_order_dir = 'asc' THEN filtered_row.tools_stage END ASC NULLS LAST,
    CASE WHEN v_order_by = 'tools_stage' AND v_order_dir = 'desc' THEN filtered_row.tools_stage END DESC NULLS LAST,

    CASE WHEN v_order_by = 'route_type' AND v_order_dir = 'asc' THEN filtered_row.route_type END ASC NULLS LAST,
    CASE WHEN v_order_by = 'route_type' AND v_order_dir = 'desc' THEN filtered_row.route_type END DESC NULLS LAST,

    CASE WHEN v_order_by = 'sheet_scope' AND v_order_dir = 'asc' THEN filtered_row.sheet_scope END ASC NULLS LAST,
    CASE WHEN v_order_by = 'sheet_scope' AND v_order_dir = 'desc' THEN filtered_row.sheet_scope END DESC NULLS LAST,

    CASE WHEN v_order_by IN ('total_pay_ex_vat', 'pay') AND v_order_dir = 'asc' THEN filtered_row.total_pay_ex_vat END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('total_pay_ex_vat', 'pay') AND v_order_dir = 'desc' THEN filtered_row.total_pay_ex_vat END DESC NULLS LAST,

    CASE WHEN v_order_by IN ('total_charge_ex_vat', 'charge') AND v_order_dir = 'asc' THEN filtered_row.total_charge_ex_vat END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('total_charge_ex_vat', 'charge') AND v_order_dir = 'desc' THEN filtered_row.total_charge_ex_vat END DESC NULLS LAST,

    CASE WHEN v_order_by IN ('margin_ex_vat', 'margin') AND v_order_dir = 'asc' THEN filtered_row.margin_ex_vat END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('margin_ex_vat', 'margin') AND v_order_dir = 'desc' THEN filtered_row.margin_ex_vat END DESC NULLS LAST,

    filtered_row.candidate_name ASC NULLS LAST,
    filtered_row.week_ending_date DESC NULLS LAST,
    filtered_row.work_date DESC NULLS LAST,
    filtered_row.timesheet_id NULLS LAST,
    filtered_row.contract_week_id NULLS LAST
  LIMIT v_limit
  OFFSET v_offset;
END;
$function$;

COMMIT;
