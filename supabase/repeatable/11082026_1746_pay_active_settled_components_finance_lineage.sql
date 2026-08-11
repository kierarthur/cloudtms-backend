-- Restore settled recovery lineage when a historical finance item has no
-- direct timesheet_id but its immutable finance component remains linked to a
-- member of the requested timesheet rotation family. This keeps pre-Draft
-- freshness exact without changing post-Draft Policy X authority.

CREATE OR REPLACE FUNCTION public._pay_active_settled_components(
  p_timesheet_ids uuid[]
)
RETURNS TABLE(
  timesheet_id uuid,
  key_type text,
  key_value text,
  amount_ex_vat numeric,
  amount_inc_vat numeric
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
WITH input_timesheets AS (
  SELECT DISTINCT
    input_timesheet_values.timesheet_id_value AS timesheet_id
  FROM unnest(COALESCE(p_timesheet_ids, ARRAY[]::uuid[])) AS input_timesheet_values(timesheet_id_value)
  WHERE input_timesheet_values.timesheet_id_value IS NOT NULL
),
rotation_scope_rows AS (
  SELECT
    scope_rows.requested_timesheet_id,
    scope_rows.booking_id,
    scope_rows.canonical_timesheet_id,
    scope_rows.family_timesheet_id,
    scope_rows.family_is_current,
    scope_rows.family_version,
    scope_rows.requested_is_canonical
  FROM public._pay_timesheet_rotation_scope(
    (
      SELECT COALESCE(
        ARRAY_AGG(input_timesheets.timesheet_id ORDER BY input_timesheets.timesheet_id),
        ARRAY[]::uuid[]
      )
      FROM input_timesheets
    )
  ) AS scope_rows
),
rotation_scope_keyed AS (
  SELECT
    rotation_scope_rows.requested_timesheet_id,
    rotation_scope_rows.booking_id,
    rotation_scope_rows.canonical_timesheet_id,
    rotation_scope_rows.family_timesheet_id,
    rotation_scope_rows.family_is_current,
    rotation_scope_rows.family_version,
    rotation_scope_rows.requested_is_canonical,
    COALESCE(rotation_scope_rows.booking_id, rotation_scope_rows.requested_timesheet_id::text) AS scope_family_key
  FROM rotation_scope_rows
  WHERE rotation_scope_rows.requested_timesheet_id IS NOT NULL
),
projection_targets AS (
  SELECT
    rotation_scope_keyed.scope_family_key,
    COALESCE(
      (
        ARRAY_AGG(DISTINCT rotation_scope_keyed.canonical_timesheet_id ORDER BY rotation_scope_keyed.canonical_timesheet_id)
        FILTER (
          WHERE COALESCE(rotation_scope_keyed.requested_is_canonical, false) = true
            AND rotation_scope_keyed.canonical_timesheet_id IS NOT NULL
        )
      )[1],
      (
        ARRAY_AGG(DISTINCT rotation_scope_keyed.requested_timesheet_id ORDER BY rotation_scope_keyed.requested_timesheet_id)
        FILTER (WHERE rotation_scope_keyed.requested_timesheet_id IS NOT NULL)
      )[1],
      (
        ARRAY_AGG(DISTINCT rotation_scope_keyed.canonical_timesheet_id ORDER BY rotation_scope_keyed.canonical_timesheet_id)
        FILTER (WHERE rotation_scope_keyed.canonical_timesheet_id IS NOT NULL)
      )[1]
    ) AS projected_timesheet_id
  FROM rotation_scope_keyed
  GROUP BY rotation_scope_keyed.scope_family_key
),
family_to_projection AS (
  SELECT DISTINCT
    rotation_scope_keyed.family_timesheet_id,
    projection_targets.projected_timesheet_id
  FROM rotation_scope_keyed
  JOIN projection_targets
    ON projection_targets.scope_family_key = rotation_scope_keyed.scope_family_key
  WHERE rotation_scope_keyed.family_timesheet_id IS NOT NULL
    AND projection_targets.projected_timesheet_id IS NOT NULL
),
active_item_ids AS (
  SELECT DISTINCT
    public.pay_batch_items.id AS pay_batch_item_id,
    family_to_projection.projected_timesheet_id AS projected_timesheet_id
  FROM family_to_projection
  JOIN public.pay_batch_items
    ON public.pay_batch_items.timesheet_id = family_to_projection.family_timesheet_id
  JOIN public.pay_batch_candidates
    ON public.pay_batch_candidates.id = public.pay_batch_items.pay_batch_candidate_id
  LEFT JOIN public.pay_bank_transfers
    ON public.pay_bank_transfers.id = public.pay_batch_items.pay_bank_transfer_id
  WHERE COALESCE(public.pay_batch_items.is_voided, false) = false
    AND UPPER(BTRIM(COALESCE(public.pay_batch_items.item_type, ''))) IN (
      'SEGMENT_DELTA',
      'EXPENSE_DELTA',
      'ADJUSTMENT_DELTA',
      'MILEAGE_DELTA'
    )
    AND (
      UPPER(BTRIM(COALESCE(public.pay_batch_candidates.settlement_status, ''))) = 'SETTLED'
      OR public.pay_batch_candidates.settled_at_utc IS NOT NULL
      OR UPPER(BTRIM(COALESCE(public.pay_bank_transfers.status, ''))) = 'COMPLETED'
      OR public.pay_bank_transfers.completed_at_utc IS NOT NULL
      OR EXISTS (
        SELECT 1
        FROM public.timesheet_pay_state_history AS active_history
        WHERE active_history.pay_batch_id = public.pay_batch_candidates.pay_batch_id
          AND active_history.timesheet_id = public.pay_batch_items.timesheet_id
      )
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_payment_correction_items AS applied_corrections
      WHERE applied_corrections.pay_batch_item_id = public.pay_batch_items.id
        AND applied_corrections.status = 'APPLIED'
        AND applied_corrections.correction_item_kind IN (
          'PRE_BANK_CANCEL',
          'NO_MONEY_UNWIND',
          'SETTLED_REVERSAL'
        )
    )
),
active_item_id_array AS (
  SELECT
    CASE
      WHEN COUNT(*) = 0 THEN ARRAY['00000000-0000-0000-0000-000000000000'::uuid]
      ELSE ARRAY_AGG(active_item_ids.pay_batch_item_id ORDER BY active_item_ids.pay_batch_item_id)
    END AS pay_batch_item_ids
  FROM active_item_ids
),
active_components AS (
  SELECT
    active_item_ids.pay_batch_item_id,
    active_item_ids.projected_timesheet_id AS component_timesheet_id,
    economic_components.key_type AS component_key_type,
    economic_components.key_value AS component_key_value,
    economic_components.source_amount_ex_vat AS component_amount_ex_vat,
    economic_components.source_amount_inc_vat AS component_amount_inc_vat
  FROM active_item_id_array
  JOIN LATERAL public._pay_batch_item_economic_components(
    p_pay_batch_id => NULL::uuid,
    p_pay_batch_item_ids => active_item_id_array.pay_batch_item_ids
  ) AS economic_components
    ON true
  JOIN active_item_ids
    ON active_item_ids.pay_batch_item_id = economic_components.pay_batch_item_id
  WHERE active_item_ids.projected_timesheet_id IS NOT NULL
    AND economic_components.timesheet_id IS NOT NULL
    AND economic_components.key_type IS NOT NULL
    AND BTRIM(COALESCE(economic_components.key_type, '')) <> ''
    AND economic_components.key_value IS NOT NULL
    AND BTRIM(COALESCE(economic_components.key_value, '')) <> ''
    AND economic_components.key_resolution_failure_reason IS NULL
    AND UPPER(BTRIM(COALESCE(economic_components.item_type, ''))) IN (
      'SEGMENT_DELTA',
      'EXPENSE_DELTA',
      'ADJUSTMENT_DELTA',
      'MILEAGE_DELTA'
    )
    AND UPPER(BTRIM(COALESCE(economic_components.key_type, ''))) IN (
      'TS_DAY',
      'TS_TOTAL',
      'ADDITIONAL_CODE',
      'ADJUSTMENT_CODE',
      'EXPENSE_CODE'
    )
    AND NOT (
      UPPER(BTRIM(COALESCE(economic_components.key_type, ''))) = 'TS_DAY'
      AND economic_components.key_value !~ '^\d{4}-\d{2}-\d{2}$'
    )
),
active_components_by_item_key AS (
  SELECT
    active_components.pay_batch_item_id,
    active_components.component_timesheet_id AS timesheet_id,
    UPPER(BTRIM(active_components.component_key_type)) AS key_type,
    active_components.component_key_value AS key_value,
    ROUND(COALESCE(SUM(COALESCE(active_components.component_amount_ex_vat, 0)), 0), 2)::numeric AS amount_ex_vat,
    ROUND(COALESCE(SUM(COALESCE(active_components.component_amount_inc_vat, 0)), 0), 2)::numeric AS amount_inc_vat
  FROM active_components
  GROUP BY
    active_components.pay_batch_item_id,
    active_components.component_timesheet_id,
    UPPER(BTRIM(active_components.component_key_type)),
    active_components.component_key_value
),
settled_finance_reservation_summary AS (
  SELECT
    finance_reservation.pay_batch_item_id,
    BOOL_OR(
      UPPER(BTRIM(COALESCE(finance_reservation.status, ''))) = 'SETTLED'
      OR finance_reservation.settled_at_utc IS NOT NULL
    ) AS has_settled_reservation,
    ROUND(COALESCE(SUM(
      ABS(COALESCE(
        finance_reservation.reserved_source_amount,
        finance_reservation.reserved_amount,
        0
      ))
    ) FILTER (
      WHERE UPPER(BTRIM(COALESCE(finance_reservation.status, ''))) = 'SETTLED'
         OR finance_reservation.settled_at_utc IS NOT NULL
    ), 0), 2)::numeric AS settled_source_amount_ex_vat
  FROM public.pay_advance_reservations AS finance_reservation
  WHERE finance_reservation.pay_batch_item_id IS NOT NULL
  GROUP BY finance_reservation.pay_batch_item_id
),
settled_finance_movement_components AS (
  SELECT
    finance_item.id AS pay_batch_item_id,
    family_to_projection.projected_timesheet_id AS timesheet_id,
    UPPER(BTRIM(finance_item.frozen_component_key_type)) AS key_type,
    BTRIM(finance_item.frozen_component_key_value) AS key_value,
    ROUND(
      CASE
        WHEN UPPER(BTRIM(finance_item.item_type)) = 'OVERPAYMENT_RECOVERY' THEN -1
        ELSE 1
      END
      * finance_source_amount.source_amount_ex_vat,
      2
    )::numeric AS amount_ex_vat,
    ROUND(
      CASE
        WHEN UPPER(BTRIM(finance_item.item_type)) = 'OVERPAYMENT_RECOVERY' THEN -1
        ELSE 1
      END
      * COALESCE(
          NULLIF(ABS(finance_item.amount_inc_vat), 0),
          finance_source_amount.source_amount_ex_vat
        ),
      2
    )::numeric AS amount_inc_vat
  FROM public.pay_batch_items AS finance_item
  JOIN public.pay_batch_candidates AS finance_candidate
    ON finance_candidate.id = finance_item.pay_batch_candidate_id
  LEFT JOIN public.pay_bank_transfers AS finance_transfer
    ON finance_transfer.id = finance_item.pay_bank_transfer_id
  LEFT JOIN settled_finance_reservation_summary AS finance_reservation
    ON finance_reservation.pay_batch_item_id = finance_item.id
  LEFT JOIN public.pay_finance_case_components AS finance_component
    ON finance_component.id = finance_item.finance_component_id
  JOIN family_to_projection
    ON family_to_projection.family_timesheet_id = CASE
      WHEN finance_item.timesheet_id IS NOT NULL THEN finance_item.timesheet_id
      WHEN NULLIF(BTRIM(COALESCE(finance_item.frozen_source_basis_json->>'timesheet_id', '')), '')
        ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN (BTRIM(finance_item.frozen_source_basis_json->>'timesheet_id'))::uuid
      WHEN finance_component.linked_timesheet_id IS NOT NULL
        THEN finance_component.linked_timesheet_id
      ELSE NULL::uuid
    END
  CROSS JOIN LATERAL (
    SELECT ROUND(ABS(COALESCE(
      NULLIF(finance_item.frozen_source_amount, 0),
      NULLIF(finance_reservation.settled_source_amount_ex_vat, 0),
      NULLIF(finance_item.amount_ex_vat, 0),
      NULLIF(finance_item.amount_inc_vat, 0)
    )), 2)::numeric AS source_amount_ex_vat
  ) AS finance_source_amount
  WHERE COALESCE(finance_item.is_voided, false) = false
    AND UPPER(BTRIM(COALESCE(finance_item.item_type, ''))) IN (
      'OVERPAYMENT_RECOVERY',
      'UNDERPAYMENT_PAYMENT'
    )
    AND finance_item.frozen_component_key_type IS NOT NULL
    AND BTRIM(finance_item.frozen_component_key_type) <> ''
    AND finance_item.frozen_component_key_value IS NOT NULL
    AND BTRIM(finance_item.frozen_component_key_value) <> ''
    AND UPPER(BTRIM(finance_item.frozen_component_key_type)) IN (
      'TS_DAY',
      'TS_TOTAL',
      'ADDITIONAL_CODE',
      'ADJUSTMENT_CODE',
      'EXPENSE_CODE'
    )
    AND NOT (
      UPPER(BTRIM(finance_item.frozen_component_key_type)) = 'TS_DAY'
      AND finance_item.frozen_component_key_value !~ '^\d{4}-\d{2}-\d{2}$'
    )
    AND finance_source_amount.source_amount_ex_vat IS NOT NULL
    AND finance_source_amount.source_amount_ex_vat > 0
    AND (
      UPPER(BTRIM(COALESCE(finance_candidate.settlement_status, ''))) = 'SETTLED'
      OR finance_candidate.settled_at_utc IS NOT NULL
      OR UPPER(BTRIM(COALESCE(finance_transfer.status, ''))) = 'COMPLETED'
      OR finance_transfer.completed_at_utc IS NOT NULL
      OR COALESCE(finance_reservation.has_settled_reservation, false) = true
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_payment_correction_items AS applied_finance_correction
      WHERE applied_finance_correction.pay_batch_item_id = finance_item.id
        AND applied_finance_correction.status = 'APPLIED'
        AND applied_finance_correction.correction_item_kind IN (
          'PRE_BANK_CANCEL',
          'NO_MONEY_UNWIND',
          'SETTLED_REVERSAL'
        )
    )
),
all_settled_components_by_item_key AS (
  SELECT
    active_components_by_item_key.pay_batch_item_id,
    active_components_by_item_key.timesheet_id,
    active_components_by_item_key.key_type,
    active_components_by_item_key.key_value,
    active_components_by_item_key.amount_ex_vat,
    active_components_by_item_key.amount_inc_vat
  FROM active_components_by_item_key

  UNION ALL

  SELECT
    settled_finance_movement_components.pay_batch_item_id,
    settled_finance_movement_components.timesheet_id,
    settled_finance_movement_components.key_type,
    settled_finance_movement_components.key_value,
    settled_finance_movement_components.amount_ex_vat,
    settled_finance_movement_components.amount_inc_vat
  FROM settled_finance_movement_components
),
active_component_totals AS (
  SELECT
    all_settled_components_by_item_key.timesheet_id,
    all_settled_components_by_item_key.key_type,
    all_settled_components_by_item_key.key_value,
    ROUND(COALESCE(SUM(COALESCE(all_settled_components_by_item_key.amount_ex_vat, 0)), 0), 2)::numeric AS amount_ex_vat,
    ROUND(COALESCE(SUM(COALESCE(all_settled_components_by_item_key.amount_inc_vat, 0)), 0), 2)::numeric AS amount_inc_vat
  FROM all_settled_components_by_item_key
  GROUP BY
    all_settled_components_by_item_key.timesheet_id,
    all_settled_components_by_item_key.key_type,
    all_settled_components_by_item_key.key_value
)
SELECT
  active_component_totals.timesheet_id,
  active_component_totals.key_type,
  active_component_totals.key_value,
  active_component_totals.amount_ex_vat,
  active_component_totals.amount_inc_vat
FROM active_component_totals
WHERE active_component_totals.timesheet_id IS NOT NULL
  AND active_component_totals.key_type IS NOT NULL
  AND active_component_totals.key_value IS NOT NULL
  AND (
    ROUND(COALESCE(active_component_totals.amount_ex_vat, 0), 2) <> 0
    OR ROUND(COALESCE(active_component_totals.amount_inc_vat, 0), 2) <> 0
  )
ORDER BY
  active_component_totals.timesheet_id,
  active_component_totals.key_type,
  active_component_totals.key_value;
$function$;

ALTER FUNCTION public._pay_active_settled_components(uuid[]) OWNER TO postgres;
REVOKE ALL ON FUNCTION public._pay_active_settled_components(uuid[]) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public._pay_active_settled_components(uuid[]) TO authenticated, service_role, postgres;
