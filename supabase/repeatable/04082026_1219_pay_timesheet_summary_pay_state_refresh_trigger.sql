-- Banking Pay bounded-scope Version 1.2.4
-- Exact installed TEST baseline; intentionally replaced in place by exact identity.
-- Policy X: pre-draft freshness/orchestration only; frozen post-draft authority is unchanged.

-- -----------------------------------------------------------------------------
-- public.pay_timesheet_summary_pay_state_refresh_trigger()
-- Installed pg_get_functiondef MD5: 6a7109587473360a0269135f98a2559f
-- -----------------------------------------------------------------------------
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
  v_diag_started_at timestamptz := clock_timestamp();
  v_chunk_started_at timestamptz;
  v_refresh_result jsonb := '{}'::jsonb;
  v_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_invalidation_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_scope_invalidation_result jsonb := '{}'::jsonb;
  v_correction_dirty_contexts jsonb := '{}'::jsonb;
  v_scope_change_tx_token uuid := NULL::uuid;
  v_correction_request_id uuid := NULL::uuid;
  v_correction_pay_batch_id uuid := NULL::uuid;
  v_correction_operation_id uuid := NULL::uuid;
  v_correction_request_status text := NULL::text;
  v_correction_selection_json jsonb := '{}'::jsonb;
  v_correction_plan_json jsonb := '{}'::jsonb;
  v_correction_lifecycle_phase text := NULL::text;
BEGIN

  PERFORM public._temp_diag_log(
    'TEMP_SUMMARY_REFRESH_STAGE',
    'TEMP_SUMMARY_REFRESH',
    NULL::text,
    jsonb_build_object(
      'function_name', 'pay_timesheet_summary_pay_state_refresh_trigger',
      'stage', 'entry',
      'trigger_table', TG_TABLE_NAME,
      'trigger_op', TG_OP,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF COALESCE(current_setting('cloudtms.lifecycle_defer_summary_refresh', true), '') = 'on'
     AND TG_TABLE_NAME IN ('timesheets', 'timesheets_financials') THEN
    PERFORM public._temp_diag_log(
      'TEMP_SUMMARY_REFRESH_STAGE',
      'TEMP_SUMMARY_REFRESH',
      NULL::text,
      jsonb_build_object(
        'function_name', 'pay_timesheet_summary_pay_state_refresh_trigger',
        'stage', CASE
          WHEN LOWER(BTRIM(COALESCE(current_setting('cloudtms.lifecycle_mutation_context', true), ''))) = 'manual_timesheet_save'
            THEN 'deferred_by_manual_save_context'
          ELSE 'deferred_by_lifecycle_context'
        END,
        'trigger_table', TG_TABLE_NAME,
        'trigger_op', TG_OP,
        'mutation_context', NULLIF(current_setting('cloudtms.lifecycle_mutation_context', true), ''),
        'summary_refresh_mode', NULLIF(current_setting('cloudtms.summary_refresh_mode', true), ''),
        'target_timesheet_id', NULLIF(current_setting('cloudtms.lifecycle_target_timesheet_id', true), ''),
        'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
      )
    );
    RETURN NULL;
  END IF;

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
        WHERE (
             new_rows.timesheet_id IS DISTINCT FROM old_rows.timesheet_id
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
           )
          AND (
             new_rows.timesheet_id IS DISTINCT FROM old_rows.timesheet_id
             OR new_rows.is_current IS DISTINCT FROM old_rows.is_current
             OR new_rows.authorised_at_utc IS NOT NULL
             OR old_rows.authorised_at_utc IS NOT NULL
             OR new_rows.pay_on_hold IS DISTINCT FROM old_rows.pay_on_hold
             OR new_rows.paid_at_utc IS DISTINCT FROM old_rows.paid_at_utc
           )
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

  IF v_total>0 AND NOT (
       to_regclass('pg_temp._bpay_wb_sync_context_v1') IS NOT NULL
       AND COALESCE(current_setting('cloudtms.pay_workbench_overpayment_sync_token',true),'')
         ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     ) THEN
    WITH resolved AS (
      SELECT DISTINCT COALESCE(scope_state.candidate_id,current_financial.candidate_id) candidate_id,
             input_id.timesheet_id
      FROM unnest(v_timesheet_ids) input_id(timesheet_id)
      LEFT JOIN private.banking_pay_workbench_timesheet_scope_state scope_state
        ON scope_state.timesheet_id=input_id.timesheet_id
      LEFT JOIN LATERAL (
        SELECT financial.candidate_id FROM public.timesheets_financials financial
        WHERE financial.timesheet_id=input_id.timesheet_id AND financial.is_current
        ORDER BY financial.id DESC LIMIT 1
      ) current_financial ON true
      WHERE COALESCE(scope_state.candidate_id,current_financial.candidate_id) IS NOT NULL
    )
    SELECT array_agg(candidate_id ORDER BY candidate_id,timesheet_id),
           array_agg(timesheet_id ORDER BY candidate_id,timesheet_id)
    INTO v_candidate_ids,v_invalidation_timesheet_ids FROM resolved;

    -- The correction-request transition relation is the strongest causal
    -- authority available: it proves that this exact statement, rather than a
    -- caller hint, caused the summary refresh.  Establish the candidate-keyed
    -- transaction-local envelope here.  The operation is intentionally
    -- optional on INSERT because request_start creates it immediately after the
    -- request row; the asynchronous dirty job cannot run before that commit.
    IF TG_TABLE_NAME='pay_payment_correction_requests'
       AND TG_OP IN ('INSERT','UPDATE')
       AND pg_catalog.cardinality(COALESCE(v_candidate_ids,ARRAY[]::uuid[]))>0 THEN
      IF TG_OP='INSERT' THEN
        SELECT request_transition.id,request_transition.pay_batch_id,
               request_transition.status,
               COALESCE(request_transition.selection_json,'{}'::jsonb),
               COALESCE(request_transition.plan_json,'{}'::jsonb)
        INTO v_correction_request_id,v_correction_pay_batch_id,
             v_correction_request_status,v_correction_selection_json,
             v_correction_plan_json
        FROM new_rows AS request_transition
        WHERE request_transition.id IS NOT NULL
        ORDER BY request_transition.id
        LIMIT 1;
      ELSE
        SELECT new_request.id,new_request.pay_batch_id,new_request.status,
               COALESCE(new_request.selection_json,'{}'::jsonb),
               COALESCE(new_request.plan_json,'{}'::jsonb)
        INTO v_correction_request_id,v_correction_pay_batch_id,
             v_correction_request_status,v_correction_selection_json,
             v_correction_plan_json
        FROM new_rows AS new_request
        JOIN old_rows AS old_request ON old_request.id=new_request.id
        WHERE new_request.pay_batch_id IS DISTINCT FROM old_request.pay_batch_id
           OR new_request.status IS DISTINCT FROM old_request.status
        ORDER BY new_request.id
        LIMIT 1;
      END IF;

      IF v_correction_request_id IS NOT NULL
         AND v_correction_pay_batch_id IS NOT NULL THEN
        SELECT operation_row.id
        INTO v_correction_operation_id
        FROM public.banking_pay_operations AS operation_row
        WHERE operation_row.operation_type='PAYMENT_CORRECTION'
          AND operation_row.input_json->>'correction_request_id'
                =v_correction_request_id::text
        ORDER BY operation_row.created_at_utc DESC,operation_row.id DESC
        LIMIT 1;

        v_correction_lifecycle_phase:=CASE
          WHEN pg_catalog.upper(pg_catalog.btrim(COALESCE(
                 v_correction_request_status,''
               ))) IN ('AUTHORISED','EXPANDED','PROCESSING','APPLIED','APPLIED_WITH_BLOCKERS')
            THEN 'REQUEST_START'
          ELSE 'REQUEST_PREPARE'
        END;

        PERFORM private.pay_workbench_correction_dirty_context_set_v1(
          p_correction_request_id:=v_correction_request_id,
          p_pay_batch_id:=v_correction_pay_batch_id,
          p_candidate_ids:=v_candidate_ids,
          p_lifecycle_phase:=v_correction_lifecycle_phase,
          p_policy_x_boundary:='POST_DRAFT_FROZEN_EVIDENCE',
          p_pre_request_authorities_json:=COALESCE(
            v_correction_selection_json->'draft_overlay_fast_pre_request_authorities',
            '{}'::jsonb
          ),
          p_operation_id:=v_correction_operation_id,
          p_work_item_id:=NULL::uuid,
          p_options_json:=pg_catalog.jsonb_build_object(
            'trigger_table',TG_TABLE_NAME,
            'trigger_operation',TG_OP,
            'request_status',v_correction_request_status,
            'requested_action',v_correction_plan_json->>'requested_action'
          )
        );
      END IF;
    END IF;

    -- A correction request may affect a subset of a statement.  Copy its exact
    -- candidate-keyed causal envelope into the durable invalidation payload,
    -- while retaining every other candidate in the normal invalidation.  A
    -- mixed statement must never suppress an unrelated economic change.
    IF pg_catalog.to_regclass('pg_temp._bpay_wb_correction_dirty_context_v1') IS NOT NULL
       AND pg_catalog.cardinality(COALESCE(v_candidate_ids,ARRAY[]::uuid[]))>0 THEN
      EXECUTE $context$
        SELECT COALESCE(
          pg_catalog.jsonb_object_agg(
            context_row.candidate_id::text,
            pg_catalog.to_jsonb(context_row)-'created_at_utc'
            ORDER BY context_row.candidate_id
          ),
          '{}'::jsonb
        )
        FROM pg_temp._bpay_wb_correction_dirty_context_v1 AS context_row
        WHERE context_row.candidate_id=ANY($1)
      $context$
      INTO v_correction_dirty_contexts
      USING v_candidate_ids;

      IF v_correction_dirty_contexts<>'{}'::jsonb THEN
        v_scope_change_tx_token:=public.pay_workbench_scope_change_tx_token_v1();
      END IF;
    END IF;

    IF cardinality(COALESCE(v_candidate_ids,ARRAY[]::uuid[]))>0 THEN
      v_scope_invalidation_result:=private.pay_workbench_scope_invalidate_v1(
        v_candidate_ids,v_invalidation_timesheet_ids,
        'DIRTY_TRIGGER:'||upper(TG_TABLE_NAME)||':'||TG_OP,v_scope_change_tx_token,
        jsonb_strip_nulls(jsonb_build_object(
          'trigger_table',TG_TABLE_NAME,
          'trigger_operation',TG_OP,
          'correction_dirty_contexts',CASE
            WHEN v_correction_dirty_contexts<>'{}'::jsonb
              THEN v_correction_dirty_contexts ELSE NULL::jsonb END,
          'request_owned_scope_change_tx_token',CASE
            WHEN v_correction_dirty_contexts<>'{}'::jsonb
              THEN v_scope_change_tx_token ELSE NULL::uuid END,
          'correction_dirty_causal_contract_version',CASE
            WHEN v_correction_dirty_contexts<>'{}'::jsonb
              THEN 'CORRECTION_OWNED_DIRTY_CAUSAL_V1' ELSE NULL::text END
        ))
      );
    END IF;
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_SUMMARY_REFRESH_STAGE',
    'TEMP_SUMMARY_REFRESH',
    CASE WHEN v_total > 0 THEN v_timesheet_ids[1]::text ELSE NULL::text END,
    jsonb_build_object(
      'function_name', 'pay_timesheet_summary_pay_state_refresh_trigger',
      'stage', 'timesheet_ids_collected',
      'trigger_table', TG_TABLE_NAME,
      'trigger_op', TG_OP,
      'targeted_timesheet_count', v_total,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  v_offset := 1;

  WHILE v_offset <= v_total LOOP
    SELECT COALESCE(
      ARRAY_AGG(chunk_rows.timesheet_id ORDER BY chunk_rows.ordinality),
      ARRAY[]::uuid[]
    )
    INTO v_chunk_ids
    FROM UNNEST(v_timesheet_ids) WITH ORDINALITY AS chunk_rows(timesheet_id, ordinality)
    WHERE chunk_rows.ordinality BETWEEN v_offset AND v_offset + 99;

    v_chunk_started_at := clock_timestamp();

    PERFORM public._temp_diag_log(
      'TEMP_SUMMARY_REFRESH_STAGE',
      'TEMP_SUMMARY_REFRESH',
      CASE WHEN COALESCE(CARDINALITY(v_chunk_ids), 0) > 0 THEN v_chunk_ids[1]::text ELSE NULL::text END,
      jsonb_build_object(
        'function_name', 'pay_timesheet_summary_pay_state_refresh_trigger',
        'stage', 'chunk_refresh_start',
        'trigger_table', TG_TABLE_NAME,
        'trigger_op', TG_OP,
        'targeted_timesheet_count', COALESCE(CARDINALITY(v_chunk_ids), 0),
        'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
      )
    );

    v_refresh_result := public.pay_timesheet_summary_pay_state_refresh(
      p_timesheet_ids => v_chunk_ids,
      p_actor_user_id => NULL::uuid
    );

    PERFORM public._temp_diag_log(
      'TEMP_SUMMARY_REFRESH_STAGE',
      'TEMP_SUMMARY_REFRESH',
      CASE WHEN COALESCE(CARDINALITY(v_chunk_ids), 0) > 0 THEN v_chunk_ids[1]::text ELSE NULL::text END,
      jsonb_build_object(
        'function_name', 'pay_timesheet_summary_pay_state_refresh_trigger',
        'stage', 'chunk_refresh_done',
        'trigger_table', TG_TABLE_NAME,
        'trigger_op', TG_OP,
        'targeted_timesheet_count', COALESCE(CARDINALITY(v_chunk_ids), 0),
        'refresh_result', COALESCE(v_refresh_result, '{}'::jsonb),
        'chunk_elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_chunk_started_at)) * 1000)::numeric, 2),
        'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
      )
    );

    v_offset := v_offset + 100;
  END LOOP;

  PERFORM public._temp_diag_log(
    'TEMP_SUMMARY_REFRESH_STAGE',
    'TEMP_SUMMARY_REFRESH',
    CASE WHEN v_total > 0 THEN v_timesheet_ids[1]::text ELSE NULL::text END,
    jsonb_build_object(
      'function_name', 'pay_timesheet_summary_pay_state_refresh_trigger',
      'stage', 'return',
      'trigger_table', TG_TABLE_NAME,
      'trigger_op', TG_OP,
      'targeted_timesheet_count', v_total,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  RETURN NULL;
END;
$function$;

ALTER FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger() OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger() FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger() TO postgres;
GRANT EXECUTE ON FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger() TO authenticated;
GRANT EXECUTE ON FUNCTION public.pay_timesheet_summary_pay_state_refresh_trigger() TO service_role;
