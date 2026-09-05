-- Bounded V8 finalizer continuation for large selected Candidate scopes.
-- Runtime authority is Miget TEST. This replacement changes orchestration only:
-- all reservation, payment, PAYE/Umbrella and summary-state owners/equations remain
-- the existing functions below. Legacy direct callers retain the prior all-summary behavior.

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
  v_bounded_v8 boolean := false;
  v_v8_unit_count integer := 0;
  v_candidate_scope_ordinal integer;
  v_finance_pending_before_count integer := 0;
  v_finance_remaining_count integer := 0;
  v_summary_after_timesheet_id uuid;
  v_summary_next_after_timesheet_id uuid;
  v_summary_processed_count integer := 0;
  v_summary_remaining_count integer := 0;
  v_summary_cursor_found boolean := false;
  v_summary_continuation boolean := false;
  v_prior_owner_result jsonb;
  v_prior_owner_receipt_sha256 text;
  v_prior_summary_remaining_count integer := 0;
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

  SELECT pg_catalog.count(*)::integer,
         pg_catalog.min(frozen_scope.candidate_scope_ordinal)
  INTO v_v8_unit_count, v_candidate_scope_ordinal
  FROM pg_temp.tmp_pay_batch_finalize_scope AS public_scope
  JOIN private.banking_pay_draft_frozen_candidate_scopes_v8 AS frozen_scope
    ON frozen_scope.operation_id = public_scope.operation_id
   AND frozen_scope.candidate_id = public_scope.candidate_id
   AND frozen_scope.resolved_pay_channel = public_scope.pay_channel
   AND frozen_scope.scope_digest_sha256 = public_scope.scope_hash
  JOIN private.banking_pay_draft_phase_units_v1 AS phase_unit
    ON phase_unit.operation_id = frozen_scope.operation_id
   AND phase_unit.candidate_scope_ordinal = frozen_scope.candidate_scope_ordinal
   AND phase_unit.phase = 'FINALISE_RESERVATIONS'
  WHERE frozen_scope.operation_id = p_operation_id
    AND frozen_scope.scope_state IN ('FROZEN','BATCH_LINKED','COMPLETE');

  v_bounded_v8 := v_scope_id_count = 1 AND v_v8_unit_count = 1;

  IF v_bounded_v8 THEN
    SELECT receipt_row.owner_result_json,
           receipt_row.receipt_digest_sha256
    INTO v_prior_owner_result, v_prior_owner_receipt_sha256
    FROM private.banking_pay_draft_owner_receipts_v1 AS receipt_row
    JOIN private.banking_pay_draft_phase_units_v1 AS phase_unit
      ON phase_unit.operation_id = receipt_row.operation_id
     AND phase_unit.phase = receipt_row.phase
     AND phase_unit.candidate_scope_ordinal = receipt_row.candidate_scope_ordinal
     AND phase_unit.last_owner_receipt_sha256 = receipt_row.receipt_digest_sha256
     AND phase_unit.next_owner_iteration = receipt_row.owner_iteration + 1
    WHERE receipt_row.operation_id = p_operation_id
      AND receipt_row.phase = 'FINALISE_RESERVATIONS'
      AND receipt_row.candidate_scope_ordinal = v_candidate_scope_ordinal
      AND receipt_row.delegated_owner_identity =
        'public.pay_batch_finalize_reservations_and_markers(uuid,text,uuid,date,date,uuid,jsonb)'
      AND receipt_row.owner_has_more
    ORDER BY receipt_row.owner_iteration DESC
    LIMIT 1;

    IF v_prior_owner_result IS NOT NULL THEN
      IF jsonb_typeof(v_prior_owner_result) <> 'object'
         OR COALESCE(v_prior_owner_result->>'bounded_summary_page','') <> 'true'
         OR COALESCE(v_prior_owner_result->>'reservation_remaining_count','') !~ '^[0-9]+$'
         OR COALESCE(v_prior_owner_result->>'reservation_pending_before_count','') !~ '^[0-9]+$'
         OR COALESCE(v_prior_owner_result->>'summary_timesheet_count','') !~ '^[0-9]+$'
         OR COALESCE(v_prior_owner_result->>'summary_timesheets_refreshed','') !~ '^[0-9]+$'
         OR COALESCE(v_prior_owner_result->>'summary_remaining_count','') !~ '^[0-9]+$' THEN
        RAISE EXCEPTION 'DRAFT_FINALIZER_CONTINUATION_RECEIPT_INVALID'
          USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
            'code','DRAFT_FINALIZER_CONTINUATION_RECEIPT_INVALID',
            'operation_id',p_operation_id,
            'candidate_scope_ordinal',v_candidate_scope_ordinal,
            'receipt_digest_sha256',v_prior_owner_receipt_sha256
          )::text;
      END IF;

      v_prior_summary_remaining_count :=
        (v_prior_owner_result->>'summary_remaining_count')::integer;
      IF (v_prior_owner_result->>'reservation_remaining_count')::integer = 0
         AND v_prior_summary_remaining_count > 0 THEN
        v_summary_continuation := true;
        IF (v_prior_owner_result->>'reservation_pending_before_count')::integer > 0
           AND (v_prior_owner_result->>'summary_timesheets_refreshed')::integer = 0
           AND NULLIF(v_prior_owner_result->>'summary_next_after_timesheet_id','') IS NULL THEN
          -- A finance-reservation page deliberately performs no summary work.
          -- Its next call starts the summary keyset before the first UUID, so a
          -- null cursor is the only valid boundary for that exact transition.
          v_summary_after_timesheet_id := NULL;
        ELSIF COALESCE(v_prior_owner_result->>'summary_next_after_timesheet_id','')
             ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
          v_summary_after_timesheet_id :=
            (v_prior_owner_result->>'summary_next_after_timesheet_id')::uuid;
        ELSE
          RAISE EXCEPTION 'DRAFT_FINALIZER_CONTINUATION_RECEIPT_INVALID'
            USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
              'code','DRAFT_FINALIZER_CONTINUATION_RECEIPT_INVALID',
              'reason','SUMMARY_CURSOR_MISSING',
              'operation_id',p_operation_id,
              'candidate_scope_ordinal',v_candidate_scope_ordinal,
              'receipt_digest_sha256',v_prior_owner_receipt_sha256
            )::text;
        END IF;
      END IF;
    END IF;
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

  PERFORM private.pay_workbench_draft_expected_effects_v1(
    p_operation_id,'FINALISE_RESERVATIONS','REGISTER',
    jsonb_build_array(
      jsonb_build_object('relation_name','pay_batch_candidates','operation','DELETE'),
      jsonb_build_object('relation_name','pay_advance_reservations','operation','INSERT'),
      jsonb_build_object('relation_name','pay_advance_reservations','operation','UPDATE'),
      jsonb_build_object('relation_name','pay_batch_items','operation','UPDATE')
    ),jsonb_build_object('pay_batch_id',p_pay_batch_id)
  );

  -- Once the first bounded V8 finalizer page has completed every existing
  -- reservation/economic validation, later pages only continue the exact
  -- Timesheet summary cursor recorded by the durable owner receipt. Re-running
  -- the full frozen-evidence scans for every 25-row page is redundant and can
  -- consume the whole statement budget even though no business decision changes.
  IF v_bounded_v8 AND v_summary_continuation THEN
    SELECT COUNT(*)::integer
    INTO v_candidate_rows_before_empty_delete
    FROM public.pay_batch_candidates AS candidate_current
    WHERE candidate_current.pay_batch_id = p_pay_batch_id
      AND EXISTS (
        SELECT 1 FROM pg_temp.tmp_pay_batch_finalize_scope AS scope_row
        WHERE scope_row.candidate_id = candidate_current.candidate_id
      );
    v_candidate_rows_after_empty_delete := v_candidate_rows_before_empty_delete;

    SELECT COUNT(*)::integer
    INTO v_finance_pending_before_count
    FROM public.pay_batch_items AS pending_item
    JOIN public.pay_batch_candidates AS pending_candidate
      ON pending_candidate.id = pending_item.pay_batch_candidate_id
    JOIN public.banking_pay_operation_candidate_allocation_rows AS pending_allocation
      ON pending_allocation.pay_batch_item_id = pending_item.id
     AND pending_allocation.operation_id = p_operation_id
    WHERE pending_candidate.pay_batch_id = p_pay_batch_id
      AND pending_allocation.candidate_scope_id IN (
        SELECT scope_row.id FROM pg_temp.tmp_pay_batch_finalize_scope AS scope_row)
      AND COALESCE(pending_item.is_voided, false) = false
      AND pending_item.finance_case_id IS NOT NULL
      AND pending_item.reservation_id IS NULL;

    IF v_finance_pending_before_count <> 0 THEN
      RAISE EXCEPTION 'DRAFT_FINALIZER_FINANCE_APPEARED_AFTER_SUMMARY_STARTED'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
          'code','DRAFT_FINALIZER_FINANCE_APPEARED_AFTER_SUMMARY_STARTED',
          'operation_id',p_operation_id,
          'candidate_scope_ordinal',v_candidate_scope_ordinal
        )::text;
    END IF;

    -- Continue by indexed keyset, not by rebuilding the complete Timesheet UUID
    -- array on every page. Count/currentness and the next 25 identities are
    -- derived from the same frozen scoped item set, but no unbounded array is
    -- created and no earlier page is rescanned merely to emit the next page.
    WITH summary_source AS MATERIALIZED (
      SELECT DISTINCT batch_item.timesheet_id
      FROM public.banking_pay_operation_candidate_allocation_rows AS allocation_row
      JOIN public.pay_batch_items AS batch_item
        ON batch_item.id = allocation_row.pay_batch_item_id
      WHERE allocation_row.operation_id = p_operation_id
        AND allocation_row.candidate_scope_id IN (
          SELECT scope_row.id FROM pg_temp.tmp_pay_batch_finalize_scope AS scope_row)
        AND allocation_row.pay_batch_id = p_pay_batch_id
        AND batch_item.timesheet_id IS NOT NULL
        AND COALESCE(batch_item.is_voided, false) = false
        AND UPPER(BTRIM(COALESCE(batch_item.item_type, ''))) IN (
          'SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA'
        )
    )
    SELECT COUNT(*)::integer,
           COUNT(*) FILTER (
             WHERE v_summary_after_timesheet_id IS NULL
                OR summary_source.timesheet_id > v_summary_after_timesheet_id
           )::integer,
           BOOL_OR(summary_source.timesheet_id = v_summary_after_timesheet_id)
    INTO v_summary_total,
         v_summary_remaining_count,
         v_summary_cursor_found
    FROM summary_source;

    IF COALESCE((v_prior_owner_result->>'summary_timesheet_count')::integer, -1)
         IS DISTINCT FROM v_summary_total
       OR (v_summary_after_timesheet_id IS NOT NULL AND NOT COALESCE(v_summary_cursor_found,false))
       OR v_summary_remaining_count IS DISTINCT FROM v_prior_summary_remaining_count THEN
      RAISE EXCEPTION 'DRAFT_FINALIZER_CONTINUATION_SCOPE_DRIFT'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
          'code','DRAFT_FINALIZER_CONTINUATION_SCOPE_DRIFT',
          'operation_id',p_operation_id,
          'candidate_scope_ordinal',v_candidate_scope_ordinal,
          'expected_timesheet_count',v_prior_owner_result->>'summary_timesheet_count',
          'actual_timesheet_count',v_summary_total,
          'expected_remaining_count',v_prior_summary_remaining_count,
          'actual_remaining_count',v_summary_remaining_count,
          'receipt_digest_sha256',v_prior_owner_receipt_sha256
        )::text;
    END IF;

    SELECT COALESCE(
      ARRAY_AGG(page_rows.timesheet_id ORDER BY page_rows.timesheet_id),
      ARRAY[]::uuid[]
    )
    INTO v_summary_chunk_ids
    FROM (
      SELECT DISTINCT batch_item.timesheet_id
      FROM public.banking_pay_operation_candidate_allocation_rows AS allocation_row
      JOIN public.pay_batch_items AS batch_item
        ON batch_item.id = allocation_row.pay_batch_item_id
      WHERE allocation_row.operation_id = p_operation_id
        AND allocation_row.candidate_scope_id IN (
          SELECT scope_row.id FROM pg_temp.tmp_pay_batch_finalize_scope AS scope_row)
        AND allocation_row.pay_batch_id = p_pay_batch_id
        AND (
          v_summary_after_timesheet_id IS NULL
          OR batch_item.timesheet_id > v_summary_after_timesheet_id
        )
        AND COALESCE(batch_item.is_voided, false) = false
        AND UPPER(BTRIM(COALESCE(batch_item.item_type, ''))) IN (
          'SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA'
        )
      ORDER BY batch_item.timesheet_id
      LIMIT 25
    ) AS page_rows;

    v_summary_processed_count := COALESCE(CARDINALITY(v_summary_chunk_ids), 0);
    IF v_summary_processed_count NOT BETWEEN 1 AND 25 THEN
      RAISE EXCEPTION 'DRAFT_FINALIZER_CONTINUATION_PAGE_EMPTY'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
          'code','DRAFT_FINALIZER_CONTINUATION_PAGE_EMPTY',
          'operation_id',p_operation_id,
          'candidate_scope_ordinal',v_candidate_scope_ordinal,
          'receipt_digest_sha256',v_prior_owner_receipt_sha256
        )::text;
    END IF;

    PERFORM public.pay_timesheet_summary_pay_state_refresh(
      p_timesheet_ids => v_summary_chunk_ids,
      p_actor_user_id => p_actor_user_id
    );
    SELECT pg_catalog.max(processed_timesheet.timesheet_id::text)::uuid
    INTO v_summary_next_after_timesheet_id
    FROM UNNEST(v_summary_chunk_ids) AS processed_timesheet(timesheet_id);
    v_summary_remaining_count :=
      v_prior_summary_remaining_count - v_summary_processed_count;

    IF v_summary_remaining_count < 0
       OR v_summary_next_after_timesheet_id <= v_summary_after_timesheet_id THEN
      RAISE EXCEPTION 'DRAFT_FINALIZER_CONTINUATION_PROGRESS_INVALID'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
          'code','DRAFT_FINALIZER_CONTINUATION_PROGRESS_INVALID',
          'operation_id',p_operation_id,
          'candidate_scope_ordinal',v_candidate_scope_ordinal,
          'processed_count',v_summary_processed_count,
          'remaining_count',v_summary_remaining_count,
          'receipt_digest_sha256',v_prior_owner_receipt_sha256
        )::text;
    END IF;

    PERFORM public.pay_batch_display_summary_touch(p_pay_batch_id);
    PERFORM public.banking_pay_batch_signal_touch(
      p_pay_batch_id => p_pay_batch_id,
      p_change_reason => 'DRAFT_ARTIFACTS_FINALISED',
      p_change_source => 'pay_batch_finalize_reservations_and_markers',
      p_change_scope_json => jsonb_build_object(
        'operation_id',p_operation_id::text,
        'candidate_scope_count',v_scope_id_count,
        'pay_channel_scope',v_scope,
        'summary_continuation',true,
        'summary_timesheets_refreshed',v_summary_processed_count
      ),
      p_touch_payment_status => false,
      p_touch_correction_progress => false,
      p_touch_alerts => false,
      p_touch_overview => true
    );
    PERFORM private.pay_workbench_draft_expected_effects_v1(
      p_operation_id,'FINALISE_RESERVATIONS','ASSERT_COMPLETE','[]'::jsonb,
      jsonb_build_object('pay_batch_id',p_pay_batch_id)
    );

    RETURN jsonb_build_object(
      'ok',true,
      'pay_batch_id',p_pay_batch_id::text,
      'operation_id',p_operation_id::text,
      'candidate_scope_count',v_scope_id_count,
      'reservations_created',0,
      'reservations_reused',0,
      'markers_updated',0,
      'failed_count',0,
      'candidate_rows_before_empty_delete',v_candidate_rows_before_empty_delete,
      'deleted_candidate_rows',0,
      'candidate_rows_after_empty_delete',v_candidate_rows_after_empty_delete,
      'awaiting_net_rows_updated',0,
      'reservation_check_excluded_pay_batch_id',p_pay_batch_id::text,
      'reservation_component_count',
        COALESCE((v_prior_owner_result->>'reservation_component_count')::integer,0),
      'reservation_requested_amount_ex_vat',
        COALESCE((v_prior_owner_result->>'reservation_requested_amount_ex_vat')::numeric,0),
      'reservation_outstanding_before_batch_ex_vat',
        COALESCE((v_prior_owner_result->>'reservation_outstanding_before_batch_ex_vat')::numeric,0),
      'bounded_summary_page',true,
      'reservation_pending_before_count',0,
      'reservation_remaining_count',0,
      'summary_timesheet_count',v_summary_total,
      'summary_timesheets_refreshed',v_summary_processed_count,
      'summary_remaining_count',v_summary_remaining_count,
      'summary_next_after_timesheet_id',v_summary_next_after_timesheet_id::text,
      'has_more',v_summary_remaining_count > 0
    );
  END IF;

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

  -- Materialise the exact scoped items and their existing economic-component
  -- owner output once. The historical finalizer derived the same component
  -- evidence independently for item validation and reservation headroom. On a
  -- large Candidate scope that duplicated the dominant work inside one RPC.
  -- These tables cache only the unchanged owner results for this transaction;
  -- they do not alter a key, amount, recovery, reservation or policy decision.
  DROP TABLE IF EXISTS pg_temp.tmp_pay_batch_finalize_items;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_batch_finalize_items ON COMMIT DROP AS
  SELECT pay_batch_item.*,
         signed_recovery.evidence_json AS signed_non_charge_recovery_evidence
  FROM public.pay_batch_items AS pay_batch_item
  JOIN public.pay_batch_candidates AS pay_batch_candidate
    ON pay_batch_candidate.id = pay_batch_item.pay_batch_candidate_id
  JOIN public.banking_pay_operation_candidate_allocation_rows AS allocation_row
    ON allocation_row.pay_batch_item_id = pay_batch_item.id
   AND allocation_row.operation_id = p_operation_id
  LEFT JOIN LATERAL (
    SELECT private.pay_batch_signed_non_charge_recovery_evidence_v1(
      to_jsonb(pay_batch_item)
    ) AS evidence_json
  ) AS signed_recovery ON true
  WHERE pay_batch_candidate.pay_batch_id = p_pay_batch_id
    AND allocation_row.candidate_scope_id IN (
      SELECT finalize_scope.id FROM pg_temp.tmp_pay_batch_finalize_scope AS finalize_scope)
    AND COALESCE(pay_batch_item.is_voided, false) = false;
  CREATE UNIQUE INDEX tmp_pay_batch_finalize_items_id_idx
    ON pg_temp.tmp_pay_batch_finalize_items(id);

  DROP TABLE IF EXISTS pg_temp.tmp_pay_batch_finalize_economic_components;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_batch_finalize_economic_components ON COMMIT DROP AS
  SELECT economic_component.*
  FROM public._pay_batch_item_economic_components(
    NULL::uuid,
    ARRAY(
      SELECT scoped_item.id
      FROM pg_temp.tmp_pay_batch_finalize_items AS scoped_item
      ORDER BY scoped_item.id
    )
  ) AS economic_component;
  CREATE INDEX tmp_pay_batch_finalize_economic_components_item_idx
    ON pg_temp.tmp_pay_batch_finalize_economic_components(pay_batch_item_id);

  WITH scoped_items AS (
    SELECT pay_batch_item.id AS pay_batch_item_id,
           pay_batch_item.timesheet_id,
           pay_batch_item.item_type,
           ROUND(pay_batch_item.amount_ex_vat, 2) AS item_amount_ex_vat,
           economic_component.key_type,
           economic_component.key_value,
           economic_component.source_amount_ex_vat,
           economic_component.key_resolution_failure_reason,
           pay_batch_item.signed_non_charge_recovery_evidence,
           NULLIF(BTRIM(COALESCE(
             pay_batch_item.frozen_source_basis_json->>'source_family_key',
             pay_batch_item.frozen_component_snapshot_json->>'source_family_key',
             ''
           )), '') AS source_family_key,
           COALESCE(
             pay_batch_item.frozen_source_basis_json->'correction_chain_residual',
             pay_batch_item.frozen_component_snapshot_json->'correction_chain_residual'
           ) AS correction_chain_residual
    FROM pg_temp.tmp_pay_batch_finalize_items AS pay_batch_item
    JOIN pg_temp.tmp_pay_batch_finalize_economic_components AS economic_component
      ON economic_component.pay_batch_item_id = pay_batch_item.id
    WHERE pay_batch_item.timesheet_id IS NOT NULL
      AND pay_batch_item.item_type IN ('SEGMENT_DELTA', 'EXPENSE_DELTA', 'ADJUSTMENT_DELTA', 'MILEAGE_DELTA')
  ), invalid_items AS (
    SELECT scoped_items.*,
           CASE
             WHEN scoped_items.key_resolution_failure_reason IS NOT NULL THEN scoped_items.key_resolution_failure_reason
             WHEN scoped_items.key_type IS NULL OR BTRIM(COALESCE(scoped_items.key_type, '')) = '' THEN 'MISSING_ECONOMIC_KEY_TYPE'
             WHEN scoped_items.key_value IS NULL OR BTRIM(COALESCE(scoped_items.key_value, '')) = '' THEN 'MISSING_ECONOMIC_KEY_VALUE'
             WHEN scoped_items.key_type = 'TS_DAY' AND scoped_items.key_value !~ '^\d{4}-\d{2}-\d{2}$' THEN 'INVALID_TS_DAY_KEY_VALUE'
             WHEN scoped_items.signed_non_charge_recovery_evidence IS NOT NULL
              AND scoped_items.item_amount_ex_vat IS DISTINCT FROM ROUND(
                (scoped_items.signed_non_charge_recovery_evidence->>'outstanding_ex_vat')::numeric,
                2
              ) THEN 'SIGNED_NON_CHARGE_RECOVERY_ITEM_AMOUNT_MISMATCH'
             WHEN scoped_items.signed_non_charge_recovery_evidence IS NULL
              AND scoped_items.source_amount_ex_vat IS NULL THEN 'MISSING_SOURCE_RESERVATION_AMOUNT'
             WHEN scoped_items.source_family_key LIKE 'correction-chain:%'
              AND jsonb_typeof(scoped_items.correction_chain_residual) IS DISTINCT FROM 'object'
               THEN 'MISSING_FROZEN_CORRECTION_CHAIN_RESIDUAL'
             WHEN scoped_items.source_family_key LIKE 'correction-chain:%'
              AND NOT EXISTS (
                SELECT 1
                FROM jsonb_array_elements(
                  CASE
                    WHEN jsonb_typeof(scoped_items.correction_chain_residual->'components') = 'array'
                      THEN scoped_items.correction_chain_residual->'components'
                    ELSE '[]'::jsonb
                  END
                ) AS frozen_component(component_json)
                WHERE UPPER(BTRIM(COALESCE(frozen_component.component_json->>'component_key_type', ''))) = scoped_items.key_type
                  AND BTRIM(COALESCE(frozen_component.component_json->>'component_key_value', '')) = scoped_items.key_value
                  AND COALESCE(frozen_component.component_json->>'effective_source_outstanding_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
              )
               THEN 'MISSING_FROZEN_CORRECTION_CHAIN_COMPONENT'
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
           'item_amount_ex_vat', invalid_items.item_amount_ex_vat,
           'signed_non_charge_recovery',
             invalid_items.signed_non_charge_recovery_evidence IS NOT NULL,
           'source_family_key', invalid_items.source_family_key,
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

  PERFORM pg_advisory_xact_lock(hashtextextended(
    'BANKING_PAY_SIGNED_NON_CHARGE_RECOVERY|'
      || pay_batch_item.timesheet_id::text || '|'
      || (pay_batch_item.signed_non_charge_recovery_evidence->>'economic_key_type') || '|'
      || (pay_batch_item.signed_non_charge_recovery_evidence->>'economic_key_value'),
    0
  ))
  FROM pg_temp.tmp_pay_batch_finalize_items AS pay_batch_item
  WHERE pay_batch_item.signed_non_charge_recovery_evidence IS NOT NULL
  ORDER BY pay_batch_item.timesheet_id,
    pay_batch_item.signed_non_charge_recovery_evidence->>'economic_key_type',
    pay_batch_item.signed_non_charge_recovery_evidence->>'economic_key_value';

  WITH scoped_item_components AS (
    SELECT economic_component.timesheet_id,
           economic_component.key_type,
           economic_component.key_value,
           CASE WHEN pay_batch_item.signed_non_charge_recovery_evidence IS NOT NULL
             THEN ROUND((pay_batch_item.signed_non_charge_recovery_evidence->>'outstanding_ex_vat')::numeric, 2)
             ELSE economic_component.source_amount_ex_vat
           END AS source_amount_ex_vat,
           CASE WHEN pay_batch_item.signed_non_charge_recovery_evidence IS NOT NULL
             THEN ROUND((pay_batch_item.signed_non_charge_recovery_evidence->>'outstanding_ex_vat')::numeric, 2)
             ELSE economic_component.target_amount_ex_vat
           END AS target_amount_ex_vat,
           CASE
             WHEN COALESCE(resolved_component.component_json->>'source_pay_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
               THEN ROUND((resolved_component.component_json->>'source_pay_ex_vat')::numeric, 2)
             ELSE NULL::numeric
           END AS resolved_source_amount_ex_vat,
           CASE
             WHEN COALESCE(
                    resolved_component.component_json->>'ready_preview_amount_ex_vat',
                    resolved_component.component_json->>'target_pay_ex_vat',
                    resolved_component.component_json->>'target_amount_ex_vat',
                    ''
                  ) ~ '^-?[0-9]+(\.[0-9]+)?$'
               THEN ROUND(COALESCE(
                 resolved_component.component_json->>'ready_preview_amount_ex_vat',
                 resolved_component.component_json->>'target_pay_ex_vat',
                 resolved_component.component_json->>'target_amount_ex_vat'
               )::numeric, 2)
             ELSE NULL::numeric
           END AS resolved_target_amount_ex_vat,
           (
              pay_batch_item.signed_non_charge_recovery_evidence IS NULL
             AND resolved_component.component_json IS NOT NULL
             AND LOWER(BTRIM(COALESCE(resolved_component.component_json->>'is_resolution_stale', 'false'))) NOT IN ('true','t','1','yes','y','on')
             AND LOWER(BTRIM(COALESCE(resolved_component.component_json->>'is_stale_saved_resolution', 'false'))) NOT IN ('true','t','1','yes','y','on')
             AND LOWER(BTRIM(COALESCE(resolved_component.component_json->>'requires_resolution', 'true'))) IN ('false','f','0','no','n','off')
             AND NULLIF(BTRIM(COALESCE(resolved_component.component_json->>'component_fingerprint', '')), '') IS NOT NULL
             AND NULLIF(BTRIM(COALESCE(resolved_component.component_json->>'source_basis_fingerprint', '')), '') IS NOT NULL
             AND NULLIF(BTRIM(COALESCE(resolved_component.component_json->>'resolved_rate_resolution_id', '')), '') IS NOT NULL
           ) AS resolved_component_is_fresh,
           CASE WHEN pay_batch_item.signed_non_charge_recovery_evidence IS NOT NULL
             THEN 'signed-non-charge-recovery:'
               || (pay_batch_item.signed_non_charge_recovery_evidence->>'evidence_digest')
             ELSE NULLIF(BTRIM(COALESCE(
               pay_batch_item.frozen_source_basis_json->>'source_family_key',
               pay_batch_item.frozen_component_snapshot_json->>'source_family_key',
               ''
             )), '')
           END AS source_family_key,
           pay_batch_item.signed_non_charge_recovery_evidence,
           frozen_chain_component.component_json AS correction_chain_component
    FROM pg_temp.tmp_pay_batch_finalize_economic_components AS economic_component
    JOIN pg_temp.tmp_pay_batch_finalize_items AS pay_batch_item
      ON pay_batch_item.id = economic_component.pay_batch_item_id
    LEFT JOIN LATERAL (
      SELECT jsonb_build_object(
               'source_pay_ex_vat',
               ROUND(SUM(
                 CASE
                   WHEN COALESCE(frozen_resolution_component.component_json->>'source_pay_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
                     THEN (frozen_resolution_component.component_json->>'source_pay_ex_vat')::numeric
                   ELSE NULL::numeric
                 END
               ), 2),
               'ready_preview_amount_ex_vat',
               ROUND(SUM(
                 CASE
                   WHEN COALESCE(
                          frozen_resolution_component.component_json->>'ready_preview_amount_ex_vat',
                          frozen_resolution_component.component_json->>'target_pay_ex_vat',
                          frozen_resolution_component.component_json->>'target_amount_ex_vat',
                          ''
                        ) ~ '^-?[0-9]+(\.[0-9]+)?$'
                     THEN COALESCE(
                            frozen_resolution_component.component_json->>'ready_preview_amount_ex_vat',
                            frozen_resolution_component.component_json->>'target_pay_ex_vat',
                            frozen_resolution_component.component_json->>'target_amount_ex_vat'
                          )::numeric
                   ELSE NULL::numeric
                 END
               ), 2),
               'is_resolution_stale',
               BOOL_OR(
                 LOWER(BTRIM(COALESCE(frozen_resolution_component.component_json->>'is_resolution_stale', 'false')))
                   IN ('true','t','1','yes','y','on')
               ),
               'is_stale_saved_resolution',
               BOOL_OR(
                 LOWER(BTRIM(COALESCE(frozen_resolution_component.component_json->>'is_stale_saved_resolution', 'false')))
                   IN ('true','t','1','yes','y','on')
               ),
               'requires_resolution',
               NOT (
                 LOWER(BTRIM(COALESCE(
                   pay_batch_item.frozen_resolution_payload_json->>'has_resolved_rate',
                   pay_batch_item.frozen_resolution_payload_json#>>'{case_resolution_summary,has_resolved_rate}',
                   'false'
                 ))) IN ('true','t','1','yes','y','on')
                 AND LOWER(BTRIM(COALESCE(
                   pay_batch_item.frozen_resolution_payload_json->>'case_resolution_satisfied_now',
                   pay_batch_item.frozen_resolution_payload_json#>>'{case_resolution_summary,case_resolution_satisfied_now}',
                   'false'
                 ))) IN ('true','t','1','yes','y','on')
                 AND BOOL_AND(
                   LOWER(BTRIM(COALESCE(frozen_resolution_component.component_json->>'requires_resolution', 'true')))
                     IN ('false','f','0','no','n','off')
                 )
                 AND BOOL_AND(
                   COALESCE(frozen_resolution_component.component_json->>'source_pay_ex_vat', '')
                     ~ '^-?[0-9]+(\.[0-9]+)?$'
                 )
                 AND BOOL_AND(
                   COALESCE(
                     frozen_resolution_component.component_json->>'ready_preview_amount_ex_vat',
                     frozen_resolution_component.component_json->>'target_pay_ex_vat',
                     frozen_resolution_component.component_json->>'target_amount_ex_vat',
                     ''
                   ) ~ '^-?[0-9]+(\.[0-9]+)?$'
                 )
               ),
               'component_fingerprint',
               CASE
                 WHEN BOOL_AND(NULLIF(BTRIM(COALESCE(frozen_resolution_component.component_json->>'component_fingerprint', '')), '') IS NOT NULL)
                   THEN md5(string_agg(
                     frozen_resolution_component.component_json->>'component_fingerprint',
                     '|' ORDER BY frozen_resolution_component.component_json->>'resolved_rate_resolution_id'
                   ))
                 ELSE NULL::text
               END,
               'source_basis_fingerprint',
               CASE
                 WHEN BOOL_AND(NULLIF(BTRIM(COALESCE(frozen_resolution_component.component_json->>'source_basis_fingerprint', '')), '') IS NOT NULL)
                   THEN md5(string_agg(
                     frozen_resolution_component.component_json->>'source_basis_fingerprint',
                     '|' ORDER BY frozen_resolution_component.component_json->>'resolved_rate_resolution_id'
                   ))
                 ELSE NULL::text
               END,
               'resolved_rate_resolution_id',
               md5(string_agg(
                 frozen_resolution_component.component_json->>'resolved_rate_resolution_id',
                 '|' ORDER BY frozen_resolution_component.component_json->>'resolved_rate_resolution_id'
               ))
             ) AS component_json
      FROM jsonb_array_elements(
        CASE
          WHEN jsonb_typeof(pay_batch_item.frozen_resolution_payload_json->'case_components') = 'array'
            THEN pay_batch_item.frozen_resolution_payload_json->'case_components'
          ELSE '[]'::jsonb
        END
      ) AS frozen_resolution_component(component_json)
      WHERE UPPER(BTRIM(COALESCE(frozen_resolution_component.component_json->>'component_key_type', ''))) = economic_component.key_type
        AND BTRIM(COALESCE(frozen_resolution_component.component_json->>'component_key_value', '')) = economic_component.key_value
        AND NULLIF(BTRIM(COALESCE(frozen_resolution_component.component_json->>'resolved_rate_resolution_id', '')), '') IS NOT NULL
      HAVING COUNT(*) > 0
    ) AS resolved_component
      ON true
    LEFT JOIN LATERAL (
      SELECT frozen_component.component_json
      FROM jsonb_array_elements(
        CASE
          WHEN jsonb_typeof(COALESCE(
            pay_batch_item.frozen_source_basis_json->'correction_chain_residual',
            pay_batch_item.frozen_component_snapshot_json->'correction_chain_residual'
          )->'components') = 'array'
            THEN COALESCE(
              pay_batch_item.frozen_source_basis_json->'correction_chain_residual',
              pay_batch_item.frozen_component_snapshot_json->'correction_chain_residual'
            )->'components'
          ELSE '[]'::jsonb
        END
      ) AS frozen_component(component_json)
      WHERE UPPER(BTRIM(COALESCE(frozen_component.component_json->>'component_key_type', ''))) = economic_component.key_type
        AND BTRIM(COALESCE(frozen_component.component_json->>'component_key_value', '')) = economic_component.key_value
      LIMIT 1
    ) AS frozen_chain_component
      ON NULLIF(BTRIM(COALESCE(
           pay_batch_item.frozen_source_basis_json->>'source_family_key',
           pay_batch_item.frozen_component_snapshot_json->>'source_family_key',
           ''
         )), '') LIKE 'correction-chain:%'
    WHERE economic_component.timesheet_id IS NOT NULL
      AND economic_component.key_type IN ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE')
      AND economic_component.key_value IS NOT NULL
      AND NOT (economic_component.key_type = 'TS_DAY' AND economic_component.key_value !~ '^\d{4}-\d{2}-\d{2}$')
      AND (
        economic_component.source_amount_ex_vat IS NOT NULL
        OR pay_batch_item.signed_non_charge_recovery_evidence IS NOT NULL
      )
  ), scoped_components AS (
    SELECT scoped_item_component.timesheet_id,
           scoped_item_component.key_type,
           scoped_item_component.key_value,
           scoped_item_component.source_family_key,
           scoped_item_component.source_family_key LIKE 'correction-chain:%' AS is_correction_chain_residual,
           BOOL_AND(
             scoped_item_component.signed_non_charge_recovery_evidence IS NOT NULL
           ) AS is_signed_non_charge_recovery,
           SUM(ROUND(COALESCE(scoped_item_component.source_amount_ex_vat, 0), 2)) AS requested_source_amount_ex_vat,
           SUM(ROUND(COALESCE(scoped_item_component.target_amount_ex_vat, 0), 2)) AS requested_target_amount_ex_vat,
           SUM(scoped_item_component.resolved_source_amount_ex_vat) AS resolved_source_amount_ex_vat,
           SUM(scoped_item_component.resolved_target_amount_ex_vat) AS resolved_target_amount_ex_vat,
           BOOL_AND(COALESCE(scoped_item_component.resolved_component_is_fresh, false)) AS resolved_component_is_fresh,
           MAX(CASE
             WHEN scoped_item_component.signed_non_charge_recovery_evidence IS NOT NULL
               THEN ROUND((scoped_item_component.signed_non_charge_recovery_evidence
                 ->>'outstanding_ex_vat')::numeric, 2)
           END) AS frozen_signed_outstanding_ex_vat,
           MAX(CASE
             WHEN scoped_item_component.signed_non_charge_recovery_evidence IS NOT NULL
               THEN ROUND((scoped_item_component.signed_non_charge_recovery_evidence
                 ->>'reserved_ex_vat')::numeric, 2)
           END) AS frozen_signed_reserved_ex_vat,
           MAX(
             CASE
               WHEN scoped_item_component.source_family_key LIKE 'correction-chain:%'
                AND COALESCE(scoped_item_component.correction_chain_component->>'effective_source_outstanding_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
                 THEN ROUND((scoped_item_component.correction_chain_component->>'effective_source_outstanding_ex_vat')::numeric, 2)
               ELSE NULL::numeric
             END
           ) AS frozen_chain_outstanding_ex_vat
    FROM scoped_item_components AS scoped_item_component
    GROUP BY
      scoped_item_component.timesheet_id,
      scoped_item_component.key_type,
      scoped_item_component.key_value,
      scoped_item_component.source_family_key
  ), timesheet_ids AS (
    SELECT COALESCE(array_agg(DISTINCT scoped_component_rows.timesheet_id), ARRAY[]::uuid[]) AS timesheet_id_array
    FROM scoped_components AS scoped_component_rows
    WHERE scoped_component_rows.is_correction_chain_residual IS NOT TRUE
      AND scoped_component_rows.is_signed_non_charge_recovery IS NOT TRUE
  ), outstanding_components AS (
    SELECT outstanding_component.timesheet_id,
           UPPER(BTRIM(COALESCE(outstanding_component.key_type, ''))) AS key_type,
           BTRIM(COALESCE(outstanding_component.key_value, '')) AS key_value,
           ROUND(COALESCE(outstanding_component.truth_ex_vat, 0), 2) AS truth_ex_vat,
           ROUND(COALESCE(outstanding_component.baseline_ex_vat, 0), 2) AS baseline_ex_vat,
           ROUND(COALESCE(outstanding_component.reserved_ex_vat, 0), 2) AS reserved_ex_vat,
           ROUND(COALESCE(outstanding_component.outstanding_ex_vat, 0), 2) AS outstanding_ex_vat
    FROM public._pay_outstanding_components(
      p_timesheet_ids => (SELECT timesheet_ids.timesheet_id_array FROM timesheet_ids),
      p_exclude_pay_batch_id => p_pay_batch_id
    ) AS outstanding_component
  ), active_signed_reservations AS (
    SELECT active_item.timesheet_id,
      active_evidence.evidence_json->>'economic_key_type' AS key_type,
      active_evidence.evidence_json->>'economic_key_value' AS key_value,
      COUNT(*)::integer AS active_reservation_count
    FROM public.pay_batch_items AS active_item
    JOIN public.pay_batch_candidates AS active_candidate
      ON active_candidate.id = active_item.pay_batch_candidate_id
    JOIN public.pay_batches AS active_batch
      ON active_batch.id = active_candidate.pay_batch_id
    LEFT JOIN public.pay_bank_transfers AS active_transfer
      ON active_transfer.id = active_item.pay_bank_transfer_id
    CROSS JOIN LATERAL (
      SELECT private.pay_batch_signed_non_charge_recovery_evidence_v1(
        to_jsonb(active_item)
      ) AS evidence_json
    ) AS active_evidence
    WHERE active_batch.id <> p_pay_batch_id
      AND public._pay_batch_status_is_active_reservation(active_batch.status)
      AND COALESCE(active_item.is_voided, false) IS NOT TRUE
      AND UPPER(BTRIM(COALESCE(active_candidate.settlement_status, ''))) <> 'SETTLED'
      AND active_candidate.settled_at_utc IS NULL
      AND UPPER(BTRIM(COALESCE(active_transfer.status, ''))) <> 'COMPLETED'
      AND active_transfer.completed_at_utc IS NULL
      AND active_evidence.evidence_json IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM scoped_components AS scoped_component_rows
        WHERE scoped_component_rows.is_signed_non_charge_recovery
          AND scoped_component_rows.timesheet_id = active_item.timesheet_id
          AND scoped_component_rows.key_type =
            active_evidence.evidence_json->>'economic_key_type'
          AND scoped_component_rows.key_value =
            active_evidence.evidence_json->>'economic_key_value'
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_payment_correction_items AS applied_correction
        WHERE applied_correction.pay_batch_item_id = active_item.id
          AND applied_correction.status = 'APPLIED'
          AND applied_correction.correction_item_kind IN (
            'PRE_BANK_CANCEL', 'NO_MONEY_UNWIND', 'SETTLED_REVERSAL'
          )
      )
    GROUP BY active_item.timesheet_id,
      active_evidence.evidence_json->>'economic_key_type',
      active_evidence.evidence_json->>'economic_key_value'
  ), joined_components AS (
    SELECT scoped_component_rows.timesheet_id,
           scoped_component_rows.key_type,
           scoped_component_rows.key_value,
           scoped_component_rows.source_family_key,
           scoped_component_rows.is_correction_chain_residual,
           scoped_component_rows.is_signed_non_charge_recovery,
           ROUND(scoped_component_rows.requested_source_amount_ex_vat, 2) AS requested_source_amount_ex_vat,
           ROUND(scoped_component_rows.requested_target_amount_ex_vat, 2) AS requested_target_amount_ex_vat,
           CASE
             WHEN scoped_component_rows.is_signed_non_charge_recovery
               THEN ROUND(scoped_component_rows.requested_source_amount_ex_vat, 2)
             WHEN scoped_component_rows.is_correction_chain_residual
               THEN ROUND(scoped_component_rows.requested_source_amount_ex_vat, 2)
             WHEN scoped_component_rows.resolved_component_is_fresh
               THEN ROUND(scoped_component_rows.requested_target_amount_ex_vat, 2)
             ELSE ROUND(scoped_component_rows.requested_source_amount_ex_vat, 2)
           END AS reservation_requested_amount_ex_vat,
           CASE
             WHEN scoped_component_rows.is_signed_non_charge_recovery
               THEN CASE WHEN COALESCE(
                 active_signed_reservation_rows.active_reservation_count, 0
               ) = 0 THEN ROUND(COALESCE(
                   scoped_component_rows.frozen_signed_outstanding_ex_vat, 0
                 ), 2)
                 ELSE 0::numeric
               END
             WHEN scoped_component_rows.is_correction_chain_residual
               THEN ROUND(COALESCE(scoped_component_rows.frozen_chain_outstanding_ex_vat, 0), 2)
             WHEN scoped_component_rows.resolved_component_is_fresh
              AND ROUND(COALESCE(scoped_component_rows.resolved_source_amount_ex_vat, 0), 2)
                  = ROUND(COALESCE(outstanding_component_rows.truth_ex_vat, 0), 2)
              AND ROUND(COALESCE(outstanding_component_rows.reserved_ex_vat, 0), 2) = 0
              AND ROUND(COALESCE(scoped_component_rows.requested_target_amount_ex_vat, 0), 2)
                  = ROUND(
                      GREATEST(
                        COALESCE(scoped_component_rows.resolved_target_amount_ex_vat, 0)
                          - COALESCE(outstanding_component_rows.baseline_ex_vat, 0),
                        0
                      ),
                      2
                    )
               THEN ROUND(
                      GREATEST(
                        COALESCE(scoped_component_rows.resolved_target_amount_ex_vat, 0)
                          - COALESCE(outstanding_component_rows.baseline_ex_vat, 0),
                        0
                      ),
                      2
                    )
             WHEN scoped_component_rows.resolved_component_is_fresh
               THEN 0::numeric
             ELSE ROUND(COALESCE(outstanding_component_rows.outstanding_ex_vat, 0), 2)
           END AS outstanding_ex_vat,
           CASE
             WHEN scoped_component_rows.is_signed_non_charge_recovery
               THEN 'FROZEN_SIGNED_NON_CHARGE_RECOVERY'
             WHEN scoped_component_rows.is_correction_chain_residual
               THEN 'FROZEN_CORRECTION_CHAIN_RESIDUAL'
             WHEN scoped_component_rows.resolved_component_is_fresh
              AND ROUND(COALESCE(scoped_component_rows.resolved_source_amount_ex_vat, 0), 2)
                  = ROUND(COALESCE(outstanding_component_rows.truth_ex_vat, 0), 2)
              AND ROUND(COALESCE(outstanding_component_rows.reserved_ex_vat, 0), 2) = 0
              AND ROUND(COALESCE(scoped_component_rows.requested_target_amount_ex_vat, 0), 2)
                  = ROUND(
                      GREATEST(
                        COALESCE(scoped_component_rows.resolved_target_amount_ex_vat, 0)
                          - COALESCE(outstanding_component_rows.baseline_ex_vat, 0),
                        0
                      ),
                      2
                    )
               THEN 'FROZEN_FRESH_PRE_DRAFT_RESOLUTION_TARGET_REMAINDER'
             WHEN scoped_component_rows.resolved_component_is_fresh
               THEN 'FROZEN_FRESH_PRE_DRAFT_RESOLUTION_MISMATCH_OR_RESERVED'
             ELSE 'LIVE_PRE_DRAFT_OUTSTANDING'
           END AS outstanding_evidence_source
    FROM scoped_components AS scoped_component_rows
    LEFT JOIN outstanding_components AS outstanding_component_rows
      ON scoped_component_rows.is_correction_chain_residual IS NOT TRUE
     AND outstanding_component_rows.timesheet_id = scoped_component_rows.timesheet_id
     AND outstanding_component_rows.key_type = scoped_component_rows.key_type
     AND outstanding_component_rows.key_value = scoped_component_rows.key_value
    LEFT JOIN active_signed_reservations AS active_signed_reservation_rows
      ON scoped_component_rows.is_signed_non_charge_recovery
     AND active_signed_reservation_rows.timesheet_id = scoped_component_rows.timesheet_id
     AND active_signed_reservation_rows.key_type = scoped_component_rows.key_type
     AND active_signed_reservation_rows.key_value = scoped_component_rows.key_value
  ), overruns AS (
    SELECT joined_component_rows.timesheet_id,
           joined_component_rows.key_type,
           joined_component_rows.key_value,
           joined_component_rows.source_family_key,
           joined_component_rows.requested_source_amount_ex_vat,
           joined_component_rows.requested_target_amount_ex_vat,
           joined_component_rows.reservation_requested_amount_ex_vat,
           joined_component_rows.outstanding_ex_vat,
           joined_component_rows.outstanding_evidence_source
    FROM joined_components AS joined_component_rows
    WHERE joined_component_rows.reservation_requested_amount_ex_vat > joined_component_rows.outstanding_ex_vat + 0.01
  )
  SELECT COALESCE(
           (
             SELECT jsonb_agg(
                      jsonb_build_object(
                        'timesheet_id', overrun_sample_rows.timesheet_id::text,
                        'key_type', overrun_sample_rows.key_type,
                        'key_value', overrun_sample_rows.key_value,
                        'source_family_key', overrun_sample_rows.source_family_key,
                        'requested_source_amount_ex_vat', overrun_sample_rows.requested_source_amount_ex_vat,
                        'requested_target_amount_ex_vat', overrun_sample_rows.requested_target_amount_ex_vat,
                        'reservation_requested_amount_ex_vat', overrun_sample_rows.reservation_requested_amount_ex_vat,
                        'outstanding_ex_vat', overrun_sample_rows.outstanding_ex_vat,
                        'outstanding_evidence_source', overrun_sample_rows.outstanding_evidence_source
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
         ROUND(COALESCE((SELECT SUM(joined_sum_rows.reservation_requested_amount_ex_vat) FROM joined_components AS joined_sum_rows), 0), 2),
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

  SELECT pg_catalog.count(*)::integer
  INTO v_finance_pending_before_count
  FROM public.pay_batch_items AS pending_item
  JOIN public.pay_batch_candidates AS pending_candidate
    ON pending_candidate.id = pending_item.pay_batch_candidate_id
  JOIN public.banking_pay_operation_candidate_allocation_rows AS pending_allocation
    ON pending_allocation.pay_batch_item_id = pending_item.id
   AND pending_allocation.operation_id = p_operation_id
  WHERE pending_candidate.pay_batch_id = p_pay_batch_id
    AND pending_allocation.candidate_scope_id IN (
      SELECT scope_row.id FROM pg_temp.tmp_pay_batch_finalize_scope AS scope_row)
    AND COALESCE(pending_item.is_voided, false) = false
    AND pending_item.finance_case_id IS NOT NULL
    AND pending_item.reservation_id IS NULL;

  IF v_bounded_v8
     AND v_summary_after_timesheet_id IS NOT NULL
     AND v_finance_pending_before_count > 0 THEN
    RAISE EXCEPTION 'DRAFT_FINALIZER_FINANCE_APPEARED_AFTER_SUMMARY_STARTED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code','DRAFT_FINALIZER_FINANCE_APPEARED_AFTER_SUMMARY_STARTED',
        'operation_id',p_operation_id,
        'candidate_scope_ordinal',v_candidate_scope_ordinal
      )::text;
  END IF;

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

  SELECT pg_catalog.count(*)::integer
  INTO v_finance_remaining_count
  FROM public.pay_batch_items AS remaining_finance_item
  JOIN public.pay_batch_candidates AS remaining_finance_candidate
    ON remaining_finance_candidate.id = remaining_finance_item.pay_batch_candidate_id
  JOIN public.banking_pay_operation_candidate_allocation_rows AS remaining_finance_allocation
    ON remaining_finance_allocation.pay_batch_item_id = remaining_finance_item.id
   AND remaining_finance_allocation.operation_id = p_operation_id
  WHERE remaining_finance_candidate.pay_batch_id = p_pay_batch_id
    AND remaining_finance_allocation.candidate_scope_id IN (
      SELECT scope_row.id FROM pg_temp.tmp_pay_batch_finalize_scope AS scope_row)
    AND COALESCE(remaining_finance_item.is_voided, false) = false
    AND remaining_finance_item.finance_case_id IS NOT NULL
    AND remaining_finance_item.reservation_id IS NULL;

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

  IF v_bounded_v8 THEN
    IF v_finance_pending_before_count > 0 THEN
      -- A finance-reservation page and a summary page are never combined. This
      -- keeps one bounded owner call measurable and makes response-loss replay
      -- resume from the durable owner receipt rather than repeating hidden work.
      v_summary_processed_count := 0;
      v_summary_remaining_count := v_summary_total;
      v_summary_next_after_timesheet_id := v_summary_after_timesheet_id;
    ELSE
      SELECT COALESCE(
        pg_catalog.array_agg(page_rows.timesheet_id ORDER BY page_rows.timesheet_id),
        ARRAY[]::uuid[]
      )
      INTO v_summary_chunk_ids
      FROM (
        SELECT candidate_timesheet.timesheet_id
        FROM pg_catalog.unnest(v_summary_timesheet_ids) AS candidate_timesheet(timesheet_id)
        WHERE v_summary_after_timesheet_id IS NULL
           OR candidate_timesheet.timesheet_id > v_summary_after_timesheet_id
        ORDER BY candidate_timesheet.timesheet_id
        LIMIT 25
      ) AS page_rows;

      v_summary_processed_count := COALESCE(pg_catalog.cardinality(v_summary_chunk_ids), 0);
      IF v_summary_processed_count > 0 THEN
        PERFORM public.pay_timesheet_summary_pay_state_refresh(
          p_timesheet_ids => v_summary_chunk_ids,
          p_actor_user_id => p_actor_user_id
        );
        SELECT pg_catalog.max(processed_timesheet.timesheet_id::text)::uuid
        INTO v_summary_next_after_timesheet_id
        FROM pg_catalog.unnest(v_summary_chunk_ids) AS processed_timesheet(timesheet_id);
      ELSE
        v_summary_next_after_timesheet_id := v_summary_after_timesheet_id;
      END IF;

      SELECT pg_catalog.count(*)::integer
      INTO v_summary_remaining_count
      FROM pg_catalog.unnest(v_summary_timesheet_ids) AS remaining_timesheet(timesheet_id)
      WHERE v_summary_next_after_timesheet_id IS NULL
         OR remaining_timesheet.timesheet_id > v_summary_next_after_timesheet_id;
    END IF;
  ELSE
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
    v_summary_processed_count := v_summary_total;
    v_summary_remaining_count := 0;
    SELECT pg_catalog.max(processed_timesheet.timesheet_id::text)::uuid
    INTO v_summary_next_after_timesheet_id
    FROM pg_catalog.unnest(v_summary_timesheet_ids) AS processed_timesheet(timesheet_id);
  END IF;

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

  PERFORM private.pay_workbench_draft_expected_effects_v1(
    p_operation_id,'FINALISE_RESERVATIONS','ASSERT_COMPLETE','[]'::jsonb,
    jsonb_build_object('pay_batch_id',p_pay_batch_id)
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
    'bounded_summary_page', v_bounded_v8,
    'reservation_pending_before_count', COALESCE(v_finance_pending_before_count, 0),
    'reservation_remaining_count', COALESCE(v_finance_remaining_count, 0),
    'summary_timesheet_count', COALESCE(v_summary_total, 0),
    'summary_timesheets_refreshed', COALESCE(v_summary_processed_count, 0),
    'summary_remaining_count', COALESCE(v_summary_remaining_count, 0),
    'summary_next_after_timesheet_id', CASE
      WHEN v_summary_next_after_timesheet_id IS NULL THEN NULL
      ELSE v_summary_next_after_timesheet_id::text
    END,
    'has_more', (
      COALESCE(v_finance_remaining_count, 0) > 0
      OR COALESCE(v_summary_remaining_count, 0) > 0
    )
  );
END;
$function$;

ALTER FUNCTION public.pay_batch_finalize_reservations_and_markers(
  uuid, text, uuid, date, date, uuid, jsonb
) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_batch_finalize_reservations_and_markers(
  uuid, text, uuid, date, date, uuid, jsonb
) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_batch_finalize_reservations_and_markers(
  uuid, text, uuid, date, date, uuid, jsonb
) TO postgres, service_role;
