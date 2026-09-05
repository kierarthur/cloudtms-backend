-- Bounded constituent-by-constituent Create Draft parity proof.
-- Runtime authority is Miget TEST. The `supabase` directory name is historical only.
-- This routine compares certified source facts with artifacts produced by the existing
-- allocation, item, finance and reservation owners. It contains no financial equation,
-- category translation, tax/VAT decision or payment-policy fallback.

CREATE OR REPLACE FUNCTION private.pay_workbench_draft_constituent_parity_compare_v8(
  p_operation_id uuid,
  p_constituent_ordinal integer
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path TO ''
AS $function$
DECLARE
  v_scope private.banking_pay_draft_frozen_certificate_scopes_v8%ROWTYPE;
  v_entry private.banking_pay_workbench_settled_certificate_entries_v8%ROWTYPE;
  v_candidate_scope private.banking_pay_draft_frozen_candidate_scopes_v8%ROWTYPE;
  v_existing private.banking_pay_draft_constituent_parity_results_v8%ROWTYPE;
  v_expected_json jsonb;
  v_allocation_json jsonb;
  v_item_json jsonb;
  v_reservation_json jsonb;
  v_actual_json jsonb;
  v_allocation_count integer := 0;
  v_item_count integer := 0;
  v_reservation_count integer := 0;
  v_allocation_total numeric := 0;
  v_source_amount_total numeric := 0;
  v_bad_allocation_count integer := 0;
  v_bad_certificate_binding_count integer := 0;
  v_bad_allocation_source_evidence_count integer := 0;
  v_bad_item_source_evidence_count integer := 0;
  v_bad_item_count integer := 0;
  v_bad_reservation_count integer := 0;
  v_source_reservation_ids jsonb := '[]'::jsonb;
  v_source_reservation_id_count integer := 0;
  v_recomputed_source_reservation_digest text;
  v_expected_digest text;
  v_allocation_digest text;
  v_item_digest text;
  v_reservation_digest text;
  v_materialisation_digest text;
  v_comparison_digest text;
  v_status text := 'MATCH';
  v_mismatch text := NULL;
  v_expected_bytes integer;
  v_actual_bytes integer;
  v_now timestamptz := pg_catalog.clock_timestamp();
BEGIN
  IF p_operation_id IS NULL
     OR p_constituent_ordinal IS NULL
     OR p_constituent_ordinal NOT BETWEEN 0 AND 49999 THEN
    RAISE EXCEPTION 'DRAFT_PARITY_EXPECTED_REFERENCE_MISSING'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'DRAFT_PARITY_EXPECTED_REFERENCE_MISSING',
        'operation_id', p_operation_id,
        'constituent_ordinal', p_constituent_ordinal
      )::text;
  END IF;

  SELECT frozen_scope.*
  INTO v_scope
  FROM private.banking_pay_draft_frozen_certificate_scopes_v8 AS frozen_scope
  WHERE frozen_scope.operation_id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND OR v_scope.freeze_state <> 'FROZEN' THEN
    RAISE EXCEPTION 'DRAFT_PARITY_NOT_FROZEN'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'DRAFT_PARITY_NOT_FROZEN',
        'operation_id', p_operation_id
      )::text;
  END IF;

  SELECT entry.*
  INTO v_entry
  FROM private.banking_pay_draft_frozen_constituent_refs_v8 AS frozen_ref
  JOIN private.banking_pay_workbench_settled_certificate_entries_v8 AS entry
    ON entry.certificate_uuid = frozen_ref.certificate_uuid
   AND entry.constituent_ordinal = frozen_ref.constituent_ordinal
  WHERE frozen_ref.operation_id = p_operation_id
    AND frozen_ref.constituent_ordinal = p_constituent_ordinal;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'DRAFT_PARITY_EXPECTED_REFERENCE_MISSING'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'DRAFT_PARITY_EXPECTED_REFERENCE_MISSING',
        'operation_id', p_operation_id,
        'constituent_ordinal', p_constituent_ordinal
      )::text;
  END IF;

  SELECT COALESCE(
           pg_catalog.jsonb_agg(pg_catalog.to_jsonb(source_reservation.source_reservation_id)
             ORDER BY source_reservation.reservation_ordinal),
           '[]'::jsonb
         ),
         pg_catalog.count(*)::integer
  INTO v_source_reservation_ids, v_source_reservation_id_count
  FROM private.banking_pay_workbench_settled_cert_source_reservations_v8 AS source_reservation
  WHERE source_reservation.certificate_uuid = v_entry.certificate_uuid
    AND source_reservation.constituent_ordinal = v_entry.constituent_ordinal;

  v_recomputed_source_reservation_digest := CASE
    WHEN v_entry.expected_reservation_amount_ex_vat IS NULL THEN NULL
    ELSE private.pay_workbench_settled_certificate_sha256_text_v8(
      private.pay_workbench_settled_certificate_stable_stringify_v8(
        pg_catalog.jsonb_build_object(
          'ordered_active_source_reservation_ids', v_source_reservation_ids,
          'source_reservation_amount_ex_vat', v_entry.expected_reservation_amount_ex_vat
        )
      )
    )
  END;

  SELECT candidate_scope.*
  INTO v_candidate_scope
  FROM private.banking_pay_draft_frozen_candidate_scope_members_v8 AS member
  JOIN private.banking_pay_draft_frozen_candidate_scopes_v8 AS candidate_scope
    ON candidate_scope.operation_id = member.operation_id
   AND candidate_scope.candidate_scope_ordinal = member.candidate_scope_ordinal
  WHERE member.operation_id = p_operation_id
    AND member.constituent_ordinal = p_constituent_ordinal;

  IF NOT FOUND
     OR v_candidate_scope.candidate_id IS DISTINCT FROM v_entry.candidate_id
     OR v_candidate_scope.resolved_pay_channel IS DISTINCT FROM v_entry.resolved_pay_channel
     OR v_candidate_scope.pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'DRAFT_PARITY_ACTUAL_OUTPUT_MISSING'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'DRAFT_PARITY_ACTUAL_OUTPUT_MISSING',
        'operation_id', p_operation_id,
        'constituent_ordinal', p_constituent_ordinal,
        'boundary', 'CANDIDATE_SCOPE_OR_BATCH'
      )::text;
  END IF;

  v_expected_json := pg_catalog.jsonb_build_object(
    'allocation_expectation', pg_catalog.jsonb_build_object(
      'basis_kind', v_entry.expected_allocation_basis_kind,
      'allocated_recovery_amount_ex_vat', v_entry.expected_allocated_recovery_amount_ex_vat,
      'result', v_entry.expected_allocation_result,
      'source_evidence_digest_sha256', v_entry.expected_allocation_source_digest_sha256
    ),
    'item_expectation', pg_catalog.jsonb_build_object(
      'semantic_kind', v_entry.expected_item_semantic_kind,
      'source_identity_digest_sha256', v_entry.expected_item_source_identity_digest_sha256,
      'amount_ex_vat', v_entry.expected_item_amount_ex_vat,
      'source_evidence_digest_sha256', v_entry.expected_item_source_digest_sha256
    ),
    'source_reservation_expectation', pg_catalog.jsonb_build_object(
      'applicability', v_entry.expected_reservation_applicability,
      'amount_ex_vat', v_entry.expected_reservation_amount_ex_vat,
      'ordered_active_source_reservation_id_count', v_source_reservation_id_count,
      'source_evidence_digest_sha256', v_entry.expected_reservation_source_digest_sha256
    )
  );

  WITH allocation_rows_base AS (
    SELECT allocation_row.*,
      COALESCE(
        NULLIF(allocation_row.allocation_basis_json->>'preview_row_id', ''),
        NULLIF(allocation_row.allocation_basis_json#>>'{line,preview_row_id}', ''),
        NULLIF(allocation_row.allocation_basis_json#>>'{line,preview_row_pk}', '')
      ) AS bound_preview_row_id,
      NULLIF(allocation_row.allocation_basis_json#>>'{draft_finance_item_plan,planned_item_key}', '') AS planned_item_key,
      NULLIF(allocation_row.allocation_basis_json#>>'{draft_finance_item_plan,planned_item_amount}', '') AS planned_item_amount,
      NULLIF(allocation_row.allocation_basis_json#>>'{draft_finance_item_plan,plan_digest}', '') AS planned_item_digest,
      allocation_row.allocation_basis_json#>'{line,workbench_settled_certificate_binding_v8}' AS certificate_binding_json,
      allocation_row.allocation_basis_json#>'{line,selection_recovery_headroom_v1}' AS allocation_source_evidence_json,
      allocation_row.allocation_basis_json->'line' AS source_line_json
    FROM public.banking_pay_operation_candidate_allocation_rows AS allocation_row
    WHERE allocation_row.operation_id = p_operation_id
      AND allocation_row.candidate_scope_id = (
        SELECT public_scope.id
        FROM public.banking_pay_operation_candidate_scope AS public_scope
        WHERE public_scope.operation_id = p_operation_id
          AND public_scope.candidate_id = v_candidate_scope.candidate_id
          AND public_scope.pay_channel = v_candidate_scope.resolved_pay_channel
        LIMIT 1
      )
      AND COALESCE(
        NULLIF(allocation_row.allocation_basis_json->>'preview_row_id', ''),
        NULLIF(allocation_row.allocation_basis_json#>>'{line,preview_row_id}', ''),
        NULLIF(allocation_row.allocation_basis_json#>>'{line,preview_row_pk}', '')
      ) = v_entry.materialised_preview_row_id::text
  ), allocation_rows AS (
    SELECT allocation_row.*,
      CASE
        WHEN pg_catalog.jsonb_typeof(allocation_row.allocation_source_evidence_json) = 'object'
          THEN private.pay_workbench_settled_certificate_sha256_text_v8(
            private.pay_workbench_settled_certificate_stable_stringify_v8(
              allocation_row.allocation_source_evidence_json
            )
          )
        ELSE NULL
      END AS actual_allocation_source_digest_sha256,
      CASE
        WHEN pg_catalog.jsonb_typeof(allocation_row.certificate_binding_json) = 'object'
          AND COALESCE(allocation_row.source_line_json->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
          THEN private.pay_workbench_settled_certificate_sha256_text_v8(
            private.pay_workbench_settled_certificate_stable_stringify_v8(
              pg_catalog.jsonb_build_object(
                'canonical_amount_ex_vat',
                  private.pay_workbench_settled_certificate_money_v8(
                    pg_catalog.to_jsonb(pg_catalog.round((allocation_row.source_line_json->>'amount_ex_vat')::numeric, 2))
                  ),
                'economic_key', pg_catalog.jsonb_build_object(
                  'key_type', allocation_row.source_line_json#>>'{economic_key,key_type}',
                  'key_value', allocation_row.source_line_json#>>'{economic_key,key_value}',
                  'timesheet_id', allocation_row.source_line_json#>>'{economic_key,timesheet_id}'
                ),
                'semantic_kind', pg_catalog.upper(pg_catalog.btrim(COALESCE(allocation_row.source_line_json->>'line_type', ''))),
                'source_identity_digest_sha256', allocation_row.certificate_binding_json->>'source_identity_digest_sha256'
              )
            )
          )
        ELSE NULL
      END AS actual_item_source_digest_sha256
    FROM allocation_rows_base AS allocation_row
  ), allocation_projection AS (
    SELECT pg_catalog.jsonb_build_object(
      'allocated_amount_ex_vat', (pg_catalog.round(allocation_row.allocated_amount, 2)::numeric(30,2))::text,
      'allocation_type', allocation_row.allocation_type,
      'candidate_id', allocation_row.candidate_id::text,
      'certificate_binding_digest_sha256', CASE
        WHEN pg_catalog.jsonb_typeof(allocation_row.certificate_binding_json) = 'object'
          THEN pg_catalog.encode(extensions.digest(pg_catalog.convert_to(allocation_row.certificate_binding_json::text, 'UTF8'), 'sha256'), 'hex')
        ELSE NULL
      END,
      'finance_case_id', CASE WHEN allocation_row.finance_case_id IS NULL THEN NULL ELSE allocation_row.finance_case_id::text END,
      'finance_component_id', CASE WHEN allocation_row.finance_component_id IS NULL THEN NULL ELSE allocation_row.finance_component_id::text END,
      'operation_source_key', allocation_row.operation_source_key,
      'pay_channel', allocation_row.pay_channel,
      'planned_item_digest_sha256', allocation_row.planned_item_digest,
      'planned_item_key', allocation_row.planned_item_key,
      'preview_row_id', allocation_row.bound_preview_row_id,
      'source_allocation_evidence_digest_sha256', allocation_row.actual_allocation_source_digest_sha256,
      'source_item_evidence_digest_sha256', allocation_row.actual_item_source_digest_sha256,
      'source_ref', allocation_row.source_ref,
      'status', allocation_row.status
    ) AS row_json,
    allocation_row.*
    FROM allocation_rows AS allocation_row
  )
  SELECT pg_catalog.count(*)::integer,
    pg_catalog.round(COALESCE(pg_catalog.sum(allocation_projection.allocated_amount), 0), 2),
    COALESCE(pg_catalog.jsonb_agg(allocation_projection.row_json ORDER BY allocation_projection.sort_order, allocation_projection.operation_source_key), '[]'::jsonb),
    pg_catalog.count(*) FILTER (WHERE
      allocation_projection.candidate_id IS DISTINCT FROM v_entry.candidate_id
      OR pg_catalog.upper(pg_catalog.btrim(COALESCE(allocation_projection.pay_channel, ''))) IS DISTINCT FROM v_entry.resolved_pay_channel
      OR allocation_projection.pay_batch_id IS DISTINCT FROM v_candidate_scope.pay_batch_id
      OR allocation_projection.bound_preview_row_id IS DISTINCT FROM v_entry.materialised_preview_row_id::text
      OR pg_catalog.upper(pg_catalog.btrim(COALESCE(allocation_projection.status, ''))) <> 'ITEM_CREATED'
      OR allocation_projection.pay_batch_item_id IS NULL
    )::integer,
    pg_catalog.count(*) FILTER (WHERE
      pg_catalog.jsonb_typeof(allocation_projection.certificate_binding_json) IS DISTINCT FROM 'object'
      OR allocation_projection.certificate_binding_json->>'binding_contract_version' IS DISTINCT FROM 'WORKBENCH_SETTLED_CERTIFICATE_BINDING_V8'
      OR allocation_projection.certificate_binding_json->>'certificate_uuid' IS DISTINCT FROM v_entry.certificate_uuid::text
      OR allocation_projection.certificate_binding_json->>'constituent_ordinal' IS DISTINCT FROM v_entry.constituent_ordinal::text
      OR allocation_projection.certificate_binding_json->>'constituent_digest_sha256' IS DISTINCT FROM v_entry.constituent_digest_sha256
      OR allocation_projection.certificate_binding_json->>'source_identity_digest_sha256' IS DISTINCT FROM v_entry.expected_item_source_identity_digest_sha256
    )::integer,
    pg_catalog.count(*) FILTER (WHERE
      CASE v_entry.expected_allocation_basis_kind
        WHEN 'NOT_APPLICABLE' THEN NOT (
          v_entry.expected_allocation_result = 'NOT_APPLICABLE'
          AND v_entry.expected_allocated_recovery_amount_ex_vat IS NULL
          AND v_entry.expected_allocation_source_digest_sha256 IS NULL
        )
        WHEN 'WORKBENCH_RECOVERY_HEADROOM_V1' THEN NOT (
          v_entry.expected_allocation_result = 'ALLOCATED_WITHIN_CERTIFIED_HEADROOM'
          AND v_entry.expected_allocated_recovery_amount_ex_vat IS NOT NULL
          AND v_entry.expected_allocation_source_digest_sha256 IS NOT NULL
          AND allocation_projection.actual_allocation_source_digest_sha256 IS NOT DISTINCT FROM v_entry.expected_allocation_source_digest_sha256
        )
        ELSE true
      END
    )::integer,
    pg_catalog.count(*) FILTER (WHERE
      pg_catalog.upper(pg_catalog.btrim(COALESCE(allocation_projection.source_line_json->>'line_type', ''))) IS DISTINCT FROM v_entry.expected_item_semantic_kind
      OR allocation_projection.source_line_json#>>'{economic_key,timesheet_id}' IS DISTINCT FROM CASE WHEN v_entry.economic_key_timesheet_id IS NULL THEN NULL ELSE v_entry.economic_key_timesheet_id::text END
      OR allocation_projection.source_line_json#>>'{economic_key,key_type}' IS DISTINCT FROM v_entry.economic_key_type
      OR allocation_projection.source_line_json#>>'{economic_key,key_value}' IS DISTINCT FROM v_entry.economic_key_value
      OR allocation_projection.actual_item_source_digest_sha256 IS DISTINCT FROM v_entry.expected_item_source_digest_sha256
    )::integer
  INTO v_allocation_count, v_allocation_total, v_allocation_json, v_bad_allocation_count,
       v_bad_certificate_binding_count, v_bad_allocation_source_evidence_count,
       v_bad_item_source_evidence_count
  FROM allocation_projection;

  WITH linked_allocations AS (
    SELECT allocation_row.*,
      NULLIF(allocation_row.allocation_basis_json#>>'{draft_finance_item_plan,planned_item_key}', '') AS planned_item_key,
      NULLIF(allocation_row.allocation_basis_json#>>'{draft_finance_item_plan,planned_item_amount}', '') AS planned_item_amount
    FROM public.banking_pay_operation_candidate_allocation_rows AS allocation_row
    WHERE allocation_row.operation_id = p_operation_id
      AND COALESCE(
        NULLIF(allocation_row.allocation_basis_json->>'preview_row_id', ''),
        NULLIF(allocation_row.allocation_basis_json#>>'{line,preview_row_id}', ''),
        NULLIF(allocation_row.allocation_basis_json#>>'{line,preview_row_pk}', '')
      ) = v_entry.materialised_preview_row_id::text
  ), item_rows AS (
    SELECT DISTINCT ON (item.id)
      item.*,
      batch_candidate.candidate_id,
      linked_allocation.id AS allocation_row_id,
      linked_allocation.operation_source_key AS allocation_source_key,
      linked_allocation.allocated_amount AS allocation_amount,
      linked_allocation.finance_case_id AS allocation_finance_case_id,
      linked_allocation.finance_component_id AS allocation_finance_component_id,
      linked_allocation.planned_item_key,
      linked_allocation.planned_item_amount
    FROM linked_allocations AS linked_allocation
    JOIN public.pay_batch_items AS item
      ON item.id = linked_allocation.pay_batch_item_id
    JOIN public.pay_batch_candidates AS batch_candidate
      ON batch_candidate.id = item.pay_batch_candidate_id
    ORDER BY item.id, linked_allocation.sort_order, linked_allocation.operation_source_key
  ), item_projection AS (
    SELECT pg_catalog.jsonb_build_object(
      'amount_ex_vat', (pg_catalog.round(COALESCE(item_row.amount_ex_vat, 0), 2)::numeric(30,2))::text,
      'amount_inc_vat', (pg_catalog.round(COALESCE(item_row.amount_inc_vat, 0), 2)::numeric(30,2))::text,
      'amount_vat', (pg_catalog.round(COALESCE(item_row.amount_vat, 0), 2)::numeric(30,2))::text,
      'candidate_id', item_row.candidate_id::text,
      'finance_case_id', CASE WHEN item_row.finance_case_id IS NULL THEN NULL ELSE item_row.finance_case_id::text END,
      'finance_component_id', CASE WHEN item_row.finance_component_id IS NULL THEN NULL ELSE item_row.finance_component_id::text END,
      'frozen_component_classification', CASE WHEN item_row.frozen_component_classification IS NULL THEN NULL ELSE item_row.frozen_component_classification::text END,
      'frozen_component_key_type', item_row.frozen_component_key_type,
      'frozen_component_key_value', item_row.frozen_component_key_value,
      'frozen_source_pay_method', item_row.frozen_source_pay_method,
      'frozen_target_pay_method', item_row.frozen_target_pay_method,
      'item_type', item_row.item_type,
      'operation_source_key', item_row.operation_source_key,
      'pay_channel', item_row.pay_channel,
      'paye_treatment', item_row.paye_treatment,
      'reservation_present', item_row.reservation_id IS NOT NULL,
      'source_ref', item_row.source_ref,
      'timesheet_id', CASE WHEN item_row.timesheet_id IS NULL THEN NULL ELSE item_row.timesheet_id::text END
    ) AS row_json,
    item_row.*
    FROM item_rows AS item_row
  )
  SELECT pg_catalog.count(*)::integer,
    COALESCE(pg_catalog.jsonb_agg(item_projection.row_json ORDER BY item_projection.operation_source_key, item_projection.item_type), '[]'::jsonb),
    pg_catalog.round(COALESCE(pg_catalog.sum(
      COALESCE(public._pay_batch_item_source_reservation_amount_ex_vat(item_projection.id), item_projection.frozen_source_amount, item_projection.amount_ex_vat, 0)
    ), 0), 2),
    pg_catalog.count(*) FILTER (WHERE
      item_projection.candidate_id IS DISTINCT FROM v_entry.candidate_id
      OR pg_catalog.upper(pg_catalog.btrim(COALESCE(item_projection.pay_channel::text, ''))) IS DISTINCT FROM v_entry.resolved_pay_channel
      OR COALESCE(item_projection.is_voided, false)
      OR item_projection.finance_case_id IS DISTINCT FROM item_projection.allocation_finance_case_id
      OR item_projection.finance_component_id IS DISTINCT FROM item_projection.allocation_finance_component_id
      OR item_projection.operation_source_key IS DISTINCT FROM COALESCE(item_projection.planned_item_key, item_projection.allocation_source_key)
      OR pg_catalog.round(COALESCE(item_projection.amount_ex_vat, 0), 2) IS DISTINCT FROM pg_catalog.round(
        CASE
          WHEN item_projection.planned_item_amount ~ '^-?[0-9]+(\.[0-9]+)?$' THEN item_projection.planned_item_amount::numeric
          ELSE item_projection.allocation_amount
        END, 2)
    )::integer
  INTO v_item_count, v_item_json, v_source_amount_total, v_bad_item_count
  FROM item_projection;

  WITH reservation_rows AS (
    SELECT DISTINCT reservation.*
    FROM public.banking_pay_operation_candidate_allocation_rows AS allocation_row
    JOIN public.pay_batch_items AS item
      ON item.id = allocation_row.pay_batch_item_id
    JOIN public.pay_advance_reservations AS reservation
      ON reservation.id = item.reservation_id
     AND reservation.pay_batch_item_id = item.id
    WHERE allocation_row.operation_id = p_operation_id
      AND COALESCE(
        NULLIF(allocation_row.allocation_basis_json->>'preview_row_id', ''),
        NULLIF(allocation_row.allocation_basis_json#>>'{line,preview_row_id}', ''),
        NULLIF(allocation_row.allocation_basis_json#>>'{line,preview_row_pk}', '')
      ) = v_entry.materialised_preview_row_id::text
  ), reservation_projection AS (
    SELECT pg_catalog.jsonb_build_object(
      'finance_case_id', reservation.finance_case_id::text,
      'finance_component_id', CASE WHEN reservation.finance_component_id IS NULL THEN NULL ELSE reservation.finance_component_id::text END,
      'frozen_component_classification', CASE WHEN reservation.frozen_component_classification IS NULL THEN NULL ELSE reservation.frozen_component_classification::text END,
      'frozen_component_key_type', reservation.frozen_component_key_type,
      'frozen_component_key_value', reservation.frozen_component_key_value,
      'frozen_rounded_target_amount', (pg_catalog.round(COALESCE(reservation.frozen_rounded_target_amount, 0), 2)::numeric(30,2))::text,
      'reserved_amount', (pg_catalog.round(reservation.reserved_amount, 2)::numeric(30,2))::text,
      'reserved_source_amount', (pg_catalog.round(COALESCE(reservation.reserved_source_amount, 0), 2)::numeric(30,2))::text,
      'status', reservation.status
    ) AS row_json,
    reservation.*
    FROM reservation_rows AS reservation
  )
  SELECT pg_catalog.count(*)::integer,
    COALESCE(pg_catalog.jsonb_agg(reservation_projection.row_json ORDER BY reservation_projection.finance_case_id, reservation_projection.finance_component_id NULLS FIRST), '[]'::jsonb),
    pg_catalog.count(*) FILTER (WHERE
      pg_catalog.upper(pg_catalog.btrim(COALESCE(reservation_projection.status, ''))) <> 'RESERVED'
      OR reservation_projection.pay_batch_id IS DISTINCT FROM v_candidate_scope.pay_batch_id
      OR pg_catalog.round(COALESCE(reservation_projection.reserved_amount, 0), 2)
         IS DISTINCT FROM pg_catalog.round(COALESCE(reservation_projection.frozen_rounded_target_amount, reservation_projection.reserved_amount, 0), 2)
    )::integer
  INTO v_reservation_count, v_reservation_json, v_bad_reservation_count
  FROM reservation_projection;

  v_allocation_json := COALESCE(v_allocation_json, '[]'::jsonb);
  v_item_json := COALESCE(v_item_json, '[]'::jsonb);
  v_reservation_json := COALESCE(v_reservation_json, '[]'::jsonb);
  v_expected_digest := pg_catalog.encode(extensions.digest(pg_catalog.convert_to(v_expected_json::text, 'UTF8'), 'sha256'), 'hex');
  v_allocation_digest := pg_catalog.encode(extensions.digest(pg_catalog.convert_to(v_allocation_json::text, 'UTF8'), 'sha256'), 'hex');
  v_item_digest := pg_catalog.encode(extensions.digest(pg_catalog.convert_to(v_item_json::text, 'UTF8'), 'sha256'), 'hex');
  v_reservation_digest := pg_catalog.encode(extensions.digest(pg_catalog.convert_to(v_reservation_json::text, 'UTF8'), 'sha256'), 'hex');

  v_actual_json := pg_catalog.jsonb_build_object(
    'allocation_digest_sha256', v_allocation_digest,
    'allocation_row_count', v_allocation_count,
    'allocation_total_ex_vat', (pg_catalog.round(v_allocation_total, 2)::numeric(30,2))::text,
    'item_digest_sha256', v_item_digest,
    'item_row_count', v_item_count,
    'reservation_digest_sha256', v_reservation_digest,
    'reservation_row_count', v_reservation_count,
    'source_amount_total_ex_vat', (pg_catalog.round(v_source_amount_total, 2)::numeric(30,2))::text
  );
  v_materialisation_digest := pg_catalog.encode(extensions.digest(pg_catalog.convert_to(v_actual_json::text, 'UTF8'), 'sha256'), 'hex');

  IF v_entry.expected_item_amount_ex_vat::numeric <> 0 AND v_allocation_count = 0 THEN
    v_mismatch := 'DRAFT_PARITY_ACTUAL_OUTPUT_MISSING';
  ELSIF v_bad_certificate_binding_count > 0 THEN
    v_mismatch := 'DRAFT_PARITY_CERTIFICATE_BINDING_MISMATCH';
  ELSIF v_bad_allocation_source_evidence_count > 0 THEN
    v_mismatch := 'DRAFT_PARITY_ALLOCATION_SOURCE_EVIDENCE_MISMATCH';
  ELSIF v_bad_item_source_evidence_count > 0 THEN
    v_mismatch := 'DRAFT_PARITY_ITEM_SOURCE_EVIDENCE_MISMATCH';
  ELSIF v_bad_allocation_count > 0 THEN
    v_mismatch := 'DRAFT_PARITY_ALLOCATION_IDENTITY_MISMATCH';
  ELSIF pg_catalog.round(v_allocation_total, 2) IS DISTINCT FROM pg_catalog.round(v_entry.expected_item_amount_ex_vat::numeric, 2) THEN
    v_mismatch := 'DRAFT_PARITY_ALLOCATION_AMOUNT_MISMATCH';
  ELSIF v_allocation_count > 0 AND v_item_count = 0 THEN
    v_mismatch := 'DRAFT_PARITY_ACTUAL_OUTPUT_MISSING';
  ELSIF v_bad_item_count > 0 THEN
    v_mismatch := 'DRAFT_PARITY_ITEM_IDENTITY_OR_AMOUNT_MISMATCH';
  ELSIF v_bad_reservation_count > 0 THEN
    v_mismatch := 'DRAFT_PARITY_RESERVATION_MISMATCH';
  ELSIF (
      v_entry.expected_reservation_applicability = 'APPLICABLE'
      AND (
        v_entry.expected_reservation_amount_ex_vat IS NULL
        OR v_entry.expected_reservation_source_digest_sha256 IS NULL
        OR v_source_reservation_id_count = 0
        OR v_recomputed_source_reservation_digest IS DISTINCT FROM v_entry.expected_reservation_source_digest_sha256
      )
    ) OR (
      v_entry.expected_reservation_applicability = 'NOT_APPLICABLE'
      AND (
        v_entry.expected_reservation_amount_ex_vat IS NOT NULL
        OR v_entry.expected_reservation_source_digest_sha256 IS NOT NULL
        OR v_source_reservation_id_count <> 0
      )
    ) THEN
    v_mismatch := 'DRAFT_PARITY_SOURCE_RESERVATION_EVIDENCE_MISMATCH';
  ELSIF v_entry.expected_allocated_recovery_amount_ex_vat IS NOT NULL
        -- Workbench headroom is an available recovery magnitude; the already
        -- checked Draft item/allocation amount is the signed payment effect.
        -- Compare magnitude to magnitude without changing either owner.
        AND pg_catalog.round(pg_catalog.abs(v_allocation_total), 2)
            IS DISTINCT FROM pg_catalog.round(pg_catalog.abs(v_entry.expected_allocated_recovery_amount_ex_vat::numeric), 2) THEN
    v_mismatch := 'DRAFT_PARITY_RECOVERY_ALLOCATION_MISMATCH';
  END IF;

  IF v_mismatch IS NOT NULL THEN
    v_status := 'MISMATCH';
  END IF;

  v_expected_bytes := pg_catalog.octet_length(v_expected_json::text);
  v_actual_bytes := pg_catalog.octet_length(v_actual_json::text)
    + pg_catalog.octet_length(v_allocation_json::text)
    + pg_catalog.octet_length(v_item_json::text)
    + pg_catalog.octet_length(v_reservation_json::text);

  IF v_expected_bytes NOT BETWEEN 1 AND 65536 OR v_actual_bytes NOT BETWEEN 1 AND 65536 THEN
    RAISE EXCEPTION 'DRAFT_PARITY_FACT_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'DRAFT_PARITY_FACT_MISMATCH',
        'boundary', 'CANONICAL_BYTE_BUDGET',
        'expected_bytes', v_expected_bytes,
        'actual_bytes', v_actual_bytes
      )::text;
  END IF;

  v_comparison_digest := pg_catalog.encode(extensions.digest(pg_catalog.convert_to(
    pg_catalog.jsonb_build_object(
      'actual', v_actual_json,
      'comparison_status', v_status,
      'constituent_ordinal', p_constituent_ordinal,
      'expected_constituent_digest_sha256', v_entry.constituent_digest_sha256,
      'expected_pre_draft_facts_digest_sha256', v_expected_digest,
      'first_mismatch_code', v_mismatch,
      'operation_id', p_operation_id::text
    )::text,
    'UTF8'
  ), 'sha256'), 'hex');

  SELECT parity_row.*
  INTO v_existing
  FROM private.banking_pay_draft_constituent_parity_results_v8 AS parity_row
  WHERE parity_row.operation_id = p_operation_id
    AND parity_row.constituent_ordinal = p_constituent_ordinal;

  IF FOUND THEN
    IF v_existing.comparison_digest_sha256 IS DISTINCT FROM v_comparison_digest
       OR v_existing.comparison_status IS DISTINCT FROM v_status
       OR v_existing.first_mismatch_code IS DISTINCT FROM v_mismatch THEN
      RAISE EXCEPTION 'DRAFT_PARITY_FACT_MISMATCH'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
          'code', 'DRAFT_PARITY_FACT_MISMATCH',
          'operation_id', p_operation_id,
          'constituent_ordinal', p_constituent_ordinal,
          'reason', 'ACTUAL_OUTPUT_CHANGED_AFTER_COMPARISON'
        )::text;
    END IF;
  ELSE
    INSERT INTO private.banking_pay_draft_constituent_parity_results_v8(
      operation_id, constituent_ordinal, certificate_uuid, candidate_scope_ordinal,
      expected_constituent_digest_sha256, expected_pre_draft_facts_digest_sha256,
      actual_allocation_row_count, actual_allocation_digest_sha256,
      actual_item_row_count, actual_item_digest_sha256,
      actual_reservation_row_count, actual_reservation_digest_sha256,
      actual_materialisation_digest_sha256, expected_canonical_byte_count,
      actual_canonical_byte_count, comparison_status, first_mismatch_code,
      comparison_digest_sha256, compared_at_utc
    ) VALUES (
      p_operation_id, p_constituent_ordinal, v_entry.certificate_uuid,
      v_candidate_scope.candidate_scope_ordinal, v_entry.constituent_digest_sha256,
      v_expected_digest, v_allocation_count, v_allocation_digest,
      v_item_count, v_item_digest, v_reservation_count, v_reservation_digest,
      v_materialisation_digest, v_expected_bytes, v_actual_bytes, v_status,
      v_mismatch, v_comparison_digest, v_now
    );
  END IF;

  RETURN pg_catalog.jsonb_build_object(
    'actual_allocation_row_count', v_allocation_count,
    'actual_item_row_count', v_item_count,
    'actual_reservation_row_count', v_reservation_count,
    'comparison_digest_sha256', v_comparison_digest,
    'comparison_status', v_status,
    'constituent_ordinal', p_constituent_ordinal,
    'first_mismatch_code', v_mismatch
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_draft_constituent_parity_compare_v8(uuid,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_draft_constituent_parity_compare_v8(uuid,integer) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.pay_workbench_draft_constituent_parity_page_v8(
  p_operation_id uuid,
  p_after_constituent_ordinal integer DEFAULT NULL,
  p_limit integer DEFAULT 256,
  p_expected_previous_receipt_sha256 text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO ''
AS $function$
DECLARE
  v_scope private.banking_pay_draft_frozen_certificate_scopes_v8%ROWTYPE;
  v_existing private.banking_pay_draft_frozen_stage_receipts_v8%ROWTYPE;
  v_previous private.banking_pay_draft_frozen_stage_receipts_v8%ROWTYPE;
  v_stage_kind constant text := 'CONSTITUENT_PARITY';
  v_page_sequence integer;
  v_row_count integer := 0;
  v_match_count integer := 0;
  v_mismatch_count integer := 0;
  v_next_after integer;
  v_has_more boolean := false;
  v_request_preimage text;
  v_request_digest text;
  v_page_preimage text;
  v_canonical_bytes integer;
  v_receipt_preimage text;
  v_receipt_digest text;
  v_first_mismatch text;
  v_result jsonb;
BEGIN
  IF p_operation_id IS NULL
     OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 256
     OR (p_after_constituent_ordinal IS NOT NULL AND p_after_constituent_ordinal NOT BETWEEN 0 AND 49999)
     OR (p_expected_previous_receipt_sha256 IS NOT NULL AND p_expected_previous_receipt_sha256 !~ '^[0-9a-f]{64}$') THEN
    RAISE EXCEPTION 'DRAFT_PARITY_REQUEST_CONFLICT'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'DRAFT_PARITY_REQUEST_CONFLICT')::text;
  END IF;

  SELECT frozen_scope.*
  INTO v_scope
  FROM private.banking_pay_draft_frozen_certificate_scopes_v8 AS frozen_scope
  WHERE frozen_scope.operation_id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND OR v_scope.freeze_state <> 'FROZEN' THEN
    RAISE EXCEPTION 'DRAFT_PARITY_NOT_FROZEN'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'DRAFT_PARITY_NOT_FROZEN')::text;
  END IF;

  v_request_preimage := pg_catalog.jsonb_build_object(
    'after_constituent_ordinal', p_after_constituent_ordinal,
    'expected_previous_receipt_sha256', p_expected_previous_receipt_sha256,
    'limit', p_limit,
    'operation_id', p_operation_id::text,
    'stage_kind', v_stage_kind
  )::text;
  v_request_digest := pg_catalog.encode(extensions.digest(pg_catalog.convert_to(v_request_preimage, 'UTF8'), 'sha256'), 'hex');

  SELECT COALESCE(pg_catalog.max(receipt.page_sequence), -1) + 1
  INTO v_page_sequence
  FROM private.banking_pay_draft_frozen_stage_receipts_v8 AS receipt
  WHERE receipt.operation_id = p_operation_id
    AND receipt.stage_kind = v_stage_kind;

  SELECT receipt.*
  INTO v_existing
  FROM private.banking_pay_draft_frozen_stage_receipts_v8 AS receipt
  WHERE receipt.operation_id = p_operation_id
    AND receipt.stage_kind = v_stage_kind
    AND receipt.after_ordinal IS NOT DISTINCT FROM p_after_constituent_ordinal;

  IF FOUND THEN
    v_page_sequence := v_existing.page_sequence;
    IF v_existing.requested_limit IS DISTINCT FROM p_limit
       OR v_existing.expected_previous_receipt_sha256 IS DISTINCT FROM p_expected_previous_receipt_sha256
       OR v_existing.request_preimage_digest_sha256 IS DISTINCT FROM v_request_digest THEN
      RAISE EXCEPTION 'DRAFT_PARITY_REQUEST_CONFLICT'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'DRAFT_PARITY_REQUEST_CONFLICT')::text;
    END IF;
  ELSIF v_page_sequence = 0 THEN
    IF p_after_constituent_ordinal IS NOT NULL OR p_expected_previous_receipt_sha256 IS NOT NULL THEN
      RAISE EXCEPTION 'DRAFT_PARITY_PAGE_GAP'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'DRAFT_PARITY_PAGE_GAP')::text;
    END IF;
  ELSE
    SELECT receipt.*
    INTO v_previous
    FROM private.banking_pay_draft_frozen_stage_receipts_v8 AS receipt
    WHERE receipt.operation_id = p_operation_id
      AND receipt.stage_kind = v_stage_kind
      AND receipt.page_sequence = v_page_sequence - 1;
    IF NOT FOUND
       OR v_previous.next_after_ordinal IS DISTINCT FROM p_after_constituent_ordinal
       OR v_previous.receipt_digest_sha256 IS DISTINCT FROM p_expected_previous_receipt_sha256
       OR NOT v_previous.has_more THEN
      RAISE EXCEPTION 'DRAFT_PARITY_PAGE_GAP'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'DRAFT_PARITY_PAGE_GAP')::text;
    END IF;
  END IF;

  DROP TABLE IF EXISTS pg_temp.tmp_banking_pay_draft_parity_page_v8;
  CREATE TEMPORARY TABLE pg_temp.tmp_banking_pay_draft_parity_page_v8(
    constituent_ordinal integer PRIMARY KEY,
    result_json jsonb NOT NULL
  ) ON COMMIT DROP;

  INSERT INTO pg_temp.tmp_banking_pay_draft_parity_page_v8(constituent_ordinal, result_json)
  SELECT frozen_ref.constituent_ordinal,
    private.pay_workbench_draft_constituent_parity_compare_v8(p_operation_id, frozen_ref.constituent_ordinal)
  FROM private.banking_pay_draft_frozen_constituent_refs_v8 AS frozen_ref
  WHERE frozen_ref.operation_id = p_operation_id
    AND frozen_ref.constituent_ordinal > COALESCE(p_after_constituent_ordinal, -1)
  ORDER BY frozen_ref.constituent_ordinal
  LIMIT p_limit;

  SELECT pg_catalog.count(*)::integer,
    pg_catalog.count(*) FILTER (WHERE page_row.result_json->>'comparison_status' = 'MATCH')::integer,
    pg_catalog.count(*) FILTER (WHERE page_row.result_json->>'comparison_status' = 'MISMATCH')::integer,
    pg_catalog.max(page_row.constituent_ordinal),
    MIN(page_row.result_json->>'first_mismatch_code') FILTER (WHERE page_row.result_json->>'comparison_status' = 'MISMATCH'),
    '[' || COALESCE(pg_catalog.string_agg(page_row.result_json::text, ',' ORDER BY page_row.constituent_ordinal), '') || ']'
  INTO v_row_count, v_match_count, v_mismatch_count, v_next_after, v_first_mismatch, v_page_preimage
  FROM pg_temp.tmp_banking_pay_draft_parity_page_v8 AS page_row;

  v_has_more := EXISTS (
    SELECT 1
    FROM private.banking_pay_draft_frozen_constituent_refs_v8 AS frozen_ref
    WHERE frozen_ref.operation_id = p_operation_id
      AND frozen_ref.constituent_ordinal > COALESCE(v_next_after, p_after_constituent_ordinal, -1)
  );
  v_canonical_bytes := pg_catalog.octet_length(v_page_preimage);
  IF v_canonical_bytes > 524288 THEN
    RAISE EXCEPTION 'DRAFT_PARITY_FACT_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'DRAFT_PARITY_FACT_MISMATCH', 'boundary', 'PAGE_BYTE_BUDGET'
      )::text;
  END IF;

  v_receipt_preimage := pg_catalog.jsonb_build_object(
    'after_constituent_ordinal', p_after_constituent_ordinal,
    'canonical_byte_count', v_canonical_bytes,
    'expected_previous_receipt_sha256', p_expected_previous_receipt_sha256,
    'first_mismatch_code', v_first_mismatch,
    'has_more', v_has_more,
    'match_count', v_match_count,
    'mismatch_count', v_mismatch_count,
    'next_after_constituent_ordinal', v_next_after,
    'operation_id', p_operation_id::text,
    'page_preimage_digest_sha256', pg_catalog.encode(extensions.digest(pg_catalog.convert_to(v_page_preimage, 'UTF8'), 'sha256'), 'hex'),
    'page_sequence', v_page_sequence,
    'request_preimage_digest_sha256', v_request_digest,
    'row_count', v_row_count,
    'stage_kind', v_stage_kind
  )::text;
  v_receipt_digest := pg_catalog.encode(extensions.digest(pg_catalog.convert_to(v_receipt_preimage, 'UTF8'), 'sha256'), 'hex');

  IF v_existing.operation_id IS NOT NULL THEN
    IF v_existing.receipt_digest_sha256 IS DISTINCT FROM v_receipt_digest
       OR v_existing.row_count IS DISTINCT FROM v_row_count
       OR v_existing.canonical_byte_count IS DISTINCT FROM v_canonical_bytes
       OR v_existing.next_after_ordinal IS DISTINCT FROM v_next_after
       OR v_existing.has_more IS DISTINCT FROM v_has_more THEN
      RAISE EXCEPTION 'DRAFT_PARITY_FACT_MISMATCH'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
          'code', 'DRAFT_PARITY_FACT_MISMATCH', 'reason', 'PAGE_REPLAY_CHANGED'
        )::text;
    END IF;
  ELSE
    INSERT INTO private.banking_pay_draft_frozen_stage_receipts_v8(
      operation_id, stage_kind, page_sequence, after_ordinal, requested_limit,
      expected_previous_receipt_sha256, request_preimage_digest_sha256,
      row_count, canonical_byte_count, next_after_ordinal, has_more,
      terminal_sentinel_present, receipt_digest_sha256, stage_status
    ) VALUES (
      p_operation_id, v_stage_kind, v_page_sequence, p_after_constituent_ordinal, p_limit,
      p_expected_previous_receipt_sha256, v_request_digest, v_row_count,
      v_canonical_bytes, v_next_after, v_has_more, true,
      v_receipt_digest, CASE WHEN v_has_more THEN 'COMMITTED' ELSE 'TERMINAL' END
    );
  END IF;

  v_result := pg_catalog.jsonb_build_object(
    'canonical_byte_count', v_canonical_bytes,
    'first_mismatch_code', v_first_mismatch,
    'has_more', v_has_more,
    'match_count', v_match_count,
    'mismatch_count', v_mismatch_count,
    'next_after_constituent_ordinal', v_next_after,
    'operation_id', p_operation_id,
    'page_receipt_digest_sha256', v_receipt_digest,
    'page_sequence', v_page_sequence,
    'replayed', v_existing.operation_id IS NOT NULL,
    'row_count', v_row_count,
    'stage_kind', v_stage_kind
  );
  RETURN v_result;
END;
$function$;

ALTER FUNCTION public.pay_workbench_draft_constituent_parity_page_v8(uuid,integer,integer,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_draft_constituent_parity_page_v8(uuid,integer,integer,text) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_draft_constituent_parity_page_v8(uuid,integer,integer,text) TO service_role;

COMMENT ON FUNCTION private.pay_workbench_draft_constituent_parity_compare_v8(uuid,integer) IS
  'H2-owned comparison of one certified constituent with unchanged Draft allocation/item/reservation artifacts; no economic or policy calculation.';
COMMENT ON FUNCTION public.pay_workbench_draft_constituent_parity_page_v8(uuid,integer,integer,text) IS
  'Service-only bounded constituent parity page (maximum 256 rows), with chained replay-safe receipts and typed mismatch evidence.';
