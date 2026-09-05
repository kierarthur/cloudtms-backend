\set ON_ERROR_STOP on

-- Assertion tail consumed by
-- tests/banking-pay-draft-v8-finance-lifecycle-headroom-p4.test.cjs.
-- The driver installs the current V8 rollback bridge, creates one canonical
-- source scenario, runs the bounded Draft state machine and then places this
-- block before the one outer ROLLBACK. Direct execution fails closed because
-- pg_temp.h2_p4_case_contract is deliberately driver-owned.
BEGIN;
SET LOCAL jit = off;
SET LOCAL statement_timeout = '15s';
SET LOCAL lock_timeout = '1500ms';
SET LOCAL idle_in_transaction_session_timeout = '30s';

-- H2_P4_ASSERTION_BEGIN
DO $h2_p4_finance_lifecycle_assertions$
DECLARE
  v_case pg_temp.h2_p4_case_contract%ROWTYPE;
  v_operation_id uuid := '43000000-0000-4000-8000-000000000100';
  v_session_id uuid := '43000000-0000-4000-8000-000000000005';
  v_unrelated_candidate_id uuid := '47000000-0000-4000-8000-000000000999';
  v_preview_json jsonb;
  v_item_json jsonb;
  v_preview_count integer;
  v_selected_preview_count integer;
  v_certificate_entry_count integer;
  v_frozen_count integer;
  v_allocation_count integer;
  v_item_count integer;
  v_reservation_amount numeric;
  v_active_reservation_amount numeric;
  v_bank_transfer_count integer;
  v_remittance_scope_count integer;
  v_unrelated_after text;
BEGIN
  SELECT * INTO STRICT v_case FROM pg_temp.h2_p4_case_contract;

  SELECT count(*),
         count(*) FILTER (WHERE preview.selected AND preview.status='READY'
                           AND preview.selection_state='SELECTED'),
         COALESCE(jsonb_agg(jsonb_build_object(
           'visible_alias',preview.row_json->>'line_type',
           'amount_ex_vat',CASE WHEN COALESCE(preview.row_json->>'amount_ex_vat','') ~ '^-?[0-9]+(\.[0-9]+)?$'
             THEN to_char(round((preview.row_json->>'amount_ex_vat')::numeric,2),'FM999999990.00') ELSE NULL END,
           'amount_vat',CASE WHEN COALESCE(preview.row_json->>'amount_vat','') ~ '^-?[0-9]+(\.[0-9]+)?$'
             THEN to_char(round((preview.row_json->>'amount_vat')::numeric,2),'FM999999990.00') ELSE NULL END,
           'amount_inc_vat',CASE WHEN COALESCE(preview.row_json->>'amount_inc_vat','') ~ '^-?[0-9]+(\.[0-9]+)?$'
             THEN to_char(round((preview.row_json->>'amount_inc_vat')::numeric,2),'FM999999990.00') ELSE NULL END,
           'paye_treatment',preview.row_json->>'paye_treatment',
           'pay_channel',preview.row_json->>'pay_channel',
           'selected',preview.selected,
           'status',preview.status,
           'selection_state',preview.selection_state,
           'presentation_reason',preview.row_json->>'presentation_reason',
           'finance_case_id',preview.row_json->>'finance_case_id',
           'carry_forward_id',preview.row_json->>'manual_adjustment_carry_forward_id'
         ) ORDER BY preview.row_ordinal,preview.id),'[]'::jsonb)
  INTO v_preview_count,v_selected_preview_count,v_preview_json
  FROM public.banking_pay_workbench_preview_rows AS preview
  WHERE preview.session_id=v_session_id
    AND (
      (preview.row_json->>'finance_case_id')::uuid=ANY(v_case.target_finance_case_ids)
      OR (v_case.target_carry_forward_id IS NOT NULL
          AND preview.row_json->>'manual_adjustment_carry_forward_id'=v_case.target_carry_forward_id::text)
    );

  SELECT count(*)
  INTO v_certificate_entry_count
  FROM private.banking_pay_workbench_settled_certificate_entries_v8 AS entry
  JOIN private.banking_pay_draft_frozen_certificate_scopes_v8 AS frozen
    ON frozen.certificate_uuid=entry.certificate_uuid
   AND frozen.operation_id=v_operation_id AND frozen.freeze_state='FROZEN'
  JOIN public.banking_pay_workbench_preview_rows AS preview
    ON preview.id=entry.materialised_preview_row_id
  WHERE (preview.row_json->>'finance_case_id')::uuid=ANY(v_case.target_finance_case_ids)
     OR (v_case.target_carry_forward_id IS NOT NULL
         AND preview.row_json->>'manual_adjustment_carry_forward_id'=v_case.target_carry_forward_id::text);

  SELECT count(*)
  INTO v_frozen_count
  FROM private.banking_pay_draft_frozen_constituent_refs_v8 AS frozen_ref
  JOIN private.banking_pay_workbench_settled_certificate_entries_v8 AS entry
    ON entry.certificate_uuid=frozen_ref.certificate_uuid
   AND entry.constituent_ordinal=frozen_ref.constituent_ordinal
  JOIN public.banking_pay_workbench_preview_rows AS preview
    ON preview.id=entry.materialised_preview_row_id
  WHERE frozen_ref.operation_id=v_operation_id
    AND ((preview.row_json->>'finance_case_id')::uuid=ANY(v_case.target_finance_case_ids)
      OR (v_case.target_carry_forward_id IS NOT NULL
          AND preview.row_json->>'manual_adjustment_carry_forward_id'=v_case.target_carry_forward_id::text));

  SELECT count(*)
  INTO v_allocation_count
  FROM public.banking_pay_operation_candidate_allocation_rows AS allocation
  WHERE allocation.operation_id=v_operation_id
    AND (
      NULLIF(allocation.allocation_basis_json#>>'{line,finance_case_id}','')::uuid
        =ANY(v_case.target_finance_case_ids)
      OR NULLIF(allocation.allocation_basis_json->>'finance_case_id','')::uuid
        =ANY(v_case.target_finance_case_ids)
      OR (v_case.target_carry_forward_id IS NOT NULL AND (
        allocation.allocation_basis_json#>>'{line,manual_adjustment_carry_forward_id}'
          =v_case.target_carry_forward_id::text
        OR allocation.allocation_basis_json->>'manual_adjustment_carry_forward_id'
          =v_case.target_carry_forward_id::text
      ))
    );

  SELECT count(*),COALESCE(jsonb_agg(jsonb_build_object(
      'item_type',item.item_type,
      'finance_case_id',item.finance_case_id,'source_ref',item.source_ref,
      'amount_ex_vat',to_char(round(item.amount_ex_vat,2),'FM999999990.00'),
      'amount_vat',to_char(round(coalesce(item.amount_vat,0),2),'FM999999990.00'),
      'amount_inc_vat',to_char(round(coalesce(item.amount_inc_vat,item.amount_ex_vat),2),'FM999999990.00'),
      'pay_channel',item.pay_channel,'paye_treatment',item.paye_treatment,
      'is_voided',item.is_voided,'has_reservation',item.reservation_id IS NOT NULL
    ) ORDER BY item.item_type,item.finance_case_id,item.id),'[]'::jsonb)
  INTO v_item_count,v_item_json
  FROM public.pay_batch_items AS item
  JOIN public.pay_batch_candidates AS batch_candidate
    ON batch_candidate.id=item.pay_batch_candidate_id
  JOIN public.pay_batches AS batch ON batch.id=batch_candidate.pay_batch_id
  WHERE batch.source_workbench_session_id=v_session_id
    AND batch_candidate.candidate_id=v_case.target_candidate_id
    AND COALESCE(item.is_voided,false)=false
    AND (item.finance_case_id=ANY(v_case.target_finance_case_ids)
      OR (v_case.target_carry_forward_id IS NOT NULL
          AND item.source_ref='carry_forward:'||v_case.target_carry_forward_id::text));

  SELECT round(COALESCE(sum(reservation.reserved_amount)
           FILTER (WHERE EXISTS (
             SELECT 1
             FROM public.pay_batches AS reservation_batch
             WHERE reservation_batch.id=reservation.pay_batch_id
               AND reservation_batch.source_workbench_session_id=v_session_id
           )),0),2),
         round(COALESCE(sum(reservation.reserved_amount)
           FILTER (WHERE reservation.status IN ('RESERVED','COMMITTED')),0),2)
  INTO v_reservation_amount,v_active_reservation_amount
  FROM public.pay_advance_reservations AS reservation
  WHERE reservation.finance_case_id=ANY(v_case.target_finance_case_ids);

  IF v_preview_count<>(v_case.expected_json->>'preview_count')::integer
     OR v_selected_preview_count<>(v_case.expected_json->>'selected_preview_count')::integer
     OR v_certificate_entry_count<>(v_case.expected_json->>'certificate_entry_count')::integer
     OR v_frozen_count<>(v_case.expected_json->>'certificate_entry_count')::integer
     OR v_allocation_count<>(v_case.expected_json->>'item_count')::integer
     OR v_item_count<>(v_case.expected_json->>'item_count')::integer
     OR round(v_reservation_amount,2)<>(v_case.expected_json->>'new_reservation_amount_ex_vat')::numeric
     OR round(v_active_reservation_amount,2)<>(v_case.expected_json->>'active_reservation_amount_ex_vat')::numeric THEN
    RAISE EXCEPTION 'H2_P4_CARDINALITY_OR_RESERVATION_MISMATCH:%',jsonb_build_object(
      'class_id',v_case.class_id,'channel',v_case.pay_channel,
      'preview_count',v_preview_count,'selected_preview_count',v_selected_preview_count,
      'certificate_entry_count',v_certificate_entry_count,'frozen_count',v_frozen_count,
      'allocation_count',v_allocation_count,'item_count',v_item_count,
      'new_reservation_amount',v_reservation_amount,
      'active_reservation_amount',v_active_reservation_amount,
      'preview',v_preview_json,'items',v_item_json,'expected',v_case.expected_json);
  END IF;

  IF v_preview_json IS DISTINCT FROM v_case.expected_json->'preview_rows' THEN
    RAISE EXCEPTION 'H2_P4_PREVIEW_IDENTITY_OR_POLICY_MISMATCH:%',jsonb_build_object(
      'class_id',v_case.class_id,'channel',v_case.pay_channel,
      'observed',v_preview_json,'expected',v_case.expected_json->'preview_rows');
  END IF;

  IF v_item_json IS DISTINCT FROM v_case.expected_json->'item_rows' THEN
    RAISE EXCEPTION 'H2_P4_DRAFT_ITEM_POLICY_MISMATCH:%',jsonb_build_object(
      'class_id',v_case.class_id,'channel',v_case.pay_channel,
      'observed',v_item_json,'expected',v_case.expected_json->'item_rows');
  END IF;

  IF v_case.target_carry_forward_id IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM public.pay_manual_adjustment_carry_forwards AS carry
    WHERE carry.id=v_case.target_carry_forward_id
      AND carry.status='RESERVED_IN_DRAFT'
      AND carry.target_pay_batch_id IS NOT NULL
      AND carry.target_pay_batch_item_id IS NOT NULL
      AND carry.target_operation_source_key IS NOT NULL
  ) THEN
    RAISE EXCEPTION 'H2_P4_CARRY_FORWARD_RESERVATION_STATE_MISMATCH:%',v_case.class_id;
  END IF;

  IF (v_case.expected_json->>'certificate_entry_count')::integer>0
     AND NOT EXISTS (
    SELECT 1 FROM public.pay_batch_items AS ordinary_item
    JOIN public.pay_batch_candidates AS ordinary_candidate
      ON ordinary_candidate.id=ordinary_item.pay_batch_candidate_id
    JOIN public.pay_batches AS ordinary_batch ON ordinary_batch.id=ordinary_candidate.pay_batch_id
    WHERE ordinary_batch.source_workbench_session_id=v_session_id
      AND ordinary_candidate.candidate_id=v_case.target_candidate_id
      AND ordinary_item.timesheet_id IS NOT NULL
      AND COALESCE(ordinary_item.is_voided,false)=false
  ) THEN
    RAISE EXCEPTION 'H2_P4_ORDINARY_CONTROL_MISSING:%',v_case.class_id;
  END IF;

  SELECT private.pay_workbench_settled_certificate_sha256_text_v8(
    private.pay_workbench_settled_certificate_stable_stringify_v8(to_jsonb(candidate_row)))
  INTO STRICT v_unrelated_after
  FROM public.candidates AS candidate_row WHERE candidate_row.id=v_unrelated_candidate_id;
  IF v_unrelated_after IS DISTINCT FROM
       (SELECT digest_sha256 FROM pg_temp.h2_p4_unrelated_before)
     OR EXISTS (SELECT 1 FROM public.banking_pay_operation_candidate_scope
                WHERE operation_id=v_operation_id AND candidate_id=v_unrelated_candidate_id)
     OR EXISTS (
       SELECT 1 FROM public.pay_batch_candidates AS other_candidate
       JOIN public.pay_batches AS other_batch ON other_batch.id=other_candidate.pay_batch_id
       WHERE other_batch.source_workbench_session_id=v_session_id
         AND other_candidate.candidate_id=v_unrelated_candidate_id
     ) THEN
    RAISE EXCEPTION 'H2_P4_UNRELATED_CANDIDATE_CHANGED:%',v_case.class_id;
  END IF;

  SELECT count(*) INTO v_bank_transfer_count
  FROM public.pay_bank_transfers AS transfer
  JOIN public.pay_batches AS batch ON batch.id=transfer.pay_batch_id
  WHERE batch.source_workbench_session_id=v_session_id;
  SELECT count(*) INTO v_remittance_scope_count
  FROM public.banking_pay_operation_remittance_scope
  WHERE operation_id=v_operation_id;
  IF v_bank_transfer_count<>0 OR v_remittance_scope_count<>0
     OR ((v_case.expected_json->>'certificate_entry_count')::integer>0
         AND NOT EXISTS (SELECT 1 FROM public.banking_pay_operations
                         WHERE id=v_operation_id AND phase='POST_CREATE_REFRESH'))
     OR ((v_case.expected_json->>'certificate_entry_count')::integer=0
         AND EXISTS (SELECT 1 FROM public.banking_pay_operations
                     WHERE id=v_operation_id)) THEN
    RAISE EXCEPTION 'H2_P4_EXTERNAL_ACTION_OR_TERMINAL_PHASE_MISMATCH:%',
      jsonb_build_object('bank_transfers',v_bank_transfer_count,
                         'remittance_scope',v_remittance_scope_count);
  END IF;

  RAISE NOTICE 'H2_P4_FINANCE_LIFECYCLE_RUNTIME_PASS=%',jsonb_build_object(
    'class_id',v_case.class_id,'pay_channel',v_case.pay_channel,
    'preview_count',v_preview_count,'selected_preview_count',v_selected_preview_count,
    'certificate_entry_count',v_certificate_entry_count,
    'allocation_count',v_allocation_count,'item_count',v_item_count,
    'new_reservation_amount_ex_vat',v_reservation_amount,
    'active_reservation_amount_ex_vat',v_active_reservation_amount,
    'unrelated_candidate_unchanged',true,'transaction_outcome','ROLLBACK');
END;
$h2_p4_finance_lifecycle_assertions$;
-- H2_P4_ASSERTION_END

ROLLBACK;
