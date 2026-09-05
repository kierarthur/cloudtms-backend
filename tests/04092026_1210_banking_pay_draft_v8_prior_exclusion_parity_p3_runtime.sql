\set ON_ERROR_STOP on

-- This is the assertion tail consumed by
-- tests/banking-pay-draft-v8-prior-exclusion-parity-p3.test.cjs --run-runtime.
-- The driver places the block between H2_P3_ASSERTION_BEGIN/END after the
-- repository's existing canonical producer, V8 bridge and bounded Draft
-- orchestration setup. Direct execution deliberately fails closed because the
-- task-local pg_temp contract tables do not exist.
BEGIN;
SET LOCAL jit = off;
SET LOCAL statement_timeout = '15s';
SET LOCAL lock_timeout = '1500ms';
SET LOCAL idle_in_transaction_session_timeout = '30s';

-- H2_P3_ASSERTION_BEGIN
DO $h2_p3_prior_exclusion_assertions$
DECLARE
  v_case pg_temp.h2_p3_case_contract%ROWTYPE;
  v_operation_id uuid := '43000000-0000-4000-8000-000000000100';
  v_session_id uuid := '43000000-0000-4000-8000-000000000005';
  v_unrelated_candidate_id uuid := '45000000-0000-4000-8000-000000000999';
  v_certificate_uuid uuid;
  v_target_preview_id uuid;
  v_target_stable_digest text;
  v_target_entry_count integer;
  v_target_frozen_count integer;
  v_target_allocation_count integer;
  v_target_item_count integer;
  v_target_universe_count integer;
  v_control_selected_count integer;
  v_control_allocation_count integer;
  v_control_item_count integer;
  v_control_parity_count integer;
  v_target_parity_count integer;
  v_bank_transfer_count integer;
  v_remittance_scope_count integer;
  v_unrelated_after text;
  v_entry private.banking_pay_workbench_settled_certificate_entries_v8%ROWTYPE;
BEGIN
  SELECT * INTO STRICT v_case FROM pg_temp.h2_p3_case_contract;
  SELECT frozen_scope.certificate_uuid
  INTO STRICT v_certificate_uuid
  FROM private.banking_pay_draft_frozen_certificate_scopes_v8 AS frozen_scope
  WHERE frozen_scope.operation_id=v_operation_id
    AND frozen_scope.freeze_state='FROZEN';
  SELECT preview.id,
         private.pay_workbench_settled_certificate_sha256_text_v8(
           private.pay_workbench_settled_certificate_stable_stringify_v8(
             pg_catalog.jsonb_build_object(
               'preview_row_id',preview.id,'row_key',preview.row_key,
               'section',preview.section,'row_ordinal',preview.row_ordinal,
               'status',preview.status,'selection_state',preview.selection_state,
               'selected',preview.selected,
               'selection_identity_digest',preview.row_json->>'selection_identity_digest'
             )
           )
         )
  INTO STRICT v_target_preview_id,v_target_stable_digest
  FROM public.banking_pay_workbench_preview_rows AS preview
  WHERE preview.session_id=v_session_id
    AND preview.row_json->'h2_p3_target'='true'::jsonb;

  SELECT count(*) INTO v_target_entry_count
  FROM private.banking_pay_workbench_settled_certificate_entries_v8 AS entry
  WHERE entry.certificate_uuid=v_certificate_uuid
    AND entry.materialised_preview_row_id=v_target_preview_id;
  SELECT count(*) INTO v_target_frozen_count
  FROM private.banking_pay_draft_frozen_constituent_refs_v8 AS frozen_ref
  JOIN private.banking_pay_workbench_settled_certificate_entries_v8 AS entry
    ON entry.certificate_uuid=frozen_ref.certificate_uuid
   AND entry.constituent_ordinal=frozen_ref.constituent_ordinal
  WHERE frozen_ref.operation_id=v_operation_id
    AND entry.materialised_preview_row_id=v_target_preview_id;
  SELECT count(*) INTO v_target_allocation_count
  FROM public.banking_pay_operation_candidate_allocation_rows AS allocation
  WHERE allocation.operation_id=v_operation_id
    AND COALESCE(
      NULLIF(allocation.allocation_basis_json->>'preview_row_id',''),
      NULLIF(allocation.allocation_basis_json#>>'{line,preview_row_id}',''),
      NULLIF(allocation.allocation_basis_json#>>'{line,preview_row_pk}','')
    )=v_target_preview_id::text;
  SELECT count(*) INTO v_target_item_count
  FROM public.banking_pay_operation_candidate_allocation_rows AS allocation
  JOIN public.pay_batch_items AS item ON item.id=allocation.pay_batch_item_id
  WHERE allocation.operation_id=v_operation_id
    AND COALESCE(
      NULLIF(allocation.allocation_basis_json->>'preview_row_id',''),
      NULLIF(allocation.allocation_basis_json#>>'{line,preview_row_id}',''),
      NULLIF(allocation.allocation_basis_json#>>'{line,preview_row_pk}','')
    )=v_target_preview_id::text
    AND COALESCE(item.is_voided,false)=false;
  SELECT count(*) INTO v_target_universe_count
  FROM private.banking_pay_workbench_settled_certificate_universe_members_v8 AS member
  WHERE member.certificate_uuid=v_certificate_uuid
    AND member.universe_kind=v_case.expected_universe
    AND member.stable_identity_digest_sha256=v_target_stable_digest;

  SELECT count(*) INTO v_control_selected_count
  FROM private.banking_pay_workbench_settled_certificate_entries_v8 AS entry
  WHERE entry.certificate_uuid=v_certificate_uuid
    AND entry.materialised_preview_row_id<>v_target_preview_id;
  SELECT count(*) INTO v_control_allocation_count
  FROM public.banking_pay_operation_candidate_allocation_rows AS allocation
  WHERE allocation.operation_id=v_operation_id
    AND COALESCE(
      NULLIF(allocation.allocation_basis_json->>'preview_row_id',''),
      NULLIF(allocation.allocation_basis_json#>>'{line,preview_row_id}',''),
      NULLIF(allocation.allocation_basis_json#>>'{line,preview_row_pk}','')
    ) IS DISTINCT FROM v_target_preview_id::text;
  SELECT count(*) INTO v_control_item_count
  FROM public.banking_pay_operation_candidate_allocation_rows AS allocation
  JOIN public.pay_batch_items AS item ON item.id=allocation.pay_batch_item_id
  WHERE allocation.operation_id=v_operation_id
    AND COALESCE(
      NULLIF(allocation.allocation_basis_json->>'preview_row_id',''),
      NULLIF(allocation.allocation_basis_json#>>'{line,preview_row_id}',''),
      NULLIF(allocation.allocation_basis_json#>>'{line,preview_row_pk}','')
    ) IS DISTINCT FROM v_target_preview_id::text
    AND COALESCE(item.is_voided,false)=false;
  SELECT count(*) INTO v_control_parity_count
  FROM private.banking_pay_draft_constituent_parity_results_v8 AS parity
  JOIN private.banking_pay_workbench_settled_certificate_entries_v8 AS entry
    ON entry.certificate_uuid=parity.certificate_uuid
   AND entry.constituent_ordinal=parity.constituent_ordinal
  WHERE parity.operation_id=v_operation_id
    AND parity.comparison_status='MATCH'
    AND entry.materialised_preview_row_id<>v_target_preview_id;
  SELECT count(*) INTO v_target_parity_count
  FROM private.banking_pay_draft_constituent_parity_results_v8 AS parity
  JOIN private.banking_pay_workbench_settled_certificate_entries_v8 AS entry
    ON entry.certificate_uuid=parity.certificate_uuid
   AND entry.constituent_ordinal=parity.constituent_ordinal
  WHERE parity.operation_id=v_operation_id
    AND parity.comparison_status='MATCH'
    AND entry.materialised_preview_row_id=v_target_preview_id;

  IF v_case.target_selected THEN
    SELECT entry.* INTO STRICT v_entry
    FROM private.banking_pay_workbench_settled_certificate_entries_v8 AS entry
    WHERE entry.certificate_uuid=v_certificate_uuid
      AND entry.materialised_preview_row_id=v_target_preview_id;
    IF v_target_entry_count<>1 OR v_target_frozen_count<>1
       OR v_target_allocation_count<>1 OR v_target_item_count<>1
       OR v_target_parity_count<>1 OR v_target_universe_count<>0
       OR v_entry.candidate_id IS DISTINCT FROM v_case.target_candidate_id
       OR v_entry.resolved_pay_channel IS DISTINCT FROM v_case.pay_channel
       OR v_entry.amount_sign<>'POSITIVE'
       OR v_entry.canonical_amount_ex_vat IS DISTINCT FROM v_case.expected_amount_ex_vat
       OR v_entry.prior_paid_amount_ex_vat IS DISTINCT FROM v_case.expected_prior_paid_ex_vat
       OR COALESCE(v_entry.expected_reservation_amount_ex_vat,'0.00')
            IS DISTINCT FROM v_case.expected_source_reservation_ex_vat
       OR (v_case.expected_supersession_treatment IS NOT NULL
           AND v_entry.supersession_treatment IS DISTINCT FROM v_case.expected_supersession_treatment)
       OR NOT EXISTS (
         SELECT 1
         FROM public.banking_pay_operation_candidate_allocation_rows AS allocation
         JOIN public.pay_batch_items AS item ON item.id=allocation.pay_batch_item_id
         JOIN public.pay_batch_candidates AS batch_candidate ON batch_candidate.id=item.pay_batch_candidate_id
         WHERE allocation.operation_id=v_operation_id
           AND COALESCE(
             NULLIF(allocation.allocation_basis_json->>'preview_row_id',''),
             NULLIF(allocation.allocation_basis_json#>>'{line,preview_row_id}',''),
             NULLIF(allocation.allocation_basis_json#>>'{line,preview_row_pk}','')
           )=v_target_preview_id::text
           AND allocation.candidate_id=v_case.target_candidate_id
           AND allocation.pay_channel=v_case.pay_channel
           AND round(allocation.allocated_amount,2)=v_case.expected_amount_ex_vat::numeric
           AND allocation.status='ITEM_CREATED'
           AND batch_candidate.candidate_id=v_case.target_candidate_id
           AND item.pay_channel=v_case.pay_channel
           AND round(item.amount_ex_vat,2)=v_case.expected_amount_ex_vat::numeric
           AND item.timesheet_id=v_entry.timesheet_id
           AND item.frozen_component_key_type=v_entry.economic_key_type
           AND item.frozen_component_key_value=v_entry.economic_key_value
       ) THEN
      RAISE EXCEPTION 'H2_P3_SELECTED_DRAFT_OUTPUT_MISMATCH:%',pg_catalog.jsonb_build_object(
        'case_id',v_case.class_id,'channel',v_case.pay_channel,
        'entry_count',v_target_entry_count,'frozen_count',v_target_frozen_count,
        'allocation_count',v_target_allocation_count,'item_count',v_target_item_count,
        'parity_count',v_target_parity_count,'entry',pg_catalog.to_jsonb(v_entry)
      );
    END IF;
  ELSE
    IF v_target_entry_count<>0 OR v_target_frozen_count<>0
       OR v_target_allocation_count<>0 OR v_target_item_count<>0
       OR v_target_parity_count<>0 OR v_target_universe_count<>1
       OR NOT EXISTS (
         SELECT 1 FROM pg_temp.h2_p3_stale_rejection
          WHERE sqlstate='P0001'
           AND error_message=v_case.expected_stale_selection_error
           AND draft_row_delta=0 AND financial_row_delta=0
       ) THEN
      RAISE EXCEPTION 'H2_P3_EXCLUSION_OR_TYPED_REJECTION_MISMATCH:%',pg_catalog.jsonb_build_object(
        'case_id',v_case.class_id,'channel',v_case.pay_channel,
        'entry_count',v_target_entry_count,'frozen_count',v_target_frozen_count,
        'allocation_count',v_target_allocation_count,'item_count',v_target_item_count,
        'parity_count',v_target_parity_count,'universe_count',v_target_universe_count,
        'stale_rejection',(SELECT pg_catalog.to_jsonb(rejection) FROM pg_temp.h2_p3_stale_rejection rejection)
      );
    END IF;
  END IF;

  IF v_control_selected_count<1
     OR v_control_allocation_count<>v_control_selected_count
     OR v_control_item_count<>v_control_selected_count
     OR v_control_parity_count<>v_control_selected_count THEN
    RAISE EXCEPTION 'H2_P3_CONTROL_CONSTITUENT_CHANGED:%',pg_catalog.jsonb_build_object(
      'case_id',v_case.class_id,'channel',v_case.pay_channel,
      'selected',v_control_selected_count,'allocations',v_control_allocation_count,
      'items',v_control_item_count,'parity',v_control_parity_count
    );
  END IF;

  SELECT private.pay_workbench_settled_certificate_sha256_text_v8(
    private.pay_workbench_settled_certificate_stable_stringify_v8(pg_catalog.to_jsonb(candidate_row))
  ) INTO STRICT v_unrelated_after
  FROM public.candidates AS candidate_row WHERE candidate_row.id=v_unrelated_candidate_id;
  IF v_unrelated_after IS DISTINCT FROM (SELECT digest_sha256 FROM pg_temp.h2_p3_unrelated_before)
     OR EXISTS (
       SELECT 1 FROM public.banking_pay_operation_candidate_scope
       WHERE operation_id=v_operation_id AND candidate_id=v_unrelated_candidate_id
     )
     OR EXISTS (
       SELECT 1 FROM public.pay_batch_candidates AS batch_candidate
       JOIN public.pay_batches AS batch ON batch.id=batch_candidate.pay_batch_id
       WHERE batch.source_workbench_session_id=v_session_id
         AND batch_candidate.candidate_id=v_unrelated_candidate_id
     ) THEN
    RAISE EXCEPTION 'H2_P3_UNRELATED_CANDIDATE_CHANGED:%',v_case.class_id;
  END IF;

  SELECT count(*) INTO v_bank_transfer_count
  FROM public.pay_bank_transfers AS transfer
  JOIN public.pay_batches AS batch ON batch.id=transfer.pay_batch_id
  WHERE batch.source_workbench_session_id=v_session_id;
  SELECT count(*) INTO v_remittance_scope_count
  FROM public.banking_pay_operation_remittance_scope
  WHERE operation_id=v_operation_id;
  IF v_bank_transfer_count<>0 OR v_remittance_scope_count<>0
     OR NOT EXISTS (
       SELECT 1 FROM public.banking_pay_operations
       WHERE id=v_operation_id AND phase='POST_CREATE_REFRESH'
     ) THEN
    RAISE EXCEPTION 'H2_P3_EXTERNAL_ACTION_OR_TERMINAL_PHASE_MISMATCH:%',
      pg_catalog.jsonb_build_object('bank_transfers',v_bank_transfer_count,
        'remittance_scope',v_remittance_scope_count);
  END IF;

  RAISE NOTICE 'H2_P3_PRIOR_EXCLUSION_RUNTIME_PASS=%',pg_catalog.jsonb_build_object(
    'class_id',v_case.class_id,'pay_channel',v_case.pay_channel,
    'target_selected',v_case.target_selected,'target_entry_count',v_target_entry_count,
    'target_allocation_count',v_target_allocation_count,'target_item_count',v_target_item_count,
    'control_selected_count',v_control_selected_count,'control_item_count',v_control_item_count,
    'unrelated_candidate_unchanged',true,'transaction_outcome','ROLLBACK'
  );
END;
$h2_p3_prior_exclusion_assertions$;
-- H2_P3_ASSERTION_END

ROLLBACK;
