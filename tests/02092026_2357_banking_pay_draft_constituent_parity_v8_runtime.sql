\set ON_ERROR_STOP on

BEGIN;

\ir fixtures/banking-pay-settled-certificate-v8-runtime-helpers.sql

DO $verification$
DECLARE
  v_operation_id constant uuid := '82000000-0000-0000-0000-000000000057';
  v_certificate_uuid constant uuid := '82000000-0000-0000-0000-000000000058';
  v_session_id constant uuid := '82000000-0000-0000-0000-000000000059';
  v_snapshot_id constant uuid := '82000000-0000-0000-0000-000000000060';
  v_batch_paye constant uuid := '82000000-0000-0000-0000-000000000061';
  v_batch_umbrella constant uuid := '82000000-0000-0000-0000-000000000062';
  v_candidate_paye constant uuid := '82000000-0000-0000-0000-000000000063';
  v_candidate_umbrella constant uuid := '82000000-0000-0000-0000-000000000064';
  v_scope_paye constant uuid := '82000000-0000-0000-0000-000000000065';
  v_scope_umbrella constant uuid := '82000000-0000-0000-0000-000000000066';
  v_preview_paye constant uuid := '82000000-0000-0000-0000-000000000067';
  v_preview_finance constant uuid := '82000000-0000-0000-0000-000000000068';
  v_case constant uuid := '82000000-0000-0000-0000-000000000069';
  v_component constant uuid := '82000000-0000-0000-0000-000000000070';
  v_item_paye constant uuid := '82000000-0000-0000-0000-000000000071';
  v_item_finance constant uuid := '82000000-0000-0000-0000-000000000072';
  v_reservation constant uuid := '82000000-0000-0000-0000-000000000073';
  v_source_reservation constant text := '82000000-0000-0000-0000-000000000074';
  v_actor_id constant uuid := '82000000-0000-0000-0000-000000000075';
  v_digest constant text := repeat('a', 64);
  v_ordinary_item_digest text;
  v_finance_item_digest text;
  v_finance_allocation_digest text;
  v_finance_reservation_digest text;
  v_finance_overlay jsonb;
  v_first jsonb;
  v_replay jsonb;
  v_second jsonb;
  v_bad jsonb;
BEGIN
  v_finance_overlay := jsonb_build_object(
    'allocated_amount_ex_vat', '-25.00',
    'contract_version', 'WORKBENCH_SELECTION_RECOVERY_HEADROOM_V1',
    'headroom_amount_ex_vat', '25.00',
    'result', 'ALLOCATED_WITHIN_CERTIFIED_HEADROOM'
  );
  v_finance_allocation_digest := private.pay_workbench_settled_certificate_sha256_text_v8(
    private.pay_workbench_settled_certificate_stable_stringify_v8(v_finance_overlay)
  );
  v_ordinary_item_digest := private.pay_workbench_settled_certificate_sha256_text_v8(
    private.pay_workbench_settled_certificate_stable_stringify_v8(jsonb_build_object(
      'canonical_amount_ex_vat','100.00',
      'economic_key',jsonb_build_object('key_type','TS_TOTAL','key_value','TOTAL','timesheet_id',NULL),
      'semantic_kind','SEGMENT',
      'source_identity_digest_sha256',repeat('1',64)
    ))
  );
  v_finance_item_digest := private.pay_workbench_settled_certificate_sha256_text_v8(
    private.pay_workbench_settled_certificate_stable_stringify_v8(jsonb_build_object(
      'canonical_amount_ex_vat','-25.00',
      'economic_key',jsonb_build_object('key_type','MANUAL_CARRY_FORWARD','key_value','RECOVERY','timesheet_id',NULL),
      'semantic_kind','OVERPAYMENT_RECOVERY',
      'source_identity_digest_sha256',repeat('4',64)
    ))
  );
  v_finance_reservation_digest := private.pay_workbench_settled_certificate_sha256_text_v8(
    private.pay_workbench_settled_certificate_stable_stringify_v8(jsonb_build_object(
      'ordered_active_source_reservation_ids',jsonb_build_array(v_source_reservation),
      'source_reservation_amount_ex_vat','25.00'
    ))
  );

  PERFORM pg_temp.h2_seed_workbench_context_v8(
    v_actor_id,v_session_id,v_snapshot_id,v_digest,1,1,1);

  INSERT INTO public.banking_pay_operations(
    id,operation_type,status,phase,idempotency_key,actor_user_id,workbench_session_id,input_json
  ) VALUES (
    v_operation_id,'DRAFT_CREATE','RUNNING','CONSTITUENT_PARITY',
    'constituent-parity-2357',v_actor_id,v_session_id,'{}'::jsonb);
  INSERT INTO public.pay_batches(
    id,pay_date,created_by_user_id,status,banking_system_snapshot,
    external_paye_system_snapshot,batch_kind_fixed,source_workbench_session_id,
    source_snapshot_run_id,source_session_version
  ) VALUES
    (v_batch_paye,'2026-09-04',v_actor_id,'DRAFT','MONZO_CSV','SAGE','PAYE',v_session_id,v_snapshot_id,1),
    (v_batch_umbrella,'2026-09-04',v_actor_id,'DRAFT','MONZO_CSV','SAGE','UMBRELLA',v_session_id,v_snapshot_id,1);

  PERFORM pg_temp.h2_seed_certificate_v8(
    v_certificate_uuid,v_session_id,v_actor_id,v_snapshot_id,v_digest,
    1,1,1,2,2,2,'75.00',v_digest);

  PERFORM pg_temp.h2_seed_certificate_entry_v8(
    v_certificate_uuid,v_session_id,0,0,v_candidate_paye,v_preview_paye,
    'PAYE','100.00',repeat('1',64),1);
  PERFORM pg_temp.h2_seed_certificate_entry_v8(
    v_certificate_uuid,v_session_id,1,1,v_candidate_umbrella,v_preview_finance,
    'UMBRELLA','-25.00',repeat('4',64),1);

  UPDATE private.banking_pay_workbench_settled_certificate_entries_v8
  SET row_key='ordinary-row',semantic_kind='SEGMENT',economic_key_type='TS_TOTAL',
      economic_key_value='TOTAL',expected_allocation_basis_kind='NOT_APPLICABLE',
      expected_allocated_recovery_amount_ex_vat=NULL,
      expected_allocation_result='NOT_APPLICABLE',
      expected_allocation_source_digest_sha256=NULL,
      expected_item_semantic_kind='SEGMENT',
      expected_item_source_digest_sha256=v_ordinary_item_digest,
      constituent_digest_sha256=repeat('3',64)
  WHERE certificate_uuid=v_certificate_uuid AND constituent_ordinal=0;

  UPDATE private.banking_pay_workbench_settled_certificate_entries_v8
  SET row_key='finance-row',semantic_kind='OVERPAYMENT_RECOVERY',
      economic_key_type='MANUAL_CARRY_FORWARD',economic_key_value='RECOVERY',
      source_reservation_amount_ex_vat='25.00',
      recovery_result_kind='ALLOCATED_WITHIN_CERTIFIED_HEADROOM',
      expected_allocation_basis_kind='WORKBENCH_RECOVERY_HEADROOM_V1',
      expected_allocated_recovery_amount_ex_vat='25.00',
      expected_allocation_result='ALLOCATED_WITHIN_CERTIFIED_HEADROOM',
      expected_allocation_source_digest_sha256=v_finance_allocation_digest,
      expected_item_semantic_kind='OVERPAYMENT_RECOVERY',
      expected_item_source_digest_sha256=v_finance_item_digest,
      expected_reservation_applicability='APPLICABLE',
      expected_reservation_amount_ex_vat='25.00',
      expected_reservation_source_digest_sha256=v_finance_reservation_digest,
      constituent_digest_sha256=repeat('8',64)
  WHERE certificate_uuid=v_certificate_uuid AND constituent_ordinal=1;

  INSERT INTO public.pay_advances(
    id,candidate_id,reason,original_amount,outstanding_amount,status,
    advance_kind,case_type,taxability,routing_kind,created_by
  ) VALUES (
    v_case,v_candidate_umbrella,'OVERPAYMENT',25,25,'ACTIVE',
    'OVERPAYMENT','OVERPAYMENT','TAXABLE','UMBRELLA_COMPANY',v_actor_id
  );

  INSERT INTO public.pay_finance_case_components(
    id,finance_case_id,candidate_id,source_family_key,component_key_type,
    component_key_value,classification,source_pay_method,source_basis_json,
    source_amount,remaining_source_amount
  ) VALUES (
    v_component,v_case,v_candidate_umbrella,'fixture-overpayment','CASE_TOTAL',
    'TOTAL','TAXABLE_CHANNEL_SENSITIVE','PAYE','{}'::jsonb,25,25
  );

  INSERT INTO private.banking_pay_workbench_settled_cert_source_reservations_v8(
    certificate_uuid,constituent_ordinal,reservation_ordinal,source_reservation_id
  ) VALUES (v_certificate_uuid,1,0,v_source_reservation);

  INSERT INTO private.banking_pay_workbench_settled_certificate_partitions_v8(
    certificate_uuid,partition_ordinal,candidate_id,resolved_pay_channel,
    constituent_count,canonical_amount_ex_vat_total,partition_digest_sha256
  ) VALUES
    (v_certificate_uuid,0,v_candidate_paye,'PAYE',1,'100.00',repeat('9',64)),
    (v_certificate_uuid,1,v_candidate_umbrella,'UMBRELLA',1,'-25.00',repeat('b',64));
  INSERT INTO private.banking_pay_workbench_settled_certificate_partition_members_v8(
    certificate_uuid,stream_ordinal,partition_ordinal,member_ordinal,
    constituent_ordinal,stable_identity_digest_sha256
  ) VALUES
    (v_certificate_uuid,0,0,0,0,repeat('3',64)),
    (v_certificate_uuid,1,1,0,1,repeat('8',64));

  INSERT INTO private.banking_pay_draft_frozen_certificate_scopes_v8(
    operation_id,certificate_uuid,pay_channel_scope,constituent_count,partition_count,
    canonical_amount_ex_vat_total,selected_constituents_digest_sha256,
    selected_partitions_digest_sha256,manifest_digest_sha256,freeze_state,frozen_at_utc
  ) VALUES (
    v_operation_id,v_certificate_uuid,'ALL',2,2,'75.00',repeat('c',64),repeat('d',64),repeat('e',64),'FROZEN',clock_timestamp()
  );
  INSERT INTO private.banking_pay_draft_frozen_constituent_refs_v8(
    operation_id,certificate_uuid,constituent_ordinal,staged_page_sequence
  ) VALUES
    (v_operation_id,v_certificate_uuid,0,0),(v_operation_id,v_certificate_uuid,1,0);
  INSERT INTO private.banking_pay_draft_frozen_partition_refs_v8(
    operation_id,certificate_uuid,partition_ordinal,staged_page_sequence
  ) VALUES
    (v_operation_id,v_certificate_uuid,0,0),(v_operation_id,v_certificate_uuid,1,0);
  INSERT INTO private.banking_pay_draft_frozen_candidate_scopes_v8(
    operation_id,candidate_scope_ordinal,certificate_uuid,partition_ordinal,candidate_id,
    resolved_pay_channel,constituent_count,canonical_amount_ex_vat_total,scope_digest_sha256,
    scope_state,pay_batch_id
  ) VALUES
    (v_operation_id,0,v_certificate_uuid,0,v_candidate_paye,'PAYE',1,'100.00',repeat('9',64),'COMPLETE',v_batch_paye),
    (v_operation_id,1,v_certificate_uuid,1,v_candidate_umbrella,'UMBRELLA',1,'-25.00',repeat('b',64),'COMPLETE',v_batch_umbrella);
  INSERT INTO private.banking_pay_draft_frozen_candidate_scope_members_v8(
    operation_id,candidate_scope_ordinal,member_ordinal,certificate_uuid,partition_ordinal,
    constituent_ordinal,stable_identity_digest_sha256
  ) VALUES
    (v_operation_id,0,0,v_certificate_uuid,0,0,repeat('1',64)),
    (v_operation_id,1,0,v_certificate_uuid,1,1,repeat('4',64));

  INSERT INTO public.banking_pay_operation_candidate_scope(
    id,operation_id,workbench_session_id,candidate_id,pay_channel,pay_batch_id,scope_hash,status
  ) VALUES
    (v_scope_paye,v_operation_id,v_session_id,v_candidate_paye,'PAYE',v_batch_paye,repeat('9',64),'DRAFTED'),
    (v_scope_umbrella,v_operation_id,v_session_id,v_candidate_umbrella,'UMBRELLA',v_batch_umbrella,repeat('b',64),'DRAFTED');

  INSERT INTO public.pay_batch_candidates(id,pay_batch_id,candidate_id) VALUES
    (extensions.gen_random_uuid(),v_batch_paye,v_candidate_paye),
    (extensions.gen_random_uuid(),v_batch_umbrella,v_candidate_umbrella);
  INSERT INTO public.pay_batch_items(
    id,pay_batch_candidate_id,item_type,timesheet_id,source_ref,amount_ex_vat,amount_vat,
    amount_inc_vat,pay_channel,is_voided,finance_case_id,reservation_id,paye_treatment,
    finance_component_id,frozen_component_classification,frozen_component_key_type,
    frozen_component_key_value,frozen_source_pay_method,frozen_target_pay_method,
    frozen_source_amount,operation_source_key
  )
  SELECT v_item_paye,candidate.id,'SEGMENT_DELTA',NULL,'ordinary-source',100,0,100,'PAYE',false,
    NULL,NULL,'GROSS_ADD',NULL,NULL,NULL,NULL,NULL,NULL,100,
    v_operation_id::text || ':allocation:' || v_scope_paye::text || ':' || v_preview_paye::text
  FROM public.pay_batch_candidates AS candidate
  WHERE candidate.pay_batch_id=v_batch_paye AND candidate.candidate_id=v_candidate_paye;
  INSERT INTO public.pay_batch_items(
    id,pay_batch_candidate_id,item_type,timesheet_id,source_ref,amount_ex_vat,amount_vat,
    amount_inc_vat,pay_channel,is_voided,finance_case_id,reservation_id,paye_treatment,
    finance_component_id,frozen_component_classification,frozen_component_key_type,
    frozen_component_key_value,frozen_source_pay_method,frozen_target_pay_method,
    frozen_source_amount,operation_source_key
  )
  SELECT v_item_finance,candidate.id,'OVERPAYMENT_RECOVERY',NULL,'advance:'||v_case::text,-25,-5,-30,'UMBRELLA',false,
    v_case,NULL,'NONE',v_component,'TAXABLE_CHANNEL_SENSITIVE','CASE_TOTAL','TOTAL','PAYE','UMBRELLA',25,
    v_operation_id::text || ':finance-plan:' || v_component::text
  FROM public.pay_batch_candidates AS candidate
  WHERE candidate.pay_batch_id=v_batch_umbrella AND candidate.candidate_id=v_candidate_umbrella;

  INSERT INTO public.pay_advance_reservations(
    id,finance_case_id,pay_batch_id,pay_batch_candidate_id,pay_batch_item_id,reserved_amount,
    status,finance_component_id,frozen_component_classification,frozen_component_key_type,
    frozen_component_key_value,reserved_source_amount,frozen_rounded_target_amount
  )
  SELECT v_reservation,v_case,v_batch_umbrella,item.pay_batch_candidate_id,v_item_finance,25,
    'RESERVED',v_component,'TAXABLE_CHANNEL_SENSITIVE','CASE_TOTAL','TOTAL',25,25
  FROM public.pay_batch_items AS item WHERE item.id=v_item_finance;
  UPDATE public.pay_batch_items SET reservation_id=v_reservation WHERE id=v_item_finance;

  INSERT INTO public.banking_pay_operation_candidate_allocation_rows(
    id,operation_id,candidate_scope_id,pay_batch_id,candidate_id,pay_channel,finance_case_id,
    finance_component_id,allocation_type,source_ref,operation_source_key,allocated_amount,
    allocation_basis_json,sort_order,status,pay_batch_item_id
  ) VALUES
    (extensions.gen_random_uuid(),v_operation_id,v_scope_paye,v_batch_paye,v_candidate_paye,'PAYE',NULL,NULL,
     'SEGMENT','ordinary-source',v_operation_id::text||':allocation:'||v_scope_paye::text||':'||v_preview_paye::text,100,
      jsonb_build_object('preview_row_id',v_preview_paye::text,'line',jsonb_build_object(
        'preview_row_pk',v_preview_paye::text,
        'preview_row_id',v_preview_paye::text,
        'line_type','SEGMENT',
        'amount_ex_vat',100,
        'economic_key',jsonb_build_object('timesheet_id',NULL,'key_type','TS_TOTAL','key_value','TOTAL'),
        'workbench_settled_certificate_binding_v8',jsonb_build_object(
          'binding_contract_version','WORKBENCH_SETTLED_CERTIFICATE_BINDING_V8',
          'certificate_uuid',v_certificate_uuid::text,
          'constituent_digest_sha256',repeat('3',64),
          'constituent_ordinal',0,
          'source_identity_digest_sha256',repeat('1',64)
        )
      )),
     1,'ITEM_CREATED',v_item_paye),
    (extensions.gen_random_uuid(),v_operation_id,v_scope_umbrella,v_batch_umbrella,v_candidate_umbrella,'UMBRELLA',v_case,v_component,
     'OVERPAYMENT_RECOVERY','advance:'||v_case::text,v_operation_id::text||':allocation:'||v_scope_umbrella::text||':'||v_preview_finance::text,-25,
     jsonb_build_object(
       'preview_row_id',v_preview_finance::text,
        'line',jsonb_build_object(
          'preview_row_pk',v_preview_finance::text,
          'preview_row_id',v_preview_finance::text,
          'line_type','OVERPAYMENT_RECOVERY',
          'amount_ex_vat',-25,
          'economic_key',jsonb_build_object('timesheet_id',NULL,'key_type','MANUAL_CARRY_FORWARD','key_value','RECOVERY'),
          'selection_recovery_headroom_v1',v_finance_overlay,
          'workbench_settled_certificate_binding_v8',jsonb_build_object(
            'binding_contract_version','WORKBENCH_SETTLED_CERTIFICATE_BINDING_V8',
            'certificate_uuid',v_certificate_uuid::text,
            'constituent_digest_sha256',repeat('8',64),
            'constituent_ordinal',1,
            'source_identity_digest_sha256',repeat('4',64)
          )
        ),
       'draft_finance_item_plan',jsonb_build_object(
         'planned_item_key',v_operation_id::text||':finance-plan:'||v_component::text,
         'planned_item_amount','-25.00','plan_digest',repeat('f',64)
       )
     ),1,'ITEM_CREATED',v_item_finance);

  BEGIN
    UPDATE public.banking_pay_operation_candidate_allocation_rows
    SET allocation_basis_json = jsonb_set(
      allocation_basis_json,
      '{line,workbench_settled_certificate_binding_v8,source_identity_digest_sha256}',
      to_jsonb(repeat('0',64))
    )
    WHERE operation_id=v_operation_id AND candidate_id=v_candidate_paye;
    v_bad := private.pay_workbench_draft_constituent_parity_compare_v8(v_operation_id,0);
    IF v_bad->>'first_mismatch_code' <> 'DRAFT_PARITY_CERTIFICATE_BINDING_MISMATCH' THEN
      RAISE EXCEPTION 'certificate binding mutation was not rejected: %',v_bad;
    END IF;
    RAISE EXCEPTION 'EXPECTED_FIXTURE_SUBTRANSACTION_ROLLBACK';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN
    IF SQLERRM <> 'EXPECTED_FIXTURE_SUBTRANSACTION_ROLLBACK' THEN RAISE; END IF;
  END;

  BEGIN
    UPDATE public.banking_pay_operation_candidate_allocation_rows
    SET allocation_basis_json = jsonb_set(allocation_basis_json,'{line,line_type}',to_jsonb('UNEXPECTED_SOURCE_KIND'::text))
    WHERE operation_id=v_operation_id AND candidate_id=v_candidate_paye;
    v_bad := private.pay_workbench_draft_constituent_parity_compare_v8(v_operation_id,0);
    IF v_bad->>'first_mismatch_code' <> 'DRAFT_PARITY_ITEM_SOURCE_EVIDENCE_MISMATCH' THEN
      RAISE EXCEPTION 'item source evidence mutation was not rejected: %',v_bad;
    END IF;
    RAISE EXCEPTION 'EXPECTED_FIXTURE_SUBTRANSACTION_ROLLBACK';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN
    IF SQLERRM <> 'EXPECTED_FIXTURE_SUBTRANSACTION_ROLLBACK' THEN RAISE; END IF;
  END;

  BEGIN
    UPDATE public.banking_pay_operation_candidate_allocation_rows
    SET allocation_basis_json = jsonb_set(
      allocation_basis_json,
      '{line,selection_recovery_headroom_v1,allocated_amount_ex_vat}',
      to_jsonb('-24.00'::text)
    )
    WHERE operation_id=v_operation_id AND candidate_id=v_candidate_umbrella;
    v_bad := private.pay_workbench_draft_constituent_parity_compare_v8(v_operation_id,1);
    IF v_bad->>'first_mismatch_code' <> 'DRAFT_PARITY_ALLOCATION_SOURCE_EVIDENCE_MISMATCH' THEN
      RAISE EXCEPTION 'allocation source evidence mutation was not rejected: %',v_bad;
    END IF;
    RAISE EXCEPTION 'EXPECTED_FIXTURE_SUBTRANSACTION_ROLLBACK';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN
    IF SQLERRM <> 'EXPECTED_FIXTURE_SUBTRANSACTION_ROLLBACK' THEN RAISE; END IF;
  END;

  BEGIN
    DELETE FROM private.banking_pay_workbench_settled_cert_source_reservations_v8
    WHERE certificate_uuid=v_certificate_uuid AND constituent_ordinal=1;
    v_bad := private.pay_workbench_draft_constituent_parity_compare_v8(v_operation_id,1);
    IF v_bad->>'first_mismatch_code' <> 'DRAFT_PARITY_SOURCE_RESERVATION_EVIDENCE_MISMATCH' THEN
      RAISE EXCEPTION 'source reservation evidence mutation was not rejected: %',v_bad;
    END IF;
    RAISE EXCEPTION 'EXPECTED_FIXTURE_SUBTRANSACTION_ROLLBACK';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN
    IF SQLERRM <> 'EXPECTED_FIXTURE_SUBTRANSACTION_ROLLBACK' THEN RAISE; END IF;
  END;

  v_first := public.pay_workbench_draft_constituent_parity_page_v8(v_operation_id,NULL,1,NULL);
  IF v_first->>'match_count' <> '1' OR v_first->>'mismatch_count' <> '0' OR v_first->>'has_more' <> 'true' THEN
    RAISE EXCEPTION 'first parity page mismatch: %',v_first;
  END IF;
  v_replay := public.pay_workbench_draft_constituent_parity_page_v8(v_operation_id,NULL,1,NULL);
  IF v_replay->>'replayed' <> 'true'
     OR v_replay->>'page_receipt_digest_sha256' IS DISTINCT FROM v_first->>'page_receipt_digest_sha256' THEN
    RAISE EXCEPTION 'parity replay mismatch: %',v_replay;
  END IF;
  v_second := public.pay_workbench_draft_constituent_parity_page_v8(
    v_operation_id,0,1,v_first->>'page_receipt_digest_sha256'
  );
  IF v_second->>'match_count' <> '1' OR v_second->>'mismatch_count' <> '0' OR v_second->>'has_more' <> 'false' THEN
    RAISE EXCEPTION 'second parity page mismatch: %',v_second;
  END IF;

  BEGIN
    UPDATE public.pay_batch_items SET amount_ex_vat=-24 WHERE id=v_item_finance;
    PERFORM public.pay_workbench_draft_constituent_parity_page_v8(
      v_operation_id,0,1,v_first->>'page_receipt_digest_sha256'
    );
    RAISE EXCEPTION 'tampered item unexpectedly replayed';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN
    IF SQLERRM <> 'DRAFT_PARITY_FACT_MISMATCH' THEN
      RAISE;
    END IF;
  END;

  IF (SELECT count(*) FROM private.banking_pay_draft_constituent_parity_results_v8
      WHERE operation_id=v_operation_id AND comparison_status='MATCH') <> 2 THEN
    RAISE EXCEPTION 'complete parity result set missing';
  END IF;
END;
$verification$;

ROLLBACK;

DO $zero_write$
BEGIN
  IF EXISTS (SELECT 1 FROM public.banking_pay_operations)
     OR EXISTS (SELECT 1 FROM public.pay_batch_items)
     OR EXISTS (SELECT 1 FROM private.banking_pay_draft_constituent_parity_results_v8) THEN
    RAISE EXCEPTION 'parity fixture rollback left durable rows';
  END IF;
END;
$zero_write$;
