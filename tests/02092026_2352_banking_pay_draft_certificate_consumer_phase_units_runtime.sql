\set ON_ERROR_STOP on

BEGIN;

\ir fixtures/banking-pay-settled-certificate-v8-runtime-helpers.sql

DO $test$
DECLARE
  v_operation_id uuid := '82000000-0000-0000-0000-000000000001';
  v_certificate_uuid uuid := '82000000-0000-0000-0000-000000000002';
  v_session_id uuid := '82000000-0000-0000-0000-000000000003';
  v_snapshot_run_id uuid := '82000000-0000-0000-0000-000000000004';
  v_actor_id uuid := '82000000-0000-0000-0000-000000000005';
  v_digest text := repeat('a', 64);
  v_certification_id text := 'WORKBENCH_SETTLED_CERTIFICATION_V2:' || repeat('a', 64);
  v_page jsonb;
  v_replay jsonb;
  v_receipt text;
  v_partition_receipt text;
  v_cursor integer;
  v_freeze jsonb;
  v_ordinal integer;
BEGIN
  PERFORM pg_temp.h2_seed_workbench_context_v8(
    v_actor_id,v_session_id,v_snapshot_run_id,'fixture-session-signature',7,11,13);

  INSERT INTO public.banking_pay_operations(
    id,operation_type,status,phase,idempotency_key,actor_user_id,workbench_session_id,input_json
  ) VALUES (
    v_operation_id,'DRAFT_CREATE','RUNNING','INSERT_ITEMS','fixture-idempotency',
    v_actor_id,v_session_id,'{}'::jsonb);

  PERFORM pg_temp.h2_seed_certificate_v8(
    v_certificate_uuid,v_session_id,v_actor_id,v_snapshot_run_id,'fixture-session-signature',
    7,11,13,257,257,257,'257.00',v_digest);

  INSERT INTO private.banking_pay_workbench_settled_certificate_channel_manifests_v8(
    certificate_uuid, pay_channel_scope, constituent_count, partition_count,
    canonical_amount_ex_vat_total, selected_constituents_digest_sha256,
    selected_partitions_digest_sha256, manifest_digest_sha256
  ) VALUES (
    v_certificate_uuid, 'ALL', 257, 257, '257.00',
    repeat('b', 64), repeat('c', 64), repeat('d', 64)
  );

  PERFORM pg_temp.h2_seed_certificate_operation_link_v8(
    v_operation_id,v_certificate_uuid,v_digest,'ALL','fixture-idempotency',repeat('d',64),'STAGING');

  FOR v_ordinal IN 0..256 LOOP
    PERFORM pg_temp.h2_seed_certificate_entry_v8(
      v_certificate_uuid,v_session_id,v_ordinal,v_ordinal,
      pg_catalog.md5('candidate-'||v_ordinal::text)::uuid,
      pg_catalog.md5('preview-'||v_ordinal::text)::uuid,
      CASE WHEN v_ordinal % 2 = 0 THEN 'PAYE' ELSE 'UMBRELLA' END,
      '1.00',encode(extensions.digest(convert_to('entry-'||v_ordinal::text,'UTF8'),'sha256'),'hex'),7);
  END LOOP;

  INSERT INTO private.banking_pay_workbench_settled_certificate_partitions_v8(
    certificate_uuid, partition_ordinal, candidate_id, resolved_pay_channel,
    constituent_count, canonical_amount_ex_vat_total, partition_digest_sha256
  )
  SELECT v_certificate_uuid, ordinal, md5('candidate-' || ordinal::text)::uuid,
    CASE WHEN ordinal % 2 = 0 THEN 'PAYE' ELSE 'UMBRELLA' END,
    1, '1.00', encode(extensions.digest(convert_to('partition-' || ordinal::text, 'UTF8'), 'sha256'), 'hex')
  FROM generate_series(0, 256) AS ordinal;

  INSERT INTO private.banking_pay_workbench_settled_certificate_partition_members_v8(
    certificate_uuid,stream_ordinal,partition_ordinal,member_ordinal,
    constituent_ordinal,stable_identity_digest_sha256
  )
  SELECT v_certificate_uuid,ordinal,ordinal,0,ordinal,
    encode(extensions.digest(convert_to('entry-'||ordinal::text,'UTF8'),'sha256'),'hex')
  FROM generate_series(0,256) AS ordinal;

  INSERT INTO private.banking_pay_draft_frozen_certificate_scopes_v8(
    operation_id, certificate_uuid, pay_channel_scope, constituent_count,
    partition_count, canonical_amount_ex_vat_total,
    selected_constituents_digest_sha256, selected_partitions_digest_sha256,
    manifest_digest_sha256, freeze_state
  ) VALUES (
    v_operation_id, v_certificate_uuid, 'ALL', 257, 257, '257.00',
    repeat('b', 64), repeat('c', 64), repeat('d', 64), 'STAGING'
  );

  v_page := public.pay_workbench_draft_certificate_constituent_ref_page_v8(
    v_operation_id, NULL, 128, NULL
  );
  IF (v_page->>'row_count')::integer <> 128
     OR NOT (v_page->>'has_more')::boolean
     OR (v_page->>'next_after_ordinal')::integer <> 127 THEN
    RAISE EXCEPTION 'constituent page one mismatch: %', v_page;
  END IF;
  v_receipt := v_page->>'page_receipt_digest_sha256';
  v_replay := public.pay_workbench_draft_certificate_constituent_ref_page_v8(
    v_operation_id, NULL, 128, NULL
  );
  IF NOT (v_replay->>'replayed')::boolean
     OR v_replay->>'page_receipt_digest_sha256' IS DISTINCT FROM v_receipt THEN
    RAISE EXCEPTION 'constituent response-loss replay mismatch: %', v_replay;
  END IF;
  v_page := public.pay_workbench_draft_certificate_constituent_ref_page_v8(
    v_operation_id, 127, 128, v_receipt
  );
  v_receipt := v_page->>'page_receipt_digest_sha256';
  IF (v_page->>'next_after_ordinal')::integer <> 255 OR NOT (v_page->>'has_more')::boolean THEN
    RAISE EXCEPTION 'constituent page two mismatch: %', v_page;
  END IF;
  v_page := public.pay_workbench_draft_certificate_constituent_ref_page_v8(
    v_operation_id, 255, 128, v_receipt
  );
  IF (v_page->>'row_count')::integer <> 1
     OR (v_page->>'has_more')::boolean
     OR (v_page->>'next_after_ordinal')::integer <> 256 THEN
    RAISE EXCEPTION 'constituent terminal page mismatch: %', v_page;
  END IF;

  v_page := public.pay_workbench_draft_certificate_partition_ref_page_v8(
    v_operation_id, NULL, 128, NULL
  );
  v_receipt := v_page->>'page_receipt_digest_sha256';
  v_page := public.pay_workbench_draft_certificate_partition_ref_page_v8(
    v_operation_id, 127, 128, v_receipt
  );
  v_receipt := v_page->>'page_receipt_digest_sha256';
  v_page := public.pay_workbench_draft_certificate_partition_ref_page_v8(
    v_operation_id, 255, 128, v_receipt
  );
  IF (v_page->>'row_count')::integer <> 1 OR (v_page->>'has_more')::boolean THEN
    RAISE EXCEPTION 'partition terminal page mismatch: %', v_page;
  END IF;
  v_receipt := v_page->>'page_receipt_digest_sha256';
  v_partition_receipt := v_receipt;

  v_page := public.pay_workbench_prepare_draft_scope_from_frozen_page_v8(
    v_operation_id,NULL,128,NULL);
  v_receipt := v_page->>'page_receipt_digest_sha256';
  v_page := public.pay_workbench_prepare_draft_scope_from_frozen_page_v8(
    v_operation_id,127,128,v_receipt);
  v_receipt := v_page->>'page_receipt_digest_sha256';
  v_page := public.pay_workbench_prepare_draft_scope_from_frozen_page_v8(
    v_operation_id,255,128,v_receipt);
  IF (v_page->>'row_count')::integer <> 1 OR (v_page->>'has_more')::boolean THEN
    RAISE EXCEPTION 'candidate-scope terminal page mismatch: %',v_page;
  END IF;
  v_receipt := v_page->>'page_receipt_digest_sha256';

  -- A changed Workbench authority before freeze must stop with no partial freeze.
  BEGIN
    UPDATE public.banking_pay_workbench_sessions
    SET authority_fence_generation = authority_fence_generation + 1
    WHERE id = v_session_id;
    PERFORM public.pay_workbench_draft_certificate_final_freeze_v8(v_operation_id, v_partition_receipt);
    RAISE EXCEPTION 'stale final freeze unexpectedly succeeded';
  EXCEPTION
    WHEN SQLSTATE 'P0001' THEN
      IF SQLERRM <> 'WORKBENCH_CERTIFICATE_FINAL_FREEZE_STALE' THEN
        RAISE;
      END IF;
  END;

  IF (SELECT freeze_state FROM private.banking_pay_draft_frozen_certificate_scopes_v8
      WHERE operation_id = v_operation_id) <> 'STAGING' THEN
    RAISE EXCEPTION 'stale final freeze changed state';
  END IF;

  v_freeze := public.pay_workbench_draft_certificate_final_freeze_v8(v_operation_id, v_partition_receipt);
  IF v_freeze->>'freeze_state' <> 'FROZEN' OR (v_freeze->>'replayed')::boolean THEN
    RAISE EXCEPTION 'final freeze mismatch: %', v_freeze;
  END IF;
  v_replay := public.pay_workbench_draft_certificate_final_freeze_v8(v_operation_id, v_partition_receipt);
  IF NOT (v_replay->>'replayed')::boolean
     OR v_replay->>'frozen_receipt_sha256' IS DISTINCT FROM v_freeze->>'frozen_receipt_sha256' THEN
    RAISE EXCEPTION 'final freeze replay mismatch: %', v_replay;
  END IF;

  -- Policy X: once frozen, a later Workbench refresh cannot poison the Draft.
  UPDATE public.banking_pay_workbench_sessions
  SET authority_fence_generation = authority_fence_generation + 1
  WHERE id = v_session_id;

  v_page := public.banking_pay_draft_phase_units_seed_v8(v_operation_id, NULL, 128, NULL);
  IF (v_page->>'row_count')::integer <> 128
     OR NOT (v_page->>'has_more')::boolean
     OR (v_page->>'next_after_ordinal')::integer <> 127 THEN
    RAISE EXCEPTION 'phase-unit page one mismatch: %', v_page;
  END IF;
  v_receipt := v_page->>'page_receipt_digest_sha256';
  v_replay := public.banking_pay_draft_phase_units_seed_v8(v_operation_id, NULL, 128, NULL);
  IF NOT (v_replay->>'replayed')::boolean
     OR v_replay->>'page_receipt_digest_sha256' IS DISTINCT FROM v_receipt THEN
    RAISE EXCEPTION 'phase-unit response-loss replay mismatch: %', v_replay;
  END IF;
  v_page := public.banking_pay_draft_phase_units_seed_v8(v_operation_id, 127, 128, v_receipt);
  v_receipt := v_page->>'page_receipt_digest_sha256';
  v_page := public.banking_pay_draft_phase_units_seed_v8(v_operation_id, 255, 128, v_receipt);
  IF (v_page->>'row_count')::integer <> 1
     OR (v_page->>'has_more')::boolean
     OR (v_page->>'phase_unit_count')::integer <> 257 THEN
    RAISE EXCEPTION 'phase-unit terminal page mismatch: %', v_page;
  END IF;

  IF (SELECT count(*) FROM private.banking_pay_draft_phase_units_v1
      WHERE operation_id = v_operation_id AND phase = 'INSERT_ITEMS') <> 257 THEN
    RAISE EXCEPTION 'phase-unit cardinality mismatch';
  END IF;

  BEGIN
    PERFORM public.banking_pay_draft_phase_units_seed_v8(v_operation_id, NULL, 129, NULL);
    RAISE EXCEPTION 'changed replay request unexpectedly succeeded';
  EXCEPTION
    WHEN SQLSTATE 'P0001' THEN
      IF SQLERRM <> 'WORKBENCH_CERTIFICATE_PAGE_REQUEST_CONFLICT' THEN
        RAISE;
      END IF;
  END;

  BEGIN
    PERFORM public.banking_pay_draft_phase_units_seed_v8(v_operation_id, NULL, 257, NULL);
    RAISE EXCEPTION '257-row page unexpectedly succeeded';
  EXCEPTION
    WHEN SQLSTATE 'P0001' THEN
      IF SQLERRM <> 'WORKBENCH_CERTIFICATE_PAGE_LIMIT_INVALID' THEN
        RAISE;
      END IF;
  END;
END;
$test$;

ROLLBACK;

DO $zero_write$
BEGIN
  IF EXISTS (SELECT 1 FROM public.banking_pay_operations)
     OR EXISTS (SELECT 1 FROM public.banking_pay_operation_scope_units)
     OR EXISTS (SELECT 1 FROM private.banking_pay_draft_phase_units_v1)
     OR EXISTS (SELECT 1 FROM private.banking_pay_draft_frozen_certificate_scopes_v8)
     OR EXISTS (SELECT 1 FROM private.banking_pay_draft_frozen_stage_receipts_v8) THEN
    RAISE EXCEPTION 'rollback did not leave zero fixture writes';
  END IF;
END;
$zero_write$;
