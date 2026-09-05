\set ON_ERROR_STOP on

BEGIN;

\ir fixtures/banking-pay-settled-certificate-v8-runtime-helpers.sql

DO $test$
DECLARE
  v_operation_id constant uuid := '84000000-0000-0000-0000-000000000001';
  v_certificate_uuid constant uuid := '84000000-0000-0000-0000-000000000002';
  v_session_id constant uuid := '84000000-0000-0000-0000-000000000003';
  v_source_run_id constant uuid := '84000000-0000-0000-0000-000000000004';
  v_candidate_id constant uuid := '84000000-0000-0000-0000-000000000005';
  v_actor_id constant uuid := '84000000-0000-0000-0000-000000000006';
  v_overall_digest constant text := repeat('4', 64);
  v_constituent_digest constant text := repeat('5', 64);
  v_partition_digest constant text := repeat('6', 64);
  v_manifest_digest constant text := repeat('7', 64);
  v_page jsonb;
  v_replay jsonb;
  v_constituent_receipt text;
  v_partition_receipt text;
  v_scope_receipt text;
  v_count integer;
  v_ordinal integer;
BEGIN
  PERFORM pg_temp.h2_seed_workbench_context_v8(
    v_actor_id,v_session_id,v_source_run_id,'scope-runtime-session',1,1,1);

  PERFORM pg_temp.h2_seed_certificate_v8(
    v_certificate_uuid,v_session_id,v_actor_id,v_source_run_id,'scope-runtime-session',
    1,1,1,257,1,1,'257.00',v_overall_digest);

  INSERT INTO private.banking_pay_workbench_settled_certificate_channel_manifests_v8(
    certificate_uuid, pay_channel_scope, constituent_count, partition_count,
    canonical_amount_ex_vat_total, selected_constituents_digest_sha256,
    selected_partitions_digest_sha256, manifest_digest_sha256
  ) VALUES (
    v_certificate_uuid, 'ALL', 257, 1, '257.00',
    v_constituent_digest, v_partition_digest, v_manifest_digest
  );

  FOR v_ordinal IN 0..256 LOOP
    PERFORM pg_temp.h2_seed_certificate_entry_v8(
      v_certificate_uuid,v_session_id,v_ordinal,0,v_candidate_id,
      pg_catalog.md5('scope-member-'||v_ordinal::text)::uuid,'PAYE','1.00',
      pg_catalog.encode(extensions.digest(
        pg_catalog.convert_to('scope-member-'||v_ordinal::text,'UTF8'),'sha256'),'hex'),1);
  END LOOP;

  INSERT INTO private.banking_pay_workbench_settled_certificate_partitions_v8(
    certificate_uuid, partition_ordinal, candidate_id, resolved_pay_channel,
    constituent_count, canonical_amount_ex_vat_total, partition_digest_sha256
  ) VALUES (
    v_certificate_uuid, 0, v_candidate_id, 'PAYE', 257, '257.00', v_partition_digest
  );

  INSERT INTO private.banking_pay_workbench_settled_certificate_partition_members_v8(
    certificate_uuid, stream_ordinal, partition_ordinal, member_ordinal, constituent_ordinal,
    stable_identity_digest_sha256
  )
  SELECT
    v_certificate_uuid,
    entry.constituent_ordinal,
    0,
    entry.constituent_ordinal,
    entry.constituent_ordinal,
    entry.constituent_digest_sha256
  FROM private.banking_pay_workbench_settled_certificate_entries_v8 AS entry
  WHERE entry.certificate_uuid = v_certificate_uuid
  ORDER BY entry.constituent_ordinal;

  INSERT INTO public.banking_pay_operations(
    id,operation_type,status,phase,idempotency_key,actor_user_id,workbench_session_id,input_json
  ) VALUES (
    v_operation_id,'DRAFT_CREATE','RUNNING','CERTIFICATE_CONSTITUENT_REFS',
    'scope-runtime-operation',v_actor_id,v_session_id,'{}'::jsonb);

  PERFORM pg_temp.h2_seed_certificate_operation_link_v8(
    v_operation_id,v_certificate_uuid,v_overall_digest,'ALL',
    'scope-runtime-operation',v_manifest_digest,'STAGING');

  INSERT INTO private.banking_pay_draft_frozen_certificate_scopes_v8(
    operation_id,certificate_uuid,pay_channel_scope,constituent_count,
    partition_count,canonical_amount_ex_vat_total,
    selected_constituents_digest_sha256,selected_partitions_digest_sha256,
    manifest_digest_sha256,freeze_state
  ) VALUES (
    v_operation_id,v_certificate_uuid,'ALL',257,1,'257.00',
    v_constituent_digest,v_partition_digest,v_manifest_digest,'STAGING'
  );

  v_page := public.pay_workbench_draft_certificate_constituent_ref_page_v8(
    v_operation_id, NULL, NULL, NULL
  );
  IF (v_page->>'row_count')::integer <> 256
     OR (v_page->>'next_after_ordinal')::integer <> 255
     OR (v_page->>'has_more')::boolean IS NOT TRUE THEN
    RAISE EXCEPTION 'default 256 constituent page did not return the expected first page: %', v_page;
  END IF;
  v_constituent_receipt := v_page->>'page_receipt_digest_sha256';

  v_page := public.pay_workbench_draft_certificate_constituent_ref_page_v8(
    v_operation_id, 255, NULL, v_constituent_receipt
  );
  IF (v_page->>'row_count')::integer <> 1
     OR (v_page->>'has_more')::boolean IS TRUE THEN
    RAISE EXCEPTION 'terminal constituent page did not return exactly one row: %', v_page;
  END IF;

  v_page := public.pay_workbench_draft_certificate_partition_ref_page_v8(
    v_operation_id, NULL, NULL, NULL
  );
  IF (v_page->>'row_count')::integer <> 1
     OR (v_page->>'has_more')::boolean IS TRUE THEN
    RAISE EXCEPTION 'partition page did not return exactly one row: %', v_page;
  END IF;
  v_partition_receipt := v_page->>'page_receipt_digest_sha256';

  v_page := public.pay_workbench_prepare_draft_scope_from_frozen_page_v8(
    v_operation_id, NULL, NULL, NULL
  );
  IF (v_page->>'row_count')::integer <> 1
     OR (v_page->>'has_more')::boolean IS TRUE
     OR (v_page->>'frozen_candidate_scope_count')::integer <> 1
     OR (v_page->>'frozen_constituent_count')::integer <> 257 THEN
    RAISE EXCEPTION 'row-backed scope page did not freeze the complete partition: %', v_page;
  END IF;
  v_scope_receipt := v_page->>'page_receipt_digest_sha256';

  v_replay := public.pay_workbench_prepare_draft_scope_from_frozen_page_v8(
    v_operation_id, NULL, NULL, NULL
  );
  IF (v_replay->>'replayed')::boolean IS NOT TRUE
     OR v_replay->>'page_receipt_digest_sha256' IS DISTINCT FROM v_scope_receipt THEN
    RAISE EXCEPTION 'response-loss replay did not return the immutable receipt: %', v_replay;
  END IF;

  BEGIN
    PERFORM public.pay_workbench_prepare_draft_scope_from_frozen_page_v8(
      v_operation_id, NULL, 128, NULL
    );
    RAISE EXCEPTION 'changed replay request unexpectedly succeeded';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN
    IF SQLERRM <> 'WORKBENCH_CERTIFICATE_PAGE_REQUEST_CONFLICT' THEN
      RAISE;
    END IF;
  END;

  PERFORM public.pay_workbench_draft_certificate_final_freeze_v8(
    v_operation_id,
    v_partition_receipt
  );

  SELECT pg_catalog.count(*)::integer
  INTO v_count
  FROM private.banking_pay_draft_frozen_candidate_scope_members_v8 AS member
  WHERE member.operation_id = v_operation_id;
  IF v_count <> 257 THEN
    RAISE EXCEPTION 'normalized membership is incomplete: %', v_count;
  END IF;

  SELECT pg_catalog.count(*)::integer
  INTO v_count
  FROM public.banking_pay_operation_candidate_scope AS public_scope
  WHERE public_scope.operation_id = v_operation_id
    AND public_scope.selected_preview_row_ids_json = '[]'::jsonb
    AND public_scope.selected_timesheet_ids_json = '[]'::jsonb
    AND public_scope.selected_finance_case_ids_json = '[]'::jsonb
    AND public_scope.effective_canonical_preview_lines_json = '[]'::jsonb
    AND public_scope.selected_canonical_preview_lines_json = '[]'::jsonb
    AND public_scope.baseline_component_rows_json = '[]'::jsonb
    AND public_scope.hidden_recovery_template_lines_json = '[]'::jsonb;
  IF v_count <> 1 THEN
    RAISE EXCEPTION 'compact legacy shell contains a forbidden member array';
  END IF;
END;
$test$;

ROLLBACK;

DO $verify_rollback$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM public.banking_pay_operations
    WHERE id = '84000000-0000-0000-0000-000000000001'
  ) THEN
    RAISE EXCEPTION 'outer rollback left operation state behind';
  END IF;
  IF EXISTS (
    SELECT 1
    FROM private.banking_pay_draft_frozen_candidate_scope_members_v8
    WHERE operation_id = '84000000-0000-0000-0000-000000000001'
  ) THEN
    RAISE EXCEPTION 'outer rollback left normalized membership behind';
  END IF;
END;
$verify_rollback$;
