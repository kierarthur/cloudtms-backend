\set ON_ERROR_STOP on

-- H2-only coordinator proof. The H1 page owners are represented by bounded
-- receipt-producing stubs so this test can exercise orchestration while the
-- H1 producer implementation is still being sealed. No finance, payment,
-- provider, settlement or remittance owner is called.

BEGIN;

\ir fixtures/banking-pay-settled-certificate-v8-runtime-helpers.sql

ALTER TABLE public.banking_pay_operations
  ADD COLUMN IF NOT EXISTS lease_owner text NULL,
  ADD COLUMN IF NOT EXISTS locked_by text NULL,
  ADD COLUMN IF NOT EXISTS lease_expires_at_utc timestamptz NULL,
  ADD COLUMN IF NOT EXISTS lock_expires_at_utc timestamptz NULL,
  ADD COLUMN IF NOT EXISTS progress_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS updated_at_utc timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp();

CREATE TEMP TABLE h2_certificate_stage_calls (
  call_ordinal bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  stage_kind text NOT NULL,
  after_ordinal integer NULL,
  row_count integer NOT NULL,
  has_more boolean NOT NULL
);

CREATE OR REPLACE FUNCTION private.h2_fixture_certificate_stage_page_v8(
  p_operation_id uuid,
  p_stage_kind text,
  p_total_count integer,
  p_after_ordinal integer,
  p_limit integer,
  p_expected_previous_receipt_sha256 text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path TO ''
AS $function$
DECLARE
  v_start integer := COALESCE(p_after_ordinal + 1, 0);
  v_row_count integer;
  v_next_after integer;
  v_has_more boolean;
  v_page_sequence integer;
  v_request_digest text;
  v_receipt_digest text;
BEGIN
  IF p_limit NOT BETWEEN 1 AND 256
     OR p_total_count < 1
     OR (p_after_ordinal IS NULL AND p_expected_previous_receipt_sha256 IS NOT NULL)
     OR (p_after_ordinal IS NOT NULL AND NOT EXISTS (
       SELECT 1
       FROM private.banking_pay_draft_frozen_stage_receipts_v8 AS previous
       WHERE previous.operation_id = p_operation_id
         AND previous.stage_kind = p_stage_kind
         AND previous.next_after_ordinal = p_after_ordinal
         AND previous.receipt_digest_sha256 = p_expected_previous_receipt_sha256
         AND previous.has_more
     )) THEN
    RAISE EXCEPTION 'H2_FIXTURE_STAGE_REQUEST_INVALID' USING ERRCODE = '22023';
  END IF;

  v_row_count := GREATEST(0, LEAST(p_limit, p_total_count - v_start));
  IF v_row_count = 0 THEN
    RAISE EXCEPTION 'H2_FIXTURE_STAGE_EMPTY_PAGE' USING ERRCODE = '55000';
  END IF;
  v_next_after := v_start + v_row_count - 1;
  v_has_more := v_next_after < p_total_count - 1;

  SELECT COALESCE(MAX(receipt.page_sequence) + 1, 0)
  INTO v_page_sequence
  FROM private.banking_pay_draft_frozen_stage_receipts_v8 AS receipt
  WHERE receipt.operation_id = p_operation_id
    AND receipt.stage_kind = p_stage_kind;

  v_request_digest := private.pay_payment_correction_sha256_v1(
    pg_catalog.jsonb_build_object(
      'operation_id', p_operation_id,
      'stage_kind', p_stage_kind,
      'after_ordinal', p_after_ordinal,
      'requested_limit', p_limit,
      'expected_previous_receipt_sha256', p_expected_previous_receipt_sha256
    )
  );
  v_receipt_digest := private.pay_payment_correction_sha256_v1(
    pg_catalog.jsonb_build_object(
      'operation_id', p_operation_id,
      'stage_kind', p_stage_kind,
      'page_sequence', v_page_sequence,
      'next_after_ordinal', v_next_after,
      'row_count', v_row_count,
      'has_more', v_has_more
    )
  );

  INSERT INTO private.banking_pay_draft_frozen_stage_receipts_v8(
    operation_id, stage_kind, page_sequence, after_ordinal, requested_limit,
    expected_previous_receipt_sha256, request_preimage_digest_sha256,
    row_count, canonical_byte_count, next_after_ordinal, has_more,
    terminal_sentinel_present, receipt_digest_sha256, stage_status
  ) VALUES (
    p_operation_id, p_stage_kind, v_page_sequence, p_after_ordinal, p_limit,
    p_expected_previous_receipt_sha256, v_request_digest,
    v_row_count, v_row_count * 64, v_next_after, v_has_more,
    true, v_receipt_digest, CASE WHEN v_has_more THEN 'COMMITTED' ELSE 'TERMINAL' END
  );

  INSERT INTO pg_temp.h2_certificate_stage_calls(stage_kind, after_ordinal, row_count, has_more)
  VALUES (p_stage_kind, p_after_ordinal, v_row_count, v_has_more);

  RETURN pg_catalog.jsonb_build_object(
    'ok', true,
    'page_sequence', v_page_sequence,
    'row_count', v_row_count,
    'next_after_ordinal', v_next_after,
    'has_more', v_has_more,
    'page_receipt_digest_sha256', v_receipt_digest,
    'replayed', false
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_draft_certificate_constituent_ref_page_v8(
  p_operation_id uuid,
  p_after_ordinal integer DEFAULT NULL,
  p_limit integer DEFAULT 256,
  p_expected_previous_receipt_sha256 text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE sql
SECURITY INVOKER
SET search_path TO ''
AS $function$
  SELECT private.h2_fixture_certificate_stage_page_v8(
    p_operation_id, 'CERTIFICATE_CONSTITUENT_REFS', 513,
    p_after_ordinal, p_limit, p_expected_previous_receipt_sha256
  );
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_draft_certificate_partition_ref_page_v8(
  p_operation_id uuid,
  p_after_ordinal integer DEFAULT NULL,
  p_limit integer DEFAULT 256,
  p_expected_previous_receipt_sha256 text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE sql
SECURITY INVOKER
SET search_path TO ''
AS $function$
  -- This represents the H1 normalized partition-member stream. Three pages of
  -- members may describe only three Candidate/channel partitions.
  SELECT private.h2_fixture_certificate_stage_page_v8(
    p_operation_id, 'CERTIFICATE_PARTITION_REFS', 513,
    p_after_ordinal, p_limit, p_expected_previous_receipt_sha256
  );
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_prepare_draft_scope_from_frozen_page_v8(
  p_operation_id uuid,
  p_after_partition_ordinal integer DEFAULT NULL,
  p_limit integer DEFAULT 256,
  p_expected_previous_receipt_sha256 text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE sql
SECURITY INVOKER
SET search_path TO ''
AS $function$
  SELECT private.h2_fixture_certificate_stage_page_v8(
    p_operation_id, 'CANDIDATE_SCOPE', 3,
    p_after_partition_ordinal, p_limit, p_expected_previous_receipt_sha256
  );
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_draft_certificate_final_freeze_v8(
  p_operation_id uuid,
  p_expected_last_stage_receipt_sha256 text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path TO ''
AS $function$
DECLARE
  v_scope private.banking_pay_draft_frozen_certificate_scopes_v8%ROWTYPE;
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM private.banking_pay_draft_frozen_stage_receipts_v8 AS receipt
    WHERE receipt.operation_id = p_operation_id
      AND receipt.stage_kind = 'CERTIFICATE_PARTITION_REFS'
      AND receipt.stage_status = 'TERMINAL'
      AND NOT receipt.has_more
      AND receipt.receipt_digest_sha256 = p_expected_last_stage_receipt_sha256
  ) OR NOT EXISTS (
    SELECT 1
    FROM private.banking_pay_draft_frozen_stage_receipts_v8 AS receipt
    WHERE receipt.operation_id = p_operation_id
      AND receipt.stage_kind = 'CANDIDATE_SCOPE'
      AND receipt.stage_status = 'TERMINAL'
      AND NOT receipt.has_more
  ) THEN
    RAISE EXCEPTION 'H2_FIXTURE_FINAL_FREEZE_INCOMPLETE' USING ERRCODE = '55000';
  END IF;

  UPDATE private.banking_pay_draft_frozen_certificate_scopes_v8 AS scope_row
  SET freeze_state = 'FROZEN', frozen_at_utc = pg_catalog.clock_timestamp()
  WHERE scope_row.operation_id = p_operation_id
    AND scope_row.freeze_state = 'STAGING'
  RETURNING * INTO v_scope;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'H2_FIXTURE_FINAL_FREEZE_SCOPE_INVALID' USING ERRCODE = '55000';
  END IF;

  INSERT INTO pg_temp.h2_certificate_stage_calls(stage_kind, after_ordinal, row_count, has_more)
  VALUES ('CERTIFICATE_FINAL_FREEZE', NULL, 0, false);

  RETURN pg_catalog.jsonb_build_object(
    'ok', true,
    'freeze_state', v_scope.freeze_state,
    'replayed', false
  );
END;
$function$;

\ir ../supabase/repeatable/03092026_1200_banking_pay_draft_certificate_stage_advance_v8.sql

DO $test$
DECLARE
  v_operation_id uuid := '83000000-0000-0000-0000-000000000001';
  v_certificate_uuid uuid := '83000000-0000-0000-0000-000000000002';
  v_session_id uuid := '83000000-0000-0000-0000-000000000003';
  v_snapshot_run_id uuid := '83000000-0000-0000-0000-000000000004';
  v_actor_id uuid := '83000000-0000-0000-0000-000000000005';
  v_result jsonb;
  v_call integer;
BEGIN
  PERFORM pg_temp.h2_seed_workbench_context_v8(
    v_actor_id,v_session_id,v_snapshot_run_id,'stage-fixture',1,1,1);

  INSERT INTO public.banking_pay_operations(
    id, operation_type, status, phase, lease_owner, lease_expires_at_utc,
    idempotency_key,actor_user_id,workbench_session_id,input_json
  ) VALUES (
    v_operation_id, 'DRAFT_CREATE', 'RUNNING', 'INITIALISE',
    'h2-stage-worker', pg_catalog.clock_timestamp() + interval '10 minutes',
    'stage-fixture-operation',v_actor_id,v_session_id,'{}'::jsonb
  );

  PERFORM pg_temp.h2_seed_certificate_v8(
    v_certificate_uuid,v_session_id,v_actor_id,v_snapshot_run_id,'stage-fixture',
    1,1,1,513,3,3,'513.00',repeat('a',64));

  INSERT INTO private.banking_pay_workbench_settled_certificate_channel_manifests_v8(
    certificate_uuid,pay_channel_scope,constituent_count,partition_count,
    canonical_amount_ex_vat_total,selected_constituents_digest_sha256,
    selected_partitions_digest_sha256,manifest_digest_sha256
  ) VALUES (
    v_certificate_uuid,'ALL',513,3,'513.00',repeat('b',64),repeat('c',64),repeat('d',64)
  );

  PERFORM pg_temp.h2_seed_certificate_operation_link_v8(
    v_operation_id,v_certificate_uuid,repeat('a',64),'ALL',
    'stage-fixture-operation',repeat('d',64),'STAGING');

  INSERT INTO private.banking_pay_draft_frozen_certificate_scopes_v8(
    operation_id, certificate_uuid, pay_channel_scope, constituent_count,
    partition_count, canonical_amount_ex_vat_total,
    selected_constituents_digest_sha256, selected_partitions_digest_sha256,
    manifest_digest_sha256, freeze_state
  ) VALUES (
    v_operation_id, v_certificate_uuid, 'ALL', 513, 3, '513.00',
    repeat('b', 64), repeat('c', 64), repeat('d', 64), 'STAGING'
  );

  -- A mixed old/new operation must not be repaired after business work appears
  -- to have started while the certified scope is still unfrozen.
  UPDATE public.banking_pay_operations SET phase = 'INSERT_ITEMS'
  WHERE id = v_operation_id;
  BEGIN
    PERFORM public.banking_pay_draft_certificate_stage_advance_v8(
      v_operation_id, 'h2-stage-worker'
    );
    RAISE EXCEPTION 'late business-phase operation unexpectedly staged';
  EXCEPTION
    WHEN SQLSTATE 'P0001' THEN
      IF SQLERRM <> 'DRAFT_CERTIFICATE_STAGE_ADVANCE_PHASE_MISMATCH' THEN
        RAISE;
      END IF;
  END;
  IF EXISTS (
    SELECT 1 FROM private.banking_pay_draft_frozen_stage_receipts_v8
    WHERE operation_id = v_operation_id
  ) THEN
    RAISE EXCEPTION 'phase mismatch wrote a staging receipt';
  END IF;
  UPDATE public.banking_pay_operations SET phase = 'INITIALISE'
  WHERE id = v_operation_id;

  FOR v_call IN 1..8 LOOP
    v_result := public.banking_pay_draft_certificate_stage_advance_v8(
      v_operation_id, 'h2-stage-worker'
    );
    IF COALESCE((v_result->>'page_call_count')::integer, 0)
         + COALESCE((v_result->>'freeze_call_count')::integer, 0) <> 1 THEN
      RAISE EXCEPTION 'stage call % performed more or less than one unit: %', v_call, v_result;
    END IF;
  END LOOP;

  IF (SELECT freeze_state
      FROM private.banking_pay_draft_frozen_certificate_scopes_v8
      WHERE operation_id = v_operation_id) <> 'FROZEN' THEN
    RAISE EXCEPTION 'certificate scope was not frozen';
  END IF;
  IF (SELECT phase FROM public.banking_pay_operations WHERE id = v_operation_id)
       <> 'DRAIN_TSFIN' THEN
    RAISE EXCEPTION 'Draft business phase did not begin after the exact final freeze';
  END IF;
  IF (SELECT count(*) FROM private.banking_pay_draft_frozen_stage_receipts_v8
      WHERE operation_id = v_operation_id AND stage_kind = 'CERTIFICATE_CONSTITUENT_REFS') <> 3
     OR (SELECT count(*) FROM private.banking_pay_draft_frozen_stage_receipts_v8
         WHERE operation_id = v_operation_id AND stage_kind = 'CERTIFICATE_PARTITION_REFS') <> 3
     OR (SELECT count(*) FROM private.banking_pay_draft_frozen_stage_receipts_v8
         WHERE operation_id = v_operation_id AND stage_kind = 'CANDIDATE_SCOPE') <> 1 THEN
    RAISE EXCEPTION 'bounded stage receipt cardinality mismatch';
  END IF;
  IF (SELECT count(*) FROM pg_temp.h2_certificate_stage_calls) <> 8 THEN
    RAISE EXCEPTION 'unexpected owner-call cardinality';
  END IF;

  -- A post-response-loss call after final freeze is idempotent and must not
  -- stage or freeze anything again.
  v_result := public.banking_pay_draft_certificate_stage_advance_v8(
    v_operation_id, 'h2-stage-worker'
  );
  IF v_result->>'work_kind' <> 'CERTIFICATE_STAGE_ALREADY_COMPLETE'
     OR NOT (v_result->>'replayed')::boolean
     OR (v_result->>'page_call_count')::integer <> 0
     OR (v_result->>'freeze_call_count')::integer <> 0
     OR (SELECT count(*) FROM pg_temp.h2_certificate_stage_calls) <> 8 THEN
    RAISE EXCEPTION 'post-freeze replay was not side-effect free: %', v_result;
  END IF;
END;
$test$;

ROLLBACK;

DO $zero_write$
BEGIN
  IF EXISTS (
    SELECT 1 FROM public.banking_pay_operations
    WHERE id = '83000000-0000-0000-0000-000000000001'
  ) OR EXISTS (
    SELECT 1 FROM private.banking_pay_draft_frozen_certificate_scopes_v8
    WHERE operation_id = '83000000-0000-0000-0000-000000000001'
  ) THEN
    RAISE EXCEPTION 'coordinator fixture rollback left durable rows';
  END IF;
END;
$zero_write$;
