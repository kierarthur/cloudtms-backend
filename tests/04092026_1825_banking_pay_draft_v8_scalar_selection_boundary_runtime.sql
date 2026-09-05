\set ON_ERROR_STOP on

-- Scalar-only boundary proof. Never materialise 50,000 or 50,001 rows: the
-- configured product ceiling is 50,000 while load fixtures stop at 5,000.
BEGIN;

\ir fixtures/banking-pay-settled-certificate-v8-runtime-helpers.sql

DO $test$
DECLARE
  v_actor_id uuid := '82500000-0000-0000-0000-000000000001';
  v_session_id uuid := '82500000-0000-0000-0000-000000000002';
  v_snapshot_run_id uuid := '82500000-0000-0000-0000-000000000003';
  v_certificate_uuid uuid := '82500000-0000-0000-0000-000000000004';
  v_build_definition text;
  v_admission_definition text;
  v_rejected boolean := false;
BEGIN
  SELECT pg_catalog.pg_get_functiondef(proc.oid)
  INTO STRICT v_build_definition
  FROM pg_catalog.pg_proc proc
  JOIN pg_catalog.pg_namespace namespace ON namespace.oid = proc.pronamespace
  WHERE namespace.nspname = 'public'
    AND proc.proname = 'pay_workbench_settled_certificate_build_start_v8'
    AND pg_catalog.pg_get_function_identity_arguments(proc.oid) = 'p_workbench_session_id uuid, p_actor_user_id uuid, p_build_idempotency_key text';

  IF v_build_definition !~ 'LIMIT[[:space:]]+50001'
     OR v_build_definition !~ 'v_selected_count[[:space:]]*>[[:space:]]*50000'
     OR v_build_definition !~ 'WORKBENCH_CERTIFICATE_SELECTED_LIMIT_EXCEEDED' THEN
    RAISE EXCEPTION 'installed certificate build owner lost the 50,000 plus sentinel boundary';
  END IF;

  SELECT pg_catalog.pg_get_functiondef(proc.oid)
  INTO STRICT v_admission_definition
  FROM pg_catalog.pg_proc proc
  JOIN pg_catalog.pg_namespace namespace ON namespace.oid = proc.pronamespace
  WHERE namespace.nspname = 'private'
    AND proc.proname = 'pay_workbench_settled_certificate_operation_admit_v8'
    AND pg_catalog.pg_get_function_identity_arguments(proc.oid) = 'p_operation_id uuid';

  IF v_admission_definition !~ 'constituent_count[[:space:]]+NOT[[:space:]]+BETWEEN[[:space:]]+1[[:space:]]+AND[[:space:]]+50000' THEN
    RAISE EXCEPTION 'installed operation admission owner lost the exact 50,000 ceiling';
  END IF;

  PERFORM pg_temp.h2_seed_workbench_context_v8(
    v_actor_id, v_session_id, v_snapshot_run_id, 'scalar-boundary-session', 1, 1, 1);
  PERFORM pg_temp.h2_seed_certificate_v8(
    v_certificate_uuid, v_session_id, v_actor_id, v_snapshot_run_id,
    'scalar-boundary-session', 1, 1, 1, 1, 1, 1, '1.00', repeat('8', 64));

  INSERT INTO private.banking_pay_workbench_settled_certificate_channel_manifests_v8(
    certificate_uuid, pay_channel_scope, constituent_count, partition_count,
    canonical_amount_ex_vat_total, selected_constituents_digest_sha256,
    selected_partitions_digest_sha256, manifest_digest_sha256
  ) VALUES (
    v_certificate_uuid, 'ALL', 50000, 1, '1.00',
    repeat('9', 64), repeat('a', 64), repeat('b', 64)
  );
  IF NOT EXISTS (
    SELECT 1
    FROM private.banking_pay_workbench_settled_certificate_channel_manifests_v8
    WHERE certificate_uuid = v_certificate_uuid
      AND pay_channel_scope = 'ALL'
      AND constituent_count = 50000
  ) THEN
    RAISE EXCEPTION '50,000 scalar manifest was not accepted';
  END IF;

  BEGIN
    INSERT INTO private.banking_pay_workbench_settled_certificate_channel_manifests_v8(
      certificate_uuid, pay_channel_scope, constituent_count, partition_count,
      canonical_amount_ex_vat_total, selected_constituents_digest_sha256,
      selected_partitions_digest_sha256, manifest_digest_sha256
    ) VALUES (
      v_certificate_uuid, 'PAYE', 50001, 1, '1.00',
      repeat('9', 64), repeat('a', 64), repeat('b', 64)
    );
    RAISE EXCEPTION '50,001 scalar manifest unexpectedly succeeded';
  EXCEPTION
    WHEN check_violation THEN
      v_rejected := true;
  END;

  IF NOT v_rejected THEN
    RAISE EXCEPTION '50,001 scalar manifest was not rejected';
  END IF;
  IF EXISTS (
    SELECT 1
    FROM private.banking_pay_workbench_settled_certificate_channel_manifests_v8
    WHERE certificate_uuid = v_certificate_uuid
      AND pay_channel_scope = 'PAYE'
  ) THEN
    RAISE EXCEPTION '50,001 scalar rejection left a partial channel manifest';
  END IF;
  IF EXISTS (
    SELECT 1
    FROM public.banking_pay_operations
    WHERE workbench_session_id = v_session_id
  ) THEN
    RAISE EXCEPTION '50,001 scalar rejection created a Draft operation';
  END IF;
END;
$test$;

ROLLBACK;

DO $zero_write$
BEGIN
  IF EXISTS (
    SELECT 1 FROM private.banking_pay_workbench_settled_certificates_v8
    WHERE certificate_uuid = '82500000-0000-0000-0000-000000000004'::uuid
  ) OR EXISTS (
    SELECT 1 FROM public.banking_pay_workbench_sessions
    WHERE id = '82500000-0000-0000-0000-000000000002'::uuid
  ) OR EXISTS (
    SELECT 1 FROM public.banking_pay_operations
    WHERE workbench_session_id = '82500000-0000-0000-0000-000000000002'::uuid
  ) THEN
    RAISE EXCEPTION 'scalar boundary rollback did not leave zero fixture rows';
  END IF;
END;
$zero_write$;
