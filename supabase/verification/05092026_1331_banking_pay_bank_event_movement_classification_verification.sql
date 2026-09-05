-- Read-only verification for the policy-neutral bank-event classification
-- storage correction. Runtime authority is Miget TEST.

DO $verification$
DECLARE
  v_definition text;
  v_validated boolean;
  v_event_ingest_definition text;
BEGIN
  SELECT
    pg_catalog.pg_get_constraintdef(constraint_row.oid, true),
    constraint_row.convalidated
  INTO v_definition, v_validated
  FROM pg_catalog.pg_constraint AS constraint_row
  WHERE constraint_row.conrelid = 'public.pay_bank_transfer_events'::pg_catalog.regclass
    AND constraint_row.conname = 'pay_bank_transfer_events_movement_classification_chk';

  IF v_definition IS DISTINCT FROM
    'CHECK (movement_classification IS NULL OR (movement_classification = ANY (ARRAY[''PRE_BANK_CANCEL''::text, ''NO_MONEY_UNWIND''::text, ''TRUE_SETTLED_REVERSAL_REQUIRED''::text, ''AMBIGUOUS_REVIEW_REQUIRED''::text, ''PROVIDER_CANCELLED_NO_MONEY''::text, ''PROVIDER_FAILED_NO_MONEY''::text])))'
     OR v_validated IS DISTINCT FROM true THEN
    RAISE EXCEPTION 'BANK_EVENT_MOVEMENT_CLASSIFICATION_CONTRACT_NOT_INSTALLED'
      USING ERRCODE = 'P0001';
  END IF;

  SELECT pg_catalog.pg_get_functiondef(routine_row.oid)
  INTO v_event_ingest_definition
  FROM pg_catalog.pg_proc AS routine_row
  JOIN pg_catalog.pg_namespace AS routine_namespace
    ON routine_namespace.oid = routine_row.pronamespace
  WHERE routine_namespace.nspname = 'public'
    AND routine_row.proname = 'pay_bank_event_ingest'
    AND pg_catalog.pg_get_function_identity_arguments(routine_row.oid) =
      'p_event_json jsonb, p_actor_user_id uuid, p_ingest_options_json jsonb';

  IF v_event_ingest_definition IS NULL
     OR pg_catalog.strpos(v_event_ingest_definition, 'movement_classification = v_classification') = 0
     OR pg_catalog.strpos(v_event_ingest_definition, 'PROVIDER_CANCELLED_NO_MONEY') = 0
     OR pg_catalog.strpos(v_event_ingest_definition, 'PROVIDER_FAILED_NO_MONEY') = 0 THEN
    RAISE EXCEPTION 'BANK_EVENT_MOVEMENT_CLASSIFICATION_WRITER_OWNER_MISMATCH'
      USING ERRCODE = 'P0001';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_catalog.pg_constraint AS replacement_row
    WHERE replacement_row.conrelid = 'public.pay_bank_transfer_events'::pg_catalog.regclass
      AND replacement_row.conname = 'pay_bank_transfer_events_move_class_v1_replacement_chk'
  ) THEN
    RAISE EXCEPTION 'BANK_EVENT_MOVEMENT_CLASSIFICATION_TEMPORARY_CONSTRAINT_RETAINED'
      USING ERRCODE = 'P0001';
  END IF;
END;
$verification$;
