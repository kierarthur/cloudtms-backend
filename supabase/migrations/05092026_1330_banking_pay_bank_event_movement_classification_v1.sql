-- Banking Pay bank-event movement-classification storage compatibility.
-- Runtime authority is Miget TEST; the `supabase` directory name is historical.
--
-- The April 2026 table constraint predates the August 2026 cancellation owner.
-- The current pay_bank_event_ingest owner deliberately records the exact
-- terminal/no-money lifecycle classifications below, but the older constraint
-- rejects them. This migration changes no classification, payment policy,
-- money-movement decision, amount, cancellation rule or provider behaviour. It
-- only permits the two existing owner-produced values to be stored.

DO $migration$
DECLARE
  v_existing_definition text;
  v_old_definition constant text :=
    'CHECK (movement_classification IS NULL OR (movement_classification = ANY (ARRAY[''PRE_BANK_CANCEL''::text, ''NO_MONEY_UNWIND''::text, ''TRUE_SETTLED_REVERSAL_REQUIRED''::text, ''AMBIGUOUS_REVIEW_REQUIRED''::text])))';
  v_new_definition constant text :=
    'CHECK (movement_classification IS NULL OR (movement_classification = ANY (ARRAY[''PRE_BANK_CANCEL''::text, ''NO_MONEY_UNWIND''::text, ''TRUE_SETTLED_REVERSAL_REQUIRED''::text, ''AMBIGUOUS_REVIEW_REQUIRED''::text, ''PROVIDER_CANCELLED_NO_MONEY''::text, ''PROVIDER_FAILED_NO_MONEY''::text])))';
BEGIN
  SELECT pg_catalog.pg_get_constraintdef(constraint_row.oid, true)
  INTO v_existing_definition
  FROM pg_catalog.pg_constraint AS constraint_row
  WHERE constraint_row.conrelid = 'public.pay_bank_transfer_events'::pg_catalog.regclass
    AND constraint_row.conname = 'pay_bank_transfer_events_movement_classification_chk';

  IF v_existing_definition IS NULL THEN
    RAISE EXCEPTION 'PAY_BANK_TRANSFER_EVENTS_MOVEMENT_CLASSIFICATION_CONSTRAINT_MISSING'
      USING ERRCODE = 'P0001';
  END IF;

  IF v_existing_definition = v_new_definition THEN
    RETURN;
  END IF;

  IF v_existing_definition IS DISTINCT FROM v_old_definition THEN
    RAISE EXCEPTION 'PAY_BANK_TRANSFER_EVENTS_MOVEMENT_CLASSIFICATION_CONSTRAINT_UNEXPECTED'
      USING ERRCODE = 'P0001',
            DETAIL = pg_catalog.jsonb_build_object(
              'code', 'PAY_BANK_TRANSFER_EVENTS_MOVEMENT_CLASSIFICATION_CONSTRAINT_UNEXPECTED'
            )::text;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_catalog.pg_constraint AS replacement_row
    WHERE replacement_row.conrelid = 'public.pay_bank_transfer_events'::pg_catalog.regclass
      AND replacement_row.conname = 'pay_bank_transfer_events_move_class_v1_replacement_chk'
  ) THEN
    RAISE EXCEPTION 'PAY_BANK_TRANSFER_EVENTS_MOVEMENT_CLASSIFICATION_REPLACEMENT_ALREADY_EXISTS'
      USING ERRCODE = 'P0001';
  END IF;

  ALTER TABLE public.pay_bank_transfer_events
    ADD CONSTRAINT pay_bank_transfer_events_move_class_v1_replacement_chk
    CHECK (
      movement_classification IS NULL
      OR movement_classification IN (
        'PRE_BANK_CANCEL',
        'NO_MONEY_UNWIND',
        'TRUE_SETTLED_REVERSAL_REQUIRED',
        'AMBIGUOUS_REVIEW_REQUIRED',
        'PROVIDER_CANCELLED_NO_MONEY',
        'PROVIDER_FAILED_NO_MONEY'
      )
    ) NOT VALID;

  ALTER TABLE public.pay_bank_transfer_events
    VALIDATE CONSTRAINT pay_bank_transfer_events_move_class_v1_replacement_chk;

  ALTER TABLE public.pay_bank_transfer_events
    DROP CONSTRAINT pay_bank_transfer_events_movement_classification_chk;

  ALTER TABLE public.pay_bank_transfer_events
    RENAME CONSTRAINT pay_bank_transfer_events_move_class_v1_replacement_chk
    TO pay_bank_transfer_events_movement_classification_chk;
END;
$migration$;
