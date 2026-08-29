-- CloudTMS migration: allow AUTO_PROCESSING as a bank-event correction disposition.
-- Safe to rerun.
-- Purpose: distinguish clean automatic correction still processing in chunks from user-action-required states.

DO $$
DECLARE
  v_constraint_name text;
  v_bad_count bigint := 0;
  v_bad_values text := null;
BEGIN
  IF to_regclass('public.pay_bank_transfer_events') IS NULL THEN
    RAISE EXCEPTION 'Required table public.pay_bank_transfer_events does not exist';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns c
    WHERE c.table_schema = 'public'
      AND c.table_name = 'pay_bank_transfer_events'
      AND c.column_name = 'correction_disposition'
  ) THEN
    RAISE EXCEPTION 'Required column public.pay_bank_transfer_events.correction_disposition does not exist';
  END IF;

  UPDATE public.pay_bank_transfer_events
  SET correction_disposition = upper(btrim(correction_disposition))
  WHERE correction_disposition IS NOT NULL
    AND correction_disposition <> upper(btrim(correction_disposition));

  UPDATE public.pay_bank_transfer_events
  SET correction_disposition = 'AUTO_PROCESSING'
  WHERE correction_disposition = 'PROCESSING';

  SELECT count(*), string_agg(DISTINCT correction_disposition, ', ' ORDER BY correction_disposition)
  INTO v_bad_count, v_bad_values
  FROM public.pay_bank_transfer_events
  WHERE correction_disposition IS NOT NULL
    AND correction_disposition NOT IN (
      'NO_CORRECTION_REQUIRED',
      'ACTION_REQUIRED',
      'AMBIGUOUS',
      'BLOCKED',
      'FAILED',
      'AUTO_PROCESSING',
      'AUTO_APPLIED'
    );

  IF COALESCE(v_bad_count, 0) > 0 THEN
    RAISE EXCEPTION 'Cannot add correction_disposition check: % existing row(s) have unsupported value(s): %',
      v_bad_count,
      v_bad_values;
  END IF;

  FOR v_constraint_name IN
    SELECT con.conname
    FROM pg_constraint con
    JOIN pg_class rel ON rel.oid = con.conrelid
    JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
    WHERE nsp.nspname = 'public'
      AND rel.relname = 'pay_bank_transfer_events'
      AND con.contype = 'c'
      AND pg_get_constraintdef(con.oid) ILIKE '%correction_disposition%'
  LOOP
    EXECUTE format(
      'ALTER TABLE public.pay_bank_transfer_events DROP CONSTRAINT IF EXISTS %I',
      v_constraint_name
    );
  END LOOP;

  ALTER TABLE public.pay_bank_transfer_events
    ADD CONSTRAINT pay_bank_transfer_events_correction_disposition_chk
    CHECK (
      correction_disposition IS NULL
      OR correction_disposition IN (
        'NO_CORRECTION_REQUIRED',
        'ACTION_REQUIRED',
        'AMBIGUOUS',
        'BLOCKED',
        'FAILED',
        'AUTO_PROCESSING',
        'AUTO_APPLIED'
      )
    ) NOT VALID;

  ALTER TABLE public.pay_bank_transfer_events
    VALIDATE CONSTRAINT pay_bank_transfer_events_correction_disposition_chk;
END $$;

COMMENT ON CONSTRAINT pay_bank_transfer_events_correction_disposition_chk
ON public.pay_bank_transfer_events
IS 'Allowed correction disposition values. AUTO_PROCESSING means an automatic payment correction is safely processing in chunks and does not itself require user action.';
