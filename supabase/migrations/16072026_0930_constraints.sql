BEGIN;

DO $migration$
DECLARE
  v_existing_definition text;
  v_existing_validated boolean := false;
  v_existing_values text[] := ARRAY[]::text[];
  v_expected_values constant text[] := ARRAY[
    'GROSS_ADD',
    'GROSS_DEDUCT',
    'NET_ADD',
    'NET_DEDUCT',
    'NONE'
  ]::text[];
  v_invalid_values text[];
BEGIN
  /*
   * Read the current named constraint, if present.
   */
  SELECT
    pg_get_constraintdef(constraint_row.oid, true),
    constraint_row.convalidated
  INTO
    v_existing_definition,
    v_existing_validated
  FROM pg_constraint AS constraint_row
  JOIN pg_class AS table_row
    ON table_row.oid = constraint_row.conrelid
  JOIN pg_namespace AS schema_row
    ON schema_row.oid = table_row.relnamespace
  WHERE schema_row.nspname = 'public'
    AND table_row.relname = 'pay_batch_items'
    AND constraint_row.conname = 'pay_batch_items_paye_treatment_chk'
    AND constraint_row.contype = 'c';

  /*
   * Extract the permitted text literals from the current definition.
   * Sorting allows an equivalent constraint to be recognised regardless
   * of the order in which the permitted values were declared.
   */
  IF v_existing_definition IS NOT NULL THEN
    SELECT COALESCE(
      array_agg(
        DISTINCT upper(matches.captures[1])
        ORDER BY upper(matches.captures[1])
      ),
      ARRAY[]::text[]
    )
    INTO v_existing_values
    FROM regexp_matches(
      v_existing_definition,
      '''([^'']+)''::text',
      'g'
    ) AS matches(captures);
  END IF;

  /*
   * Once the correct validated constraint exists, repeated migration runs
   * perform no schema change.
   */
  IF v_existing_validated
     AND v_existing_values = v_expected_values
     AND position(
       'PAYE_TREATMENT IS NULL'
       IN upper(v_existing_definition)
     ) > 0
  THEN
    /*
     * Remove a stale temporary constraint only if one was somehow left by
     * an earlier non-transactional/manual execution.
     */
    ALTER TABLE public.pay_batch_items
      DROP CONSTRAINT IF EXISTS
        pay_batch_items_paye_treatment_chk_replacement;

    RETURN;
  END IF;

  /*
   * Fail clearly rather than installing a constraint that existing rows
   * cannot satisfy. NULL remains valid.
   */
  SELECT array_agg(
    DISTINCT item_row.paye_treatment
    ORDER BY item_row.paye_treatment
  )
  INTO v_invalid_values
  FROM public.pay_batch_items AS item_row
  WHERE item_row.paye_treatment IS NOT NULL
    AND NOT (
      item_row.paye_treatment = ANY (v_expected_values)
    );

  IF COALESCE(array_length(v_invalid_values, 1), 0) > 0 THEN
    RAISE EXCEPTION
      'Cannot replace pay_batch_items_paye_treatment_chk: invalid existing paye_treatment values: %',
      v_invalid_values;
  END IF;

  /*
   * Ensure the deterministic temporary name is free. This also makes the
   * migration recoverable if someone previously executed only part of it
   * outside a transaction.
   */
  ALTER TABLE public.pay_batch_items
    DROP CONSTRAINT IF EXISTS
      pay_batch_items_paye_treatment_chk_replacement;

  /*
   * Install and validate the new constraint before removing the old one.
   * The existing narrower constraint continues protecting the table while
   * this replacement is validated.
   */
  ALTER TABLE public.pay_batch_items
    ADD CONSTRAINT pay_batch_items_paye_treatment_chk_replacement
    CHECK (
      paye_treatment IS NULL
      OR paye_treatment = ANY (
        ARRAY[
          'GROSS_ADD'::text,
          'GROSS_DEDUCT'::text,
          'NET_ADD'::text,
          'NET_DEDUCT'::text,
          'NONE'::text
        ]
      )
    )
    NOT VALID;

  ALTER TABLE public.pay_batch_items
    VALIDATE CONSTRAINT
      pay_batch_items_paye_treatment_chk_replacement;

  /*
   * Atomically replace the old named constraint with the validated one.
   */
  ALTER TABLE public.pay_batch_items
    DROP CONSTRAINT IF EXISTS pay_batch_items_paye_treatment_chk;

  ALTER TABLE public.pay_batch_items
    RENAME CONSTRAINT pay_batch_items_paye_treatment_chk_replacement
    TO pay_batch_items_paye_treatment_chk;
END;
$migration$;

COMMIT;