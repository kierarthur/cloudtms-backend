-- Purpose:
--   Extend pay_snooze_warning_ack_scope_chk so dated TIMESHEET_EXPENSE
--   warning acknowledgements can retain their exact source_ref identity.
--
-- Rerun behaviour:
--   The migration derives PostgreSQL's parsed form of the exact required CHECK
--   expression on a temporary table and compares that expression with the
--   installed canonical constraint. If the installed constraint is already
--   identical and validated, the migration exits without altering or locking
--   the application table. Otherwise, it creates and validates a candidate
--   constraint before replacing the canonical constraint.
--
-- Transaction handling:
--   Intentionally contains no explicit BEGIN/COMMIT. The DO statement is one
--   atomic PostgreSQL statement, and omitting transaction-control statements
--   avoids interfering with the migration runner's own transaction/history
--   handling.

DO $migration$
DECLARE
  v_table_oid oid;
  v_existing_type "char";
  v_existing_expression text;
  v_existing_validated boolean := false;
  v_existing_no_inherit boolean := false;
  v_candidate_type "char";
  v_candidate_expression text;
  v_candidate_no_inherit boolean := false;
  v_expected_table_oid oid;
  v_expected_expression text;
  v_expected_no_inherit boolean := false;
BEGIN
  SELECT table_row.oid
  INTO v_table_oid
  FROM pg_class AS table_row
  JOIN pg_namespace AS schema_row
    ON schema_row.oid = table_row.relnamespace
  WHERE schema_row.nspname = 'public'
    AND table_row.relname = 'pay_snooze_warning_acknowledgements'
    AND table_row.relkind IN ('r', 'p');

  IF v_table_oid IS NULL THEN
    RAISE EXCEPTION 'PAY_SNOOZE_WARNING_ACK_TABLE_MISSING'
      USING ERRCODE = '42P01',
            DETAIL = 'Required table public.pay_snooze_warning_acknowledgements does not exist.';
  END IF;

  SELECT constraint_row.contype
  INTO v_existing_type
  FROM pg_constraint AS constraint_row
  WHERE constraint_row.conrelid = v_table_oid
    AND constraint_row.conname = 'pay_snooze_warning_ack_scope_chk';

  IF v_existing_type IS NOT NULL AND v_existing_type <> 'c' THEN
    RAISE EXCEPTION 'PAY_SNOOZE_WARNING_ACK_SCOPE_CONSTRAINT_TYPE_INVALID'
      USING ERRCODE = '42809',
            DETAIL = 'public.pay_snooze_warning_ack_scope_chk exists but is not a CHECK constraint.';
  END IF;

  SELECT constraint_row.contype
  INTO v_candidate_type
  FROM pg_constraint AS constraint_row
  WHERE constraint_row.conrelid = v_table_oid
    AND constraint_row.conname = 'pay_snooze_warning_ack_scope_chk_v2';

  IF v_candidate_type IS NOT NULL AND v_candidate_type <> 'c' THEN
    RAISE EXCEPTION 'PAY_SNOOZE_WARNING_ACK_SCOPE_CANDIDATE_TYPE_INVALID'
      USING ERRCODE = '42809',
            DETAIL = 'public.pay_snooze_warning_ack_scope_chk_v2 exists but is not a CHECK constraint.';
  END IF;

  IF v_candidate_type = 'c' THEN
    ALTER TABLE public.pay_snooze_warning_acknowledgements
      DROP CONSTRAINT pay_snooze_warning_ack_scope_chk_v2;
    v_candidate_type := NULL;
  END IF;

  CREATE TEMPORARY TABLE pay_snooze_warning_ack_scope_expected_20260715 (
    snooze_kind text,
    source_ref text,
    timesheet_id uuid,
    segment_id text,
    segment_stable_key text,
    CONSTRAINT pay_snooze_warning_ack_scope_expected_chk
    CHECK (
      (
        snooze_kind = ANY (
          ARRAY[
            'OVERPAYMENT_RECOVERY'::text,
            'PAYMENT_ADVANCE_REPAYMENT'::text,
            'MANUAL_DEBT_RECOVERY'::text
          ]
        )
        AND source_ref ~* '^advance:[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'::text
        AND timesheet_id IS NULL
        AND segment_id IS NULL
        AND segment_stable_key IS NULL
      )
      OR
      (
        snooze_kind = 'DO_NOT_PAY'::text
        AND lower(btrim(source_ref)) ~ '^timesheet-expense:[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}:(expenses|travel|accommodation|other|mileage):[0-9a-f]{32}$'::text
        AND timesheet_id IS NOT NULL
        AND split_part(lower(btrim(source_ref)), ':'::text, 2) = lower(timesheet_id::text)
        AND segment_id IS NULL
        AND segment_stable_key IS NULL
      )
      OR
      (
        snooze_kind = ANY (
          ARRAY[
            'DO_NOT_PAY'::text,
            'BLOCKED_TIMESHEET'::text,
            'TIMESHEET_PAYMENT'::text
          ]
        )
        AND source_ref IS NULL
      )
    )
  ) ON COMMIT DROP;

  v_expected_table_oid := to_regclass('pg_temp.pay_snooze_warning_ack_scope_expected_20260715');

  SELECT
    pg_get_expr(constraint_row.conbin, constraint_row.conrelid, true),
    constraint_row.connoinherit
  INTO
    v_expected_expression,
    v_expected_no_inherit
  FROM pg_constraint AS constraint_row
  WHERE constraint_row.conrelid = v_expected_table_oid
    AND constraint_row.conname = 'pay_snooze_warning_ack_scope_expected_chk'
    AND constraint_row.contype = 'c';

  IF v_expected_expression IS NULL THEN
    RAISE EXCEPTION 'PAY_SNOOZE_WARNING_ACK_SCOPE_EXPECTED_EXPRESSION_MISSING'
      USING ERRCODE = '42704',
            DETAIL = 'The expected CHECK expression could not be resolved.';
  END IF;

  SELECT
    pg_get_expr(constraint_row.conbin, constraint_row.conrelid, true),
    constraint_row.convalidated,
    constraint_row.connoinherit
  INTO
    v_existing_expression,
    v_existing_validated,
    v_existing_no_inherit
  FROM pg_constraint AS constraint_row
  WHERE constraint_row.conrelid = v_table_oid
    AND constraint_row.conname = 'pay_snooze_warning_ack_scope_chk'
    AND constraint_row.contype = 'c';

  IF v_existing_expression IS NOT NULL
     AND v_existing_validated IS TRUE
     AND v_existing_no_inherit = v_expected_no_inherit
     AND v_existing_expression = v_expected_expression THEN
    DROP TABLE pay_snooze_warning_ack_scope_expected_20260715;
    RETURN;
  END IF;

  DROP TABLE pay_snooze_warning_ack_scope_expected_20260715;

  ALTER TABLE public.pay_snooze_warning_acknowledgements
    ADD CONSTRAINT pay_snooze_warning_ack_scope_chk_v2
    CHECK (
      (
        snooze_kind = ANY (
          ARRAY[
            'OVERPAYMENT_RECOVERY'::text,
            'PAYMENT_ADVANCE_REPAYMENT'::text,
            'MANUAL_DEBT_RECOVERY'::text
          ]
        )
        AND source_ref ~* '^advance:[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'::text
        AND timesheet_id IS NULL
        AND segment_id IS NULL
        AND segment_stable_key IS NULL
      )
      OR
      (
        snooze_kind = 'DO_NOT_PAY'::text
        AND lower(btrim(source_ref)) ~ '^timesheet-expense:[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}:(expenses|travel|accommodation|other|mileage):[0-9a-f]{32}$'::text
        AND timesheet_id IS NOT NULL
        AND split_part(lower(btrim(source_ref)), ':'::text, 2) = lower(timesheet_id::text)
        AND segment_id IS NULL
        AND segment_stable_key IS NULL
      )
      OR
      (
        snooze_kind = ANY (
          ARRAY[
            'DO_NOT_PAY'::text,
            'BLOCKED_TIMESHEET'::text,
            'TIMESHEET_PAYMENT'::text
          ]
        )
        AND source_ref IS NULL
      )
    ) NOT VALID;

  SELECT
    pg_get_expr(constraint_row.conbin, constraint_row.conrelid, true),
    constraint_row.connoinherit
  INTO
    v_candidate_expression,
    v_candidate_no_inherit
  FROM pg_constraint AS constraint_row
  WHERE constraint_row.conrelid = v_table_oid
    AND constraint_row.conname = 'pay_snooze_warning_ack_scope_chk_v2'
    AND constraint_row.contype = 'c';

  IF v_candidate_expression IS NULL
     OR v_candidate_expression <> v_expected_expression
     OR v_candidate_no_inherit <> v_expected_no_inherit THEN
    RAISE EXCEPTION 'PAY_SNOOZE_WARNING_ACK_SCOPE_CANDIDATE_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = 'The candidate CHECK constraint does not match the expected parsed expression.';
  END IF;

  ALTER TABLE public.pay_snooze_warning_acknowledgements
    VALIDATE CONSTRAINT pay_snooze_warning_ack_scope_chk_v2;

  IF v_existing_expression IS NOT NULL THEN
    ALTER TABLE public.pay_snooze_warning_acknowledgements
      DROP CONSTRAINT pay_snooze_warning_ack_scope_chk;
  END IF;

  ALTER TABLE public.pay_snooze_warning_acknowledgements
    RENAME CONSTRAINT pay_snooze_warning_ack_scope_chk_v2
    TO pay_snooze_warning_ack_scope_chk;
END;
$migration$;
