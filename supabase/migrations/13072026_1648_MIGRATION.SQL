-- CloudTMS Banking Pay case-resolution immutable-origin finalisation
--
-- This migration is safe to rerun. It:
--   1. verifies the prerequisite schema, guard trigger and bounded backfill RPC;
--   2. completes any remaining valid provenance backfill in bounded pages;
--   3. fails closed if any provenance is unresolved, invalid or inconsistent;
--   4. validates the origin-session foreign key;
--   5. makes origin session/pay-date mandatory; and
--   6. removes browser-role update access to immutable origin columns.
--
-- Policy X: provenance metadata/constraints only. No payment economics,
-- source rates, draft artefacts, settlement or historical amounts are changed.
--
-- IMPORTANT: place this migration after:
--   - the origin prerequisite migration;
--   - pay_workbench_case_resolution_origin_guard_v1 and its trigger; and
--   - pay_workbench_case_resolution_origin_backfill_v1.

BEGIN;

SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '0';

-- Prevent two finalisation attempts from running concurrently.
SELECT pg_catalog.pg_advisory_xact_lock(
  pg_catalog.hashtext('cloudtms:banking-pay:case-resolution-origin-finalise:v1')
);

-- ---------------------------------------------------------------------------
-- Fail-closed prerequisite verification.
-- ---------------------------------------------------------------------------
DO $migration$
DECLARE
  v_missing_columns text[];
  v_guard_trigger_ok boolean := false;
BEGIN
  IF pg_catalog.to_regclass('public.banking_pay_workbench_session_case_resolutions') IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_RESOLUTION_ORIGIN_FINALISE_TABLE_MISSING'
      USING ERRCODE = 'P0001';
  END IF;

  IF pg_catalog.to_regclass('public.banking_pay_workbench_sessions') IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_RESOLUTION_ORIGIN_FINALISE_SESSION_TABLE_MISSING'
      USING ERRCODE = 'P0001';
  END IF;

  SELECT COALESCE(
           pg_catalog.array_agg(required_column.column_name ORDER BY required_column.column_name),
           ARRAY[]::text[]
         )
  INTO v_missing_columns
  FROM (
    VALUES
      ('resolution_origin_session_id'::text),
      ('resolution_origin_pay_date'::text),
      ('resolution_origin_source_basis_fingerprint'::text),
      ('payload_json'::text),
      ('session_id'::text)
  ) AS required_column(column_name)
  WHERE NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_attribute AS attribute_row
    WHERE attribute_row.attrelid = 'public.banking_pay_workbench_session_case_resolutions'::regclass
      AND attribute_row.attname = required_column.column_name
      AND attribute_row.attnum > 0
      AND attribute_row.attisdropped IS FALSE
  );

  IF pg_catalog.cardinality(v_missing_columns) > 0 THEN
    RAISE EXCEPTION 'BANKING_PAY_RESOLUTION_ORIGIN_FINALISE_COLUMNS_MISSING'
      USING ERRCODE = 'P0001',
            DETAIL = pg_catalog.jsonb_build_object(
              'code', 'BANKING_PAY_RESOLUTION_ORIGIN_FINALISE_COLUMNS_MISSING',
              'missing_columns', pg_catalog.to_jsonb(v_missing_columns)
            )::text;
  END IF;

  IF pg_catalog.to_regprocedure(
       'public.pay_workbench_case_resolution_origin_backfill_v1(uuid,integer,boolean)'
     ) IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_RESOLUTION_ORIGIN_BACKFILL_FUNCTION_MISSING'
      USING ERRCODE = 'P0001';
  END IF;

  IF pg_catalog.to_regprocedure(
       'public.pay_workbench_case_resolution_origin_guard_v1()'
     ) IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_RESOLUTION_ORIGIN_GUARD_FUNCTION_MISSING'
      USING ERRCODE = 'P0001';
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_trigger AS trigger_row
    JOIN pg_catalog.pg_proc AS function_row
      ON function_row.oid = trigger_row.tgfoid
    JOIN pg_catalog.pg_namespace AS function_schema
      ON function_schema.oid = function_row.pronamespace
    WHERE trigger_row.tgrelid = 'public.banking_pay_workbench_session_case_resolutions'::regclass
      AND trigger_row.tgname = 'trg_bpay_wb_case_resolution_origin_guard'
      AND trigger_row.tgisinternal IS FALSE
      AND trigger_row.tgenabled IN ('O', 'A')
      AND function_schema.nspname = 'public'
      AND function_row.proname = 'pay_workbench_case_resolution_origin_guard_v1'
  )
  INTO v_guard_trigger_ok;

  IF v_guard_trigger_ok IS NOT TRUE THEN
    RAISE EXCEPTION 'BANKING_PAY_RESOLUTION_ORIGIN_GUARD_TRIGGER_MISSING_OR_DISABLED'
      USING ERRCODE = 'P0001';
  END IF;
END;
$migration$;

-- ---------------------------------------------------------------------------
-- Complete the existing bounded provenance backfill.
--
-- Each RPC call processes at most 1,000 rows. Under a normal transactional
-- migration runner the pages commit with this file; installations requiring
-- page-by-page commits should pre-run the same RPC in committed pages. This
-- loop remains the final fail-closed catch-up and is safe to rerun.
-- ---------------------------------------------------------------------------
DO $migration$
DECLARE
  v_after_id uuid := NULL::uuid;
  v_next_id uuid := NULL::uuid;
  v_result jsonb := '{}'::jsonb;
  v_iteration integer := 0;
  v_max_iterations constant integer := 100000;
  v_has_more boolean := false;
  v_complete boolean := false;
  v_invalid_count integer := 0;
BEGIN
  LOOP
    v_iteration := v_iteration + 1;

    IF v_iteration > v_max_iterations THEN
      RAISE EXCEPTION 'BANKING_PAY_RESOLUTION_ORIGIN_BACKFILL_ITERATION_LIMIT'
        USING ERRCODE = 'P0001',
              DETAIL = pg_catalog.jsonb_build_object(
                'code', 'BANKING_PAY_RESOLUTION_ORIGIN_BACKFILL_ITERATION_LIMIT',
                'max_iterations', v_max_iterations,
                'after_id', CASE WHEN v_after_id IS NULL THEN NULL ELSE v_after_id::text END
              )::text;
    END IF;

    SELECT public.pay_workbench_case_resolution_origin_backfill_v1(
      p_after_id => v_after_id,
      p_limit => 1000,
      p_apply => true
    )
    INTO v_result;

    IF pg_catalog.jsonb_typeof(v_result) IS DISTINCT FROM 'object' THEN
      RAISE EXCEPTION 'BANKING_PAY_RESOLUTION_ORIGIN_BACKFILL_INVALID_RESULT'
        USING ERRCODE = 'P0001';
    END IF;

    v_invalid_count := COALESCE(NULLIF(v_result->>'invalid_count', '')::integer, 0);
    v_has_more := COALESCE(NULLIF(v_result->>'has_more', '')::boolean, false);
    v_complete := COALESCE(NULLIF(v_result->>'complete', '')::boolean, false);

    IF COALESCE(NULLIF(v_result->>'ok', '')::boolean, false) IS NOT TRUE
       OR v_invalid_count <> 0 THEN
      RAISE EXCEPTION 'BANKING_PAY_RESOLUTION_ORIGIN_BACKFILL_INVALID_PROVENANCE'
        USING ERRCODE = 'P0001',
              DETAIL = pg_catalog.jsonb_build_object(
                'code', 'BANKING_PAY_RESOLUTION_ORIGIN_BACKFILL_INVALID_PROVENANCE',
                'iteration', v_iteration,
                'after_id', CASE WHEN v_after_id IS NULL THEN NULL ELSE v_after_id::text END,
                'result', v_result
              )::text;
    END IF;

    IF v_has_more IS NOT TRUE THEN
      IF v_complete IS NOT TRUE THEN
        RAISE EXCEPTION 'BANKING_PAY_RESOLUTION_ORIGIN_BACKFILL_INCOMPLETE'
          USING ERRCODE = 'P0001',
                DETAIL = pg_catalog.jsonb_build_object(
                  'code', 'BANKING_PAY_RESOLUTION_ORIGIN_BACKFILL_INCOMPLETE',
                  'iteration', v_iteration,
                  'result', v_result
                )::text;
      END IF;
      EXIT;
    END IF;

    BEGIN
      v_next_id := NULLIF(v_result->>'next_cursor', '')::uuid;
    EXCEPTION WHEN OTHERS THEN
      RAISE EXCEPTION 'BANKING_PAY_RESOLUTION_ORIGIN_BACKFILL_CURSOR_INVALID'
        USING ERRCODE = 'P0001',
              DETAIL = pg_catalog.jsonb_build_object(
                'code', 'BANKING_PAY_RESOLUTION_ORIGIN_BACKFILL_CURSOR_INVALID',
                'iteration', v_iteration,
                'result', v_result
              )::text;
    END;

    IF v_next_id IS NULL OR v_next_id IS NOT DISTINCT FROM v_after_id THEN
      RAISE EXCEPTION 'BANKING_PAY_RESOLUTION_ORIGIN_BACKFILL_NO_PROGRESS'
        USING ERRCODE = 'P0001',
              DETAIL = pg_catalog.jsonb_build_object(
                'code', 'BANKING_PAY_RESOLUTION_ORIGIN_BACKFILL_NO_PROGRESS',
                'iteration', v_iteration,
                'after_id', CASE WHEN v_after_id IS NULL THEN NULL ELSE v_after_id::text END,
                'next_cursor', v_result->>'next_cursor'
              )::text;
    END IF;

    v_after_id := v_next_id;
  END LOOP;
END;
$migration$;

-- ---------------------------------------------------------------------------
-- Build a deterministic post-backfill validation snapshot.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS pg_temp._bpay_resolution_origin_finalise_validation;

CREATE TEMP TABLE pg_temp._bpay_resolution_origin_finalise_validation
ON COMMIT DROP
AS
WITH base_rows AS (
  SELECT resolution_row.id,
         resolution_row.session_id,
         resolution_row.resolution_origin_session_id,
         resolution_row.resolution_origin_pay_date,
         NULLIF(BTRIM(COALESCE(
           resolution_row.resolution_origin_source_basis_fingerprint,
           ''
         )), '') AS physical_origin_fingerprint,
         current_session.id AS current_session_found_id,
         current_session.actor_user_id AS current_actor_user_id,
         current_session.pay_date AS current_session_pay_date,
         origin_session.id AS origin_session_found_id,
         origin_session.actor_user_id AS origin_actor_user_id,
         origin_session.pay_date AS origin_session_pay_date,
         NULLIF(BTRIM(COALESCE(
           resolution_row.payload_json->>'resolution_origin_session_id',
           ''
         )), '') AS payload_origin_session_id_text,
         NULLIF(BTRIM(COALESCE(
           resolution_row.payload_json->>'resolution_origin_pay_date',
           ''
         )), '') AS payload_origin_pay_date_text,
         NULLIF(BTRIM(COALESCE(
           resolution_row.payload_json->>'resolution_origin_source_basis_fingerprint',
           ''
         )), '') AS payload_origin_fingerprint
  FROM public.banking_pay_workbench_session_case_resolutions AS resolution_row
  LEFT JOIN public.banking_pay_workbench_sessions AS current_session
    ON current_session.id = resolution_row.session_id
  LEFT JOIN public.banking_pay_workbench_sessions AS origin_session
    ON origin_session.id = resolution_row.resolution_origin_session_id
),
parsed_rows AS (
  SELECT base_row.*,
         CASE
           WHEN base_row.payload_origin_session_id_text
                  ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
             THEN base_row.payload_origin_session_id_text::uuid
           ELSE NULL::uuid
         END AS payload_origin_session_id,
         CASE
           WHEN base_row.payload_origin_pay_date_text ~ '^\d{4}-\d{2}-\d{2}$'
                AND pg_catalog.to_char(
                      pg_catalog.to_date(base_row.payload_origin_pay_date_text, 'YYYY-MM-DD'),
                      'YYYY-MM-DD'
                    ) = base_row.payload_origin_pay_date_text
             THEN pg_catalog.to_date(base_row.payload_origin_pay_date_text, 'YYYY-MM-DD')
           ELSE NULL::date
         END AS payload_origin_pay_date
  FROM base_rows AS base_row
),
classified_rows AS (
  SELECT parsed_row.*,
         (
           parsed_row.resolution_origin_session_id IS NULL
           OR parsed_row.resolution_origin_pay_date IS NULL
         ) AS is_unresolved,
         (
           parsed_row.current_session_found_id IS NULL
           OR (
             parsed_row.resolution_origin_session_id IS NOT NULL
             AND parsed_row.origin_session_found_id IS NULL
           )
           OR (
             parsed_row.current_session_found_id IS NOT NULL
             AND parsed_row.origin_session_found_id IS NOT NULL
             AND parsed_row.origin_actor_user_id IS DISTINCT FROM parsed_row.current_actor_user_id
           )
           OR (
             parsed_row.current_session_found_id IS NOT NULL
             AND parsed_row.origin_session_found_id IS NOT NULL
             AND parsed_row.origin_session_pay_date > parsed_row.current_session_pay_date
           )
           OR (
             parsed_row.origin_session_found_id IS NOT NULL
             AND parsed_row.resolution_origin_pay_date IS DISTINCT FROM parsed_row.origin_session_pay_date
           )
           OR (
             parsed_row.payload_origin_session_id_text IS NOT NULL
             AND parsed_row.payload_origin_session_id IS NULL
           )
           OR (
             parsed_row.payload_origin_pay_date_text IS NOT NULL
             AND parsed_row.payload_origin_pay_date IS NULL
           )
         ) AS is_invalid
  FROM parsed_rows AS parsed_row
)
SELECT classified_row.id,
       classified_row.is_unresolved,
       classified_row.is_invalid,
       (
         classified_row.is_unresolved IS FALSE
         AND classified_row.is_invalid IS FALSE
         AND (
           classified_row.payload_origin_session_id_text IS NULL
           OR classified_row.payload_origin_pay_date_text IS NULL
           OR classified_row.payload_origin_session_id
                IS DISTINCT FROM classified_row.resolution_origin_session_id
           OR classified_row.payload_origin_pay_date
                IS DISTINCT FROM classified_row.resolution_origin_pay_date
           OR classified_row.payload_origin_fingerprint
                IS DISTINCT FROM classified_row.physical_origin_fingerprint
         )
       ) AS is_ambiguous_or_inconsistent,
       pg_catalog.jsonb_strip_nulls(
         pg_catalog.jsonb_build_object(
           'resolution_id', classified_row.id::text,
           'session_id', classified_row.session_id::text,
           'origin_session_id', CASE
             WHEN classified_row.resolution_origin_session_id IS NULL THEN NULL
             ELSE classified_row.resolution_origin_session_id::text
           END,
           'origin_pay_date', CASE
             WHEN classified_row.resolution_origin_pay_date IS NULL THEN NULL
             ELSE classified_row.resolution_origin_pay_date::text
           END,
           'current_session_found', classified_row.current_session_found_id IS NOT NULL,
           'origin_session_found', classified_row.origin_session_found_id IS NOT NULL,
           'current_actor_user_id', CASE
             WHEN classified_row.current_actor_user_id IS NULL THEN NULL
             ELSE classified_row.current_actor_user_id::text
           END,
           'origin_actor_user_id', CASE
             WHEN classified_row.origin_actor_user_id IS NULL THEN NULL
             ELSE classified_row.origin_actor_user_id::text
           END,
           'current_session_pay_date', CASE
             WHEN classified_row.current_session_pay_date IS NULL THEN NULL
             ELSE classified_row.current_session_pay_date::text
           END,
           'origin_session_pay_date', CASE
             WHEN classified_row.origin_session_pay_date IS NULL THEN NULL
             ELSE classified_row.origin_session_pay_date::text
           END,
           'payload_origin_session_id', classified_row.payload_origin_session_id_text,
           'payload_origin_pay_date', classified_row.payload_origin_pay_date_text,
           'physical_origin_fingerprint', classified_row.physical_origin_fingerprint,
           'payload_origin_fingerprint', classified_row.payload_origin_fingerprint
         )
       ) AS evidence_json
FROM classified_rows AS classified_row;

DO $migration$
DECLARE
  v_unresolved_count bigint := 0;
  v_invalid_count bigint := 0;
  v_ambiguous_count bigint := 0;
  v_samples jsonb := '[]'::jsonb;
BEGIN
  SELECT COUNT(*) FILTER (WHERE validation_row.is_unresolved),
         COUNT(*) FILTER (WHERE validation_row.is_invalid),
         COUNT(*) FILTER (WHERE validation_row.is_ambiguous_or_inconsistent)
  INTO v_unresolved_count,
       v_invalid_count,
       v_ambiguous_count
  FROM pg_temp._bpay_resolution_origin_finalise_validation AS validation_row;

  IF v_unresolved_count <> 0
     OR v_invalid_count <> 0
     OR v_ambiguous_count <> 0 THEN
    SELECT COALESCE(
             pg_catalog.jsonb_agg(sample_row.evidence_json ORDER BY sample_row.id),
             '[]'::jsonb
           )
    INTO v_samples
    FROM (
      SELECT validation_row.id,
             validation_row.evidence_json
      FROM pg_temp._bpay_resolution_origin_finalise_validation AS validation_row
      WHERE validation_row.is_unresolved
         OR validation_row.is_invalid
         OR validation_row.is_ambiguous_or_inconsistent
      ORDER BY validation_row.id
      LIMIT 25
    ) AS sample_row;

    RAISE EXCEPTION 'BANKING_PAY_RESOLUTION_ORIGIN_FINALISATION_BLOCKED'
      USING ERRCODE = 'P0001',
            DETAIL = pg_catalog.jsonb_build_object(
              'code', 'BANKING_PAY_RESOLUTION_ORIGIN_FINALISATION_BLOCKED',
              'unresolved_count', v_unresolved_count,
              'invalid_count', v_invalid_count,
              'ambiguous_or_inconsistent_count', v_ambiguous_count,
              'samples', v_samples
            )::text;
  END IF;
END;
$migration$;

-- ---------------------------------------------------------------------------
-- Ensure the named foreign key exists with the required exact relationship.
-- ---------------------------------------------------------------------------
DO $migration$
DECLARE
  v_constraint_oid oid;
  v_origin_attnum smallint;
  v_session_id_attnum smallint;
  v_constraint_ok boolean := false;
BEGIN
  SELECT attribute_row.attnum::smallint
  INTO v_origin_attnum
  FROM pg_catalog.pg_attribute AS attribute_row
  WHERE attribute_row.attrelid = 'public.banking_pay_workbench_session_case_resolutions'::regclass
    AND attribute_row.attname = 'resolution_origin_session_id'
    AND attribute_row.attnum > 0
    AND attribute_row.attisdropped IS FALSE;

  SELECT attribute_row.attnum::smallint
  INTO v_session_id_attnum
  FROM pg_catalog.pg_attribute AS attribute_row
  WHERE attribute_row.attrelid = 'public.banking_pay_workbench_sessions'::regclass
    AND attribute_row.attname = 'id'
    AND attribute_row.attnum > 0
    AND attribute_row.attisdropped IS FALSE;

  SELECT constraint_row.oid,
         (
           constraint_row.contype = 'f'
           AND constraint_row.confrelid = 'public.banking_pay_workbench_sessions'::regclass
           AND constraint_row.confdeltype = 'r'
           AND constraint_row.conkey = ARRAY[v_origin_attnum]::smallint[]
           AND constraint_row.confkey = ARRAY[v_session_id_attnum]::smallint[]
         )
  INTO v_constraint_oid,
       v_constraint_ok
  FROM pg_catalog.pg_constraint AS constraint_row
  WHERE constraint_row.conrelid = 'public.banking_pay_workbench_session_case_resolutions'::regclass
    AND constraint_row.conname = 'bpay_wb_case_resolution_origin_session_fk';

  IF v_constraint_oid IS NULL THEN
    ALTER TABLE public.banking_pay_workbench_session_case_resolutions
      ADD CONSTRAINT bpay_wb_case_resolution_origin_session_fk
      FOREIGN KEY (resolution_origin_session_id)
      REFERENCES public.banking_pay_workbench_sessions(id)
      ON DELETE RESTRICT
      NOT VALID;
  ELSIF v_constraint_ok IS NOT TRUE THEN
    RAISE EXCEPTION 'BANKING_PAY_RESOLUTION_ORIGIN_FK_DEFINITION_MISMATCH'
      USING ERRCODE = 'P0001';
  END IF;
END;
$migration$;

DO $migration$
DECLARE
  v_fk_validated boolean := false;
  v_origin_session_not_null boolean := false;
  v_origin_pay_date_not_null boolean := false;
BEGIN
  SELECT constraint_row.convalidated
  INTO v_fk_validated
  FROM pg_catalog.pg_constraint AS constraint_row
  WHERE constraint_row.conrelid = 'public.banking_pay_workbench_session_case_resolutions'::regclass
    AND constraint_row.conname = 'bpay_wb_case_resolution_origin_session_fk'
    AND constraint_row.contype = 'f';

  IF v_fk_validated IS NOT TRUE THEN
    ALTER TABLE public.banking_pay_workbench_session_case_resolutions
      VALIDATE CONSTRAINT bpay_wb_case_resolution_origin_session_fk;
  END IF;

  SELECT attribute_row.attnotnull
  INTO v_origin_session_not_null
  FROM pg_catalog.pg_attribute AS attribute_row
  WHERE attribute_row.attrelid = 'public.banking_pay_workbench_session_case_resolutions'::regclass
    AND attribute_row.attname = 'resolution_origin_session_id'
    AND attribute_row.attnum > 0
    AND attribute_row.attisdropped IS FALSE;

  IF v_origin_session_not_null IS NOT TRUE THEN
    ALTER TABLE public.banking_pay_workbench_session_case_resolutions
      ALTER COLUMN resolution_origin_session_id SET NOT NULL;
  END IF;

  SELECT attribute_row.attnotnull
  INTO v_origin_pay_date_not_null
  FROM pg_catalog.pg_attribute AS attribute_row
  WHERE attribute_row.attrelid = 'public.banking_pay_workbench_session_case_resolutions'::regclass
    AND attribute_row.attname = 'resolution_origin_pay_date'
    AND attribute_row.attnum > 0
    AND attribute_row.attisdropped IS FALSE;

  IF v_origin_pay_date_not_null IS NOT TRUE THEN
    ALTER TABLE public.banking_pay_workbench_session_case_resolutions
      ALTER COLUMN resolution_origin_pay_date SET NOT NULL;
  END IF;
END;
$migration$;

-- ---------------------------------------------------------------------------
-- Origin values are written by guarded internal authorities, not browser roles.
-- Repeated REVOKE operations are idempotent.
-- ---------------------------------------------------------------------------
REVOKE UPDATE (
  resolution_origin_session_id,
  resolution_origin_pay_date,
  resolution_origin_source_basis_fingerprint
) ON TABLE public.banking_pay_workbench_session_case_resolutions FROM PUBLIC;

REVOKE ALL ON FUNCTION public.pay_workbench_case_resolution_origin_backfill_v1(uuid, integer, boolean)
FROM PUBLIC;

DO $migration$
DECLARE
  v_role_name name;
BEGIN
  FOR v_role_name IN
    SELECT role_row.rolname
    FROM pg_catalog.pg_roles AS role_row
    WHERE role_row.rolname IN ('anon', 'authenticated')
    ORDER BY role_row.rolname
  LOOP
    EXECUTE pg_catalog.format(
      'REVOKE UPDATE (resolution_origin_session_id, resolution_origin_pay_date, resolution_origin_source_basis_fingerprint) ON TABLE public.banking_pay_workbench_session_case_resolutions FROM %I',
      v_role_name
    );

    EXECUTE pg_catalog.format(
      'REVOKE ALL ON FUNCTION public.pay_workbench_case_resolution_origin_backfill_v1(uuid, integer, boolean) FROM %I',
      v_role_name
    );
  END LOOP;
END;
$migration$;

-- ---------------------------------------------------------------------------
-- Final post-condition assertion. This makes a rerun a verification pass.
-- ---------------------------------------------------------------------------
DO $migration$
DECLARE
  v_origin_session_not_null boolean := false;
  v_origin_pay_date_not_null boolean := false;
  v_fk_validated boolean := false;
BEGIN
  SELECT attribute_row.attnotnull
  INTO v_origin_session_not_null
  FROM pg_catalog.pg_attribute AS attribute_row
  WHERE attribute_row.attrelid = 'public.banking_pay_workbench_session_case_resolutions'::regclass
    AND attribute_row.attname = 'resolution_origin_session_id'
    AND attribute_row.attnum > 0
    AND attribute_row.attisdropped IS FALSE;

  SELECT attribute_row.attnotnull
  INTO v_origin_pay_date_not_null
  FROM pg_catalog.pg_attribute AS attribute_row
  WHERE attribute_row.attrelid = 'public.banking_pay_workbench_session_case_resolutions'::regclass
    AND attribute_row.attname = 'resolution_origin_pay_date'
    AND attribute_row.attnum > 0
    AND attribute_row.attisdropped IS FALSE;

  SELECT constraint_row.convalidated
  INTO v_fk_validated
  FROM pg_catalog.pg_constraint AS constraint_row
  WHERE constraint_row.conrelid = 'public.banking_pay_workbench_session_case_resolutions'::regclass
    AND constraint_row.conname = 'bpay_wb_case_resolution_origin_session_fk'
    AND constraint_row.contype = 'f';

  IF v_origin_session_not_null IS NOT TRUE
     OR v_origin_pay_date_not_null IS NOT TRUE
     OR v_fk_validated IS NOT TRUE THEN
    RAISE EXCEPTION 'BANKING_PAY_RESOLUTION_ORIGIN_FINALISATION_POSTCONDITION_FAILED'
      USING ERRCODE = 'P0001',
            DETAIL = pg_catalog.jsonb_build_object(
              'code', 'BANKING_PAY_RESOLUTION_ORIGIN_FINALISATION_POSTCONDITION_FAILED',
              'resolution_origin_session_id_not_null', v_origin_session_not_null,
              'resolution_origin_pay_date_not_null', v_origin_pay_date_not_null,
              'origin_session_fk_validated', v_fk_validated
            )::text;
  END IF;
END;
$migration$;

COMMIT;
