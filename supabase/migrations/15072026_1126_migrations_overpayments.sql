BEGIN;

DO $migration$
DECLARE
  v_index_oid oid;
  v_index_definition text;
  v_index_table_schema text;
  v_index_table_name text;
  v_index_is_unique boolean;
  v_index_access_method text;
  v_index_key_count integer;
  v_index_attribute_count integer;
  v_index_key_columns text[];
  v_index_predicate text;
  v_actual_predicate_canonical text;
  v_expected_predicate_canonical constant text :=
    'advance_kind=''underpayment''::pay_advance_kind_enumandstatus=anyarray[''active''::pay_advance_status_enum,''paid_off''::pay_advance_status_enum]';
  v_duplicate record;
BEGIN
  -- Serialize concurrent executions of this exact migration.
  PERFORM pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(
      'public.uq_pay_advances_underpayment_case',
      0
    )
  );

  SELECT
    index_class.oid,
    pg_catalog.pg_get_indexdef(index_class.oid),
    table_schema.nspname,
    table_class.relname,
    index_row.indisunique,
    access_method.amname,
    index_row.indnkeyatts,
    index_row.indnatts,
    ARRAY(
      SELECT pg_catalog.pg_get_indexdef(
               index_class.oid,
               key_position.position,
               true
             )
      FROM pg_catalog.generate_series(
             1,
             index_row.indnkeyatts
           ) AS key_position(position)
      ORDER BY key_position.position
    ),
    pg_catalog.pg_get_expr(
      index_row.indpred,
      index_row.indrelid,
      true
    )
  INTO
    v_index_oid,
    v_index_definition,
    v_index_table_schema,
    v_index_table_name,
    v_index_is_unique,
    v_index_access_method,
    v_index_key_count,
    v_index_attribute_count,
    v_index_key_columns,
    v_index_predicate
  FROM pg_catalog.pg_class AS index_class
  JOIN pg_catalog.pg_namespace AS index_schema
    ON index_schema.oid = index_class.relnamespace
  JOIN pg_catalog.pg_index AS index_row
    ON index_row.indexrelid = index_class.oid
  JOIN pg_catalog.pg_class AS table_class
    ON table_class.oid = index_row.indrelid
  JOIN pg_catalog.pg_namespace AS table_schema
    ON table_schema.oid = table_class.relnamespace
  JOIN pg_catalog.pg_am AS access_method
    ON access_method.oid = index_class.relam
  WHERE index_schema.nspname = 'public'
    AND index_class.relname = 'uq_pay_advances_underpayment_case';

  IF v_index_oid IS NOT NULL THEN
    v_actual_predicate_canonical := pg_catalog.regexp_replace(
      pg_catalog.lower(
        pg_catalog.replace(
          COALESCE(v_index_predicate, ''),
          'public.',
          ''
        )
      ),
      '[[:space:]()]+',
      '',
      'g'
    );

    IF v_index_table_schema = 'public'
       AND v_index_table_name = 'pay_advances'
       AND v_index_is_unique IS TRUE
       AND v_index_access_method = 'btree'
       AND v_index_key_count = 3
       AND v_index_attribute_count = 3
       AND v_index_key_columns = ARRAY[
         'candidate_id',
         'linked_timesheet_id',
         'baseline_signature'
       ]::text[]
       AND v_actual_predicate_canonical = v_expected_predicate_canonical
    THEN
      RAISE NOTICE
        'Index public.uq_pay_advances_underpayment_case already exists with the required definition; no action taken.';
      RETURN;
    END IF;

    RAISE EXCEPTION USING
      ERRCODE = '42P07',
      MESSAGE = 'Index public.uq_pay_advances_underpayment_case already exists with a conflicting definition.',
      DETAIL = COALESCE(v_index_definition, '<definition unavailable>'),
      HINT = 'Inspect the existing index. Do not drop or replace it automatically until the conflict has been reviewed.';
  END IF;

  IF pg_catalog.to_regclass('public.pay_advances') IS NULL THEN
    RAISE EXCEPTION USING
      ERRCODE = '42P01',
      MESSAGE = 'Required table public.pay_advances does not exist.';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_attribute AS attribute_row
    WHERE attribute_row.attrelid = 'public.pay_advances'::pg_catalog.regclass
      AND attribute_row.attname = ANY (
        ARRAY[
          'candidate_id',
          'linked_timesheet_id',
          'baseline_signature',
          'advance_kind',
          'status'
        ]::text[]
      )
      AND attribute_row.attnum > 0
      AND attribute_row.attisdropped IS FALSE
    GROUP BY attribute_row.attrelid
    HAVING pg_catalog.count(*) = 5
  ) THEN
    RAISE EXCEPTION USING
      ERRCODE = '42703',
      MESSAGE = 'public.pay_advances is missing one or more columns required by uq_pay_advances_underpayment_case.';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_type AS type_row
    JOIN pg_catalog.pg_namespace AS type_schema
      ON type_schema.oid = type_row.typnamespace
    JOIN pg_catalog.pg_enum AS enum_row
      ON enum_row.enumtypid = type_row.oid
    WHERE type_schema.nspname = 'public'
      AND type_row.typname = 'pay_advance_kind_enum'
      AND enum_row.enumlabel = 'UNDERPAYMENT'
  ) THEN
    RAISE EXCEPTION USING
      ERRCODE = '22P02',
      MESSAGE = 'Enum value public.pay_advance_kind_enum.UNDERPAYMENT is unavailable.';
  END IF;

  IF (
    SELECT pg_catalog.count(*)
    FROM pg_catalog.pg_type AS type_row
    JOIN pg_catalog.pg_namespace AS type_schema
      ON type_schema.oid = type_row.typnamespace
    JOIN pg_catalog.pg_enum AS enum_row
      ON enum_row.enumtypid = type_row.oid
    WHERE type_schema.nspname = 'public'
      AND type_row.typname = 'pay_advance_status_enum'
      AND enum_row.enumlabel = ANY (ARRAY['ACTIVE', 'PAID_OFF']::text[])
  ) <> 2 THEN
    RAISE EXCEPTION USING
      ERRCODE = '22P02',
      MESSAGE = 'Required pay_advance_status_enum values ACTIVE and PAID_OFF are unavailable.';
  END IF;

  -- Prevent writes between the duplicate preflight and index creation.
  LOCK TABLE public.pay_advances IN SHARE MODE;

  SELECT
    duplicate_rows.candidate_id,
    duplicate_rows.linked_timesheet_id,
    duplicate_rows.baseline_signature,
    duplicate_rows.duplicate_count,
    duplicate_rows.row_ids
  INTO v_duplicate
  FROM (
    SELECT
      advance_row.candidate_id,
      advance_row.linked_timesheet_id,
      advance_row.baseline_signature,
      pg_catalog.count(*) AS duplicate_count,
      pg_catalog.array_agg(
        advance_row.id
        ORDER BY advance_row.id
      ) AS row_ids
    FROM public.pay_advances AS advance_row
    WHERE advance_row.advance_kind =
            'UNDERPAYMENT'::public.pay_advance_kind_enum
      AND advance_row.status = ANY (
        ARRAY[
          'ACTIVE'::public.pay_advance_status_enum,
          'PAID_OFF'::public.pay_advance_status_enum
        ]
      )
      -- PostgreSQL unique indexes treat NULL key values as distinct by default.
      -- Match the exact enforcement semantics of the existing Overpayment index.
      AND advance_row.linked_timesheet_id IS NOT NULL
      AND advance_row.baseline_signature IS NOT NULL
    GROUP BY
      advance_row.candidate_id,
      advance_row.linked_timesheet_id,
      advance_row.baseline_signature
    HAVING pg_catalog.count(*) > 1
    ORDER BY
      pg_catalog.count(*) DESC,
      advance_row.candidate_id,
      advance_row.linked_timesheet_id,
      advance_row.baseline_signature
    LIMIT 1
  ) AS duplicate_rows;

  IF v_duplicate.duplicate_count IS NOT NULL THEN
    RAISE EXCEPTION USING
      ERRCODE = '23505',
      MESSAGE = 'Cannot create public.uq_pay_advances_underpayment_case because duplicate active/history-preserving UNDERPAYMENT cases already exist.',
      DETAIL = pg_catalog.format(
        'candidate_id=%s, linked_timesheet_id=%s, baseline_signature=%s, duplicate_count=%s, row_ids=%s',
        v_duplicate.candidate_id,
        v_duplicate.linked_timesheet_id,
        v_duplicate.baseline_signature,
        v_duplicate.duplicate_count,
        v_duplicate.row_ids
      ),
      HINT = 'Review and resolve the duplicate economic authority before rerunning this migration. This migration intentionally performs no automatic data consolidation.';
  END IF;

  EXECUTE $create_index$
    CREATE UNIQUE INDEX uq_pay_advances_underpayment_case
      ON public.pay_advances USING btree
      (candidate_id, linked_timesheet_id, baseline_signature)
      WHERE (
        advance_kind = 'UNDERPAYMENT'::public.pay_advance_kind_enum
        AND status = ANY (
          ARRAY[
            'ACTIVE'::public.pay_advance_status_enum,
            'PAID_OFF'::public.pay_advance_status_enum
          ]
        )
      )
  $create_index$;

  RAISE NOTICE
    'Created public.uq_pay_advances_underpayment_case.';
END;
$migration$;

COMMIT;
