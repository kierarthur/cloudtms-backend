DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n
      ON n.oid = t.typnamespace
    WHERE n.nspname = 'public'
      AND t.typname = 'ts_fin_processing_status_enum'
  ) AND NOT EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n
      ON n.oid = t.typnamespace
    JOIN pg_enum e
      ON e.enumtypid = t.oid
    WHERE n.nspname = 'public'
      AND t.typname = 'ts_fin_processing_status_enum'
      AND e.enumlabel = 'UNPROCESSED'
  ) THEN
    ALTER TYPE public.ts_fin_processing_status_enum ADD VALUE 'UNPROCESSED';
  END IF;
END
$$;

DO $$
DECLARE
  v_constraint_def text;
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n
      ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relname = 'manual_timesheet_queue'
      AND c.relkind = 'r'
  ) THEN
    SELECT pg_get_constraintdef(con.oid, true)
      INTO v_constraint_def
    FROM pg_constraint con
    JOIN pg_class rel
      ON rel.oid = con.conrelid
    JOIN pg_namespace nsp
      ON nsp.oid = rel.relnamespace
    WHERE nsp.nspname = 'public'
      AND rel.relname = 'manual_timesheet_queue'
      AND con.conname = 'manual_timesheet_queue_status_chk';

    IF v_constraint_def IS NULL THEN
      ALTER TABLE public.manual_timesheet_queue
        ADD CONSTRAINT manual_timesheet_queue_status_chk
        CHECK (
          status = ANY (
            ARRAY[
              'QUEUED'::text,
              'ATTACHED'::text,
              'DISCARDED'::text,
              'STAGED'::text
            ]
          )
        ) NOT VALID;

      ALTER TABLE public.manual_timesheet_queue
        VALIDATE CONSTRAINT manual_timesheet_queue_status_chk;

    ELSIF position('STAGED' in upper(v_constraint_def)) = 0 THEN
      ALTER TABLE public.manual_timesheet_queue
        DROP CONSTRAINT manual_timesheet_queue_status_chk;

      ALTER TABLE public.manual_timesheet_queue
        ADD CONSTRAINT manual_timesheet_queue_status_chk
        CHECK (
          status = ANY (
            ARRAY[
              'QUEUED'::text,
              'ATTACHED'::text,
              'DISCARDED'::text,
              'STAGED'::text
            ]
          )
        ) NOT VALID;

      ALTER TABLE public.manual_timesheet_queue
        VALIDATE CONSTRAINT manual_timesheet_queue_status_chk;
    END IF;
  END IF;
END
$$;
