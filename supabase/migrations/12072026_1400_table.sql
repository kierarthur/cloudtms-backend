CREATE TABLE IF NOT EXISTS public.timesheet_r2_cleanup_queue (
  delete_operation_id text NOT NULL,
  r2_key text NOT NULL,
  requested_timesheet_id uuid NULL,
  deleted_timesheet_ids uuid[] NOT NULL DEFAULT ARRAY[]::uuid[],
  status text NOT NULL DEFAULT 'PENDING',
  attempt_count integer NOT NULL DEFAULT 1,
  last_error text NULL,
  claim_token uuid NULL,
  claimed_at_utc timestamptz NULL,
  next_attempt_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),
  first_failed_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),
  last_attempt_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),
  completed_at_utc timestamptz NULL,
  CONSTRAINT timesheet_r2_cleanup_queue_pk PRIMARY KEY (delete_operation_id, r2_key),
  CONSTRAINT timesheet_r2_cleanup_queue_key_nonblank CHECK (btrim(r2_key) <> ''),
  CONSTRAINT timesheet_r2_cleanup_queue_status_ck CHECK (status IN ('PENDING', 'IN_PROGRESS', 'COMPLETE')),
  CONSTRAINT timesheet_r2_cleanup_queue_attempt_ck CHECK (attempt_count >= 1)
);

-- Idempotent upgrade path for an interrupted or earlier partial installation.
ALTER TABLE public.timesheet_r2_cleanup_queue
  ADD COLUMN IF NOT EXISTS claim_token uuid NULL,
  ADD COLUMN IF NOT EXISTS claimed_at_utc timestamptz NULL,
  ADD COLUMN IF NOT EXISTS next_attempt_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),
  ADD COLUMN IF NOT EXISTS completed_at_utc timestamptz NULL;

DO $do$
DECLARE
  v_constraint record;
BEGIN
  SELECT c.oid, pg_catalog.pg_get_constraintdef(c.oid) AS definition
    INTO v_constraint
  FROM pg_catalog.pg_constraint AS c
  WHERE c.conrelid = 'public.timesheet_r2_cleanup_queue'::regclass
    AND c.conname = 'timesheet_r2_cleanup_queue_status_ck';

  IF v_constraint.oid IS NULL
     OR v_constraint.definition NOT LIKE '%IN_PROGRESS%'
     OR v_constraint.definition NOT LIKE '%COMPLETE%' THEN
    IF v_constraint.oid IS NOT NULL THEN
      ALTER TABLE public.timesheet_r2_cleanup_queue
        DROP CONSTRAINT timesheet_r2_cleanup_queue_status_ck;
    END IF;
    ALTER TABLE public.timesheet_r2_cleanup_queue
      ADD CONSTRAINT timesheet_r2_cleanup_queue_status_ck
      CHECK (status IN ('PENDING', 'IN_PROGRESS', 'COMPLETE'));
  END IF;

  SELECT c.oid, pg_catalog.pg_get_constraintdef(c.oid) AS definition
    INTO v_constraint
  FROM pg_catalog.pg_constraint AS c
  WHERE c.conrelid = 'public.timesheet_r2_cleanup_queue'::regclass
    AND c.conname = 'timesheet_r2_cleanup_queue_attempt_ck';

  IF v_constraint.oid IS NULL OR v_constraint.definition NOT LIKE '%>= 1%' THEN
    IF v_constraint.oid IS NOT NULL THEN
      ALTER TABLE public.timesheet_r2_cleanup_queue
        DROP CONSTRAINT timesheet_r2_cleanup_queue_attempt_ck;
    END IF;
    UPDATE public.timesheet_r2_cleanup_queue
       SET attempt_count = 1
     WHERE attempt_count < 1;
    ALTER TABLE public.timesheet_r2_cleanup_queue
      ADD CONSTRAINT timesheet_r2_cleanup_queue_attempt_ck
      CHECK (attempt_count >= 1);
  END IF;
END
$do$;

DROP INDEX IF EXISTS public.timesheet_r2_cleanup_queue_pending_idx;
CREATE INDEX timesheet_r2_cleanup_queue_pending_idx
  ON public.timesheet_r2_cleanup_queue (
    next_attempt_at_utc,
    last_attempt_at_utc,
    delete_operation_id,
    r2_key
  )
  WHERE status IN ('PENDING', 'IN_PROGRESS');

ALTER TABLE public.timesheet_r2_cleanup_queue ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE public.timesheet_r2_cleanup_queue FROM PUBLIC;
DO $do$
DECLARE
  v_role text;
BEGIN
  FOREACH v_role IN ARRAY ARRAY['anon', 'authenticated', 'service_role']::text[] LOOP
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = v_role) THEN
      EXECUTE pg_catalog.format(
        'REVOKE ALL ON TABLE public.timesheet_r2_cleanup_queue FROM %I',
        v_role
      );
    END IF;
  END LOOP;
END
$do$;
