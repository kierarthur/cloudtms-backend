-- CloudTMS Banking Pay full-cutover DB corrections
-- Purpose:
--   Full migration to row-backed Banking Pay workbench/progress/provider attempt foundations.
--   This is intentionally stricter than the compatibility migration: it removes the legacy
--   banking_pay_workbench_sessions.scope_candidate_ids column entirely and closes any existing
--   OPEN workbench sessions so new RPCs must use row-backed session scope.
--
-- Safe to rerun.
-- Expected deployment order:
--   Run during the planned cutover/downtime and deploy the matching RPC/backend/frontend changes
--   in the same release. Old RPCs that still reference scope_candidate_ids will fail after this
--   migration until replaced.
--
-- Verified against the supplied ZIP schema export:
--   - public.banking_pay_workbench_sessions exists and currently has scope_candidate_ids.
--   - public.banking_pay_workbench_session_scope / preview_rows / candidate_line_work exist.
--   - public.banking_pay_operation_provider_attempts exists with provider_* column names.

BEGIN;

SET LOCAL lock_timeout TO '30s';
SET LOCAL statement_timeout TO '10min';

-- -----------------------------------------------------------------------------
-- 0. Required table guardrails
-- -----------------------------------------------------------------------------
DO $$
DECLARE
  v_missing text[] := ARRAY[]::text[];
BEGIN
  IF to_regclass('public.banking_pay_workbench_sessions') IS NULL THEN
    v_missing := array_append(v_missing, 'public.banking_pay_workbench_sessions');
  END IF;
  IF to_regclass('public.banking_pay_workbench_session_scope') IS NULL THEN
    v_missing := array_append(v_missing, 'public.banking_pay_workbench_session_scope');
  END IF;
  IF to_regclass('public.banking_pay_workbench_preview_rows') IS NULL THEN
    v_missing := array_append(v_missing, 'public.banking_pay_workbench_preview_rows');
  END IF;
  IF to_regclass('public.banking_pay_workbench_candidate_line_work') IS NULL THEN
    v_missing := array_append(v_missing, 'public.banking_pay_workbench_candidate_line_work');
  END IF;
  IF to_regclass('public.banking_pay_operation_provider_attempts') IS NULL THEN
    v_missing := array_append(v_missing, 'public.banking_pay_operation_provider_attempts');
  END IF;
  IF to_regclass('public.banking_pay_operations') IS NULL THEN
    v_missing := array_append(v_missing, 'public.banking_pay_operations');
  END IF;
  IF to_regclass('public.banking_pay_operation_transfer_scope') IS NULL THEN
    v_missing := array_append(v_missing, 'public.banking_pay_operation_transfer_scope');
  END IF;
  IF to_regclass('public.banking_pay_operation_chunks') IS NULL THEN
    v_missing := array_append(v_missing, 'public.banking_pay_operation_chunks');
  END IF;

  IF array_length(v_missing, 1) IS NOT NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_FULL_CUTOVER_DB_CORRECTIONS_MISSING_TABLES: %', array_to_string(v_missing, ', ')
      USING ERRCODE = 'P0001';
  END IF;
END;
$$;

-- -----------------------------------------------------------------------------
-- 1. Durable cached workbench progress/counter fields
-- -----------------------------------------------------------------------------
ALTER TABLE public.banking_pay_workbench_sessions
  ADD COLUMN IF NOT EXISTS scope_next_cursor_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS scope_seed_complete boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS scope_total_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS scope_seeded_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS scope_ready_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS scope_pending_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS scope_failed_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS line_units_total integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS line_units_ready integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS line_units_pending integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS line_units_failed integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS preview_row_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS selected_row_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS section_counts_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS candidate_sample_rows_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS progress_state text NOT NULL DEFAULT 'PENDING',
  ADD COLUMN IF NOT EXISTS progress_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS progress_counter_version bigint NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS progress_updated_at_utc timestamptz NOT NULL DEFAULT now();

DO $$
DECLARE
  v_col text;
  v_default_sql text;
BEGIN
  FOR v_col, v_default_sql IN
    SELECT * FROM (VALUES
      ('scope_next_cursor_json', '''{}''::jsonb'),
      ('scope_seed_complete', 'false'),
      ('scope_total_count', '0'),
      ('scope_seeded_count', '0'),
      ('scope_ready_count', '0'),
      ('scope_pending_count', '0'),
      ('scope_failed_count', '0'),
      ('line_units_total', '0'),
      ('line_units_ready', '0'),
      ('line_units_pending', '0'),
      ('line_units_failed', '0'),
      ('preview_row_count', '0'),
      ('selected_row_count', '0'),
      ('section_counts_json', '''{}''::jsonb'),
      ('candidate_sample_rows_json', '''[]''::jsonb'),
      ('progress_state', '''PENDING''::text'),
      ('progress_json', '''{}''::jsonb'),
      ('progress_counter_version', '0'),
      ('progress_updated_at_utc', 'now()')
    ) AS defaults(column_name, default_sql)
  LOOP
    EXECUTE format(
      'UPDATE public.banking_pay_workbench_sessions SET %I = %s WHERE %I IS NULL',
      v_col,
      v_default_sql,
      v_col
    );

    EXECUTE format(
      'ALTER TABLE public.banking_pay_workbench_sessions ALTER COLUMN %I SET DEFAULT %s',
      v_col,
      v_default_sql
    );

    EXECUTE format(
      'ALTER TABLE public.banking_pay_workbench_sessions ALTER COLUMN %I SET NOT NULL',
      v_col
    );
  END LOOP;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.banking_pay_workbench_sessions'::regclass
      AND conname = 'bpay_workbench_sessions_progress_counts_nonnegative_chk'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_sessions
      ADD CONSTRAINT bpay_workbench_sessions_progress_counts_nonnegative_chk
      CHECK (
        scope_total_count >= 0
        AND scope_seeded_count >= 0
        AND scope_ready_count >= 0
        AND scope_pending_count >= 0
        AND scope_failed_count >= 0
        AND line_units_total >= 0
        AND line_units_ready >= 0
        AND line_units_pending >= 0
        AND line_units_failed >= 0
        AND preview_row_count >= 0
        AND selected_row_count >= 0
        AND progress_counter_version >= 0
      );
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.banking_pay_workbench_sessions'::regclass
      AND conname = 'bpay_workbench_sessions_progress_json_shape_chk'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_sessions
      ADD CONSTRAINT bpay_workbench_sessions_progress_json_shape_chk
      CHECK (
        jsonb_typeof(scope_next_cursor_json) = 'object'
        AND jsonb_typeof(section_counts_json) = 'object'
        AND jsonb_typeof(candidate_sample_rows_json) = 'array'
        AND jsonb_typeof(progress_json) = 'object'
      );
  END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS idx_bpay_workbench_sessions_actor_status_updated
ON public.banking_pay_workbench_sessions USING btree (actor_user_id, status, updated_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_bpay_workbench_sessions_progress_state
ON public.banking_pay_workbench_sessions USING btree (status, progress_state, updated_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_bpay_workbench_sessions_scope_seed
ON public.banking_pay_workbench_sessions USING btree (status, scope_seed_complete, updated_at_utc DESC);

COMMENT ON COLUMN public.banking_pay_workbench_sessions.scope_next_cursor_json IS
  'Durable keyset cursor/progress marker for row-backed candidate scope seeding. New Banking Pay workbench code must use public.banking_pay_workbench_session_scope, not legacy candidate arrays.';
COMMENT ON COLUMN public.banking_pay_workbench_sessions.scope_seed_complete IS
  'True only when the row-backed session scope has been completely seeded.';
COMMENT ON COLUMN public.banking_pay_workbench_sessions.scope_total_count IS
  'Cached total row-backed scope count when known. Maintained by seed/chunk functions; progress RPCs must not COUNT(*) full scope on each poll.';
COMMENT ON COLUMN public.banking_pay_workbench_sessions.scope_seeded_count IS
  'Cached number of candidate scope rows seeded into public.banking_pay_workbench_session_scope.';
COMMENT ON COLUMN public.banking_pay_workbench_sessions.scope_ready_count IS
  'Cached ready candidate scope count. Maintained by bounded workbench job/chunk functions.';
COMMENT ON COLUMN public.banking_pay_workbench_sessions.scope_pending_count IS
  'Cached pending candidate scope count. Maintained by bounded workbench job/chunk functions.';
COMMENT ON COLUMN public.banking_pay_workbench_sessions.scope_failed_count IS
  'Cached failed candidate scope count. Maintained by bounded workbench job/chunk functions.';
COMMENT ON COLUMN public.banking_pay_workbench_sessions.line_units_total IS
  'Cached total line-work unit count. Progress RPCs must not count all line-work rows on each poll.';
COMMENT ON COLUMN public.banking_pay_workbench_sessions.line_units_ready IS
  'Cached ready line-work unit count. Maintained incrementally by line-work processing/materialisation.';
COMMENT ON COLUMN public.banking_pay_workbench_sessions.line_units_pending IS
  'Cached pending line-work unit count. Maintained incrementally by line-work processing/materialisation.';
COMMENT ON COLUMN public.banking_pay_workbench_sessions.line_units_failed IS
  'Cached failed line-work unit count. Maintained incrementally by line-work processing/materialisation.';
COMMENT ON COLUMN public.banking_pay_workbench_sessions.preview_row_count IS
  'Cached total materialised preview-row count. Preview/progress RPCs must not COUNT(*) preview rows on each poll.';
COMMENT ON COLUMN public.banking_pay_workbench_sessions.selected_row_count IS
  'Cached selected preview-row count. Selection updates must maintain this incrementally.';
COMMENT ON COLUMN public.banking_pay_workbench_sessions.section_counts_json IS
  'Cached per-section preview-row counts used by preview/page metadata.';
COMMENT ON COLUMN public.banking_pay_workbench_sessions.candidate_sample_rows_json IS
  'Tiny capped candidate sample for progress UI only. Never store or return full candidate scope here.';
COMMENT ON COLUMN public.banking_pay_workbench_sessions.progress_state IS
  'Compact cached workbench progress state for progress-only RPCs.';
COMMENT ON COLUMN public.banking_pay_workbench_sessions.progress_json IS
  'Compact cached workbench progress metadata. Do not store full candidate/status/preview arrays.';
COMMENT ON COLUMN public.banking_pay_workbench_sessions.progress_counter_version IS
  'Monotonic cached-progress version for workbench UI polling and change detection.';
COMMENT ON COLUMN public.banking_pay_workbench_sessions.progress_updated_at_utc IS
  'Last time cached progress counters were updated.';

-- -----------------------------------------------------------------------------
-- 2. Full cutover from legacy workbench session arrays to row-backed session scope
-- -----------------------------------------------------------------------------
-- Close any existing OPEN runtime sessions before replacing the uniqueness model and
-- dropping legacy array scope. This avoids carrying partially seeded legacy sessions
-- into the new row-backed implementation.
UPDATE public.banking_pay_workbench_sessions
SET
  status = 'DISCARDED',
  discarded_at_utc = COALESCE(discarded_at_utc, now()),
  progress_state = 'CUTOVER_DISCARDED',
  progress_json = jsonb_strip_nulls(
    COALESCE(progress_json, '{}'::jsonb)
    || jsonb_build_object(
      'cutover_discarded_at_utc', now()::text,
      'cutover_reason', 'BANKING_PAY_ROW_BACKED_SCOPE_FULL_CUTOVER'
    )
  ),
  progress_counter_version = progress_counter_version + 1,
  progress_updated_at_utc = now(),
  updated_at_utc = now()
WHERE status = 'OPEN';

-- Remove old global one-open-session restrictions and old global signature uniqueness.
DROP INDEX IF EXISTS public.ux_bpay_workbench_sessions_single_open;
DROP INDEX IF EXISTS public.ux_bpay_workbench_sessions_signature_open;

-- Remove the legacy array lookup index and then remove the array column completely.
DROP INDEX IF EXISTS public.idx_bpay_workbench_sessions_scope_candidates_gin;
ALTER TABLE public.banking_pay_workbench_sessions DROP COLUMN IF EXISTS scope_candidate_ids;

-- Keep OPEN-session uniqueness scoped to the actor and filter/session signature only.
CREATE UNIQUE INDEX IF NOT EXISTS ux_bpay_workbench_sessions_actor_signature_open
ON public.banking_pay_workbench_sessions USING btree (actor_user_id, session_signature)
WHERE status = 'OPEN';

COMMENT ON INDEX public.ux_bpay_workbench_sessions_actor_signature_open IS
  'Full cutover OPEN-session uniqueness: one OPEN session per actor_user_id + session_signature. Replaces legacy global single-open and global signature-open indexes.';

COMMENT ON TABLE public.banking_pay_workbench_sessions IS
  'Banking Pay workbench sessions. Full cutover: candidate scope is row-backed in public.banking_pay_workbench_session_scope; legacy scope_candidate_ids has been removed.';

-- One-off counter backfill from row-backed runtime tables. Ongoing correctness must be
-- maintained incrementally by the new bounded seed/process/materialise RPCs.
WITH
scope_counts AS (
  SELECT
    session_id,
    count(*)::integer AS scope_total_count,
    count(*)::integer AS scope_seeded_count,
    count(*) FILTER (WHERE upper(coalesce(status, '')) IN ('READY', 'COMPLETE', 'COMPLETED'))::integer AS scope_ready_count,
    count(*) FILTER (WHERE upper(coalesce(status, '')) IN ('FAILED', 'ERROR'))::integer AS scope_failed_count,
    count(*) FILTER (
      WHERE upper(coalesce(status, '')) NOT IN ('READY', 'COMPLETE', 'COMPLETED', 'FAILED', 'ERROR')
    )::integer AS scope_pending_count
  FROM public.banking_pay_workbench_session_scope
  GROUP BY session_id
),
line_counts AS (
  SELECT
    session_id,
    count(*)::integer AS line_units_total,
    count(*) FILTER (WHERE upper(coalesce(status, '')) IN ('READY', 'MATERIALISED', 'MATERIALIZED', 'COMPLETE', 'COMPLETED'))::integer AS line_units_ready,
    count(*) FILTER (WHERE upper(coalesce(status, '')) IN ('FAILED', 'ERROR'))::integer AS line_units_failed,
    count(*) FILTER (
      WHERE upper(coalesce(status, '')) NOT IN ('READY', 'MATERIALISED', 'MATERIALIZED', 'COMPLETE', 'COMPLETED', 'FAILED', 'ERROR')
    )::integer AS line_units_pending
  FROM public.banking_pay_workbench_candidate_line_work
  GROUP BY session_id
),
preview_counts AS (
  SELECT
    session_id,
    count(*)::integer AS preview_row_count,
    count(*) FILTER (
      WHERE coalesce(selected, false) = true
         OR upper(coalesce(selection_state, '')) = 'SELECTED'
    )::integer AS selected_row_count
  FROM public.banking_pay_workbench_preview_rows
  GROUP BY session_id
),
section_counts AS (
  SELECT
    section_count_rows.session_id,
    jsonb_object_agg(section_count_rows.section, section_count_rows.row_count ORDER BY section_count_rows.section) AS section_counts_json
  FROM (
    SELECT
      session_id,
      section,
      count(*)::integer AS row_count
    FROM public.banking_pay_workbench_preview_rows
    GROUP BY session_id, section
  ) AS section_count_rows
  GROUP BY section_count_rows.session_id
),
sample_scope_rows AS (
  SELECT
    scoped_rows.session_id,
    jsonb_agg(
      jsonb_strip_nulls(jsonb_build_object(
        'candidate_id', scoped_rows.candidate_id::text,
        'status', scoped_rows.status,
        'scope_ordinal', scoped_rows.scope_ordinal,
        'seeded', scoped_rows.seeded,
        'dirty', scoped_rows.dirty
      ))
      ORDER BY scoped_rows.scope_ordinal, scoped_rows.candidate_id
    ) AS candidate_sample_rows_json
  FROM (
    SELECT
      scope_row.*,
      row_number() OVER (PARTITION BY scope_row.session_id ORDER BY scope_row.scope_ordinal, scope_row.candidate_id) AS sample_rn
    FROM public.banking_pay_workbench_session_scope AS scope_row
  ) AS scoped_rows
  WHERE scoped_rows.sample_rn <= 25
  GROUP BY scoped_rows.session_id
),
all_session_ids AS (
  SELECT id AS session_id
  FROM public.banking_pay_workbench_sessions
)
UPDATE public.banking_pay_workbench_sessions AS session_update
SET
  scope_total_count = coalesce(scope_counts.scope_total_count, 0),
  scope_seeded_count = coalesce(scope_counts.scope_seeded_count, 0),
  scope_ready_count = coalesce(scope_counts.scope_ready_count, 0),
  scope_pending_count = coalesce(scope_counts.scope_pending_count, 0),
  scope_failed_count = coalesce(scope_counts.scope_failed_count, 0),
  line_units_total = coalesce(line_counts.line_units_total, 0),
  line_units_ready = coalesce(line_counts.line_units_ready, 0),
  line_units_pending = coalesce(line_counts.line_units_pending, 0),
  line_units_failed = coalesce(line_counts.line_units_failed, 0),
  preview_row_count = coalesce(preview_counts.preview_row_count, 0),
  selected_row_count = coalesce(preview_counts.selected_row_count, 0),
  section_counts_json = coalesce(section_counts.section_counts_json, '{}'::jsonb),
  candidate_sample_rows_json = coalesce(sample_scope_rows.candidate_sample_rows_json, '[]'::jsonb),
  progress_state = CASE
    WHEN session_update.status = 'DISCARDED' THEN 'CUTOVER_DISCARDED'
    WHEN coalesce(scope_counts.scope_failed_count, 0) > 0 OR coalesce(line_counts.line_units_failed, 0) > 0 THEN 'PARTIAL_WITH_ERRORS'
    WHEN coalesce(session_update.scope_seed_complete, false) = true
      AND coalesce(scope_counts.scope_pending_count, 0) = 0
      AND coalesce(line_counts.line_units_pending, 0) = 0 THEN 'READY'
    WHEN coalesce(scope_counts.scope_seeded_count, 0) > 0
      OR coalesce(line_counts.line_units_total, 0) > 0
      OR coalesce(preview_counts.preview_row_count, 0) > 0 THEN 'RUNNING'
    ELSE 'PENDING'
  END,
  progress_json = jsonb_strip_nulls(
    coalesce(session_update.progress_json, '{}'::jsonb)
    || jsonb_build_object(
      'schema_backfilled_at_utc', now()::text,
      'schema_backfill_reason', 'BANKING_PAY_ROW_BACKED_PROGRESS_COUNTERS_FULL_CUTOVER',
      'counter_source', 'row_backed_tables'
    )
  ),
  progress_counter_version = session_update.progress_counter_version + 1,
  progress_updated_at_utc = now(),
  updated_at_utc = now()
FROM all_session_ids
LEFT JOIN scope_counts ON scope_counts.session_id = all_session_ids.session_id
LEFT JOIN line_counts ON line_counts.session_id = all_session_ids.session_id
LEFT JOIN preview_counts ON preview_counts.session_id = all_session_ids.session_id
LEFT JOIN section_counts ON section_counts.session_id = all_session_ids.session_id
LEFT JOIN sample_scope_rows ON sample_scope_rows.session_id = all_session_ids.session_id
WHERE session_update.id = all_session_ids.session_id;

-- -----------------------------------------------------------------------------
-- 3. Provider attempt table hardening and legacy-column cleanup
-- -----------------------------------------------------------------------------
ALTER TABLE public.banking_pay_operation_provider_attempts
  ADD COLUMN IF NOT EXISTS provider_idempotency_key text,
  ADD COLUMN IF NOT EXISTS provider_request_id text,
  ADD COLUMN IF NOT EXISTS provider_transaction_id text,
  ADD COLUMN IF NOT EXISTS compact_error_summary_json jsonb,
  ADD COLUMN IF NOT EXISTS created_at_utc timestamptz NOT NULL DEFAULT now();

-- If any earlier bad migration added legacy columns, migrate their values into the real columns first.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'banking_pay_operation_provider_attempts' AND column_name = 'idempotency_key'
  ) THEN
    EXECUTE 'UPDATE public.banking_pay_operation_provider_attempts SET provider_idempotency_key = COALESCE(provider_idempotency_key, idempotency_key) WHERE provider_idempotency_key IS NULL AND idempotency_key IS NOT NULL';
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'banking_pay_operation_provider_attempts' AND column_name = 'request_id'
  ) THEN
    EXECUTE 'UPDATE public.banking_pay_operation_provider_attempts SET provider_request_id = COALESCE(provider_request_id, request_id) WHERE provider_request_id IS NULL AND request_id IS NOT NULL';
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'banking_pay_operation_provider_attempts' AND column_name = 'attempted_at_utc'
  ) THEN
    EXECUTE 'UPDATE public.banking_pay_operation_provider_attempts SET created_at_utc = COALESCE(created_at_utc, attempted_at_utc, now()) WHERE created_at_utc IS NULL';
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'banking_pay_operation_provider_attempts' AND column_name = 'compact_error_summary'
  ) THEN
    EXECUTE 'UPDATE public.banking_pay_operation_provider_attempts SET compact_error_summary_json = COALESCE(compact_error_summary_json, compact_error_summary) WHERE compact_error_summary_json IS NULL AND compact_error_summary IS NOT NULL';
  END IF;
END;
$$;

ALTER TABLE public.banking_pay_operation_provider_attempts
  ALTER COLUMN created_at_utc SET DEFAULT now(),
  ALTER COLUMN created_at_utc SET NOT NULL;

-- Complete cleanup: remove legacy bad-column names if they exist.
ALTER TABLE public.banking_pay_operation_provider_attempts DROP COLUMN IF EXISTS idempotency_key;
ALTER TABLE public.banking_pay_operation_provider_attempts DROP COLUMN IF EXISTS request_id;
ALTER TABLE public.banking_pay_operation_provider_attempts DROP COLUMN IF EXISTS attempted_at_utc;
ALTER TABLE public.banking_pay_operation_provider_attempts DROP COLUMN IF EXISTS compact_error_summary;

CREATE INDEX IF NOT EXISTS idx_bpay_provider_attempts_idempotency
ON public.banking_pay_operation_provider_attempts USING btree (provider_idempotency_key)
WHERE provider_idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_bpay_provider_attempts_request
ON public.banking_pay_operation_provider_attempts USING btree (provider_request_id)
WHERE provider_request_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_bpay_provider_attempts_transaction
ON public.banking_pay_operation_provider_attempts USING btree (provider_transaction_id)
WHERE provider_transaction_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_bpay_provider_attempts_operation_created
ON public.banking_pay_operation_provider_attempts USING btree (operation_id, created_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_bpay_provider_attempts_transfer_created
ON public.banking_pay_operation_provider_attempts USING btree (transfer_scope_id, created_at_utc DESC);

COMMENT ON TABLE public.banking_pay_operation_provider_attempts IS
  'Append-only compact provider attempt audit. Full cutover: provider_idempotency_key/provider_request_id/compact_error_summary_json are the canonical columns; legacy idempotency_key/request_id/attempted_at_utc/compact_error_summary columns are removed.';

-- -----------------------------------------------------------------------------
-- 4. Correct provider-attempt recorder to use the actual provider_attempts columns
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.banking_pay_operation_provider_attempt_record(
  p_operation_id uuid,
  p_pay_batch_id uuid DEFAULT NULL::uuid,
  p_transfer_scope_id uuid DEFAULT NULL::uuid,
  p_provider_chunk_id uuid DEFAULT NULL::uuid,
  p_idempotency_key text DEFAULT NULL::text,
  p_request_id text DEFAULT NULL::text,
  p_provider_transaction_id text DEFAULT NULL::text,
  p_previous_state text DEFAULT NULL::text,
  p_new_state text DEFAULT NULL::text,
  p_lease_owner text DEFAULT NULL::text,
  p_request_json jsonb DEFAULT NULL::jsonb,
  p_response_json jsonb DEFAULT NULL::jsonb,
  p_error_json jsonb DEFAULT NULL::jsonb,
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_operation_row public.banking_pay_operations%ROWTYPE;
  v_effective_pay_batch_id uuid := p_pay_batch_id;
  v_request_hash text := NULL::text;
  v_response_hash text := NULL::text;
  v_error_summary jsonb := NULL::jsonb;
  v_attempt_id uuid := NULL::uuid;
BEGIN
  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_PROVIDER_ATTEMPT_OPERATION_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_PROVIDER_ATTEMPT_OPERATION_ID_REQUIRED')::text;
  END IF;

  IF NULLIF(BTRIM(COALESCE(p_new_state, '')), '') IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_PROVIDER_ATTEMPT_NEW_STATE_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_PROVIDER_ATTEMPT_NEW_STATE_REQUIRED', 'operation_id', p_operation_id::text)::text;
  END IF;

  SELECT operation_row.*
  INTO v_operation_row
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'BANKING_PAY_PROVIDER_ATTEMPT_OPERATION_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_PROVIDER_ATTEMPT_OPERATION_NOT_FOUND', 'operation_id', p_operation_id::text)::text;
  END IF;

  v_effective_pay_batch_id := COALESCE(v_effective_pay_batch_id, v_operation_row.pay_batch_id);

  IF p_transfer_scope_id IS NOT NULL THEN
    PERFORM 1
    FROM public.banking_pay_operation_transfer_scope AS transfer_scope_check
    WHERE transfer_scope_check.id = p_transfer_scope_id
      AND transfer_scope_check.operation_id = p_operation_id
      AND (v_effective_pay_batch_id IS NULL OR transfer_scope_check.pay_batch_id = v_effective_pay_batch_id);

    IF NOT FOUND THEN
      RAISE EXCEPTION 'BANKING_PAY_PROVIDER_ATTEMPT_TRANSFER_SCOPE_NOT_FOUND'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_PROVIDER_ATTEMPT_TRANSFER_SCOPE_NOT_FOUND', 'operation_id', p_operation_id::text, 'transfer_scope_id', p_transfer_scope_id::text)::text;
    END IF;
  END IF;

  IF p_provider_chunk_id IS NOT NULL THEN
    PERFORM 1
    FROM public.banking_pay_operation_chunks AS provider_chunk_check
    WHERE provider_chunk_check.id = p_provider_chunk_id
      AND provider_chunk_check.operation_id = p_operation_id;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'BANKING_PAY_PROVIDER_ATTEMPT_CHUNK_NOT_FOUND'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_PROVIDER_ATTEMPT_CHUNK_NOT_FOUND', 'operation_id', p_operation_id::text, 'provider_chunk_id', p_provider_chunk_id::text)::text;
    END IF;
  END IF;

  IF p_request_json IS NOT NULL AND jsonb_typeof(p_request_json) <> 'object' THEN
    RAISE EXCEPTION 'BANKING_PAY_PROVIDER_ATTEMPT_REQUEST_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_PROVIDER_ATTEMPT_REQUEST_JSON_MUST_BE_OBJECT', 'operation_id', p_operation_id::text)::text;
  END IF;

  IF p_response_json IS NOT NULL AND jsonb_typeof(p_response_json) <> 'object' THEN
    RAISE EXCEPTION 'BANKING_PAY_PROVIDER_ATTEMPT_RESPONSE_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_PROVIDER_ATTEMPT_RESPONSE_JSON_MUST_BE_OBJECT', 'operation_id', p_operation_id::text)::text;
  END IF;

  IF p_error_json IS NOT NULL AND jsonb_typeof(p_error_json) <> 'object' THEN
    RAISE EXCEPTION 'BANKING_PAY_PROVIDER_ATTEMPT_ERROR_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_PROVIDER_ATTEMPT_ERROR_JSON_MUST_BE_OBJECT', 'operation_id', p_operation_id::text)::text;
  END IF;

  v_request_hash := CASE WHEN p_request_json IS NULL THEN NULL ELSE md5(p_request_json::text) END;
  v_response_hash := CASE WHEN p_response_json IS NULL THEN NULL ELSE md5(p_response_json::text) END;
  v_error_summary := CASE
    WHEN p_error_json IS NULL THEN NULL::jsonb
    ELSE jsonb_strip_nulls(jsonb_build_object(
      'code', COALESCE(NULLIF(BTRIM(p_error_json->>'code'), ''), NULLIF(BTRIM(p_error_json->>'error_code'), '')),
      'message', LEFT(COALESCE(NULLIF(BTRIM(p_error_json->>'message'), ''), NULLIF(BTRIM(p_error_json->>'error'), ''), p_error_json::text), 1000),
      'sqlstate', NULLIF(BTRIM(p_error_json->>'sqlstate'), ''),
      'provider_status', NULLIF(BTRIM(p_error_json->>'provider_status'), '')
    ))
  END;

  INSERT INTO public.banking_pay_operation_provider_attempts (
    operation_id,
    pay_batch_id,
    transfer_scope_id,
    provider_chunk_id,
    provider_idempotency_key,
    provider_request_id,
    provider_transaction_id,
    previous_state,
    new_state,
    lease_owner,
    compact_request_hash,
    compact_response_hash,
    compact_error_summary_json,
    created_at_utc
  )
  VALUES (
    p_operation_id,
    v_effective_pay_batch_id,
    p_transfer_scope_id,
    p_provider_chunk_id,
    NULLIF(BTRIM(COALESCE(p_idempotency_key, '')), ''),
    NULLIF(BTRIM(COALESCE(p_request_id, '')), ''),
    NULLIF(BTRIM(COALESCE(p_provider_transaction_id, '')), ''),
    NULLIF(BTRIM(COALESCE(p_previous_state, '')), ''),
    UPPER(BTRIM(COALESCE(p_new_state, ''))),
    NULLIF(BTRIM(COALESCE(p_lease_owner, '')), ''),
    v_request_hash,
    v_response_hash,
    v_error_summary,
    v_now
  )
  RETURNING id INTO v_attempt_id;

  UPDATE public.banking_pay_operations AS operation_update
  SET progress_json = jsonb_strip_nulls(
        COALESCE(operation_update.progress_json, '{}'::jsonb)
        || jsonb_build_object(
          'last_provider_attempt', jsonb_strip_nulls(jsonb_build_object(
            'attempt_id', CASE WHEN v_attempt_id IS NULL THEN NULL ELSE v_attempt_id::text END,
            'recorded_at_utc', v_now::text,
            'transfer_scope_id', CASE WHEN p_transfer_scope_id IS NULL THEN NULL ELSE p_transfer_scope_id::text END,
            'provider_chunk_id', CASE WHEN p_provider_chunk_id IS NULL THEN NULL ELSE p_provider_chunk_id::text END,
            'previous_state', NULLIF(BTRIM(COALESCE(p_previous_state, '')), ''),
            'new_state', UPPER(BTRIM(COALESCE(p_new_state, ''))),
            'provider_transaction_id_present', NULLIF(BTRIM(COALESCE(p_provider_transaction_id, '')), '') IS NOT NULL,
            'request_id_present', NULLIF(BTRIM(COALESCE(p_request_id, '')), '') IS NOT NULL,
            'error_present', p_error_json IS NOT NULL
          ))
        )
      ),
      updated_at_utc = v_now
  WHERE operation_update.id = p_operation_id;

  RETURN jsonb_build_object(
    'ok', true,
    'attempt_id', CASE WHEN v_attempt_id IS NULL THEN NULL ELSE v_attempt_id::text END,
    'operation_id', p_operation_id::text,
    'pay_batch_id', CASE WHEN v_effective_pay_batch_id IS NULL THEN NULL ELSE v_effective_pay_batch_id::text END,
    'transfer_scope_id', CASE WHEN p_transfer_scope_id IS NULL THEN NULL ELSE p_transfer_scope_id::text END,
    'provider_chunk_id', CASE WHEN p_provider_chunk_id IS NULL THEN NULL ELSE p_provider_chunk_id::text END,
    'previous_state', NULLIF(BTRIM(COALESCE(p_previous_state, '')), ''),
    'new_state', UPPER(BTRIM(COALESCE(p_new_state, ''))),
    'attempted_at_utc', v_now::text,
    'created_at_utc', v_now::text,
    'compact_request_hash', v_request_hash,
    'compact_response_hash', v_response_hash,
    'error_present', p_error_json IS NOT NULL
  );
END;
$function$;

COMMENT ON FUNCTION public.banking_pay_operation_provider_attempt_record(
  uuid, uuid, uuid, uuid, text, text, text, text, text, text, jsonb, jsonb, jsonb, uuid
) IS
  'Records compact provider attempt/audit rows using canonical provider_* columns. Full cutover replacement for legacy idempotency_key/request_id/attempted_at_utc/compact_error_summary inserts.';

-- -----------------------------------------------------------------------------
-- 5. Post-migration assertions
-- -----------------------------------------------------------------------------
DO $$
DECLARE
  v_scope_candidate_exists boolean;
  v_global_single_open_exists boolean;
  v_global_signature_open_exists boolean;
  v_actor_signature_open_exists boolean;
BEGIN
  SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'banking_pay_workbench_sessions'
      AND column_name = 'scope_candidate_ids'
  ) INTO v_scope_candidate_exists;

  SELECT to_regclass('public.ux_bpay_workbench_sessions_single_open') IS NOT NULL INTO v_global_single_open_exists;
  SELECT to_regclass('public.ux_bpay_workbench_sessions_signature_open') IS NOT NULL INTO v_global_signature_open_exists;
  SELECT to_regclass('public.ux_bpay_workbench_sessions_actor_signature_open') IS NOT NULL INTO v_actor_signature_open_exists;

  IF v_scope_candidate_exists THEN
    RAISE EXCEPTION 'BANKING_PAY_FULL_CUTOVER_FAILED_SCOPE_CANDIDATE_IDS_STILL_EXISTS'
      USING ERRCODE = 'P0001';
  END IF;

  IF v_global_single_open_exists OR v_global_signature_open_exists THEN
    RAISE EXCEPTION 'BANKING_PAY_FULL_CUTOVER_FAILED_GLOBAL_OPEN_INDEX_STILL_EXISTS'
      USING ERRCODE = 'P0001';
  END IF;

  IF NOT v_actor_signature_open_exists THEN
    RAISE EXCEPTION 'BANKING_PAY_FULL_CUTOVER_FAILED_ACTOR_SIGNATURE_OPEN_INDEX_MISSING'
      USING ERRCODE = 'P0001';
  END IF;
END;
$$;

COMMIT;
