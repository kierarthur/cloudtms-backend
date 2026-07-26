BEGIN;

-- -----------------------------------------------------------------------------
-- CloudTMS invoice Batch Generate / Batch Issue modal query support
-- -----------------------------------------------------------------------------
-- Purpose:
--   Adds only supporting indexes required by the locked high-volume invoice
--   batch modal design:
--     - server-side candidate paging;
--     - filtered generation and issue candidate lists;
--     - exact generated-document readiness lookup;
--     - root-operation result paging.
--
-- Safety / rerun policy:
--   - Safe to rerun: every index uses IF NOT EXISTS.
--   - Does not alter invoice economics, VAT, invoice totals, payment, settlement,
--     delivery authority, generated document identity, or legal issue authority.
--   - Does not mutate invoice/timesheet/operation data.
--   - Does not create any workflow/selection tables.
--   - Uses a short lock timeout so it fails rather than waiting indefinitely.
-- -----------------------------------------------------------------------------

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '5min';

-- -----------------------------------------------------------------------------
-- 1) Batch Generate source paging / filtering support
-- -----------------------------------------------------------------------------

DO $migration$
BEGIN
  IF to_regclass('public.timesheets_financials') IS NULL THEN
    RAISE EXCEPTION 'Required table public.timesheets_financials is missing';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'timesheets_financials'
      AND column_name IN ('client_id', 'candidate_id', 'timesheet_id', 'is_current')
    GROUP BY table_schema, table_name
    HAVING count(*) = 4
  ) THEN
    RAISE EXCEPTION 'public.timesheets_financials is missing one or more required columns: client_id, candidate_id, timesheet_id, is_current';
  END IF;

  EXECUTE $sql$
    CREATE INDEX IF NOT EXISTS idx_tsfin_invoice_batch_candidate_scope
    ON public.timesheets_financials (client_id, candidate_id, timesheet_id)
    WHERE is_current
  $sql$;

  EXECUTE $sql$
    CREATE INDEX IF NOT EXISTS idx_tsfin_invoice_batch_client_scope
    ON public.timesheets_financials (client_id, timesheet_id)
    WHERE is_current
  $sql$;
END
$migration$;

COMMENT ON INDEX public.idx_tsfin_invoice_batch_candidate_scope IS
'Supports Batch Generate candidate filtering and paging by client/candidate/current financial source. Presentation/query support only; no economics authority.';

COMMENT ON INDEX public.idx_tsfin_invoice_batch_client_scope IS
'Supports Batch Generate candidate filtering and paging by client/current financial source. Presentation/query support only; no economics authority.';

DO $migration$
BEGIN
  IF to_regclass('public.timesheets') IS NULL THEN
    RAISE EXCEPTION 'Required table public.timesheets is missing';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'timesheets'
      AND column_name IN ('week_ending_date', 'timesheet_id', 'is_current', 'revoked_at')
    GROUP BY table_schema, table_name
    HAVING count(*) = 4
  ) THEN
    RAISE EXCEPTION 'public.timesheets is missing one or more required columns: week_ending_date, timesheet_id, is_current, revoked_at';
  END IF;

  EXECUTE $sql$
    CREATE INDEX IF NOT EXISTS idx_timesheets_invoice_batch_week_scope
    ON public.timesheets (week_ending_date, timesheet_id)
    WHERE is_current
      AND revoked_at IS NULL
  $sql$;

  -- Candidate filtering is required by the locked modal policy.  Some historical
  -- schemas expose candidate_id directly on timesheets; create the more selective
  -- index only where that column exists.
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'timesheets'
      AND column_name = 'candidate_id'
  ) THEN
    EXECUTE $sql$
      CREATE INDEX IF NOT EXISTS idx_timesheets_invoice_batch_candidate_week_scope
      ON public.timesheets (candidate_id, week_ending_date, timesheet_id)
      WHERE is_current
        AND revoked_at IS NULL
    $sql$;
  END IF;
END
$migration$;

COMMENT ON INDEX public.idx_timesheets_invoice_batch_week_scope IS
'Supports Batch Generate week-ending date/range filtering and keyset paging. Presentation/query support only.';

DO $migration$
BEGIN
  IF to_regclass('public.idx_timesheets_invoice_batch_candidate_week_scope') IS NOT NULL THEN
    COMMENT ON INDEX public.idx_timesheets_invoice_batch_candidate_week_scope IS
    'Supports Batch Generate candidate + week-ending filtering where public.timesheets.candidate_id exists. Presentation/query support only.';
  END IF;
END
$migration$;

-- -----------------------------------------------------------------------------
-- 2) Batch Issue candidate paging / filtering support
-- -----------------------------------------------------------------------------

DO $migration$
BEGIN
  IF to_regclass('public.invoices') IS NULL THEN
    RAISE EXCEPTION 'Required table public.invoices is missing';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'invoices'
      AND column_name IN ('client_id', 'document_state', 'created_at', 'id', 'type', 'status')
    GROUP BY table_schema, table_name
    HAVING count(*) = 6
  ) THEN
    RAISE EXCEPTION 'public.invoices is missing one or more required columns: client_id, document_state, created_at, id, type, status';
  END IF;

  EXECUTE $sql$
    CREATE INDEX IF NOT EXISTS idx_invoices_batch_issue_scope
    ON public.invoices (client_id, document_state, created_at DESC, id)
    WHERE type = 'INVOICE'
      AND status IN ('DRAFT', 'ON_HOLD')
  $sql$;
END
$migration$;

COMMENT ON INDEX public.idx_invoices_batch_issue_scope IS
'Supports Batch Issue candidate filtering/paging for generated, unissued invoice rows. Presentation/query support only.';

-- -----------------------------------------------------------------------------
-- 3) Exact generated-document readiness lookup
-- -----------------------------------------------------------------------------

DO $migration$
BEGIN
  IF to_regclass('public.invoice_document_versions') IS NULL THEN
    RAISE EXCEPTION 'Required table public.invoice_document_versions is missing';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'invoice_document_versions'
      AND column_name IN ('entity_type', 'entity_id', 'purpose', 'source_revision', 'status', 'created_at_utc', 'id')
    GROUP BY table_schema, table_name
    HAVING count(*) = 7
  ) THEN
    RAISE EXCEPTION 'public.invoice_document_versions is missing one or more required columns: entity_type, entity_id, purpose, source_revision, status, created_at_utc, id';
  END IF;

  EXECUTE $sql$
    CREATE INDEX IF NOT EXISTS idx_invoice_document_versions_candidate_readiness
    ON public.invoice_document_versions (
      entity_type,
      entity_id,
      purpose,
      source_revision,
      status,
      created_at_utc DESC,
      id DESC
    )
  $sql$;
END
$migration$;

COMMENT ON INDEX public.idx_invoice_document_versions_candidate_readiness IS
'Supports exact draft/final/timesheet document readiness checks for invoice batch modals and async document viewing.';

-- -----------------------------------------------------------------------------
-- 4) Root-operation result paging support
-- -----------------------------------------------------------------------------

DO $migration$
BEGIN
  IF to_regclass('public.invoice_operation_chunks') IS NULL THEN
    RAISE EXCEPTION 'Required table public.invoice_operation_chunks is missing';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'invoice_operation_chunks'
      AND column_name IN ('operation_id', 'chunk_type', 'payload_json', 'id')
    GROUP BY table_schema, table_name
    HAVING count(*) = 4
  ) THEN
    RAISE EXCEPTION 'public.invoice_operation_chunks is missing one or more required columns: operation_id, chunk_type, payload_json, id';
  END IF;

  EXECUTE $sql$
    CREATE INDEX IF NOT EXISTS idx_invoice_chunks_batch_result_selection_key
    ON public.invoice_operation_chunks (
      operation_id,
      chunk_type,
      (payload_json ->> 'selection_key'),
      id
    )
    WHERE chunk_type IN ('GENERATION_GROUP', 'ISSUE_INVOICE')
  $sql$;
END
$migration$;

COMMENT ON INDEX public.idx_invoice_chunks_batch_result_selection_key IS
'Supports paged Batch Generate/Batch Issue root-operation result rows by selection_key. Does not affect queue authority.';

-- -----------------------------------------------------------------------------
-- Planner stats refresh for the newly indexed paths.
-- -----------------------------------------------------------------------------

ANALYZE public.timesheets_financials;
ANALYZE public.timesheets;
ANALYZE public.invoices;
ANALYZE public.invoice_document_versions;
ANALYZE public.invoice_operation_chunks;

COMMIT;
