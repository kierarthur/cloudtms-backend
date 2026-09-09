-- Banking Pay Candidate dirty-apply cohort lookup indexes.
--
-- These indexes are queue-discovery only. They do not alter queue identity,
-- eligibility, Policy X classification, or any financial/economic authority.
-- The names are new and reserved to this migration, so an interrupted
-- concurrent build can be removed and rebuilt safely before the migration is
-- entered in the immutable release ledger.

\set ON_ERROR_STOP on

DROP INDEX CONCURRENTLY IF EXISTS public.idx_bpay_wb_jobs_candidate_dirty_active_cohort_v1;

CREATE INDEX CONCURRENTLY idx_bpay_wb_jobs_candidate_dirty_active_cohort_v1
  ON public.banking_pay_workbench_jobs (candidate_id, id)
  WHERE job_type = 'WORKBENCH_CANDIDATE_DIRTY_APPLY'
    AND status IN ('QUEUED', 'RUNNING')
    AND candidate_id IS NOT NULL;

DROP INDEX CONCURRENTLY IF EXISTS public.idx_bpay_wb_jobs_legacy_candidate_dirty_active_cohort_v1;

CREATE INDEX CONCURRENTLY idx_bpay_wb_jobs_legacy_candidate_dirty_active_cohort_v1
  ON public.banking_pay_workbench_jobs (
    (lower(btrim(COALESCE(payload_json ->> 'candidate_id', '')))),
    id
  )
  WHERE job_type = 'WORKBENCH_CANDIDATE_DIRTY_APPLY'
    AND status IN ('QUEUED', 'RUNNING')
    AND candidate_id IS NULL;

DO $migration_verification$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_catalog.pg_class AS index_class
    JOIN pg_catalog.pg_namespace AS index_namespace
      ON index_namespace.oid = index_class.relnamespace
    JOIN pg_catalog.pg_index AS index_catalog
      ON index_catalog.indexrelid = index_class.oid
    WHERE index_namespace.nspname = 'public'
      AND index_class.relname IN (
        'idx_bpay_wb_jobs_candidate_dirty_active_cohort_v1',
        'idx_bpay_wb_jobs_legacy_candidate_dirty_active_cohort_v1'
      )
      AND index_catalog.indisvalid IS NOT TRUE
  ) OR (
    SELECT count(*)
    FROM pg_catalog.pg_class AS index_class
    JOIN pg_catalog.pg_namespace AS index_namespace
      ON index_namespace.oid = index_class.relnamespace
    JOIN pg_catalog.pg_index AS index_catalog
      ON index_catalog.indexrelid = index_class.oid
    WHERE index_namespace.nspname = 'public'
      AND index_class.relname IN (
        'idx_bpay_wb_jobs_candidate_dirty_active_cohort_v1',
        'idx_bpay_wb_jobs_legacy_candidate_dirty_active_cohort_v1'
      )
      AND index_catalog.indisvalid
  ) <> 2 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_DIRTY_COHORT_INDEX_INVALID'
      USING ERRCODE = '55000';
  END IF;
END;
$migration_verification$;
