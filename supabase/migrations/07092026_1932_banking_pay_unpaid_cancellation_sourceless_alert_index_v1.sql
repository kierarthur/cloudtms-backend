-- One-time CloudTMS schema migration: bounded lookup support for the existing
-- correction-item ledger's source-restoration investigation marker.
-- No business row is changed and no new financial authority is created.

\set ON_ERROR_STOP on

begin;

-- There are no active historical correction requests in the authorised TEST
-- cutover.  Prove that exact precondition before installing even the supporting
-- indexes.  Terminal history is retained unchanged; no row is migrated,
-- rewritten, deleted, or assigned a guessed communication contract.
DO $zero_active_payment_correction_cutover$
DECLARE
  v_active_request_count integer;
  v_active_operation_count integer;
BEGIN
  IF pg_catalog.to_regclass('public.pay_payment_correction_requests') IS NULL
     OR pg_catalog.to_regclass('public.banking_pay_operations') IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_CUTOVER_REQUIRED_RELATION_MISSING';
  END IF;

  SELECT pg_catalog.count(*)::integer
  INTO v_active_request_count
  FROM public.pay_payment_correction_requests AS request_row
  WHERE request_row.status IS NULL
     OR pg_catalog.upper(pg_catalog.btrim(request_row.status)) NOT IN (
       'APPLIED', 'APPLIED_WITH_BLOCKERS', 'BLOCKED',
       'FAILED', 'REJECTED', 'CANCELLED'
     );

  SELECT pg_catalog.count(*)::integer
  INTO v_active_operation_count
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.operation_type = 'PAYMENT_CORRECTION'
    AND (
      operation_row.status IS NULL
      OR pg_catalog.upper(pg_catalog.btrim(operation_row.status)) NOT IN (
        'COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED'
      )
    );

  IF v_active_request_count <> 0 OR v_active_operation_count <> 0 THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_RELEASE_REQUIRES_ZERO_ACTIVE_REQUESTS: requests=%, operations=%',
      v_active_request_count,
      v_active_operation_count;
  END IF;
END
$zero_active_payment_correction_cutover$;

CREATE INDEX IF NOT EXISTS pay_payment_correction_items_sourceless_investigation_batch_idx
  ON public.pay_payment_correction_items (
    pay_batch_id,
    applied_at_utc DESC NULLS LAST,
    id DESC
  )
  WHERE status = 'APPLIED'
    AND after_snapshot_json #>> '{source_restoration,status}' = 'NEEDS_INVESTIGATION'
    AND after_snapshot_json #>> '{source_restoration,policy}' = 'PRESERVE_FROZEN_FACTS_NO_RECONSTRUCTION';

CREATE INDEX IF NOT EXISTS pay_payment_correction_items_sourceless_investigation_item_idx
  ON public.pay_payment_correction_items (
    pay_batch_item_id,
    applied_at_utc DESC NULLS LAST,
    id DESC
  )
  WHERE status = 'APPLIED'
    AND pay_batch_item_id IS NOT NULL
    AND after_snapshot_json #>> '{source_restoration,status}' = 'NEEDS_INVESTIGATION'
    AND after_snapshot_json #>> '{source_restoration,policy}' = 'PRESERVE_FROZEN_FACTS_NO_RECONSTRUCTION';

-- The normal Banking alert-panel request is deliberately unfiltered.  This
-- index matches its newest-first order, so a finite request can stop after the
-- requested prefix instead of sorting every historical investigation row.
CREATE INDEX IF NOT EXISTS idx_pay_correction_items_sourceless_investigation_recent
  ON public.pay_payment_correction_items (
    applied_at_utc DESC NULLS LAST,
    id DESC
  )
  WHERE status = 'APPLIED'
    AND pay_batch_item_id IS NOT NULL
    AND after_snapshot_json #>> '{source_restoration,status}' = 'NEEDS_INVESTIGATION'
    AND after_snapshot_json #>> '{source_restoration,policy}' = 'PRESERVE_FROZEN_FACTS_NO_RECONSTRUCTION';

commit;
