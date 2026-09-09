-- Bounded lookup for the exact frozen Candidate/channel scopes returned by
-- cancellation. This is transport/indexing only: no financial value or policy
-- classification is stored or derived here.

\set ON_ERROR_STOP on

DROP INDEX CONCURRENTLY IF EXISTS private.banking_pay_draft_frozen_scopes_v8_batch_candidate_idx;

CREATE INDEX CONCURRENTLY banking_pay_draft_frozen_scopes_v8_batch_candidate_idx
  ON private.banking_pay_draft_frozen_candidate_scopes_v8 (
    pay_batch_id,
    candidate_id,
    resolved_pay_channel,
    operation_id,
    candidate_scope_ordinal
  );

DO $migration_verification$
DECLARE
  v_index_oid oid;
BEGIN
  SELECT index_class.oid
  INTO v_index_oid
  FROM pg_catalog.pg_class AS index_class
  JOIN pg_catalog.pg_namespace AS index_namespace
    ON index_namespace.oid = index_class.relnamespace
  JOIN pg_catalog.pg_index AS index_catalog
    ON index_catalog.indexrelid = index_class.oid
  WHERE index_namespace.nspname = 'private'
    AND index_class.relname = 'banking_pay_draft_frozen_scopes_v8_batch_candidate_idx'
    AND index_catalog.indisvalid
    AND index_catalog.indisready;

  IF v_index_oid IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_CANCEL_RETURN_SCOPE_INDEX_INVALID'
      USING ERRCODE = '55000';
  END IF;
END;
$migration_verification$;
