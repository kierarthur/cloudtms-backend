-- Banking Pay bounded-scope: permit a bootstrapped READY registry to become
-- generation-dirty while its queued ordinary refresh is outstanding.
--
-- initialisation_status records bootstrap completion. Freshness remains
-- fail-closed through evaluated_generation <= dirty_generation and the
-- generation/source-sequence fences owned by the build and publication path.
-- Policy X is unchanged: this migration contains orchestration metadata only.

BEGIN;

ALTER TABLE private.banking_pay_workbench_candidate_scope_registry
  DROP CONSTRAINT IF EXISTS bpay_wb_scope_registry_terminal_chk;

ALTER TABLE private.banking_pay_workbench_candidate_scope_registry
  ADD CONSTRAINT bpay_wb_scope_registry_terminal_chk CHECK (
    (initialisation_status <> 'READY' OR (
      initialised_at_utc IS NOT NULL
      AND failure_json = '{}'::jsonb
    ))
    AND (initialisation_status <> 'FAILED' OR failure_json <> '{}'::jsonb)
    AND updated_at_utc >= created_at_utc
    AND last_dirtied_at_utc >= created_at_utc
    AND (last_evaluated_at_utc IS NULL OR last_evaluated_at_utc >= created_at_utc)
    AND (initialised_at_utc IS NULL OR initialised_at_utc >= created_at_utc)
  );

COMMIT;
