-- The workflow replay probe searches the durable audit ledger by its globally
-- unique idempotency key before it knows whether a receipt already exists.
-- Keep the existing workflow-first index for workflow history, and add the
-- complementary request-key-first lookup used by that bounded probe.
create index if not exists idx_audit_candidate_workflow_mutation_request_v1
on public.audit_events (
  correlation_id,
  ts_utc desc,
  id desc
)
include (object_id_text, before_json, after_json)
where object_type='candidate_workflow_mutation_receipt';
