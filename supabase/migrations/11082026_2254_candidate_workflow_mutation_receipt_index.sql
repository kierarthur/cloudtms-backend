-- Durable workflow mutation replay lookup.  The receipt remains in the
-- existing audit authority; this index does not introduce another Candidate
-- business table or public API surface.
create index if not exists idx_audit_candidate_workflow_mutation_receipt_v1
on public.audit_events (
  object_type,
  object_id_text,
  correlation_id,
  ts_utc desc,
  id desc
)
where object_type='candidate_workflow_mutation_receipt';
