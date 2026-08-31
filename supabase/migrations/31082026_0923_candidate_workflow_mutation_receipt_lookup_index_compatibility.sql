-- Install the bounded request-key lookup without covering JSON values. Some
-- historical audit payloads are larger than PostgreSQL permits in a B-tree
-- INCLUDE tuple. The following 0924 migration is retained immutably after its
-- failed TEST release attempt; because this index already exists, its guarded
-- CREATE INDEX is a safe no-op on every subsequent UPGRADE and NEW replay.
create index if not exists idx_audit_candidate_workflow_mutation_request_v1
on public.audit_events (
  correlation_id,
  ts_utc desc,
  id desc
)
where object_type='candidate_workflow_mutation_receipt';
