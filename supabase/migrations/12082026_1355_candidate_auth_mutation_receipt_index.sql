-- Durable Candidate authentication/account mutation replay lookup.  The
-- existing audit authority remains the only storage owner; this introduces no
-- Candidate business table or public RPC.
create index if not exists idx_audit_candidate_auth_mutation_receipt_v1
on public.audit_events (
  object_type,
  object_id_text,
  correlation_id,
  ts_utc desc,
  id desc
)
where object_type='candidate_auth_mutation_receipt';
