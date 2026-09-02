-- Candidate PAPER cancellation must retire the exact workflow generation and
-- QR source before reopening the logical Timesheet.  These predicates are on
-- JSON-backed mail authority, so the general JSONB GIN index cannot serve the
-- existing scalar equality lookups reliably at operational table size.
create index if not exists idx_mail_outbox_candidate_paper_workflow_v1
on public.mail_outbox (
  (payment_scope_json->>'candidate_workflow_id'),
  (payment_scope_json->>'candidate_workflow_generation'),
  id
)
include (context_id,status,attempt_lease_token,attempt_lease_expires_at_utc)
where type='TIMESHEET_QR'
  and context_kind='timesheets';

create index if not exists idx_mail_outbox_candidate_paper_token_v1
on public.mail_outbox (
  (lower(coalesce(payment_scope_json->>'qr_token_hash',''))),
  (payment_scope_json->>'candidate_workflow_id'),
  (payment_scope_json->>'candidate_workflow_generation'),
  context_id
)
where type='TIMESHEET_QR'
  and context_kind='timesheets';

create index if not exists idx_timesheets_candidate_paper_source_v1
on public.timesheets (booking_id,contract_id,week_ending_date,timesheet_id)
where is_current=true
  and archived_at_utc is null;
