-- A blocked invoice-issue chunk is an active uniqueness owner only while its
-- parent operation is active.  Parent-aware validation and classification
-- enforce that runtime guard.  The physical unique index therefore protects
-- the concurrently executable chunk states and permits terminal operations to
-- retain blocked result rows as immutable audit history.
drop index if exists public.uq_invoice_operation_chunks_active_issue_invoice;

create unique index uq_invoice_operation_chunks_active_issue_invoice
on public.invoice_operation_chunks(entity_id)
where chunk_type='ISSUE_INVOICE'
  and entity_type='INVOICE'
  and status in('QUEUED','RUNNING','WAITING','RETRY_WAIT');
