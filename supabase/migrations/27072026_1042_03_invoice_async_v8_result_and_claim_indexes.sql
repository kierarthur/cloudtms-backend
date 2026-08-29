-- CloudTMS TEST Invoice Async V8/V2 bounded claim, release and result indexes.

create index if not exists idx_invoice_operation_chunks_claim_v8
on public.invoice_operation_chunks (
  status,
  is_manifest_member,
  manifest_committed,
  priority desc,
  run_after_utc,
  created_at_utc,
  id
)
where chunk_type <> 'DOCUMENT_INPUT'
  and status in ('QUEUED','RETRY_WAIT','RUNNING');

create index if not exists idx_invoice_manifest_release_v8
on public.invoice_operation_chunks (
  operation_id,
  manifest_generation,
  manifest_committed,
  status,
  phase,
  selection_key,
  id
)
where is_manifest_member;

create index if not exists idx_invoice_batch_result_all_v8
on public.invoice_operation_chunks (
  operation_id,
  manifest_generation,
  selection_key,
  id
)
where result_visible
  and replaced_by_chunk_id is null
  and chunk_type in ('GENERATION_GROUP','ISSUE_INVOICE');

create index if not exists idx_invoice_batch_result_category_v8
on public.invoice_operation_chunks (
  operation_id,
  manifest_generation,
  result_category,
  selection_key,
  id
)
where result_visible
  and replaced_by_chunk_id is null
  and chunk_type in ('GENERATION_GROUP','ISSUE_INVOICE');
