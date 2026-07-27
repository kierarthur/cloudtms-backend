-- CloudTMS Invoice Async V8/V2 operation-control idempotency receipts.
-- TEST only. Adds no workflow table and does not mutate business data.
begin;

alter table public.invoice_operations
  drop constraint if exists invoice_operations_type_ck;

alter table public.invoice_operations
  add constraint invoice_operations_type_ck check (
    operation_type in (
      'GENERATE_INVOICES',
      'BUILD_DOCUMENT',
      'PREPARE_ASSET',
      'ISSUE_INVOICES',
      'DELIVER_INVOICES',
      'RECONCILE_INVOICE_WORK',
      'OPERATION_CONTROL_REQUEST'
    )
  );

create unique index if not exists
  idx_invoice_operation_control_receipt_actor_token_v8
on public.invoice_operations(actor_user_id, idempotency_key)
where operation_type = 'OPERATION_CONTROL_REQUEST';

comment on index
  public.idx_invoice_operation_control_receipt_actor_token_v8 is
  'V8 durable operation-control idempotency authority. One receipt per actor and request-token identity.';

commit;
