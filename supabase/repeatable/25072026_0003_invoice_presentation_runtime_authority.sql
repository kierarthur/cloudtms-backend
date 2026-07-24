\set ON_ERROR_STOP on

begin;

do $guard$
begin
  if exists (
    select 1
    from public.invoice_operations
    where status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
  ) then
    raise exception using
      errcode = '55000',
      message = 'INVOICE_PRESENTATION_CUTOVER_ACTIVE_WORK';
  end if;
end;
$guard$;

\ir 24072026_1217_invoice_async_processor_contract_v4/24072026_1217_private_invoice_document_advance_batch.sql
\ir 24072026_1217_invoice_async_processor_contract_v4/24072026_1217_private_invoice_issue_advance_batch.sql
\ir 24072026_1217_invoice_async_processor_contract_v4/24072026_1217_invoice_work_context_batch.sql
\ir 24072026_1217_invoice_async_processor_contract_v4/24072026_1217_private_invoice_delivery_advance_batch.sql

commit;
