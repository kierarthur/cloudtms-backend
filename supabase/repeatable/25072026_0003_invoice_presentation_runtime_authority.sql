\set ON_ERROR_STOP on

begin;

\ir 24072026_1217_invoice_async_processor_contract_v4/24072026_1217_invoice_work_complete_batch.sql

select exists (
  select 1
  from public.invoice_operations
  where status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
) as invoice_presentation_active_work
\gset

\if :invoice_presentation_active_work
do $deferred$
begin
  raise notice 'INVOICE_PRESENTATION_CUTOVER_DEFERRED_ACTIVE_WORK';
end;
$deferred$;
\else

\ir 24072026_1217_invoice_async_processor_contract_v4/24072026_1217_private_invoice_document_advance_batch_v6_downstream.sql
\ir 24072026_1217_invoice_async_processor_contract_v4/24072026_1217_private_invoice_document_advance_batch.sql
\ir 24072026_1217_invoice_async_processor_contract_v4/24072026_1217_private_invoice_issue_advance_batch.sql
\ir 24072026_1217_invoice_async_processor_contract_v4/24072026_1217_invoice_work_context_batch.sql
\ir 24072026_1217_invoice_async_processor_contract_v4/24072026_1217_private_invoice_delivery_advance_batch.sql

\endif

commit;
