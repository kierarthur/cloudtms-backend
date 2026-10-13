-- One-time CloudTMS schema/data migration: banking_pay_next_accepted_resolution_refs
-- WB29 / BD158: exact accepted treatment versions in existing native stores.
-- NULL is historical; new descriptor cuts insert explicit empty/nonempty arrays.

\set ON_ERROR_STOP on

begin;

alter table private.bpay_next_work_choice add column accepted_resolution_refs jsonb
 check(accepted_resolution_refs is null or case when jsonb_typeof(accepted_resolution_refs)='array' then
  jsonb_array_length(accepted_resolution_refs)<=100 and octet_length(accepted_resolution_refs::text)<=32768 else false end);
alter table private.bpay_next_run_line add column accepted_resolution_refs jsonb
 check(accepted_resolution_refs is null or case when jsonb_typeof(accepted_resolution_refs)='array' then
  jsonb_array_length(accepted_resolution_refs)<=5 and octet_length(accepted_resolution_refs::text)<=8192 else false end);
-- Existing RLS/ACL and frozen-line UPDATE/DELETE/TRUNCATE guards cover the columns.

commit;
