-- PF08 review correction: CHECK must reject missing PREPARE kind on null Work.
-- Installed2259 remains immutable; no existing row is rewritten.

\set ON_ERROR_STOP on

begin;

alter table private.bpay_next_workbench_command_receipt drop constraint bpay_next_workbench_retry_work_ck;
alter table private.bpay_next_workbench_command_receipt add constraint bpay_next_workbench_retry_work_ck
 check(work_id is not null or(result->>'retry_kind') is not distinct from 'PREPARE');

commit;
