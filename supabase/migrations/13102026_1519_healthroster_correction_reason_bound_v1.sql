-- One-time CloudTMS schema/data migration: healthroster_correction_reason_bound_v1
-- The service/public correction reason contract is 1..1000 characters.
-- Preserve the already locked request migration; add its tighter bound here.

\set ON_ERROR_STOP on

begin;

alter table private.timesheet_correction_requests
  add constraint timesheet_correction_reason_bound_v1 check(length(btrim(reason)) between 1 and 1000);

commit;
