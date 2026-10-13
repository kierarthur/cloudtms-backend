-- One-time CloudTMS schema/data migration: banking_pay_next_resolution_remaining_basis
-- WB-45 / BD153 source review: immutable exact save-time native residual scope.
-- New resolution table is empty by construction; no backfill or business action.

\set ON_ERROR_STOP on

begin;

alter table private.bpay_next_component_resolution add column original_position_basis jsonb not null
 check(jsonb_typeof(original_position_basis)='object' and octet_length(original_position_basis::text)<=2048);

commit;
