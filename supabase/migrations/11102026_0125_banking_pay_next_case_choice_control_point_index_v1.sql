-- One-time CloudTMS schema/data migration: banking_pay_next_case_choice_control_point_index_v1
-- WB-26: index the existing per-component latest control read, including cleared controls.
-- Private additive schema only; no business rows or public-table triggers.

\set ON_ERROR_STOP on

begin;

create index bpay_next_snooze_case_control_point_idx
  on private.bpay_next_snooze (case_component_id, control_sequence desc)
  where case_component_id is not null;

commit;
