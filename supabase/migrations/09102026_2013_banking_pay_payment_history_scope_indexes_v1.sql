-- F-06 re-scaffold of reviewed staged 06102026_1206_banking_pay_payment_history_scope_indexes_v1.sql; canonical read-only history indexes.
-- Two missing read-only Office history access paths. No money/owner/data changes.
-- Existing component-only CASE-event index excludes older untyped events;
-- existing command-member key leads with command_id, not Candidate.
-- Verified no equivalent full-scope indexes exist in the current inventory.

\set ON_ERROR_STOP on

begin;

create index bpay_next_case_event_history_idx
  on private.bpay_next_case_event(case_id,occurred_at_utc,id);
create index bpay_next_command_member_history_idx
  on private.bpay_next_command_member(candidate_id,command_id);

commit;
