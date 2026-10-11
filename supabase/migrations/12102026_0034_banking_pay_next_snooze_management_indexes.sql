-- WB-39 native active-control management pagination only.
\set ON_ERROR_STOP on
begin;
create index bpay_next_snooze_created_desc_idx on private.bpay_next_snooze(created_at_utc desc,id) where active;
create index bpay_next_snooze_created_asc_idx on private.bpay_next_snooze(created_at_utc asc,id) where active;
create index bpay_next_snooze_candidate_created_idx on private.bpay_next_snooze(candidate_id,created_at_utc desc,id) where active;
commit;
