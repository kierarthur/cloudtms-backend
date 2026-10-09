-- F-06 re-scaffold of reviewed staged 06102026_0236_banking_pay_next_channel_page_index_v1.sql; canonical channel lookup index.
-- STAGED ONLY. Root must scaffold a new canonical migration; do not edit core1324.
\set ON_ERROR_STOP on
begin;
create index bpay_next_run_worker_channel_page_idx
  on private.bpay_next_run_worker(run_id,target_pay_channel,candidate_id,id);
commit;
