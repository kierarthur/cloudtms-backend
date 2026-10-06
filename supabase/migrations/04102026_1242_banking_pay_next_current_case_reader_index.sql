-- Only current open/paused finance cases. Closed case history is not a list
-- input; component pages use the existing unique(case_id,component_ordinal).
\set ON_ERROR_STOP on
begin;
create index bpay_next_case_current_candidate_page_idx
  on private.bpay_next_finance_case(candidate_id,id)
  where status in ('OPEN','PAUSED');
commit;
