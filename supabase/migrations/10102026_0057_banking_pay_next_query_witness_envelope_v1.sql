-- F-25 complete QuerySync witness envelope. Successor of locally installed0020.
-- Up to1000 historical-resolved inputs plus1000 distinct omissions need4000
-- actual comparison/decision witnesses. No evidence truncation or extra input
-- or canonical-stage capacity is admitted. System Hours remains exactly2*N.
\set ON_ERROR_STOP on
begin;
alter table private.bpay_next_query_root_completion
 drop constraint bpay_next_query_root_completion_original_witness_count_check,
 add constraint bpay_next_query_root_completion_original_witness_count_check
 check(original_witness_count between 0 and 4000);
alter table private.bpay_next_query_root_completion_witness
 drop constraint bpay_next_query_root_completion_witness_witness_ordinal_check,
 add constraint bpay_next_query_root_completion_witness_witness_ordinal_check
 check(witness_ordinal between 1 and 4000);
commit;
