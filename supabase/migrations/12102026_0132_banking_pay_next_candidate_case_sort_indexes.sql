-- FC-02 SCR114 / owner A3e: native current Case counters copied only into existing current read keys.
\set ON_ERROR_STOP on
begin;
alter table private.bpay_next_case_list_key add column list_funded numeric;
alter table private.bpay_next_case_list_key add column list_recovered numeric;
alter table private.bpay_next_case_list_key add column list_written_off numeric;
update private.bpay_next_case_list_key k set list_funded=c.principal_funded,list_recovered=c.principal_recovered,list_written_off=c.principal_written_off from private.bpay_next_finance_case c where c.id=k.case_id;
create index bpay_next_case_list_funded_asc_idx on private.bpay_next_case_list_key ((list_funded is null),list_funded asc nulls last,(created_at_utc is null),created_at_utc desc nulls last,case_id);
create index bpay_next_case_list_funded_desc_idx on private.bpay_next_case_list_key ((list_funded is null),list_funded desc nulls last,(created_at_utc is null),created_at_utc desc nulls last,case_id);
create index bpay_next_case_list_recovered_asc_idx on private.bpay_next_case_list_key ((list_recovered is null),list_recovered asc nulls last,(created_at_utc is null),created_at_utc desc nulls last,case_id);
create index bpay_next_case_list_recovered_desc_idx on private.bpay_next_case_list_key ((list_recovered is null),list_recovered desc nulls last,(created_at_utc is null),created_at_utc desc nulls last,case_id);
create index bpay_next_case_list_written_off_asc_idx on private.bpay_next_case_list_key ((list_written_off is null),list_written_off asc nulls last,(created_at_utc is null),created_at_utc desc nulls last,case_id);
create index bpay_next_case_list_written_off_desc_idx on private.bpay_next_case_list_key ((list_written_off is null),list_written_off desc nulls last,(created_at_utc is null),created_at_utc desc nulls last,case_id);
commit;
