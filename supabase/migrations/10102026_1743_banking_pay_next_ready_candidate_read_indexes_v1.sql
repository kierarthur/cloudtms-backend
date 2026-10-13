-- WB12 finite Status keyset and owner E6 indexed eligible count support only.
\set ON_ERROR_STOP on
begin;
create index bpay_next_wb12_candidate_status_idx on private.bpay_next_workbench_candidate_summary(section,pay_channel,client_scope,(payable_count=0),(bank_warning_code is null),(coalesce(bank_warning_code,'') collate "C"),candidate_sort_name collate "C",candidate_id);
create index bpay_next_wb12_candidate_eligible_count_idx on private.bpay_next_workbench_candidate_summary(section,pay_channel,client_scope,candidate_id) include(payable_count,deduction_item_count) where payable_count>0;
commit;
