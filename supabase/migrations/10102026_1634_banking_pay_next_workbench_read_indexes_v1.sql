-- WB-10 exact current maintained filter/keyset support. No history/money writes.
-- Reuse1252 FUTURE_WEEK eligible_from_date/id partial,1100 native snooze_due,
--1429 finite seed/metadata/task/due indexes, and existing native PK identities.
\set ON_ERROR_STOP on
begin;

create index bpay_next_wb10_item_section_candidate_idx on private.bpay_next_workbench_item(section,candidate_id,id);
create index bpay_next_wb10_item_candidate_week_idx on private.bpay_next_workbench_item(candidate_id,section,week_ending_date,work_id,id);
create index bpay_next_wb10_item_client_section_idx on private.bpay_next_workbench_item(client_id,section,id);
create index bpay_next_wb10_item_channel_section_idx on private.bpay_next_workbench_item(target_pay_channel,section,id);
create index bpay_next_wb10_item_reason_candidate_idx on private.bpay_next_workbench_item(section,reason_code,candidate_id,work_id,id);
create index bpay_next_wb10_candidate_name_idx on private.bpay_next_workbench_candidate_summary(section,pay_channel,client_scope,candidate_sort_name collate "C",(tms_ref is null),(coalesce(tms_ref,'') collate "C"),candidate_id);
create index bpay_next_wb10_candidate_amount_idx on private.bpay_next_workbench_candidate_summary(section,pay_channel,client_scope,(unknown_amount_count>0),(case when unknown_amount_count=0 then amount else 0 end),candidate_id);
create index bpay_next_wb10_candidate_deductions_idx on private.bpay_next_workbench_candidate_summary(section,pay_channel,client_scope,(deduction_item_count>0),candidate_sort_name collate "C",(tms_ref is null),(coalesce(tms_ref,'') collate "C"),candidate_id);
create index bpay_next_wb10_candidate_name_prefix_idx on private.bpay_next_workbench_candidate_summary(section,pay_channel,client_scope,candidate_sort_name text_pattern_ops,candidate_id);
create index bpay_next_wb10_candidate_ref_prefix_idx on private.bpay_next_workbench_candidate_summary(section,pay_channel,client_scope,tms_ref text_pattern_ops,candidate_id);
create index bpay_next_wb10_item_name_idx on private.bpay_next_workbench_item(section,(coalesce(candidate_display_name,'') collate "C"),candidate_id,id);
create index bpay_next_wb10_item_payment_idx on private.bpay_next_workbench_item(section,(coalesce(timesheet_reference,'') collate "C"),id);
create index bpay_next_wb10_item_amount_idx on private.bpay_next_workbench_item(section,(nominal_source_ex_vat is null),(coalesce(nominal_source_ex_vat,0)),id);
create index bpay_next_wb10_item_name_prefix_idx on private.bpay_next_workbench_item(section,candidate_display_name text_pattern_ops,id);
create index bpay_next_wb10_item_ref_prefix_idx on private.bpay_next_workbench_item(section,candidate_reference text_pattern_ops,id);
create index bpay_next_wb10_item_ts_prefix_idx on private.bpay_next_workbench_item(section,timesheet_reference text_pattern_ops,id);
create index bpay_next_wb10_item_group_page_idx on private.bpay_next_workbench_item(group_key,section,id);
create index bpay_next_wb10_item_group_all_page_idx on private.bpay_next_workbench_item(group_key,id);
create index bpay_next_wb10_item_section_page_idx on private.bpay_next_workbench_item(section,id);
create index bpay_next_wb10_item_candidate_page_idx on private.bpay_next_workbench_item(candidate_id,section,id);
create index bpay_next_wb10_item_reason_page_idx on private.bpay_next_workbench_item(section,reason_code,id);
create index bpay_next_wb10_group_week_idx on private.bpay_next_workbench_group(candidate_id,(week_ending_date is null),(coalesce(week_ending_date,date '0001-01-01')),group_kind collate "C",(coalesce(work_id,case_id)));
create index bpay_next_wb10_scope_client_page_idx on private.bpay_next_workbench_scope_summary(section,pay_channel,client_scope collate "C");

commit;
