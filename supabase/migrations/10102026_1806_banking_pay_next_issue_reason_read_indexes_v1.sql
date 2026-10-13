-- WB14 reason header uses maintained decision priority and native item identity.
\set ON_ERROR_STOP on
begin;
create index bpay_next_wb14_reason_sort_idx on private.bpay_next_workbench_item(section,reason_priority,reason_code collate "C",id);
create index bpay_next_wb14_group_reason_sort_idx on private.bpay_next_workbench_item(group_key,section,reason_code,reason_priority,id);
commit;
