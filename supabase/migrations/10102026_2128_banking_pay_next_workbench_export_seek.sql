-- WB18 own export native group -> complete item walk; no financial writer.
\set ON_ERROR_STOP on
begin;
create index bpay_next_wb18_export_group_item_idx on private.bpay_next_workbench_item(group_key,candidate_id,case_component_id,id);
commit;
