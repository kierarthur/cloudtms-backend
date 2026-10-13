-- WB23 original private page receipts only. No rows/backfill or public trigger.
\set ON_ERROR_STOP on
begin;
alter table private.bpay_next_selection_page
 add column matching_request_json jsonb,
 add column captured_result_json jsonb,
 drop constraint bpay_next_selection_page_work_ids_json_check,
 drop constraint bpay_next_selection_page_item_count_check,
 add constraint bpay_next_selection_page_work_ids_json_check check (
   jsonb_typeof(work_ids_json)='array' and jsonb_array_length(work_ids_json)=item_count
   and octet_length(work_ids_json::text)<=262144),
 add constraint bpay_next_selection_page_item_count_check check (
   item_count between 0 and 100 and (item_count>0 or matching_request_json is not null)),
 add constraint bpay_next_selection_matching_receipt_bound check (
   matching_request_json is null or (jsonb_typeof(matching_request_json)='object'
    and octet_length(matching_request_json::text)<=32768 and captured_result_json is not null)),
 add constraint bpay_next_selection_captured_result_bound check (
   captured_result_json is null or (jsonb_typeof(captured_result_json)='object'
    and octet_length(captured_result_json::text)<=120000));
create index bpay_next_selection_matching_page_idx on private.bpay_next_selection_page(run_id,page_no desc)
 where matching_request_json is not null;
alter table private.bpay_next_selection_exclusion_page
 add column captured_result_json jsonb,
 add constraint bpay_next_exclusion_captured_result_bound check (
   captured_result_json is null or (jsonb_typeof(captured_result_json)='object'
    and octet_length(captured_result_json::text)<=120000));
commit;
