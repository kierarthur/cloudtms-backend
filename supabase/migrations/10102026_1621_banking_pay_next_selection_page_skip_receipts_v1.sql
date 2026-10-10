-- DR-01 additive bounded original request/result on the existing private page.
-- No public table/trigger, business rows, producer or money-owner change.
\set ON_ERROR_STOP on
begin;
alter table private.bpay_next_selection_page
  add column choices_request_json jsonb,
  add column skipped_json jsonb not null default '[]'::jsonb,
  add constraint bpay_next_selection_page_choices_request_bound check (
    choices_request_json is null or (jsonb_typeof(choices_request_json)='array'
      and jsonb_array_length(choices_request_json)=item_count
      and octet_length(choices_request_json::text)<=32768)),
  add constraint bpay_next_selection_page_skipped_bound check (
    jsonb_typeof(skipped_json)='array' and jsonb_array_length(skipped_json)<=item_count
      and octet_length(skipped_json::text)<=32768);
commit;
