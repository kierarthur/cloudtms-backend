-- WB-29 / BD154: required header note only, no data rewrite or public trigger.

\set ON_ERROR_STOP on

begin;

alter table private.bpay_next_workbench_item
 drop constraint bpay_next_workbench_item_channel_note_code_check;
alter table private.bpay_next_workbench_item
 add constraint bpay_next_workbench_item_channel_note_code_check
 check(channel_note_code in ('CHANNEL_RESOLVER','MOVE_CHANNEL_RESOLVER','CHANNEL_RESOLUTION_REQUIRED'));

commit;
