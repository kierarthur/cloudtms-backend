-- PS-04: one WORK recovery mirror for one actual Case event, across operations.
\set ON_ERROR_STOP on
begin;
create unique index bpay_next_work_recovery_event_once
  on private.bpay_next_financial_effect(operation_item_id)
  where effect_kind='WORK_COLLECTION_RECOVERED';
commit;
