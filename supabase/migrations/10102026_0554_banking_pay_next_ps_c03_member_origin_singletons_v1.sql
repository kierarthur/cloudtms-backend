-- PS-03: exact singleton origins. No data backfill or public exposure.
\set ON_ERROR_STOP on
begin;
create unique index bpay_next_transfer_singleton_origin_once
  on private.bpay_next_transfer_member(transfer_id,subject_kind)
  where subject_kind in ('NET_ADJUSTMENT','CASH_REISSUE');
commit;
