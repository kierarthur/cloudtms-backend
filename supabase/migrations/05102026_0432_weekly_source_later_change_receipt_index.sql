-- Point lookup for the existing immutable later-change decision receipt.
-- Preserve the owner's global idempotency-key conflict semantics and its
-- original ts_utc/id first-receipt ordering. This is deliberately NONUNIQUE:
-- no historical receipt is adopted, rewritten, collapsed or deleted.
-- No new financial authority, receipt table, browser grant or pay calculation.
\set ON_ERROR_STOP on

begin;

create index idx_weekly_source_later_change_receipt_v1
on public.audit_events ((after_json->>'idempotency_key'),ts_utc,id)
where action='WEEKLY_SOURCE_LATER_CHANGE_DECIDED';

comment on index public.idx_weekly_source_later_change_receipt_v1 is
  'Exact existing later-change receipt lookup: global idempotency key, then original ts_utc/id order; nonunique and no historical rewrite.';

commit;
