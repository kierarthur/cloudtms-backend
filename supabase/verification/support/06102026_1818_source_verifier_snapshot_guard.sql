-- Verification only: one transaction snapshot for complete-row comparisons.
-- Otherwise an unrelated committed background update can be mistaken for a
-- write by this rollback fixture. Every field and every original guard remains
-- included; this transaction still sees ALL of its own writes. A write conflict
-- still fails closed. This changes no application isolation or Banking policy.
-- Included callers must establish their outer isolation BEFORE their first read.
do $source_verifier_snapshot_guard$
begin
  if pg_catalog.current_setting('transaction_isolation')<>'repeatable read' then
    raise exception using errcode='P0001',
      message='SOURCE_VERIFIER_TRANSACTION_SNAPSHOT_REQUIRED';
  end if;
end $source_verifier_snapshot_guard$;
