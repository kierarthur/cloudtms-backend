-- One-time rollout fence for the jointly reviewed bounded Local-only witness.
-- No historical origin, hash or pending request is rewritten. If an older
-- Local common publication exists, release stops for explicit compatibility
-- review instead of silently invalidating its immutable receipt/release.
\set ON_ERROR_STOP on

begin;

lock table public.weekly_source_entitlement_heads,
  public.weekly_source_pending_entitlement_bundles,
  private.weekly_source_local_protected_decision_receipts in share mode;

do $cutover$
begin
  if exists(select 1 from public.weekly_source_entitlement_heads h
      where h.source_origin_json->>'origin_kind'='PROTECTED_LOCAL_DECISION_V1')
    or exists(select 1 from public.weekly_source_pending_entitlement_bundles p
      where p.request_json#>>'{financial_request,source_revision,origin_kind}'='PROTECTED_LOCAL_DECISION_V1')
    or exists(select 1 from private.weekly_source_local_protected_decision_receipts r
      where r.common_decision_bundle_id is not null or r.publication_origin_kind is not null
        or r.approved_snapshot_json->'local_publication_origin' is not null) then
    raise exception 'WEEKLY_SOURCE_LOCAL_FLAT_ORIGIN_CUTOVER_REQUIRES_EMPTY_OLD_PROTOCOL'
      using errcode='55000';
  end if;
end;
$cutover$;

commit;
