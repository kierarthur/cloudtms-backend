-- Joint Source/Banking V4 I3: retain the original validated I1 inventory digest
-- when an initial authorised TSFIN approval has no immutable Source HEAD.
-- No backfill: an old unqualified initial approval stays unavailable to I3.

\set ON_ERROR_STOP on

begin;

alter table private.bpay_next_work_revision
  add column source_inventory_digest bytea,
  add constraint bpay_next_revision_source_inventory_digest_check check (
    source_inventory_digest is null or (
      pg_catalog.octet_length(source_inventory_digest)=32
      and source_kind in ('SOURCE','PROTECTED') and source_event_id is not null
      and (source_kind<>'PROTECTED' or source_head_id is not null)
    )
  );

comment on column private.bpay_next_work_revision.source_inventory_digest is
  'Original validated Source I1 approval_basis.inventory_digest, captured only at genuine approval INSERT; NULL is not certified absence. Immutable under bpay_next_revision_guard_v1. No historical backfill.';

-- The installed generic revision guard compares every INSERT-captured field
-- on its sole seal UPDATE and forbids all changes after sealing. Do not add
-- an UPDATE path for this digest, a live-inventory hash or a GUC exemption.

commit;
