-- One-time CloudTMS schema/data migration: weekly_source_local_publication_origin_v2
-- Joint V4 section 5.3: typed local receipt provenance, not a financial owner.
-- Existing preauthorisation receipts retain NULL provenance; no backfill.

\set ON_ERROR_STOP on

begin;

alter table private.weekly_source_local_protected_decision_receipts
  add column common_decision_bundle_id uuid,
  add column common_bundle_revision bigint,
  add column publication_origin_kind text,
  add column publication_origin_digest bytea,
  add column source_qualification_digest bytea,
  add constraint weekly_source_local_publication_origin_complete_v2 check (
    (common_decision_bundle_id is null and common_bundle_revision is null
      and publication_origin_kind is null and publication_origin_digest is null
      and source_qualification_digest is null)
    or
    (common_decision_bundle_id is not null and common_bundle_revision is not null
      and common_bundle_revision>=1
      and publication_origin_kind is not null
      and publication_origin_kind='PROTECTED_LOCAL_DECISION_V1'
      and publication_origin_digest is not null and octet_length(publication_origin_digest)=32
      and source_qualification_digest is not null and octet_length(source_qualification_digest)=32)
  ),
  add constraint weekly_source_local_publication_bundle_v2 foreign key (
    common_decision_bundle_id,common_bundle_revision
  ) references public.weekly_source_entitlement_decision_bundles(
    decision_bundle_id,bundle_revision
  ) deferrable initially deferred;

comment on column private.weekly_source_local_protected_decision_receipts.common_decision_bundle_id is
  'Actual common publication bundle for this local decision; absent before first Authorise. Sealed in the original PREPARING transaction, never inferred from a later HEAD.';
comment on column private.weekly_source_local_protected_decision_receipts.publication_origin_digest is
  'Canonical closed PROTECTED_LOCAL_DECISION_V1 origin digest; not a Final manifest or a Banking application certificate.';

commit;
