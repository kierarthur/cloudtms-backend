-- One-time CloudTMS schema/data migration: weekly_source_final_value_snapshot_authority
-- WP-F2 / plan 03 sections 4, 24.4 and 24.5. Append nullable immutable
-- comparison witnesses; existing snapshots retain their original facts/hashes.
-- The established IMMUTABLE_APPEND_ONLY guard covers every added column.
-- No historical backfill, current-setting inference, RLS/ACL or pay change.

\set ON_ERROR_STOP on

begin;

alter table public.weekly_source_final_snapshot_lines
  add column authority_subset_json jsonb,
  add column authority_subset_sha256 bytea,
  add column invoice_economics_sha256 bytea,
  add constraint weekly_source_snapshot_authority_subset_shape check (
    (authority_subset_json is null and authority_subset_sha256 is null)
    or (
      authority_subset_json is not null and authority_subset_sha256 is not null
      and pg_catalog.jsonb_typeof(authority_subset_json)='object'
      and authority_subset_json ?& array[
        'authority_mode','self_bill_enabled','source_group_id','self_bill_correction_presentation'
      ]::text[]
      and (authority_subset_json - array[
        'authority_mode','self_bill_enabled','source_group_id','self_bill_correction_presentation'
      ]::text[])='{}'::jsonb
      and pg_catalog.jsonb_typeof(authority_subset_json->'authority_mode')='string'
      and authority_subset_json->>'authority_mode'='SOURCE_AUTHORITY'
      and pg_catalog.jsonb_typeof(authority_subset_json->'self_bill_enabled')='boolean'
      and authority_subset_json->'self_bill_enabled'='true'::jsonb
      and pg_catalog.jsonb_typeof(authority_subset_json->'source_group_id')='string'
      and (authority_subset_json->>'source_group_id') ~
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      and authority_subset_json->>'self_bill_correction_presentation' in (
        'FULL_REVERSAL_REPLACEMENT','NET_DIFFERENCE_PRESENTATION'
      )
      and pg_catalog.octet_length(authority_subset_sha256)=32
    )
  ),
  add constraint weekly_source_snapshot_invoice_economics_hash_shape check (
    invoice_economics_sha256 is null or pg_catalog.octet_length(invoice_economics_sha256)=32
  );

comment on column public.weekly_source_final_snapshot_lines.authority_subset_json is
  'WP-F2 exact four-field source authority at INSERT only; no historical backfill.';
comment on column public.weekly_source_final_snapshot_lines.authority_subset_sha256 is
  'SHA-256 domain WEEKLY_SOURCE_AUTHORITY_SUBSET_V1 of the stored four-field authority.';
comment on column public.weekly_source_final_snapshot_lines.invoice_economics_sha256 is
  'WP-F2 separate SOURCE_AMOUNT invoice-money witness; does not change pay state.';

commit;
