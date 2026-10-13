-- F-06 re-scaffold of reviewed staged 07102026_0018_banking_pay_next_initial_tsfin_umbrella_work_shape_v1.sql; additive local source only.
-- One-time CloudTMS schema/data migration: banking_pay_next_initial_tsfin_umbrella_work_shape_v1
-- State the exact authority, safety boundary, and verification before implementation.

\set ON_ERROR_STOP on

begin;

-- E-only additive candidate. Root must use the normal migration generator.
-- No backfill, publication, activation, position seed or financial history write.
alter table private.bpay_next_source_component_binding_version
  add column origin_kind text not null default 'COMMON_HEAD_RECEIPT_V1',
  add column initial_root_authorisation_id uuid references public.weekly_source_root_authorisations(id) on delete restrict,
  add column initial_financial_snapshot_id uuid references public.timesheets_financials(id) on delete restrict,
  add column initial_root_timesheet_id uuid references public.timesheets(timesheet_id) on delete restrict,
  add column initial_root_version integer,
  add column initial_authorisation_generation integer,
  add column initial_authorised_row_signature text,
  add column initial_financial_snapshot_digest bytea,
  add column initial_inventory_digest bytea,
  add column initial_entitlement_digest bytea,
  add column initial_detail_digest bytea,
  add column initial_policy_digest bytea,
  add column initial_certificate_digest bytea,
  alter column source_receipt_id drop not null,
  alter column decision_bundle_id drop not null,
  alter column bundle_revision drop not null,
  alter column request_digest drop not null,
  alter column head_id drop not null,
  add constraint bpay_next_binding_initial_component_uq unique(initial_root_authorisation_id,source_component_id),
  add constraint bpay_next_binding_origin_shape check (
    (origin_kind='COMMON_HEAD_RECEIPT_V1'
      and source_receipt_id is not null and decision_bundle_id is not null
      and bundle_revision is not null and request_digest is not null and head_id is not null
      and initial_root_authorisation_id is null and initial_financial_snapshot_id is null
      and initial_root_timesheet_id is null and initial_root_version is null
      and initial_authorisation_generation is null and initial_authorised_row_signature is null
      and initial_financial_snapshot_digest is null and initial_inventory_digest is null
      and initial_entitlement_digest is null and initial_detail_digest is null
      and initial_policy_digest is null and initial_certificate_digest is null)
    or (origin_kind='INITIAL_AUTHORISED_TSFIN_V1'
      and source_receipt_id is null and decision_bundle_id is null and bundle_revision is null
      and request_digest is null and head_id is null
      and initial_root_authorisation_id is not null and initial_financial_snapshot_id is not null
      and initial_root_timesheet_id is not null and initial_root_version is not null and initial_root_version>0
      and initial_authorisation_generation is not null and initial_authorisation_generation>0
      and initial_authorised_row_signature is not null
      and char_length(initial_authorised_row_signature) between 1 and 512
      and initial_financial_snapshot_digest is not null and octet_length(initial_financial_snapshot_digest)=32
      and initial_inventory_digest is not null and octet_length(initial_inventory_digest)=32
      and initial_entitlement_digest is not null and octet_length(initial_entitlement_digest)=32
      and initial_detail_digest is not null and octet_length(initial_detail_digest)=32
      and initial_policy_digest is not null and octet_length(initial_policy_digest)=32
      and initial_certificate_digest is not null and octet_length(initial_certificate_digest)=32
      and component_state='PRESENT' and approved_line_id is not null
      and component_sha256 is not null and classification_line_id is not null and classification_line_id=approved_line_id
      and chosen_detail_sha256 is null
      and canonical_work_id=approval_work_id and canonical_component_key=approval_component_key
      and publication_command_id_2 is null and publication_work_id_2 is null and publication_revision_id_2 is null
      and publication_work_id_1=approval_work_id and publication_revision_id_1=approval_revision_id)
  );

alter table private.bpay_next_net_projection add constraint bpay_next_projection_umbrella_work_cash_bridge_v1 check (input_kind <> 'UMBRELLA' OR (gross_ex_vat >= 0 AND gross_vat >= 0 AND gross_inc_vat = gross_ex_vat + gross_vat AND entered_paye_net IS NULL AND request_command_id IS NULL AND accepted_gross_additions = 0 AND accepted_gross_deductions = 0 AND accepted_net_additions = 0 AND accepted_recoveries = 0 AND cash_amount = gross_inc_vat));

commit;
