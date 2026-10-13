-- E-only full-shape replacement for UNADOPTED 0056 V1, not a follow-on to V1.
-- Requires current W 1245 and Source V2 0105 witness schema. No backfill.
\set ON_ERROR_STOP on
begin;
alter table private.bpay_next_source_component_binding_version add constraint bpay_next_binding_state_identity_v2
  unique(id,component_state);

create table private.bpay_next_source_comparison_receipt (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  receipt_kind text not null check(receipt_kind in ('REVIEW','PENDING_REVIEW','RESERVE','SETTLE','RELEASE','CASE_RECOVERY','CASE_RECONCILE','CASE_RESERVE','CASE_RELEASE')),
  qualification_state text not null check(qualification_state in ('READY','PENDING','REVIEW')),
  review_issue_code text,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  canonical_work_id uuid not null,
  canonical_component_key text not null,
  source_binding_version_id uuid not null,
  approval_work_id uuid not null,
  approval_revision_id uuid not null,
  approval_component_key text not null,
  approval_component_state text not null check(approval_component_state in ('PRESENT','REMOVED')),
  approved_line_id uuid,
  binding_digest bytea not null check(octet_length(binding_digest)=32),
  approval_component_sha256 bytea check(octet_length(approval_component_sha256)=32),
  approval_chosen_detail_sha256 bytea check(octet_length(approval_chosen_detail_sha256)=32),
  review_certificate_id uuid not null references private.weekly_source_destination_reprice_witness_v1(id) on delete restrict,
  review_sha256 bytea not null check(octet_length(review_sha256)=32),
  basis_review_id uuid not null,
  previous_comparison_id uuid,
  comparison_revision_before bigint not null check(comparison_revision_before>=0),
  comparison_revision bigint not null check(comparison_revision=comparison_revision_before+1),
  source_book_revision_before bigint not null check(source_book_revision_before>0),
  source_book_revision bigint not null,
  native_realised_paye_before private.bpay_next_penny_amount not null,
  native_realised_umbrella_before private.bpay_next_penny_amount not null,
  native_held_paye_before private.bpay_next_penny_amount not null check(native_held_paye_before>=0),
  native_held_umbrella_before private.bpay_next_penny_amount not null check(native_held_umbrella_before>=0),
  native_realised_paye private.bpay_next_penny_amount not null,
  native_realised_umbrella private.bpay_next_penny_amount not null,
  native_held_paye private.bpay_next_penny_amount not null check(native_held_paye>=0),
  native_held_umbrella private.bpay_next_penny_amount not null check(native_held_umbrella>=0),
  comparable_paid_ex_vat_before private.bpay_next_penny_amount,
  comparable_held_ex_vat_before private.bpay_next_penny_amount check(comparable_held_ex_vat_before>=0),
  comparable_paid_ex_vat private.bpay_next_penny_amount,
  comparable_held_ex_vat private.bpay_next_penny_amount check(comparable_held_ex_vat>=0),
  approved_source_ex_vat private.bpay_next_penny_amount not null,
  observed_approved_source_ex_vat private.bpay_next_penny_amount not null,
  selected_residual_ex_vat private.bpay_next_penny_amount,
  target_pay_channel text not null check(target_pay_channel in ('PAYE','UMBRELLA')),
  comparison_policy_id uuid not null,
  policy_window_id uuid not null,
  effective_pay_date date not null check(isfinite(effective_pay_date)),
  umbrella_vat_chargeable boolean not null,
  selected_mode text not null check(selected_mode in
    ('NATIVE_IDENTITY','SUGGESTED_EQUIVALENT_BASIS','MANUAL_REPLACEMENT_RATE','MANUAL_AMOUNT')),
  quote_json jsonb not null check(jsonb_typeof(quote_json)='object' and octet_length(quote_json::text)<=32768),
  basis_sha256 bytea not null check(octet_length(basis_sha256)=32),
  -- One current certificate carries the actual Case tuple, not another Case balance.
  work_collection_id uuid references private.bpay_next_work_collection(id) on delete restrict,
  case_id uuid references private.bpay_next_finance_case(id) on delete restrict,
  case_component_id uuid,
  case_basis_before jsonb,
  case_basis jsonb,
  case_resolution jsonb,
  operation_job_id uuid references private.bpay_next_job(id) on delete restrict,
  operation_identity uuid,
  operation_hold_id uuid references private.bpay_next_hold(id) on delete restrict,
  operation_effect_id uuid references private.bpay_next_financial_effect(id) on delete restrict,
  operation_case_event_id uuid references private.bpay_next_case_event(id) on delete restrict,
  operation_case_instruction_id uuid references private.bpay_next_run_case_instruction(id) on delete restrict,
  operation_case_result_id uuid references private.bpay_next_case_allocation_result(id) on delete restrict,
  operation_case_hold_id uuid references private.bpay_next_case_hold(id) on delete restrict,
  native_source_channel text check(native_source_channel in ('PAYE','UMBRELLA')),
  native_delta_realised private.bpay_next_penny_amount,
  native_delta_held private.bpay_next_penny_amount,
  comparable_operation_ex_vat private.bpay_next_penny_amount check(comparable_operation_ex_vat>=0),
  local_work_id uuid,
  local_component_key text,
  local_scalar_before jsonb,
  local_scalar_after jsonb,
  actor_user_id uuid not null references public.tms_users(id) on delete restrict,
  reason text not null check(octet_length(reason) between 1 and 1024 and btrim(reason)<>''),
  accepted_at_utc timestamptz not null,
  snapshot_sha256 bytea not null check(octet_length(snapshot_sha256)=32),
  unique(canonical_work_id,canonical_component_key,comparison_revision),
  unique(id,canonical_work_id,canonical_component_key,comparison_revision),
  unique(id,canonical_work_id,canonical_component_key),
  unique(id,source_binding_version_id,canonical_work_id,canonical_component_key),
  unique(id,source_binding_version_id,canonical_work_id,canonical_component_key,approval_component_state),
  unique(id,work_collection_id,canonical_work_id,canonical_component_key),
  unique(id,source_binding_version_id,canonical_work_id,canonical_component_key,source_book_revision,
    approval_work_id,approval_revision_id,approval_component_key,approved_line_id),
  unique(id,source_binding_version_id,canonical_work_id,canonical_component_key,source_book_revision,
    approval_work_id,approval_revision_id,approval_component_key,approved_source_ex_vat,
    comparable_paid_ex_vat,comparable_held_ex_vat,selected_residual_ex_vat),
  foreign key(canonical_work_id,canonical_component_key) references private.bpay_next_position(work_id,component_key) on delete restrict,
  foreign key(source_binding_version_id,canonical_work_id,canonical_component_key)
    references private.bpay_next_source_component_binding_version(id,canonical_work_id,canonical_component_key) on delete restrict,
  foreign key(source_binding_version_id,approval_work_id,approval_revision_id,approval_component_key)
    references private.bpay_next_source_component_binding_version(id,approval_work_id,approval_revision_id,approval_component_key) on delete restrict,
  foreign key(source_binding_version_id,approval_component_state) references private.bpay_next_source_component_binding_version(id,component_state) on delete restrict,
  foreign key(approval_revision_id,approved_line_id,approval_component_key)
    references private.bpay_next_approved_line(revision_id,id,component_key) on delete restrict,
  foreign key(comparison_policy_id,policy_window_id)
    references private.bpay_next_valuation_policy_window(policy_id,source_window_id) on delete restrict,
  foreign key(basis_review_id,canonical_work_id,canonical_component_key)
    references private.bpay_next_source_comparison_receipt(id,canonical_work_id,canonical_component_key) on delete restrict deferrable initially deferred,
  foreign key(previous_comparison_id,canonical_work_id,canonical_component_key)
    references private.bpay_next_source_comparison_receipt(id,canonical_work_id,canonical_component_key) on delete restrict,
  foreign key(local_work_id,local_component_key) references private.bpay_next_position(work_id,component_key) on delete restrict,
  foreign key(case_component_id,case_id,candidate_id) references private.bpay_next_case_component(id,case_id,candidate_id) on delete restrict,
  check((work_collection_id is null and case_id is null and case_component_id is null and case_basis is null and case_resolution is null)
    or (work_collection_id is not null and case_id is not null and case_component_id is not null and case_basis is not null and case_resolution is not null
      and jsonb_typeof(case_basis)='object' and jsonb_typeof(case_resolution)='object')),
  check((previous_comparison_id is null)=(comparison_revision_before=0)),
  check((approval_component_state='PRESENT' and approved_line_id is not null)
    or (approval_component_state='REMOVED' and approved_line_id is null and observed_approved_source_ex_vat=0)),
  check((qualification_state in ('READY','PENDING')
      and ((qualification_state='READY' and review_issue_code is null)
        or (qualification_state='PENDING' and review_issue_code='SOURCE_PUBLICATION_PENDING')) and comparable_paid_ex_vat is not null
      and comparable_held_ex_vat is not null and selected_residual_ex_vat is not null
      and selected_residual_ex_vat=approved_source_ex_vat-comparable_paid_ex_vat-comparable_held_ex_vat)
    or (qualification_state='REVIEW' and review_issue_code is not null
      and ((comparable_paid_ex_vat is null and comparable_held_ex_vat is null and selected_residual_ex_vat is null)
        or (review_issue_code in ('CASE_RECONCILIATION_REQUIRED','CASE_CAPACITY_COMPARISON_REVIEW_REQUIRED') and comparable_paid_ex_vat is not null and comparable_held_ex_vat is not null
          and selected_residual_ex_vat is not null and selected_residual_ex_vat=approved_source_ex_vat-comparable_paid_ex_vat-comparable_held_ex_vat)))),
  check((receipt_kind in ('REVIEW','PENDING_REVIEW')
      and ((receipt_kind='REVIEW' and qualification_state='READY')
        or (receipt_kind='PENDING_REVIEW' and qualification_state='PENDING' and basis_review_id=id))
      and source_book_revision=source_book_revision_before and operation_job_id is null and operation_identity is null
      and operation_hold_id is null and operation_effect_id is null and operation_case_event_id is null
      and operation_case_instruction_id is null and operation_case_result_id is null and operation_case_hold_id is null and native_source_channel is null
      and native_delta_realised is null and native_delta_held is null and comparable_operation_ex_vat is null
      and local_work_id is null and local_component_key is null and local_scalar_before is null and local_scalar_after is null
      and (native_realised_paye,native_realised_umbrella,native_held_paye,native_held_umbrella)
        =(native_realised_paye_before,native_realised_umbrella_before,native_held_paye_before,native_held_umbrella_before))
    or (receipt_kind='CASE_RECONCILE' and previous_comparison_id is not null and source_book_revision=source_book_revision_before
      and operation_job_id is not null and operation_identity=work_collection_id and work_collection_id is not null
      and operation_hold_id is null and operation_effect_id is null and operation_case_instruction_id is null and operation_case_result_id is null
      and operation_case_hold_id is null
      and native_source_channel is null and native_delta_realised=0 and native_delta_held=0 and comparable_operation_ex_vat is null
      and local_work_id is null and local_component_key is null and local_scalar_before is null and local_scalar_after is null
      and (native_realised_paye,native_realised_umbrella,native_held_paye,native_held_umbrella)
        =(native_realised_paye_before,native_realised_umbrella_before,native_held_paye_before,native_held_umbrella_before))
    or (receipt_kind in ('CASE_RESERVE','CASE_RELEASE') and previous_comparison_id is not null and source_book_revision=source_book_revision_before
      and operation_job_id is not null and operation_case_hold_id is not null and operation_identity=operation_case_hold_id
      and operation_case_instruction_id is not null and operation_case_result_id is not null and operation_case_event_id is null
      and operation_hold_id is null and operation_effect_id is null and native_source_channel is not null and native_delta_realised=0 and native_delta_held=0
      and local_work_id is null and local_component_key is null and local_scalar_before is null and local_scalar_after is null
      and (native_realised_paye,native_realised_umbrella,native_held_paye,native_held_umbrella)
        =(native_realised_paye_before,native_realised_umbrella_before,native_held_paye_before,native_held_umbrella_before)
      and (comparable_paid_ex_vat,comparable_held_ex_vat)=(comparable_paid_ex_vat_before,comparable_held_ex_vat_before))
    or (receipt_kind not in ('REVIEW','PENDING_REVIEW','CASE_RECONCILE','CASE_RESERVE','CASE_RELEASE') and previous_comparison_id is not null and source_book_revision=source_book_revision_before+1
      and operation_job_id is not null and operation_identity is not null and native_source_channel is not null
      and native_delta_realised is not null and native_delta_held is not null and local_work_id is not null
      and local_component_key is not null and local_scalar_before is not null and local_scalar_after is not null and operation_case_hold_id is null)),
  check(receipt_kind not in ('RESERVE','RELEASE') or (operation_hold_id is not null and operation_identity=operation_hold_id
    and operation_effect_id is null and operation_case_event_id is null and operation_case_instruction_id is null and operation_case_result_id is null)),
  check(receipt_kind<>'SETTLE' or (operation_effect_id is not null and operation_identity=operation_effect_id and operation_hold_id is not null
    and operation_case_event_id is null and operation_case_instruction_id is null and operation_case_result_id is null)),
  check(receipt_kind<>'CASE_RECOVERY' or (operation_effect_id is not null and operation_identity=operation_effect_id and operation_hold_id is null
    and operation_case_event_id is not null and operation_case_instruction_id is not null and operation_case_result_id is not null)),
  check(target_pay_channel<>'PAYE' or umbrella_vat_chargeable=false)
);
create unique index bpay_next_comparison_operation_once_v2 on private.bpay_next_source_comparison_receipt(receipt_kind,operation_identity)
  where receipt_kind not in ('REVIEW','PENDING_REVIEW','CASE_RECONCILE');
create unique index bpay_next_comparison_case_reconcile_once_v2 on private.bpay_next_source_comparison_receipt(operation_job_id,work_collection_id,previous_comparison_id)
  where receipt_kind='CASE_RECONCILE';
create unique index bpay_next_comparison_source_review_once_v2 on private.bpay_next_source_comparison_receipt(review_certificate_id,source_binding_version_id)
  where receipt_kind in ('REVIEW','PENDING_REVIEW');

-- This is a frozen held-child certificate, not a mutable balance or atom ledger.
create table private.bpay_next_source_comparison_hold_mapping (
  basis_review_id uuid not null references private.bpay_next_source_comparison_receipt(id) on delete restrict,
  hold_id uuid not null references private.bpay_next_hold(id) on delete restrict,
  run_line_id uuid not null references private.bpay_next_run_line(id) on delete restrict,
  native_source_channel text not null check(native_source_channel in ('PAYE','UMBRELLA')),
  native_reserved_ex_vat private.bpay_next_penny_amount not null check(native_reserved_ex_vat>0),
  comparable_reserved_ex_vat private.bpay_next_penny_amount not null check(comparable_reserved_ex_vat>=0),
  created_by_comparison_id uuid not null references private.bpay_next_source_comparison_receipt(id) on delete restrict,
  mapping_sha256 bytea not null check(octet_length(mapping_sha256)=32),
  primary key(basis_review_id,hold_id)
);

-- Active Case capacity is NOT WORK H. Pin only its comparable recovery effect.
create table private.bpay_next_source_comparison_case_hold_mapping (
  basis_review_id uuid not null references private.bpay_next_source_comparison_receipt(id) on delete restrict,
  case_hold_id uuid not null references private.bpay_next_case_hold(id) on delete restrict,
  instruction_id uuid not null references private.bpay_next_run_case_instruction(id) on delete restrict,
  allocation_result_id uuid not null,
  native_target_channel text not null check(native_target_channel in ('PAYE','UMBRELLA')),
  native_target_ex_vat private.bpay_next_penny_amount not null check(native_target_ex_vat>0),
  comparable_recovery_ex_vat private.bpay_next_penny_amount not null check(comparable_recovery_ex_vat>=0),
  created_by_comparison_id uuid not null references private.bpay_next_source_comparison_receipt(id) on delete restrict,
  mapping_sha256 bytea not null check(octet_length(mapping_sha256)=32),
  primary key(basis_review_id,case_hold_id),
  unique(basis_review_id,case_hold_id,instruction_id,allocation_result_id,native_target_channel,native_target_ex_vat),
  foreign key(allocation_result_id,instruction_id) references private.bpay_next_case_allocation_result(id,instruction_id) on delete restrict
);

alter table private.bpay_next_position
  add column source_comparison_receipt_id uuid,
  add column source_comparison_revision bigint not null default 0,
  add constraint bpay_next_position_comparison_shape_v2 check((source_comparison_receipt_id is null and source_comparison_revision=0)
    or (source_comparison_receipt_id is not null and source_comparison_revision>0 and source_book_state='READY')),
  add constraint bpay_next_position_comparison_current_fk_v2 foreign key
    (source_comparison_receipt_id,work_id,component_key,source_comparison_revision)
    references private.bpay_next_source_comparison_receipt(id,canonical_work_id,canonical_component_key,comparison_revision) on delete restrict;

alter table private.bpay_next_run_position
  add column source_comparison_receipt_id uuid,
  add column comparable_paid_ex_vat private.bpay_next_penny_amount,
  add column comparable_held_ex_vat private.bpay_next_penny_amount,
  add column selected_residual_ex_vat private.bpay_next_penny_amount,
  add constraint bpay_next_run_position_comparison_shape_v2 check
    ((source_comparison_receipt_id is null and comparable_paid_ex_vat is null and comparable_held_ex_vat is null and selected_residual_ex_vat is null)
    or (source_comparison_receipt_id is not null and comparable_paid_ex_vat is not null and comparable_held_ex_vat is not null
      and comparable_held_ex_vat>=0 and selected_residual_ex_vat is not null and source_binding_version_id is not null)),
  add constraint bpay_next_run_position_comparison_fk_v2 foreign key(source_comparison_receipt_id)
    references private.bpay_next_source_comparison_receipt(id) on delete restrict;
-- Exact core1324 generated name/expression; fail closed if the real preimage differs.
do $residual_preimage$ begin
  if (select pg_get_constraintdef(c.oid) from pg_constraint c
      where c.conrelid='private.bpay_next_run_position'::regclass and c.conname='bpay_next_run_position_check')
    is distinct from 'CHECK ((residual_source_ex_vat = ((approved_source_ex_vat - realised_source_ex_vat) - held_source_ex_vat)))' then
    raise exception 'BPAY_NEXT_COMPARISON_RESIDUAL_CONSTRAINT_PREIMAGE_MISMATCH';end if;
end $residual_preimage$;
alter table private.bpay_next_run_position drop constraint bpay_next_run_position_check;
alter table private.bpay_next_run_position
  add constraint bpay_next_run_position_residual_v2 check
    ((source_comparison_receipt_id is null and residual_source_ex_vat=approved_source_ex_vat-realised_source_ex_vat-held_source_ex_vat)
      or (source_comparison_receipt_id is not null and residual_source_ex_vat=selected_residual_ex_vat
        and selected_residual_ex_vat=approved_source_ex_vat-comparable_paid_ex_vat-comparable_held_ex_vat)),
  add constraint bpay_next_run_position_exact_comparison_v2 foreign key(source_comparison_receipt_id,
    source_binding_version_id,canonical_source_work_id,canonical_source_component_key,source_book_revision,
    work_id,captured_revision_id,component_key,approved_source_ex_vat,comparable_paid_ex_vat,comparable_held_ex_vat,selected_residual_ex_vat)
    references private.bpay_next_source_comparison_receipt(id,source_binding_version_id,canonical_work_id,canonical_component_key,source_book_revision,
      approval_work_id,approval_revision_id,approval_component_key,approved_source_ex_vat,comparable_paid_ex_vat,comparable_held_ex_vat,selected_residual_ex_vat)
    on delete restrict;
alter table private.bpay_next_run_line
  add column source_comparison_receipt_id uuid references private.bpay_next_source_comparison_receipt(id) on delete restrict,
  add constraint bpay_next_run_line_comparison_shape_v2 check(source_comparison_receipt_id is null
    or (source_binding_version_id is not null and source_book_revision>0 and updates_original_source_scalar is not null)),
  add constraint bpay_next_run_line_comparison_exact_v2 foreign key(source_comparison_receipt_id,source_binding_version_id,
    canonical_source_work_id,canonical_source_component_key,source_book_revision,work_id,captured_revision_id,component_key,approved_line_id)
    references private.bpay_next_source_comparison_receipt(id,source_binding_version_id,canonical_work_id,canonical_component_key,
      source_book_revision,approval_work_id,approval_revision_id,approval_component_key,approved_line_id) on delete restrict,
  add constraint bpay_next_run_line_comparison_effect_uq_v2 unique(id,source_comparison_receipt_id,source_binding_version_id,
    canonical_source_work_id,canonical_source_component_key,source_pay_channel);

-- Conditional legacy collection FK: retain exact old scope whenever comparison is NULL.
-- Generated columns are not caller-writable and do not hold another balance.
do $collection_preimages$ begin
  if (select regexp_replace(pg_get_constraintdef(c.oid),'[[:space:]]','','g') from pg_constraint c
    where c.conrelid='private.bpay_next_run_position'::regclass and c.conname='bpay_next_position_collection_origin_fk')
    is distinct from 'FOREIGNKEY(work_collection_id,work_id,component_key)REFERENCESprivate.bpay_next_work_collection(id,work_id,component_key)' then
    raise exception 'BPAY_NEXT_COMPARISON_COLLECTION_FK_PREIMAGE_MISMATCH';end if;
  if (select regexp_replace(regexp_replace(pg_get_expr(c.conbin,c.conrelid),'::(numeric|bigint|integer|text)','','g'),'[[:space:]()]','','g') from pg_constraint c
    where c.conrelid='private.bpay_next_run_position'::regclass and c.conname='bpay_next_position_collection_shape_check')
    is distinct from 'work_collection_idISNULLANDNOTmanaged_capture_completedORwork_collection_idISNOTNULLANDwork_collection_revision>0ANDNOTneeds_financial_resolutionANDresidual_source_ex_vat<0' then
    raise exception 'BPAY_NEXT_COMPARISON_COLLECTION_SHAPE_PREIMAGE_MISMATCH';end if;
  if (select regexp_replace(regexp_replace(pg_get_expr(c.conbin,c.conrelid),'::(numeric|bigint|integer|text)','','g'),'[[:space:]()]','','g') from pg_constraint c
    where c.conrelid='private.bpay_next_run_position'::regclass and c.conname='bpay_next_position_line_or_managed_check')
    is distinct from 'approved_line_idISNOTNULLORneeds_financial_resolutionORwork_collection_idISNOTNULL' then
    raise exception 'BPAY_NEXT_COMPARISON_COLLECTION_LINE_PREIMAGE_MISMATCH';end if;
end $collection_preimages$;
alter table private.bpay_next_run_position
  add column legacy_collection_work_id uuid generated always as (case when source_comparison_receipt_id is null then work_id end) stored,
  add column legacy_collection_component_key text generated always as (case when source_comparison_receipt_id is null then component_key end) stored,
  add column comparison_approval_state text generated always as (case when source_comparison_receipt_id is null then null
    when approved_line_id is null then 'REMOVED' else 'PRESENT' end) stored,
  drop constraint bpay_next_position_collection_origin_fk,
  add constraint bpay_next_position_collection_origin_fk foreign key(work_collection_id,legacy_collection_work_id,legacy_collection_component_key)
    references private.bpay_next_work_collection(id,work_id,component_key),
  add constraint bpay_next_position_collection_canonical_v2 foreign key(work_collection_id,canonical_source_work_id,canonical_source_component_key)
    references private.bpay_next_work_collection(id,work_id,component_key),
  add constraint bpay_next_position_collection_comparison_v2 foreign key(source_comparison_receipt_id,work_collection_id,
    canonical_source_work_id,canonical_source_component_key)
    references private.bpay_next_source_comparison_receipt(id,work_collection_id,canonical_work_id,canonical_component_key),
  add constraint bpay_next_position_comparison_state_v2 foreign key(source_comparison_receipt_id,source_binding_version_id,
    canonical_source_work_id,canonical_source_component_key,comparison_approval_state)
    references private.bpay_next_source_comparison_receipt(id,source_binding_version_id,canonical_work_id,canonical_component_key,approval_component_state),
  drop constraint bpay_next_position_collection_shape_check,
  add constraint bpay_next_position_collection_shape_check check
    ((work_collection_id is null and not managed_capture_completed)
      or (work_collection_id is not null and work_collection_revision>0 and not needs_financial_resolution and residual_source_ex_vat<0)
      or (source_comparison_receipt_id is not null and comparison_approval_state='REMOVED' and work_collection_id is null
        and managed_capture_completed and not needs_financial_resolution and approved_source_ex_vat=0
        and comparable_paid_ex_vat=0 and comparable_held_ex_vat=0 and selected_residual_ex_vat=0 and residual_source_ex_vat=0)),
  drop constraint bpay_next_position_line_or_managed_check,
  add constraint bpay_next_position_line_or_managed_check check
    (approved_line_id is not null or needs_financial_resolution or work_collection_id is not null
      or (source_comparison_receipt_id is not null and comparison_approval_state='REMOVED' and approved_source_ex_vat=0
        and comparable_paid_ex_vat=0 and comparable_held_ex_vat=0 and selected_residual_ex_vat=0 and residual_source_ex_vat=0));

-- INPUT comparison is frozen on the actual Case instruction. No future output FK.
alter table private.bpay_next_run_case_instruction
  add column source_comparison_receipt_id uuid,
  add column source_binding_version_id uuid,
  add column canonical_source_work_id uuid,
  add column canonical_source_component_key text,
  add column source_approval_work_id uuid,
  add column source_approval_revision_id uuid,
  add column source_approval_component_key text,
  add column source_approval_state text,
  add column source_approved_line_id uuid,
  add constraint bpay_next_case_instruction_comparison_shape_v2 check
    ((source_comparison_receipt_id is null and source_binding_version_id is null and canonical_source_work_id is null
      and canonical_source_component_key is null and source_approval_work_id is null and source_approval_revision_id is null
      and source_approval_component_key is null and source_approval_state is null and source_approved_line_id is null)
    or (source_comparison_receipt_id is not null and source_binding_version_id is not null and canonical_source_work_id is not null
      and canonical_source_component_key is not null and source_approval_work_id is not null and source_approval_revision_id is not null
      and source_approval_component_key is not null and source_approval_state is not null and instruction_kind='RECOVERY'
      and ((source_approval_state='PRESENT' and source_approved_line_id is not null)
        or (source_approval_state='REMOVED' and source_approved_line_id is null)))),
  add constraint bpay_next_case_instruction_comparison_fk_v2 foreign key(source_comparison_receipt_id,
    source_binding_version_id,canonical_source_work_id,canonical_source_component_key)
    references private.bpay_next_source_comparison_receipt(id,source_binding_version_id,canonical_work_id,canonical_component_key)
    on delete restrict,
  add constraint bpay_next_case_instruction_approval_fk_v2 foreign key(source_binding_version_id,
    source_approval_work_id,source_approval_revision_id,source_approval_component_key)
    references private.bpay_next_source_component_binding_version(id,approval_work_id,approval_revision_id,approval_component_key) on delete restrict,
  add constraint bpay_next_case_instruction_approval_state_v2 foreign key(source_binding_version_id,source_approval_state)
    references private.bpay_next_source_component_binding_version(id,component_state) on delete restrict,
  add constraint bpay_next_case_instruction_line_fk_v2 foreign key(source_approval_revision_id,source_approved_line_id,source_approval_component_key)
    references private.bpay_next_approved_line(revision_id,id,component_key) on delete restrict,
  add constraint bpay_next_case_instruction_effect_source_uq_v2 unique(id,source_comparison_receipt_id,
    source_binding_version_id,canonical_source_work_id,canonical_source_component_key,target_pay_channel,
      source_approval_work_id,source_approval_component_key);

do $instruction_preimage$ begin
  if (select regexp_replace(regexp_replace(pg_get_expr(c.conbin,c.conrelid),'::(numeric|bigint|integer|text)','','g'),'[[:space:]()]','','g') from pg_constraint c
    where c.conrelid='private.bpay_next_run_case_instruction'::regclass and c.conname='bpay_next_instruction_collection_shape_check')
    is distinct from 'work_collection_idISNULLORcase_kind=''OVERPAYMENT''ANDtax_treatment=''TAXABLE''ANDsource_pay_channel=''PAYE''ANDtarget_pay_channel=''PAYE''ANDinstruction_kind=''RECOVERY''ANDdirection=''DEDUCTION''ANDpayroll_stage=''GROSS_DEDUCT''ANDhold_purpose=''GROSS_RECOVERY''' then
    raise exception 'BPAY_NEXT_COMPARISON_INSTRUCTION_COLLECTION_PREIMAGE_MISMATCH';end if;
end $instruction_preimage$;
alter table private.bpay_next_run_case_instruction drop constraint bpay_next_instruction_collection_shape_check;
alter table private.bpay_next_run_case_instruction add constraint bpay_next_instruction_collection_shape_check check
  (work_collection_id is null or
    (case_kind='OVERPAYMENT' and tax_treatment='TAXABLE' and instruction_kind='RECOVERY' and direction='DEDUCTION'
      and payroll_stage='GROSS_DEDUCT' and hold_purpose='GROSS_RECOVERY'
      and ((source_comparison_receipt_id is null and source_pay_channel='PAYE' and target_pay_channel='PAYE')
        or (source_comparison_receipt_id is not null and source_pay_channel in ('PAYE','UMBRELLA') and target_pay_channel in ('PAYE','UMBRELLA')))));
alter table private.bpay_next_case_component add constraint bpay_next_case_component_channel_identity_v2
  unique(id,case_id,candidate_id,source_pay_channel);
alter table private.bpay_next_run_case_instruction add constraint bpay_next_case_instruction_counter_channel_v2
  foreign key(case_component_id,case_id,candidate_id,source_pay_channel)
  references private.bpay_next_case_component(id,case_id,candidate_id,source_pay_channel) on delete restrict;

-- Preserve 1245 WORK-line FK verbatim. Null WORK provenance only in a closed Case branch.
alter table private.bpay_next_financial_effect
  add column source_comparison_receipt_id uuid,
  add column source_case_instruction_id uuid,
  add column source_case_result_id uuid,
  add column source_comparison_basis_review_id uuid,
  add column source_comparison_case_hold_id uuid,
  add column comparison_instruction_receipt_id uuid generated always as
    (case when source_comparison_case_hold_id is null then source_comparison_receipt_id end) stored,
  add column source_case_native_ex_vat numeric generated always as (-target_amount_ex_vat) stored,
  drop constraint bpay_next_effect_bound_shape,
  add constraint bpay_next_effect_bound_shape check
    ((source_run_line_id is null and source_binding_version_id is null and canonical_source_work_id is null
      and canonical_source_component_key is null and native_source_channel is null and source_case_event_id is null
      and source_comparison_receipt_id is null and source_case_instruction_id is null and source_case_result_id is null
      and source_comparison_basis_review_id is null and source_comparison_case_hold_id is null)
    or (source_run_line_id is not null and source_binding_version_id is not null and canonical_source_work_id is not null
      and canonical_source_component_key is not null and native_source_channel is not null and native_source_channel in ('PAYE','UMBRELLA')
      and (source_case_event_id is null or case_id is not null) and source_case_instruction_id is null and source_case_result_id is null
      and source_comparison_basis_review_id is null and source_comparison_case_hold_id is null)
    or (source_run_line_id is null and source_binding_version_id is not null and canonical_source_work_id is not null
      and canonical_source_component_key is not null and native_source_channel is not null and native_source_channel in ('PAYE','UMBRELLA')
      and source_case_event_id is not null and source_comparison_receipt_id is not null and source_case_instruction_id is not null
      and source_case_result_id is not null and case_id is not null and effect_kind='WORK_COLLECTION_RECOVERED'
      and source_disposition_ex_vat<0
      and ((source_comparison_basis_review_id is null and source_comparison_case_hold_id is null)
        or (source_comparison_basis_review_id is not null and source_comparison_case_hold_id is not null)))),
  add constraint bpay_next_effect_comparison_fk_v2 foreign key(source_comparison_receipt_id,
    source_binding_version_id,canonical_source_work_id,canonical_source_component_key)
    references private.bpay_next_source_comparison_receipt(id,source_binding_version_id,canonical_work_id,canonical_component_key) on delete restrict,
  add constraint bpay_next_effect_work_comparison_line_v2 foreign key(source_run_line_id,source_comparison_receipt_id,
    source_binding_version_id,canonical_source_work_id,canonical_source_component_key,native_source_channel)
    references private.bpay_next_run_line(id,source_comparison_receipt_id,source_binding_version_id,
      canonical_source_work_id,canonical_source_component_key,source_pay_channel) on delete restrict,
  add constraint bpay_next_effect_case_instruction_v2 foreign key(source_case_instruction_id,comparison_instruction_receipt_id,
    source_binding_version_id,canonical_source_work_id,canonical_source_component_key,native_source_channel,work_id,component_key)
    references private.bpay_next_run_case_instruction(id,source_comparison_receipt_id,
      source_binding_version_id,canonical_source_work_id,canonical_source_component_key,target_pay_channel,
      source_approval_work_id,source_approval_component_key) on delete restrict,
  add constraint bpay_next_effect_case_result_v2 foreign key(source_case_result_id,source_case_instruction_id)
    references private.bpay_next_case_allocation_result(id,instruction_id) on delete restrict,
  add constraint bpay_next_effect_retained_case_hold_v2 foreign key(source_comparison_basis_review_id,source_comparison_case_hold_id,
    source_case_instruction_id,source_case_result_id,native_source_channel,source_case_native_ex_vat)
    references private.bpay_next_source_comparison_case_hold_mapping(basis_review_id,case_hold_id,instruction_id,
      allocation_result_id,native_target_channel,native_target_ex_vat) on delete restrict;

alter table private.bpay_next_case_event add constraint bpay_next_case_event_exact_result_uq_v2
  unique(id,case_id,instruction_id,allocation_result_id);
alter table private.bpay_next_financial_effect add constraint bpay_next_effect_exact_case_event_v2
  foreign key(source_case_event_id,case_id,source_case_instruction_id,source_case_result_id)
  references private.bpay_next_case_event(id,case_id,instruction_id,allocation_result_id) on delete restrict;

alter table private.bpay_next_source_comparison_receipt owner to postgres;
alter table private.bpay_next_source_comparison_hold_mapping owner to postgres;
alter table private.bpay_next_source_comparison_case_hold_mapping owner to postgres;
alter table private.bpay_next_source_comparison_receipt enable row level security;
alter table private.bpay_next_source_comparison_receipt force row level security;
alter table private.bpay_next_source_comparison_hold_mapping enable row level security;
alter table private.bpay_next_source_comparison_hold_mapping force row level security;
alter table private.bpay_next_source_comparison_case_hold_mapping enable row level security;
alter table private.bpay_next_source_comparison_case_hold_mapping force row level security;
revoke all on private.bpay_next_source_comparison_receipt,private.bpay_next_source_comparison_hold_mapping,
  private.bpay_next_source_comparison_case_hold_mapping from public,anon,authenticated,service_role;
do $owner_policy$ begin
  execute format('create policy bpay_next_comparison_owner_v2 on private.bpay_next_source_comparison_receipt for all to %I using(true) with check(true)',current_user);
  execute format('create policy bpay_next_comparison_hold_owner_v2 on private.bpay_next_source_comparison_hold_mapping for all to %I using(true) with check(true)',current_user);
  execute format('create policy bpay_next_comparison_case_hold_owner_v2 on private.bpay_next_source_comparison_case_hold_mapping for all to %I using(true) with check(true)',current_user);
end $owner_policy$;
commit;
