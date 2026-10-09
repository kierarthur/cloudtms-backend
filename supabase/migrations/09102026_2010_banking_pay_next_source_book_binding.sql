-- F-06 re-scaffold of reviewed staged 06102026_1245_banking_pay_next_source_book_binding.sql; additive local source only.
-- Additive SOURCE-economic book and receipt-derived direct identity only.
-- No backfill, history aggregation, activation, financial operation or selector.
-- Existing children/scalars and their source/target amounts remain unchanged.

\set ON_ERROR_STOP on

begin;

alter table private.bpay_next_financial_effect
  add constraint bpay_next_effect_source_book_identity_uq unique (id,work_id,component_key);
alter table private.bpay_next_hold
  add constraint bpay_next_hold_source_book_identity_uq unique (id,work_id,component_key);
alter table private.bpay_next_case_event
  add constraint bpay_next_case_event_source_book_identity_uq unique (id,case_id);
alter table private.bpay_next_approved_line
  add constraint bpay_next_line_source_component_uq unique (revision_id,id,component_key,source_component_id);
alter table private.bpay_next_publication
  add constraint bpay_next_publication_source_binding_uq unique (command_id,work_id,revision_id,candidate_id);
alter table private.weekly_source_entitlement_publication_receipts
  add constraint bpay_next_receipt_source_binding_uq
  unique (id,candidate_id,decision_bundle_id,bundle_revision,request_digest);

alter table private.bpay_next_position
  add column source_book_state text not null default 'UNBOUND',
  add column source_book_revision bigint not null default 0,
  add column native_realised_paye private.bpay_next_penny_amount,
  add column native_realised_umbrella private.bpay_next_penny_amount,
  add column native_held_paye private.bpay_next_penny_amount,
  add column native_held_umbrella private.bpay_next_penny_amount,
  add column source_book_seed_kind text,
  add column source_book_seed_applied_revision_id uuid,
  add column source_book_seed_revision_id uuid,
  add column source_book_seed_line_id uuid,
  add column source_book_seed_effect_id uuid,
  add column source_book_seed_hold_id uuid,
  add column source_book_seed_channel text,
  add column source_book_seed_digest bytea,
  add constraint bpay_next_position_source_book_shape check (
    (source_book_state='UNBOUND' and source_book_revision=0
      and native_realised_paye is null and native_realised_umbrella is null
      and native_held_paye is null and native_held_umbrella is null
      and source_book_seed_kind is null and source_book_seed_applied_revision_id is null
      and source_book_seed_revision_id is null and source_book_seed_line_id is null
      and source_book_seed_effect_id is null and source_book_seed_hold_id is null
      and source_book_seed_channel is null and source_book_seed_digest is null)
    or (source_book_state='READY' and source_book_revision>=1
      and native_realised_paye is not null and native_realised_umbrella is not null
      and native_held_paye is not null and native_held_umbrella is not null
      and pg_catalog.abs(native_realised_paye)<10000000000000000
      and pg_catalog.abs(native_realised_umbrella)<10000000000000000
      and native_held_paye between 0 and 9999999999999999.99
      and native_held_umbrella between 0 and 9999999999999999.99
      and source_book_seed_kind is not null and source_book_seed_kind in ('ZERO','ORIGINAL_POSITION')
      and source_book_seed_applied_revision_id is not null and source_book_seed_revision_id is not null
      and source_book_seed_channel is not null and source_book_seed_channel in ('PAYE','UMBRELLA')
      and source_book_seed_digest is not null and pg_catalog.octet_length(source_book_seed_digest)=32
      and (source_book_seed_kind<>'ZERO' or (source_book_seed_effect_id is null
        and source_book_seed_hold_id is null)))) ,
  add constraint bpay_next_position_seed_applied_fk foreign key (work_id,source_book_seed_applied_revision_id)
    references private.bpay_next_work_revision(work_id,id) on delete restrict,
  add constraint bpay_next_position_seed_revision_fk foreign key (work_id,source_book_seed_revision_id)
    references private.bpay_next_work_revision(work_id,id) on delete restrict,
  add constraint bpay_next_position_seed_line_fk foreign key (source_book_seed_revision_id,source_book_seed_line_id,component_key)
    references private.bpay_next_approved_line(revision_id,id,component_key) on delete restrict,
  add constraint bpay_next_position_seed_effect_fk foreign key (source_book_seed_effect_id,work_id,component_key)
    references private.bpay_next_financial_effect(id,work_id,component_key) on delete restrict,
  add constraint bpay_next_position_seed_hold_fk foreign key (source_book_seed_hold_id,work_id,component_key)
    references private.bpay_next_hold(id,work_id,component_key) on delete restrict;

-- Metadata may exist BEFORE async POSITION_APPLY. The immutable original
-- approved-line witness is the anchor; only a READY snapshot needs a position.
create table private.bpay_next_source_component_binding (
  canonical_work_id uuid not null,
  canonical_component_key text not null,
  candidate_id uuid not null,
  source_component_id uuid not null,
  canonical_revision_id uuid not null,
  canonical_line_id uuid not null,
  current_binding_version_id uuid not null,
  binding_revision bigint not null check (binding_revision>0),
  primary key (canonical_work_id,canonical_component_key),
  unique (candidate_id,source_component_id),
  unique (canonical_work_id,canonical_component_key,candidate_id,source_component_id),
  check (canonical_component_key='SOURCE:'||source_component_id::text),
  foreign key (canonical_work_id,candidate_id)
    references private.bpay_next_work(id,candidate_id) on delete restrict,
  foreign key (canonical_work_id,canonical_revision_id)
    references private.bpay_next_work_revision(work_id,id) on delete restrict,
  foreign key (canonical_revision_id,canonical_line_id,canonical_component_key,source_component_id)
    references private.bpay_next_approved_line(revision_id,id,component_key,source_component_id) on delete restrict
);

create table private.bpay_next_source_component_binding_version (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  canonical_work_id uuid not null,
  canonical_component_key text not null,
  candidate_id uuid not null,
  source_component_id uuid not null,
  binding_revision bigint not null check (binding_revision>0),
  source_receipt_id uuid not null,
  decision_bundle_id uuid not null,
  bundle_revision bigint not null,
  request_digest bytea not null check (pg_catalog.octet_length(request_digest)=32),
  approval_work_id uuid not null,
  approval_revision_id uuid not null,
  approval_component_key text not null,
  approved_line_id uuid,
  head_id uuid not null,
  component_state text not null check (component_state in ('PRESENT','REMOVED')),
  approval_source_channel text not null check (approval_source_channel in ('PAYE','UMBRELLA')),
  tax_treatment text check (tax_treatment in ('TAXABLE','NON_TAXABLE','NOT_APPLICABLE')),
  classification_line_id uuid,
  component_sha256 bytea,
  chosen_detail_sha256 bytea,
  publication_command_id_1 uuid not null,
  publication_work_id_1 uuid not null,
  publication_revision_id_1 uuid not null,
  publication_command_id_2 uuid,
  publication_work_id_2 uuid,
  publication_revision_id_2 uuid,
  selection_state text not null default 'UNBOUND' check (selection_state='UNBOUND'),
  binding_digest bytea not null check (pg_catalog.octet_length(binding_digest)=32),
  unique (canonical_work_id,canonical_component_key,binding_revision),
  unique (source_receipt_id,source_component_id),
  unique (id,canonical_work_id,canonical_component_key,candidate_id,binding_revision),
  unique (id,canonical_work_id,canonical_component_key),
  unique (id,approval_work_id,approval_revision_id,approval_component_key),
  foreign key (canonical_work_id,canonical_component_key,candidate_id,source_component_id)
    references private.bpay_next_source_component_binding(canonical_work_id,canonical_component_key,candidate_id,source_component_id)
    on delete restrict deferrable initially deferred,
  foreign key (source_receipt_id,candidate_id,decision_bundle_id,bundle_revision,request_digest)
    references private.weekly_source_entitlement_publication_receipts(id,candidate_id,decision_bundle_id,bundle_revision,request_digest)
    on delete restrict,
  foreign key (approval_work_id,candidate_id) references private.bpay_next_work(id,candidate_id) on delete restrict,
  foreign key (approval_work_id,approval_revision_id) references private.bpay_next_work_revision(work_id,id) on delete restrict,
  foreign key (approval_revision_id,approved_line_id,approval_component_key,source_component_id)
    references private.bpay_next_approved_line(revision_id,id,component_key,source_component_id) on delete restrict,
  foreign key (classification_line_id) references private.bpay_next_approved_line(id) on delete restrict,
  foreign key (head_id,decision_bundle_id,bundle_revision)
    references public.weekly_source_entitlement_heads(id,decision_bundle_id,bundle_revision) on delete restrict,
  foreign key (publication_command_id_1,publication_work_id_1,publication_revision_id_1,candidate_id)
    references private.bpay_next_publication(command_id,work_id,revision_id,candidate_id) on delete restrict,
  foreign key (publication_command_id_2,publication_work_id_2,publication_revision_id_2,candidate_id)
    references private.bpay_next_publication(command_id,work_id,revision_id,candidate_id) on delete restrict,
  check (approval_component_key=canonical_component_key),
  check ((component_state='PRESENT')=(approved_line_id is not null)),
  check ((component_state='PRESENT')=(component_sha256 is not null)),
  check (component_sha256 is null or pg_catalog.octet_length(component_sha256)=32),
  check (chosen_detail_sha256 is null or pg_catalog.octet_length(chosen_detail_sha256)=32),
  check ((publication_command_id_2 is null and publication_work_id_2 is null and publication_revision_id_2 is null)
    or (publication_command_id_2 is not null and publication_work_id_2 is not null and publication_revision_id_2 is not null
      and publication_command_id_2<>publication_command_id_1 and publication_work_id_2<>publication_work_id_1))
);
alter table private.bpay_next_source_component_binding
  add constraint bpay_next_binding_current_version_fk
  foreign key (current_binding_version_id,canonical_work_id,canonical_component_key,candidate_id,binding_revision)
    references private.bpay_next_source_component_binding_version(id,canonical_work_id,canonical_component_key,candidate_id,binding_revision)
    on delete restrict deferrable initially deferred;
create index bpay_next_binding_version_approval_idx
  on private.bpay_next_source_component_binding_version(approval_work_id,approval_revision_id,id);
create index bpay_next_binding_version_receipt_idx
  on private.bpay_next_source_component_binding_version(source_receipt_id,source_component_id);

-- Nullable additive provenance for future connected capture/posting owners.
-- No existing line/effect is updated. These are identities, never caller money.
alter table private.bpay_next_run_line
  add column source_binding_version_id uuid,
  add column canonical_source_work_id uuid,
  add column canonical_source_component_key text,
  add column source_book_revision bigint,
  add column updates_original_source_scalar boolean,
  add constraint bpay_next_run_line_bound_shape check (
    (source_binding_version_id is null and canonical_source_work_id is null
      and canonical_source_component_key is null and source_book_revision is null
      and updates_original_source_scalar is null)
    or (source_binding_version_id is not null and canonical_source_work_id is not null
      and canonical_source_component_key is not null and source_book_revision is not null
      and source_book_revision>0 and updates_original_source_scalar is not null)),
  add constraint bpay_next_run_line_binding_fk foreign key (source_binding_version_id,canonical_source_work_id,canonical_source_component_key)
    references private.bpay_next_source_component_binding_version(id,canonical_work_id,canonical_component_key) on delete restrict,
  add constraint bpay_next_run_line_binding_approval_fk foreign key (source_binding_version_id,work_id,captured_revision_id,component_key)
    references private.bpay_next_source_component_binding_version(id,approval_work_id,approval_revision_id,approval_component_key) on delete restrict,
  add constraint bpay_next_run_line_canonical_position_fk foreign key (canonical_source_work_id,canonical_source_component_key)
    references private.bpay_next_position(work_id,component_key) on delete restrict,
  add constraint bpay_next_run_line_effect_binding_uq unique (id,source_binding_version_id,canonical_source_work_id,canonical_source_component_key,source_pay_channel);
alter table private.bpay_next_financial_effect
  add column source_run_line_id uuid,
  add column source_binding_version_id uuid,
  add column canonical_source_work_id uuid,
  add column canonical_source_component_key text,
  add column native_source_channel text,
  add column source_case_event_id uuid,
  add constraint bpay_next_effect_bound_shape check (
    (source_run_line_id is null and source_binding_version_id is null and canonical_source_work_id is null
      and canonical_source_component_key is null and native_source_channel is null and source_case_event_id is null)
    or (source_run_line_id is not null and source_binding_version_id is not null and canonical_source_work_id is not null
      and canonical_source_component_key is not null and native_source_channel is not null
      and native_source_channel in ('PAYE','UMBRELLA')
      and (source_case_event_id is null or case_id is not null))),
  add constraint bpay_next_effect_source_case_event_fk foreign key (source_case_event_id,case_id)
    references private.bpay_next_case_event(id,case_id) on delete restrict,
  add constraint bpay_next_effect_bound_line_fk foreign key
    (source_run_line_id,source_binding_version_id,canonical_source_work_id,canonical_source_component_key,native_source_channel)
    references private.bpay_next_run_line(id,source_binding_version_id,canonical_source_work_id,canonical_source_component_key,source_pay_channel)
    on delete restrict;

alter table private.bpay_next_source_component_binding owner to postgres;
alter table private.bpay_next_source_component_binding_version owner to postgres;
alter table private.bpay_next_source_component_binding enable row level security;
alter table private.bpay_next_source_component_binding force row level security;
alter table private.bpay_next_source_component_binding_version enable row level security;
alter table private.bpay_next_source_component_binding_version force row level security;
revoke all on private.bpay_next_source_component_binding,
  private.bpay_next_source_component_binding_version from public,anon,authenticated,service_role;
do $source_binding_policies$
begin
  execute pg_catalog.format('create policy bpay_next_source_binding_owner_all on private.bpay_next_source_component_binding for all to %I using (true) with check (true)',current_user);
  execute pg_catalog.format('create policy bpay_next_source_binding_version_owner_all on private.bpay_next_source_component_binding_version for all to %I using (true) with check (true)',current_user);
end
$source_binding_policies$;

-- E-only proposed nullable capture provenance. No existing child is changed.


alter table private.bpay_next_run_position
  add column source_binding_version_id uuid,
  add column canonical_source_work_id uuid,
  add column canonical_source_component_key text,
  add column source_book_revision bigint,
  add constraint bpay_next_run_position_source_bound_shape check (
    (source_binding_version_id is null and canonical_source_work_id is null
      and canonical_source_component_key is null and source_book_revision is null)
    or (source_binding_version_id is not null and canonical_source_work_id is not null
      and canonical_source_component_key is not null and source_book_revision is not null
      and source_book_revision>0)),
  add constraint bpay_next_run_position_source_binding_fk
    foreign key (source_binding_version_id,canonical_source_work_id,canonical_source_component_key)
    references private.bpay_next_source_component_binding_version(id,canonical_work_id,canonical_component_key)
    on delete restrict,
  add constraint bpay_next_run_position_source_approval_fk
    foreign key (source_binding_version_id,work_id,captured_revision_id,component_key)
    references private.bpay_next_source_component_binding_version(id,approval_work_id,approval_revision_id,approval_component_key)
    on delete restrict,
  add constraint bpay_next_run_position_source_position_fk
    foreign key (canonical_source_work_id,canonical_source_component_key)
    references private.bpay_next_position(work_id,component_key) on delete restrict;



commit;
