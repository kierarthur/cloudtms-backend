-- One maintained collection owner per original ordinary WORK component.
-- No backfill, fabricated CASE_CREATE request, schedule, recovery or write-off.
-- Original payroll SOURCE disposition is not reversed by a bank cash return.

\set ON_ERROR_STOP on

begin;

create table private.bpay_next_work_collection (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  work_id uuid not null,
  component_key text not null check (pg_catalog.octet_length(component_key) between 1 and 256),
  candidate_id uuid not null,
  original_effect_id uuid not null unique references private.bpay_next_financial_effect(id) on delete restrict,
  original_run_line_id uuid not null references private.bpay_next_run_line(id) on delete restrict,
  original_revision_id uuid not null,
  original_approved_line_id uuid not null,
  original_transfer_id uuid not null references private.bpay_next_transfer(id) on delete restrict,
  original_source_pay_channel text not null check (original_source_pay_channel='PAYE'),
  -- NULL/unknown original classification is retained, never read as non-taxable.
  original_tax_treatment text,
  paid_basis_qualified boolean not null,
  original_valuation_policy_id uuid not null references private.bpay_next_valuation_policy(id) on delete restrict,
  case_id uuid unique,
  case_component_id uuid unique,
  qualification_state text not null check (qualification_state in
    ('READY','ORIGINAL_TAX_UNBOUND','PAID_BASIS_UNBOUND','CURRENT_APPROVAL_UNBOUND','CURRENT_SOURCE_UNBOUND','SIGNED_SOURCE_UNBOUND')),
  observed_revision_id uuid not null,
  observed_approved_source_ex_vat private.bpay_next_penny_amount not null,
  observed_realised_source_ex_vat private.bpay_next_penny_amount not null,
  reconcile_revision bigint not null default 1 check (reconcile_revision>0),
  last_job_id uuid not null references private.bpay_next_job(id) on delete restrict,
  last_effect_id uuid references private.bpay_next_financial_effect(id) on delete restrict,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (work_id,component_key),
  unique (id,candidate_id),
  foreign key (work_id,component_key) references private.bpay_next_position(work_id,component_key) on delete restrict,
  foreign key (work_id,candidate_id) references private.bpay_next_work(id,candidate_id) on delete restrict,
  foreign key (work_id,original_revision_id) references private.bpay_next_work_revision(work_id,id) on delete restrict,
  foreign key (original_revision_id,original_approved_line_id,component_key)
    references private.bpay_next_approved_line(revision_id,id,component_key) on delete restrict,
  foreign key (work_id,observed_revision_id) references private.bpay_next_work_revision(work_id,id) on delete restrict,
  foreign key (case_id,candidate_id) references private.bpay_next_finance_case(id,candidate_id) on delete restrict,
  foreign key (case_component_id,case_id,candidate_id)
    references private.bpay_next_case_component(id,case_id,candidate_id) on delete restrict,
  check ((case_id is null)=(case_component_id is null)),
  check (case_id is null or original_tax_treatment is not distinct from 'TAXABLE'),
  check (not paid_basis_qualified or original_tax_treatment is not distinct from 'TAXABLE'),
  check (qualification_state<>'READY' or paid_basis_qualified),
  check (pg_catalog.isfinite(created_at_utc) and pg_catalog.isfinite(updated_at_utc))
);
create index bpay_next_work_collection_candidate_idx
  on private.bpay_next_work_collection(candidate_id,work_id,component_key);
alter table private.bpay_next_work_collection enable row level security;
-- The managed installer maps only the audited logical owner. Do not SET ROLE
-- postgres or grant a browser/service role access to this private authority.
alter table private.bpay_next_work_collection owner to postgres;
revoke all on private.bpay_next_work_collection from public,anon,authenticated,service_role;

commit;
