-- DR-16: one maintained Run/channel summary shared with DR-27.
-- Additive, owner-only. No history backfill, allocation or runtime switch.

\set ON_ERROR_STOP on

begin;

create table private.bpay_next_run_channel_totals (
  run_id uuid not null references private.bpay_next_pay_run(id) on delete restrict,
  pay_channel text not null check (pay_channel in ('PAYE','UMBRELLA')),
  candidate_count bigint not null default 0 check (candidate_count>=0),
  ready_count bigint not null default 0 check (ready_count between 0 and candidate_count),
  gross_ex_vat numeric(38,2) not null default 0,
  gross_vat numeric(38,2) not null default 0,
  gross_inc_vat numeric(38,2) not null default 0,
  gross_stage_deductions numeric(38,2) not null default 0 check (gross_stage_deductions>=0),
  net_recovery_reserved numeric(38,2) not null default 0 check (net_recovery_reserved>=0),
  cash_known numeric(38,2) not null default 0 check (cash_known>=0),
  net_missing_count bigint not null default 0 check (net_missing_count between 0 and candidate_count),
  applicable_net_missing_count bigint not null default 0 check (applicable_net_missing_count between 0 and net_missing_count),
  net_replacement_pending_count bigint not null default 0 check (net_replacement_pending_count between 0 and candidate_count),
  net_refused_code text check (pg_catalog.octet_length(net_refused_code) between 1 and 256),
  totals_revision bigint not null default 0 check (totals_revision>=0),
  updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  primary key (run_id,pay_channel),
  check (gross_inc_vat=gross_ex_vat+gross_vat)
);
alter table private.bpay_next_run_channel_totals enable row level security;
alter table private.bpay_next_run_channel_totals owner to postgres;
revoke all on private.bpay_next_run_channel_totals from public,anon,authenticated,service_role;

-- A deterministic current refusal seek, rather than a Run/history scan.
create index bpay_next_worker_review_net_refusal_idx
  on private.bpay_next_run_worker(run_id,target_pay_channel,review_issue_code collate "C",id)
  where status='REVIEW' and net_request_revision>net_projection_revision
    and review_issue_code is not null;

-- Exact bounded Candidate review sorts are served by the original reader.
create index bpay_next_worker_review_channel_idx
  on private.bpay_next_run_worker(run_id,target_pay_channel,id);
create index bpay_next_worker_review_gross_idx
  on private.bpay_next_run_worker(run_id,gross_ex_vat,id);
create index bpay_next_worker_review_state_idx
  on private.bpay_next_run_worker(run_id,status,id);

-- Required full outstanding is not derivable from weekly nominal/capacity.
-- Nullable for immutable historical instructions; no fabricated backfill.
alter table private.bpay_next_run_case_instruction
  add column captured_component_outstanding_source_ex_vat private.bpay_next_penny_amount
    check (captured_component_outstanding_source_ex_vat>=0),
  add column captured_case_outstanding_source_ex_vat private.bpay_next_penny_amount
    check (captured_case_outstanding_source_ex_vat>=0),
  add constraint bpay_next_instruction_review_outstanding_pair check
    ((captured_component_outstanding_source_ex_vat is null)=(captured_case_outstanding_source_ex_vat is null));

-- Excluded components have no financial instruction. Preserve an honest
-- one-time observation on their existing immutable membership, not a new
-- monetary authority. Original accepted Rule/control choice remains in LEAF.
alter table private.bpay_next_case_selection_item
  add column review_observed_outstanding_source_ex_vat private.bpay_next_penny_amount
    check (review_observed_outstanding_source_ex_vat>=0),
  add column review_observed_due_source_ex_vat private.bpay_next_penny_amount
    check (review_observed_due_source_ex_vat>=0),
  add column review_observed_at_utc timestamptz check (pg_catalog.isfinite(review_observed_at_utc)),
  add constraint bpay_next_case_selection_review_observation_shape check
    ((review_observed_at_utc is null and review_observed_outstanding_source_ex_vat is null and review_observed_due_source_ex_vat is null)
     or (not is_selected and review_observed_at_utc is not null and review_observed_outstanding_source_ex_vat is not null
      and (review_observed_due_source_ex_vat is null or review_observed_due_source_ex_vat<=review_observed_outstanding_source_ex_vat)));

commit;
