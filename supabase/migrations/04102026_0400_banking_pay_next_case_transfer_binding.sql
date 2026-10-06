-- Exact original-transfer binding and typed case constituents. No runtime
-- routing, paid/funded backfill, external payment or service/browser grants.
\set ON_ERROR_STOP on
begin;

create table private.bpay_next_case_transfer_build (
  transfer_id uuid primary key,
  run_worker_id uuid not null,
  candidate_id uuid not null,
  projection_id uuid not null,
  draft_state_id uuid not null,
  net_state_id uuid not null,
  preparation_revision bigint not null check (preparation_revision>0),
  selection_revision bigint not null check (selection_revision>0),
  stage text not null check (stage in ('WORK','CASE','FINAL','COMPLETE')),
  checkpoint bigint not null default 0 check (checkpoint>=0),
  expected_work_count bigint not null check (expected_work_count>=0),
  expected_case_count bigint not null check (expected_case_count>=0),
  processed_work_count bigint not null default 0 check (processed_work_count>=0),
  processed_case_count bigint not null default 0 check (processed_case_count>=0),
  work_cursor bigint check (work_cursor>0),
  cursor_age_key bigint,
  cursor_case_id uuid,
  cursor_component_ordinal bigint check (cursor_component_ordinal>0),
  cursor_case_component_id uuid,
  work_cash_total private.bpay_next_penny_amount not null default 0 check (work_cash_total>=0),
  gross_additions_total private.bpay_next_penny_amount not null default 0 check (gross_additions_total>=0),
  gross_deductions_total private.bpay_next_penny_amount not null default 0 check (gross_deductions_total>=0),
  net_additions_total private.bpay_next_penny_amount not null default 0 check (net_additions_total>=0),
  net_recoveries_total private.bpay_next_penny_amount not null default 0 check (net_recoveries_total>=0),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  completed_at_utc timestamptz,
  foreign key (transfer_id,run_worker_id) references private.bpay_next_transfer(id,run_worker_id) on delete restrict,
  foreign key (run_worker_id,candidate_id) references private.bpay_next_run_worker(id,candidate_id) on delete restrict,
  foreign key (projection_id,run_worker_id) references private.bpay_next_net_projection(id,run_worker_id) on delete restrict,
  foreign key (draft_state_id,run_worker_id,candidate_id,preparation_revision,selection_revision)
    references private.bpay_next_case_allocation_state(id,run_worker_id,candidate_id,preparation_revision,selection_revision) on delete restrict,
  foreign key (net_state_id,run_worker_id,candidate_id,preparation_revision,selection_revision)
    references private.bpay_next_case_allocation_state(id,run_worker_id,candidate_id,preparation_revision,selection_revision) on delete restrict,
  check (processed_work_count<=expected_work_count and processed_case_count<=expected_case_count),
  check ((cursor_age_key is null and cursor_case_id is null and cursor_component_ordinal is null and cursor_case_component_id is null)
    or (cursor_age_key is not null and cursor_case_id is not null and cursor_component_ordinal is not null and cursor_case_component_id is not null)),
  check (stage<>'COMPLETE' or (processed_work_count=expected_work_count
    and processed_case_count=expected_case_count and completed_at_utc is not null))
);

alter table private.bpay_next_transfer_member
  add column case_instruction_id uuid references private.bpay_next_run_case_instruction(id) on delete restrict,
  add column case_allocation_result_id uuid,
  add constraint bpay_next_transfer_case_result_fk foreign key (case_allocation_result_id,case_instruction_id)
    references private.bpay_next_case_allocation_result(id,instruction_id) on delete restrict;

-- Replace ONLY the exact audited old subject/line/hold CHECK. A zero result
-- can carry its real shortfall identity without manufacturing an ACTIVE hold.
do $case_member_shape$
declare v_constraint name;
  v_expected text:=pg_catalog.regexp_replace($audited_check$
    (((subject_kind = 'WORK'::text) AND (run_line_id IS NOT NULL) AND (case_hold_id IS NULL))
     OR ((subject_kind = 'CASE'::text) AND (case_hold_id IS NOT NULL) AND (run_line_id IS NULL))
     OR ((subject_kind = ANY (ARRAY['NET_ADJUSTMENT'::text, 'CASH_REISSUE'::text]))
       AND (run_line_id IS NULL) AND (case_hold_id IS NULL)))
  $audited_check$,'[[:space:]()]','','g');
begin
  select c.conname into strict v_constraint from pg_catalog.pg_constraint c
    where c.conrelid='private.bpay_next_transfer_member'::regclass and c.contype='c'
      and pg_catalog.regexp_replace(pg_catalog.pg_get_expr(c.conbin,c.conrelid),'[[:space:]()]','','g')=v_expected;
  execute pg_catalog.format('alter table private.bpay_next_transfer_member drop constraint %I',v_constraint);
end
$case_member_shape$;
alter table private.bpay_next_transfer_member add constraint bpay_next_transfer_member_exact_shape check (
  (subject_kind='WORK' and run_line_id is not null and case_hold_id is null
    and case_instruction_id is null and case_allocation_result_id is null)
  or (subject_kind='CASE' and run_line_id is null and case_instruction_id is not null
    and case_allocation_result_id is not null
    and (case_hold_id is not null or signed_cash_contribution=0))
  or (subject_kind in ('NET_ADJUSTMENT','CASH_REISSUE') and run_line_id is null and case_hold_id is null
    and case_instruction_id is null and case_allocation_result_id is null));
create unique index bpay_next_transfer_case_instruction_once
  on private.bpay_next_transfer_member(transfer_id,case_instruction_id) where subject_kind='CASE';
create index bpay_next_transfer_case_result_idx
  on private.bpay_next_transfer_member(case_allocation_result_id,transfer_id);

alter table private.bpay_next_case_transfer_build owner to postgres;
alter table private.bpay_next_case_transfer_build enable row level security;
revoke all on table private.bpay_next_case_transfer_build from public,anon,authenticated,service_role;
commit;
