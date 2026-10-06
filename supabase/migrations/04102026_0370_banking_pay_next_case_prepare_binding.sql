-- Exact resumable PREPARE binding on the existing allocation state. No new
-- public phase, history calculator, backfill or runtime activation.
\set ON_ERROR_STOP on
begin;

-- Root lock + first case-header seal maintain this admission counter. It is
-- not a count obtained by scanning all selected Candidates during final seal.
alter table private.bpay_next_pay_run
  add column sealed_case_candidate_count bigint not null default 0
    check (sealed_case_candidate_count>=0 and sealed_case_candidate_count<=selected_candidate_count);

-- The old unnamed CHECK has different names in NEW/rehearsal installs. Locate
-- only its exact audited predicate, fail closed on missing/duplicate matches,
-- and preserve the separate WORK/Candidate FK, REVIEW status and byte checks.
do $case_issue_scope$
declare v_constraint name;
begin
  select c.conname into strict v_constraint from pg_catalog.pg_constraint c
    where c.conrelid='private.bpay_next_run_worker'::regclass and c.contype='c'
      and pg_catalog.regexp_replace(pg_catalog.pg_get_expr(c.conbin,c.conrelid),'[[:space:]()]','','g')
        ='review_issue_codeISNULL=review_issue_work_idISNULL';
  execute pg_catalog.format('alter table private.bpay_next_run_worker drop constraint %I',v_constraint);
end
$case_issue_scope$;
alter table private.bpay_next_run_worker add constraint bpay_next_worker_issue_scope_ck check (
  (review_issue_code is null and review_issue_work_id is null)
  or (review_issue_code in ('CASE_SELECTION_REQUIRED','CASE_TARGET_UNSUPPORTED','CASE_INPUT_UNBOUND',
      'CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND','CASE_NO_PAYABLE_AMOUNT') and review_issue_work_id is null)
  or (review_issue_code is not null and review_issue_code not in ('CASE_SELECTION_REQUIRED','CASE_TARGET_UNSUPPORTED','CASE_INPUT_UNBOUND',
      'CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND','CASE_NO_PAYABLE_AMOUNT') and review_issue_work_id is not null));

alter table private.bpay_next_case_allocation_state
  add column prepare_stage text not null default 'NOT_CASE_PREPARE'
    check (prepare_stage in ('NOT_CASE_PREPARE','WEEK_CAPTURE','INSTRUCTION_CAPTURE','ALLOCATE','COMPLETE','REVIEW')),
  add column instruction_capture_cursor bigint not null default 0 check (instruction_capture_cursor>=0),
  add column captured_instruction_count bigint not null default 0 check (captured_instruction_count>=0),
  add column captured_default_floor private.bpay_next_penny_amount check (captured_default_floor>=0),
  add column captured_default_floor_revision bigint check (captured_default_floor_revision>0),
  add column pay_week_start date check (pg_catalog.isfinite(pay_week_start) and extract(isodow from pay_week_start)=1),
  add column captured_week_revision bigint check (captured_week_revision>0),
  add column prior_pay_date_cursor date,
  add column prior_created_at_cursor timestamptz,
  add column prior_worker_cursor uuid,
  add column captured_prior_take_home private.bpay_next_penny_amount not null default 0 check (captured_prior_take_home>=0),
  add column captured_work_gross private.bpay_next_penny_amount not null default 0 check (captured_work_gross>=0),
  add column allocated_gross_additions private.bpay_next_penny_amount not null default 0 check (allocated_gross_additions>=0),
  add column allocated_gross_deductions private.bpay_next_penny_amount not null default 0 check (allocated_gross_deductions>=0),
  add column allocated_net_additions private.bpay_next_penny_amount not null default 0 check (allocated_net_additions>=0),
  add column payout_instruction_count bigint not null default 0 check (payout_instruction_count>=0),
  add constraint bpay_next_case_prepare_week_fk foreign key (candidate_id,pay_week_start)
    references private.bpay_next_worker_week(candidate_id,pay_week_start) on delete restrict,
  add constraint bpay_next_case_prepare_capture_count check (captured_instruction_count<=expected_instruction_count),
  add constraint bpay_next_case_prepare_capture_shape check (
    prepare_stage='NOT_CASE_PREPARE' or (pass_kind='DRAFT' and captured_default_floor is not null
      and captured_default_floor_revision is not null and pay_week_start is not null and captured_week_revision is not null)),
  add constraint bpay_next_case_prepare_prior_cursor check (
    (prior_pay_date_cursor is null and prior_created_at_cursor is null and prior_worker_cursor is null)
    or (prior_pay_date_cursor is not null and prior_created_at_cursor is not null and prior_worker_cursor is not null
      and pg_catalog.isfinite(prior_pay_date_cursor) and pg_catalog.isfinite(prior_created_at_cursor))),
  add constraint bpay_next_case_prepare_allocated_shape check (
    prepare_stage not in ('ALLOCATE','COMPLETE') or captured_instruction_count=expected_instruction_count),
  add constraint bpay_next_case_prepare_complete_shape check (
    prepare_stage<>'COMPLETE' or status='COMPLETE');

-- Only an exact current NEW-arrangement metadata probe, never payment history
-- or monetary aggregation. This proves a missing maintained week fact rather
-- than silently treating a previous arrangement as zero.
create index bpay_next_worker_current_week_basis_idx on private.bpay_next_run_worker(candidate_id,run_id,id)
  where status in ('READY','DRAFT','ISSUED','COMPLETE');
create index bpay_next_run_current_pay_date_idx on private.bpay_next_pay_run(pay_date,created_at_utc,id);

commit;
