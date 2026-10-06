-- Ordinary PAYE automatic collection: typed frozen origin and capture accounting.
-- No backfill, balance rewrite, public grant, manual schedule or Source change.

\set ON_ERROR_STOP on

begin;

alter table private.bpay_next_work_collection
  add constraint bpay_next_collection_work_key_unique unique(id,work_id,component_key),
  add constraint bpay_next_collection_case_candidate_unique unique(id,case_id,candidate_id);

alter table private.bpay_next_run_position
  add column work_collection_id uuid,
  add column work_collection_revision bigint,
  add column managed_capture_completed boolean not null default false,
  add constraint bpay_next_position_collection_origin_fk
    foreign key(work_collection_id,work_id,component_key)
    references private.bpay_next_work_collection(id,work_id,component_key),
  add constraint bpay_next_position_collection_pair_check check
    ((work_collection_id is null)=(work_collection_revision is null)),
  add constraint bpay_next_position_collection_shape_check check
    ((work_collection_id is null and not managed_capture_completed)
      or (work_collection_id is not null and work_collection_revision>0
        and not needs_financial_resolution and residual_source_ex_vat<0));

-- Replace only the audited old nullable-approved-line rule. A managed removed
-- zero has original paid evidence, not a fabricated current approved line.
do $constraint$
declare v_name name;v_count integer;
begin
  select count(*),min(c.conname::text)::name into v_count,v_name
    from pg_catalog.pg_constraint c
    where c.conrelid='private.bpay_next_run_position'::regclass and c.contype='c'
      and pg_catalog.pg_get_expr(c.conbin,c.conrelid)=
        '((approved_line_id IS NOT NULL) OR needs_financial_resolution)';
  if v_count<>1 then
    raise exception 'BPAY_NEXT_COLLECTION_OLD_POSITION_CHECK_UNEXPECTED';
  end if;
  execute pg_catalog.format('alter table private.bpay_next_run_position drop constraint %I',v_name);
end
$constraint$;
alter table private.bpay_next_run_position
  add constraint bpay_next_position_line_or_managed_check check
    (approved_line_id is not null or needs_financial_resolution or work_collection_id is not null);
create index bpay_next_position_unconsumed_collection_idx
  on private.bpay_next_run_position(run_worker_id,work_id,component_key)
  where work_collection_id is not null and not managed_capture_completed;

alter table private.bpay_next_run_worker
  add column expected_managed_position_count bigint not null default 0 check(expected_managed_position_count>=0),
  add column captured_managed_position_count bigint not null default 0 check(captured_managed_position_count>=0),
  add constraint bpay_next_worker_managed_capture_count_check
    check(captured_managed_position_count<=expected_managed_position_count);

alter table private.bpay_next_run_case_instruction
  add column work_collection_id uuid,
  add constraint bpay_next_instruction_collection_origin_fk
    foreign key(work_collection_id,case_id,candidate_id)
    references private.bpay_next_work_collection(id,case_id,candidate_id),
  add constraint bpay_next_instruction_collection_shape_check check
    (work_collection_id is null or
      (case_kind='OVERPAYMENT' and tax_treatment='TAXABLE'
        and source_pay_channel='PAYE' and target_pay_channel='PAYE'
        and instruction_kind='RECOVERY' and direction='DEDUCTION'
        and payroll_stage='GROSS_DEDUCT' and hold_purpose='GROSS_RECOVERY'));

-- An automatic debt is unscheduled. Opening due remains immutable audit
-- evidence, not a cap that prevents later genuine debt in the same pay week.
alter table private.bpay_next_case_period
  add column work_collection_id uuid,
  add constraint bpay_next_period_collection_origin_fk
    foreign key(work_collection_id,case_id,candidate_id)
    references private.bpay_next_work_collection(id,case_id,candidate_id),
  add constraint bpay_next_period_collection_shape_check check
    (work_collection_id is null or
      (case_component_id=work_collection_id and rule_id=work_collection_id));
do $constraint$
declare v_name name;v_count integer;
begin
  select count(*),min(c.conname::text)::name into v_count,v_name
    from pg_catalog.pg_constraint c
    where c.conrelid='private.bpay_next_case_period'::regclass and c.contype='c'
      and pg_catalog.regexp_replace(pg_catalog.pg_get_expr(c.conbin,c.conrelid),'[[:space:]()]','','g')=
        'realised_recovery_source_ex_vat::numeric+active_unrealised_recovery_source_ex_vat::numeric<=opening_due_source_ex_vat::numeric';
  if v_count<>1 then raise exception 'BPAY_NEXT_COLLECTION_OLD_PERIOD_CHECK_UNEXPECTED';end if;
  execute pg_catalog.format('alter table private.bpay_next_case_period drop constraint %I',v_name);
end
$constraint$;
alter table private.bpay_next_case_period
  add constraint bpay_next_period_scheduled_due_check check
    (work_collection_id is not null or
      realised_recovery_source_ex_vat+active_unrealised_recovery_source_ex_vat<=opening_due_source_ex_vat);

commit;
