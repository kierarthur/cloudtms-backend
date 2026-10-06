-- Additive exact PAYE NET request/projection binding. No runtime switch,
-- history migration, payment action, case reprice or browser/service grant.
\set ON_ERROR_STOP on
begin;

alter table private.bpay_next_paye_net_request
  alter column entered_paye_net drop not null,
  alter column entered_paye_net type private.bpay_next_penny_amount,
  alter column frozen_gross_inc_vat type private.bpay_next_penny_amount,
  add column input_kind text not null default 'PAYE_MANUAL'
    check (input_kind in ('PAYE_MANUAL','CASE_PAYOUT')),
  add column actor_user_id uuid references public.tms_users(id) on delete restrict,
  add column case_draft_state_id uuid references private.bpay_next_case_allocation_state(id) on delete restrict,
  add column preparation_revision bigint check (preparation_revision>0),
  add column selection_revision bigint check (selection_revision>0),
  add column expected_projection_revision bigint check (expected_projection_revision>=0),
  add constraint bpay_next_net_request_input_shape check (
    (input_kind='PAYE_MANUAL' and entered_paye_net is not null)
    or (input_kind='CASE_PAYOUT' and entered_paye_net is null and frozen_gross_inc_vat=0)),
  add constraint bpay_next_net_request_case_shape check (
    (case_draft_state_id is null and actor_user_id is null and preparation_revision is null
      and selection_revision is null and expected_projection_revision is null and input_kind='PAYE_MANUAL')
    or (case_draft_state_id is not null and actor_user_id is not null and preparation_revision is not null
      and selection_revision is not null and expected_projection_revision is not null
      and request_no=expected_projection_revision+1)),
  add constraint bpay_next_net_request_case_state_fk
    foreign key (case_draft_state_id,run_worker_id,candidate_id,preparation_revision,selection_revision)
    references private.bpay_next_case_allocation_state(id,run_worker_id,candidate_id,preparation_revision,selection_revision)
    on delete restrict;

alter table private.bpay_next_case_allocation_state
  add column net_stage text not null default 'NOT_CASE_NET'
    check (net_stage in ('NOT_CASE_NET','WEEK_CAPTURE','ALLOCATE','COMPLETE','REVIEW')),
  add column net_requires_week_binding boolean not null default false,
  add column net_issue_code text check (net_issue_code in
    ('CASE_INPUT_UNBOUND','CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND')),
  add constraint bpay_next_net_state_shape check (net_stage='NOT_CASE_NET'
    or (pass_kind='NET' and pay_week_start is not null and captured_week_revision is not null)),
  add constraint bpay_next_net_state_complete check (net_stage<>'COMPLETE'
    or (status='COMPLETE' and projection_id is not null and net_issue_code is null));

create index bpay_next_case_instruction_net_protected_idx
  on private.bpay_next_run_case_instruction(run_worker_id,preparation_revision,selection_revision,id)
  where payroll_stage='NET_DEDUCT' and case_kind in ('LOAN','ADVANCE','MANUAL_DEBT');
create index bpay_next_case_result_positive_idx
  on private.bpay_next_case_allocation_result(state_id,instruction_id)
  where allocated_source_ex_vat>0;

-- Earlier arrangements use the accepted BANK amount (DEDUCT-R06), while the
-- entered payroll net remains a distinct saved fact. A CASE_PAYOUT contributes
-- no weekly earnings even though its own accepted cash is positive.
alter table private.bpay_next_worker_week_contribution
  add column accepted_projection_id uuid,
  add column accepted_bank_cash private.bpay_next_penny_amount check (accepted_bank_cash>=0),
  add constraint bpay_next_week_projection_fk foreign key (accepted_projection_id,original_run_worker_id)
    references private.bpay_next_net_projection(id,run_worker_id) on delete restrict,
  add constraint bpay_next_week_bank_cash_pair check
    ((accepted_projection_id is null)=(accepted_bank_cash is null)),
  add constraint bpay_next_week_payroll_bank_required check
    (basis_kind<>'PAYROLL_NET' or accepted_projection_id is not null);

-- The exact audited three-way eligibility CHECK is unnamed in 0300. Do not
-- rely on PostgreSQL's generated name or drop the other contribution guards.
do $week_bank_basis$
declare
  v_constraint name;
  v_expected text:=pg_catalog.regexp_replace($audited_check$
    (((eligibility_state = 'UNBOUND'::text) AND (eligible_arranged_amount IS NULL))
     OR ((eligibility_state = 'EXCLUDED'::text) AND (eligible_arranged_amount IS NOT NULL)
       AND ((eligible_arranged_amount)::numeric = (0)::numeric))
     OR ((eligibility_state = 'ELIGIBLE'::text) AND (eligible_arranged_amount IS NOT NULL)
       AND ((eligible_arranged_amount)::numeric = (CASE WHEN (basis_kind = 'PAYROLL_NET'::text)
         THEN original_payroll_net ELSE original_gross_amount END)::numeric)))
  $audited_check$,'[[:space:]()]','','g');
begin
  select c.conname into strict v_constraint from pg_catalog.pg_constraint c
    where c.conrelid='private.bpay_next_worker_week_contribution'::regclass and c.contype='c'
      and pg_catalog.regexp_replace(pg_catalog.pg_get_expr(c.conbin,c.conrelid),'[[:space:]()]','','g')=v_expected;
  execute pg_catalog.format('alter table private.bpay_next_worker_week_contribution drop constraint %I',v_constraint);
end
$week_bank_basis$;
alter table private.bpay_next_worker_week_contribution add constraint bpay_next_week_eligible_bank_basis check (
  (eligibility_state='UNBOUND' and eligible_arranged_amount is null)
  or (eligibility_state='EXCLUDED' and eligible_arranged_amount is not null and eligible_arranged_amount=0)
  or (eligibility_state='ELIGIBLE' and eligible_arranged_amount is not null
    and eligible_arranged_amount=case when accepted_projection_id is not null
      then accepted_bank_cash else original_gross_amount end));

-- All extended relations preserve their existing owner-only ACL/RLS profile.
commit;
