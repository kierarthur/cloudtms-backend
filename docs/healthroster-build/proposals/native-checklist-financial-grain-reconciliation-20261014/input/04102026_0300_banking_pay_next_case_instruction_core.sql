-- Additive typed case storage; no case owner, runtime switch or backfill.
-- Existing no-case owners keep their exact defaults and guards. Unbound old
-- cases remain unbound, never inferred from legacy finance/payment history.
-- Policy X: post-Draft rules/basis are captured in run_case_instruction.
-- Later repeatable owners must install seal/transition/append-only guards;
-- migrations precede those function definitions in a clean NEW replay.
\set ON_ERROR_STOP on
begin;

-- Unconstrained numeric preserves exact inputs BEFORE validation, unlike a
-- numeric(18,2) cast that rounds 0.001. Signed carry is permitted. Individual
-- owners retain the core's storage/range admission and encoded-page bounds.
create domain private.bpay_next_penny_amount as numeric
  check (value is null or (value::text not in ('NaN','Infinity','-Infinity')
    and value=pg_catalog.trunc(value,2)));

create table private.bpay_next_case_rule (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  case_id uuid not null,
  candidate_id uuid not null,
  rule_revision bigint not null check (rule_revision>0),
  case_kind text not null check (case_kind in ('LOAN','ADVANCE','OVERPAYMENT','CREDIT','MANUAL_DEBT')),
  case_subtype text not null check (case_subtype in ('LOAN','PAYMENT_ADVANCE','OVERPAYMENT','UNDERPAYMENT','MANUAL_CREDIT','MANUAL_DEBT')),
  tax_treatment text not null check (tax_treatment in ('TAXABLE','NON_TAXABLE','NOT_APPLICABLE')),
  case_created_at_utc timestamptz not null check (pg_catalog.isfinite(case_created_at_utc)),
  original_funded_at_utc timestamptz check (pg_catalog.isfinite(original_funded_at_utc)),
  original_funding_event_id uuid references private.bpay_next_case_event(id) on delete restrict,
  minimum_earnings_threshold private.bpay_next_penny_amount check (minimum_earnings_threshold>=0),
  take_home_floor_override private.bpay_next_penny_amount check (take_home_floor_override>=0),
  weekly_due_source_ex_vat private.bpay_next_penny_amount check (weekly_due_source_ex_vat>0),
  schedule_start_monday date,
  next_due_monday date,
  schedule_week_count bigint check (schedule_week_count>0),
  captured_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (case_id,rule_revision),
  unique (id,case_id,candidate_id),
  unique (id,case_id,candidate_id,rule_revision),
  unique (id,case_id,candidate_id,case_kind,case_subtype,tax_treatment),
  foreign key (case_id,candidate_id) references private.bpay_next_finance_case(id,candidate_id) on delete restrict,
  check ((case_kind='LOAN' and case_subtype='LOAN')
    or (case_kind='ADVANCE' and case_subtype='PAYMENT_ADVANCE')
    or (case_kind='OVERPAYMENT' and case_subtype='OVERPAYMENT')
    or (case_kind='CREDIT' and case_subtype in ('UNDERPAYMENT','MANUAL_CREDIT'))
    or (case_kind='MANUAL_DEBT' and case_subtype='MANUAL_DEBT')),
  check ((case_kind in ('LOAN','ADVANCE') and tax_treatment='NOT_APPLICABLE')
    or (case_kind not in ('LOAN','ADVANCE') and tax_treatment in ('TAXABLE','NON_TAXABLE'))),
  check (original_funded_at_utc is null or original_funded_at_utc>=case_created_at_utc),
  check ((original_funded_at_utc is null)=(original_funding_event_id is null)),
  check ((weekly_due_source_ex_vat is null and schedule_start_monday is null
      and next_due_monday is null and schedule_week_count is null)
    or (weekly_due_source_ex_vat is not null and schedule_start_monday is not null
      and next_due_monday is not null and schedule_week_count is not null
      and pg_catalog.isfinite(schedule_start_monday) and pg_catalog.isfinite(next_due_monday)
      and extract(isodow from schedule_start_monday)=1 and extract(isodow from next_due_monday)=1
      and next_due_monday>=schedule_start_monday))
);
create index bpay_next_case_rule_candidate_idx on private.bpay_next_case_rule(candidate_id,case_id,rule_revision);
alter table private.bpay_next_finance_case
  add column case_revision bigint not null default 0 check (case_revision>=0),
  add column current_rule_id uuid,
  add column current_rule_revision bigint check (current_rule_revision>0),
  add constraint bpay_next_case_current_rule_shape check ((current_rule_id is null)=(current_rule_revision is null)),
  add constraint bpay_next_case_current_rule_fk foreign key (current_rule_id,id,candidate_id,current_rule_revision)
    references private.bpay_next_case_rule(id,case_id,candidate_id,rule_revision) on delete restrict;

-- Current exact component/capacity facts, not the immutable instruction.
-- Mutable rules/valuation never become an FK target for frozen amounts.
create table private.bpay_next_case_component (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  case_id uuid not null,
  candidate_id uuid not null,
  component_key text not null check (pg_catalog.octet_length(component_key) between 1 and 256),
  component_ordinal bigint not null check (component_ordinal>0),
  component_revision bigint not null check (component_revision>0),
  rule_id uuid not null,
  case_kind text not null,
  case_subtype text not null,
  tax_treatment text not null,
  instruction_kind text not null check (instruction_kind in ('PAYOUT','RECOVERY','CREDIT')),
  direction text not null check (direction in ('PAYMENT','DEDUCTION')),
  payroll_stage text not null check (payroll_stage in ('GROSS_ADD','GROSS_DEDUCT','NET_ADD','NET_DEDUCT')),
  source_pay_channel text not null check (source_pay_channel in ('PAYE','UMBRELLA')),
  currency text not null check (currency='GBP'),
  approved_source_ex_vat private.bpay_next_penny_amount not null check (approved_source_ex_vat>=0),
  funded_source_ex_vat private.bpay_next_penny_amount not null default 0 check (funded_source_ex_vat>=0),
  recovered_source_ex_vat private.bpay_next_penny_amount not null default 0 check (recovered_source_ex_vat>=0),
  paid_credit_source_ex_vat private.bpay_next_penny_amount not null default 0 check (paid_credit_source_ex_vat>=0),
  written_off_source_ex_vat private.bpay_next_penny_amount not null default 0 check (written_off_source_ex_vat>=0),
  active_payout_source_ex_vat private.bpay_next_penny_amount not null default 0 check (active_payout_source_ex_vat>=0),
  active_recovery_source_ex_vat private.bpay_next_penny_amount not null default 0 check (active_recovery_source_ex_vat>=0),
  resolution_state text not null check (resolution_state in ('RESOLVED','REVIEW')),
  target_pay_channel text check (target_pay_channel in ('PAYE','UMBRELLA')),
  target_ex_vat private.bpay_next_penny_amount check (target_ex_vat>=0),
  target_vat private.bpay_next_penny_amount check (target_vat>=0),
  target_inc_vat private.bpay_next_penny_amount check (target_inc_vat>=0),
  valuation_policy_id uuid,
  valuation_window_id uuid,
  resolution_ref text check (pg_catalog.octet_length(resolution_ref) between 1 and 256),
  updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (case_id,component_key),
  unique (case_id,component_ordinal),
  unique (id,case_id,candidate_id),
  foreign key (rule_id,case_id,candidate_id,case_kind,case_subtype,tax_treatment)
    references private.bpay_next_case_rule(id,case_id,candidate_id,case_kind,case_subtype,tax_treatment) on delete restrict,
  foreign key (valuation_policy_id,valuation_window_id)
    references private.bpay_next_valuation_policy_window(policy_id,source_window_id) on delete restrict,
  check ((instruction_kind='PAYOUT' and case_kind in ('LOAN','ADVANCE') and direction='PAYMENT' and payroll_stage='NET_ADD')
    or (instruction_kind='CREDIT' and case_kind='CREDIT' and direction='PAYMENT'
      and payroll_stage=case when tax_treatment='TAXABLE' then 'GROSS_ADD' else 'NET_ADD' end)
    or (instruction_kind='RECOVERY' and case_kind in ('LOAN','ADVANCE','OVERPAYMENT','MANUAL_DEBT') and direction='DEDUCTION'
      and payroll_stage=case when tax_treatment='TAXABLE' then 'GROSS_DEDUCT' else 'NET_DEDUCT' end)),
  check (funded_source_ex_vat<=approved_source_ex_vat),
  check (case_kind in ('LOAN','ADVANCE') or funded_source_ex_vat=0),
  check (case_kind='CREDIT' or paid_credit_source_ex_vat=0),
  check (recovered_source_ex_vat+written_off_source_ex_vat+active_recovery_source_ex_vat<=
    case when case_kind in ('LOAN','ADVANCE') then funded_source_ex_vat else approved_source_ex_vat end),
  check (funded_source_ex_vat+paid_credit_source_ex_vat+active_payout_source_ex_vat<=approved_source_ex_vat),
  -- Paid, written-off and still-reserved CREDIT slices are disjoint uses of
  -- the same approved credit; a paid/write-off pair cannot fund a new payout.
  check (case_kind<>'CREDIT' or
    paid_credit_source_ex_vat+written_off_source_ex_vat+active_payout_source_ex_vat<=approved_source_ex_vat),
  check ((resolution_state='REVIEW' and target_pay_channel is null and target_ex_vat is null
      and target_vat is null and target_inc_vat is null and valuation_policy_id is null
      and valuation_window_id is null and resolution_ref is null)
    or (resolution_state='RESOLVED' and target_pay_channel is not null and target_ex_vat is not null
      and target_vat is not null and target_inc_vat is not null and valuation_policy_id is not null
      and valuation_window_id is not null and resolution_ref is not null and target_inc_vat=target_ex_vat+target_vat))
);
create index bpay_next_case_component_candidate_idx on private.bpay_next_case_component(candidate_id,case_id,component_ordinal,id);
create index bpay_next_case_component_rule_idx on private.bpay_next_case_component(rule_id,id);
create index bpay_next_case_component_policy_idx on private.bpay_next_case_component(valuation_policy_id,valuation_window_id);

-- Period-opening due is independent of the later outstanding balance.
-- realised and active-unrealised are DISJOINT counters, never max(overlap).
create table private.bpay_next_case_period (
  case_component_id uuid not null,
  case_id uuid not null,
  candidate_id uuid not null,
  pay_week_start date not null check (pg_catalog.isfinite(pay_week_start) and extract(isodow from pay_week_start)=1),
  rule_id uuid not null,
  period_revision bigint not null check (period_revision>0),
  opening_outstanding_source_ex_vat private.bpay_next_penny_amount not null check (opening_outstanding_source_ex_vat>=0),
  opening_due_source_ex_vat private.bpay_next_penny_amount not null check (opening_due_source_ex_vat>=0),
  realised_recovery_source_ex_vat private.bpay_next_penny_amount not null default 0 check (realised_recovery_source_ex_vat>=0),
  active_unrealised_recovery_source_ex_vat private.bpay_next_penny_amount not null default 0 check (active_unrealised_recovery_source_ex_vat>=0),
  active_payout_source_ex_vat private.bpay_next_penny_amount not null default 0 check (active_payout_source_ex_vat>=0),
  primary key (case_component_id,pay_week_start),
  unique (case_component_id,case_id,candidate_id,pay_week_start),
  foreign key (case_component_id,case_id,candidate_id) references private.bpay_next_case_component(id,case_id,candidate_id) on delete restrict,
  foreign key (rule_id,case_id,candidate_id) references private.bpay_next_case_rule(id,case_id,candidate_id) on delete restrict,
  check (opening_due_source_ex_vat<=opening_outstanding_source_ex_vat),
  check (realised_recovery_source_ex_vat+active_unrealised_recovery_source_ex_vat<=opening_due_source_ex_vat)
);
create index bpay_next_case_period_candidate_idx on private.bpay_next_case_period(candidate_id,pay_week_start,case_component_id);
create index bpay_next_case_period_rule_idx on private.bpay_next_case_period(rule_id,case_component_id,pay_week_start);

-- A bounded, row-backed case selection is distinct from WORK membership.
-- Even case-only candidates use the existing exact Candidate enrollment.
create table private.bpay_next_case_selection (
  run_id uuid not null,
  candidate_id uuid not null,
  selection_revision bigint not null check (selection_revision>0),
  status text not null check (status in ('OPEN','SEALED')),
  page_count bigint not null default 0 check (page_count>=0),
  item_count bigint not null default 0 check (item_count>=0),
  selected_count bigint not null default 0 check (selected_count>=0),
  excluded_count bigint not null default 0 check (excluded_count>=0),
  sealed_at_utc timestamptz,
  primary key (run_id,candidate_id,selection_revision),
  foreign key (run_id,candidate_id) references private.bpay_next_selection_candidate(run_id,candidate_id) on delete restrict,
  check (item_count=selected_count+excluded_count),
  check ((status='SEALED')=(sealed_at_utc is not null))
);
create table private.bpay_next_case_selection_page (
  run_id uuid not null,
  candidate_id uuid not null,
  selection_revision bigint not null,
  page_no bigint not null check (page_no>0),
  request_id uuid not null,
  first_selection_no bigint not null check (first_selection_no>0),
  item_count integer not null check (item_count between 1 and 100),
  selected_count integer not null check (selected_count>=0),
  excluded_count integer not null check (excluded_count>=0),
  primary key (run_id,candidate_id,selection_revision,page_no),
  unique (run_id,request_id),
  foreign key (run_id,candidate_id,selection_revision) references private.bpay_next_case_selection(run_id,candidate_id,selection_revision) on delete restrict,
  check (item_count=selected_count+excluded_count)
);
create table private.bpay_next_case_selection_item (
  run_id uuid not null,
  candidate_id uuid not null,
  selection_revision bigint not null,
  page_no bigint not null,
  item_no integer not null check (item_no between 1 and 100),
  selection_no bigint not null check (selection_no>0),
  case_component_id uuid not null,
  case_id uuid not null,
  is_selected boolean not null,
  primary key (run_id,candidate_id,selection_revision,case_component_id),
  unique (run_id,candidate_id,selection_revision,selection_no),
  unique (run_id,candidate_id,selection_revision,page_no,item_no),
  unique (run_id,candidate_id,selection_revision,case_component_id,case_id,is_selected),
  foreign key (run_id,candidate_id,selection_revision,page_no)
    references private.bpay_next_case_selection_page(run_id,candidate_id,selection_revision,page_no) on delete restrict,
  foreign key (case_component_id,case_id,candidate_id) references private.bpay_next_case_component(id,case_id,candidate_id) on delete restrict
);
create index bpay_next_case_selection_component_idx on private.bpay_next_case_selection_item(case_component_id,run_id);
create index bpay_next_case_selection_selected_page_idx on private.bpay_next_case_selection_item(run_id,candidate_id,selection_revision,selection_no) where is_selected;

alter table private.bpay_next_run_worker
  add column preparation_revision bigint not null default 1 check (preparation_revision>0),
  add column case_selection_revision bigint not null default 0 check (case_selection_revision>=0),
  add column case_instruction_count bigint not null default 0 check (case_instruction_count>=0),
  add column case_allocated_count bigint not null default 0 check (case_allocated_count>=0),
  add column active_case_hold_count bigint not null default 0 check (active_case_hold_count>=0),
  add column case_pending_binding_count bigint not null default 0 check (case_pending_binding_count>=0),
  add constraint bpay_next_worker_run_candidate_key unique (id,run_id,candidate_id);

-- One immutable captured instruction per selected component/preparation.
-- Age, stage, NULL override/default resolution and SOURCE/TARGET valuation
-- are saved facts. No mutable current-rule/amount fallback after capture.
create table private.bpay_next_run_case_instruction (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  run_worker_id uuid not null,
  run_id uuid not null,
  candidate_id uuid not null,
  selection_revision bigint not null check (selection_revision>0),
  preparation_revision bigint not null check (preparation_revision>0),
  case_component_id uuid not null,
  case_id uuid not null,
  is_selected boolean not null default true check (is_selected),
  captured_case_revision bigint not null check (captured_case_revision>0),
  captured_component_revision bigint not null check (captured_component_revision>0),
  captured_period_revision bigint not null check (captured_period_revision>0),
  rule_id uuid not null,
  rule_revision bigint not null check (rule_revision>0),
  case_kind text not null,
  case_subtype text not null,
  tax_treatment text not null,
  instruction_kind text not null check (instruction_kind in ('PAYOUT','RECOVERY','CREDIT')),
  direction text not null check (direction in ('PAYMENT','DEDUCTION')),
  payroll_stage text not null check (payroll_stage in ('GROSS_ADD','GROSS_DEDUCT','NET_ADD','NET_DEDUCT')),
  hold_purpose text not null check (hold_purpose in ('PAYOUT','GROSS_RECOVERY','NET_RECOVERY_CAPACITY')),
  age_key bigint not null,
  debt_age_at_utc timestamptz not null check (pg_catalog.isfinite(debt_age_at_utc)),
  component_ordinal bigint not null check (component_ordinal>0),
  pay_week_start date not null check (pg_catalog.isfinite(pay_week_start) and extract(isodow from pay_week_start)=1),
  source_pay_channel text not null check (source_pay_channel in ('PAYE','UMBRELLA')),
  target_pay_channel text not null check (target_pay_channel in ('PAYE','UMBRELLA')),
  allocation_channel text not null check (allocation_channel in ('PAYE','UMBRELLA')),
  currency text not null check (currency='GBP'),
  nominal_source_ex_vat private.bpay_next_penny_amount not null check (nominal_source_ex_vat>=0),
  nominal_target_ex_vat private.bpay_next_penny_amount not null check (nominal_target_ex_vat>=0),
  nominal_target_vat private.bpay_next_penny_amount not null check (nominal_target_vat>=0),
  nominal_target_inc_vat private.bpay_next_penny_amount not null check (nominal_target_inc_vat>=0),
  capacity_source_ex_vat private.bpay_next_penny_amount not null check (capacity_source_ex_vat>=0),
  capacity_target_ex_vat private.bpay_next_penny_amount not null check (capacity_target_ex_vat>=0),
  capacity_target_vat private.bpay_next_penny_amount not null check (capacity_target_vat>=0),
  capacity_target_inc_vat private.bpay_next_penny_amount not null check (capacity_target_inc_vat>=0),
  minimum_earnings_threshold private.bpay_next_penny_amount check (minimum_earnings_threshold>=0),
  take_home_floor_override private.bpay_next_penny_amount check (take_home_floor_override>=0),
  captured_default_floor private.bpay_next_penny_amount not null check (captured_default_floor>=0),
  captured_default_floor_revision bigint not null check (captured_default_floor_revision>0),
  resolved_take_home_floor private.bpay_next_penny_amount not null check (resolved_take_home_floor>=0),
  valuation_policy_id uuid not null,
  valuation_window_id uuid not null,
  resolution_ref text not null check (pg_catalog.octet_length(resolution_ref) between 1 and 256),
  explanation text check (pg_catalog.octet_length(explanation)<=8192),
  captured_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (run_worker_id,preparation_revision,case_component_id),
  unique (id,run_worker_id,candidate_id,preparation_revision),
  unique (id,run_worker_id,candidate_id,preparation_revision,selection_revision,hold_purpose,allocation_channel),
  unique (id,run_worker_id,candidate_id,case_component_id,case_id,hold_purpose,pay_week_start),
  unique (id,case_id,candidate_id,case_component_id,case_kind),
  foreign key (run_worker_id,run_id,candidate_id) references private.bpay_next_run_worker(id,run_id,candidate_id) on delete restrict,
  foreign key (run_id,candidate_id,selection_revision,case_component_id,case_id,is_selected)
    references private.bpay_next_case_selection_item(run_id,candidate_id,selection_revision,case_component_id,case_id,is_selected) on delete restrict,
  foreign key (rule_id,case_id,candidate_id,rule_revision) references private.bpay_next_case_rule(id,case_id,candidate_id,rule_revision) on delete restrict,
  foreign key (rule_id,case_id,candidate_id,case_kind,case_subtype,tax_treatment)
    references private.bpay_next_case_rule(id,case_id,candidate_id,case_kind,case_subtype,tax_treatment) on delete restrict,
  foreign key (case_component_id,case_id,candidate_id,pay_week_start) references private.bpay_next_case_period(case_component_id,case_id,candidate_id,pay_week_start) on delete restrict,
  foreign key (valuation_policy_id,valuation_window_id) references private.bpay_next_valuation_policy_window(policy_id,source_window_id) on delete restrict,
  check (age_key=pg_catalog.floor(extract(epoch from debt_age_at_utc)*1000000)::bigint),
  check (nominal_target_inc_vat=nominal_target_ex_vat+nominal_target_vat),
  check (capacity_target_inc_vat=capacity_target_ex_vat+capacity_target_vat),
  check (capacity_source_ex_vat<=nominal_source_ex_vat and capacity_target_ex_vat<=nominal_target_ex_vat),
  check (resolved_take_home_floor=case when take_home_floor_override is null then captured_default_floor else take_home_floor_override end),
  check ((instruction_kind='PAYOUT' and case_kind in ('LOAN','ADVANCE') and direction='PAYMENT' and payroll_stage='NET_ADD' and hold_purpose='PAYOUT')
    or (instruction_kind='CREDIT' and case_kind='CREDIT' and direction='PAYMENT' and hold_purpose='PAYOUT'
      and payroll_stage=case when tax_treatment='TAXABLE' then 'GROSS_ADD' else 'NET_ADD' end)
    or (instruction_kind='RECOVERY' and case_kind in ('LOAN','ADVANCE','OVERPAYMENT','MANUAL_DEBT') and direction='DEDUCTION'
      and payroll_stage=case when tax_treatment='TAXABLE' then 'GROSS_DEDUCT' else 'NET_DEDUCT' end
      and hold_purpose=case when tax_treatment='TAXABLE' then 'GROSS_RECOVERY' else 'NET_RECOVERY_CAPACITY' end))
);
create index bpay_next_case_instruction_order_idx on private.bpay_next_run_case_instruction
  (run_worker_id,preparation_revision,age_key,case_id,component_ordinal,case_component_id);
create index bpay_next_case_instruction_rule_idx on private.bpay_next_run_case_instruction(rule_id,id);
create index bpay_next_case_instruction_component_idx on private.bpay_next_run_case_instruction(case_component_id,pay_week_start,id);
create index bpay_next_case_instruction_policy_idx on private.bpay_next_run_case_instruction(valuation_policy_id,valuation_window_id);

-- Carried state is one worker budget plus caller-held per-channel E. PREVIEW
-- has no financial command/job; authoritative passes use the existing lane.
alter table private.bpay_next_job add constraint bpay_next_job_case_owner_key unique (id,command_id,candidate_id,module_epoch);
create table private.bpay_next_case_allocation_state (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  run_worker_id uuid not null,
  run_id uuid not null,
  candidate_id uuid not null,
  selection_revision bigint not null check (selection_revision>0),
  preparation_revision bigint not null check (preparation_revision>0),
  pass_kind text not null check (pass_kind in ('PREVIEW','DRAFT','NET')),
  projection_no bigint not null check (projection_no>=0),
  projection_id uuid,
  command_id uuid,
  job_id uuid,
  module_epoch bigint not null check (module_epoch>0),
  owner_epoch bigint not null check (owner_epoch>0),
  financial_view_revision bigint not null check (financial_view_revision>=0),
  status text not null check (status in ('BUILDING','COMPLETE','OUTDATED','CANCELLED')),
  weekly_binding_state text not null check (weekly_binding_state in ('NOT_AFFECTED','BOUND','UNBOUND')),
  expected_instruction_count bigint not null check (expected_instruction_count>=0),
  processed_instruction_count bigint not null default 0 check (processed_instruction_count>=0),
  initial_worker_take_home private.bpay_next_penny_amount not null check (initial_worker_take_home>=0),
  remaining_worker_take_home private.bpay_next_penny_amount not null,
  recovered_source_total private.bpay_next_penny_amount not null default 0 check (recovered_source_total>=0),
  recovered_target_total private.bpay_next_penny_amount not null default 0 check (recovered_target_total>=0),
  cursor_age_key bigint,
  cursor_case_id uuid,
  cursor_component_ordinal bigint check (cursor_component_ordinal>0),
  cursor_case_component_id uuid,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  completed_at_utc timestamptz,
  unique (run_worker_id,selection_revision,preparation_revision,pass_kind,projection_no),
  unique (id,run_worker_id,candidate_id,preparation_revision),
  unique (id,run_worker_id,candidate_id,preparation_revision,selection_revision),
  unique (id,run_worker_id,candidate_id,preparation_revision,selection_revision,pass_kind),
  foreign key (run_worker_id,run_id,candidate_id) references private.bpay_next_run_worker(id,run_id,candidate_id) on delete restrict,
  foreign key (run_id,candidate_id,selection_revision) references private.bpay_next_case_selection(run_id,candidate_id,selection_revision) on delete restrict,
  foreign key (job_id,command_id,candidate_id,module_epoch) references private.bpay_next_job(id,command_id,candidate_id,module_epoch) on delete restrict,
  foreign key (projection_id,run_worker_id) references private.bpay_next_net_projection(id,run_worker_id) on delete restrict,
  check ((pass_kind='PREVIEW' and command_id is null and job_id is null)
    or (pass_kind<>'PREVIEW' and command_id is not null and job_id is not null)),
  check ((pass_kind='NET' and projection_no>0) or (pass_kind<>'NET' and projection_no=0 and projection_id is null)),
  check (processed_instruction_count<=expected_instruction_count),
  check (remaining_worker_take_home<=initial_worker_take_home),
  check ((cursor_age_key is null and cursor_case_id is null and cursor_component_ordinal is null and cursor_case_component_id is null)
    or (cursor_age_key is not null and cursor_case_id is not null and cursor_component_ordinal is not null and cursor_case_component_id is not null)),
  check (status<>'COMPLETE' or (processed_instruction_count=expected_instruction_count
    and weekly_binding_state<>'UNBOUND' and completed_at_utc is not null))
);
create index bpay_next_case_allocation_job_idx on private.bpay_next_case_allocation_state(job_id,id);
create index bpay_next_case_allocation_projection_idx on private.bpay_next_case_allocation_state(projection_id,id);
create table private.bpay_next_case_allocation_channel (
  state_id uuid not null references private.bpay_next_case_allocation_state(id) on delete restrict,
  allocation_channel text not null check (allocation_channel in ('PAYE','UMBRELLA')),
  initial_headroom private.bpay_next_penny_amount not null check (initial_headroom>=0),
  remaining_headroom private.bpay_next_penny_amount not null check (remaining_headroom>=0),
  primary key (state_id,allocation_channel),
  check (remaining_headroom<=initial_headroom)
);
create table private.bpay_next_case_allocation_result (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  state_id uuid not null,
  instruction_id uuid not null,
  run_worker_id uuid not null,
  candidate_id uuid not null,
  preparation_revision bigint not null check (preparation_revision>0),
  selection_revision bigint not null check (selection_revision>0),
  result_no bigint not null check (result_no>0),
  pass_kind text not null check (pass_kind in ('PREVIEW','DRAFT','NET')),
  allocation_channel text not null,
  hold_purpose text not null check (hold_purpose in ('PAYOUT','GROSS_RECOVERY','NET_RECOVERY_CAPACITY')),
  nominal_target_ex_vat private.bpay_next_penny_amount not null check (nominal_target_ex_vat>=0),
  usable_capacity_target_ex_vat private.bpay_next_penny_amount not null check (usable_capacity_target_ex_vat>=0),
  allocated_source_ex_vat private.bpay_next_penny_amount not null check (allocated_source_ex_vat>=0),
  allocated_target_ex_vat private.bpay_next_penny_amount not null check (allocated_target_ex_vat>=0),
  allocated_target_vat private.bpay_next_penny_amount not null check (allocated_target_vat>=0),
  allocated_target_inc_vat private.bpay_next_penny_amount not null check (allocated_target_inc_vat>=0),
  shortfall_target_ex_vat private.bpay_next_penny_amount not null check (shortfall_target_ex_vat>=0),
  cap_reason text check (cap_reason in ('TAKE_HOME_FLOOR','EARNINGS_THRESHOLD','PAY_HEADROOM','CASE_CAPACITY')),
  affordability_reason text check (affordability_reason in ('TAKE_HOME_FLOOR','EARNINGS_THRESHOLD','PAY_HEADROOM')),
  capacity_reason text check (capacity_reason='CASE_CAPACITY'),
  channel_headroom_before private.bpay_next_penny_amount not null check (channel_headroom_before>=0),
  channel_headroom_after private.bpay_next_penny_amount not null check (channel_headroom_after>=0),
  worker_take_home_before private.bpay_next_penny_amount not null,
  worker_take_home_after private.bpay_next_penny_amount not null,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (state_id,instruction_id),
  unique (state_id,result_no),
  unique (id,instruction_id),
  unique (id,instruction_id,pass_kind),
  foreign key (state_id,run_worker_id,candidate_id,preparation_revision,selection_revision,pass_kind)
    references private.bpay_next_case_allocation_state(id,run_worker_id,candidate_id,preparation_revision,selection_revision,pass_kind) on delete restrict,
  foreign key (instruction_id,run_worker_id,candidate_id,preparation_revision,selection_revision,hold_purpose,allocation_channel)
    references private.bpay_next_run_case_instruction(id,run_worker_id,candidate_id,preparation_revision,selection_revision,hold_purpose,allocation_channel) on delete restrict,
  foreign key (state_id,allocation_channel) references private.bpay_next_case_allocation_channel(state_id,allocation_channel) on delete restrict,
  check (allocated_target_inc_vat=allocated_target_ex_vat+allocated_target_vat),
  check (allocated_target_ex_vat<=nominal_target_ex_vat and allocated_target_ex_vat<=usable_capacity_target_ex_vat),
  check (shortfall_target_ex_vat=nominal_target_ex_vat-allocated_target_ex_vat),
  check ((hold_purpose='PAYOUT' and channel_headroom_after=channel_headroom_before
      and worker_take_home_after=worker_take_home_before and cap_reason is null and affordability_reason is null)
    or (hold_purpose<>'PAYOUT' and channel_headroom_after=channel_headroom_before-allocated_target_ex_vat
      and worker_take_home_after=worker_take_home_before-allocated_target_ex_vat))
);
create index bpay_next_case_result_instruction_idx on private.bpay_next_case_allocation_result(instruction_id,state_id);

-- Retain the existing transfer-member FK to case_hold. New typed holds link
-- an exact instruction/result; old unbound rows are not manufactured anew.
alter table private.bpay_next_case_hold
  add column instruction_id uuid,
  add column allocation_result_id uuid,
  add column allocation_pass_kind text check (allocation_pass_kind in ('DRAFT','NET')),
  add column case_component_id uuid,
  add column purpose text check (purpose in ('PAYOUT','GROSS_RECOVERY','NET_RECOVERY_CAPACITY')),
  add column pay_week_start date,
  add column projection_id uuid,
  add column source_reserved_ex_vat private.bpay_next_penny_amount check (source_reserved_ex_vat>=0),
  add column target_amount_ex_vat private.bpay_next_penny_amount check (target_amount_ex_vat>=0),
  add column target_amount_vat private.bpay_next_penny_amount check (target_amount_vat>=0),
  add column target_amount_inc_vat private.bpay_next_penny_amount check (target_amount_inc_vat>=0),
  add constraint bpay_next_case_hold_result_once unique (allocation_result_id),
  add constraint bpay_next_case_hold_typed_key unique (id,case_component_id,case_id,candidate_id,pay_week_start,purpose),
  add constraint bpay_next_case_hold_instruction_fk foreign key (instruction_id,run_worker_id,candidate_id,case_component_id,case_id,purpose,pay_week_start)
    references private.bpay_next_run_case_instruction(id,run_worker_id,candidate_id,case_component_id,case_id,hold_purpose,pay_week_start) on delete restrict,
  add constraint bpay_next_case_hold_result_fk foreign key (allocation_result_id,instruction_id,allocation_pass_kind)
    references private.bpay_next_case_allocation_result(id,instruction_id,pass_kind) on delete restrict,
  add constraint bpay_next_case_hold_projection_fk foreign key (projection_id,run_worker_id)
    references private.bpay_next_net_projection(id,run_worker_id) on delete restrict,
  add constraint bpay_next_case_hold_typed_shape check (
    (instruction_id is null and allocation_result_id is null and allocation_pass_kind is null and case_component_id is null and purpose is null
      and pay_week_start is null and projection_id is null and source_reserved_ex_vat is null
      and target_amount_ex_vat is null and target_amount_vat is null and target_amount_inc_vat is null)
    or (instruction_id is not null and allocation_result_id is not null and allocation_pass_kind is not null and case_component_id is not null and purpose is not null
      and pay_week_start is not null and source_reserved_ex_vat is not null and target_amount_ex_vat is not null
      and target_amount_vat is not null and target_amount_inc_vat is not null
      and source_reserved_ex_vat>0 and amount=source_reserved_ex_vat and target_amount_inc_vat=target_amount_ex_vat+target_amount_vat
      and (purpose<>'NET_RECOVERY_CAPACITY' or status<>'REALISED' or projection_id is not null)));
create index bpay_next_case_hold_period_idx on private.bpay_next_case_hold(case_component_id,pay_week_start,id);
create index bpay_next_case_hold_projection_idx on private.bpay_next_case_hold(projection_id,id);
create unique index bpay_next_case_hold_active_instruction_once on private.bpay_next_case_hold(instruction_id)
  where instruction_id is not null and status='ACTIVE';

-- One reservation/effect identity occupies exactly one consumption class.
-- ACTIVE -> REALISED moves the amount, not a second overlapping row. Owners
-- update this record, period/component counters and hold/event atomically.
create table private.bpay_next_case_capacity_use (
  case_hold_id uuid primary key,
  case_component_id uuid not null,
  case_id uuid not null,
  candidate_id uuid not null,
  pay_week_start date not null,
  purpose text not null check (purpose in ('PAYOUT','GROSS_RECOVERY','NET_RECOVERY_CAPACITY')),
  status text not null check (status in ('ACTIVE','REALISED','RELEASED')),
  source_amount_ex_vat private.bpay_next_penny_amount not null check (source_amount_ex_vat>0),
  realisation_event_id uuid unique,
  foreign key (case_hold_id,case_component_id,case_id,candidate_id,pay_week_start,purpose)
    references private.bpay_next_case_hold(id,case_component_id,case_id,candidate_id,pay_week_start,purpose) on delete restrict,
  foreign key (case_component_id,case_id,candidate_id,pay_week_start)
    references private.bpay_next_case_period(case_component_id,case_id,candidate_id,pay_week_start) on delete restrict,
  check ((status='REALISED')=(realisation_event_id is not null))
);
create index bpay_next_case_capacity_period_idx on private.bpay_next_case_capacity_use(case_component_id,pay_week_start,status,case_hold_id);

-- Loan funding, credit payment, recovery and shortfall have distinct facts.
-- Non-loan debt starts from approved legitimate debt, not fictitious funding.
alter table private.bpay_next_finance_case
  alter column principal_approved type private.bpay_next_penny_amount,
  alter column principal_funded type private.bpay_next_penny_amount,
  alter column principal_recovered type private.bpay_next_penny_amount,
  alter column principal_written_off type private.bpay_next_penny_amount,
  alter column active_hold_amount type private.bpay_next_penny_amount,
  add column principal_paid_credit private.bpay_next_penny_amount not null default 0 check (principal_paid_credit>=0),
  add column active_payout_hold_amount private.bpay_next_penny_amount not null default 0 check (active_payout_hold_amount>=0),
  add column active_recovery_hold_amount private.bpay_next_penny_amount not null default 0 check (active_recovery_hold_amount>=0),
  drop constraint bpay_next_finance_case_check,
  add constraint bpay_next_case_principal_balance check (
    principal_recovered+principal_written_off+principal_paid_credit<=
      case when case_kind in ('LOAN','ADVANCE') then principal_funded else principal_approved end),
  add constraint bpay_next_case_funding_shape check (principal_funded<=principal_approved
    and (case_kind in ('LOAN','ADVANCE') or principal_funded=0)
    and (case_kind='CREDIT' or principal_paid_credit=0)
    and (case_kind<>'CREDIT' or principal_recovered=0)),
  add constraint bpay_next_case_kind_identity_key unique (id,candidate_id,case_kind);
alter table private.bpay_next_case_rule add constraint bpay_next_case_rule_kind_fk
  foreign key (case_id,candidate_id,case_kind) references private.bpay_next_finance_case(id,candidate_id,case_kind) on delete restrict;
alter table private.bpay_next_case_component add constraint bpay_next_case_credit_not_debt
  check (case_kind<>'CREDIT' or (recovered_source_ex_vat=0 and active_recovery_source_ex_vat=0));

alter table private.bpay_next_case_event
  alter column approved_delta type private.bpay_next_penny_amount,
  alter column funded_delta type private.bpay_next_penny_amount,
  alter column recovered_delta type private.bpay_next_penny_amount,
  alter column written_off_delta type private.bpay_next_penny_amount,
  drop constraint bpay_next_case_event_event_kind_check,
  add constraint bpay_next_case_event_kind_check check (event_kind in ('APPROVED','FUNDED','RECOVERED','PAID','SHORTFALL','WRITTEN_OFF','CORRECTED')),
  add column instruction_id uuid,
  add column allocation_result_id uuid,
  add column run_worker_id uuid,
  add column candidate_id uuid,
  add column case_component_id uuid,
  add column event_case_kind text,
  add column hold_purpose text,
  add column pay_week_start date,
  add column source_amount_ex_vat private.bpay_next_penny_amount check (source_amount_ex_vat>=0),
  add column target_amount_ex_vat private.bpay_next_penny_amount check (target_amount_ex_vat>=0),
  add column target_amount_vat private.bpay_next_penny_amount check (target_amount_vat>=0),
  add column target_amount_inc_vat private.bpay_next_penny_amount check (target_amount_inc_vat>=0),
  add column paid_credit_delta private.bpay_next_penny_amount not null default 0,
  add column shortfall_source_ex_vat private.bpay_next_penny_amount check (shortfall_source_ex_vat>=0),
  add column cap_reason text check (cap_reason in ('TAKE_HOME_FLOOR','EARNINGS_THRESHOLD','PAY_HEADROOM','CASE_CAPACITY')),
  add constraint bpay_next_case_event_typed_key unique (id,case_component_id,case_id,candidate_id,pay_week_start,hold_purpose),
  add constraint bpay_next_case_event_hold_scope_fk foreign key (instruction_id,run_worker_id,candidate_id,case_component_id,case_id,hold_purpose,pay_week_start)
    references private.bpay_next_run_case_instruction(id,run_worker_id,candidate_id,case_component_id,case_id,hold_purpose,pay_week_start) on delete restrict,
  add constraint bpay_next_case_event_instruction_fk foreign key (instruction_id,case_id,candidate_id,case_component_id,event_case_kind)
    references private.bpay_next_run_case_instruction(id,case_id,candidate_id,case_component_id,case_kind) on delete restrict,
  add constraint bpay_next_case_event_result_fk foreign key (allocation_result_id,instruction_id)
    references private.bpay_next_case_allocation_result(id,instruction_id) on delete restrict,
  add constraint bpay_next_case_event_worker_fk foreign key (run_worker_id,candidate_id)
    references private.bpay_next_run_worker(id,candidate_id) on delete restrict,
  add constraint bpay_next_case_event_transfer_fk foreign key (original_transfer_id,run_worker_id)
    references private.bpay_next_transfer(id,run_worker_id) on delete restrict,
  add constraint bpay_next_case_event_typed_shape check (
    (instruction_id is null and allocation_result_id is null and run_worker_id is null and candidate_id is null
      and case_component_id is null and event_case_kind is null and hold_purpose is null and pay_week_start is null
      and source_amount_ex_vat is null and target_amount_ex_vat is null and target_amount_vat is null
      and target_amount_inc_vat is null and shortfall_source_ex_vat is null and cap_reason is null
      and paid_credit_delta=0 and event_kind not in ('PAID','SHORTFALL'))
    or (instruction_id is not null and allocation_result_id is not null and run_worker_id is not null and candidate_id is not null
      and case_component_id is not null and event_case_kind is not null and hold_purpose is not null and pay_week_start is not null
      and source_amount_ex_vat is not null and target_amount_ex_vat is not null and target_amount_vat is not null
      and target_amount_inc_vat is not null and shortfall_source_ex_vat is not null
      and target_amount_inc_vat=target_amount_ex_vat+target_amount_vat
      and (event_kind='SHORTFALL' or source_amount_ex_vat>0)
      and ((event_kind='FUNDED' and event_case_kind in ('LOAN','ADVANCE') and hold_purpose='PAYOUT'
        and funded_delta=source_amount_ex_vat and approved_delta=0 and recovered_delta=0 and written_off_delta=0 and paid_credit_delta=0 and shortfall_source_ex_vat=0)
        or (event_kind='RECOVERED' and hold_purpose in ('GROSS_RECOVERY','NET_RECOVERY_CAPACITY')
          and recovered_delta=source_amount_ex_vat and funded_delta=0 and approved_delta=0 and written_off_delta=0 and paid_credit_delta=0 and shortfall_source_ex_vat=0)
        or (event_kind='PAID' and event_case_kind='CREDIT' and hold_purpose='PAYOUT'
          and paid_credit_delta=source_amount_ex_vat and approved_delta=0 and funded_delta=0 and recovered_delta=0 and written_off_delta=0 and shortfall_source_ex_vat=0)
        or (event_kind='SHORTFALL' and hold_purpose in ('GROSS_RECOVERY','NET_RECOVERY_CAPACITY') and shortfall_source_ex_vat>0
          and source_amount_ex_vat=0 and target_amount_inc_vat=0 and approved_delta=0 and funded_delta=0
          and recovered_delta=0 and written_off_delta=0 and paid_credit_delta=0 and cap_reason is not null))));
create unique index bpay_next_case_event_original_once_idx on private.bpay_next_case_event(instruction_id,event_kind)
  where instruction_id is not null and event_kind in ('FUNDED','RECOVERED','PAID','SHORTFALL');
create index bpay_next_case_event_component_idx on private.bpay_next_case_event(case_component_id,pay_week_start,id);
create index bpay_next_case_event_result_idx on private.bpay_next_case_event(allocation_result_id,id);
alter table private.bpay_next_case_event add constraint bpay_next_case_event_case_key unique (id,case_id);
alter table private.bpay_next_case_rule add constraint bpay_next_case_rule_funding_event_fk
  foreign key (original_funding_event_id,case_id) references private.bpay_next_case_event(id,case_id) on delete restrict;
create index bpay_next_case_rule_funding_event_idx on private.bpay_next_case_rule(original_funding_event_id,case_id);
alter table private.bpay_next_case_capacity_use add constraint bpay_next_case_capacity_event_fk
  foreign key (realisation_event_id,case_component_id,case_id,candidate_id,pay_week_start,purpose)
    references private.bpay_next_case_event(id,case_component_id,case_id,candidate_id,pay_week_start,hold_purpose) on delete restrict;
alter table private.bpay_next_case_hold alter column amount type private.bpay_next_penny_amount;

-- CASE_PAYOUT is its own zero-work cash basis, not a fabricated payroll net.
alter table private.bpay_next_net_projection
  drop constraint bpay_next_net_projection_input_kind_check,
  drop constraint bpay_next_net_projection_check1,
  add constraint bpay_next_projection_input_kind check (input_kind in ('PAYE_MANUAL','PAYE_IMPORT','UMBRELLA','CASE_PAYOUT')),
  add constraint bpay_next_projection_net_nullability check ((input_kind in ('UMBRELLA','CASE_PAYOUT'))=(entered_paye_net is null)),
  add column accepted_net_additions private.bpay_next_penny_amount not null default 0 check (accepted_net_additions>=0),
  add column accepted_gross_additions private.bpay_next_penny_amount not null default 0 check (accepted_gross_additions>=0),
  add column accepted_gross_deductions private.bpay_next_penny_amount not null default 0 check (accepted_gross_deductions>=0),
  add constraint bpay_next_projection_case_cash check (
    (input_kind='UMBRELLA')
    or (input_kind in ('PAYE_MANUAL','PAYE_IMPORT') and cash_amount=entered_paye_net+accepted_net_additions-accepted_recoveries)
    or (input_kind='CASE_PAYOUT' and gross_ex_vat=0 and gross_vat=0 and gross_inc_vat=0
      and accepted_gross_additions=0 and accepted_gross_deductions=0 and accepted_recoveries=0 and cash_amount=accepted_net_additions));
create table private.bpay_next_case_payout_basis (
  run_worker_id uuid not null,
  candidate_id uuid not null,
  preparation_revision bigint not null check (preparation_revision>0),
  projection_id uuid,
  currency text not null check (currency='GBP'),
  beneficiary_kind text not null check (beneficiary_kind='CANDIDATE'),
  beneficiary_id uuid not null,
  expected_instruction_count bigint not null check (expected_instruction_count>0),
  captured_instruction_count bigint not null check (captured_instruction_count>=0),
  source_total_ex_vat private.bpay_next_penny_amount not null check (source_total_ex_vat>=0),
  target_total_ex_vat private.bpay_next_penny_amount not null check (target_total_ex_vat>=0),
  target_total_vat private.bpay_next_penny_amount not null check (target_total_vat>=0),
  target_total_inc_vat private.bpay_next_penny_amount not null check (target_total_inc_vat>=0),
  status text not null check (status in ('BUILDING','SEALED')),
  primary key (run_worker_id,preparation_revision),
  foreign key (run_worker_id,candidate_id) references private.bpay_next_run_worker(id,candidate_id) on delete restrict,
  foreign key (projection_id,run_worker_id) references private.bpay_next_net_projection(id,run_worker_id) on delete restrict,
  check (beneficiary_id=candidate_id),
  check (captured_instruction_count<=expected_instruction_count),
  check (target_total_inc_vat=target_total_ex_vat+target_total_vat),
  check (status<>'SEALED' or captured_instruction_count=expected_instruction_count)
);
create index bpay_next_case_payout_projection_idx on private.bpay_next_case_payout_basis(projection_id,run_worker_id);

-- Maintained week facts avoid an all-run/history SUM in allocation. Original
-- payroll ownership is unique; cash reissue updates that contribution rather
-- than manufacturing another earnings contribution. B1 is NOT chosen here.
create table private.bpay_next_worker_week (
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  pay_week_start date not null check (pg_catalog.isfinite(pay_week_start) and extract(isodow from pay_week_start)=1),
  period_revision bigint not null check (period_revision>0),
  default_floor private.bpay_next_penny_amount not null check (default_floor>=0),
  default_floor_revision bigint not null check (default_floor_revision>0),
  resolved_arranged_take_home private.bpay_next_penny_amount not null check (resolved_arranged_take_home>=0),
  unresolved_contribution_count bigint not null default 0 check (unresolved_contribution_count>=0),
  return_floor_binding_ref text check (pg_catalog.octet_length(return_floor_binding_ref) between 1 and 256),
  return_floor_binding_revision bigint check (return_floor_binding_revision>0),
  primary key (candidate_id,pay_week_start),
  check ((return_floor_binding_ref is null)=(return_floor_binding_revision is null))
);
create table private.bpay_next_worker_week_contribution (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  candidate_id uuid not null,
  pay_week_start date not null,
  original_run_worker_id uuid not null,
  original_pay_date date not null check (pg_catalog.isfinite(original_pay_date)),
  original_created_at_utc timestamptz not null check (pg_catalog.isfinite(original_created_at_utc)),
  original_transfer_id uuid,
  returned_cash_id uuid,
  reissue_transfer_id uuid,
  contribution_revision bigint not null check (contribution_revision>0),
  basis_kind text not null check (basis_kind in ('PAYROLL_NET','GROSS_FALLBACK')),
  original_gross_amount private.bpay_next_penny_amount not null check (original_gross_amount>=0),
  original_payroll_net private.bpay_next_penny_amount check (original_payroll_net>=0),
  payment_state text not null check (payment_state in ('ARRANGED','PAID','FAILED','CANCELLED','RETURNED_OWED','REISSUED_PAID')),
  eligibility_state text not null check (eligibility_state in ('ELIGIBLE','EXCLUDED','UNBOUND')),
  eligible_arranged_amount private.bpay_next_penny_amount check (eligible_arranged_amount>=0),
  return_floor_binding_ref text check (pg_catalog.octet_length(return_floor_binding_ref) between 1 and 256),
  primary_binding_revision bigint not null check (primary_binding_revision>0),
  unique (original_run_worker_id),
  foreign key (candidate_id,pay_week_start) references private.bpay_next_worker_week(candidate_id,pay_week_start) on delete restrict,
  foreign key (original_run_worker_id,candidate_id) references private.bpay_next_run_worker(id,candidate_id) on delete restrict,
  foreign key (original_transfer_id,original_run_worker_id) references private.bpay_next_transfer(id,run_worker_id) on delete restrict,
  foreign key (returned_cash_id,original_transfer_id,candidate_id) references private.bpay_next_return_cash(id,original_transfer_id,candidate_id) on delete restrict,
  foreign key (reissue_transfer_id,candidate_id) references private.bpay_next_transfer(id,candidate_id) on delete restrict,
  check (original_pay_date>=pay_week_start and original_pay_date<pay_week_start+7),
  check ((basis_kind='PAYROLL_NET' and original_payroll_net is not null)
    or (basis_kind='GROSS_FALLBACK' and original_payroll_net is null)),
  check ((eligibility_state='UNBOUND' and eligible_arranged_amount is null)
    or (eligibility_state='EXCLUDED' and eligible_arranged_amount is not null and eligible_arranged_amount=0)
    or (eligibility_state='ELIGIBLE' and eligible_arranged_amount is not null and eligible_arranged_amount=case when basis_kind='PAYROLL_NET' then original_payroll_net else original_gross_amount end)),
  check (payment_state not in ('FAILED','CANCELLED') or eligibility_state='EXCLUDED'),
  check (payment_state not in ('RETURNED_OWED','REISSUED_PAID') or
    (returned_cash_id is not null and original_transfer_id is not null
      and (return_floor_binding_ref is not null or eligibility_state='UNBOUND'))),
  check (payment_state<>'REISSUED_PAID' or reissue_transfer_id is not null)
);
create index bpay_next_worker_week_prior_idx on private.bpay_next_worker_week_contribution
  (candidate_id,pay_week_start,original_pay_date,original_created_at_utc,original_run_worker_id);
create index bpay_next_worker_week_transfer_idx on private.bpay_next_worker_week_contribution(original_transfer_id,id);
create index bpay_next_worker_week_return_idx on private.bpay_next_worker_week_contribution(returned_cash_id,id);
create index bpay_next_worker_week_reissue_idx on private.bpay_next_worker_week_contribution(reissue_transfer_id,id);

alter domain private.bpay_next_penny_amount owner to postgres;
revoke all on type private.bpay_next_penny_amount from public,anon,authenticated,service_role;
alter table private.bpay_next_case_rule owner to postgres;
alter table private.bpay_next_case_component owner to postgres;
alter table private.bpay_next_case_period owner to postgres;
alter table private.bpay_next_case_selection owner to postgres;
alter table private.bpay_next_case_selection_page owner to postgres;
alter table private.bpay_next_case_selection_item owner to postgres;
alter table private.bpay_next_run_case_instruction owner to postgres;
alter table private.bpay_next_case_allocation_state owner to postgres;
alter table private.bpay_next_case_allocation_channel owner to postgres;
alter table private.bpay_next_case_allocation_result owner to postgres;
alter table private.bpay_next_case_capacity_use owner to postgres;
alter table private.bpay_next_case_payout_basis owner to postgres;
alter table private.bpay_next_worker_week owner to postgres;
alter table private.bpay_next_worker_week_contribution owner to postgres;
revoke all on private.bpay_next_case_rule,private.bpay_next_case_component,
  private.bpay_next_case_period,private.bpay_next_case_selection,
  private.bpay_next_case_selection_page,private.bpay_next_case_selection_item,
  private.bpay_next_run_case_instruction,private.bpay_next_case_allocation_state,
  private.bpay_next_case_allocation_channel,private.bpay_next_case_allocation_result,
  private.bpay_next_case_capacity_use,private.bpay_next_case_payout_basis,
  private.bpay_next_worker_week,private.bpay_next_worker_week_contribution
  from public,anon,authenticated,service_role;
-- Existing extended relations retain their already owner-only ACLs.
commit;
