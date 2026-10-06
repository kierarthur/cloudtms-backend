-- Typed immutable receipt for the ordered CASE_CREATE owner. No legacy
-- backfill, funding, fake WORK, public endpoint or runtime activation.
-- Filename is coordinator-assigned for this connected implementation slice.
\set ON_ERROR_STOP on
begin;

create table private.bpay_next_case_create_request (
  command_id uuid primary key references private.bpay_next_command(id) on delete restrict,
  candidate_id uuid not null,
  actor_user_id uuid not null references public.tms_users(id) on delete restrict,
  case_id uuid not null unique,
  rule_id uuid not null unique,
  primary_component_id uuid not null unique,
  recovery_component_id uuid unique,
  case_kind text not null check (case_kind in ('LOAN','ADVANCE','MANUAL_DEBT','CREDIT')),
  case_subtype text not null,
  tax_treatment text not null,
  principal_source_ex_vat private.bpay_next_penny_amount not null check (principal_source_ex_vat>0),
  source_pay_channel text not null check (source_pay_channel in ('PAYE','UMBRELLA')),
  currency text not null check (currency='GBP'),
  valuation_policy_id uuid not null references private.bpay_next_valuation_policy(id) on delete restrict,
  due_date date check (pg_catalog.isfinite(due_date)),
  input_start_monday date check (pg_catalog.isfinite(input_start_monday) and extract(isodow from input_start_monday)=1),
  input_weekly_due private.bpay_next_penny_amount check (input_weekly_due>0),
  input_week_count bigint check (input_week_count>0),
  weekly_due_source_ex_vat private.bpay_next_penny_amount check (weekly_due_source_ex_vat>0),
  schedule_week_count bigint check (schedule_week_count>0),
  minimum_earnings_threshold private.bpay_next_penny_amount check (minimum_earnings_threshold>=0),
  take_home_floor_override private.bpay_next_penny_amount check (take_home_floor_override>=0),
  approval_reason text not null check (pg_catalog.octet_length(approval_reason) between 1 and 2048 and pg_catalog.btrim(approval_reason)<>''),
  accepted_at_utc timestamptz not null check (pg_catalog.isfinite(accepted_at_utc)),
  opening_pay_week_start date not null check (pg_catalog.isfinite(opening_pay_week_start) and extract(isodow from opening_pay_week_start)=1),
  foreign key (command_id,candidate_id) references private.bpay_next_command_member(command_id,candidate_id) on delete restrict,
  check (case_id=command_id and rule_id=command_id and primary_component_id=command_id),
  check ((case_kind='LOAN' and case_subtype='LOAN' and tax_treatment='NOT_APPLICABLE')
    or (case_kind='ADVANCE' and case_subtype='PAYMENT_ADVANCE' and tax_treatment='NOT_APPLICABLE')
    or (case_kind='MANUAL_DEBT' and case_subtype='MANUAL_DEBT' and tax_treatment in ('TAXABLE','NON_TAXABLE'))
    or (case_kind='CREDIT' and case_subtype='MANUAL_CREDIT' and tax_treatment in ('TAXABLE','NON_TAXABLE'))),
  check ((case_kind in ('LOAN','ADVANCE') and recovery_component_id is not null and recovery_component_id<>primary_component_id)
    or (case_kind not in ('LOAN','ADVANCE') and recovery_component_id is null)),
  check ((case_kind='CREDIT' and input_start_monday is null and input_weekly_due is null
      and input_week_count is null and weekly_due_source_ex_vat is null and schedule_week_count is null
      and minimum_earnings_threshold is null and take_home_floor_override is null)
    or (case_kind<>'CREDIT' and input_start_monday is not null
      and (input_weekly_due is not null or input_week_count is not null)
      and weekly_due_source_ex_vat is not null and schedule_week_count is not null
      and weekly_due_source_ex_vat*schedule_week_count>=principal_source_ex_vat))
);
create index bpay_next_case_create_request_candidate_idx on private.bpay_next_case_create_request(candidate_id,command_id);
create index bpay_next_case_create_request_actor_idx on private.bpay_next_case_create_request(actor_user_id,command_id);
create index bpay_next_case_create_request_policy_idx on private.bpay_next_case_create_request(valuation_policy_id,command_id);
alter table private.bpay_next_case_create_request owner to postgres;
revoke all on table private.bpay_next_case_create_request from public,anon,authenticated,service_role;

commit;
