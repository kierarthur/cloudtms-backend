-- One-time CloudTMS schema/data migration: banking_pay_next_case_channel_resolution_receipts
-- State the exact authority, safety boundary, and verification before implementation.

\set ON_ERROR_STOP on

begin;

-- FC13 / BD148: empty private decisions; no backfill or money action.
create table private.bpay_next_case_channel_request (
 command_id uuid primary key,
 case_id uuid not null,
 candidate_id uuid not null,
 actor_user_id uuid not null references public.tms_users(id) on delete restrict,
 module_epoch bigint not null check(module_epoch>0),
 command_sequence bigint not null check(command_sequence>0),
 expected_case_revision bigint not null check(expected_case_revision>0 and expected_case_revision<9223372036854775807),
 previous_rule_id uuid not null,
 previous_rule_revision bigint not null check(previous_rule_revision>0 and previous_rule_revision<9223372036854775807),
 new_rule_id uuid unique,
 new_rule_revision bigint,
 original_command_id uuid references private.bpay_next_case_create_request(command_id) on delete restrict,
 approval_edit_command_id uuid references private.bpay_next_case_edit_request(command_id) on delete restrict,
 approved_principal private.bpay_next_penny_amount,
 approval_input jsonb,
 input jsonb not null check(jsonb_typeof(input)='object' and octet_length(input::text)<=8192),
 preview_token jsonb not null check(jsonb_typeof(preview_token)='object' and octet_length(preview_token::text)<=32768),
 target_pay_channel text not null check(target_pay_channel in ('PAYE','UMBRELLA')),
 target_umbrella_id uuid references public.umbrellas(id) on delete restrict,
 target_umbrella_vat_chargeable boolean,
 effective_date date not null check(isfinite(effective_date) and effective_date between date '0001-01-01' and date '9999-12-31'),
 source_basis private.bpay_next_penny_amount not null check(source_basis>0),
 target_total_ex_vat private.bpay_next_penny_amount not null check(target_total_ex_vat>0),
 target_total_vat private.bpay_next_penny_amount not null check(target_total_vat>=0),
 target_total_inc_vat private.bpay_next_penny_amount not null,
 target_terms jsonb,
 source_terms jsonb,
 expected_component_count bigint not null check(expected_component_count>0),
 accepted_at_utc timestamptz not null check(isfinite(accepted_at_utc)),
 unique(command_id,case_id,candidate_id),
 foreign key(command_id,command_sequence) references private.bpay_next_command(id,agency_sequence) on delete restrict,
 foreign key(command_id,candidate_id) references private.bpay_next_command_member(command_id,candidate_id) on delete restrict,
 foreign key(case_id,candidate_id) references private.bpay_next_finance_case(id,candidate_id) on delete restrict,
 foreign key(previous_rule_id,case_id,candidate_id,previous_rule_revision)
  references private.bpay_next_case_rule(id,case_id,candidate_id,rule_revision) on delete restrict,
 check(target_total_inc_vat=target_total_ex_vat+target_total_vat),
 check((target_pay_channel='PAYE' and target_umbrella_id is null and target_umbrella_vat_chargeable is null)
  or(target_pay_channel='UMBRELLA' and target_umbrella_id is not null and target_umbrella_vat_chargeable is not null)),
 check((new_rule_id is null and new_rule_revision is null and original_command_id is null
   and approved_principal is null and approval_input is null and approval_edit_command_id is null)
  or(new_rule_id is not null and new_rule_id=command_id and new_rule_revision is not null
   and new_rule_revision=previous_rule_revision+1 and original_command_id is not null
   and approved_principal is not null and approved_principal>0 and approval_input is not null
   and jsonb_typeof(approval_input)='object' and octet_length(approval_input::text)<=12288)),
 check((target_terms is null and source_terms is null)
  or(target_terms is not null and source_terms is not null and jsonb_typeof(target_terms)='object'
   and jsonb_typeof(source_terms)='object' and octet_length(target_terms::text)<=8192 and octet_length(source_terms::text)<=8192))
);
create index bpay_next_case_channel_request_candidate_idx on private.bpay_next_case_channel_request(candidate_id,case_id,command_sequence);
create index bpay_next_case_channel_request_actor_idx on private.bpay_next_case_channel_request(actor_user_id,command_id);

create table private.bpay_next_case_channel_component (
 command_id uuid not null,
 case_component_id uuid not null,
 case_id uuid not null,
 candidate_id uuid not null,
 component_ordinal bigint not null check(component_ordinal>0),
 previous_component_revision bigint not null check(previous_component_revision>0 and previous_component_revision<9223372036854775807),
 applied_component_revision bigint not null check(applied_component_revision=previous_component_revision+1),
 original_rule_id uuid not null,
 applied_rule_id uuid not null,
 source_pay_channel text not null check(source_pay_channel in ('PAYE','UMBRELLA')),
 target_pay_channel text not null check(target_pay_channel in ('PAYE','UMBRELLA')),
 tax_treatment text not null check(tax_treatment in ('TAXABLE','NON_TAXABLE')),
 instruction_kind text not null check(instruction_kind in ('RECOVERY','CREDIT')),
 applied_basis_source_amount_ex_vat private.bpay_next_penny_amount not null check(applied_basis_source_amount_ex_vat>=0),
 target_ex_vat private.bpay_next_penny_amount not null check(target_ex_vat>=0),
 target_vat private.bpay_next_penny_amount not null check(target_vat>=0),
 target_inc_vat private.bpay_next_penny_amount not null check(target_inc_vat=target_ex_vat+target_vat),
 fixed_reimbursement boolean not null,
 resolution_mode public.pay_finance_component_resolution_mode_enum not null,
 valuation_policy_id uuid not null,
 valuation_window_id uuid not null,
 quote_operands jsonb not null check(jsonb_typeof(quote_operands)='object' and octet_length(quote_operands::text)<=8192),
 original_component jsonb not null check(jsonb_typeof(original_component)='object' and octet_length(original_component::text)<=16384),
 captured_at_utc timestamptz not null check(isfinite(captured_at_utc)),
 primary key(command_id,case_component_id),
 unique(command_id,component_ordinal),
 foreign key(command_id,case_id,candidate_id) references private.bpay_next_case_channel_request(command_id,case_id,candidate_id) on delete restrict,
 foreign key(case_component_id,case_id,candidate_id) references private.bpay_next_case_component(id,case_id,candidate_id) on delete restrict,
 foreign key(original_rule_id,case_id,candidate_id) references private.bpay_next_case_rule(id,case_id,candidate_id) on delete restrict,
 foreign key(applied_rule_id,case_id,candidate_id) references private.bpay_next_case_rule(id,case_id,candidate_id) on delete restrict,
 foreign key(valuation_policy_id,valuation_window_id) references private.bpay_next_valuation_policy_window(policy_id,source_window_id) on delete restrict,
 check(resolution_mode in ('SUGGESTED_EQUIVALENT_BASIS','MANUAL_AMOUNT')),
 check(fixed_reimbursement=(tax_treatment='NON_TAXABLE')),
 check(not fixed_reimbursement or target_ex_vat=applied_basis_source_amount_ex_vat)
);
create index bpay_next_case_channel_component_version_idx on private.bpay_next_case_channel_component(case_component_id,applied_component_revision,command_id);
create index bpay_next_case_channel_component_case_idx on private.bpay_next_case_channel_component(case_id,candidate_id,command_id);

create table private.bpay_next_case_channel_outcome (
 command_id uuid primary key,
 case_id uuid not null,
 candidate_id uuid not null,
 job_id uuid not null unique references private.bpay_next_job(id) on delete restrict,
 outcome text not null check(outcome in ('APPLIED','BLOCKED')),
 applied_case_revision bigint,
 applied_rule_id uuid,
 applied_component_count bigint not null check(applied_component_count>=0),
 issue_code text,
 completed_at_utc timestamptz not null check(isfinite(completed_at_utc)),
 foreign key(command_id,case_id,candidate_id) references private.bpay_next_case_channel_request(command_id,case_id,candidate_id) on delete restrict,
 foreign key(applied_rule_id,case_id,candidate_id) references private.bpay_next_case_rule(id,case_id,candidate_id) on delete restrict,
 check((outcome='APPLIED' and applied_case_revision is not null and applied_case_revision>0
   and applied_component_count>0 and issue_code is null)
  or(outcome='BLOCKED' and applied_case_revision is null and applied_rule_id is null
   and applied_component_count=0 and issue_code is not null and issue_code in
    ('BPAY_NEXT_CASE_PROTECTION_CHANGED','BPAY_NEXT_CASE_CHANNEL_UNSUPPORTED','BPAY_NEXT_CASE_CHANNEL_NO_BALANCE',
     'BPAY_NEXT_CASE_PREVIEW_CHANGED','BPAY_NEXT_CASE_PROTECTION_BALANCE_INVALID','BPAY_NEXT_CASE_CHANNEL_CONTEXT_UNAVAILABLE')))
);
create index bpay_next_case_channel_outcome_case_idx on private.bpay_next_case_channel_outcome(case_id,candidate_id,completed_at_utc,command_id);

alter table private.bpay_next_case_channel_request owner to postgres;
alter table private.bpay_next_case_channel_component owner to postgres;
alter table private.bpay_next_case_channel_outcome owner to postgres;
revoke all on private.bpay_next_case_channel_request,private.bpay_next_case_channel_component,
 private.bpay_next_case_channel_outcome from public,anon,authenticated,service_role;

commit;
