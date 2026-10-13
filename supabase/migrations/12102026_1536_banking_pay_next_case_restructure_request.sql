-- One-time CloudTMS schema/data migration: banking_pay_next_case_restructure_request
-- FC12 / BD147: empty private immutable arrangement receipts, no financial backfill.

\set ON_ERROR_STOP on

begin;

create table private.bpay_next_case_restructure_request (
 command_id uuid primary key,
 candidate_id uuid not null,
 case_id uuid not null,
 actor_user_id uuid not null references public.tms_users(id) on delete restrict,
 module_epoch bigint not null check(module_epoch>0),
 command_sequence bigint not null check(command_sequence>0),
 expected_case_revision bigint not null check(expected_case_revision>0 and expected_case_revision<9223372036854775807),
 previous_rule_id uuid not null,
 previous_rule_revision bigint not null check(previous_rule_revision>0 and previous_rule_revision<9223372036854775807),
 new_rule_id uuid not null unique,
 new_rule_revision bigint not null check(new_rule_revision=previous_rule_revision+1),
 original_command_id uuid not null references private.bpay_next_case_create_request(command_id) on delete restrict,
 approved_principal private.bpay_next_penny_amount not null check(approved_principal>0),
 approval_input jsonb not null check(jsonb_typeof(approval_input)='object' and octet_length(approval_input::text)<=12288),
 approval_edit_command_id uuid references private.bpay_next_case_edit_request(command_id) on delete restrict,
 input jsonb not null check(jsonb_typeof(input)='object' and octet_length(input::text)<=8192),
 terms jsonb not null check(jsonb_typeof(terms)='object' and octet_length(terms::text)<=8192),
 preview_token jsonb not null check(jsonb_typeof(preview_token)='object' and octet_length(preview_token::text)<=16384),
 accepted_at_utc timestamptz not null check(isfinite(accepted_at_utc)),
 unique(command_id,case_id,candidate_id),
 foreign key(command_id,command_sequence) references private.bpay_next_command(id,agency_sequence) on delete restrict,
 foreign key(command_id,candidate_id) references private.bpay_next_command_member(command_id,candidate_id) on delete restrict,
 foreign key(case_id,candidate_id) references private.bpay_next_finance_case(id,candidate_id) on delete restrict,
 foreign key(previous_rule_id,case_id,candidate_id,previous_rule_revision)
  references private.bpay_next_case_rule(id,case_id,candidate_id,rule_revision) on delete restrict,
 check(new_rule_id=command_id)
);
create index bpay_next_case_restructure_candidate_idx on private.bpay_next_case_restructure_request(candidate_id,case_id,command_sequence);
create index bpay_next_case_restructure_actor_idx on private.bpay_next_case_restructure_request(actor_user_id,command_id);
create table private.bpay_next_case_restructure_outcome (
 command_id uuid primary key,
 case_id uuid not null,
 candidate_id uuid not null,
 job_id uuid not null unique references private.bpay_next_job(id) on delete restrict,
 outcome text not null check(outcome in ('APPLIED','BLOCKED')),
 applied_case_revision bigint,
 applied_rule_id uuid,
 issue_code text,
 completed_at_utc timestamptz not null check(isfinite(completed_at_utc)),
 foreign key(command_id,case_id,candidate_id) references private.bpay_next_case_restructure_request(command_id,case_id,candidate_id) on delete restrict,
 foreign key(applied_rule_id,case_id,candidate_id) references private.bpay_next_case_rule(id,case_id,candidate_id) on delete restrict,
 check((outcome='APPLIED' and applied_case_revision>0 and applied_case_revision is not null and applied_rule_id=command_id and applied_rule_id is not null and issue_code is null)
  or(outcome='BLOCKED' and applied_case_revision is null and applied_rule_id is null and issue_code in
    ('BPAY_NEXT_CASE_PROTECTION_CHANGED','BPAY_NEXT_CASE_RESTRUCTURE_UNSUPPORTED','BPAY_NEXT_CASE_RESTRUCTURE_NO_BALANCE',
     'BPAY_NEXT_CASE_RESTRUCTURE_TOTAL_CHANGED','BPAY_NEXT_CASE_PROTECTION_BALANCE_INVALID') and issue_code is not null))
);
create index bpay_next_case_restructure_outcome_case_idx on private.bpay_next_case_restructure_outcome(case_id,candidate_id,completed_at_utc,command_id);
create table private.bpay_next_case_restructure_period (
 restructure_command_id uuid not null references private.bpay_next_case_restructure_request(command_id) on delete restrict,
 case_component_id uuid not null,
 pay_week_start date not null,
 period_revision bigint not null check(period_revision>1),
 previous_period_revision bigint not null check(previous_period_revision=period_revision-1),
 case_id uuid not null,
 candidate_id uuid not null,
 previous_rule_id uuid not null,
 rule_id uuid not null,
 previous_opening_outstanding private.bpay_next_penny_amount not null check(previous_opening_outstanding>=0),
 previous_opening_due private.bpay_next_penny_amount not null check(previous_opening_due>=0),
 opening_outstanding private.bpay_next_penny_amount not null check(opening_outstanding>=0),
 opening_due private.bpay_next_penny_amount not null check(opening_due>=0),
 captured_at_utc timestamptz not null default transaction_timestamp(),
 primary key(case_component_id,pay_week_start,period_revision),
 foreign key(case_component_id,case_id,candidate_id) references private.bpay_next_case_component(id,case_id,candidate_id) on delete restrict,
 foreign key(case_component_id,pay_week_start) references private.bpay_next_case_period(case_component_id,pay_week_start) on delete restrict,
 foreign key(previous_rule_id,case_id,candidate_id) references private.bpay_next_case_rule(id,case_id,candidate_id) on delete restrict,
 foreign key(rule_id,case_id,candidate_id) references private.bpay_next_case_rule(id,case_id,candidate_id) on delete restrict,
 foreign key(restructure_command_id,case_id,candidate_id) references private.bpay_next_case_restructure_request(command_id,case_id,candidate_id) on delete restrict
);
create index bpay_next_case_restructure_period_request_idx on private.bpay_next_case_restructure_period(restructure_command_id,case_component_id,pay_week_start);
alter table private.bpay_next_case_restructure_request owner to postgres;
alter table private.bpay_next_case_restructure_outcome owner to postgres;
alter table private.bpay_next_case_restructure_period owner to postgres;
revoke all on private.bpay_next_case_restructure_request,private.bpay_next_case_restructure_outcome,private.bpay_next_case_restructure_period from public,anon,authenticated,service_role;

commit;
