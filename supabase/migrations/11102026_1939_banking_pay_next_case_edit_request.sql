-- One-time CloudTMS schema/data migration: banking_pay_next_case_edit_request
-- State the exact authority, safety boundary, and verification before implementation.

\set ON_ERROR_STOP on

begin;

-- FC09 immutable approved amendment and exact period opening history only.
-- Empty additive private storage; no public trigger, backfill or money action.
create table private.bpay_next_case_edit_request (
 command_id uuid primary key references private.bpay_next_command(id) on delete restrict,
 candidate_id uuid not null,
 actor_user_id uuid not null references public.tms_users(id) on delete restrict,
 case_id uuid not null,
 original_command_id uuid not null references private.bpay_next_case_create_request(command_id) on delete restrict,
 expected_case_revision bigint not null check(expected_case_revision>0),
 previous_rule_id uuid not null,
 previous_rule_revision bigint not null check(previous_rule_revision>0),
 new_rule_id uuid not null unique,
 new_rule_revision bigint not null check(new_rule_revision=previous_rule_revision+1),
 previous_principal private.bpay_next_penny_amount not null check(previous_principal>0),
 principal private.bpay_next_penny_amount not null check(principal>0),
 input jsonb not null check(jsonb_typeof(input)='object' and octet_length(input::text)<=12288),
 terms jsonb not null check(jsonb_typeof(terms)='object' and octet_length(terms::text)<=12288),
 preview_token jsonb not null check(jsonb_typeof(preview_token)='object' and octet_length(preview_token::text)<=32768),
 accepted_at_utc timestamptz not null check(isfinite(accepted_at_utc)),
 unique(command_id,case_id,candidate_id),
 foreign key(command_id,candidate_id) references private.bpay_next_command_member(command_id,candidate_id) on delete restrict,
 foreign key(case_id,candidate_id) references private.bpay_next_finance_case(id,candidate_id) on delete restrict,
 foreign key(previous_rule_id,case_id,candidate_id,previous_rule_revision)
  references private.bpay_next_case_rule(id,case_id,candidate_id,rule_revision) on delete restrict,
 check(new_rule_id=command_id and new_rule_id<>previous_rule_id),
 check((input->>'candidate_id')::uuid=candidate_id and (input->>'principal')::numeric=principal)
);
create index bpay_next_case_edit_candidate_idx on private.bpay_next_case_edit_request(candidate_id,case_id,command_id);
create index bpay_next_case_edit_previous_rule_idx on private.bpay_next_case_edit_request(previous_rule_id,command_id);
create index bpay_next_case_edit_original_idx on private.bpay_next_case_edit_request(original_command_id,command_id);
create index bpay_next_case_edit_actor_idx on private.bpay_next_case_edit_request(actor_user_id,command_id);
create index bpay_next_case_nonapproved_event_idx on private.bpay_next_case_event(case_id,id) where event_kind<>'APPROVED';
create index bpay_next_case_period_used_idx on private.bpay_next_case_period(case_component_id,pay_week_start)
 where realised_recovery_source_ex_vat<>0 or active_unrealised_recovery_source_ex_vat<>0 or active_payout_source_ex_vat<>0;

create table private.bpay_next_case_period_amendment (
 edit_command_id uuid not null,
 case_component_id uuid not null,
 case_id uuid not null,
 candidate_id uuid not null,
 pay_week_start date not null check(isfinite(pay_week_start) and extract(isodow from pay_week_start)=1),
 previous_period_revision bigint not null check(previous_period_revision>0),
 period_revision bigint not null check(period_revision=previous_period_revision+1),
 previous_rule_id uuid not null,
 rule_id uuid not null,
 previous_opening_outstanding private.bpay_next_penny_amount not null check(previous_opening_outstanding>=0),
 previous_opening_due private.bpay_next_penny_amount not null check(previous_opening_due>=0 and previous_opening_due<=previous_opening_outstanding),
 opening_outstanding private.bpay_next_penny_amount not null check(opening_outstanding>=0),
 opening_due private.bpay_next_penny_amount not null check(opening_due>=0 and opening_due<=opening_outstanding),
 recorded_at_utc timestamptz not null default transaction_timestamp(),
 primary key(case_component_id,pay_week_start,period_revision),
 foreign key(edit_command_id,case_id,candidate_id) references private.bpay_next_case_edit_request(command_id,case_id,candidate_id) on delete restrict,
 foreign key(case_component_id,case_id,candidate_id,pay_week_start)
  references private.bpay_next_case_period(case_component_id,case_id,candidate_id,pay_week_start) on delete restrict,
 foreign key(previous_rule_id,case_id,candidate_id) references private.bpay_next_case_rule(id,case_id,candidate_id) on delete restrict,
 foreign key(rule_id,case_id,candidate_id) references private.bpay_next_case_rule(id,case_id,candidate_id) on delete restrict,
 check(previous_rule_id<>rule_id)
);
create index bpay_next_case_period_amendment_edit_idx on private.bpay_next_case_period_amendment(edit_command_id,case_component_id,pay_week_start);
create index bpay_next_case_period_amendment_rule_idx on private.bpay_next_case_period_amendment(rule_id,case_component_id,pay_week_start);
create index bpay_next_case_period_amendment_previous_idx on private.bpay_next_case_period_amendment(previous_rule_id,case_component_id,pay_week_start);
alter table private.bpay_next_case_edit_request owner to postgres;
alter table private.bpay_next_case_period_amendment owner to postgres;
revoke all on table private.bpay_next_case_edit_request,private.bpay_next_case_period_amendment from public,anon,authenticated,service_role;

commit;
