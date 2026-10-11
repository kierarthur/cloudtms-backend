-- One-time CloudTMS schema/data migration: banking_pay_next_case_pause_request
-- FC10 / BD120: additive empty private status audit, no money rows/backfill.

\set ON_ERROR_STOP on

begin;

create table private.bpay_next_case_pause_request (
 command_id uuid primary key,
 candidate_id uuid not null,
 case_id uuid not null,
 actor_user_id uuid not null references public.tms_users(id) on delete restrict,
 action text not null check(action in ('CASE_PAUSE','CASE_RESUME')),
 module_epoch bigint not null check(module_epoch>0),
 command_sequence bigint not null check(command_sequence>0),
 expected_case_revision bigint not null check(expected_case_revision>0 and expected_case_revision<9223372036854775807),
 prior_status text not null,
 target_status text not null,
 reason text check(reason is null or (length(btrim(reason))>0 and octet_length(reason)<=2048)),
 original_args jsonb not null check(jsonb_typeof(original_args)='object' and octet_length(original_args::text)<=16384),
 accepted_at_utc timestamptz not null check(isfinite(accepted_at_utc)),
 unique(command_id,case_id,candidate_id,action),
 foreign key(command_id,command_sequence) references private.bpay_next_command(id,agency_sequence) on delete restrict,
 foreign key(command_id,candidate_id) references private.bpay_next_command_member(command_id,candidate_id) on delete restrict,
 foreign key(case_id,candidate_id) references private.bpay_next_finance_case(id,candidate_id) on delete restrict,
 check((action='CASE_PAUSE' and prior_status='OPEN' and target_status='PAUSED')
    or (action='CASE_RESUME' and prior_status='PAUSED' and target_status='OPEN'))
);
create index bpay_next_case_pause_candidate_idx on private.bpay_next_case_pause_request(candidate_id,case_id,command_sequence);
create index bpay_next_case_pause_actor_idx on private.bpay_next_case_pause_request(actor_user_id,command_id);
create table private.bpay_next_case_pause_outcome (
 command_id uuid primary key,
 candidate_id uuid not null,
 case_id uuid not null,
 action text not null,
 job_id uuid not null unique references private.bpay_next_job(id) on delete restrict,
 outcome text not null check(outcome in ('APPLIED','BLOCKED')),
 applied_status text,
 applied_case_revision bigint,
 issue_code text,
 protection_summary jsonb,
 first_protection jsonb,
 completed_at_utc timestamptz not null check(isfinite(completed_at_utc)),
 foreign key(command_id,case_id,candidate_id,action)
   references private.bpay_next_case_pause_request(command_id,case_id,candidate_id,action) on delete restrict,
 foreign key(command_id,candidate_id,action)
   references private.bpay_next_job(command_id,candidate_id,job_kind) on delete restrict,
 check((outcome='APPLIED' and issue_code is null and applied_case_revision is not null and applied_case_revision>0 and applied_status is not null
    and applied_status=case action when 'CASE_PAUSE' then 'PAUSED' else 'OPEN' end)
   or (outcome='BLOCKED' and applied_status is null and applied_case_revision is null and issue_code is not null
    and issue_code in ('BPAY_NEXT_CASE_PROTECTION_CHANGED','BPAY_NEXT_CASE_PAUSE_UNSUPPORTED',
     'BPAY_NEXT_CASE_PAUSE_STATE_CHANGED','BPAY_NEXT_CASE_PROTECTION_PENDING',
     'BPAY_NEXT_CASE_PROTECTED_SLICE','BPAY_NEXT_CASE_PROTECTION_BALANCE_INVALID'))),
 check(protection_summary is null or (jsonb_typeof(protection_summary)='object' and octet_length(protection_summary::text)<=4096)),
 check(first_protection is null or (jsonb_typeof(first_protection)='object' and octet_length(first_protection::text)<=4096))
);
create index bpay_next_case_pause_outcome_case_idx on private.bpay_next_case_pause_outcome(case_id,candidate_id,completed_at_utc,command_id);
alter table private.bpay_next_case_pause_request owner to postgres;
alter table private.bpay_next_case_pause_outcome owner to postgres;
revoke all on private.bpay_next_case_pause_request,private.bpay_next_case_pause_outcome from public,anon,authenticated,service_role;

commit;
