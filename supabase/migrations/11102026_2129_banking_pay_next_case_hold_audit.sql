-- FC14 / BD121: real native hold transitions, empty additive history only.
\set ON_ERROR_STOP on
begin;
create table private.bpay_next_case_hold_transition (
 hold_id uuid not null references private.bpay_next_case_hold(id) on delete restrict,
 event_ordinal smallint not null check(event_ordinal in (1,2)),
 transition text not null check(transition in ('ACTIVE','RELEASED','REALISED')),
 case_id uuid not null, candidate_id uuid not null,
 run_worker_id uuid not null, case_component_id uuid not null,
 instruction_id uuid not null references private.bpay_next_run_case_instruction(id) on delete restrict,
 allocation_result_id uuid not null references private.bpay_next_case_allocation_result(id) on delete restrict,
 cause_job_id uuid not null references private.bpay_next_job(id) on delete restrict,
 command_id uuid not null references private.bpay_next_command(id) on delete restrict,
 command_kind text not null,
 actor_kind text not null check(actor_kind in ('HUMAN','SYSTEM','UNKNOWN')),
 actor_user_id uuid references public.tms_users(id) on delete restrict,
 reason text check(reason is null or octet_length(reason)<=4096),
 source_amount_ex_vat numeric(18,2) not null check(source_amount_ex_vat>0),
 target_amount_ex_vat numeric(18,2) not null check(target_amount_ex_vat>=0),
 target_amount_vat numeric(18,2) not null check(target_amount_vat>=0),
 target_amount_inc_vat numeric(18,2) not null,
 occurred_at_utc timestamptz not null check(isfinite(occurred_at_utc)),
 primary key(hold_id,event_ordinal),
 foreign key(case_component_id,case_id,candidate_id) references private.bpay_next_case_component(id,case_id,candidate_id) on delete restrict,
 foreign key(run_worker_id,candidate_id) references private.bpay_next_run_worker(id,candidate_id) on delete restrict,
 check((event_ordinal=1 and transition='ACTIVE') or(event_ordinal=2 and transition in ('RELEASED','REALISED'))),
 check((actor_kind='HUMAN')=(actor_user_id is not null)),
 check(target_amount_inc_vat=target_amount_ex_vat+target_amount_vat)
);
create index bpay_next_case_hold_transition_case_time_idx on private.bpay_next_case_hold_transition(case_id,candidate_id,occurred_at_utc,hold_id,event_ordinal);
alter table private.bpay_next_case_hold_transition owner to postgres;
alter table private.bpay_next_case_hold_transition enable row level security;
revoke all on private.bpay_next_case_hold_transition from public,anon,authenticated,service_role;
create trigger bpay_next_case_hold_transition_immutable_v1 before update or delete on private.bpay_next_case_hold_transition
 for each row execute function private.bpay_next_effect_immutable_v1();
create trigger bpay_next_case_hold_transition_truncate_v1 before truncate on private.bpay_next_case_hold_transition
 for each statement execute function private.bpay_next_effect_immutable_v1();
-- These sources retain actual timestamps; no history is manufactured from current status.
create index bpay_next_case_effect_audit_idx on private.bpay_next_financial_effect(case_id,occurred_at_utc,id) where case_id is not null;
create index bpay_next_case_edit_audit_idx on private.bpay_next_case_edit_request(case_id,candidate_id,accepted_at_utc,command_id);
create index bpay_next_case_pause_audit_idx on private.bpay_next_case_pause_request(case_id,candidate_id,accepted_at_utc,command_id);
create index bpay_next_case_snooze_audit_idx on private.bpay_next_snooze_event((after_state->>'case_id'),candidate_id,occurred_at_utc,id)
 where after_state->>'scope_kind'='CASE';
create index bpay_next_case_net_audit_idx on private.bpay_next_paye_net_request(case_draft_state_id,accepted_at_utc,command_id);
create index bpay_next_case_instruction_audit_idx on private.bpay_next_run_case_instruction(case_id,candidate_id,run_worker_id,preparation_revision,selection_revision,id);
commit;
