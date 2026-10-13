-- WB22 actor-local choice memory. No money authority, public trigger or backfill.
\set ON_ERROR_STOP on
begin;
create table private.bpay_next_selection_mode_memory (
 run_id uuid not null references private.bpay_next_pay_run(id),
 row_kind text not null check(row_kind in ('HEADER','LEAF')),
 leaf_key text not null check(octet_length(leaf_key) between 1 and 600),
 agency_id uuid not null,owner_epoch bigint not null check(owner_epoch>0),
 actor_user_id uuid not null references public.tms_users(id),official_pay_date date not null,
 scope jsonb not null check(jsonb_typeof(scope)='object' and octet_length(scope::text)<=512),
 mode text not null check(mode in ('ALL_MATCHING','NONE')),
 intent_revision bigint not null check(intent_revision>=0),
 baseline_run_id uuid,candidate_id uuid,work_id uuid,approved_line_id uuid,case_component_id uuid,
 basis jsonb not null check(jsonb_typeof(basis)='object' and octet_length(basis::text)<=8192),
 raw_intent jsonb,accepted_context jsonb,command_id uuid,agency_sequence bigint,
 captured_at_utc timestamptz not null default transaction_timestamp(),acknowledged_at_utc timestamptz,
 primary key(run_id,row_kind,leaf_key),
 check((row_kind='HEADER' and leaf_key='HEADER' and candidate_id is null and work_id is null
  and approved_line_id is null and case_component_id is null and raw_intent is not null
  and jsonb_typeof(raw_intent)='object' and octet_length(raw_intent::text)<=32768
  and accepted_context is not null and jsonb_typeof(accepted_context)='object' and octet_length(accepted_context::text)<=32768)
 or (row_kind='LEAF' and candidate_id is not null and raw_intent is null and accepted_context is null
  and ((work_id is not null and approved_line_id is not null and case_component_id is null)
   or (work_id is null and approved_line_id is null and case_component_id is not null)))),
 check(agency_sequence is null or agency_sequence>0),
 check(acknowledged_at_utc is null or row_kind='HEADER' and command_id is not null and agency_sequence is not null)
);
create index bpay_next_selection_memory_namespace_idx on private.bpay_next_selection_mode_memory
 (agency_id,owner_epoch,actor_user_id,official_pay_date,scope,agency_sequence desc,run_id)
 where row_kind='HEADER' and acknowledged_at_utc is not null;
create index bpay_next_selection_memory_work_idx on private.bpay_next_selection_mode_memory(run_id,candidate_id,work_id,approved_line_id)
 where row_kind='LEAF';
create table private.bpay_next_selection_exclusion_memory (
 run_id uuid not null references private.bpay_next_pay_run(id),
 agency_id uuid not null,owner_epoch bigint not null check(owner_epoch>0),
 actor_user_id uuid not null references public.tms_users(id),official_pay_date date not null,
 scope jsonb not null check(jsonb_typeof(scope)='object' and octet_length(scope::text)<=512),
 intent_kind text not null check(intent_kind in ('candidates','works','components','cases')),
 native_id uuid not null,candidate_id uuid not null,work_id uuid,expected_revision_id uuid,
 approved_line_id uuid,case_component_id uuid,selected boolean not null,
 basis_ref_run_id uuid not null,intent_json jsonb not null
  check(jsonb_typeof(intent_json)='object' and octet_length(intent_json::text)<=1024),
 captured_at_utc timestamptz not null default transaction_timestamp(),
 primary key(run_id,intent_kind,native_id),
 check((intent_kind='candidates' and native_id=candidate_id and work_id is null and approved_line_id is null and case_component_id is null)
  or (intent_kind='works' and native_id=work_id and work_id is not null and expected_revision_id is not null and approved_line_id is null and case_component_id is null)
  or (intent_kind='components' and native_id=approved_line_id and work_id is not null and expected_revision_id is not null and approved_line_id is not null and case_component_id is null)
  or (intent_kind='cases' and native_id=case_component_id and work_id is null and approved_line_id is null and case_component_id is not null))
);
create index bpay_next_selection_exclusion_memory_candidate_idx on private.bpay_next_selection_exclusion_memory(run_id,candidate_id,intent_kind,native_id);
alter table private.bpay_next_selection_mode_memory owner to postgres;
alter table private.bpay_next_selection_exclusion_memory owner to postgres;
alter table private.bpay_next_selection_mode_memory enable row level security;
alter table private.bpay_next_selection_mode_memory force row level security;
alter table private.bpay_next_selection_exclusion_memory enable row level security;
alter table private.bpay_next_selection_exclusion_memory force row level security;
create policy bpay_next_selection_mode_memory_owner on private.bpay_next_selection_mode_memory to postgres using(true) with check(true);
create policy bpay_next_selection_exclusion_memory_owner on private.bpay_next_selection_exclusion_memory to postgres using(true) with check(true);
revoke all on private.bpay_next_selection_mode_memory,private.bpay_next_selection_exclusion_memory from public,anon,authenticated,service_role;
commit;
