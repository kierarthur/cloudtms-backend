-- DR-37 additive owner-only staged replacement. No holds, backfill or agency switch.

\set ON_ERROR_STOP on

begin;

alter table private.bpay_next_run_worker
 add column net_request_high_water bigint not null default 0 check(net_request_high_water>=0),
 add column net_replacement_pending_id uuid;
-- Exact catalogue-audited old REVIEW-only branch is retained. READY may carry
-- only the current three closed NET-refusal codes, never a preparation failure.
do $worker_issue_check$
begin
 if not exists(select 1 from pg_catalog.pg_constraint c
  where c.conrelid='private.bpay_next_run_worker'::regclass and c.conname='bpay_next_run_worker_check1' and c.contype='c'
   and pg_catalog.pg_get_expr(c.conbin,c.conrelid)='((review_issue_code IS NULL) OR (status = ''REVIEW''::text))') then
  raise exception using errcode='23514',message='BPAY_NEXT_NET_WORKER_ISSUE_CHECK_NOT_AUDITED';end if;
end $worker_issue_check$;
alter table private.bpay_next_run_worker drop constraint bpay_next_run_worker_check1,
 add constraint bpay_next_run_worker_check1 check(review_issue_code is null or status='REVIEW'
  or (status='READY' and target_pay_channel='PAYE' and case_selection_revision>0 and net_replacement_pending_id is null
   and net_request_revision=net_projection_revision and net_request_high_water>net_projection_revision
   and review_issue_code in ('CASE_INPUT_UNBOUND','CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND')));
create index bpay_next_ready_net_refusal_header_idx on private.bpay_next_run_worker(run_id,target_pay_channel,review_issue_code collate "C",id)
 where status='READY' and net_replacement_pending_id is null and net_request_revision=net_projection_revision
  and review_issue_code in ('CASE_INPUT_UNBOUND','CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND');
-- A pre-protocol unfinished Case NET must finish using its installed owner
-- before release. Historical DONE receipts remain readable; no business backfill.
do $pending_case_net$
begin
 if exists(select 1 from private.bpay_next_paye_net_request r join private.bpay_next_job j on j.command_id=r.command_id
  where r.case_draft_state_id is not null and j.job_kind='PAYE_NET_ENTRY' and j.status<>'DONE') then
  raise exception using errcode='23514',message='BPAY_NEXT_NET_LEGACY_IN_PROGRESS_RELEASE_BLOCKED';end if;
end $pending_case_net$;
alter table private.bpay_next_paye_net_request drop constraint bpay_next_net_request_case_shape,
 add constraint bpay_next_net_request_case_shape check(
 (case_draft_state_id is null and actor_user_id is null and preparation_revision is null
  and selection_revision is null and expected_projection_revision is null and input_kind='PAYE_MANUAL')
 or (case_draft_state_id is not null and actor_user_id is not null and preparation_revision is not null
  and selection_revision is not null and expected_projection_revision is not null and request_no>expected_projection_revision));
do $stage_check$
declare v_name name;
begin
 select c.conname into strict v_name from pg_catalog.pg_constraint c
 where c.conrelid='private.bpay_next_case_allocation_state'::regclass and c.contype='c'
  and c.conkey=array[(select a.attnum from pg_catalog.pg_attribute a where a.attrelid=c.conrelid and a.attname='net_stage')]
  and pg_catalog.regexp_replace(pg_catalog.pg_get_expr(c.conbin,c.conrelid),'[[:space:]()]','','g')=
   pg_catalog.regexp_replace('(net_stage = ANY (ARRAY[''NOT_CASE_NET''::text, ''WEEK_CAPTURE''::text, ''ALLOCATE''::text, ''COMPLETE''::text, ''REVIEW''::text]))','[[:space:]()]','','g');
 if v_name<>'bpay_next_case_allocation_state_net_stage_check' then
  raise exception using errcode='23514',message='BPAY_NEXT_NET_ALLOCATION_STAGE_NOT_AUDITED';end if;
end $stage_check$;
alter table private.bpay_next_case_allocation_state drop constraint bpay_next_case_allocation_state_net_stage_check;
alter table private.bpay_next_case_allocation_state add constraint bpay_next_net_allocation_stage check(
 net_stage in ('NOT_CASE_NET','WEEK_CAPTURE','ALLOCATE','PRECHECK','STAGING','COMMITTING',
 'CANCEL_REQUESTED','CANCELLING','COMPLETE','REFUSED','CANCELLED','REVIEW'));

create table private.bpay_next_net_replacement(
 id uuid primary key references private.bpay_next_paye_net_request(command_id) on delete restrict,
 run_worker_id uuid not null,run_id uuid not null,candidate_id uuid not null,
 pay_week_start date not null check(pg_catalog.isfinite(pay_week_start) and extract(isodow from pay_week_start)=1),
 request_no bigint not null check(request_no>0),prior_projection_no bigint not null check(prior_projection_no>=0 and prior_projection_no<request_no),
 prior_projection_id uuid, draft_state_id uuid not null,prior_state_id uuid,
 target_state_id uuid unique references private.bpay_next_case_allocation_state(id) on delete restrict,
 original_job_id uuid unique,module_epoch bigint not null check(module_epoch>0),
 preparation_revision bigint not null check(preparation_revision>0),selection_revision bigint not null check(selection_revision>0),
 stage text not null default 'PRECHECK' check(stage in ('PRECHECK','STAGING','COMMITTING','CANCEL_REQUESTED','CANCELLING','COMPLETE','REFUSED','CANCELLED')),
 staging_step text not null default 'WEEK' check(staging_step in ('WEEK','INSTRUCTIONS','VALIDATE','DONE')),
 expected_instruction_count bigint not null check(expected_instruction_count>=0),
 staged_instruction_count bigint not null default 0 check(staged_instruction_count>=0),
 validated_instruction_count bigint not null default 0 check(validated_instruction_count>=0),
 committed_instruction_count bigint not null default 0 check(committed_instruction_count>=0),
 staged_deduction_count bigint not null default 0 check(staged_deduction_count>=0),
 committed_deduction_count bigint not null default 0 check(committed_deduction_count>=0),
 staged_old_hold_count bigint not null default 0 check(staged_old_hold_count>=0),
 staged_old_source_total private.bpay_next_penny_amount not null default 0 check(staged_old_source_total>=0),
 proposed_hold_count bigint not null default 0 check(proposed_hold_count>=0),
 proposed_source_total private.bpay_next_penny_amount not null default 0 check(proposed_source_total>=0),
 committed_old_hold_count bigint not null default 0 check(committed_old_hold_count>=0),
 committed_old_source_total private.bpay_next_penny_amount not null default 0 check(committed_old_source_total>=0),
 committed_new_hold_count bigint not null default 0 check(committed_new_hold_count>=0),
 committed_new_source_total private.bpay_next_penny_amount not null default 0 check(committed_new_source_total>=0),
 cleanup_instruction_count bigint not null default 0 check(cleanup_instruction_count>=0),
 expected_releasable_hold_count bigint not null default 0 check(expected_releasable_hold_count>=0),
 released_hold_count bigint not null default 0 check(released_hold_count>=0),
 released_source_total private.bpay_next_penny_amount not null default 0 check(released_source_total>=0),
 cursor_age_key bigint,cursor_case_id uuid,cursor_component_ordinal bigint check(cursor_component_ordinal>0),cursor_component_id uuid,
 validation_cursor bigint not null default 0 check(validation_cursor>=0),commit_cursor bigint not null default 0 check(commit_cursor>=0),
 metadata_pay_date_cursor date,metadata_created_at_cursor timestamptz,metadata_run_cursor uuid,
 validation_step text not null default 'INSTRUCTIONS' check(validation_step in ('INSTRUCTIONS','PRIOR_RUNS','DONE')),
 validation_pay_date_cursor date,validation_created_at_cursor timestamptz,validation_run_cursor uuid,
 cleanup_age_key bigint,cleanup_case_id uuid,cleanup_component_ordinal bigint check(cleanup_component_ordinal>0),cleanup_component_id uuid,
 cleanup_step text not null default 'INVENTORY' check(cleanup_step in ('INVENTORY','RELEASE','DONE')),
 inventory_instruction_count bigint not null default 0 check(inventory_instruction_count>=0),
 inventory_age_key bigint,inventory_case_id uuid,inventory_component_ordinal bigint check(inventory_component_ordinal>0),inventory_component_id uuid,
 expected_releasable_source_total private.bpay_next_penny_amount not null default 0 check(expected_releasable_source_total>=0),
 initial_channel_headroom private.bpay_next_penny_amount not null check(initial_channel_headroom>=0),
 remaining_channel_headroom private.bpay_next_penny_amount not null check(remaining_channel_headroom>=0),
 initial_take_home private.bpay_next_penny_amount not null check(initial_take_home>=0),
 remaining_take_home private.bpay_next_penny_amount not null check(remaining_take_home>=0),
 gross_additions private.bpay_next_penny_amount not null default 0 check(gross_additions>=0),
 gross_deductions private.bpay_next_penny_amount not null default 0 check(gross_deductions>=0),
 net_additions private.bpay_next_penny_amount not null default 0 check(net_additions>=0),
 recoveries private.bpay_next_penny_amount not null default 0 check(recoveries>=0),
 cash_amount private.bpay_next_penny_amount check(cash_amount>=0),
 accepted_projection_id uuid references private.bpay_next_net_projection(id) on delete restrict,
 refusal_code text check(refusal_code in ('CASE_INPUT_UNBOUND','CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND')),
 cancel_command_id uuid unique references private.bpay_next_cancel_request(command_id) on delete restrict,
 created_at_utc timestamptz not null default pg_catalog.transaction_timestamp() check(pg_catalog.isfinite(created_at_utc)),
 updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp() check(pg_catalog.isfinite(updated_at_utc)),
 finished_at_utc timestamptz check(pg_catalog.isfinite(finished_at_utc)),
 unique(id,run_worker_id,candidate_id),unique(run_worker_id,request_no),
 foreign key(run_worker_id,run_id,candidate_id) references private.bpay_next_run_worker(id,run_id,candidate_id) on delete restrict,
 foreign key(id,candidate_id) references private.bpay_next_command_member(command_id,candidate_id) on delete restrict,
 foreign key(original_job_id,id,candidate_id,module_epoch) references private.bpay_next_job(id,command_id,candidate_id,module_epoch) on delete restrict,
 foreign key(draft_state_id,run_worker_id,candidate_id,preparation_revision,selection_revision)
 references private.bpay_next_case_allocation_state(id,run_worker_id,candidate_id,preparation_revision,selection_revision) on delete restrict,
 foreign key(prior_state_id,run_worker_id,candidate_id,preparation_revision,selection_revision)
 references private.bpay_next_case_allocation_state(id,run_worker_id,candidate_id,preparation_revision,selection_revision) on delete restrict,
 foreign key(prior_projection_id,run_worker_id) references private.bpay_next_net_projection(id,run_worker_id) on delete restrict,
 check((prior_projection_no=0)=(prior_projection_id is null) and (prior_state_id is null)=(prior_projection_id is null)),
 check(staged_instruction_count<=expected_instruction_count and validated_instruction_count<=staged_instruction_count
 and committed_instruction_count<=validated_instruction_count and cleanup_instruction_count<=expected_instruction_count
 and inventory_instruction_count<=expected_instruction_count and released_hold_count<=expected_releasable_hold_count
 and released_source_total<=expected_releasable_source_total),
 check(remaining_channel_headroom<=initial_channel_headroom and remaining_take_home<=initial_take_home),
 check(staged_deduction_count<=staged_instruction_count and committed_deduction_count<=staged_deduction_count
 and staged_old_hold_count<=staged_deduction_count and proposed_hold_count<=staged_deduction_count
 and committed_old_hold_count<=staged_old_hold_count and committed_old_source_total<=staged_old_source_total
 and committed_new_hold_count<=proposed_hold_count and committed_new_source_total<=proposed_source_total),
 check((cursor_age_key is null and cursor_case_id is null and cursor_component_ordinal is null and cursor_component_id is null)
 or (cursor_age_key is not null and cursor_case_id is not null and cursor_component_ordinal is not null and cursor_component_id is not null)),
 check((cleanup_age_key is null and cleanup_case_id is null and cleanup_component_ordinal is null and cleanup_component_id is null)
 or (cleanup_age_key is not null and cleanup_case_id is not null and cleanup_component_ordinal is not null and cleanup_component_id is not null)),
 check((stage in ('COMPLETE','REFUSED','CANCELLED'))=(finished_at_utc is not null)),
 check((original_job_id is null)=(target_state_id is null)),
 check(stage in ('PRECHECK','REFUSED') or original_job_id is not null),
 check((inventory_age_key is null and inventory_case_id is null and inventory_component_ordinal is null and inventory_component_id is null)
 or (inventory_age_key is not null and inventory_case_id is not null and inventory_component_ordinal is not null and inventory_component_id is not null)),
 check((metadata_pay_date_cursor is null and metadata_created_at_cursor is null and metadata_run_cursor is null)
 or (metadata_pay_date_cursor is not null and metadata_created_at_cursor is not null and metadata_run_cursor is not null
 and pg_catalog.isfinite(metadata_pay_date_cursor) and pg_catalog.isfinite(metadata_created_at_cursor))),
 check((validation_pay_date_cursor is null and validation_created_at_cursor is null and validation_run_cursor is null)
 or (validation_pay_date_cursor is not null and validation_created_at_cursor is not null and validation_run_cursor is not null
 and pg_catalog.isfinite(validation_pay_date_cursor) and pg_catalog.isfinite(validation_created_at_cursor))),
 check((stage='REFUSED')=(refusal_code is not null)),
 check(stage<>'COMPLETE' or (accepted_projection_id is not null and cash_amount is not null
 and committed_instruction_count=expected_instruction_count and staging_step='DONE')),
 check(stage<>'CANCELLED' or (cleanup_step='DONE' and inventory_instruction_count=expected_instruction_count
 and cleanup_instruction_count=expected_instruction_count and released_hold_count=expected_releasable_hold_count
 and released_source_total=expected_releasable_source_total)),
 check(stage not in ('CANCEL_REQUESTED','CANCELLING','CANCELLED') or cancel_command_id is not null),
 check(updated_at_utc>=created_at_utc and (finished_at_utc is null or finished_at_utc>=created_at_utc))
);
create unique index bpay_next_net_replacement_pending_worker_idx on private.bpay_next_net_replacement(run_worker_id)
 where stage not in ('COMPLETE','REFUSED','CANCELLED');
create index bpay_next_net_replacement_candidate_idx on private.bpay_next_net_replacement(candidate_id,run_worker_id,request_no desc);
alter table private.bpay_next_run_worker add constraint bpay_next_worker_net_replacement_fk
 foreign key(net_replacement_pending_id,id,candidate_id) references private.bpay_next_net_replacement(id,run_worker_id,candidate_id) on delete restrict;
alter table private.bpay_next_candidate_week_commit_fence
 add column replacement_id uuid references private.bpay_next_net_replacement(id) on delete restrict;

-- Same typed proposed result, never a provisional INSERT into accepted results.
create table private.bpay_next_net_staged_result(
 like private.bpay_next_case_allocation_result including defaults including constraints,
 replacement_id uuid not null references private.bpay_next_net_replacement(id) on delete restrict,
 draft_result_id uuid not null references private.bpay_next_case_allocation_result(id) on delete restrict,
 prior_hold_id uuid references private.bpay_next_case_hold(id) on delete restrict,
 prior_source_amount private.bpay_next_penny_amount not null check(prior_source_amount>=0),
 case_id uuid not null references private.bpay_next_finance_case(id) on delete restrict,
 case_component_id uuid not null references private.bpay_next_case_component(id) on delete restrict,
 pay_week_start date not null,captured_case_revision bigint not null check(captured_case_revision>0),
 captured_component_revision bigint not null check(captured_component_revision>0),captured_period_revision bigint not null check(captured_period_revision>0),
 payroll_stage text not null check(payroll_stage in ('GROSS_ADD','GROSS_DEDUCT','NET_ADD','NET_DEDUCT')),
 primary key(id),unique(replacement_id,instruction_id),unique(replacement_id,result_no),
 foreign key(state_id,run_worker_id,candidate_id,preparation_revision,selection_revision,pass_kind)
 references private.bpay_next_case_allocation_state(id,run_worker_id,candidate_id,preparation_revision,selection_revision,pass_kind) on delete restrict,
 foreign key(instruction_id,run_worker_id,candidate_id,preparation_revision,selection_revision,hold_purpose,allocation_channel)
 references private.bpay_next_run_case_instruction(id,run_worker_id,candidate_id,preparation_revision,selection_revision,hold_purpose,allocation_channel) on delete restrict,
 foreign key(state_id,allocation_channel) references private.bpay_next_case_allocation_channel(state_id,allocation_channel) on delete restrict,
 foreign key(case_component_id,case_id,candidate_id,pay_week_start)
 references private.bpay_next_case_period(case_component_id,case_id,candidate_id,pay_week_start) on delete restrict,
 check((prior_hold_id is null)=(prior_source_amount=0))
);
create index bpay_next_net_staged_prior_hold_idx on private.bpay_next_net_staged_result(prior_hold_id,replacement_id) where prior_hold_id is not null;
-- Proposed deltas for several frozen components sharing one Case; no live balance.
create table private.bpay_next_net_staged_case_capacity(
 replacement_id uuid not null references private.bpay_next_net_replacement(id) on delete restrict,
 case_id uuid not null references private.bpay_next_finance_case(id) on delete restrict,
 captured_case_revision bigint not null check(captured_case_revision>0),
 captured_outstanding_source private.bpay_next_penny_amount not null check(captured_outstanding_source>=0),
 captured_active_recovery_source private.bpay_next_penny_amount not null check(captured_active_recovery_source>=0),
 proposed_release_source private.bpay_next_penny_amount not null default 0 check(proposed_release_source>=0),
 proposed_reserve_source private.bpay_next_penny_amount not null default 0 check(proposed_reserve_source>=0),
 staged_instruction_count bigint not null default 0 check(staged_instruction_count>=0),
 committed_instruction_count bigint not null default 0 check(committed_instruction_count>=0 and committed_instruction_count<=staged_instruction_count),
 committed_release_source private.bpay_next_penny_amount not null default 0 check(committed_release_source>=0 and committed_release_source<=proposed_release_source),
 committed_reserve_source private.bpay_next_penny_amount not null default 0 check(committed_reserve_source>=0 and committed_reserve_source<=proposed_reserve_source),
 primary key(replacement_id,case_id)
);
create table private.bpay_next_paye_net_request_refusal(
 command_id uuid primary key references private.bpay_next_net_replacement(id) on delete restrict,
 run_worker_id uuid not null,candidate_id uuid not null,request_no bigint not null check(request_no>0),
 refusal_code text not null check(refusal_code in ('CASE_INPUT_UNBOUND','CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND')),
 refused_at_utc timestamptz not null default pg_catalog.transaction_timestamp() check(pg_catalog.isfinite(refused_at_utc)),
 foreign key(command_id,run_worker_id,candidate_id) references private.bpay_next_net_replacement(id,run_worker_id,candidate_id) on delete restrict,
 unique(run_worker_id,request_no)
);
-- R2 immutable cleanup certificate; exact membership remains in frozen/staged rows.
create table private.bpay_next_net_cancel_handoff(
 replacement_id uuid primary key references private.bpay_next_net_replacement(id) on delete restrict,
 cancel_command_id uuid not null unique references private.bpay_next_cancel_request(command_id) on delete restrict,
 original_job_id uuid not null references private.bpay_next_job(id) on delete restrict,
 run_worker_id uuid not null,candidate_id uuid not null,
 draft_state_id uuid not null references private.bpay_next_case_allocation_state(id) on delete restrict,
 prior_state_id uuid references private.bpay_next_case_allocation_state(id) on delete restrict,
 target_state_id uuid not null references private.bpay_next_case_allocation_state(id) on delete restrict,
 prior_projection_id uuid references private.bpay_next_net_projection(id) on delete restrict,
 target_projection_id uuid references private.bpay_next_net_projection(id) on delete restrict,
 expected_instruction_count bigint not null check(expected_instruction_count>=0),cleaned_instruction_count bigint not null check(cleaned_instruction_count=expected_instruction_count),
 expected_hold_count bigint not null check(expected_hold_count>=0),released_hold_count bigint not null check(released_hold_count=expected_hold_count),
 expected_source_total private.bpay_next_penny_amount not null check(expected_source_total>=0),
 released_source_total private.bpay_next_penny_amount not null check(released_source_total=expected_source_total),
 cleanup_age_key bigint,cleanup_case_id uuid,cleanup_component_ordinal bigint check(cleanup_component_ordinal>0),cleanup_component_id uuid,
 completed_at_utc timestamptz not null default pg_catalog.transaction_timestamp() check(pg_catalog.isfinite(completed_at_utc)),
 foreign key(replacement_id,run_worker_id,candidate_id) references private.bpay_next_net_replacement(id,run_worker_id,candidate_id) on delete restrict,
 foreign key(cancel_command_id,candidate_id) references private.bpay_next_command_member(command_id,candidate_id) on delete restrict
);
-- Exact immutable hold membership captured by a bounded cancellation inventory
-- under the same fence, before any cleanup. No historical hold scan.
create table private.bpay_next_net_cancel_hold_member(
 replacement_id uuid not null references private.bpay_next_net_replacement(id) on delete restrict,
 case_hold_id uuid not null references private.bpay_next_case_hold(id) on delete restrict,
 instruction_id uuid not null references private.bpay_next_run_case_instruction(id) on delete restrict,
 allocation_state_id uuid not null references private.bpay_next_case_allocation_state(id) on delete restrict,
 source_amount private.bpay_next_penny_amount not null check(source_amount>0),
 primary key(replacement_id,case_hold_id),unique(replacement_id,instruction_id)
);
alter table private.bpay_next_net_replacement owner to postgres;
alter table private.bpay_next_net_replacement enable row level security;
alter table private.bpay_next_net_replacement force row level security;
create policy bpay_next_net_replacement_owner on private.bpay_next_net_replacement to current_user using(true) with check(true);
revoke all on table private.bpay_next_net_replacement from public,anon,authenticated,service_role;
alter table private.bpay_next_net_staged_result owner to postgres;
alter table private.bpay_next_net_staged_result enable row level security;
alter table private.bpay_next_net_staged_result force row level security;
create policy bpay_next_net_replacement_owner on private.bpay_next_net_staged_result to current_user using(true) with check(true);
revoke all on table private.bpay_next_net_staged_result from public,anon,authenticated,service_role;
alter table private.bpay_next_net_staged_case_capacity owner to postgres;
alter table private.bpay_next_net_staged_case_capacity enable row level security;
alter table private.bpay_next_net_staged_case_capacity force row level security;
create policy bpay_next_net_replacement_owner on private.bpay_next_net_staged_case_capacity to current_user using(true) with check(true);
revoke all on table private.bpay_next_net_staged_case_capacity from public,anon,authenticated,service_role;
alter table private.bpay_next_paye_net_request_refusal owner to postgres;
alter table private.bpay_next_paye_net_request_refusal enable row level security;
alter table private.bpay_next_paye_net_request_refusal force row level security;
create policy bpay_next_net_replacement_owner on private.bpay_next_paye_net_request_refusal to current_user using(true) with check(true);
revoke all on table private.bpay_next_paye_net_request_refusal from public,anon,authenticated,service_role;
alter table private.bpay_next_net_cancel_handoff owner to postgres;
alter table private.bpay_next_net_cancel_handoff enable row level security;
alter table private.bpay_next_net_cancel_handoff force row level security;
create policy bpay_next_net_replacement_owner on private.bpay_next_net_cancel_handoff to current_user using(true) with check(true);
revoke all on table private.bpay_next_net_cancel_handoff from public,anon,authenticated,service_role;
alter table private.bpay_next_net_cancel_hold_member owner to postgres;
alter table private.bpay_next_net_cancel_hold_member enable row level security;
alter table private.bpay_next_net_cancel_hold_member force row level security;
create policy bpay_next_net_replacement_owner on private.bpay_next_net_cancel_hold_member to current_user using(true) with check(true);
revoke all on table private.bpay_next_net_cancel_hold_member from public,anon,authenticated,service_role;

commit;
