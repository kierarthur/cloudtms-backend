-- EX-08 / BD114: immutable future-current-fact receipt, never captured money.

\set ON_ERROR_STOP on

begin;

create table private.bpay_next_pay_method_change_receipt (
 operation_id uuid primary key,candidate_id uuid not null references public.candidates(id),
 actor_user_id uuid not null references public.tms_users(id),owner_epoch bigint not null check(owner_epoch>0),
 request jsonb not null check(jsonb_typeof(request)='object' and octet_length(request::text)<=8192),
 accepted_context jsonb not null check(jsonb_typeof(accepted_context)='object' and octet_length(accepted_context::text)<=4096),
 source_pay_channel text not null check(source_pay_channel in ('PAYE','UMBRELLA')),
 source_umbrella_id uuid references public.umbrellas(id),
 target_pay_channel text not null check(target_pay_channel in ('PAYE','UMBRELLA')),
 target_umbrella_id uuid references public.umbrellas(id),target_umbrella_vat_chargeable boolean,
 post_metadata_version bigint not null check(post_metadata_version>0),
 task_id uuid not null references private.bpay_next_workbench_task(id),task_generation bigint not null check(task_generation>0),
 affected_member_count integer not null check(affected_member_count between 0 and 1000),
 result jsonb not null check(jsonb_typeof(result)='object' and octet_length(result::text)<=8192),
 accepted_at_utc timestamptz not null default transaction_timestamp(),
 unique(operation_id,candidate_id),unique(candidate_id,post_metadata_version),
 check((target_pay_channel='PAYE' and target_umbrella_id is null and target_umbrella_vat_chargeable is null)
  or (target_pay_channel='UMBRELLA' and target_umbrella_id is not null and target_umbrella_vat_chargeable is not null))
);
create table private.bpay_next_pay_method_change_member (
 operation_id uuid not null,candidate_id uuid not null,member_key text not null check(octet_length(member_key) between 1 and 1024),
 member_kind text not null check(member_kind in ('WORK','CASE','PROTECTED_WORK','PROTECTED_CASE','TRANSFER')),
 work_id uuid,revision_id uuid,approved_line_id uuid,approved_segment_id uuid,
 case_id uuid,case_component_id uuid,case_component_revision bigint,transfer_id uuid references private.bpay_next_transfer(id),
 run_id uuid references private.bpay_next_pay_run(id),
 source_pay_channel text check(source_pay_channel in ('PAYE','UMBRELLA')),source_umbrella_id uuid references public.umbrellas(id),
 classification text not null check(classification in ('COMPATIBLE','NEEDS_DECISION','UNCHANGED','PROTECTED')),
 source_amount numeric,quote jsonb,
 primary key(operation_id,member_key),
 foreign key(operation_id,candidate_id) references private.bpay_next_pay_method_change_receipt(operation_id,candidate_id),
 foreign key(work_id,candidate_id) references private.bpay_next_work(id,candidate_id),
 foreign key(work_id,revision_id) references private.bpay_next_work_revision(work_id,id),
 foreign key(revision_id,approved_line_id) references private.bpay_next_approved_line(revision_id,id),
 foreign key(revision_id,approved_segment_id) references private.bpay_next_approved_segment(revision_id,id),
 foreign key(transfer_id,candidate_id) references private.bpay_next_transfer(id,candidate_id),
 foreign key(case_id,candidate_id) references private.bpay_next_finance_case(id,candidate_id),
 foreign key(case_component_id,case_id,candidate_id) references private.bpay_next_case_component(id,case_id,candidate_id),
 check((member_kind in ('WORK','PROTECTED_WORK') and work_id is not null and revision_id is not null and case_id is null and transfer_id is null)
  or(member_kind in ('CASE','PROTECTED_CASE') and case_id is not null and case_component_id is not null and case_component_revision is not null and work_id is null and transfer_id is null)
  or(member_kind='TRANSFER' and transfer_id is not null and work_id is null and case_id is null)),
 check(source_amount is null or(source_amount::text not in ('NaN','Infinity','-Infinity') and source_amount=trunc(source_amount,2))),
 check(quote is null or(jsonb_typeof(quote)='object' and octet_length(quote::text)<=4096)),
 check((member_kind in ('PROTECTED_WORK','PROTECTED_CASE','TRANSFER'))=(classification='PROTECTED'))
);
create index bpay_next_pay_method_change_work_idx on private.bpay_next_pay_method_change_member(work_id,revision_id,operation_id) where work_id is not null;
create index bpay_next_pay_method_change_case_idx on private.bpay_next_pay_method_change_member(case_component_id,case_component_revision,operation_id) where case_component_id is not null;
-- Preview seeks current Candidate protection, including pre-claim preparation.
create index bpay_next_pay_method_change_selected_work_idx on private.bpay_next_run_choice_member(candidate_id,run_id,work_id)
 where member_kind='WORK' and selected and skip_code is null;
create index bpay_next_pay_method_change_worker_idx on private.bpay_next_run_worker(candidate_id,run_id,id);
create index bpay_next_pay_method_change_transfer_idx on private.bpay_next_transfer(candidate_id,id)
 where status in ('DRAFT','SCHEDULED','ISSUED_CSV','SUBMITTED','UNKNOWN','RETURNED');
do $acl$
declare v_name text;
begin
 foreach v_name in array array['bpay_next_pay_method_change_receipt','bpay_next_pay_method_change_member'] loop
  execute format('alter table private.%I owner to postgres',v_name);
  execute format('alter table private.%I enable row level security',v_name);
  execute format('alter table private.%I force row level security',v_name);
  execute format('revoke all on private.%I from public,anon,authenticated,service_role',v_name);
  execute format('create policy cloudtms_miget_service_owner_all on private.%I to postgres using(true) with check(true)',v_name);
  execute format('create trigger %I before update or delete on private.%I for each row execute function private.bpay_next_protected_source_receipt_immutable_v1()',v_name||'_immutable',v_name);
  execute format('create trigger %I before truncate on private.%I for each statement execute function private.bpay_next_protected_source_receipt_immutable_v1()',v_name||'_no_truncate',v_name);
 end loop;
end
$acl$;

commit;
