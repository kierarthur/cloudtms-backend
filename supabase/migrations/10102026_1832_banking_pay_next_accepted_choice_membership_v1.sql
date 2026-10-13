-- One-time CloudTMS schema/data migration: banking_pay_next_accepted_choice_membership_v1
-- State the exact authority, safety boundary, and verification before implementation.

\set ON_ERROR_STOP on

begin;

-- WB21 input identities only. No business backfill or monetary snapshot.
alter table private.bpay_next_pay_run
  add column accepted_choice_descriptor jsonb,
  add column accepted_choice_command_id uuid,
  add column uploaded_choice_work_count bigint not null default 0 check(uploaded_choice_work_count>=0),
  add column uploaded_selection_count bigint not null default 0 check(uploaded_selection_count>=0),
  add column exclusion_page_count bigint not null default 0 check(exclusion_page_count>=0),
  add constraint bpay_next_accepted_choice_descriptor_size
    check(accepted_choice_descriptor is null or (jsonb_typeof(accepted_choice_descriptor)='object'
      and octet_length(accepted_choice_descriptor::text)<=32768)),
  add constraint bpay_next_accepted_choice_identity_pair
    check((accepted_choice_descriptor is null)=(accepted_choice_command_id is null));

create table private.bpay_next_run_choice_member (
  run_id uuid not null references private.bpay_next_pay_run(id),
  member_kind text not null check(member_kind in ('WORK','CASE_COMPONENT')),
  member_id uuid not null,
  candidate_id uuid not null references public.candidates(id),
  work_id uuid,
  expected_revision_id uuid,
  case_component_id uuid references private.bpay_next_case_component(id),
  selection_mode text check(selection_mode in ('ALL','SUBSET')),
  selected boolean not null,
  member_no bigint not null check(member_no>0),
  skip_code text check(skip_code in ('BPAY_NEXT_WORK_WEEK_NOT_YET_PAYABLE',
    'BPAY_NEXT_SELECTION_SNOOZED','BPAY_NEXT_SELECTION_PAY_ON_HOLD',
    'BPAY_NEXT_SELECTION_WORK_NOT_APPROVED','BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT',
    'BPAY_NEXT_SELECTION_SIGNED_LINE_NOT_ELIGIBLE',
    'SOURCE_PAY_QUERY_OPEN','SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE')),
  primary key(run_id,member_kind,member_id),
  foreign key(work_id,candidate_id) references private.bpay_next_work(id,candidate_id),
  foreign key(work_id,expected_revision_id) references private.bpay_next_work_revision(work_id,id),
  unique(run_id,member_kind,member_no),
  check((member_kind='WORK' and work_id is not null and member_id=work_id and expected_revision_id is not null
      and case_component_id is null and selection_mode is not null)
    or (member_kind='CASE_COMPONENT' and case_component_id is not null and member_id=case_component_id and work_id is null
      and expected_revision_id is null and selection_mode is null and skip_code is null))
);
create index bpay_next_run_choice_member_candidate_idx
  on private.bpay_next_run_choice_member(run_id,candidate_id,member_kind,member_id);

create table private.bpay_next_run_selection_exclusion (
  run_id uuid not null references private.bpay_next_pay_run(id),
  page_no bigint not null check(page_no>0),
  work_id uuid references private.bpay_next_work(id),
  candidate_id uuid references public.candidates(id),
  actor_user_id uuid not null references public.tms_users(id),
  created_at_utc timestamptz not null default transaction_timestamp(),
  unique(run_id,work_id),
  unique(run_id,candidate_id),
  check((work_id is null) <> (candidate_id is null))
);
create table private.bpay_next_selection_exclusion_page (
  run_id uuid not null references private.bpay_next_pay_run(id),
  page_no bigint not null check(page_no>0),
  request_json jsonb not null check(jsonb_typeof(request_json)='object'
    and octet_length(request_json::text)<=32768),
  primary key(run_id,page_no)
);

alter table private.bpay_next_run_choice_member owner to postgres;
alter table private.bpay_next_run_selection_exclusion owner to postgres;
alter table private.bpay_next_selection_exclusion_page owner to postgres;
alter table private.bpay_next_run_choice_member enable row level security;
alter table private.bpay_next_run_choice_member force row level security;
alter table private.bpay_next_run_selection_exclusion enable row level security;
alter table private.bpay_next_run_selection_exclusion force row level security;
alter table private.bpay_next_selection_exclusion_page enable row level security;
alter table private.bpay_next_selection_exclusion_page force row level security;
revoke all on private.bpay_next_run_choice_member,private.bpay_next_run_selection_exclusion,
  private.bpay_next_selection_exclusion_page from public,anon,authenticated,service_role;
create policy cloudtms_miget_service_owner_all on private.bpay_next_run_choice_member
  for all to CURRENT_USER using(true) with check(true);
create policy cloudtms_miget_service_owner_all on private.bpay_next_run_selection_exclusion
  for all to CURRENT_USER using(true) with check(true);
create policy cloudtms_miget_service_owner_all on private.bpay_next_selection_exclusion_page
  for all to CURRENT_USER using(true) with check(true);

commit;
