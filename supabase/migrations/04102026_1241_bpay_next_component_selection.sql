-- Revision-bound pre-Draft choices. No business-row backfill, economics,
-- legacy session, history bootstrap or public/private application grant.

\set ON_ERROR_STOP on

begin;

alter table private.bpay_next_pay_run
  add column work_choice_count bigint not null default 0 check(work_choice_count>=0),
  add column sealed_work_choice_count bigint not null default 0
    check(sealed_work_choice_count>=0 and sealed_work_choice_count<=work_choice_count);
alter table private.bpay_next_run_worker
  add column expected_selected_component_count bigint not null default 0
    check(expected_selected_component_count>=0),
  add column captured_selected_component_count bigint not null default 0
    check(captured_selected_component_count>=0
      and captured_selected_component_count<=expected_selected_component_count);

create table private.bpay_next_work_choice (
  run_id uuid not null,
  work_id uuid not null,
  expected_revision_id uuid,
  selection_mode text not null check(selection_mode in ('ALL','SUBSET')),
  selection_state text not null check(selection_state in ('OPEN','SEALED')),
  page_count bigint not null default 0 check(page_count>=0),
  component_count bigint not null default 0 check(component_count>=0),
  primary key(run_id,work_id),
  unique(run_id,work_id,expected_revision_id),
  foreign key(run_id,work_id) references private.bpay_next_run_selection(run_id,work_id) on delete restrict,
  foreign key(work_id,expected_revision_id) references private.bpay_next_work_revision(work_id,id) on delete restrict,
  check((selection_mode='ALL' and selection_state='SEALED' and page_count=0 and component_count=0)
    or (selection_mode='SUBSET' and expected_revision_id is not null
      and ((page_count=0)=(component_count=0))
      and (selection_state='OPEN' or (page_count>0 and component_count>0))))
);
create table private.bpay_next_component_selection_page (
  run_id uuid not null,
  work_id uuid not null,
  page_no bigint not null check(page_no>0),
  first_selection_no bigint not null check(first_selection_no>0),
  item_count integer not null check(item_count between 1 and 100),
  primary key(run_id,work_id,page_no),
  foreign key(run_id,work_id) references private.bpay_next_work_choice(run_id,work_id) on delete restrict
);
create table private.bpay_next_selected_component (
  run_id uuid not null,
  work_id uuid not null,
  expected_revision_id uuid not null,
  approved_line_id uuid not null,
  component_key text not null check(pg_catalog.char_length(component_key) between 1 and 256),
  selection_no bigint not null check(selection_no>0),
  page_no bigint not null check(page_no>0),
  page_item_no integer not null check(page_item_no between 1 and 100),
  primary key(run_id,work_id,approved_line_id),
  unique(run_id,work_id,component_key),
  unique(run_id,work_id,selection_no),
  unique(run_id,work_id,page_no,page_item_no),
  foreign key(run_id,work_id,expected_revision_id)
    references private.bpay_next_work_choice(run_id,work_id,expected_revision_id) on delete restrict,
  foreign key(expected_revision_id,approved_line_id,component_key)
    references private.bpay_next_approved_line(revision_id,id,component_key) on delete restrict,
  foreign key(run_id,work_id,page_no)
    references private.bpay_next_component_selection_page(run_id,work_id,page_no) on delete restrict
);
-- Unique indexes above supply capture(run,work,key), page replay and member
-- keyset access. No scan of every current position is needed for SUBSET.
alter table private.bpay_next_work_choice enable row level security;
alter table private.bpay_next_component_selection_page enable row level security;
alter table private.bpay_next_selected_component enable row level security;
revoke all on private.bpay_next_work_choice,
  private.bpay_next_component_selection_page,private.bpay_next_selected_component
  from public,anon,authenticated,service_role;

commit;
