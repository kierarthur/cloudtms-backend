-- WB-36: exact shift attribution beneath existing WORK:DAY / SOURCE parents.
-- Local build. No public-table trigger, backfill or per-shift payment unit.

\set ON_ERROR_STOP on

begin;

alter table private.bpay_next_shift_detail add column segment_key text
  check(segment_key is null or (length(segment_key) between 1 and 245 and btrim(segment_key)=segment_key));

create table private.bpay_next_approved_segment (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  revision_id uuid not null, approved_line_id uuid not null, component_key text not null,
  segment_key text not null check(length(segment_key) between 1 and 245 and btrim(segment_key)=segment_key),
  shift_detail_id uuid not null unique references private.bpay_next_shift_detail(id),
  work_date date not null,
  shift_start_at timestamptz, shift_end_at timestamptz,
  shift_start_local text, shift_end_local text, shift_overnight boolean,
  approved_hours numeric(18,6) not null,
  hours_day numeric(18,6) not null, hours_night numeric(18,6) not null,
  hours_sat numeric(18,6) not null, hours_sun numeric(18,6) not null, hours_bh numeric(18,6) not null,
  pay_day numeric(18,6), pay_night numeric(18,6), pay_sat numeric(18,6),
  pay_sun numeric(18,6), pay_bh numeric(18,6),
  approved_source_ex_vat numeric(18,2) not null, pay_excluded boolean not null,
  unique(revision_id,segment_key), unique(revision_id,id),
  unique(revision_id,id,component_key,segment_key),
  foreign key(revision_id,approved_line_id,component_key)
    references private.bpay_next_approved_line(revision_id,id,component_key),
  check(approved_hours=hours_day+hours_night+hours_sat+hours_sun+hours_bh),
  check(not pay_excluded or approved_source_ex_vat=0)
);
create index bpay_next_approved_segment_parent_idx
  on private.bpay_next_approved_segment(revision_id,component_key,segment_key);
-- Persisted worker-only operands make whole/expense CHECK a point read.
-- Populated while sealing the real approved revision, never by CHECK.
create table private.bpay_next_revision_worker_basis (
  revision_id uuid primary key references private.bpay_next_work_revision(id),
  whole_basis bytea not null check(octet_length(whole_basis)=32),
  expense_basis bytea not null check(octet_length(expense_basis)=32)
);
alter table private.bpay_next_work_choice
  add column segment_page_count bigint not null default 0 check(segment_page_count>=0),
  add column segment_count bigint not null default 0 check(segment_count>=0),
  add column segment_sealed_count bigint check(segment_sealed_count>=0),
  add column segment_control_sequence bigint check(segment_control_sequence>=0),
  add constraint bpay_next_choice_segment_seal_check
    check(segment_sealed_count is null or segment_sealed_count=segment_count);
alter table private.bpay_next_pay_run
  add column snooze_control_sequence bigint check(snooze_control_sequence>=0),
  add column snooze_business_date date,
  add column segment_sealed_choice_count bigint not null default 0 check(segment_sealed_choice_count>=0);
create table private.bpay_next_segment_selection_page (
  run_id uuid not null, work_id uuid not null, page_no bigint not null check(page_no>0),
  request_id uuid not null unique, request_body jsonb not null,
  item_count integer not null check(item_count between 1 and 100),
  first_member_no bigint not null check(first_member_no>0),
  captured_at_utc timestamptz not null default pg_catalog.clock_timestamp(),
  primary key(run_id,work_id,page_no),
  foreign key(run_id,work_id) references private.bpay_next_work_choice(run_id,work_id)
);
create table private.bpay_next_selected_segment (
  run_id uuid not null, work_id uuid not null, expected_revision_id uuid not null,
  segment_key text not null, component_key text not null, approved_segment_id uuid not null,
  selection_no bigint not null check(selection_no>0),
  disposition text not null check(disposition in ('INCLUDED','SNOOZED')),
  snooze_id uuid, snooze_version bigint, control_sequence bigint not null check(control_sequence>=0),
  primary key(run_id,work_id,segment_key), unique(run_id,work_id,selection_no),
  foreign key(run_id,work_id,expected_revision_id)
    references private.bpay_next_work_choice(run_id,work_id,expected_revision_id),
  foreign key(expected_revision_id,approved_segment_id,component_key,segment_key)
    references private.bpay_next_approved_segment(revision_id,id,component_key,segment_key),
  check((disposition='SNOOZED')=(snooze_id is not null)),
  check((snooze_id is null)=(snooze_version is null))
);
create index bpay_next_selected_segment_parent_idx
  on private.bpay_next_selected_segment(run_id,work_id,component_key,segment_key);
create table private.bpay_next_run_work_segment (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  run_work_id uuid not null, run_worker_id uuid not null, work_id uuid not null,
  captured_revision_id uuid not null, segment_key text not null, approved_segment_id uuid,
  disposition text not null check(disposition in ('INCLUDED','SNOOZED','NOT_SELECTED','REMOVED')),
  unique(run_work_id,segment_key), unique(id,run_work_id,work_id,captured_revision_id,segment_key),
  foreign key(run_work_id,run_worker_id,work_id,captured_revision_id)
    references private.bpay_next_run_work(id,run_worker_id,work_id,captured_revision_id),
  foreign key(captured_revision_id,approved_segment_id)
    references private.bpay_next_approved_segment(revision_id,id),
  check((disposition='REMOVED')=(approved_segment_id is null))
);
-- Several real original parents may belong to one moved stable shift.
create table private.bpay_next_run_work_segment_parent (
  run_work_segment_id uuid not null, run_work_id uuid not null, work_id uuid not null,
  captured_revision_id uuid not null, segment_key text not null, component_key text not null,
  approved_segment_id uuid, approved_line_id uuid, work_date date,
  approved_source_ex_vat numeric(18,2) not null, realised_source_ex_vat numeric(18,2) not null,
  held_source_ex_vat numeric(18,2) not null check(held_source_ex_vat>=0),
  residual_source_ex_vat numeric(18,2) not null,
  included_positive_source_ex_vat numeric(18,2) not null check(included_positive_source_ex_vat>=0),
  excluded_positive_source_ex_vat numeric(18,2) not null check(excluded_positive_source_ex_vat>=0),
  recovery_source_ex_vat numeric(18,2) not null check(recovery_source_ex_vat>=0),
  held_overhang_source_ex_vat numeric(18,2) not null check(held_overhang_source_ex_vat>=0),
  primary key(run_work_segment_id,component_key), unique(run_work_id,component_key,segment_key),
  foreign key(run_work_segment_id,run_work_id,work_id,captured_revision_id,segment_key)
    references private.bpay_next_run_work_segment(id,run_work_id,work_id,captured_revision_id,segment_key),
  foreign key(run_work_id,component_key) references private.bpay_next_run_position(run_work_id,component_key),
  foreign key(captured_revision_id,approved_segment_id,component_key,segment_key)
    references private.bpay_next_approved_segment(revision_id,id,component_key,segment_key),
  check(residual_source_ex_vat=approved_source_ex_vat-realised_source_ex_vat-held_source_ex_vat),
  check(included_positive_source_ex_vat+excluded_positive_source_ex_vat-recovery_source_ex_vat
    -held_overhang_source_ex_vat=residual_source_ex_vat),
  check(approved_segment_id is not null or (approved_line_id is null and approved_source_ex_vat=0))
);
create index bpay_next_run_segment_parent_idx
  on private.bpay_next_run_work_segment_parent(run_work_id,component_key,segment_key);
create table private.bpay_next_work_segment_origin_share (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  hold_id uuid not null references private.bpay_next_hold(id), run_line_id uuid not null,
  run_work_segment_id uuid not null, work_id uuid not null, component_key text not null, segment_key text not null,
  source_reserved_ex_vat numeric(18,2) not null check(source_reserved_ex_vat>0),
  target_reserved_ex_vat numeric(18,2) not null check(target_reserved_ex_vat>=0),
  target_reserved_vat numeric(18,2) not null check(target_reserved_vat>=0),
  target_reserved_inc_vat numeric(18,2) not null,
  unique(hold_id,segment_key), unique(id,work_id,component_key,segment_key),
  foreign key(run_line_id,work_id,component_key) references private.bpay_next_run_line(id,work_id,component_key),
  foreign key(run_work_segment_id,component_key)
    references private.bpay_next_run_work_segment_parent(run_work_segment_id,component_key),
  check(target_reserved_inc_vat=target_reserved_ex_vat+target_reserved_vat)
);
create index bpay_next_segment_share_balance_idx
  on private.bpay_next_work_segment_origin_share(work_id,component_key,segment_key,hold_id);
create table private.bpay_next_work_segment_effect_share (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  effect_id uuid not null references private.bpay_next_financial_effect(id),
  work_id uuid not null, component_key text not null, segment_key text not null,
  source_disposition_ex_vat numeric(18,2) not null,
  target_ex_vat numeric(18,2) not null, target_vat numeric(18,2) not null, target_inc_vat numeric(18,2) not null,
  unique(effect_id,segment_key), check(target_inc_vat=target_ex_vat+target_vat)
);
create table private.bpay_next_work_segment_effect_origin (
  effect_share_id uuid not null references private.bpay_next_work_segment_effect_share(id),
  origin_share_id uuid not null references private.bpay_next_work_segment_origin_share(id),
  source_disposition_ex_vat numeric(18,2) not null check(source_disposition_ex_vat<>0),
  primary key(effect_share_id,origin_share_id)
);
create index bpay_next_segment_effect_origin_credit_idx on private.bpay_next_work_segment_effect_origin(origin_share_id,effect_share_id);
create index bpay_next_segment_effect_balance_idx
  on private.bpay_next_work_segment_effect_share(work_id,component_key,segment_key,effect_id);
-- Exact original paid gross reserved by the existing one-parent collection.
-- Status/counters remain on the real Case hold; these facts never move credit.
create table private.bpay_next_work_segment_recovery_share (
  case_hold_id uuid not null references private.bpay_next_case_hold(id),
  origin_share_id uuid not null,work_collection_id uuid not null,
  work_id uuid not null,component_key text not null,segment_key text not null,
  captured_revision_id uuid not null references private.bpay_next_work_revision(id),
  reserved_source_ex_vat numeric(18,2) not null check(reserved_source_ex_vat>0),
  primary key(case_hold_id,origin_share_id),
  foreign key(origin_share_id,work_id,component_key,segment_key)
    references private.bpay_next_work_segment_origin_share(id,work_id,component_key,segment_key),
  foreign key(work_collection_id,work_id,component_key)
    references private.bpay_next_work_collection(id,work_id,component_key)
);
create index bpay_next_segment_recovery_capacity_idx
  on private.bpay_next_work_segment_recovery_share(work_id,component_key,segment_key,case_hold_id,origin_share_id);
create index bpay_next_segment_recovery_origin_idx on private.bpay_next_work_segment_recovery_share(origin_share_id,case_hold_id);
create table private.bpay_next_work_segment_write_off_share (
  case_event_id uuid not null references private.bpay_next_case_event(id),
  origin_share_id uuid not null,work_collection_id uuid not null,
  work_id uuid not null,component_key text not null,segment_key text not null,
  written_off_source_ex_vat numeric(18,2) not null check(written_off_source_ex_vat>0),
  primary key(case_event_id,origin_share_id),
  foreign key(origin_share_id,work_id,component_key,segment_key)
    references private.bpay_next_work_segment_origin_share(id,work_id,component_key,segment_key),
  foreign key(work_collection_id,work_id,component_key)
    references private.bpay_next_work_collection(id,work_id,component_key)
);
create index bpay_next_segment_write_off_capacity_idx
  on private.bpay_next_work_segment_write_off_share(work_id,component_key,segment_key,case_event_id,origin_share_id);
create index bpay_next_segment_write_off_origin_idx on private.bpay_next_work_segment_write_off_share(origin_share_id,case_event_id);
alter table private.bpay_next_work_collection
  add column segment_attribution_complete boolean not null default false,
  add column observed_segment_negative_ex_vat numeric(18,2) not null default 0 check(observed_segment_negative_ex_vat>=0);
create table private.bpay_next_run_segment_capture (
  run_worker_id uuid primary key references private.bpay_next_run_worker(id),
  cursor_work_id uuid, cursor_segment_key text, cursor_component_key text,
  expected_count bigint not null check(expected_count>=0), processed_count bigint not null default 0 check(processed_count>=0),
  complete boolean not null default false,
  check(processed_count<=expected_count), check(not complete or processed_count=expected_count),
  check((cursor_work_id is null)=(cursor_segment_key is null)),
  check((cursor_work_id is null)=(cursor_component_key is null)),
  check(processed_count=0 or cursor_work_id is not null)
);
alter table private.bpay_next_run_position
  add column segment_attribution_complete boolean not null default false,
  add column segment_expected_member_count bigint not null default 0 check(segment_expected_member_count>=0),
  add column segment_member_count bigint not null default 0 check(segment_member_count>=0),
  add column segment_included_positive_ex_vat numeric(18,2) not null default 0 check(segment_included_positive_ex_vat>=0),
  add column segment_excluded_positive_ex_vat numeric(18,2) not null default 0 check(segment_excluded_positive_ex_vat>=0),
  add column segment_recovery_ex_vat numeric(18,2) not null default 0 check(segment_recovery_ex_vat>=0),
  add column segment_held_overhang_ex_vat numeric(18,2) not null default 0 check(segment_held_overhang_ex_vat>=0),
  add column snooze_id uuid, add column snooze_version bigint,
  add column snooze_excluded_positive_ex_vat numeric(18,2) not null default 0 check(snooze_excluded_positive_ex_vat>=0),
  add constraint bpay_next_run_position_segment_count_check check(segment_member_count<=segment_expected_member_count),
  add constraint bpay_next_run_position_segment_complete_check check(not segment_attribution_complete or segment_member_count=segment_expected_member_count),
  add constraint bpay_next_run_position_snooze_pair_check check((snooze_id is null)=(snooze_version is null));
alter table private.bpay_next_run_position drop constraint bpay_next_position_collection_shape_check;
alter table private.bpay_next_run_position add constraint bpay_next_position_collection_shape_check check
  ((work_collection_id is null and not managed_capture_completed)
    or (work_collection_id is not null and work_collection_revision>0 and not needs_financial_resolution
      and (residual_source_ex_vat<0 or (segment_attribution_complete and segment_recovery_ex_vat>0))));
alter table private.bpay_next_run_position drop constraint bpay_next_position_line_or_managed_check;
alter table private.bpay_next_run_position add constraint bpay_next_position_line_or_managed_check check
  (approved_line_id is not null or needs_financial_resolution or work_collection_id is not null
    or (segment_attribution_complete and segment_member_count>0 and approved_source_ex_vat=0
      and realised_source_ex_vat=0 and held_source_ex_vat=0));

do $security$
declare v_table text;
begin
  foreach v_table in array array['bpay_next_approved_segment','bpay_next_revision_worker_basis','bpay_next_segment_selection_page',
    'bpay_next_selected_segment','bpay_next_run_work_segment','bpay_next_run_work_segment_parent',
    'bpay_next_work_segment_origin_share','bpay_next_work_segment_effect_share',
    'bpay_next_work_segment_effect_origin','bpay_next_run_segment_capture',
    'bpay_next_work_segment_recovery_share','bpay_next_work_segment_write_off_share'] loop
    execute pg_catalog.format('alter table private.%I enable row level security',v_table);
    execute pg_catalog.format('revoke all on private.%I from public,anon,authenticated,service_role',v_table);
  end loop;
end
$security$;
alter table private.bpay_next_approved_segment owner to postgres;
alter table private.bpay_next_revision_worker_basis owner to postgres;
alter table private.bpay_next_segment_selection_page owner to postgres;
alter table private.bpay_next_selected_segment owner to postgres;
alter table private.bpay_next_run_work_segment owner to postgres;
alter table private.bpay_next_run_work_segment_parent owner to postgres;
alter table private.bpay_next_work_segment_origin_share owner to postgres;
alter table private.bpay_next_work_segment_effect_share owner to postgres;
alter table private.bpay_next_work_segment_effect_origin owner to postgres;
alter table private.bpay_next_run_segment_capture owner to postgres;
alter table private.bpay_next_work_segment_recovery_share owner to postgres;
alter table private.bpay_next_work_segment_write_off_share owner to postgres;
create policy service_owner on private.bpay_next_approved_segment to CURRENT_USER using(true) with check(true);
create policy service_owner on private.bpay_next_revision_worker_basis to CURRENT_USER using(true) with check(true);
create policy service_owner on private.bpay_next_segment_selection_page to CURRENT_USER using(true) with check(true);
create policy service_owner on private.bpay_next_selected_segment to CURRENT_USER using(true) with check(true);
create policy service_owner on private.bpay_next_run_work_segment to CURRENT_USER using(true) with check(true);
create policy service_owner on private.bpay_next_run_work_segment_parent to CURRENT_USER using(true) with check(true);
create policy service_owner on private.bpay_next_work_segment_origin_share to CURRENT_USER using(true) with check(true);
create policy service_owner on private.bpay_next_work_segment_effect_share to CURRENT_USER using(true) with check(true);
create policy service_owner on private.bpay_next_work_segment_effect_origin to CURRENT_USER using(true) with check(true);
create policy service_owner on private.bpay_next_run_segment_capture to CURRENT_USER using(true) with check(true);
create policy service_owner on private.bpay_next_work_segment_recovery_share to CURRENT_USER using(true) with check(true);
create policy service_owner on private.bpay_next_work_segment_write_off_share to CURRENT_USER using(true) with check(true);

commit;
