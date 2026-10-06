-- Connect the isolated 0470 selector to genuine Source CURRENT writers.
-- No historical bootstrap/data sweep. 0470 was isolated and unreleased;
-- these new required facts are populated only by the actual typed writers.
begin;

create table private.bpay_next_source_current_observations (
  scope_id uuid not null references private.bpay_next_source_current_scopes(id),
  final_revision_id uuid not null references public.weekly_source_final_revisions(id),
  order_key bit(128) not null,
  manifest_hash bytea not null check(octet_length(manifest_hash)=32),
  active boolean not null,
  accepted_by_session_id uuid references public.weekly_final_source_correction_sessions(id),
  retracted_by_session_id uuid references public.weekly_final_source_correction_sessions(id),
  primary key(scope_id,final_revision_id),
  check(retracted_by_session_id is null or not active)
);
create index bp_next_source_observation_head_idx on private.bpay_next_source_current_observations
  (scope_id,order_key desc,final_revision_id) where active;
create index bp_next_source_observation_revision_idx on private.bpay_next_source_current_observations
  (final_revision_id,scope_id) where active;
alter table private.bpay_next_source_current_observations owner to postgres;
alter table private.bpay_next_source_current_observations enable row level security;
revoke all on private.bpay_next_source_current_observations from public,anon,authenticated,service_role;

-- Transaction working relations, not a persisted root-list receipt. Every
-- successful owner removes its exact operation before returning. The deferred
-- constraint additionally forbids accidentally committing working copies.
create table private.bpay_next_source_current_lock_operations (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  transaction_id xid8 not null default pg_catalog.pg_current_xact_id(),
  upload_id uuid references public.weekly_source_uploads(id),
  projection_publication_id uuid references public.weekly_source_projection_publications(id),
  generation integer check(generation>=1),
  old_revision_id uuid references public.weekly_source_final_revisions(id),
  source_group_id uuid not null references public.weekly_source_groups(id),
  client_id uuid not null references public.clients(id),
  coverage_start date,
  coverage_end date,
  check((upload_id is null)=(generation is null)),
  check((upload_id is null)=(projection_publication_id is null)),
  check(upload_id is not null or old_revision_id is not null),
  check((coverage_start is null)=(coverage_end is null)),
  check(coverage_start is null or coverage_start<=coverage_end)
);
create table private.bpay_next_source_current_lock_members (
  operation_id uuid not null references private.bpay_next_source_current_lock_operations(id) on delete cascade,
  root_timesheet_id uuid not null references public.timesheets(timesheet_id),
  raw_booking_id text not null check(pg_catalog.btrim(raw_booking_id)<>''),
  canonical_booking_id text not null,
  primary key(operation_id,root_timesheet_id),
  check(canonical_booking_id=pg_catalog.btrim(raw_booking_id))
);
create index bp_next_source_lock_member_order_idx on private.bpay_next_source_current_lock_members
  (operation_id,canonical_booking_id,raw_booking_id,root_timesheet_id);
alter table private.bpay_next_source_current_lock_operations owner to postgres;
alter table private.bpay_next_source_current_lock_members owner to postgres;
alter table private.bpay_next_source_current_lock_operations enable row level security;
alter table private.bpay_next_source_current_lock_members enable row level security;
revoke all on private.bpay_next_source_current_lock_operations,private.bpay_next_source_current_lock_members
  from public,anon,authenticated,service_role;
create function private.bpay_next_source_lock_operation_cleanup_assert_v1()
returns trigger language plpgsql security definer set search_path=pg_catalog,private as $f$
begin
  if exists(select 1 from private.bpay_next_source_current_lock_operations o where o.id=new.id) then
    raise exception 'BPAY_NEXT_SOURCE_LOCK_OPERATION_NOT_RELEASED' using errcode='55000';
  end if;
  return null;
end $f$;
alter function private.bpay_next_source_lock_operation_cleanup_assert_v1() owner to postgres;
revoke all on function private.bpay_next_source_lock_operation_cleanup_assert_v1()
  from public,anon,authenticated,service_role;
create constraint trigger bp_next_source_lock_operation_cleanup
  after insert on private.bpay_next_source_current_lock_operations
  deferrable initially deferred for each row
  execute function private.bpay_next_source_lock_operation_cleanup_assert_v1();

-- Current-only coverage inventories must NOT start by enumerating the growing
-- historical weekly_work_events relation. These immutable identity scalars
-- come from the exact bound event/scope when the maintained pointer is written.
alter table private.bpay_next_source_current_events
  add column source_group_id uuid not null references public.weekly_source_groups(id),
  add column client_id uuid not null references public.clients(id),
  add column work_date date not null;
alter table private.bpay_next_source_current_expenses
  add column source_group_id uuid not null references public.weekly_source_groups(id),
  add column client_id uuid not null references public.clients(id),
  add column work_date date not null;
create index bp_next_source_hr_coverage_idx on private.bpay_next_source_current_events
  (source_group_id,client_id,work_date,scope_id,work_event_id)
  where source_kind='HR' and (source_present or ambiguous);
create index bp_next_source_expense_coverage_idx on private.bpay_next_source_current_expenses
  (source_group_id,client_id,work_date,scope_id,work_event_id) where positive;

-- Exact emitted-revision keyset inventories. Work is proportional only to the
-- actual new/removed revision payload, never all family/revision history.
create index bp_next_source_nhsp_emission_idx on public.weekly_source_billing_movements
  (final_revision_id,id) where source_profile_kind='NHSP_TRUST_BACKING_REPORT'
    and source_line_kind in ('NHSP_PHYSICAL_POSITIVE','NHSP_PHYSICAL_FULL_NEGATIVE');
create index bp_next_source_transition_emission_idx on public.weekly_source_state_transitions
  (final_revision_id,id);
create index bp_next_source_expense_emission_idx on public.weekly_expense_authority_generations
  (final_revision_id,id);
create index bp_next_source_origin_revision_seek_idx on private.bpay_next_source_current_origins
  (final_revision_id,origin_kind,origin_id) where active;
commit;
