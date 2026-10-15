-- ISOLATED current-Source selector storage. No released Source writer or
-- protected context is connected by this migration. No historical bootstrap.
-- 0475 admits only actual CURRENT origins (PREPARED origins are invisible).
-- The NHSP tree stores counts, not money. Its real signed date/time/int key
-- has 128 bits: each update touches 129 PK nodes, each search <=128 nodes.
-- History may increase retained storage, never the number of rows read by a
-- point update/search. Source economic snapshots/lineages remain authoritative.
begin;

create table private.bpay_next_source_current_scopes (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  -- Maintained current root pointer. Captured origins retain their ORIGINAL
  -- immutable lineage/invoice physical identities independently of this field.
  root_timesheet_id uuid not null references public.timesheets(timesheet_id),
  family_booking_id text not null check(char_length(family_booking_id) between 1 and 200),
  source_group_id uuid not null references public.weekly_source_groups(id),
  candidate_id uuid not null references public.candidates(id),
  client_id uuid not null references public.clients(id),
  contract_id uuid not null references public.contracts(id),
  week_ending_date date not null,
  unique(source_group_id,family_booking_id,candidate_id,client_id,contract_id,week_ending_date),
  unique(root_timesheet_id,source_group_id)
);

create table private.bpay_next_source_current_origins (
  origin_kind text not null check(origin_kind in ('MOVEMENT','TRANSITION','EXPENSE')),
  origin_id uuid not null,
  movement_id uuid references public.weekly_source_billing_movements(id),
  transition_id uuid references public.weekly_source_state_transitions(id),
  expense_authority_id uuid references public.weekly_expense_authority_generations(id),
  scope_id uuid not null references private.bpay_next_source_current_scopes(id),
  work_event_id uuid not null references public.weekly_work_events(id),
  final_revision_id uuid not null references public.weekly_source_final_revisions(id),
  order_key bit(128) not null,
  source_hash bytea not null check(octet_length(source_hash)=32),
  row_resolution_id uuid references public.weekly_source_row_resolutions(id),
  lineage_id uuid references public.weekly_source_row_timesheet_lineages(id),
  economic_snapshot_id uuid references public.weekly_source_row_economic_snapshots(id),
  selected_movement_id uuid references public.weekly_source_billing_movements(id),
  snapshot_line_id uuid references public.weekly_source_final_snapshot_lines(id),
  magnitude_hash bytea check(magnitude_hash is null or octet_length(magnitude_hash)=32),
  contribution smallint check(contribution in (-1,1)),
  representative_created_at timestamptz not null,
  active boolean not null default false,
  retracted_by_session_id uuid references public.weekly_final_source_correction_sessions(id),
  primary key(origin_kind,origin_id),
  check((origin_kind='MOVEMENT' and movement_id is not null and movement_id=origin_id and transition_id is null and expense_authority_id is null
      and magnitude_hash is not null and contribution is not null)
    or (origin_kind='TRANSITION' and transition_id is not null and transition_id=origin_id and movement_id is null and expense_authority_id is null
      and magnitude_hash is null and contribution is null)
    or (origin_kind='EXPENSE' and expense_authority_id is not null and expense_authority_id=origin_id and movement_id is null and transition_id is null
      and magnitude_hash is null and contribution is null))
);
-- Exact representative reselection, exact active HR state and exact correction
-- inventory are separate indexes; no latest movement UUID is an entitlement.
create index bp_next_source_rep_idx on private.bpay_next_source_current_origins
  (scope_id,work_event_id,magnitude_hash,order_key,representative_created_at desc,(origin_id::text) desc)
  where active and origin_kind='MOVEMENT' and contribution=1;
create index bp_next_source_transition_idx on private.bpay_next_source_current_origins
  (scope_id,work_event_id,order_key desc,(origin_id::text) desc)
  where active and origin_kind='TRANSITION';
create index bp_next_source_revision_origin_idx on private.bpay_next_source_current_origins
  (final_revision_id,scope_id,origin_kind,origin_id) where active;

create table private.bpay_next_source_current_revisions (
  scope_id uuid not null references private.bpay_next_source_current_scopes(id),
  final_revision_id uuid not null references public.weekly_source_final_revisions(id),
  order_key bit(128) not null,
  active_origin_count bigint not null check(active_origin_count>=0),
  primary key(scope_id,final_revision_id)
);
create index bp_next_source_revision_head_idx on private.bpay_next_source_current_revisions
  (scope_id,order_key desc,final_revision_id) where active_origin_count>0;

create table private.bpay_next_source_current_magnitudes (
  scope_id uuid not null references private.bpay_next_source_current_scopes(id),
  work_event_id uuid not null references public.weekly_work_events(id),
  magnitude_hash bytea not null check(octet_length(magnitude_hash)=32),
  economic_magnitude jsonb not null check(jsonb_typeof(economic_magnitude)='object'),
  surviving_order_key bit(128),
  selected_movement_id uuid references public.weekly_source_billing_movements(id),
  primary key(scope_id,work_event_id,magnitude_hash),
  check((surviving_order_key is null)=(selected_movement_id is null))
);
create index bp_next_source_magnitude_head_idx on private.bpay_next_source_current_magnitudes
  (scope_id,work_event_id,surviving_order_key desc,magnitude_hash)
  where surviving_order_key is not null;

create table private.bpay_next_source_current_nodes (
  scope_id uuid not null,
  work_event_id uuid not null,
  magnitude_hash bytea not null,
  depth smallint not null check(depth between 0 and 128),
  prefix bit(128) not null,
  positive_count bigint not null default 0 check(positive_count>=0),
  negative_count bigint not null default 0 check(negative_count>=0),
  net bigint not null default 0,
  max_positive_suffix bigint,
  primary key(scope_id,work_event_id,magnitude_hash,depth,prefix),
  foreign key(scope_id,work_event_id,magnitude_hash)
    references private.bpay_next_source_current_magnitudes(scope_id,work_event_id,magnitude_hash),
  check(depth=128 or (positive_count=0 and negative_count=0)),
  check(depth<>128 or (net=positive_count-negative_count
    and max_positive_suffix is not distinct from case when positive_count>0 then net else null end))
);

create table private.bpay_next_source_current_snapshots (
  snapshot_line_id uuid primary key references public.weekly_source_final_snapshot_lines(id),
  scope_id uuid not null references private.bpay_next_source_current_scopes(id),
  work_event_id uuid not null references public.weekly_work_events(id),
  movement_id uuid not null references public.weekly_source_billing_movements(id)
);

create table private.bpay_next_source_current_events (
  scope_id uuid not null references private.bpay_next_source_current_scopes(id),
  work_event_id uuid not null references public.weekly_work_events(id),
  source_kind text not null check(source_kind in ('NHSP','HR')),
  source_present boolean not null,
  ambiguous boolean not null default false,
  selected_movement_id uuid references public.weekly_source_billing_movements(id),
  selected_transition_id uuid references public.weekly_source_state_transitions(id),
  selected_order_key bit(128),
  primary key(scope_id,work_event_id),
  check(not source_present or selected_movement_id is not null),
  check(not ambiguous or not source_present)
);
create index bp_next_source_current_event_page_idx on private.bpay_next_source_current_events
  (scope_id,work_event_id) where source_present or ambiguous;

create table private.bpay_next_source_current_expenses (
  scope_id uuid not null references private.bpay_next_source_current_scopes(id),
  work_event_id uuid not null references public.weekly_work_events(id),
  authority_id uuid not null references public.weekly_expense_authority_generations(id),
  positive boolean not null,
  primary key(scope_id,work_event_id),
  unique(authority_id)
);
create index bp_next_source_current_expense_page_idx on private.bpay_next_source_current_expenses
  (scope_id,work_event_id) where positive;

create table private.bpay_next_protected_current_decisions (
  family_id uuid not null references public.weekly_exceptional_pay_target_families(id),
  work_event_id uuid not null references public.weekly_work_events(id),
  event_id uuid not null references public.weekly_exceptional_pay_family_events(id),
  event_sequence bigint not null,
  state text not null check(state in ('WAIT','ACCEPTED_SOURCE','NOT_WORKED','FIRST_AUTHORISATION_WITHDRAWN')),
  event_hash bytea not null check(octet_length(event_hash)=32),
  primary key(family_id,work_event_id),
  unique(event_id)
);
create index bp_next_protected_current_wait_idx on private.bpay_next_protected_current_decisions
  (family_id,work_event_id) where state='WAIT';

-- Owner-only storage. No browser/service-role permission or policy is added.
do $acl$
declare v_table text;
begin
  foreach v_table in array array[
    'bpay_next_source_current_scopes','bpay_next_source_current_origins',
    'bpay_next_source_current_revisions','bpay_next_source_current_magnitudes',
    'bpay_next_source_current_nodes','bpay_next_source_current_snapshots',
    'bpay_next_source_current_events','bpay_next_source_current_expenses',
    'bpay_next_protected_current_decisions'
  ] loop
    execute format('alter table private.%I owner to %I',v_table,current_user);
    execute format('alter table private.%I enable row level security',v_table);
    execute format('revoke all on table private.%I from public,anon,authenticated,service_role',v_table);
  end loop;
end $acl$;
commit;
