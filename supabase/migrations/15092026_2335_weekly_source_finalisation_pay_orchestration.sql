-- Additive durability for the post-final-source ordinary Timesheet/TSFIN join.
-- Source finalisation and invoice manifests remain authoritative even when a
-- candidate-pay projection later needs recovery or the existing locked-root
-- correction path.

\set ON_ERROR_STOP on

begin;

create table public.weekly_source_finalisation_pay_runs (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  final_revision_id uuid not null unique
    references public.weekly_source_final_revisions(id) on delete restrict,
  source_cycle_id uuid not null
    references public.weekly_source_cycles(id) on delete restrict,
  requested_by_user_id uuid not null
    references public.tms_users(id) on delete restrict,
  orchestration_key text not null unique
    check (pg_catalog.char_length(pg_catalog.btrim(orchestration_key)) between 1 and 200),
  final_revision_manifest_hash bytea not null
    check (pg_catalog.octet_length(final_revision_manifest_hash)=32),
  task_manifest_hash bytea not null
    check (pg_catalog.octet_length(task_manifest_hash)=32),
  task_count integer not null check (task_count>=0),
  terminal_task_count integer not null default 0
    check (terminal_task_count between 0 and task_count),
  action_required_task_count integer not null default 0
    check (action_required_task_count between 0 and task_count),
  state text not null check (state in (
    'READY','RUNNING','RECOVERY_REQUIRED','ACTION_REQUIRED','COMPLETE'
  )),
  run_hash bytea not null unique check (pg_catalog.octet_length(run_hash)=32),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  completed_at_utc timestamptz,
  check ((state='COMPLETE')=(completed_at_utc is not null)),
  check (state<>'COMPLETE' or terminal_task_count=task_count),
  check (state<>'COMPLETE' or action_required_task_count=0)
);
alter table public.weekly_source_finalisation_pay_runs owner to postgres;

create table public.weekly_source_finalisation_pay_tasks (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  run_id uuid not null
    references public.weekly_source_finalisation_pay_runs(id) on delete restrict,
  task_ordinal integer not null check (task_ordinal>=1),
  client_manifest_id uuid not null
    references public.weekly_source_client_manifests(id) on delete restrict,
  root_timesheet_id uuid not null
    references public.timesheets(timesheet_id) on delete restrict,
  -- WB-016 and 25 section 14 "Removed": a stored timesheet_id is not the
  -- current Timesheet for ever.  The task carries the Timesheet family identity
  -- and the version it was opened against so every retry, recovery and delayed
  -- release resolves through the family instead of trusting the physical id.
  root_family_booking_id text not null
    check (pg_catalog.char_length(pg_catalog.btrim(root_family_booking_id)) between 1 and 200),
  root_timesheet_version integer not null check (root_timesheet_version>=1),
  projection_idempotency_key text not null unique
    check (pg_catalog.char_length(pg_catalog.btrim(projection_idempotency_key)) between 1 and 200),
  prepared_context_json jsonb not null
    check (pg_catalog.jsonb_typeof(prepared_context_json)='object'),
  prepared_context_hash bytea not null
    check (pg_catalog.octet_length(prepared_context_hash)=32),
  -- Gate 2 / S9: 'REFUSED_LOCKED' is removed, not re-routed.  A paid, invoiced
  -- or Draft-frozen root is no longer an obstacle, because the later-change
  -- path no longer mutates the root: it composes a complete proposal and stops.
  -- The terminal states mirror the projection receipt's outcome exactly.
  state text not null check (state in (
    'READY','SUBMISSION_STARTED','RECOVERY_REQUIRED','PREPARED_FOR_AUTHORISATION',
    'PROPOSED','NO_OP_FIRST_NEGATIVE','TARGET_MANAGED_SUPPRESSED','FAILED'
  )),
  attempt_count integer not null default 0 check (attempt_count>=0),
  version bigint not null default 1 check (version>=1),
  projection_receipt_id uuid unique
    references public.weekly_source_ordinary_pay_projection_receipts(id) on delete restrict,
  bounded_error_json jsonb
    check (bounded_error_json is null or pg_catalog.jsonb_typeof(bounded_error_json)='object'),
  started_at_utc timestamptz,
  completed_at_utc timestamptz,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (run_id,task_ordinal),
  unique (run_id,prepared_context_hash),
  check ((attempt_count=0)=(started_at_utc is null)),
  check ((state in ('PREPARED_FOR_AUTHORISATION','PROPOSED','NO_OP_FIRST_NEGATIVE',
                    'TARGET_MANAGED_SUPPRESSED'))
    =(projection_receipt_id is not null)),
  check ((state in ('PREPARED_FOR_AUTHORISATION','PROPOSED','NO_OP_FIRST_NEGATIVE',
                    'TARGET_MANAGED_SUPPRESSED','FAILED'))
    =(completed_at_utc is not null)),
  check (state not in ('RECOVERY_REQUIRED','FAILED') or bounded_error_json is not null)
);
alter table public.weekly_source_finalisation_pay_tasks owner to postgres;
-- WB-016: one task per Timesheet FAMILY per run.  The old
-- unique (run_id,root_timesheet_id) let a rotation produce a second task for
-- the same week, so the family key replaces it.  root_timesheet_id keeps a
-- plain index because the foreign keys and existence predicates still use it.
create unique index weekly_source_finalisation_pay_tasks_run_family_uq
  on public.weekly_source_finalisation_pay_tasks(run_id,pg_catalog.btrim(root_family_booking_id));
create index weekly_source_finalisation_pay_tasks_root_idx
  on public.weekly_source_finalisation_pay_tasks(root_timesheet_id);
create index weekly_source_finalisation_pay_tasks_state_idx
  on public.weekly_source_finalisation_pay_tasks(run_id,state,task_ordinal);

alter table public.weekly_source_finalisation_pay_runs enable row level security;
alter table public.weekly_source_finalisation_pay_runs force row level security;
revoke all on table public.weekly_source_finalisation_pay_runs
  from public,anon,authenticated,service_role;
grant select,insert,update on table public.weekly_source_finalisation_pay_runs
  to service_role;

alter table public.weekly_source_finalisation_pay_tasks enable row level security;
alter table public.weekly_source_finalisation_pay_tasks force row level security;
revoke all on table public.weekly_source_finalisation_pay_tasks
  from public,anon,authenticated,service_role;
grant select,insert,update on table public.weekly_source_finalisation_pay_tasks
  to service_role;

do $weekly_source_finalisation_pay_rls$
declare
  v_table text;
begin
  foreach v_table in array array[
    'weekly_source_finalisation_pay_runs',
    'weekly_source_finalisation_pay_tasks'
  ]::text[]
  loop
    execute pg_catalog.format(
      'create policy cloudtms_miget_service_owner_all on public.%I for all to %I, service_role using (true) with check (true)',
      v_table,current_user
    );
  end loop;
end;
$weekly_source_finalisation_pay_rls$;

commit;
