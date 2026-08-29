begin;

create table if not exists public.import_review_weekly_validation_resolutions (
  id uuid primary key default gen_random_uuid(),
  import_id uuid not null references public.import_review_states(import_id) on delete cascade,
  hr_row_id uuid not null references public.hr_rows(id) on delete cascade,
  timesheet_id uuid not null references public.timesheets(timesheet_id) on delete restrict,
  resolution_code text not null,
  status text not null default 'CURRENT',
  evidence_fingerprint text not null,
  preview_generation integer not null,
  state_version bigint not null,
  selected_at_utc timestamptz not null default now(),
  selected_by_user_id uuid,
  stale_at_utc timestamptz,
  stale_reason_code text,
  applied_operation_id uuid references public.import_apply_operations(id) on delete restrict,
  applied_at_utc timestamptz,
  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now(),
  unique (import_id, hr_row_id)
);

do $constraints$
begin
  if not exists (
    select 1 from pg_constraint
    where conrelid='public.import_review_weekly_validation_resolutions'::regclass
      and conname='import_review_weekly_validation_resolution_code_ck'
  ) then
    alter table public.import_review_weekly_validation_resolutions
      add constraint import_review_weekly_validation_resolution_code_ck
      check (resolution_code='CANDIDATE_DID_NOT_WORK');
  end if;

  if not exists (
    select 1 from pg_constraint
    where conrelid='public.import_review_weekly_validation_resolutions'::regclass
      and conname='import_review_weekly_validation_resolution_status_ck'
  ) then
    alter table public.import_review_weekly_validation_resolutions
      add constraint import_review_weekly_validation_resolution_status_ck
      check (status in ('CURRENT','STALE','CLEARED','APPLIED'));
  end if;

  if not exists (
    select 1 from pg_constraint
    where conrelid='public.import_review_weekly_validation_resolutions'::regclass
      and conname='import_review_weekly_validation_resolution_bounds_ck'
  ) then
    alter table public.import_review_weekly_validation_resolutions
      add constraint import_review_weekly_validation_resolution_bounds_ck check (
        length(evidence_fingerprint)=64
        and evidence_fingerprint ~ '^[0-9a-f]{64}$'
        and preview_generation >= 0
        and state_version > 0
        and (status <> 'APPLIED' or (applied_operation_id is not null and applied_at_utc is not null))
      );
  end if;
end
$constraints$;

create index if not exists import_review_weekly_validation_resolution_import_idx
  on public.import_review_weekly_validation_resolutions(import_id,status,hr_row_id);
create index if not exists import_review_weekly_validation_resolution_timesheet_idx
  on public.import_review_weekly_validation_resolutions(timesheet_id);
create index if not exists import_review_weekly_validation_resolution_operation_idx
  on public.import_review_weekly_validation_resolutions(applied_operation_id)
  where applied_operation_id is not null;

drop trigger if exists trg_import_review_weekly_validation_resolution_guard
  on public.import_review_weekly_validation_resolutions;
create trigger trg_import_review_weekly_validation_resolution_guard
before update or delete on public.import_review_weekly_validation_resolutions
for each row execute function public._import_review_daily_resolution_guard_v1();

alter table public.import_review_weekly_validation_resolutions enable row level security;
revoke all on table public.import_review_weekly_validation_resolutions
  from public, anon, authenticated, service_role;

comment on table public.import_review_weekly_validation_resolutions is
  'HealthRoster Weekly validation-only exceptions. CANDIDATE_DID_NOT_WORK records that one imported HealthRoster shift was absent from a proved submitted Weekly timesheet. It never authorises an hours, TSFIN, invoice, payment or other financial mutation.';

commit;
