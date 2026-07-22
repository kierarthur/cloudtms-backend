-- Immutable per-action outcomes allow one durable import review to be
-- completed in bounded candidate/client batches.  Deferred actions are not
-- written here and therefore remain reversible while the review is open.

create table if not exists public.import_review_action_outcomes (
  action_id text primary key,
  import_id uuid not null references public.import_review_states(import_id) on delete restrict,
  operation_id uuid not null references public.import_apply_operations(id) on delete restrict,
  action_kind text not null,
  source_identity text not null,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  contract_id uuid references public.contracts(id) on delete restrict,
  hr_row_id uuid references public.hr_rows(id) on delete restrict,
  timesheet_id uuid references public.timesheets(timesheet_id) on delete restrict,
  shift_id uuid references public.nhsp_shifts(id) on delete restrict,
  evidence_fingerprint text not null,
  completed_label text not null,
  summary_json jsonb not null default '{}'::jsonb,
  applied_at_utc timestamptz not null default now(),
  applied_by_user_id uuid not null references public.tms_users(id) on delete restrict,
  unique (operation_id, action_id)
);

do $constraints$
begin
  if not exists (
    select 1 from pg_constraint
    where conrelid='public.import_review_action_outcomes'::regclass
      and conname='import_review_action_outcomes_identity_ck'
  ) then
    alter table public.import_review_action_outcomes
      add constraint import_review_action_outcomes_identity_ck check (
        length(action_id)=64 and action_id ~ '^[0-9a-f]{64}$'
        and length(source_identity) between 1 and 1024
        and length(evidence_fingerprint)=64 and evidence_fingerprint ~ '^[0-9a-f]{64}$'
        and length(completed_label) between 1 and 160
        and pg_column_size(summary_json)<=32768
      );
  end if;
end
$constraints$;

create index if not exists import_review_action_outcomes_import_idx
  on public.import_review_action_outcomes(import_id,applied_at_utc,action_id);
create index if not exists import_review_action_outcomes_candidate_client_idx
  on public.import_review_action_outcomes(import_id,candidate_id,client_id,applied_at_utc);
create index if not exists import_review_action_outcomes_operation_idx
  on public.import_review_action_outcomes(operation_id,action_id);

alter table public.import_review_action_outcomes enable row level security;
revoke all on table public.import_review_action_outcomes from public,anon,authenticated,service_role;

-- The repository migration runner installs repeatables before one-time
-- migrations.  Create the append-only trigger here as well when the lifecycle
-- repeatable has already supplied its guard; the lifecycle repeatable performs
-- the reciprocal check for migration-first/manual installs.
do $outcome_trigger$
begin
  if to_regprocedure('public._import_review_action_outcomes_immutable_guard_v1()') is not null
     and not exists (
       select 1 from pg_trigger
       where tgrelid='public.import_review_action_outcomes'::regclass
         and tgname='trg_import_review_action_outcomes_immutable' and not tgisinternal
     ) then
    execute 'create trigger trg_import_review_action_outcomes_immutable before update or delete on public.import_review_action_outcomes for each row execute function public._import_review_action_outcomes_immutable_guard_v1()';
  end if;
end
$outcome_trigger$;

do $shape$
declare v_missing text;
begin
  with required(column_name,udt_name,is_nullable) as (values
    ('action_id','text','NO'),('import_id','uuid','NO'),('operation_id','uuid','NO'),
    ('action_kind','text','NO'),('source_identity','text','NO'),('candidate_id','uuid','NO'),('client_id','uuid','NO'),
    ('evidence_fingerprint','text','NO'),('completed_label','text','NO'),
    ('summary_json','jsonb','NO'),('applied_at_utc','timestamptz','NO'),
    ('applied_by_user_id','uuid','NO')
  )
  select string_agg(r.column_name,', ' order by r.column_name) into v_missing
  from required r
  left join information_schema.columns c
    on c.table_schema='public' and c.table_name='import_review_action_outcomes'
   and c.column_name=r.column_name and c.udt_name=r.udt_name and c.is_nullable=r.is_nullable
  where c.column_name is null;
  if v_missing is not null then
    raise exception 'IMPORT_REVIEW_INCREMENTAL_OUTCOME_SHAPE_INVALID: %',v_missing;
  end if;
end
$shape$;
