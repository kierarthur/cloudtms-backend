alter table public.candidate_submission_workflows
  add column if not exists creation_request_sha256 bytea,
  add column if not exists creation_identity_json jsonb;

do $migration$
begin
  if not exists (
    select 1
    from pg_constraint
    where conrelid='public.candidate_submission_workflows'::regclass
      and conname='candidate_submission_workflows_creation_sha256_ck'
  ) then
    alter table public.candidate_submission_workflows
      add constraint candidate_submission_workflows_creation_sha256_ck
      check (
        creation_request_sha256 is null
        or octet_length(creation_request_sha256)=32
      );
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conrelid='public.candidate_submission_workflows'::regclass
      and conname='candidate_submission_workflows_creation_identity_ck'
  ) then
    alter table public.candidate_submission_workflows
      add constraint candidate_submission_workflows_creation_identity_ck
      check (
        creation_identity_json is null
        or jsonb_typeof(creation_identity_json)='object'
      );
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conrelid='public.candidate_submission_workflows'::regclass
      and conname='candidate_submission_workflows_creation_identity_group_ck'
  ) then
    alter table public.candidate_submission_workflows
      add constraint candidate_submission_workflows_creation_identity_group_ck
      check (
        (creation_request_sha256 is null and creation_identity_json is null)
        or (creation_request_sha256 is not null and creation_identity_json is not null)
      );
  end if;
end;
$migration$;

comment on column public.candidate_submission_workflows.creation_request_sha256 is
  'Immutable SHA-256 of the canonical Candidate workflow creation identity. Exact CREATE and rejected-resubmission replay compare this receipt instead of mutable lifecycle columns.';

comment on column public.candidate_submission_workflows.creation_identity_json is
  'Immutable canonical Candidate workflow creation identity, including initial route and stable worked-row or DAILY booking family. Lifecycle mutation must never rewrite it.';
