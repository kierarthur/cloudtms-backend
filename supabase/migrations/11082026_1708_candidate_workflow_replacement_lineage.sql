alter table public.candidate_submission_workflows
  add column if not exists replacement_of_workflow_id uuid;

do $migration$
begin
  if not exists (
    select 1
    from pg_catalog.pg_constraint
    where conname = 'candidate_submission_workflows_replacement_of_fk'
      and conrelid = 'public.candidate_submission_workflows'::regclass
  ) then
    alter table public.candidate_submission_workflows
      add constraint candidate_submission_workflows_replacement_of_fk
      foreign key (replacement_of_workflow_id)
      references public.candidate_submission_workflows(id)
      on delete restrict;
  end if;

  if not exists (
    select 1
    from pg_catalog.pg_constraint
    where conname = 'candidate_submission_workflows_replacement_not_self_ck'
      and conrelid = 'public.candidate_submission_workflows'::regclass
  ) then
    alter table public.candidate_submission_workflows
      add constraint candidate_submission_workflows_replacement_not_self_ck
      check (replacement_of_workflow_id is null or replacement_of_workflow_id <> id);
  end if;
end;
$migration$;

create unique index if not exists candidate_submission_workflows_replacement_source_uq
  on public.candidate_submission_workflows (replacement_of_workflow_id)
  where replacement_of_workflow_id is not null;

comment on column public.candidate_submission_workflows.replacement_of_workflow_id is
  'Server-owned direct lineage to the rejected workflow replaced by this workflow.';
