-- A Daily Candidate receipt that is explicitly and permanently removed by
-- Office must release the Timesheet foreign keys while retaining immutable
-- Candidate components as terminal audit history. Active Daily workflows keep
-- the original exact Timesheet identity shape unchanged.
alter table public.candidate_submission_workflows
  drop constraint if exists candidate_submission_workflows_identity_shape_ck;

alter table public.candidate_submission_workflows
  add constraint candidate_submission_workflows_identity_shape_ck check (
    (
      workflow_kind in ('CONTRACT_HOURS','CONTRACT_EXPENSE','CONTRACT_COMBINED')
      and contract_id is not null
      and contract_week_id is not null
      and week_ending_date is not null
    )
    or (
      workflow_kind='DAILY'
      and work_date is not null
      and contract_week_id is null
      and week_ending_date is null
      and (
        (
          target_timesheet_id is not null
          and anchor_timesheet_id=target_timesheet_id
        )
        or (
          state='CANCELLED'
          and target_timesheet_id is null
          and anchor_timesheet_id is null
          and issue_codes @> '["OFFICE_PERMANENTLY_DELETED_DAILY_RECEIPT"]'::jsonb
        )
      )
    )
  );

comment on constraint candidate_submission_workflows_identity_shape_ck
  on public.candidate_submission_workflows is
  'Active Daily workflows retain one exact Timesheet target/anchor. Only an Office-permanently-deleted, terminal Daily receipt may release both Timesheet links while its immutable components remain audit-only.';
