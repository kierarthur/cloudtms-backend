-- Retain a weekly Candidate workflow as terminal audit history after Office
-- permanently deletes its financially clean Timesheet removal unit. Active
-- weekly workflows keep the existing exact Contract Week identity shape.
alter table public.candidate_submission_workflows
  drop constraint if exists candidate_submission_workflows_identity_shape_ck;

alter table public.candidate_submission_workflows
  add constraint candidate_submission_workflows_identity_shape_ck check (
    (
      workflow_kind in ('CONTRACT_HOURS','CONTRACT_EXPENSE','CONTRACT_COMBINED')
      and contract_id is not null
      and week_ending_date is not null
      and (
        contract_week_id is not null
        or (
          state in ('REFUSED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED')
          and contract_week_id is null
          and target_timesheet_id is null
          and anchor_timesheet_id is null
          and issue_codes @> '["OFFICE_PERMANENTLY_DELETED_TIMESHEET"]'::jsonb
        )
      )
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
  'Active workflows retain exact live Timesheet and Contract Week identity. Only an Office-permanently-deleted terminal workflow may release those restrictive links while retaining immutable audit history.';
