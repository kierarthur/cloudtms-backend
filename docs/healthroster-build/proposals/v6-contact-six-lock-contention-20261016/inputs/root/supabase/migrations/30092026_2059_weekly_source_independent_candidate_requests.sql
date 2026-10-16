-- One-time CloudTMS schema/data migration: weekly_source_independent_candidate_requests
-- Separate ownership of missing-week submissions from signed-hours questions.
-- Preserve existing request IDs, membership and clocks. No notifications sent.

\set ON_ERROR_STOP on

begin;

alter table public.weekly_candidate_cohorts add column current_submission_generation_id uuid
  references public.weekly_candidate_outreach_generations(id) on delete restrict;
update public.weekly_candidate_cohorts cohort
set current_submission_generation_id=generation.id,current_generation_id=null
from public.weekly_candidate_outreach_generations generation
where generation.id=cohort.current_generation_id and generation.request_kind='SUBMIT_TIMESHEET';
drop index public.weekly_candidate_outreach_generations_active_uq;
create unique index weekly_candidate_outreach_generations_active_uq
  on public.weekly_candidate_outreach_generations(candidate_cohort_id,request_kind) where state='ACTIVE';
alter table public.weekly_manager_cohort_due_events add column requested_week_membership_id uuid
  references public.weekly_timesheet_submission_request_memberships(id) on delete restrict;

commit;
