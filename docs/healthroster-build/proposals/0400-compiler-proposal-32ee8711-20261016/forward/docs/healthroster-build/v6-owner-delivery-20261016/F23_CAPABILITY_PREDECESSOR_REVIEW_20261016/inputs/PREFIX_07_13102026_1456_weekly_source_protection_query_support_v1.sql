-- One-time CloudTMS schema/data migration: weekly_source_protection_query_support_v1
-- WP-P/WP-F7: optional automatic actor on existing global settings.
-- No existing actor is selected/backfilled; no mail or financial mutation.

\set ON_ERROR_STOP on

begin;

alter table public.weekly_source_global_settings
  add column automatic_reconcile_actor_user_id uuid references public.tms_users(id) on delete restrict;

-- Exact (a) question scope on the existing secure-manager due queue. Legacy
-- NULL events retain their existing requested-week/cohort semantics.
alter table public.weekly_manager_cohort_due_events
  add column not_finalised_comparison_revision_id uuid
    references public.weekly_issue_comparison_revisions(id) on delete restrict;
alter table public.weekly_manager_cohort_due_events
  add constraint weekly_manager_not_finalised_due_scope_ck check (
    not_finalised_comparison_revision_id is null
    or (event_kind='EARLY_ALL' and requested_week_membership_id is null)
  );

commit;
