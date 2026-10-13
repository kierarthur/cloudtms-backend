-- First-install current Candidate runtime final reload authority (BD178).
-- Preserve complete native physical definitions/ACLs; invoke no business owner.

\set ON_ERROR_STOP on

\ir 10102026_1623_candidate_notification_management_v1.sql
\ir 10102026_1624_candidate_notification_consumers_v1.sql
\ir 12102026_2110_candidate_daily_legacy_two_way_availability_v1.sql
\ir 13102026_0029_candidate_daily_missing_legacy_choice_reconciliation_v1.sql
\ir 13102026_1033_candidate_daily_availability_copy_readiness_v1.sql

notify pgrst, 'reload schema';
