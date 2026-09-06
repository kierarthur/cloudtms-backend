-- Reassert the current Office Candidate rejection/delete owner after the
-- historical Office adapter repeatable is refreshed.  The adapter still owns
-- the Timesheet detail projection, while this later owner preserves the
-- submitted/manager-approved rejection guard and linked pending-expense scope.

\ir 05092026_2100_candidate_submission_delete_reject_guard_v1.sql
