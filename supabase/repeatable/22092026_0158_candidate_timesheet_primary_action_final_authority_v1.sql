-- Repeatable CloudTMS final authority: candidate_timesheet_primary_action_final_authority_v1
--
-- Managed upgrades apply only new or changed repeatables. Reassert the current
-- Candidate action owner after all historical action overlays so an upgraded
-- database and a clean Candidate install expose the same final behaviour.

\set ON_ERROR_STOP on

\ir 05092026_0941_candidate_protected_additional_expense_action_v1.sql
