-- Reinstall the current Candidate Timesheet-card projection after the
-- historical read closure was restored during the manager-refusal release.
-- This keeps the approved base-expense fallback authoritative.

\ir 27082026_2350_candidate_timesheet_card_base_expense_fallback_v1.sql
