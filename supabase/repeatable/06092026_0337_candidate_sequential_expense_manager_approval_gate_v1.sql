-- Repeatable CloudTMS function authority:
-- candidate_sequential_expense_manager_approval_gate_v1
--
-- One unfinished Candidate expense submission remains the exclusive claim for
-- a Candidate, Contract and week. Once manager approval has safely finalised
-- that submission, it no longer blocks the next expense-only claim. Agency
-- authorisation is deliberately not a creation gate.

\set ON_ERROR_STOP on

-- Reapply the complete workflow-creation and Timesheet-action authorities in
-- their required successor order. Both included files finish with their exact
-- owner and ACL contracts.
\ir 04092026_1952_candidate_expense_history_anchor_recovery_v1.sql
\ir 05092026_0941_candidate_protected_additional_expense_action_v1.sql
