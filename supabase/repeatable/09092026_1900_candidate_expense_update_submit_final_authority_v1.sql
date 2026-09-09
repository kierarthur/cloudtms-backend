-- Final Candidate pending-expense submit authority.
--
-- The current 06092026_1636 Candidate expense policy is intentionally
-- repeatable and may therefore replay its earlier submit definition during an
-- UPGRADE.  The later 08092026_0918 owner adds the established root-snapshot
-- and immutable-history validation.  Bind the replaying root first and then
-- reapply that exact later owner so NEW and UPGRADE finish identically.
--
-- This closure defines no new Candidate, expense, payment, tax or Banking
-- policy.  Both included authorities remain independently source-owned.

\set ON_ERROR_STOP on

\ir 06092026_1636_candidate_advanced_expense_component_policy_v1.sql
\ir 08092026_0918_candidate_expense_update_root_snapshot_validation_v1.sql
