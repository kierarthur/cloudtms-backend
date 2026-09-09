-- Final Candidate duplicate-expense review authority.
--
-- Later changes to the broad 04092026_1952 Candidate workflow owner can
-- legitimately replay that closure during UPGRADE.  That historical closure
-- also contains the superseded duplicate-review helper.  Reapply the current
-- narrow owner afterwards so NEW and UPGRADE finish with the same definition.
-- No business rule is defined here: the single current owner remains the
-- exact 08092026_0631 replacement and this closure only establishes order.

\set ON_ERROR_STOP on

\ir 08092026_0631_candidate_duplicate_expense_anchor_inclusion_v1.sql
