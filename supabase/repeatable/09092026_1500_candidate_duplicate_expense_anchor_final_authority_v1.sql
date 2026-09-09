-- Final Candidate duplicate-expense review authority.
--
-- The current 27082026_1436 Candidate withdrawal read authority transitively
-- replays the broad 04092026_1952 Candidate workflow owner during UPGRADE.
-- That historical closure also contains the superseded duplicate-review
-- helper.  Bind that complete replay root first, then reapply the current
-- narrow owner so NEW, UPGRADE and a later change to that root all finish with
-- the same definition.
-- No business rule is defined here: the single current owner remains the
-- exact 08092026_0631 replacement and this closure only establishes order.

\set ON_ERROR_STOP on

\ir 27082026_1436_candidate_withdrawal_read_authority_v1.sql
\ir 08092026_0631_candidate_duplicate_expense_anchor_inclusion_v1.sql
