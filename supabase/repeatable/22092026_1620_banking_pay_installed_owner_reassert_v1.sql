-- Restore the five canonical Banking Pay owners previously recorded installed
-- in release 20260822-test-authority-upgrade-6a56f1ca03c7 (9 September).
-- A clean NEW rebuild already produces these definitions. The existing TEST
-- ledger has their current closure hashes but its installed bodies are older.
-- Reuse complete canonical authorities; do not edit ledgers or copy hosted SQL.
\set ON_ERROR_STOP on
\ir 07092026_2013_banking_pay_unpaid_cancellation_communication_v2_prepare_v1.sql
\ir 07092026_2140_banking_pay_manual_carry_forward_economic_identity_v1.sql
\ir 08092026_0518_banking_pay_candidate_dirty_cohort_authority_v1.sql
\ir 08092026_1200_banking_pay_cancel_return_selection_intent_v1.sql
\ir 09092026_0020_banking_pay_no_money_workbench_return_v1.sql
