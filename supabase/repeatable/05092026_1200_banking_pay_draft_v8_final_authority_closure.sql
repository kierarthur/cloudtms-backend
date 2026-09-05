-- Final H1/H2 Banking Pay authority closure.
--
-- The H1 recovery closure intentionally replays historical compatibility roots.
-- On UPGRADE, that closure can be newly pending while later H2 definitions are
-- already recorded as installed. Reassert every reviewed current definition
-- needed by the combined H1/H2 release so NEW, UPGRADE and exact retry converge to
-- one byte-identical authority. The Candidate weekly owner is also replayed
-- unchanged because the current shared TEST successor amended it after the H1
-- closure was generated. This file defines no function and changes no
-- selection, amount, tax, VAT, payment, provider or cancellation policy.
\set ON_ERROR_STOP on

\ir 30082026_2358_banking_pay_dirty_apply_family_authority_repair_v1.sql
\ir 27082026_2205_candidate_weekly_manager_finalisation_authority_v1.sql
\ir 04092026_1330_banking_pay_manual_carry_forward_selection_authority_v1.sql
\ir 04092026_1360_banking_pay_manual_carry_forward_allocation_seed_v8.sql
\ir 05092026_0310_banking_pay_draft_integrity_setwise_v8.sql
\ir 05092026_0405_banking_pay_one_candidate_cancellation_scope_integrity_v1.sql
\ir 04092026_2350_banking_pay_cancellation_completion_v1.sql
\ir 05092026_0655_banking_pay_active_draft_reservation_evidence_precedence_v1.sql
