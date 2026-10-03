-- The resumed historical H1/H2 closure replays the old pay-cache trigger
-- definition after Stage 2 A35 was already ledgered. CREATE OR REPLACE resets
-- that function's planner-local configuration, so restore the exact reviewed
-- A35 setting after all historical function reassertions. This changes no
-- function body, payment decision, provider call or external side effect.
\ir 26092026_0207_banking_pay_stage2_session_plan_cache_mode_v1.sql
