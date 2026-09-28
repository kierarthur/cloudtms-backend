-- Stage 2 successor (v5.1): reassert the current canonical final authorities after the ten
-- changed candidate/expense/invoice closures that hosted TEST re-runs in the same UPGRADE.
-- Hosted TEST records older ledger hashes for 27082026_1436, 04092026_1952,
-- 06092026_0337, 07092026_0331, 07092026_0611, 08092026_1615, 09092026_1500 and three
-- others, so UPGRADE re-runs them. Those closures carry older pages of
-- public.candidate_app_timesheet_page_v1 and public.client_create_with_settings_v1, whose
-- current authorities (24092026_2247 and 22092026_1226) are unchanged and would not re-run.
-- Hosted TEST also carries an older replay of the two Weekly Source dispatch routines whose
-- current authority is 15092026_2311. A clean NEW rebuild already produces these exact
-- definitions; this closure only replays the complete unchanged canonical files, in their
-- original source order, after every other pending closure. Do not edit ledgers or copy
-- hosted SQL. The shared routine private._timesheet_duplicate_expense_review_v1 is not
-- included: 26092026_0202 (definition) and 26092026_0209 (access list) remain its single
-- final authority.
\set ON_ERROR_STOP on
\ir 15092026_2311_weekly_source_delivery_targets_v1.sql
\ir 22092026_1226_client_initial_settings_baseline_v1.sql
\ir 24092026_2247_candidate_provisional_expense_carrier_lifecycle.sql
