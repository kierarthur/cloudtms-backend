-- Final authority for Candidate withdrawal reads and transitions.
--
-- The original Candidate read bundle owns the withdrawal action contract, but
-- it also contains older definitions of several public read routines. Keeping
-- those source dependencies inside this final authority means that any future
-- edit to either legacy bundle changes this repeatable's closure and therefore
-- replays the complete current authority chain in the correct order.
\ir 07082026_2108_candidate_app_read_and_missing_week_rpcs_v1.sql
\ir 07082026_2120_candidate_workflow_transition_atomic_v1.sql

-- Restore every later public read authority displaced by the legacy bundle.
\ir 25082026_2024_candidate_nullif_runtime_correction_v1.sql
\ir 25082026_2043_candidate_home_notification_runtime_v1.sql
\ir 25082026_2255_candidate_home_actionable_timesheet_count_v1.sql
\ir 26082026_1432_candidate_home_draft_counts_v1.sql
\ir 26082026_1516_candidate_timesheet_card_draft_linkage_v1.sql
\ir 26082026_1537_candidate_home_draft_identity_v1.sql
\ir 27082026_0858_candidate_finalised_artifact_readiness_v1.sql
\ir 27082026_2350_candidate_timesheet_card_base_expense_fallback_v1.sql
\ir 28082026_0214_candidate_manager_refusal_resubmission_v1.sql
\ir 28082026_0505_candidate_refused_card_recovery_v1.sql
\ir 28082026_2002_candidate_daily_detail_projection_v1.sql
\ir 29082026_0012_candidate_daily_active_window_entry_v1.sql
\ir 30082026_0125_candidate_submitted_weekly_card_linkage.sql

-- The legacy transition bundle above is required to rebuild the withdrawal
-- surface, but it must never remain the installed transition authority.  End
-- with the latest complete replacement so a closure replay cannot roll back
-- expense-carrier anchoring, Daily admission or manager-handoff recovery.
\ir 30082026_1903_candidate_expense_carrier_anchor_route_v1.sql
