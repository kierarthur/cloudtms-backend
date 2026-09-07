-- Final current-authority closure for the Candidate QR/settings/invoice bundle.
--
-- The historical bundle remains a legitimate dependency of the Candidate
-- workflow. When it changes, it also replays three definitions that now have
-- newer, narrower owners. Replay those exact current owners immediately after
-- the bundle so managed UPGRADE and clean NEW installations finish identically.
-- The final include restores the protected service-only QR refusal boundary.
--
-- This file owns no business rule and contains no replacement function body.
-- Its recursive release hash binds the complete order below.

\set ON_ERROR_STOP on

\ir 07082026_2225_candidate_app_qr_settings_invoice_replacements_v1.sql
\ir 22082026_1706_daily_validation_compatibility_authorities_v1.sql
\ir 02092026_1834_candidate_expense_separation_delivery_v1.sql
\ir 03092026_1641_contract_settings_effective_authority_v1.sql
\ir 03092026_1645_invoice_generation_frozen_settings_authority_v1.sql
\ir 04092026_1500_invoice_frozen_settings_evaluation_barrier_v1.sql
\ir 04092026_1603_candidate_expense_email_admission_v1.sql
\ir 04092026_1710_timesheet_qr_refuse_service_acl_v1.sql
\ir 04092026_1901_client_planned_override_refresh_v1.sql
