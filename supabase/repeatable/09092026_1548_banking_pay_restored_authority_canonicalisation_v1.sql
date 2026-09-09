-- Banking Pay restored-authority canonicalisation closure.
--
-- A provider restore can preserve CRLF bytes inside pg_get_functiondef even
-- when the repository's canonical LF definitions and every semantic contract
-- field are unchanged.  The strict final-authority verifier deliberately does
-- not weaken its byte-identical check.  Reapply only the exact current owners
-- needed by that verifier so a managed UPGRADE converges to the same bytes as
-- a clean NEW release.
--
-- These owners preserve the existing Workbench, Timesheet and Candidate
-- behaviour.  This closure defines no function itself and changes no
-- selection, eligibility, amount, gross/net, tax, VAT, channel, payment,
-- provider, cancellation or settlement policy.

\set ON_ERROR_STOP on

\ir 29082026_0326_banking_pay_release_authority_repair_v1.sql
\ir 27082026_2205_candidate_weekly_manager_finalisation_authority_v1.sql
\ir 29082026_0613_banking_pay_replaced_candidate_owner_repair_v1.sql
\ir 04092026_2355_banking_pay_workbench_selection_owner_reassert_v1.sql
\ir 30082026_1232_candidate_qr_document_revision_order_v1.sql
