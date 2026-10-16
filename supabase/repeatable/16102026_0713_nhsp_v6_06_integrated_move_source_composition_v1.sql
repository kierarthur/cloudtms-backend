-- Repeatable CloudTMS function/view authority: nhsp_v6_06_integrated_move_source_composition_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- Source composition: exact frozen Health physical Contract owners00f25571.
-- Public Accept and all41 helpers are owner-only; no Banking producer is supplied.
-- Native/financial/API/contract/PartB gates remain required before TEST publication.
\ir includes/nhsp_v6_health_review_context.sqlinc
\ir includes/nhsp_v6_health_native_apply.sqlinc
\ir includes/nhsp_v6_health_composition_wrappers.sqlinc
\ir includes/nhsp_v6_health_contract_final_acl.sqlinc


notify pgrst, 'reload schema';

commit;
