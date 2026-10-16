-- Repeatable CloudTMS function/view authority: daily_public_services_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.
\set ON_ERROR_STOP on
begin;
\ir 16102026_1342_nhsp_v6_daily_public_services/01_public_services.sqlinc
\ir 16102026_1342_nhsp_v6_daily_public_services/99_acl.sqlinc
notify pgrst, 'reload schema';
commit;
