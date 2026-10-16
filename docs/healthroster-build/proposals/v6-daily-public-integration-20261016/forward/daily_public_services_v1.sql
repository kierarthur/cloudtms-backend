-- Repeatable CloudTMS function/view authority: daily_public_services_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.
\set ON_ERROR_STOP on
begin;
\ir sql/01_public_services.sqlinc
\ir sql/99_acl.sqlinc
notify pgrst, 'reload schema';
commit;
