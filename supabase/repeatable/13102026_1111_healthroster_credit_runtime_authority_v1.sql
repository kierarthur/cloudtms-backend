-- Final credit runtime successor. Historical roots remain unchanged.
-- The same recursive changed closure ends with its approved ACL state.
\set ON_ERROR_STOP on
begin;
\ir 13102026_1111_healthroster_credit_runtime/13102026_1111_resume.sql
\ir 13102026_1111_healthroster_credit_runtime/13102026_1111_support.sql
\ir 13102026_1111_healthroster_credit_runtime/13102026_1111_start.sql
\ir 13102026_1111_healthroster_credit_runtime/13102026_1111_resolver.sql
\ir 13102026_1111_healthroster_credit_runtime/13102026_1111_generation.sql
\ir 13102026_1111_healthroster_credit_runtime/13102026_1111_unissue.sql
\ir 13102026_1111_healthroster_credit_runtime/13102026_1111_legacy.sql
\ir 13102026_1111_healthroster_credit_runtime/13102026_1111_dispatcher.sql
\ir 13102026_1111_healthroster_credit_runtime/13102026_1111_acl.sql
notify pgrst, 'reload schema';
commit;
