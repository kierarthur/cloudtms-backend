-- Final credit runtime successor. Historical roots remain unchanged.
-- The same recursive changed closure ends with its approved ACL state.
\set ON_ERROR_STOP on
begin;
\ir 13102026_1111_healthroster_credit_runtime/13102026_1111_resume.sqlinc
\ir 13102026_1111_healthroster_credit_runtime/13102026_1111_support.sqlinc
\ir 13102026_1111_healthroster_credit_runtime/13102026_1111_start.sqlinc
\ir 13102026_1111_healthroster_credit_runtime/13102026_1111_resolver.sqlinc
\ir 13102026_1111_healthroster_credit_runtime/13102026_1111_generation.sqlinc
\ir 13102026_1111_healthroster_credit_runtime/13102026_1111_unissue.sqlinc
\ir 13102026_1111_healthroster_credit_runtime/13102026_1111_legacy.sqlinc
\ir 13102026_1111_healthroster_credit_runtime/13102026_1111_dispatcher.sqlinc
\ir 13102026_1111_healthroster_credit_runtime/13102026_1111_acl.sqlinc
notify pgrst, 'reload schema';
commit;
