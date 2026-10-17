\set ON_ERROR_STOP on
begin;
-- Fresh complete Health context owner; historical0858 remains unchanged.
\ir 17102026_0910_nhsp_v6_daily_role_identity/01_complete_context.sqlinc
\ir 17102026_0910_nhsp_v6_daily_role_identity/99_acl.sqlinc
commit;
