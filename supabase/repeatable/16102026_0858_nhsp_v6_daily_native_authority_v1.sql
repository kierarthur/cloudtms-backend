-- Repeatable CloudTMS function/view authority: nhsp_v6_daily_native_authority_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- Complete frozen native Health Daily authorities; actual financial publisher remains a dependency.
-- Source00f25571; exact52 private owners stay owner-only; sole public context remains service-only.
\ir 16102026_0858_nhsp_v6_daily_native_authority/01_context.sqlinc
\ir 16102026_0858_nhsp_v6_daily_native_authority/02_admission.sqlinc
\ir 16102026_0858_nhsp_v6_daily_native_authority/03_requests.sqlinc
\ir 16102026_0858_nhsp_v6_daily_native_authority/04_final_acl.sqlinc

notify pgrst, 'reload schema';

commit;
