-- Repeatable CloudTMS function/view authority: nhsp_v6_candidate_native_authority_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- Exact f5668bf4 final Candidate55 after complete native Invoice0955 and Daily0858.
-- The final eight-argument workflow is included once; no superseded owner follows.
\ir 16102026_1003_nhsp_v6_candidate_native_authority/01_final_functions.sqlinc
\ir 16102026_1003_nhsp_v6_candidate_native_authority/02_native_trigger_effects.sqlinc
\ir 16102026_1003_nhsp_v6_candidate_native_authority/03_final_acl.sqlinc

commit;
