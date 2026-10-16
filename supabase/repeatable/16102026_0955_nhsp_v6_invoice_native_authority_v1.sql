-- Repeatable CloudTMS function/view authority: nhsp_v6_invoice_native_authority_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- Frozen complete Invoice assembly1808fbad:94 functions and165 exact permission/effect statements.
-- Actual Daily authority precedes; final Candidate55 follows. No pending first-use before all owners exist.
-- Exact native VAT no-CASCADE DROP/replacement/ACL stays in this single transaction.
\ir 16102026_0955_nhsp_v6_invoice_native_authority/01_complete_invoice_authorities.sqlinc

notify pgrst, 'reload schema';

commit;
