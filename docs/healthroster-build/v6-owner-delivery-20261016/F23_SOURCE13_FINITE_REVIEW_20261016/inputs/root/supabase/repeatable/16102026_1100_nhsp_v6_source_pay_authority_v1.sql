-- Repeatable CloudTMS function/view authority: nhsp_v6_source_pay_authority_v1
-- Source-only Health assembly; actual cross-owner dependencies remain mandatory.

\set ON_ERROR_STOP on

begin;

-- Actual Bank relations/keys and all native owners are required; no fallback schema.
\ir 16102026_1100_nhsp_v6_source_pay_authority/01_complete_source_authorities.sqlinc
\ir 16102026_1100_nhsp_v6_source_pay_authority/02_exact_health_trigger_effects.sqlinc

notify pgrst,'reload schema';

commit;
