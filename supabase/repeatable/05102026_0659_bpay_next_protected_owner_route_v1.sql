-- Repeatable CloudTMS function/view authority: bpay_next_protected_owner_route_v1
-- Canonical atomic wrapper. Callers include the same pure fragment inside
-- their existing transaction, before the SQL-language Source selectors.

\set ON_ERROR_STOP on

begin;

\ir includes/05102026_0659_bpay_next_protected_owner_route_v1.sqlinc

commit;
