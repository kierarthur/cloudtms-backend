-- Repeatable CloudTMS function/view authority: nhsp_v6_source_pending_current_v1
-- Source-only Health assembly; actual cross-owner dependencies remain mandatory.

\set ON_ERROR_STOP on

begin;

-- Full1100Source foundation and genuine Physical/Candidate/Invoice precede.
\ir 16102026_1110_nhsp_v6_source_pending_current/01_complete_pending_current.sqlinc

notify pgrst,'reload schema';

commit;
