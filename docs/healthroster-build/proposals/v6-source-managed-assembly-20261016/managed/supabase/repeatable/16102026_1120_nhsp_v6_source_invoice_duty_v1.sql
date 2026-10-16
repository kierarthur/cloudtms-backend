-- Repeatable CloudTMS function/view authority: nhsp_v6_source_invoice_duty_v1
-- Source-only Health assembly; actual cross-owner dependencies remain mandatory.

\set ON_ERROR_STOP on

begin;

-- Source1110helper precedes this complete one-call Invoice successor.
\ir 16102026_1120_nhsp_v6_source_invoice_duty/01_complete_invoice_duty.sqlinc

notify pgrst,'reload schema';

commit;
