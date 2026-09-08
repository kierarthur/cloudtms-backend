\set ON_ERROR_STOP on

-- Candidate generation and correction validation must classify ordinary
-- worked, self-bill and expense-only Timesheets with the same stream rules.
-- Changing the nested validator also replays the historical Invoice V8
-- runtime bundle. Restore its established current authorities first, then
-- leave the changed correction validator as the final definition.

begin;

\ir 07092026_0611_candidate_invoice_current_authority_closure_v1.sql
\ir 23072026_2207_invoice_queue_stage1_revision8/23072026_2207_private_invoice_correction_validate_batch.sql

commit;
