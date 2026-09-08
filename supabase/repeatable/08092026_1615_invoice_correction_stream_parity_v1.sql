\set ON_ERROR_STOP on

-- Candidate generation and correction validation must classify ordinary
-- worked, self-bill and expense-only Timesheets with the same stream rules.

begin;

\ir 23072026_2207_invoice_queue_stage1_revision8/23072026_2207_private_invoice_correction_validate_batch.sql

commit;
