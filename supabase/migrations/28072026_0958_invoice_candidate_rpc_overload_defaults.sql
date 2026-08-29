-- Remove defaults from the two retained, execution-revoked V1 forensic
-- overloads before their authoritative repeatable definitions are installed.
-- PostgreSQL cannot remove argument defaults with CREATE OR REPLACE FUNCTION,
-- so existing databases require this one-time drop/recreate transition.

drop function if exists public.invoice_batch_generate_candidates(
  boolean,
  integer,
  text[],
  jsonb
);

drop function if exists public.invoice_batch_issue_candidates(
  boolean,
  integer,
  jsonb
);
