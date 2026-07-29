ALTER TABLE public.banking_pay_workbench_sessions
  ADD COLUMN IF NOT EXISTS scope_discovery_checked_at_utc timestamptz;

COMMENT ON COLUMN public.banking_pay_workbench_sessions.scope_discovery_checked_at_utc IS
  'Durable upper watermark for bounded Banking Pay session-open candidate discovery. Candidates whose pay_candidate change counter advances after this instant are shortlisted by the existing worker queue.';
