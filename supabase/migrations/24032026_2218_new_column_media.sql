ALTER TABLE public.comms_outbox
  ADD COLUMN IF NOT EXISTS attempt_lease_token text,
  ADD COLUMN IF NOT EXISTS attempt_leased_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS attempt_lease_expires_at_utc timestamptz;
