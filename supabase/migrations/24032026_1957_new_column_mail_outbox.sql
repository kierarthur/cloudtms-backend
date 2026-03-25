alter table public.mail_outbox
  add column if not exists attempt_lease_token text null,
  add column if not exists attempt_leased_at_utc timestamptz null,
  add column if not exists attempt_lease_expires_at_utc timestamptz null;
