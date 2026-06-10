-- CloudTMS Banking Pay completion notice durability marker
-- Notification-state only: these columns are not payment, settlement,
-- remittance, VAT, reservation, or economic truth.

alter table public.pay_batches
  add column if not exists completion_notice_queued_at_utc timestamptz,
  add column if not exists completion_notice_reference text,
  add column if not exists completion_notice_last_attempt_at_utc timestamptz,
  add column if not exists completion_notice_next_attempt_at_utc timestamptz,
  add column if not exists completion_notice_last_result_json jsonb;

create index if not exists idx_pay_batches_completion_notice_due
on public.pay_batches (
  completion_notice_queued_at_utc,
  completion_notice_next_attempt_at_utc,
  status,
  rail_provider_snapshot,
  completed_at_utc
);

-- Verified in the supplied schema: deterministic_outbox_key is already protected
-- by a unique index. Keep this defensive idempotent guard for environments
-- missing that index.
create unique index if not exists ux_mail_outbox_deterministic_outbox_key
on public.mail_outbox (deterministic_outbox_key)
where deterministic_outbox_key is not null;
