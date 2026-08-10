-- Add one durable success-alert kind per cancellation request.  This is
-- orchestration evidence only: no payment or finance authority is changed.

BEGIN;

SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.banking_alert_success_events
  DROP CONSTRAINT IF EXISTS banking_alert_success_events_kind_chk;

ALTER TABLE public.banking_alert_success_events
  ADD CONSTRAINT banking_alert_success_events_kind_chk CHECK (
    UPPER(BTRIM(alert_kind)) IN (
      'BATCH_SCHEDULED_SUCCESS',
      'BATCH_SETTLED_SUCCESS',
      'BATCH_CANCELLATION_SUCCESS'
    )
  );

COMMIT;
