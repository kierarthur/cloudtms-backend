BEGIN;

-- =========================================================
-- 1) TABLE AMENDMENTS
-- =========================================================

ALTER TABLE public.mail_outbox
  ADD COLUMN IF NOT EXISTS scheduled_for_utc timestamptz NULL,
  ADD COLUMN IF NOT EXISTS next_attempt_at_utc timestamptz NULL;

ALTER TABLE public.comms_outbox
  ADD COLUMN IF NOT EXISTS scheduled_for_utc timestamptz NULL,
  ADD COLUMN IF NOT EXISTS next_attempt_at_utc timestamptz NULL;

ALTER TABLE public.mailshot_runs
  ADD COLUMN IF NOT EXISTS delivery_timing_json jsonb NOT NULL DEFAULT '{}'::jsonb;

-- =========================================================
-- 2) INDEXES
-- =========================================================

CREATE INDEX IF NOT EXISTS idx_mail_outbox_ready_queue
  ON public.mail_outbox
  USING btree (
    status,
    COALESCE(next_attempt_at_utc, scheduled_for_utc, created_at_utc),
    created_at_utc
  )
  WHERE status = 'QUEUED';

CREATE INDEX IF NOT EXISTS idx_comms_outbox_ready_queue
  ON public.comms_outbox
  USING btree (
    channel,
    status,
    COALESCE(next_attempt_at_utc, scheduled_for_utc, created_at_utc),
    created_at_utc
  )
  WHERE status = 'QUEUED';

-- =========================================================
-- 3) VIEW AMENDMENT
-- IMPORTANT:
-- - column order is preserved exactly as it exists now
-- - new columns are appended at the far right only
-- =========================================================

CREATE OR REPLACE VIEW public.v_outbox_unified AS
SELECT
    'EMAIL'::text AS channel,
    o.id AS outbox_id,
    o.type AS outbox_type,
    (o.status)::text AS status,
    o.provider_status AS delivery_status,
    o.created_at_utc,
    o.sent_at,
    o.delivered_at,
    o.read_at,
    o.failed_at,
    o."to" AS to_address,
    o.cc,
    o.bcc,
    o.reply_to,
    o.importance,
    o.email_type,
    o.subject,
    o.body_text,
    o.body_html,
    o.attachments,
    o.reference,
    o.provider_message_id,
    o.last_error,
    o.created_by,
    o.recipient_kind,
    o.recipient_id,
    o.context_kind,
    o.context_id,
    o.mailshot_run_id,
    o.document_template_id,
    o.scheduled_for_utc,
    o.next_attempt_at_utc
FROM public.mail_outbox o

UNION ALL

SELECT
    c.channel,
    c.id AS outbox_id,
    NULL::text AS outbox_type,
    c.status,
    CASE
        WHEN c.read_at IS NOT NULL THEN 'READ'::text
        WHEN c.delivered_at IS NOT NULL THEN 'DELIVERED'::text
        WHEN c.sent_at IS NOT NULL THEN 'SENT'::text
        WHEN c.failed_at IS NOT NULL THEN 'FAILED'::text
        ELSE NULL::text
    END AS delivery_status,
    c.created_at_utc,
    c.sent_at,
    c.delivered_at,
    c.read_at,
    c.failed_at,
    c.to_address,
    NULL::text AS cc,
    NULL::text AS bcc,
    NULL::text AS reply_to,
    NULL::text AS importance,
    NULL::text AS email_type,
    NULL::text AS subject,
    c.message_text AS body_text,
    NULL::text AS body_html,
    NULL::jsonb AS attachments,
    NULL::text AS reference,
    c.provider_message_id,
    c.last_error,
    c.created_by,
    c.recipient_kind,
    c.recipient_id,
    c.context_kind,
    c.context_id,
    c.mailshot_run_id,
    c.document_template_id,
    c.scheduled_for_utc,
    c.next_attempt_at_utc
FROM public.comms_outbox c;

COMMIT;
