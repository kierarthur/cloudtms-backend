CREATE TABLE IF NOT EXISTS public.pay_snooze_warning_acknowledgements (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  token_hash text NOT NULL,
  actor_user_id uuid NOT NULL REFERENCES public.tms_users(id) ON DELETE CASCADE,
  validation_phase text NOT NULL,
  candidate_id uuid NOT NULL REFERENCES public.candidates(id) ON DELETE CASCADE,
  timesheet_id uuid REFERENCES public.timesheets(timesheet_id) ON DELETE CASCADE,
  segment_id text,
  segment_stable_key text,
  source_ref text,
  snooze_kind text NOT NULL,
  snooze_until_date date,
  warning_code text NOT NULL,
  scope_fingerprint text NOT NULL,
  resolution_fingerprint text,
  date_context_fingerprint text NOT NULL,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  expires_at_utc timestamptz NOT NULL,
  last_used_at_utc timestamptz,
  CONSTRAINT pay_snooze_warning_ack_phase_chk CHECK (validation_phase IN ('PRE_OPEN', 'PRE_SAVE')),
  CONSTRAINT pay_snooze_warning_ack_kind_chk CHECK (snooze_kind IN (
    'DO_NOT_PAY',
    'BLOCKED_TIMESHEET',
    'TIMESHEET_PAYMENT',
    'OVERPAYMENT_RECOVERY',
    'PAYMENT_ADVANCE_REPAYMENT',
    'MANUAL_DEBT_RECOVERY'
  )),
  CONSTRAINT pay_snooze_warning_ack_code_chk CHECK (warning_code IN (
    'RATE_RESOLUTION_REQUIRES_REVIEW_AFTER_UNSNOOZE',
    'SNOOZE_DATE_BEFORE_NEXT_PAY_RUN',
    'SNOOZE_INCLUDES_NEXT_PAY_RUN'
  )),
  CONSTRAINT pay_snooze_warning_ack_token_hash_chk CHECK (token_hash ~ '^[0-9a-f]{32}$'),
  CONSTRAINT pay_snooze_warning_ack_expiry_chk CHECK (expires_at_utc > created_at_utc)
);

ALTER TABLE public.pay_snooze_warning_acknowledgements
  ADD COLUMN IF NOT EXISTS source_ref text;

ALTER TABLE public.pay_snooze_warning_acknowledgements
  DROP CONSTRAINT IF EXISTS pay_snooze_warning_ack_scope_chk;

DELETE FROM public.pay_snooze_warning_acknowledgements AS invalid_ack
WHERE (
    invalid_ack.snooze_kind IN ('OVERPAYMENT_RECOVERY', 'PAYMENT_ADVANCE_REPAYMENT', 'MANUAL_DEBT_RECOVERY')
    AND (
      invalid_ack.source_ref IS NULL
      OR invalid_ack.source_ref !~* '^advance:[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
      OR invalid_ack.timesheet_id IS NOT NULL
      OR invalid_ack.segment_id IS NOT NULL
      OR invalid_ack.segment_stable_key IS NOT NULL
    )
  )
  OR (
    invalid_ack.snooze_kind IN ('DO_NOT_PAY', 'BLOCKED_TIMESHEET', 'TIMESHEET_PAYMENT')
    AND invalid_ack.source_ref IS NOT NULL
  );

ALTER TABLE public.pay_snooze_warning_acknowledgements
  ADD CONSTRAINT pay_snooze_warning_ack_scope_chk CHECK (
    (
      snooze_kind IN ('OVERPAYMENT_RECOVERY', 'PAYMENT_ADVANCE_REPAYMENT', 'MANUAL_DEBT_RECOVERY')
      AND source_ref ~* '^advance:[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
      AND timesheet_id IS NULL
      AND segment_id IS NULL
      AND segment_stable_key IS NULL
    )
    OR
    (
      snooze_kind IN ('DO_NOT_PAY', 'BLOCKED_TIMESHEET', 'TIMESHEET_PAYMENT')
      AND source_ref IS NULL
    )
  ) NOT VALID;

ALTER TABLE public.pay_snooze_warning_acknowledgements
  VALIDATE CONSTRAINT pay_snooze_warning_ack_scope_chk;

CREATE UNIQUE INDEX IF NOT EXISTS ux_pay_snooze_warning_ack_token_hash
  ON public.pay_snooze_warning_acknowledgements (token_hash);

CREATE INDEX IF NOT EXISTS idx_pay_snooze_warning_ack_actor_expiry
  ON public.pay_snooze_warning_acknowledgements (actor_user_id, expires_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_pay_snooze_warning_ack_expiry
  ON public.pay_snooze_warning_acknowledgements (expires_at_utc, id);

DROP INDEX IF EXISTS public.idx_pay_snooze_warning_ack_scope;

CREATE INDEX idx_pay_snooze_warning_ack_scope
  ON public.pay_snooze_warning_acknowledgements (
    actor_user_id,
    candidate_id,
    timesheet_id,
    source_ref,
    snooze_kind,
    validation_phase,
    expires_at_utc DESC
  );

ALTER TABLE public.pay_snooze_warning_acknowledgements OWNER TO postgres;
REVOKE ALL ON TABLE public.pay_snooze_warning_acknowledgements FROM PUBLIC;
REVOKE ALL ON TABLE public.pay_snooze_warning_acknowledgements FROM anon;
REVOKE ALL ON TABLE public.pay_snooze_warning_acknowledgements FROM authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.pay_snooze_warning_acknowledgements TO service_role;

DROP FUNCTION IF EXISTS public.pay_snooze_validate_request_v1(
  uuid,
  uuid,
  uuid,
  text,
  text,
  text,
  date,
  text
);

