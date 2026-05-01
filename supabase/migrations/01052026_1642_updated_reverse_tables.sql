BEGIN;

-- =========================================================
-- CloudTMS payment correction DB safety additions
-- Purpose:
--   1) Purpose-bind 2FA reauth challenges so PAYMENT_REVERSAL
--      cannot accidentally reuse PAYMENT_SCHEDULE reauth.
--   2) Add structured payment-scope metadata to mail_outbox so
--      correction RPCs can cancel only the selected remittance /
--      payout notice scope.
--
-- Safe to re-run.
-- =========================================================


-- =========================================================
-- 1. tms_login_2fa_challenges.purpose
-- =========================================================

ALTER TABLE public.tms_login_2fa_challenges
  ADD COLUMN IF NOT EXISTS purpose text;

UPDATE public.tms_login_2fa_challenges
SET purpose = 'PAYMENT_SCHEDULE'
WHERE purpose IS NULL
   OR purpose NOT IN (
     'PAYMENT_SCHEDULE',
     'PAYMENT_REVERSAL',
     'PAYE_SAME_WEEK_OVERRIDE'
   );

ALTER TABLE public.tms_login_2fa_challenges
  ALTER COLUMN purpose SET DEFAULT 'PAYMENT_SCHEDULE';

ALTER TABLE public.tms_login_2fa_challenges
  ALTER COLUMN purpose SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.tms_login_2fa_challenges'::regclass
      AND conname = 'tms_login_2fa_challenges_purpose_chk'
  ) THEN
    ALTER TABLE public.tms_login_2fa_challenges
      ADD CONSTRAINT tms_login_2fa_challenges_purpose_chk
      CHECK (
        purpose IN (
          'PAYMENT_SCHEDULE',
          'PAYMENT_REVERSAL',
          'PAYE_SAME_WEEK_OVERRIDE'
        )
      );
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_tms_login_2fa_challenges_user_purpose_expires
ON public.tms_login_2fa_challenges (user_id, purpose, expires_at_utc);


-- =========================================================
-- 2. mail_outbox.payment_scope_json
-- =========================================================

ALTER TABLE public.mail_outbox
  ADD COLUMN IF NOT EXISTS payment_scope_json jsonb;

UPDATE public.mail_outbox
SET payment_scope_json = '{}'::jsonb
WHERE payment_scope_json IS NULL
   OR jsonb_typeof(payment_scope_json) <> 'object';

ALTER TABLE public.mail_outbox
  ALTER COLUMN payment_scope_json SET DEFAULT '{}'::jsonb;

ALTER TABLE public.mail_outbox
  ALTER COLUMN payment_scope_json SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.mail_outbox'::regclass
      AND conname = 'mail_outbox_payment_scope_json_object_chk'
  ) THEN
    ALTER TABLE public.mail_outbox
      ADD CONSTRAINT mail_outbox_payment_scope_json_object_chk
      CHECK (jsonb_typeof(payment_scope_json) = 'object');
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS mail_outbox_payment_scope_json_gin_idx
ON public.mail_outbox
USING gin (payment_scope_json);


-- =========================================================
-- 3. Defensive verification only.
--    These already passed your introspection, so this does not
--    alter them. It fails loudly only if a future DB is missing
--    the required correction contract.
-- =========================================================

DO $$
DECLARE
  v_mapping_constraint_ok boolean := false;
  v_transfer_status_constraint_ok boolean := false;
BEGIN
  SELECT EXISTS (
    SELECT 1
    FROM pg_constraint c
    WHERE c.conrelid = 'public.pay_bank_transfer_events'::regclass
      AND pg_get_constraintdef(c.oid) ILIKE '%mapping_method%'
      AND pg_get_constraintdef(c.oid) ILIKE '%TRANSFER_ID%'
      AND pg_get_constraintdef(c.oid) ILIKE '%PROVIDER_EVENT_ID%'
      AND pg_get_constraintdef(c.oid) ILIKE '%PROVIDER_REFERENCE%'
      AND pg_get_constraintdef(c.oid) ILIKE '%REQUEST_ID%'
      AND pg_get_constraintdef(c.oid) ILIKE '%RAIL_TX_ID%'
      AND pg_get_constraintdef(c.oid) ILIKE '%PAYMENT_REFERENCE%'
      AND pg_get_constraintdef(c.oid) ILIKE '%MANUAL_TRANSFER_SELECTION%'
      AND pg_get_constraintdef(c.oid) ILIKE '%AMOUNT_ONLY_UNIQUE%'
      AND pg_get_constraintdef(c.oid) ILIKE '%UNMATCHED%'
      AND pg_get_constraintdef(c.oid) ILIKE '%AMBIGUOUS%'
      AND pg_get_constraintdef(c.oid) ILIKE '%LEGACY_NO_ARTIFACT%'
  )
  INTO v_mapping_constraint_ok;

  IF NOT v_mapping_constraint_ok THEN
    RAISE EXCEPTION
      'pay_bank_transfer_events.mapping_method constraint is missing required correction mapping values. Update that constraint before continuing.';
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM pg_constraint c
    WHERE c.conrelid = 'public.pay_bank_transfers'::regclass
      AND pg_get_constraintdef(c.oid) ILIKE '%status%'
      AND pg_get_constraintdef(c.oid) ILIKE '%PENDING%'
      AND pg_get_constraintdef(c.oid) ILIKE '%PROCESSING%'
      AND pg_get_constraintdef(c.oid) ILIKE '%UNKNOWN%'
      AND pg_get_constraintdef(c.oid) ILIKE '%COMPLETED%'
      AND pg_get_constraintdef(c.oid) ILIKE '%FAILED%'
      AND pg_get_constraintdef(c.oid) ILIKE '%DECLINED%'
      AND pg_get_constraintdef(c.oid) ILIKE '%REJECTED%'
      AND pg_get_constraintdef(c.oid) ILIKE '%CANCELLED%'
      AND pg_get_constraintdef(c.oid) ILIKE '%VOIDED%'
      AND pg_get_constraintdef(c.oid) ILIKE '%RETURNED%'
      AND pg_get_constraintdef(c.oid) ILIKE '%REVERTED%'
      AND pg_get_constraintdef(c.oid) ILIKE '%BLOCKED%'
      AND pg_get_constraintdef(c.oid) ILIKE '%FAILED_BEFORE_COMMIT%'
      AND pg_get_constraintdef(c.oid) ILIKE '%SUBMISSION_FAILED%'
  )
  INTO v_transfer_status_constraint_ok;

  IF NOT v_transfer_status_constraint_ok THEN
    RAISE EXCEPTION
      'pay_bank_transfers.status constraint is missing required correction statuses. Update that constraint before continuing.';
  END IF;
END $$;


-- =========================================================
-- 4. Final migration verification.
-- =========================================================

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'tms_login_2fa_challenges'
      AND column_name = 'purpose'
      AND is_nullable = 'NO'
      AND column_default IS NOT NULL
  ) THEN
    RAISE EXCEPTION
      'Migration verification failed: tms_login_2fa_challenges.purpose is missing, nullable, or has no default.';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'mail_outbox'
      AND column_name = 'payment_scope_json'
      AND is_nullable = 'NO'
      AND column_default IS NOT NULL
  ) THEN
    RAISE EXCEPTION
      'Migration verification failed: mail_outbox.payment_scope_json is missing, nullable, or has no default.';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'public'
      AND tablename = 'tms_login_2fa_challenges'
      AND indexname = 'idx_tms_login_2fa_challenges_user_purpose_expires'
  ) THEN
    RAISE EXCEPTION
      'Migration verification failed: idx_tms_login_2fa_challenges_user_purpose_expires missing.';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'public'
      AND tablename = 'mail_outbox'
      AND indexname = 'mail_outbox_payment_scope_json_gin_idx'
  ) THEN
    RAISE EXCEPTION
      'Migration verification failed: mail_outbox_payment_scope_json_gin_idx missing.';
  END IF;
END $$;

COMMIT;
