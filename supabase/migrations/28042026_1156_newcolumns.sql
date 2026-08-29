BEGIN;

-- =========================================================
-- CloudTMS Banking Pay execution / CSV settlement metadata
-- Safe, rerunnable schema migration
-- =========================================================


-- =========================================================
-- 1) public.settings_defaults
-- Add organisation-level CSV format settings.
--
-- Existing pay_export_csv_columns_json remains the source for:
--   - selected predefined columns
--   - column order
--
-- New pay_export_csv_format_json is formatting-only:
--   - header overrides
--   - delimiter
--   - line ending
--   - quoting
--   - BOM
--   - sort-code/account-number formatting
--   - amount decimal display
--
-- This must never become payment truth.
-- =========================================================

ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS pay_export_csv_format_json jsonb;

UPDATE public.settings_defaults AS sd
SET pay_export_csv_format_json = '{}'::jsonb
WHERE sd.pay_export_csv_format_json IS NULL;

ALTER TABLE public.settings_defaults
  ALTER COLUMN pay_export_csv_format_json SET DEFAULT '{}'::jsonb;

ALTER TABLE public.settings_defaults
  ALTER COLUMN pay_export_csv_format_json SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint c
    WHERE c.conrelid = 'public.settings_defaults'::regclass
      AND c.conname = 'settings_defaults_pay_export_csv_format_json_shape_chk'
  ) THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_pay_export_csv_format_json_shape_chk
      CHECK (
        jsonb_typeof(pay_export_csv_format_json) = 'object'

        AND (
          NOT (pay_export_csv_format_json ? 'headers')
          OR jsonb_typeof(pay_export_csv_format_json -> 'headers') = 'object'
        )

        AND (
          NOT (pay_export_csv_format_json ? 'include_header')
          OR jsonb_typeof(pay_export_csv_format_json -> 'include_header') = 'boolean'
        )

        AND (
          NOT (pay_export_csv_format_json ? 'bom')
          OR jsonb_typeof(pay_export_csv_format_json -> 'bom') = 'boolean'
        )

        AND (
          NOT (pay_export_csv_format_json ? 'delimiter')
          OR jsonb_typeof(pay_export_csv_format_json -> 'delimiter') = 'string'
        )

        AND (
          NOT (pay_export_csv_format_json ? 'empty_value')
          OR jsonb_typeof(pay_export_csv_format_json -> 'empty_value') = 'string'
        )

        AND (
          NOT (pay_export_csv_format_json ? 'line_ending')
          OR pay_export_csv_format_json ->> 'line_ending' IN ('LF', 'CRLF')
        )

        AND (
          NOT (pay_export_csv_format_json ? 'quote_mode')
          OR pay_export_csv_format_json ->> 'quote_mode' IN ('MINIMAL', 'ALL')
        )

        AND (
          NOT (pay_export_csv_format_json ? 'sort_code_format')
          OR pay_export_csv_format_json ->> 'sort_code_format' IN ('AS_STORED', 'DIGITS', 'HYPHENATED')
        )

        AND (
          NOT (pay_export_csv_format_json ? 'account_number_format')
          OR pay_export_csv_format_json ->> 'account_number_format' IN ('AS_STORED', 'DIGITS')
        )

        AND (
          NOT (pay_export_csv_format_json ? 'amount_decimals')
          OR (
            jsonb_typeof(pay_export_csv_format_json -> 'amount_decimals') = 'number'
            AND (pay_export_csv_format_json ->> 'amount_decimals') ~ '^[0-4]$'
          )
        )
      );
  END IF;
END $$;

COMMENT ON COLUMN public.settings_defaults.pay_export_csv_format_json IS
'Organisation-level Banking CSV output formatting config. Formatting only: selected columns/order remain in pay_export_csv_columns_json; payment truth remains frozen pay batch artifacts/pay_bank_transfers.';


-- =========================================================
-- 2) public.pay_batches
-- Add durable CSV generation evidence, active execution intent,
-- and final settlement confirmation evidence.
--
-- These are lifecycle/audit metadata only.
-- They must not become payment truth.
-- =========================================================

ALTER TABLE public.pay_batches
  ADD COLUMN IF NOT EXISTS bank_csv_export_json jsonb;

ALTER TABLE public.pay_batches
  ADD COLUMN IF NOT EXISTS execution_intent_json jsonb;

ALTER TABLE public.pay_batches
  ADD COLUMN IF NOT EXISTS settlement_confirmation_json jsonb;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint c
    WHERE c.conrelid = 'public.pay_batches'::regclass
      AND c.conname = 'pay_batches_bank_csv_export_json_object_chk'
  ) THEN
    ALTER TABLE public.pay_batches
      ADD CONSTRAINT pay_batches_bank_csv_export_json_object_chk
      CHECK (
        bank_csv_export_json IS NULL
        OR jsonb_typeof(bank_csv_export_json) = 'object'
      );
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint c
    WHERE c.conrelid = 'public.pay_batches'::regclass
      AND c.conname = 'pay_batches_execution_intent_json_shape_chk'
  ) THEN
    ALTER TABLE public.pay_batches
      ADD CONSTRAINT pay_batches_execution_intent_json_shape_chk
      CHECK (
        execution_intent_json IS NULL
        OR (
          jsonb_typeof(execution_intent_json) = 'object'

          AND (
            NOT (execution_intent_json ? 'execution_mode')
            OR execution_intent_json ->> 'execution_mode' IN (
              'STANDARD_BANK',
              'CSV_SETTLEMENT',
              'EXTERNAL_SETTLEMENT'
            )
          )

          AND (
            NOT (execution_intent_json ? 'suppress_remittances')
            OR jsonb_typeof(execution_intent_json -> 'suppress_remittances') = 'boolean'
          )

          AND (
            NOT (execution_intent_json ? 'suppress_remittances_confirmed')
            OR jsonb_typeof(execution_intent_json -> 'suppress_remittances_confirmed') = 'boolean'
          )

          AND (
            NOT (execution_intent_json ? 'csv_uploaded_confirmed')
            OR jsonb_typeof(execution_intent_json -> 'csv_uploaded_confirmed') = 'boolean'
          )
        )
      );
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint c
    WHERE c.conrelid = 'public.pay_batches'::regclass
      AND c.conname = 'pay_batches_settlement_confirmation_json_shape_chk'
  ) THEN
    ALTER TABLE public.pay_batches
      ADD CONSTRAINT pay_batches_settlement_confirmation_json_shape_chk
      CHECK (
        settlement_confirmation_json IS NULL
        OR (
          jsonb_typeof(settlement_confirmation_json) = 'object'

          AND (
            NOT (settlement_confirmation_json ? 'settlement_mode')
            OR settlement_confirmation_json ->> 'settlement_mode' IN (
              'STANDARD_BANK',
              'CSV_SETTLEMENT',
              'EXTERNAL_SETTLEMENT'
            )
          )

          AND (
            NOT (settlement_confirmation_json ? 'suppress_remittances')
            OR jsonb_typeof(settlement_confirmation_json -> 'suppress_remittances') = 'boolean'
          )
        )
      );
  END IF;
END $$;

COMMENT ON COLUMN public.pay_batches.bank_csv_export_json IS
'Latest CloudTMS-generated banking CSV evidence for this pay batch: generated timestamp/user, scope, filename, row count, total amount, transfer hash, selected columns, CSV format summary, provider/env snapshot. Evidence only; not payment truth.';

COMMENT ON COLUMN public.pay_batches.execution_intent_json IS
'Active/pending authorised execution or settlement intent for this pay batch. Stores selected execution_mode, payment date/schedule, pay channel scope, remittance suppression choice, CSV confirmation data, external settlement comment, auth request id, and frozen-by metadata. Intent/audit only; not payment truth.';

COMMENT ON COLUMN public.pay_batches.settlement_confirmation_json IS
'Final settlement confirmation evidence for this pay batch after commit. Stores settlement mode, settled timestamp/user, payment date, CSV bank reference or external comment, CSV export hash, remittance suppression flag, and auth request id. Evidence only; not payment truth.';


-- =========================================================
-- 3) public.pay_batch_auth_requests
-- Add frozen execution intent per authorisation request.
--
-- RPC logic must freeze and compare this so later approvers
-- approve the same mode/remittance choice as the first approver.
-- =========================================================

ALTER TABLE public.pay_batch_auth_requests
  ADD COLUMN IF NOT EXISTS execution_intent_json jsonb;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint c
    WHERE c.conrelid = 'public.pay_batch_auth_requests'::regclass
      AND c.conname = 'pay_batch_auth_requests_execution_intent_json_shape_chk'
  ) THEN
    ALTER TABLE public.pay_batch_auth_requests
      ADD CONSTRAINT pay_batch_auth_requests_execution_intent_json_shape_chk
      CHECK (
        execution_intent_json IS NULL
        OR (
          jsonb_typeof(execution_intent_json) = 'object'

          AND (
            NOT (execution_intent_json ? 'execution_mode')
            OR execution_intent_json ->> 'execution_mode' IN (
              'STANDARD_BANK',
              'CSV_SETTLEMENT',
              'EXTERNAL_SETTLEMENT'
            )
          )

          AND (
            NOT (execution_intent_json ? 'suppress_remittances')
            OR jsonb_typeof(execution_intent_json -> 'suppress_remittances') = 'boolean'
          )

          AND (
            NOT (execution_intent_json ? 'csv_uploaded_confirmed')
            OR jsonb_typeof(execution_intent_json -> 'csv_uploaded_confirmed') = 'boolean'
          )
        )
      );
  END IF;
END $$;

COMMENT ON COLUMN public.pay_batch_auth_requests.execution_intent_json IS
'Frozen execution/settlement intent for this authorisation request. Later approvers must approve this same execution mode and remittance suppression choice. Intent/audit only; not payment truth.';


COMMIT;
