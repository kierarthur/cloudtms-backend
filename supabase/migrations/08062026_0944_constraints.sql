BEGIN;

--------------------------------------------------------------------------------
-- DB migration only.
-- No RPC/function amendments included.
--
-- Purpose:
-- 1) Allow the exact proven PAYMENT_EXECUTE operation waiting statuses.
-- 2) Allow the exact proven rail event source values written by current code.
-- 3) Allow the exact proven mapping method written by current rail-update code.
--
-- This intentionally does NOT broaden constraints beyond the proven values.
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- 1. public.banking_pay_operations.status
-- Adds only:
--   WAITING_AUTHORISATION
--   WAITING_PROVIDER
--------------------------------------------------------------------------------

ALTER TABLE public.banking_pay_operations
  DROP CONSTRAINT IF EXISTS banking_pay_operations_status_chk;

ALTER TABLE public.banking_pay_operations
  ADD CONSTRAINT banking_pay_operations_status_chk
  CHECK (
    status = ANY (
      ARRAY[
        'QUEUED',
        'RUNNING',
        'WAITING',
        'WAITING_AUTHORISATION',
        'WAITING_PROVIDER',
        'COMPLETE',
        'FAILED',
        'CANCELLED',
        'REVIEW_REQUIRED'
      ]::text[]
    )
  );

--------------------------------------------------------------------------------
-- 2. public.pay_bank_transfer_events.event_source
-- Adds only:
--   PROVIDER_RESPONSE
--   LOCAL_STATE
--------------------------------------------------------------------------------

ALTER TABLE public.pay_bank_transfer_events
  DROP CONSTRAINT IF EXISTS pay_bank_transfer_events_event_source_chk;

ALTER TABLE public.pay_bank_transfer_events
  ADD CONSTRAINT pay_bank_transfer_events_event_source_chk
  CHECK (
    event_source = ANY (
      ARRAY[
        'PROVIDER_WEBHOOK',
        'PROVIDER_POLL',
        'PROVIDER_RESPONSE',
        'LOCAL_STATE',
        'MANUAL_CONFIRM',
        'MANUAL_EVIDENCE',
        'SYSTEM'
      ]::text[]
    )
  );

--------------------------------------------------------------------------------
-- 3. public.pay_bank_transfer_events.mapping_method
-- Adds only:
--   OPERATION_TRANSFER_SCOPE
--------------------------------------------------------------------------------

ALTER TABLE public.pay_bank_transfer_events
  DROP CONSTRAINT IF EXISTS pay_bank_transfer_events_mapping_method_chk;

ALTER TABLE public.pay_bank_transfer_events
  ADD CONSTRAINT pay_bank_transfer_events_mapping_method_chk
  CHECK (
    mapping_method IS NULL
    OR mapping_method = ANY (
      ARRAY[
        'TRANSFER_ID',
        'PROVIDER_EVENT_ID',
        'PROVIDER_REFERENCE',
        'PROVIDER_TRANSACTION_ID',
        'REQUEST_ID',
        'RAIL_TX_ID',
        'PAYMENT_REFERENCE',
        'OPERATION_TRANSFER_SCOPE',
        'MANUAL_TRANSFER_SELECTION',
        'AMOUNT_ONLY_UNIQUE',
        'UNMATCHED',
        'AMBIGUOUS',
        'LEGACY_NO_ARTIFACT'
      ]::text[]
    )
  );

COMMIT;
