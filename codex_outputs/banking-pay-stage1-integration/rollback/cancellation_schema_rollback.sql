-- Banking Pay cancellation Stage 1 schema rollback.
-- Run only after restoring the 15 exact installed replacement-function
-- baselines in installed_before/01..15. The compatibility definitions/ACLs in
-- installed_before/16..19 may then be restored before or after this transaction.
-- TEST only. Never run automatically.

BEGIN;

SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '120s';

DROP FUNCTION IF EXISTS public.pay_payment_correction_integrity_check_v1(uuid,uuid,integer);
DROP FUNCTION IF EXISTS public.pay_payment_correction_selection_prepare_chunk_v1(uuid,uuid,jsonb,integer,text,uuid);
DROP FUNCTION IF EXISTS public.pay_batch_payment_status_page_v1(uuid,uuid,jsonb,text,text,integer,jsonb);
DROP FUNCTION IF EXISTS public.pay_payment_correction_status_get_v1(uuid,uuid);
DROP FUNCTION IF EXISTS public.pay_payment_correction_expire_due_v1(integer);
DROP FUNCTION IF EXISTS public.pay_payment_correction_reauth_bind_v1(uuid,uuid,text,text,timestamptz,timestamptz);
DROP FUNCTION IF EXISTS private.pay_payment_mutation_guard_v1(uuid,uuid,text);
DROP FUNCTION IF EXISTS private.pay_payment_correction_sha256_v1(jsonb);

DROP INDEX IF EXISTS public.ux_banking_pay_operations_correction_request;
DROP INDEX IF EXISTS public.idx_pay_payment_correction_requests_due_v2;
DROP INDEX IF EXISTS public.ux_pay_payment_correction_requests_active_batch_v2;

DROP TABLE IF EXISTS public.pay_payment_correction_request_candidates;

ALTER TABLE public.pay_payment_correction_requests
  DROP CONSTRAINT IF EXISTS pay_payment_correction_requests_reauth_hash_chk,
  DROP CONSTRAINT IF EXISTS pay_payment_correction_requests_reauth_consumed_chk,
  DROP CONSTRAINT IF EXISTS pay_payment_correction_requests_status_chk,
  DROP COLUMN IF EXISTS reauth_proof_hash,
  DROP COLUMN IF EXISTS reauth_expires_at_utc,
  DROP COLUMN IF EXISTS reauth_consumed_at_utc;

ALTER TABLE public.pay_payment_correction_requests
  ADD CONSTRAINT pay_payment_correction_requests_status_chk
  CHECK (status = ANY (ARRAY[
    'REQUESTED'::text,
    'AWAITING_AUTHORISATION'::text,
    'AUTHORISED'::text,
    'EXPANDED'::text,
    'PROCESSING'::text,
    'APPLIED'::text,
    'APPLIED_WITH_BLOCKERS'::text,
    'BLOCKED'::text,
    'FAILED'::text,
    'REJECTED'::text,
    'CANCELLED'::text
  ]));

ALTER TABLE public.banking_pay_operations
  DROP CONSTRAINT IF EXISTS banking_pay_operations_operation_type_chk;
ALTER TABLE public.banking_pay_operations
  ADD CONSTRAINT banking_pay_operations_operation_type_chk
  CHECK (operation_type = ANY (ARRAY[
    'DRAFT_CREATE'::text,
    'PAYMENT_EXECUTE'::text,
    'PAYMENT_RETRY_BLOCKED_FUNDS'::text,
    'PAYMENT_SETTLEMENT'::text,
    'REMITTANCE_QUEUE'::text,
    'PREVIEW_REFRESH'::text
  ]));

DELETE FROM public.banking_pay_operation_config
WHERE operation_type = 'PAYMENT_CORRECTION';

ALTER TABLE public.banking_pay_operation_config
  DROP CONSTRAINT IF EXISTS banking_pay_operation_config_operation_type_chk;
ALTER TABLE public.banking_pay_operation_config
  ADD CONSTRAINT banking_pay_operation_config_operation_type_chk
  CHECK (operation_type = ANY (ARRAY[
    'ALL'::text,
    'DRAFT_CREATE'::text,
    'PAYMENT_EXECUTE'::text,
    'PAYMENT_RETRY_BLOCKED_FUNDS'::text,
    'PAYMENT_SETTLEMENT'::text,
    'REMITTANCE_QUEUE'::text,
    'PREVIEW_REFRESH'::text
  ]));

ALTER TABLE public.settings_defaults
  DROP CONSTRAINT IF EXISTS settings_defaults_bpay_cancel_max_candidates_chk,
  DROP CONSTRAINT IF EXISTS settings_defaults_bpay_cancel_max_items_chk,
  DROP CONSTRAINT IF EXISTS settings_defaults_bpay_cancel_candidate_items_chk,
  DROP CONSTRAINT IF EXISTS settings_defaults_bpay_cancel_candidate_rows_chk,
  DROP COLUMN IF EXISTS banking_pay_candidate_cancellation_enabled,
  DROP COLUMN IF EXISTS banking_pay_correction_max_candidates,
  DROP COLUMN IF EXISTS banking_pay_correction_max_active_items,
  DROP COLUMN IF EXISTS banking_pay_correction_max_active_items_per_candidate,
  DROP COLUMN IF EXISTS banking_pay_correction_max_source_rows_per_candidate;

COMMIT;
