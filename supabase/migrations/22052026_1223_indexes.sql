-- Banking Pay execution timeout/index support
-- Transaction-safe and rerunnable.
-- Do NOT use CREATE INDEX CONCURRENTLY in this migration file.

SET lock_timeout = '5s';
SET statement_timeout = '0';

CREATE INDEX IF NOT EXISTS idx_pay_batch_auth_requests_active_batch_state_created
ON public.pay_batch_auth_requests (pay_batch_id, state, created_at_utc DESC, id DESC)
WHERE state IN ('AWAITING', 'PENDING_AUTHORISATION', 'AUTHORISED');

CREATE INDEX IF NOT EXISTS idx_banking_pay_operation_transfer_scope_operation_batch_status_channel_transfer
ON public.banking_pay_operation_transfer_scope (operation_id, pay_batch_id, status, pay_channel, pay_bank_transfer_id);

CREATE INDEX IF NOT EXISTS idx_pay_bank_transfer_events_transfer_source_state_received
ON public.pay_bank_transfer_events (pay_bank_transfer_id, event_source, normalised_state, received_at_utc DESC)
WHERE pay_bank_transfer_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_banking_pay_operation_chunks_operation_phase_type_status_lock
ON public.banking_pay_operation_chunks (operation_id, phase, chunk_type, status, lock_expires_at_utc);

CREATE INDEX IF NOT EXISTS idx_pay_bank_transfers_batch_request_id
ON public.pay_bank_transfers (pay_batch_id, request_id);

CREATE INDEX IF NOT EXISTS idx_pay_bank_transfers_batch_status_id
ON public.pay_bank_transfers (pay_batch_id, status, id);

RESET lock_timeout;
RESET statement_timeout;
