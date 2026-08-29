-- CloudTMS Banking Pay cancellation: Stage 1 schema and disabled operation configuration.
-- Planning authority: Final Sign-Off Implementation Plan + locked PAYE Overview addendum.
-- TEST installation is deliberately not performed by this implementation task.

BEGIN;

SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '120s';

CREATE TABLE IF NOT EXISTS public.pay_payment_correction_request_candidates (
    correction_request_id uuid NOT NULL,
    selection_ordinal bigint NOT NULL,
    pay_batch_candidate_id uuid NOT NULL,
    candidate_scope_hash text NOT NULL,
    active_item_count integer NOT NULL,
    source_row_count integer NOT NULL,
    active_amount numeric(14,2) NOT NULL,
    pay_batch_item_ids uuid[] NOT NULL,
    shared_instruction_scope_hash text NULL,
    eligibility_code_at_plan text NOT NULL,
    created_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT pay_payment_correction_request_candidates_pkey
        PRIMARY KEY (correction_request_id, pay_batch_candidate_id),
    CONSTRAINT pay_payment_correction_request_candidates_ordinal_key
        UNIQUE (correction_request_id, selection_ordinal),
    CONSTRAINT pay_payment_correction_request_candidates_request_fkey
        FOREIGN KEY (correction_request_id)
        REFERENCES public.pay_payment_correction_requests(id)
        ON DELETE RESTRICT,
    CONSTRAINT pay_payment_correction_request_candidates_candidate_fkey
        FOREIGN KEY (pay_batch_candidate_id)
        REFERENCES public.pay_batch_candidates(id)
        ON DELETE RESTRICT,
    CONSTRAINT pay_payment_correction_request_candidates_ordinal_chk
        CHECK (selection_ordinal > 0),
    CONSTRAINT pay_payment_correction_request_candidates_item_count_chk
        CHECK (
            active_item_count BETWEEN 1 AND 128
            AND active_item_count = cardinality(pay_batch_item_ids)
        ),
    CONSTRAINT pay_payment_correction_request_candidates_source_count_chk
        CHECK (source_row_count BETWEEN 1 AND 512),
    CONSTRAINT pay_payment_correction_request_candidates_amount_chk
        CHECK (active_amount >= 0::numeric),
    CONSTRAINT pay_payment_correction_request_candidates_candidate_hash_chk
        CHECK (candidate_scope_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT pay_payment_correction_request_candidates_instruction_hash_chk
        CHECK (
            shared_instruction_scope_hash IS NULL
            OR shared_instruction_scope_hash ~ '^[0-9a-f]{64}$'
        )
);

ALTER TABLE public.pay_payment_correction_request_candidates OWNER TO postgres;
ALTER TABLE public.pay_payment_correction_request_candidates ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.pay_payment_correction_request_candidates FORCE ROW LEVEL SECURITY;

CREATE INDEX IF NOT EXISTS idx_pay_payment_correction_request_candidates_instruction
    ON public.pay_payment_correction_request_candidates (
        correction_request_id,
        shared_instruction_scope_hash,
        selection_ordinal
    )
    WHERE shared_instruction_scope_hash IS NOT NULL;

REVOKE ALL ON TABLE public.pay_payment_correction_request_candidates FROM PUBLIC;
REVOKE ALL ON TABLE public.pay_payment_correction_request_candidates FROM anon;
REVOKE ALL ON TABLE public.pay_payment_correction_request_candidates FROM authenticated;
REVOKE ALL ON TABLE public.pay_payment_correction_request_candidates FROM service_role;

ALTER TABLE public.pay_payment_correction_requests
    ADD COLUMN IF NOT EXISTS reauth_proof_hash text NULL,
    ADD COLUMN IF NOT EXISTS reauth_expires_at_utc timestamptz NULL,
    ADD COLUMN IF NOT EXISTS reauth_consumed_at_utc timestamptz NULL;

ALTER TABLE public.pay_payment_correction_requests
    DROP CONSTRAINT IF EXISTS pay_payment_correction_requests_reauth_hash_chk,
    DROP CONSTRAINT IF EXISTS pay_payment_correction_requests_reauth_consumed_chk;

ALTER TABLE public.pay_payment_correction_requests
    ADD CONSTRAINT pay_payment_correction_requests_reauth_hash_chk
        CHECK (
            reauth_proof_hash IS NULL
            OR reauth_proof_hash ~ '^[0-9a-f]{64}$'
        ),
    ADD CONSTRAINT pay_payment_correction_requests_reauth_consumed_chk
        CHECK (
            reauth_consumed_at_utc IS NULL
            OR reauth_proof_hash IS NOT NULL
        );

ALTER TABLE public.pay_payment_correction_requests
    DROP CONSTRAINT IF EXISTS pay_payment_correction_requests_status_chk;

ALTER TABLE public.pay_payment_correction_requests
    ADD CONSTRAINT pay_payment_correction_requests_status_chk
    CHECK (
        status IN (
            'PLANNING',
            'PLANNED',
            'REQUESTED',
            'AWAITING_AUTHORISATION',
            'AUTHORISED',
            'EXPANDED',
            'PROCESSING',
            'APPLIED',
            'APPLIED_WITH_BLOCKERS',
            'BLOCKED',
            'FAILED',
            'REJECTED',
            'CANCELLED'
        )
    );

CREATE UNIQUE INDEX IF NOT EXISTS ux_pay_payment_correction_requests_active_batch_v2
    ON public.pay_payment_correction_requests(pay_batch_id)
    WHERE status IN (
        'REQUESTED',
        'AWAITING_AUTHORISATION',
        'AUTHORISED',
        'EXPANDED',
        'PROCESSING'
    );

CREATE INDEX IF NOT EXISTS idx_pay_payment_correction_requests_due_v2
    ON public.pay_payment_correction_requests(status, requested_at_utc, id)
    WHERE status IN (
        'PLANNING',
        'PLANNED',
        'REQUESTED',
        'AWAITING_AUTHORISATION'
    );

ALTER TABLE public.banking_pay_operations
    DROP CONSTRAINT IF EXISTS banking_pay_operations_operation_type_chk;

ALTER TABLE public.banking_pay_operations
    ADD CONSTRAINT banking_pay_operations_operation_type_chk
    CHECK (
        operation_type IN (
            'DRAFT_CREATE',
            'PAYMENT_EXECUTE',
            'PAYMENT_RETRY_BLOCKED_FUNDS',
            'PAYMENT_SETTLEMENT',
            'REMITTANCE_QUEUE',
            'PREVIEW_REFRESH',
            'PAYMENT_CORRECTION'
        )
    );

CREATE UNIQUE INDEX IF NOT EXISTS ux_banking_pay_operations_correction_request
    ON public.banking_pay_operations ((input_json ->> 'correction_request_id'))
    WHERE operation_type = 'PAYMENT_CORRECTION'
      AND input_json ? 'correction_request_id';

ALTER TABLE public.banking_pay_operation_config
    DROP CONSTRAINT IF EXISTS banking_pay_operation_config_operation_type_chk;

ALTER TABLE public.banking_pay_operation_config
    ADD CONSTRAINT banking_pay_operation_config_operation_type_chk
    CHECK (
        operation_type IN (
            'ALL',
            'DRAFT_CREATE',
            'PAYMENT_EXECUTE',
            'PAYMENT_RETRY_BLOCKED_FUNDS',
            'PAYMENT_SETTLEMENT',
            'REMITTANCE_QUEUE',
            'PREVIEW_REFRESH',
            'PAYMENT_CORRECTION'
        )
    );

INSERT INTO public.banking_pay_operation_config (
    operation_type,
    phase,
    chunk_type,
    default_chunk_size,
    min_chunk_size,
    max_chunk_size,
    max_advance_ms,
    lock_seconds,
    enabled
)
VALUES
    ('PAYMENT_CORRECTION', 'PREPARE_SELECTION', 'CANDIDATE_SCOPE', 50, 1, 100, 7500, 60, false),
    ('PAYMENT_CORRECTION', 'EXPAND_WORK', 'CANDIDATE_SCOPE', 50, 1, 100, 7500, 60, false),
    ('PAYMENT_CORRECTION', 'PROCESS_CHUNKS', 'CANDIDATE_SCOPE', 10, 1, 25, 7500, 60, false),
    ('PAYMENT_CORRECTION', 'FINALISE', 'CANDIDATE_SCOPE', 100, 1, 100, 7500, 60, false),
    ('PAYMENT_CORRECTION', 'REFRESH_WORKBENCH', 'CANDIDATE_SCOPE', 100, 1, 100, 7500, 60, false)
ON CONFLICT (operation_type, phase, chunk_type)
DO UPDATE SET
    default_chunk_size = EXCLUDED.default_chunk_size,
    min_chunk_size = EXCLUDED.min_chunk_size,
    max_chunk_size = EXCLUDED.max_chunk_size,
    max_advance_ms = EXCLUDED.max_advance_ms,
    lock_seconds = EXCLUDED.lock_seconds,
    enabled = false;

ALTER TABLE public.settings_defaults
    ADD COLUMN IF NOT EXISTS banking_pay_candidate_cancellation_enabled boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS banking_pay_correction_max_candidates integer NOT NULL DEFAULT 10000,
    ADD COLUMN IF NOT EXISTS banking_pay_correction_max_active_items integer NOT NULL DEFAULT 250000,
    ADD COLUMN IF NOT EXISTS banking_pay_correction_max_active_items_per_candidate integer NOT NULL DEFAULT 128,
    ADD COLUMN IF NOT EXISTS banking_pay_correction_max_source_rows_per_candidate integer NOT NULL DEFAULT 512;

ALTER TABLE public.settings_defaults
    DROP CONSTRAINT IF EXISTS settings_defaults_bpay_cancel_max_candidates_chk,
    DROP CONSTRAINT IF EXISTS settings_defaults_bpay_cancel_max_items_chk,
    DROP CONSTRAINT IF EXISTS settings_defaults_bpay_cancel_candidate_items_chk,
    DROP CONSTRAINT IF EXISTS settings_defaults_bpay_cancel_candidate_rows_chk;

ALTER TABLE public.settings_defaults
    ADD CONSTRAINT settings_defaults_bpay_cancel_max_candidates_chk
        CHECK (banking_pay_correction_max_candidates BETWEEN 1 AND 10000),
    ADD CONSTRAINT settings_defaults_bpay_cancel_max_items_chk
        CHECK (banking_pay_correction_max_active_items BETWEEN 1 AND 250000),
    ADD CONSTRAINT settings_defaults_bpay_cancel_candidate_items_chk
        CHECK (banking_pay_correction_max_active_items_per_candidate BETWEEN 1 AND 128),
    ADD CONSTRAINT settings_defaults_bpay_cancel_candidate_rows_chk
        CHECK (banking_pay_correction_max_source_rows_per_candidate BETWEEN 1 AND 512);

COMMIT;
