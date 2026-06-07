ALTER TABLE public.banking_pay_operation_chunks
DROP CONSTRAINT IF EXISTS banking_pay_operation_chunks_chunk_type_chk;

ALTER TABLE public.banking_pay_operation_chunks
ADD CONSTRAINT banking_pay_operation_chunks_chunk_type_chk
CHECK (
  chunk_type = ANY (
    ARRAY[
      'CANDIDATE_SCOPE'::text,
      'TSFIN'::text,
      'PAYEE_READINESS'::text,
      'TRANSFER_GROUP'::text,
      'TRANSFER_SCOPE_ITEM_SEED'::text,
      'TRANSFER_SCOPE_ROLLUP'::text,
      'TRANSFER_SUBMIT'::text,
      'RAIL_UPDATE'::text,
      'SETTLEMENT'::text,
      'REMITTANCE'::text,
      'PAYOUT_NOTICE'::text,
      'PREVIEW_PAGE'::text,
      'FRESHNESS_VALIDATE'::text
    ]
  )
);
