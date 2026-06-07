ALTER TABLE public.banking_pay_operation_transfer_scope
DROP CONSTRAINT IF EXISTS banking_pay_operation_transfer_scope_status_chk;

ALTER TABLE public.banking_pay_operation_transfer_scope
ADD CONSTRAINT banking_pay_operation_transfer_scope_status_chk
CHECK (
  status = ANY (
    ARRAY[
      'PENDING'::text,
      'ROLLED_UP'::text,
      'PREPARED'::text,
      'SUBMITTED'::text,
      'FAILED'::text,
      'SKIPPED'::text
    ]
  )
);
