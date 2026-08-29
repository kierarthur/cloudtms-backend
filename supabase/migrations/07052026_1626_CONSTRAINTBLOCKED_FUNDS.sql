BEGIN;

ALTER TABLE public.pay_payment_return_notice_groups
DROP CONSTRAINT IF EXISTS pay_payment_return_notice_groups_kind_chk;

ALTER TABLE public.pay_payment_return_notice_groups
ADD CONSTRAINT pay_payment_return_notice_groups_kind_chk
CHECK (
  notice_kind = ANY (
    ARRAY[
      'BANK_FAILURE_DETECTED'::text,
      'BLOCKED_FUNDS'::text,
      'NO_MONEY_UNWIND_REQUIRED'::text,
      'NO_MONEY_UNWIND_APPLIED'::text,
      'SETTLED_RETURN_DETECTED'::text,
      'SETTLED_REVERSAL_REQUIRED'::text,
      'SETTLED_REVERSAL_APPLIED'::text,
      'AUTO_CORRECTION_BLOCKED'::text,
      'MANUAL_CORRECTION_APPLIED'::text
    ]
  )
);

COMMIT;
