DROP FUNCTION IF EXISTS public.pay_manual_credit_adjustment_create(
  uuid,
  numeric,
  uuid,
  text,
  text
);

DROP FUNCTION IF EXISTS public.pay_manual_credit_adjustment_update(
  uuid,
  uuid,
  numeric,
  text,
  text
);
