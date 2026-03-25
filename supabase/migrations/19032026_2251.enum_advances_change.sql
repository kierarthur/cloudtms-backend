DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n
      ON n.oid = t.typnamespace
    JOIN pg_enum e
      ON e.enumtypid = t.oid
    WHERE n.nspname = 'public'
      AND t.typname = 'pay_finance_component_classification_enum'
      AND e.enumlabel = 'NET_PAY_FIXED_RECOVERY'
  ) THEN
    ALTER TYPE public.pay_finance_component_classification_enum
      ADD VALUE 'NET_PAY_FIXED_RECOVERY';
  END IF;
END
$$;
