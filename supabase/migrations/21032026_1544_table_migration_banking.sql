DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n
      ON n.oid = t.typnamespace
    WHERE n.nspname = 'public'
      AND t.typname = 'pay_finance_taxability_enum'
  ) THEN
    CREATE TYPE public.pay_finance_taxability_enum AS ENUM (
      'TAXABLE',
      'NON_TAXABLE'
    );
  ELSE
    BEGIN
      ALTER TYPE public.pay_finance_taxability_enum ADD VALUE IF NOT EXISTS 'TAXABLE';
    EXCEPTION
      WHEN duplicate_object THEN NULL;
    END;
    BEGIN
      ALTER TYPE public.pay_finance_taxability_enum ADD VALUE IF NOT EXISTS 'NON_TAXABLE';
    EXCEPTION
      WHEN duplicate_object THEN NULL;
    END;
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n
      ON n.oid = t.typnamespace
    WHERE n.nspname = 'public'
      AND t.typname = 'pay_finance_routing_kind_enum'
  ) THEN
    CREATE TYPE public.pay_finance_routing_kind_enum AS ENUM (
      'NORMAL_PAY_ROUTE',
      'UMBRELLA_COMPANY',
      'ONE_OFF_SPECIFIED_BANK_ACCOUNT'
    );
  ELSE
    BEGIN
      ALTER TYPE public.pay_finance_routing_kind_enum ADD VALUE IF NOT EXISTS 'NORMAL_PAY_ROUTE';
    EXCEPTION
      WHEN duplicate_object THEN NULL;
    END;
    BEGIN
      ALTER TYPE public.pay_finance_routing_kind_enum ADD VALUE IF NOT EXISTS 'UMBRELLA_COMPANY';
    EXCEPTION
      WHEN duplicate_object THEN NULL;
    END;
    BEGIN
      ALTER TYPE public.pay_finance_routing_kind_enum ADD VALUE IF NOT EXISTS 'ONE_OFF_SPECIFIED_BANK_ACCOUNT';
    EXCEPTION
      WHEN duplicate_object THEN NULL;
    END;
  END IF;
END
$$;
