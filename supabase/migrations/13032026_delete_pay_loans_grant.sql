drop function if exists public.pay_loans_grant(
  uuid,
  numeric,
  numeric,
  integer,
  date,
  uuid,
  text
);

drop function if exists public.pay_loans_grant(
  uuid,
  numeric,
  numeric,
  integer,
  date,
  uuid,
  text,
  public.pay_finance_case_type_enum
);
