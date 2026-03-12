drop function if exists public.pay_create_draft_batches_split(
  date,
  date,
  uuid,
  jsonb,
  uuid,
  uuid,
  uuid[],
  text,
  public.pay_override_mode_enum
);
