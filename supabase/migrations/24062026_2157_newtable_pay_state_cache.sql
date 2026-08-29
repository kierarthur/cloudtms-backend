BEGIN;

CREATE TABLE IF NOT EXISTS public.timesheet_summary_pay_state_cache (
  timesheet_id uuid PRIMARY KEY
    REFERENCES public.timesheets(timesheet_id) ON DELETE CASCADE,
  paid_to_date_ex_vat numeric NOT NULL DEFAULT 0,
  last_paid_at_utc timestamptz NULL,
  reserved_ex_vat numeric NOT NULL DEFAULT 0,
  outstanding_ex_vat numeric NOT NULL DEFAULT 0,
  net_delta_ex_vat numeric NOT NULL DEFAULT 0,
  active_advance boolean NOT NULL DEFAULT false,
  active_processing boolean NOT NULL DEFAULT false,
  summary_state_applies boolean NOT NULL DEFAULT false,
  advance_override_created_at_utc timestamptz NULL,
  advance_authorisation_consumed_at_utc timestamptz NULL,
  summary_pay_status_code text NOT NULL DEFAULT 'UNPAID',
  summary_pay_icon_code text NOT NULL DEFAULT 'NONE',
  summary_badge_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
  refreshed_at_utc timestamptz NOT NULL DEFAULT now(),
  refreshed_by_user_id uuid NULL,
  CONSTRAINT timesheet_summary_pay_state_cache_status_chk CHECK (
    summary_pay_status_code = ANY (
      ARRAY['PAID','PARTIALLY_PAID','PROCESSING','OVERPAID','UNPAID']::text[]
    )
  ),
  CONSTRAINT timesheet_summary_pay_state_cache_icon_chk CHECK (
    summary_pay_icon_code = ANY (
      ARRAY['COIN','HALF_COIN','CLOCK','RED_COIN','NONE']::text[]
    )
  ),
  CONSTRAINT timesheet_summary_pay_state_cache_badges_chk CHECK (
    summary_badge_codes <@ ARRAY[
      '__PAY_BADGE_ADV__',
      '__PAY_BADGE_OVERPAID__',
      '__PAY_BADGE_PROCESSING__'
    ]::text[]
  )
);

-- Idempotent support for an interrupted/partially-applied deployment.
ALTER TABLE public.timesheet_summary_pay_state_cache
  ADD COLUMN IF NOT EXISTS summary_state_applies boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS advance_override_created_at_utc timestamptz NULL,
  ADD COLUMN IF NOT EXISTS advance_authorisation_consumed_at_utc timestamptz NULL;

COMMENT ON TABLE public.timesheet_summary_pay_state_cache IS
  'Mutation-side cache for Timesheets Summary payment display state. It is not a settlement ledger and must not be used as payment authority.';

COMMENT ON COLUMN public.timesheet_summary_pay_state_cache.summary_state_applies IS
  'True only when the display cache should override legacy Timesheets Summary payment fallbacks.';

COMMENT ON COLUMN public.timesheet_summary_pay_state_cache.advance_authorisation_consumed_at_utc IS
  'Display-only sticky marker: an Advance Pay badge was consumed by normal authorisation. It does not mutate the source override or any frozen batch artefact.';

COMMENT ON COLUMN public.timesheet_summary_pay_state_cache.summary_badge_codes IS
  'Internal UI overlay tokens. Issue filtering must ignore these tokens.';

COMMIT;
