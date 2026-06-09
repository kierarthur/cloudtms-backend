ALTER TABLE public.timesheet_pay_state
  ADD COLUMN IF NOT EXISTS summary_pay_status_code text NULL,
  ADD COLUMN IF NOT EXISTS summary_pay_icon_code text NULL,
  ADD COLUMN IF NOT EXISTS summary_pay_paid_at_utc timestamptz NULL,
  ADD COLUMN IF NOT EXISTS summary_net_delta_ex_vat numeric(12,2) NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint c
    WHERE c.conrelid = 'public.timesheet_pay_state'::regclass
      AND c.conname = 'timesheet_pay_state_summary_pay_status_code_chk'
  ) THEN
    ALTER TABLE public.timesheet_pay_state
      ADD CONSTRAINT timesheet_pay_state_summary_pay_status_code_chk
      CHECK (
        summary_pay_status_code IS NULL
        OR summary_pay_status_code IN ('PAID','PARTIALLY_PAID','PROCESSING','ADVANCED','UNPAID')
      );
  END IF;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint c
    WHERE c.conrelid = 'public.timesheet_pay_state'::regclass
      AND c.conname = 'timesheet_pay_state_summary_pay_icon_code_chk'
  ) THEN
    ALTER TABLE public.timesheet_pay_state
      ADD CONSTRAINT timesheet_pay_state_summary_pay_icon_code_chk
      CHECK (
        summary_pay_icon_code IS NULL
        OR summary_pay_icon_code IN ('COIN','HALF_COIN','CLOCK','RED_COIN','NONE')
      );
  END IF;
END;
$$;
