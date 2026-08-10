-- Banking Pay correction-owned dirty deferral is disabled on installation.
-- TEST enablement follows observe-only causal-envelope verification.

ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS banking_pay_correction_request_dirty_deferral_v1_enabled
    boolean NOT NULL DEFAULT false;

COMMENT ON COLUMN public.settings_defaults.banking_pay_correction_request_dirty_deferral_v1_enabled IS
  'Defers an exactly correlated correction-owned candidate dirty job until its durable correction operation reaches the financial/Workbench terminal boundary.';
