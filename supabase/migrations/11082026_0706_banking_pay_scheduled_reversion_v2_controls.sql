/*
 * Banking Pay scheduled/executed-not-submitted cancellation V2 controls.
 *
 * Installation is deliberately inert.  TEST enablement is a separate,
 * evidence-gated operation after the saved and installed authorities match.
 */

ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS banking_pay_scheduled_cancellation_reversion_v2_observe_enabled
    boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS banking_pay_scheduled_cancellation_reversion_v2_publish_enabled
    boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS banking_pay_correction_held_dirty_route_absorption_v1_enabled
    boolean NOT NULL DEFAULT false;

ALTER TABLE public.settings_defaults
  DROP CONSTRAINT IF EXISTS settings_bpay_scheduled_reversion_v2_dependency_ck;

ALTER TABLE public.settings_defaults
  ADD CONSTRAINT settings_bpay_scheduled_reversion_v2_dependency_ck CHECK (
    banking_pay_scheduled_cancellation_reversion_v2_publish_enabled IS NOT TRUE
    OR (
      banking_pay_scheduled_cancellation_reversion_v2_observe_enabled IS TRUE
      AND banking_pay_cancellation_reversion_observe_v1_enabled IS TRUE
      AND banking_pay_cancellation_reversion_publish_v1_enabled IS TRUE
      AND banking_pay_workbench_semantic_ready_publication_v3_enabled IS TRUE
      AND banking_pay_workbench_semantic_ready_draft_guard_v2_enabled IS TRUE
    )
  );

COMMENT ON COLUMN public.settings_defaults.banking_pay_scheduled_cancellation_reversion_v2_observe_enabled IS
  'Captures and verifies exact scheduled/executed-not-submitted V2 cancellation-reversion authority without publishing it.';
COMMENT ON COLUMN public.settings_defaults.banking_pay_scheduled_cancellation_reversion_v2_publish_enabled IS
  'Allows exact scheduled/executed-not-submitted V2 certified cancellation reversion after every fail-closed authority fence passes.';
COMMENT ON COLUMN public.settings_defaults.banking_pay_correction_held_dirty_route_absorption_v1_enabled IS
  'Coalesces the exact correction-owned dirty job to the current or single active Workbench authority in the route-election commit.';
