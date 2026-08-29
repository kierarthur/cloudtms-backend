/*
 * Banking Pay V2 execution-refresh-owner bridge controls.
 *
 * Installation is deliberately inert. TEST enablement is a separate,
 * evidence-gated operation after saved/installed definition parity passes.
 */

ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS banking_pay_execution_refresh_owner_bridge_v1_observe_enabled
    boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS banking_pay_execution_refresh_owner_bridge_v1_publish_enabled
    boolean NOT NULL DEFAULT false;

ALTER TABLE public.settings_defaults
  DROP CONSTRAINT IF EXISTS settings_bpay_execution_refresh_owner_bridge_v1_dependency_ck;

ALTER TABLE public.settings_defaults
  ADD CONSTRAINT settings_bpay_execution_refresh_owner_bridge_v1_dependency_ck CHECK (
    banking_pay_execution_refresh_owner_bridge_v1_publish_enabled IS NOT TRUE
    OR (
      banking_pay_execution_refresh_owner_bridge_v1_observe_enabled IS TRUE
      AND banking_pay_scheduled_cancellation_reversion_v2_observe_enabled IS TRUE
      AND banking_pay_scheduled_cancellation_reversion_v2_publish_enabled IS TRUE
      AND banking_pay_cancellation_reversion_observe_v1_enabled IS TRUE
      AND banking_pay_cancellation_reversion_publish_v1_enabled IS TRUE
      AND banking_pay_workbench_semantic_ready_publication_v3_enabled IS TRUE
      AND banking_pay_workbench_semantic_ready_draft_guard_v2_enabled IS TRUE
    )
  );

COMMENT ON COLUMN public.settings_defaults.banking_pay_execution_refresh_owner_bridge_v1_observe_enabled IS
  'Observes the exact closed V2 execution-owned refresh build while current V3 publication is wholly absent.';
COMMENT ON COLUMN public.settings_defaults.banking_pay_execution_refresh_owner_bridge_v1_publish_enabled IS
  'Allows the exact V2 execution-refresh-owner proof to satisfy pre-request cancellation authority without weakening economic or money-movement fences.';
