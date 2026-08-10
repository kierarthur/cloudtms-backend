-- TEST-first controls for exact Draft self-invalidation prevention.
-- All controls install disabled and are enabled only after the saved/installed
-- function catalogue and executable Draft regressions pass.

ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS banking_pay_draft_expected_effects_v1_enabled boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS banking_pay_draft_self_invalidation_claim_deferral_v1_enabled boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS banking_pay_draft_create_adoption_v1_enabled boolean NOT NULL DEFAULT false;

COMMENT ON COLUMN public.settings_defaults.banking_pay_draft_expected_effects_v1_enabled IS
  'Suppresses only transaction-bound, operation-scoped canonical DRAFT_CREATE effects from Workbench dirty fanout.';
COMMENT ON COLUMN public.settings_defaults.banking_pay_draft_self_invalidation_claim_deferral_v1_enabled IS
  'Defers a proven DRAFT_CREATE self-invalidation source-build claim while the owning Draft operation is still freezing artifacts.';
COMMENT ON COLUMN public.settings_defaults.banking_pay_draft_create_adoption_v1_enabled IS
  'Requires the DRAFT_CREATE post-mutation patch to prove current V3 authority and absence of an unrelated pending owner before completion.';
