-- Banking Pay targeted lifecycle delta admission and source-build execution profile.
-- TEST rollout defaults are deliberately fail-closed: expanded delta routes are disabled
-- and newly-created builds retain the installed profile-1 behaviour until acceptance gates pass.

ALTER TABLE public.banking_pay_workbench_candidate_delta_projection_runs
  ADD COLUMN IF NOT EXISTS admission_seal_version integer,
  ADD COLUMN IF NOT EXISTS admission_seal_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS admission_seal_digest text,
  ADD COLUMN IF NOT EXISTS admission_sealed_at_utc timestamptz;

ALTER TABLE public.banking_pay_workbench_candidate_delta_projection_runs
  DROP CONSTRAINT IF EXISTS banking_pay_workbench_delta_projection_runs_admission_seal_json_object_chk,
  DROP CONSTRAINT IF EXISTS banking_pay_workbench_delta_projection_runs_admission_seal_shape_chk;

ALTER TABLE public.banking_pay_workbench_candidate_delta_projection_runs
  ADD CONSTRAINT banking_pay_workbench_delta_projection_runs_admission_seal_json_object_chk
    CHECK (jsonb_typeof(admission_seal_json) = 'object'),
  ADD CONSTRAINT banking_pay_workbench_delta_projection_runs_admission_seal_shape_chk
    CHECK (
      (
        admission_seal_version IS NULL
        AND admission_seal_json = '{}'::jsonb
        AND admission_seal_digest IS NULL
        AND admission_sealed_at_utc IS NULL
      )
      OR
      (
        admission_seal_version = 1
        AND admission_seal_json <> '{}'::jsonb
        AND admission_seal_digest ~ '^[0-9a-f]{64}$'
        AND admission_sealed_at_utc IS NOT NULL
      )
    );

COMMENT ON COLUMN public.banking_pay_workbench_candidate_delta_projection_runs.admission_seal_json IS
  'Immutable database-owned targeted-delta admission authority. Job payload fields are hints only.';
COMMENT ON COLUMN public.banking_pay_workbench_candidate_delta_projection_runs.admission_seal_digest IS
  'Lowercase SHA-256 of the canonical jsonb::text admission seal.';

ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_delta_enable_simple_authorise boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_delta_enable_simple_unauthorise boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_delta_enable_exact_import_family boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_source_build_execution_profile_version integer NOT NULL DEFAULT 1;

ALTER TABLE public.settings_defaults
  DROP CONSTRAINT IF EXISTS settings_defaults_banking_pay_workbench_source_build_profile_chk;

ALTER TABLE public.settings_defaults
  ADD CONSTRAINT settings_defaults_banking_pay_workbench_source_build_profile_chk
    CHECK (banking_pay_workbench_source_build_execution_profile_version IN (1, 2));

UPDATE public.settings_defaults
SET banking_pay_workbench_delta_enable_simple_authorise = false,
    banking_pay_workbench_delta_enable_simple_unauthorise = false,
    banking_pay_workbench_delta_enable_exact_import_family = false,
    banking_pay_workbench_source_build_execution_profile_version = 1;

COMMENT ON COLUMN public.settings_defaults.banking_pay_workbench_source_build_execution_profile_version IS
  'Frozen into each economic build on first claim. 1=installed paging/reconciliation; 2=packed paging/set-based reconciliation.';
