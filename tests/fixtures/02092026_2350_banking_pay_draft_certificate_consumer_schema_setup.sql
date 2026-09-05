\set ON_ERROR_STOP on

DO $fixture$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'anon') THEN
    CREATE ROLE anon NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'authenticated') THEN
    CREATE ROLE authenticated NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'service_role') THEN
    CREATE ROLE service_role NOLOGIN;
  END IF;
END;
$fixture$;

CREATE SCHEMA IF NOT EXISTS extensions;
CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA extensions;
CREATE SCHEMA IF NOT EXISTS private;

CREATE TABLE public.banking_pay_operations (
  id uuid PRIMARY KEY,
  operation_type text NOT NULL,
  status text NOT NULL,
  phase text NOT NULL
);

CREATE TABLE public.pay_batches (
  id uuid PRIMARY KEY
);

CREATE TABLE public.banking_pay_workbench_sessions (
  id uuid PRIMARY KEY,
  status text NOT NULL,
  version bigint NOT NULL,
  progress_counter_version bigint NOT NULL,
  progress_state text NOT NULL,
  source_snapshot_run_id uuid NOT NULL,
  session_signature text NOT NULL,
  scope_change_generation_target bigint NOT NULL,
  scope_change_generation_applied bigint NOT NULL,
  scope_change_generation_shadow_checked bigint NOT NULL,
  authority_fence_generation bigint NOT NULL,
  discarded_at_utc timestamptz NULL,
  replacement_session_id uuid NULL
);

CREATE TABLE private.banking_pay_workbench_settled_certificates_v8 (
  certificate_uuid uuid PRIMARY KEY,
  certification_id text NULL UNIQUE,
  overall_digest_sha256 text NULL,
  lifecycle text NOT NULL,
  workbench_session_id uuid NOT NULL REFERENCES public.banking_pay_workbench_sessions(id),
  session_version bigint NOT NULL,
  progress_counter_version bigint NOT NULL,
  source_snapshot_run_id uuid NOT NULL,
  session_signature text NOT NULL,
  scope_change_generation_target bigint NOT NULL,
  scope_change_generation_applied bigint NOT NULL,
  scope_change_generation_shadow_checked bigint NOT NULL,
  authority_fence_generation bigint NOT NULL,
  selected_constituent_count integer NOT NULL,
  selected_partition_count integer NOT NULL
);

CREATE TABLE private.banking_pay_workbench_settled_certificate_channel_manifests_v8 (
  certificate_uuid uuid NOT NULL REFERENCES private.banking_pay_workbench_settled_certificates_v8(certificate_uuid),
  pay_channel_scope text NOT NULL,
  constituent_count integer NOT NULL,
  partition_count integer NOT NULL,
  canonical_amount_ex_vat_total text NOT NULL,
  selected_constituents_digest_sha256 text NOT NULL,
  selected_partitions_digest_sha256 text NOT NULL,
  manifest_digest_sha256 text NOT NULL,
  PRIMARY KEY (certificate_uuid, pay_channel_scope)
);

CREATE TABLE private.banking_pay_workbench_settled_certificate_operation_links_v8 (
  operation_id uuid PRIMARY KEY REFERENCES public.banking_pay_operations(id),
  certificate_uuid uuid NOT NULL REFERENCES private.banking_pay_workbench_settled_certificates_v8(certificate_uuid),
  certification_id text NOT NULL,
  overall_digest_sha256 text NOT NULL,
  pay_channel_scope text NOT NULL,
  idempotency_key text NOT NULL,
  link_state text NOT NULL
);

CREATE TABLE private.banking_pay_workbench_settled_certificate_entries_v8 (
  certificate_uuid uuid NOT NULL REFERENCES private.banking_pay_workbench_settled_certificates_v8(certificate_uuid),
  constituent_ordinal integer NOT NULL,
  resolved_pay_channel text NOT NULL,
  preview_row_id uuid NULL,
  candidate_id uuid NULL,
  canonical_amount_ex_vat text NOT NULL DEFAULT '1.00',
  source_identity_digest_sha256 text NOT NULL DEFAULT repeat('0', 64),
  PRIMARY KEY (certificate_uuid, constituent_ordinal)
);

CREATE TABLE private.banking_pay_workbench_settled_certificate_partitions_v8 (
  certificate_uuid uuid NOT NULL REFERENCES private.banking_pay_workbench_settled_certificates_v8(certificate_uuid),
  partition_ordinal integer NOT NULL,
  candidate_id uuid NOT NULL,
  resolved_pay_channel text NOT NULL,
  constituent_count integer NOT NULL,
  canonical_amount_ex_vat_total text NOT NULL,
  partition_digest_sha256 text NOT NULL,
  PRIMARY KEY (certificate_uuid, partition_ordinal),
  UNIQUE (certificate_uuid, candidate_id, resolved_pay_channel)
);

CREATE TABLE private.banking_pay_workbench_settled_certificate_partition_members_v8 (
  certificate_uuid uuid NOT NULL,
  partition_ordinal integer NOT NULL,
  member_ordinal integer NOT NULL,
  constituent_ordinal integer NOT NULL,
  stable_identity_digest_sha256 text NOT NULL,
  PRIMARY KEY (certificate_uuid, partition_ordinal, member_ordinal),
  FOREIGN KEY (certificate_uuid, partition_ordinal)
    REFERENCES private.banking_pay_workbench_settled_certificate_partitions_v8(certificate_uuid, partition_ordinal)
);

CREATE TABLE public.banking_pay_operation_scope_units (
  id uuid PRIMARY KEY DEFAULT extensions.gen_random_uuid(),
  operation_id uuid NOT NULL REFERENCES public.banking_pay_operations(id),
  pay_batch_id uuid NULL REFERENCES public.pay_batches(id),
  phase text NOT NULL,
  unit_type text NOT NULL,
  unit_key text NOT NULL,
  unit_payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  unit_ordinal bigint NOT NULL,
  status text NOT NULL DEFAULT 'PENDING',
  chunk_id uuid NULL,
  result_hash text NULL,
  error_json jsonb NULL,
  created_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (operation_id, phase, unit_type, unit_key)
);

CREATE TABLE public.banking_pay_operation_candidate_scope (
  id uuid PRIMARY KEY DEFAULT extensions.gen_random_uuid(),
  operation_id uuid NOT NULL REFERENCES public.banking_pay_operations(id),
  workbench_session_id uuid NOT NULL REFERENCES public.banking_pay_workbench_sessions(id),
  source_snapshot_run_id uuid NULL,
  source_session_version bigint NULL,
  candidate_state_id uuid NULL,
  candidate_id uuid NOT NULL,
  pay_channel text NOT NULL,
  pay_batch_id uuid NULL REFERENCES public.pay_batches(id),
  selected_preview_row_ids_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  selected_timesheet_ids_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  selected_finance_case_ids_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  effective_candidate_fragment_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  effective_summary_fragment_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  effective_paye_candidate_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  effective_non_paye_payee_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  effective_payees_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  effective_case_resolution_states_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  effective_canonical_preview_lines_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  selected_canonical_preview_lines_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  baseline_component_rows_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  hidden_recovery_template_lines_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  candidate_totals_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  allocation_basis_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  scope_hash text NOT NULL,
  chunk_sequence integer NULL,
  status text NOT NULL DEFAULT 'PENDING',
  created_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (operation_id, candidate_id, pay_channel)
);

CREATE OR REPLACE FUNCTION public.banking_pay_hot_path_budget_apply(p_route_class text DEFAULT 'DISPLAY')
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO ''
AS $function$
BEGIN
  IF pg_catalog.upper(pg_catalog.btrim(COALESCE(p_route_class, ''))) NOT IN ('DISPLAY', 'WORKBENCH_CHUNK') THEN
    RAISE EXCEPTION 'fixture route class invalid';
  END IF;
END;
$function$;

CREATE OR REPLACE FUNCTION private.pay_payment_correction_sha256_v1(p_value jsonb)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
SECURITY INVOKER
SET search_path TO 'pg_catalog', 'extensions', 'pg_temp'
AS $function$
  SELECT pg_catalog.encode(
    extensions.digest(pg_catalog.convert_to(p_value::text, 'UTF8'), 'sha256'),
    'hex'
  );
$function$;
