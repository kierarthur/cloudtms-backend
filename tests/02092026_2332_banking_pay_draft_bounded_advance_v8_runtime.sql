\set ON_ERROR_STOP on

BEGIN;

ALTER TABLE public.banking_pay_operations
  ADD COLUMN IF NOT EXISTS actor_user_id uuid,
  ADD COLUMN IF NOT EXISTS workbench_session_id uuid,
  ADD COLUMN IF NOT EXISTS input_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS progress_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS lease_owner text,
  ADD COLUMN IF NOT EXISTS locked_by text,
  ADD COLUMN IF NOT EXISTS lease_expires_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS lock_expires_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS updated_at_utc timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
  ADD COLUMN IF NOT EXISTS frozen_selected_row_count integer,
  ADD COLUMN IF NOT EXISTS frozen_source_session_version bigint,
  ADD COLUMN IF NOT EXISTS pay_batch_id uuid,
  ADD COLUMN IF NOT EXISTS root_operation_id uuid,
  ADD COLUMN IF NOT EXISTS idempotency_key text NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS config_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS result_json jsonb,
  ADD COLUMN IF NOT EXISTS error_json jsonb,
  ADD COLUMN IF NOT EXISTS total_units integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS completed_units integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS failed_units integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS current_chunk_index integer,
  ADD COLUMN IF NOT EXISTS chunk_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS runner_state text,
  ADD COLUMN IF NOT EXISTS requires_user_action boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS resume_reason text,
  ADD COLUMN IF NOT EXISTS run_after_utc timestamptz,
  ADD COLUMN IF NOT EXISTS heartbeat_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS last_advanced_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS created_at_utc timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp(),
  ADD COLUMN IF NOT EXISTS started_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS completed_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS failed_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS scope_freeze_status text NOT NULL DEFAULT 'NONE',
  ADD COLUMN IF NOT EXISTS frozen_scope_change_generation bigint,
  ADD COLUMN IF NOT EXISTS scope_frozen_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS source_scope_seed_complete boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS frozen_candidate_scope_count integer,
  ADD COLUMN IF NOT EXISTS frozen_operation_scope_hash text,
  ADD COLUMN IF NOT EXISTS frozen_source_snapshot_run_id uuid,
  ADD COLUMN IF NOT EXISTS post_freeze_scope_status text NOT NULL DEFAULT 'NONE',
  ADD COLUMN IF NOT EXISTS post_freeze_observed_generation bigint,
  ADD COLUMN IF NOT EXISTS post_freeze_relevant_generation bigint,
  ADD COLUMN IF NOT EXISTS post_freeze_scope_checked_at_utc timestamptz;

CREATE TABLE IF NOT EXISTS public.pay_batch_candidates (
  id uuid PRIMARY KEY,
  pay_batch_id uuid NOT NULL REFERENCES public.pay_batches(id)
);
CREATE TABLE IF NOT EXISTS public.pay_batch_items (
  id uuid PRIMARY KEY,
  pay_batch_candidate_id uuid NOT NULL REFERENCES public.pay_batch_candidates(id),
  is_voided boolean NOT NULL DEFAULT false,
  finance_case_id uuid NULL,
  reservation_id uuid NULL
);
ALTER TABLE public.pay_batches
  ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'DRAFT',
  ADD COLUMN IF NOT EXISTS source_scope_change_generation bigint,
  ADD COLUMN IF NOT EXISTS source_workbench_session_id uuid,
  ADD COLUMN IF NOT EXISTS source_session_version bigint,
  ADD COLUMN IF NOT EXISTS source_snapshot_run_id uuid,
  ADD COLUMN IF NOT EXISTS freshness_validation_status text,
  ADD COLUMN IF NOT EXISTS freshness_checked_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS scope_generation_observed_at_shell bigint,
  ADD COLUMN IF NOT EXISTS freshness_result_json jsonb,
  ADD COLUMN IF NOT EXISTS rail_provider_snapshot text NOT NULL DEFAULT 'REVOLUT',
  ADD COLUMN IF NOT EXISTS rail_env_snapshot text NOT NULL DEFAULT 'PROD';

CREATE TABLE IF NOT EXISTS public.app_change_counters (
  entity_key text PRIMARY KEY,
  seq bigint NOT NULL DEFAULT 0,
  scope_change_generation bigint NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp()
);

CREATE TABLE IF NOT EXISTS public.settings_defaults (
  id integer PRIMARY KEY,
  banking_pay_source_publication_identity_enforce_v1_enabled boolean NOT NULL DEFAULT false
);
INSERT INTO public.settings_defaults(id,banking_pay_source_publication_identity_enforce_v1_enabled)
SELECT 1,false
WHERE NOT EXISTS (SELECT 1 FROM public.settings_defaults WHERE id=1);

INSERT INTO public.tms_users(id,email,role,password_hash)
VALUES
  ('10000000-0000-0000-0000-000000000003','h2-bounded-003@example.invalid','admin','test-only'),
  ('10000000-0000-0000-0000-000000000103','h2-bounded-103@example.invalid','admin','test-only'),
  ('10000000-0000-0000-0000-000000000203','h2-bounded-203@example.invalid','admin','test-only')
ON CONFLICT (id) DO NOTHING;

INSERT INTO public.candidates(id,pay_method)
VALUES
  ('10000000-0000-0000-0000-000000000005','PAYE'),
  ('10000000-0000-0000-0000-000000000105','PAYE'),
  ('10000000-0000-0000-0000-000000000106','UMBRELLA'),
  ('10000000-0000-0000-0000-000000000205','PAYE')
ON CONFLICT (id) DO NOTHING;

INSERT INTO public.banking_pay_snapshot_runs(
  id,pay_date,week_ending_cutoff,pay_week_start,
  eligibility_from_date,eligibility_to_date,status,is_active
)
VALUES
  ('10000000-0000-0000-0000-000000000007','2026-09-04','2026-09-06','2026-08-31','2026-08-31','2026-09-06','READY',false),
  ('10000000-0000-0000-0000-000000000107','2026-09-04','2026-09-06','2026-08-31','2026-08-31','2026-09-06','READY',false),
  ('10000000-0000-0000-0000-000000000207','2026-09-04','2026-09-06','2026-08-31','2026-08-31','2026-09-06','READY',false)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS public.banking_pay_operation_chunks (
  id uuid PRIMARY KEY DEFAULT extensions.gen_random_uuid(),
  operation_id uuid NOT NULL REFERENCES public.banking_pay_operations(id),
  chunk_type text NOT NULL,
  status text NOT NULL,
  unit_count integer NOT NULL,
  completed_count integer NOT NULL,
  failed_count integer NOT NULL
);

DO $install_missing_scope_blocker_stub$
BEGIN
  IF pg_catalog.to_regprocedure(
       'public.pay_workbench_scope_blocker_state_v1(uuid,bigint,uuid)') IS NULL THEN
    EXECUTE $ddl$
      CREATE FUNCTION public.pay_workbench_scope_blocker_state_v1(
        p_session_id uuid, p_scope_generation bigint, p_operation_id uuid DEFAULT NULL::uuid
      )
      RETURNS jsonb LANGUAGE sql STABLE SECURITY DEFINER SET search_path TO ''
      AS $body$
        SELECT '{"upstream_active_count":0,"upstream_unresolved_failure_count":0,"failure_sample":[]}'::jsonb
      $body$
    $ddl$;
  END IF;
END;
$install_missing_scope_blocker_stub$;
CREATE TABLE IF NOT EXISTS public.banking_pay_operation_candidate_allocation_rows (
  id uuid PRIMARY KEY,
  operation_id uuid NOT NULL REFERENCES public.banking_pay_operations(id),
  candidate_scope_id uuid NOT NULL REFERENCES public.banking_pay_operation_candidate_scope(id),
  pay_batch_item_id uuid NULL REFERENCES public.pay_batch_items(id)
);

CREATE TEMPORARY TABLE h2_bounded_owner_calls (
  call_sequence bigint GENERATED ALWAYS AS IDENTITY,
  owner_name text NOT NULL
) ON COMMIT DROP;

CREATE TEMPORARY TABLE h2_bounded_integrity_results (
  pay_batch_id uuid PRIMARY KEY,
  result_json jsonb NOT NULL CHECK (jsonb_typeof(result_json) = 'object')
) ON COMMIT DROP;

CREATE OR REPLACE FUNCTION pg_temp.h2_bounded_insert_certificate(
  p_certificate_uuid uuid,
  p_session_id uuid,
  p_actor_id uuid,
  p_snapshot_run_id uuid,
  p_session_signature text,
  p_constituent_count integer,
  p_partition_count integer,
  p_total_ex_vat text,
  p_overall_digest_sha256 text
)
RETURNS void
LANGUAGE plpgsql
SET search_path TO ''
AS $function$
BEGIN
  INSERT INTO private.banking_pay_workbench_settled_certificates_v8(
    certificate_uuid,certificate_contract,certification_id,overall_digest_sha256,lifecycle,
    workbench_session_id,session_version,progress_counter_version,progress_state,
    source_snapshot_run_id,session_signature,pay_date,week_ending_cutoff,filters_digest_sha256,
    scope_change_generation_target,scope_change_generation_applied,
    scope_change_generation_shadow_checked,authority_fence_generation,
    publication_count,publications_digest_sha256,scope_total_count,scope_seeded_count,
    scope_ready_count,scope_pending_count,scope_failed_count,line_units_total,line_units_ready,
    line_units_pending,line_units_failed,selected_row_count,selected_page_order,
    selected_page_size_max,selected_pages_fetched,selected_terminal_sentinel_seen,
    selected_sentinel_overflow,all_selected_rows_loaded,
    server_selected_preview_row_ids_provided,server_selected_ids_equal_materialised_selected_ids,
    ready_action_required_blocked_pairwise_disjoint,active_draft_rows_excluded,
    ineligible_rows_excluded,snoozed_rows_excluded,unloaded_selection_gap_count,
    queued_current_job_count,running_current_job_count,unresolved_current_job_count,
    invalid_current_job_pointer_count,historical_terminal_rows_retained,
    historical_terminal_rows_are_not_current_authority,can_create_draft,
    selected_eligible_ready_row_count,blocking_reason_count,gate_digest_sha256,
    selected_constituent_count,selected_canonical_amount_ex_vat_total,selected_partition_count,
    selected_partitions_ordering,policy_contract_version,
    before_policy_projection_digest_sha256,after_policy_projection_digest_sha256,
    policy_digests_equal,execution_recovery_delta_only,forbidden_policy_delta_count,
    no_payment_policy_change,build_id,build_idempotency_key,created_by_user_id,sealed_at_utc,
    filter_binding_mode,filter_context_digest_sha256
  ) VALUES (
    p_certificate_uuid,'WORKBENCH_SETTLED_CERTIFICATION_V2',
    'WORKBENCH_SETTLED_CERTIFICATION_V2:'||p_overall_digest_sha256,
    p_overall_digest_sha256,'SEALED_CURRENT',p_session_id,1,1,'READY',p_snapshot_run_id,
    p_session_signature,'2026-09-04','2026-09-06',pg_catalog.repeat('1',64),
    1,1,1,1,p_partition_count,pg_catalog.repeat('2',64),p_constituent_count,p_constituent_count,
    p_constituent_count,0,0,p_constituent_count,p_constituent_count,0,0,p_constituent_count,
    'row_ordinal asc, preview_row_id asc',256,1,true,false,true,true,true,true,true,true,true,
    0,0,0,0,0,true,true,true,p_constituent_count,0,pg_catalog.repeat('3',64),
    p_constituent_count,p_total_ex_vat,p_partition_count,
    'minimum constituent ordinal asc, candidate_id asc, resolved_pay_channel asc',
    'WORKBENCH_PAYMENT_POLICY_PARITY_V2',pg_catalog.repeat('4',64),pg_catalog.repeat('4',64),
    true,true,0,true,p_certificate_uuid,'h2-bounded-'||p_certificate_uuid::text,
    p_actor_id,pg_catalog.clock_timestamp(),'EXACT_CERTIFIED_SELECTED_UNIVERSE',
    pg_catalog.repeat('5',64)
  );
END;
$function$;

CREATE OR REPLACE FUNCTION pg_temp.h2_bounded_insert_entry(
  p_certificate_uuid uuid,
  p_constituent_ordinal integer,
  p_candidate_id uuid,
  p_preview_row_id uuid,
  p_pay_channel text,
  p_amount_ex_vat text,
  p_source_identity_digest_sha256 text
)
RETURNS void
LANGUAGE plpgsql
SET search_path TO ''
AS $function$
DECLARE
  v_source_publication_id uuid := pg_catalog.md5(
    p_certificate_uuid::text||':publication:'||p_constituent_ordinal::text)::uuid;
  v_source_build_run_id uuid := pg_catalog.md5(
    p_certificate_uuid::text||':build:'||p_constituent_ordinal::text)::uuid;
BEGIN
  INSERT INTO private.banking_pay_workbench_settled_certificate_publications_v8(
    certificate_uuid,scope_ordinal,candidate_id,candidate_state_id,candidate_state_status,
    source_change_seq,source_build_run_id,source_publication_id,
    certified_publication_session_version,publication_attestation_version,
    publication_attestation_digest_sha256,publication_parity_ok,publication_attested_at_utc
  ) VALUES (
    p_certificate_uuid,p_constituent_ordinal,p_candidate_id,
    pg_catalog.md5(p_certificate_uuid::text||':state:'||p_constituent_ordinal::text)::uuid,
    'READY',1,v_source_build_run_id,v_source_publication_id,1,1,
    pg_catalog.repeat('6',64),true,pg_catalog.clock_timestamp()
  ) ON CONFLICT (certificate_uuid,scope_ordinal) DO NOTHING;

  INSERT INTO private.banking_pay_workbench_settled_certificate_entries_v8(
    certificate_uuid,constituent_ordinal,preview_row_id,materialised_preview_row_id,
    presentation_preview_row_id,row_key,source_kind,source_row_key,source_publication_id,
    source_change_seq,source_build_run_id,source_identity_digest_sha256,
    candidate_publication_ordinal,candidate_id,resolved_pay_channel,resolved_payment_method,
    amount_sign,semantic_kind,economic_key_type,economic_key_value,canonical_amount_ex_vat,
    prior_payment_treatment,prior_paid_amount_ex_vat,prior_payment_evidence_digest_sha256,
    supersession_treatment,supersession_evidence_digest_sha256,recovery_result_kind,
    expected_allocation_basis_kind,expected_allocation_result,expected_item_semantic_kind,
    expected_item_source_identity_digest_sha256,expected_item_amount_ex_vat,
    expected_item_source_digest_sha256,expected_reservation_applicability,
    all_same_key_count,all_same_key_digest_sha256,signed_match_count,signed_match_digest_sha256,
    decisive_signed_count,decisive_signed_digest_sha256,readiness_class,selection_state,
    selected,draftable,is_ready_for_draft,constituent_digest_sha256
  ) VALUES (
    p_certificate_uuid,p_constituent_ordinal,p_preview_row_id,p_preview_row_id,
    p_preview_row_id::text,'fixture:'||p_constituent_ordinal::text,'TIMESHEET',
    'fixture-source:'||p_constituent_ordinal::text,v_source_publication_id,1,v_source_build_run_id,
    p_source_identity_digest_sha256,p_constituent_ordinal,p_candidate_id,p_pay_channel,
    'BANK_TRANSFER',CASE WHEN p_amount_ex_vat::numeric < 0 THEN 'NEGATIVE'
      WHEN p_amount_ex_vat::numeric > 0 THEN 'POSITIVE' ELSE 'ZERO' END,
    'WORKED_TIME','TS_TOTAL',p_preview_row_id::text,p_amount_ex_vat,'UNPAID','0.00',
    pg_catalog.repeat('7',64),'CURRENT',pg_catalog.repeat('8',64),'NOT_APPLICABLE',
    'STANDARD','EXPECTED','WORKED_TIME',p_source_identity_digest_sha256,p_amount_ex_vat,
    pg_catalog.repeat('9',64),'NOT_APPLICABLE',0,pg_catalog.repeat('a',64),0,
    pg_catalog.repeat('b',64),0,pg_catalog.repeat('c',64),'READY','SELECTED',true,true,true,
    pg_catalog.repeat('d',64)
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_batch_shell_ensure_from_operation(
  p_operation_id uuid, p_workbench_session_id uuid, p_actor_user_id uuid,
  p_batch_kind text, p_pay_channel text, p_input_json jsonb DEFAULT '{}'::jsonb
)
RETURNS TABLE(pay_batch_id uuid, created boolean, scope jsonb)
LANGUAGE plpgsql SECURITY DEFINER SET search_path TO ''
AS $function$
DECLARE
  v_batch_id uuid := CASE WHEN p_pay_channel = 'PAYE'
    THEN '20000000-0000-0000-0000-000000000001'::uuid
    ELSE '20000000-0000-0000-0000-000000000002'::uuid END;
BEGIN
  INSERT INTO pg_temp.h2_bounded_owner_calls(owner_name) VALUES ('CREATE_BATCH_SHELLS');
  INSERT INTO public.pay_batches(
    id,pay_date,status,banking_system_snapshot,external_paye_system_snapshot,
    rail_provider_snapshot,rail_env_snapshot
  ) VALUES (
    v_batch_id,(p_input_json->>'pay_date')::date,'DRAFT','REVOLUT_API','SAGE',
    'REVOLUT','PROD'
  ) ON CONFLICT (id) DO NOTHING;
  UPDATE public.banking_pay_operation_candidate_scope
  SET pay_batch_id = v_batch_id, status = 'ALLOCATED'
  WHERE operation_id = p_operation_id AND pay_channel = p_pay_channel;
  RETURN QUERY SELECT v_batch_id, true, pg_catalog.jsonb_build_object(
    'pay_channel', p_pay_channel,
    'candidate_scope_count', 1
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_prepare_draft_allocation_rows_seed(
  p_operation_id uuid, p_candidate_scope_ids jsonb DEFAULT NULL::jsonb
)
RETURNS TABLE(candidate_scopes_processed integer, allocation_rows_inserted integer,
  allocation_rows_reused integer, failures integer)
LANGUAGE plpgsql SECURITY DEFINER SET search_path TO ''
AS $function$
BEGIN
  INSERT INTO pg_temp.h2_bounded_owner_calls(owner_name) VALUES ('SEED_ALLOCATION_ROWS');
  RETURN QUERY SELECT pg_catalog.jsonb_array_length(p_candidate_scope_ids), 1, 0, 0;
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_batch_insert_candidates_from_preview(
  p_pay_batch_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid,
  p_operation_id uuid DEFAULT NULL::uuid, p_candidate_scope_ids jsonb DEFAULT NULL::jsonb)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO '' AS $function$
BEGIN INSERT INTO pg_temp.h2_bounded_owner_calls(owner_name) VALUES ('INSERT_CANDIDATES');
RETURN '{"ok":true,"has_more":false}'::jsonb; END; $function$;
CREATE OR REPLACE FUNCTION public.pay_batch_insert_items_from_preview(
  p_pay_batch_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid,
  p_operation_id uuid DEFAULT NULL::uuid, p_candidate_scope_ids jsonb DEFAULT NULL::jsonb)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO '' AS $function$
DECLARE v_prior_calls integer;
BEGIN
SELECT pg_catalog.count(*)::integer INTO v_prior_calls
FROM pg_temp.h2_bounded_owner_calls WHERE owner_name='INSERT_ITEMS';
INSERT INTO pg_temp.h2_bounded_owner_calls(owner_name) VALUES ('INSERT_ITEMS');
RETURN pg_catalog.jsonb_build_object('ok',true,'has_more',v_prior_calls=0);
END; $function$;
CREATE OR REPLACE FUNCTION public.pay_batch_apply_finance_adjustments(
  p_pay_batch_id uuid, p_pay_channel_scope text,
  p_actor_user_id uuid DEFAULT NULL::uuid, p_vat_rate_pct numeric DEFAULT NULL::numeric,
  p_week_start date DEFAULT NULL::date, p_operation_id uuid DEFAULT NULL::uuid,
  p_candidate_scope_ids jsonb DEFAULT NULL::jsonb)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO '' AS $function$
BEGIN INSERT INTO pg_temp.h2_bounded_owner_calls(owner_name) VALUES ('APPLY_FINANCE_ADJUSTMENTS');
RETURN '{"ok":true,"has_more":false}'::jsonb; END; $function$;
CREATE OR REPLACE FUNCTION public.pay_batch_finalize_reservations_and_markers(
  p_pay_batch_id uuid, p_pay_channel_scope text, p_actor_user_id uuid DEFAULT NULL::uuid,
  p_pay_date date DEFAULT NULL::date, p_week_start date DEFAULT NULL::date,
  p_operation_id uuid DEFAULT NULL::uuid,
  p_candidate_scope_ids jsonb DEFAULT NULL::jsonb)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO '' AS $function$
BEGIN INSERT INTO pg_temp.h2_bounded_owner_calls(owner_name) VALUES ('FINALISE_RESERVATIONS');
RETURN '{"ok":true,"has_more":false}'::jsonb; END; $function$;
CREATE OR REPLACE FUNCTION public.pay_batch_populate_candidate_summaries(
  p_pay_batch_id uuid, p_pay_channel_scope text, p_actor_user_id uuid DEFAULT NULL::uuid,
  p_operation_id uuid DEFAULT NULL::uuid, p_candidate_scope_ids jsonb DEFAULT NULL::jsonb)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO '' AS $function$
BEGIN INSERT INTO pg_temp.h2_bounded_owner_calls(owner_name) VALUES ('POPULATE_CANDIDATE_SUMMARIES');
RETURN '{"ok":true,"has_more":false}'::jsonb; END; $function$;
CREATE OR REPLACE FUNCTION public.pay_batch_create_timesheet_snapshots(
  p_pay_batch_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid,
  p_operation_id uuid DEFAULT NULL::uuid,
  p_candidate_scope_ids jsonb DEFAULT NULL::jsonb)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO '' AS $function$
BEGIN INSERT INTO pg_temp.h2_bounded_owner_calls(owner_name) VALUES ('CREATE_TIMESHEET_SNAPSHOTS');
RETURN '{"ok":true,"has_more":false}'::jsonb; END; $function$;
CREATE OR REPLACE FUNCTION public.pay_batch_build_item_breakdowns(
  p_pay_batch_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid,
  p_operation_id uuid DEFAULT NULL::uuid,
  p_candidate_scope_ids jsonb DEFAULT NULL::jsonb)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO '' AS $function$
BEGIN INSERT INTO pg_temp.h2_bounded_owner_calls(owner_name) VALUES ('BUILD_ITEM_BREAKDOWNS');
RETURN '{"ok":true,"has_more":false}'::jsonb; END; $function$;
CREATE OR REPLACE FUNCTION public.pay_batch_assert_integrity(
  p_pay_batch_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid,
  p_operation_id uuid DEFAULT NULL::uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO '' AS $function$
DECLARE v_result jsonb;
BEGIN
  INSERT INTO pg_temp.h2_bounded_owner_calls(owner_name) VALUES ('ASSERT_INTEGRITY');
  SELECT configured.result_json INTO v_result
  FROM pg_temp.h2_bounded_integrity_results AS configured
  WHERE configured.pay_batch_id = p_pay_batch_id;
  RETURN COALESCE(v_result, '{"ok":true,"pass":true}'::jsonb);
END; $function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_patch_preview_after_batch_mutation(
  p_session_id uuid, p_pay_batch_id uuid, p_operation_type text,
  p_actor_user_id uuid DEFAULT NULL::uuid, p_options_json jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO '' AS $function$
BEGIN
  INSERT INTO pg_temp.h2_bounded_owner_calls(owner_name) VALUES ('POST_CREATE_REFRESH');
  RETURN pg_catalog.jsonb_build_object(
    'ok', true,
    'patch_applied', true,
    'replacement_session_required', false,
    'targeted_refresh_enqueued', true,
    'targeted_refresh_enqueued_count', 1,
    'affected_candidate_count', 1,
    'affected_candidate_ids', '[]'::jsonb,
    'patched_row_ids', '[]'::jsonb,
    'affected_row_ids', '[]'::jsonb,
    'affected_timesheet_ids', '[]'::jsonb,
    'affected_economic_keys', '[]'::jsonb,
    'targeted_refresh_candidate_ids', '[]'::jsonb,
    'complex_refresh_candidate_ids', '[]'::jsonb,
    'pay_batch_id', p_pay_batch_id,
    'operation_type', p_operation_type,
    'source_session_id', p_session_id
  );
END; $function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_draft_constituent_parity_page_v8(
  p_operation_id uuid, p_after_constituent_ordinal integer DEFAULT NULL,
  p_limit integer DEFAULT 256, p_expected_previous_receipt_sha256 text DEFAULT NULL
)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO '' AS $function$
DECLARE
  v_cert uuid;
  v_receipt text := pg_catalog.repeat('9', 64);
BEGIN
  INSERT INTO pg_temp.h2_bounded_owner_calls(owner_name) VALUES ('CONSTITUENT_PARITY');
  SELECT certificate_uuid INTO STRICT v_cert
  FROM private.banking_pay_draft_frozen_certificate_scopes_v8
  WHERE operation_id = p_operation_id;
  INSERT INTO private.banking_pay_draft_constituent_parity_results_v8(
    operation_id,constituent_ordinal,certificate_uuid,candidate_scope_ordinal,
    expected_constituent_digest_sha256,expected_pre_draft_facts_digest_sha256,
    actual_allocation_row_count,actual_allocation_digest_sha256,
    actual_item_row_count,actual_item_digest_sha256,
    actual_reservation_row_count,actual_reservation_digest_sha256,
    actual_materialisation_digest_sha256,expected_canonical_byte_count,
    actual_canonical_byte_count,comparison_status,first_mismatch_code,
    comparison_digest_sha256,compared_at_utc
  ) VALUES (
    p_operation_id,0,v_cert,0,pg_catalog.repeat('1',64),pg_catalog.repeat('2',64),
    1,pg_catalog.repeat('3',64),1,pg_catalog.repeat('4',64),0,pg_catalog.repeat('5',64),
    pg_catalog.repeat('6',64),10,10,'MATCH',NULL,pg_catalog.repeat('7',64),pg_catalog.clock_timestamp()
  ) ON CONFLICT (operation_id,constituent_ordinal) DO NOTHING;
  INSERT INTO private.banking_pay_draft_frozen_stage_receipts_v8(
    operation_id,stage_kind,page_sequence,after_ordinal,requested_limit,
    expected_previous_receipt_sha256,request_preimage_digest_sha256,row_count,
    canonical_byte_count,next_after_ordinal,has_more,terminal_sentinel_present,
    receipt_digest_sha256,stage_status
  ) VALUES (
    p_operation_id,'CONSTITUENT_PARITY',0,NULL,p_limit,NULL,pg_catalog.repeat('8',64),
    1,10,0,false,true,v_receipt,'TERMINAL'
  ) ON CONFLICT (operation_id,stage_kind,page_sequence) DO NOTHING;
  RETURN pg_catalog.jsonb_build_object(
    'ok',true,'row_count',1,'match_count',1,'mismatch_count',0,
    'next_after_constituent_ordinal',0,'has_more',false,
    'page_receipt_digest_sha256',v_receipt
  );
END;
$function$;

\ir ../supabase/repeatable/02092026_2330_banking_pay_draft_bounded_advance_v8.sql
\ir ../supabase/repeatable/09082026_1128_banking_pay_operation_finish_post_draft_authority.sql
\ir ../supabase/repeatable/02092026_2331_banking_pay_draft_terminal_finish_v8.sql
\ir ../supabase/repeatable/03092026_1451_banking_pay_draft_terminal_context_v8.sql

DO $fixture$
DECLARE
  v_operation_id constant uuid := '10000000-0000-0000-0000-000000000001';
  v_session_id constant uuid := '10000000-0000-0000-0000-000000000002';
  v_actor_id constant uuid := '10000000-0000-0000-0000-000000000003';
  v_cert constant uuid := '10000000-0000-0000-0000-000000000004';
  v_candidate constant uuid := '10000000-0000-0000-0000-000000000005';
  v_scope_id constant uuid := '10000000-0000-0000-0000-000000000006';
  v_result jsonb;
  v_finish jsonb;
  v_terminal_input jsonb;
  v_call_count integer;
  v_step integer := 0;
BEGIN
  INSERT INTO public.banking_pay_workbench_sessions(
    id,actor_user_id,pay_date,week_ending_cutoff,status,version,
    progress_counter_version,progress_state,source_snapshot_run_id,
    session_signature,scope_change_generation_target,scope_change_generation_applied,
    scope_change_generation_shadow_checked,authority_fence_generation
  ) VALUES (
    v_session_id,v_actor_id,'2026-09-04','2026-09-06','OPEN',1,1,'READY',
    '10000000-0000-0000-0000-000000000007',
    pg_catalog.repeat('a',64),1,1,1,1
  );
  INSERT INTO public.banking_pay_operations(
    id,operation_type,status,phase,actor_user_id,workbench_session_id,idempotency_key,input_json,
    lease_owner,lease_expires_at_utc,frozen_selected_row_count
  ) VALUES (
    v_operation_id,'DRAFT_CREATE','RUNNING','SEED_ALLOCATION_ROWS',v_actor_id,v_session_id,
    'h2-bounded-main',
    '{"pay_date":"2026-09-04","week_start":"2026-08-31","batch_kind":"STANDARD_PAYRUN","rail_provider_snapshot":"REVOLUT","rail_env_snapshot":"PROD","same_week_paye_override":{"pay_date":"2026-09-04","pay_week_start":"2026-08-31"}}',
    'h2-test-worker',pg_catalog.clock_timestamp()+interval '5 minutes',1
  );
  UPDATE public.banking_pay_operations
  SET frozen_source_session_version=1,
      frozen_source_snapshot_run_id='10000000-0000-0000-0000-000000000007'
  WHERE id=v_operation_id;
  PERFORM pg_temp.h2_bounded_insert_certificate(
    v_cert,v_session_id,v_actor_id,'10000000-0000-0000-0000-000000000007',
    pg_catalog.repeat('a',64),1,1,'10.00',pg_catalog.repeat('b',64));
  PERFORM pg_temp.h2_bounded_insert_entry(
    v_cert,0,v_candidate,'10000000-0000-0000-0000-000000000008',
    'PAYE','10.00',pg_catalog.repeat('c',64));
  INSERT INTO private.banking_pay_workbench_settled_certificate_partitions_v8(
    certificate_uuid,partition_ordinal,candidate_id,resolved_pay_channel,constituent_count,
    canonical_amount_ex_vat_total,partition_digest_sha256
  ) VALUES (v_cert,0,v_candidate,'PAYE',1,'10.00',pg_catalog.repeat('d',64));
  INSERT INTO private.banking_pay_workbench_settled_certificate_partition_members_v8(
    certificate_uuid,stream_ordinal,partition_ordinal,member_ordinal,constituent_ordinal,
    stable_identity_digest_sha256
  ) VALUES (v_cert,0,0,0,0,pg_catalog.repeat('c',64));
  INSERT INTO private.banking_pay_draft_frozen_certificate_scopes_v8(
    operation_id,certificate_uuid,pay_channel_scope,constituent_count,partition_count,
    canonical_amount_ex_vat_total,selected_constituents_digest_sha256,
    selected_partitions_digest_sha256,manifest_digest_sha256,freeze_state,frozen_at_utc
  ) VALUES (
    v_operation_id,v_cert,'ALL',1,1,'10.00',pg_catalog.repeat('e',64),
    pg_catalog.repeat('f',64),pg_catalog.repeat('0',64),'FROZEN',pg_catalog.clock_timestamp()
  );
  INSERT INTO private.banking_pay_draft_frozen_constituent_refs_v8(
    operation_id,certificate_uuid,constituent_ordinal,staged_page_sequence
  ) VALUES (v_operation_id,v_cert,0,0);
  INSERT INTO private.banking_pay_draft_frozen_partition_refs_v8(
    operation_id,certificate_uuid,partition_ordinal,staged_page_sequence
  ) VALUES (v_operation_id,v_cert,0,0);
  INSERT INTO private.banking_pay_draft_frozen_stage_receipts_v8(
    operation_id,stage_kind,page_sequence,after_ordinal,requested_limit,
    expected_previous_receipt_sha256,request_preimage_digest_sha256,row_count,
    canonical_byte_count,next_after_ordinal,has_more,terminal_sentinel_present,
    receipt_digest_sha256,stage_status
  ) VALUES (
    v_operation_id,'CERTIFICATE_PARTITION_REFS',0,NULL,256,NULL,
    pg_catalog.repeat('6',64),1,1,NULL,false,true,pg_catalog.repeat('3',64),'TERMINAL'
  );
  INSERT INTO private.banking_pay_draft_frozen_candidate_scopes_v8(
    operation_id,candidate_scope_ordinal,certificate_uuid,partition_ordinal,candidate_id,
    resolved_pay_channel,constituent_count,canonical_amount_ex_vat_total,scope_digest_sha256,
    scope_state
  ) VALUES (v_operation_id,0,v_cert,0,v_candidate,'PAYE',1,'10.00',pg_catalog.repeat('1',64),'FROZEN');
  INSERT INTO private.banking_pay_draft_frozen_candidate_scope_members_v8(
    operation_id,candidate_scope_ordinal,member_ordinal,certificate_uuid,partition_ordinal,
    constituent_ordinal,stable_identity_digest_sha256
  ) VALUES (v_operation_id,0,0,v_cert,0,0,pg_catalog.repeat('c',64));
  INSERT INTO private.banking_pay_workbench_settled_certificate_operation_links_v8(
    operation_id,certificate_uuid,certification_id,overall_digest_sha256,pay_channel_scope,
    idempotency_key,admission_request_digest_sha256,channel_manifest_digest_sha256,link_state,
    filter_context_digest_sha256,filter_scope_manifest_digest_sha256,
    same_week_paye_override_continue,same_week_paye_override_verified,
    same_week_paye_override_used,same_week_paye_override_pay_date,
    same_week_paye_override_pay_week_start,same_week_paye_override_pay_week_end,
    same_week_paye_override_digest_sha256
  ) VALUES (
    v_operation_id,v_cert,
    'WORKBENCH_SETTLED_CERTIFICATION_V2:'||pg_catalog.repeat('b',64),
    pg_catalog.repeat('b',64),'ALL','h2-terminal-fixture',pg_catalog.repeat('1',64),
    pg_catalog.repeat('2',64),'FROZEN',pg_catalog.repeat('5',64),pg_catalog.repeat('3',64),
    false,false,false,'2026-09-04','2026-08-31','2026-09-06',pg_catalog.repeat('4',64));
  INSERT INTO public.banking_pay_operation_candidate_scope(
    id,operation_id,workbench_session_id,source_snapshot_run_id,source_session_version,
    candidate_id,pay_channel,scope_hash,status
  ) VALUES (v_scope_id,v_operation_id,v_session_id,'10000000-0000-0000-0000-000000000007',1,
    v_candidate,'PAYE',pg_catalog.repeat('2',32),'PENDING');

  LOOP
    v_step := v_step + 1;
    IF v_step > 20 THEN RAISE EXCEPTION 'bounded executor did not converge'; END IF;
    v_result := public.banking_pay_draft_advance_bounded_v8(
      v_operation_id,'h2-test-worker',pg_catalog.repeat('3',64));
    IF COALESCE((v_result->>'business_owner_call_count')::integer,0) NOT BETWEEN 0 AND 1 THEN
      RAISE EXCEPTION 'more than one business owner call in one bounded advance: %',v_result;
    END IF;
    EXIT WHEN v_result->>'work_kind' = 'READY_FOR_TERMINAL_FINISH';
  END LOOP;

  SELECT pg_catalog.count(*)::integer INTO v_call_count FROM pg_temp.h2_bounded_owner_calls;
  IF v_call_count <> 13 THEN
    RAISE EXCEPTION 'expected exactly 13 bounded owner calls including one continuation and one durable post-Draft patch, got %',v_call_count;
  END IF;
  IF (SELECT phase FROM public.banking_pay_operations WHERE id=v_operation_id) <> 'POST_CREATE_REFRESH' THEN
    RAISE EXCEPTION 'bounded executor did not stop at terminal adapter boundary';
  END IF;
  IF (SELECT count(*) FROM private.banking_pay_draft_phase_units_v1
      WHERE operation_id=v_operation_id AND unit_state<>'COMPLETE') <> 0 THEN
    RAISE EXCEPTION 'bounded executor left an incomplete phase unit';
  END IF;
  IF (SELECT count(*) FROM private.banking_pay_draft_owner_receipts_v1
      WHERE operation_id=v_operation_id) <> 12 THEN
    RAISE EXCEPTION 'unexpected durable owner receipt count';
  END IF;

  INSERT INTO public.app_change_counters(entity_key,seq,scope_change_generation)
  VALUES ('pay_candidate_scope_generation',1,1),('pay_candidate:'||v_candidate::text,1,1)
  ON CONFLICT (entity_key) DO UPDATE SET seq=EXCLUDED.seq,scope_change_generation=EXCLUDED.scope_change_generation;
  UPDATE public.banking_pay_operation_candidate_scope
  SET allocation_basis_json=pg_catalog.jsonb_build_object(
        'source_build_run_id','10000000-0000-0000-0000-000000000009',
        'source_publication_id','10000000-0000-0000-0000-000000000010',
        'source_identity_digest',pg_catalog.repeat('4',64),
        'semantic_proof_digest',pg_catalog.repeat('5',64),
        'source_publication_attestation',pg_catalog.jsonb_build_object(
          'attestation_version','CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3',
          'semantic_contract_version','READY_TO_PAY_SEMANTIC_V2',
          'semantic_ready',true,'parity_complete',true)),
      status='DRAFTED'
  WHERE operation_id=v_operation_id;
  UPDATE public.pay_batches
  SET source_scope_change_generation=1,source_workbench_session_id=v_session_id,
      source_session_version=1,source_snapshot_run_id='10000000-0000-0000-0000-000000000007'
  WHERE id='20000000-0000-0000-0000-000000000001';
  UPDATE public.banking_pay_operations
  SET scope_freeze_status='FROZEN',frozen_scope_change_generation=1,
      scope_frozen_at_utc=pg_catalog.clock_timestamp(),source_scope_seed_complete=true,
      frozen_candidate_scope_count=1,frozen_selected_row_count=1,
      frozen_operation_scope_hash=(
        SELECT pg_catalog.md5(COALESCE(pg_catalog.string_agg(
          scope_row.candidate_id::text||':'||scope_row.pay_channel||':'||scope_row.scope_hash,
          '|' ORDER BY scope_row.pay_channel,scope_row.candidate_id),''))
        FROM public.banking_pay_operation_candidate_scope AS scope_row
        WHERE scope_row.operation_id=v_operation_id),
      frozen_source_session_version=1,
      frozen_source_snapshot_run_id='10000000-0000-0000-0000-000000000007'
  WHERE id=v_operation_id;

  IF v_result->'terminal_prepare'
       IS DISTINCT FROM private.banking_pay_draft_terminal_context_build_v8(v_operation_id) THEN
    RAISE EXCEPTION 'bounded executor terminal context drifted from its row-backed owner: returned=%, direct=%',
      v_result->'terminal_prepare',
      private.banking_pay_draft_terminal_context_build_v8(v_operation_id);
  END IF;
  IF v_result#>>'{terminal_prepare,contract}' <> 'BANKING_PAY_DRAFT_TERMINAL_CONTEXT_V8'
     OR v_result#>>'{terminal_prepare,workbench_session_id}' <> v_session_id::text
     OR v_result#>>'{terminal_prepare,created_batch_count}' <> '1'
     OR v_result#>'{terminal_prepare,pay_batch_ids}'
          <> pg_catalog.jsonb_build_array('20000000-0000-0000-0000-000000000001'::uuid)
     OR v_result#>'{terminal_prepare,created_pay_batch_ids}'
          <> pg_catalog.jsonb_build_array('20000000-0000-0000-0000-000000000001'::uuid)
     OR v_result#>'{terminal_prepare,reservation_availability,preview_rows}' <> '[]'::jsonb THEN
    RAISE EXCEPTION 'row-backed terminal context omitted or changed accepted terminal facts: %',
      v_result->'terminal_prepare';
  END IF;

  -- The Worker adds only orchestration flags.  Every payment-bearing terminal
  -- field below comes from the row-backed terminal-context owner.
  v_terminal_input := (v_result->'terminal_prepare') || pg_catalog.jsonb_build_object(
    'operation_type','DRAFT_CREATE',
    'backend_runner_owned',true,
    'frontend_completion_required',false
  );
  BEGIN
    PERFORM * FROM public.banking_pay_draft_operation_finish_v8(
      v_operation_id,'COMPLETE',v_terminal_input||'{"candidate_count":2}'::jsonb,NULL);
    RAISE EXCEPTION 'terminal mismatch negative unexpectedly succeeded';
  EXCEPTION WHEN SQLSTATE '55000' THEN
    IF SQLERRM <> 'DRAFT_CREATE_TERMINAL_RESULT_CONTRACT_MISMATCH' THEN RAISE; END IF;
  END;
  IF (SELECT status FROM public.banking_pay_operations WHERE id=v_operation_id)<>'RUNNING'
     OR EXISTS (SELECT 1 FROM private.banking_pay_draft_operation_terminal_results_v8
                WHERE operation_id=v_operation_id)
     OR EXISTS (SELECT 1 FROM public.banking_pay_operation_candidate_scope
                WHERE operation_id=v_operation_id AND allocation_basis_json ? 'post_draft_authority') THEN
    RAISE EXCEPTION 'terminal mismatch did not fail atomically';
  END IF;

  SELECT pg_catalog.to_jsonb(finish_row) INTO v_finish
  FROM public.banking_pay_draft_operation_finish_v8(
    v_operation_id,'COMPLETE',v_terminal_input,NULL) AS finish_row;
  IF COALESCE((v_finish->>'finished')::boolean,false) IS NOT TRUE
     OR v_finish->>'status'<>'COMPLETE'
     OR (v_finish->'result_json'->>'post_draft_authority_contract_version')<>'POST_DRAFT_LIVE_AUTHORITY_V2'
     OR (SELECT freeze_state FROM private.banking_pay_draft_frozen_certificate_scopes_v8
          WHERE operation_id=v_operation_id)<>'TERMINAL_COMPLETE'
     OR (SELECT link_state FROM private.banking_pay_workbench_settled_certificate_operation_links_v8
          WHERE operation_id=v_operation_id)<>'TERMINAL_COMPLETE'
     OR NOT EXISTS (SELECT 1 FROM private.banking_pay_draft_operation_terminal_results_v8
          WHERE operation_id=v_operation_id AND created_pay_batch_count=1) THEN
    RAISE EXCEPTION 'row-backed terminal adapter did not preserve terminal contract: %',v_finish;
  END IF;
  SELECT pg_catalog.to_jsonb(finish_row) INTO v_finish
  FROM public.banking_pay_draft_operation_finish_v8(
    v_operation_id,'COMPLETE','{}'::jsonb,NULL) AS finish_row;
  IF v_finish->>'not_finished_reason'<>'ALREADY_TERMINAL' THEN
    RAISE EXCEPTION 'terminal replay was not idempotent: %',v_finish;
  END IF;
END;
$fixture$;

-- Compare the row-backed terminal adapter with the currently accepted legacy
-- DRAFT_CREATE finish for equivalent one-Candidate PAYE artifacts. IDs and
-- timestamps differ by construction; every policy-bearing output is equal.
DO $terminal_parity_fixture$
DECLARE
  v_v8_operation constant uuid := '10000000-0000-0000-0000-000000000001';
  v_v1_operation constant uuid := '10000000-0000-0000-0000-000000000011';
  v_session constant uuid := '10000000-0000-0000-0000-000000000002';
  v_candidate constant uuid := '10000000-0000-0000-0000-000000000005';
  v_scope constant uuid := '10000000-0000-0000-0000-000000000012';
  v_batch constant uuid := '20000000-0000-0000-0000-000000000011';
  v_preview constant uuid := '10000000-0000-0000-0000-000000000008';
  v_result jsonb;
  v_v1_authority jsonb;
  v_v8_authority jsonb;
BEGIN
  INSERT INTO public.banking_pay_operations(
    id,operation_type,status,phase,actor_user_id,workbench_session_id,idempotency_key,input_json,
    scope_freeze_status,frozen_scope_change_generation,scope_frozen_at_utc,
    source_scope_seed_complete,frozen_candidate_scope_count,frozen_selected_row_count,
    frozen_operation_scope_hash,frozen_source_session_version,frozen_source_snapshot_run_id
  ) VALUES (
    v_v1_operation,'DRAFT_CREATE','RUNNING','POST_CREATE_REFRESH',
    '10000000-0000-0000-0000-000000000003',v_session,'h2-v1-terminal-parity','{}',
    'FROZEN',1,pg_catalog.clock_timestamp(),true,1,1,
    pg_catalog.md5(v_candidate::text||':PAYE:'||pg_catalog.repeat('2',32)),1,
    '10000000-0000-0000-0000-000000000007'
  );
  INSERT INTO public.pay_batches(
    id,pay_date,status,banking_system_snapshot,external_paye_system_snapshot,
    rail_provider_snapshot,rail_env_snapshot,source_scope_change_generation,source_workbench_session_id,
    source_session_version,source_snapshot_run_id
  ) VALUES (
    v_batch,'2026-09-04','DRAFT','REVOLUT_API','SAGE','REVOLUT','PROD',
    1,v_session,1,'10000000-0000-0000-0000-000000000007');
  INSERT INTO public.banking_pay_operation_candidate_scope(
    id,operation_id,workbench_session_id,source_snapshot_run_id,source_session_version,
    candidate_id,pay_channel,pay_batch_id,selected_preview_row_ids_json,
    allocation_basis_json,scope_hash,status
  ) VALUES (
    v_scope,v_v1_operation,v_session,'10000000-0000-0000-0000-000000000007',1,
    v_candidate,'PAYE',v_batch,pg_catalog.jsonb_build_array(v_preview),
    pg_catalog.jsonb_build_object(
      'source_build_run_id','10000000-0000-0000-0000-000000000009',
      'source_publication_id','10000000-0000-0000-0000-000000000010',
      'source_identity_digest',pg_catalog.repeat('4',64),
      'semantic_proof_digest',pg_catalog.repeat('5',64),
      'source_publication_attestation',pg_catalog.jsonb_build_object(
        'attestation_version','CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3',
        'semantic_contract_version','READY_TO_PAY_SEMANTIC_V2',
        'semantic_ready',true,'parity_complete',true)),
    pg_catalog.repeat('2',32),'DRAFTED'
  );
  INSERT INTO public.banking_pay_operation_chunks(
    operation_id,phase,chunk_type,sequence_no,status,unit_count,completed_count,failed_count
  ) VALUES (v_v1_operation,'POST_CREATE_REFRESH','CANDIDATE_SCOPE',1,'COMPLETE',1,1,0);

  SELECT pg_catalog.to_jsonb(finish_row) INTO v_result
  FROM public.banking_pay_operation_finish(
    v_v1_operation,'COMPLETE',pg_catalog.jsonb_build_object(
      'ok',true,'operation_id',v_v1_operation,'operation_type','DRAFT_CREATE',
      'pay_batch_ids',pg_catalog.jsonb_build_array(v_batch),
      'created_pay_batch_ids',pg_catalog.jsonb_build_array(v_batch),
      'primary_pay_batch_id',v_batch,'pay_batch_id',v_batch,'created_batch_count',1,
      'paye_pay_batch_id',v_batch,'umbrella_pay_batch_id',NULL,
      'created_batches',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'pay_batch_id',v_batch,'pay_channel','PAYE')),
      'backend_runner_owned',true,'frontend_completion_required',false),NULL
  ) AS finish_row;
  IF COALESCE((v_result->>'finished')::boolean,false) IS NOT TRUE THEN
    RAISE EXCEPTION 'accepted V1 terminal owner did not complete: %',v_result;
  END IF;

  SELECT allocation_basis_json->'post_draft_authority' INTO v_v1_authority
  FROM public.banking_pay_operation_candidate_scope WHERE operation_id=v_v1_operation;
  SELECT allocation_basis_json->'post_draft_authority' INTO v_v8_authority
  FROM public.banking_pay_operation_candidate_scope WHERE operation_id=v_v8_operation;
  IF v_v1_authority->>'contract_version' IS DISTINCT FROM v_v8_authority->>'contract_version'
     OR v_v1_authority->>'policy_x_authority' IS DISTINCT FROM v_v8_authority->>'policy_x_authority'
     OR v_v1_authority->>'fast_reversion_eligible' IS DISTINCT FROM v_v8_authority->>'fast_reversion_eligible'
     OR (SELECT result_json->>'post_draft_authority_contract_version' FROM public.banking_pay_operations WHERE id=v_v1_operation)
          IS DISTINCT FROM
        (SELECT result_json->>'post_draft_authority_contract_version' FROM public.banking_pay_operations WHERE id=v_v8_operation)
     OR (SELECT result_json->>'post_draft_authority_count' FROM public.banking_pay_operations WHERE id=v_v1_operation)
          IS DISTINCT FROM
        (SELECT result_json->>'post_draft_authority_count' FROM public.banking_pay_operations WHERE id=v_v8_operation)
     OR (SELECT freshness_validation_status FROM public.pay_batches WHERE id=v_batch)
          IS DISTINCT FROM
        (SELECT freshness_validation_status FROM public.pay_batches WHERE id='20000000-0000-0000-0000-000000000001') THEN
    RAISE EXCEPTION 'V1/V8 terminal policy projection differs: v1 %, v8 %',v_v1_authority,v_v8_authority;
  END IF;
END;
$terminal_parity_fixture$;

-- Preserve the existing policy for a mixed PAYE/Umbrella result: one shell
-- containing only already-reserved payments is cancelled by the current
-- Worker cleanup owner, while the independently valid channel continues.
DO $mixed_integrity_fixture$
DECLARE
  v_operation_id constant uuid := '10000000-0000-0000-0000-000000000101';
  v_session_id constant uuid := '10000000-0000-0000-0000-000000000102';
  v_actor_id constant uuid := '10000000-0000-0000-0000-000000000103';
  v_cert constant uuid := '10000000-0000-0000-0000-000000000104';
  v_paye_candidate constant uuid := '10000000-0000-0000-0000-000000000105';
  v_umbrella_candidate constant uuid := '10000000-0000-0000-0000-000000000106';
  v_paye_batch constant uuid := '20000000-0000-0000-0000-000000000001';
  v_umbrella_batch constant uuid := '20000000-0000-0000-0000-000000000002';
  v_result jsonb;
BEGIN
  TRUNCATE pg_temp.h2_bounded_owner_calls, pg_temp.h2_bounded_integrity_results;
  INSERT INTO public.pay_batches(
    id,pay_date,status,banking_system_snapshot,external_paye_system_snapshot,
    rail_provider_snapshot,rail_env_snapshot
  ) VALUES
    (v_paye_batch,'2026-09-04','DRAFT','REVOLUT_API','SAGE','REVOLUT','PROD'),
    (v_umbrella_batch,'2026-09-04','DRAFT','REVOLUT_API','SAGE','REVOLUT','PROD')
  ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status;
  UPDATE public.pay_batches SET status = 'DRAFT' WHERE id IN (v_paye_batch, v_umbrella_batch);
  INSERT INTO pg_temp.h2_bounded_integrity_results(pay_batch_id,result_json)
  VALUES (v_paye_batch, '{"ok":false,"pass":false,"code":"PAY_DRAFT_ALL_SELECTED_PAYMENTS_ALREADY_RESERVED"}');

  INSERT INTO public.banking_pay_workbench_sessions(
    id,actor_user_id,pay_date,week_ending_cutoff,status,version,
    progress_counter_version,progress_state,source_snapshot_run_id,
    session_signature,scope_change_generation_target,scope_change_generation_applied,
    scope_change_generation_shadow_checked,authority_fence_generation
  ) VALUES (
    v_session_id,v_actor_id,'2026-09-04','2026-09-06','OPEN',1,1,'READY',
    '10000000-0000-0000-0000-000000000107',
    pg_catalog.repeat('e',64),1,1,1,1
  );
  INSERT INTO public.banking_pay_operations(
    id,operation_type,status,phase,actor_user_id,workbench_session_id,idempotency_key,input_json,
    lease_owner,lease_expires_at_utc,frozen_selected_row_count
  ) VALUES (
    v_operation_id,'DRAFT_CREATE','RUNNING','ASSERT_INTEGRITY',v_actor_id,v_session_id,
    'h2-bounded-mixed-integrity',
    '{"pay_date":"2026-09-04","week_start":"2026-08-31","batch_kind":"STANDARD_PAYRUN","rail_provider_snapshot":"REVOLUT","rail_env_snapshot":"PROD","same_week_paye_override":{"pay_date":"2026-09-04","pay_week_start":"2026-08-31"}}',
    'h2-test-worker',pg_catalog.clock_timestamp()+interval '5 minutes',2
  );
  PERFORM pg_temp.h2_bounded_insert_certificate(
    v_cert,v_session_id,v_actor_id,'10000000-0000-0000-0000-000000000107',
    pg_catalog.repeat('e',64),2,2,'30.00',pg_catalog.repeat('8',64));
  INSERT INTO private.banking_pay_workbench_settled_certificate_partitions_v8(
    certificate_uuid,partition_ordinal,candidate_id,resolved_pay_channel,constituent_count,
    canonical_amount_ex_vat_total,partition_digest_sha256
  ) VALUES
    (v_cert,0,v_paye_candidate,'PAYE',1,'10.00',pg_catalog.repeat('c',64)),
    (v_cert,1,v_umbrella_candidate,'UMBRELLA',1,'20.00',pg_catalog.repeat('d',64));
  PERFORM pg_temp.h2_bounded_insert_entry(
    v_cert,0,v_paye_candidate,'10000000-0000-0000-0000-000000000108',
    'PAYE','10.00',pg_catalog.repeat('4',64));
  PERFORM pg_temp.h2_bounded_insert_entry(
    v_cert,1,v_umbrella_candidate,'10000000-0000-0000-0000-000000000109',
    'UMBRELLA','20.00',pg_catalog.repeat('5',64));
  INSERT INTO private.banking_pay_draft_frozen_certificate_scopes_v8(
    operation_id,certificate_uuid,pay_channel_scope,constituent_count,partition_count,
    canonical_amount_ex_vat_total,selected_constituents_digest_sha256,
    selected_partitions_digest_sha256,manifest_digest_sha256,freeze_state,frozen_at_utc
  ) VALUES (
    v_operation_id,v_cert,'ALL',2,2,'30.00',pg_catalog.repeat('e',64),
    pg_catalog.repeat('f',64),pg_catalog.repeat('0',64),'FROZEN',pg_catalog.clock_timestamp()
  );
  INSERT INTO private.banking_pay_draft_frozen_constituent_refs_v8(
    operation_id,certificate_uuid,constituent_ordinal,staged_page_sequence
  ) VALUES (v_operation_id,v_cert,0,0),(v_operation_id,v_cert,1,0);
  INSERT INTO private.banking_pay_draft_frozen_stage_receipts_v8(
    operation_id,stage_kind,page_sequence,after_ordinal,requested_limit,
    expected_previous_receipt_sha256,request_preimage_digest_sha256,row_count,
    canonical_byte_count,next_after_ordinal,has_more,terminal_sentinel_present,
    receipt_digest_sha256,stage_status
  ) VALUES (
    v_operation_id,'CERTIFICATE_PARTITION_REFS',0,NULL,256,NULL,
    pg_catalog.repeat('6',64),2,1,NULL,false,true,pg_catalog.repeat('3',64),'TERMINAL'
  );
  INSERT INTO private.banking_pay_draft_frozen_candidate_scopes_v8(
    operation_id,candidate_scope_ordinal,certificate_uuid,partition_ordinal,candidate_id,
    resolved_pay_channel,constituent_count,canonical_amount_ex_vat_total,scope_digest_sha256,
    pay_batch_id,scope_state
  ) VALUES
    (v_operation_id,0,v_cert,0,v_paye_candidate,'PAYE',1,'10.00',pg_catalog.repeat('1',64),v_paye_batch,'BATCH_LINKED'),
    (v_operation_id,1,v_cert,1,v_umbrella_candidate,'UMBRELLA',1,'20.00',pg_catalog.repeat('2',64),v_umbrella_batch,'BATCH_LINKED');
  INSERT INTO private.banking_pay_draft_operation_created_batches_v8(
    operation_id,batch_ordinal,pay_batch_id,candidate_scope_ordinal
  ) VALUES
    (v_operation_id,0,v_paye_batch,0),
    (v_operation_id,1,v_umbrella_batch,1);

  v_result := public.banking_pay_draft_advance_bounded_v8(v_operation_id,'h2-test-worker',pg_catalog.repeat('3',64));
  IF v_result->>'integrity_state' <> 'SKIPPED_EMPTY_RESERVED' THEN
    RAISE EXCEPTION 'PAYE empty-reserved shell was not classified safely: %',v_result;
  END IF;
  v_result := public.banking_pay_draft_advance_bounded_v8(v_operation_id,'h2-test-worker',pg_catalog.repeat('3',64));
  IF v_result->>'integrity_state' <> 'PASS' THEN
    RAISE EXCEPTION 'independent Umbrella shell did not retain PASS: %',v_result;
  END IF;
  v_result := public.banking_pay_draft_advance_bounded_v8(v_operation_id,'h2-test-worker',pg_catalog.repeat('3',64));
  IF v_result->>'work_kind' <> 'EMPTY_RESERVED_BATCH_CLEANUP_REQUIRED'
     OR v_result->'skipped_empty_pay_batch_ids' <> pg_catalog.jsonb_build_array(v_paye_batch) THEN
    RAISE EXCEPTION 'existing empty-shell cleanup handoff was not preserved: %',v_result;
  END IF;

  -- Simulate the result of the unchanged Worker cleanup owner, not a new V8
  -- cancellation policy. The next bounded call verifies that durable result.
  UPDATE public.pay_batches SET status = 'CANCELLED' WHERE id = v_paye_batch;
  v_result := public.banking_pay_draft_advance_bounded_v8(v_operation_id,'h2-test-worker',pg_catalog.repeat('3',64));
  IF v_result->>'work_kind' <> 'CONSTITUENT_PARITY_PAGE' THEN
    RAISE EXCEPTION 'valid channel did not continue after empty-shell cleanup: %',v_result;
  END IF;
  IF (SELECT integrity_state FROM private.banking_pay_draft_operation_created_batches_v8
      WHERE operation_id=v_operation_id AND pay_batch_id=v_paye_batch) <> 'CANCELLED_EMPTY_RESERVED'
     OR (SELECT integrity_state FROM private.banking_pay_draft_operation_created_batches_v8
      WHERE operation_id=v_operation_id AND pay_batch_id=v_umbrella_batch) <> 'PASS' THEN
    RAISE EXCEPTION 'mixed-channel integrity dispositions drifted';
  END IF;
END;
$mixed_integrity_fixture$;

-- Preserve the existing all-empty outcome: cleanup first, then return the
-- established typed failure. No successful Draft is published.
DO $all_empty_integrity_fixture$
DECLARE
  v_operation_id constant uuid := '10000000-0000-0000-0000-000000000201';
  v_session_id constant uuid := '10000000-0000-0000-0000-000000000202';
  v_actor_id constant uuid := '10000000-0000-0000-0000-000000000203';
  v_cert constant uuid := '10000000-0000-0000-0000-000000000204';
  v_candidate constant uuid := '10000000-0000-0000-0000-000000000205';
  v_batch constant uuid := '20000000-0000-0000-0000-000000000001';
  v_result jsonb;
BEGIN
  TRUNCATE pg_temp.h2_bounded_owner_calls, pg_temp.h2_bounded_integrity_results;
  UPDATE public.pay_batches SET status = 'DRAFT' WHERE id = v_batch;
  INSERT INTO pg_temp.h2_bounded_integrity_results(pay_batch_id,result_json)
  VALUES (v_batch, '{"ok":false,"pass":false,"code":"PAY_DRAFT_ALL_SELECTED_PAYMENTS_ALREADY_RESERVED"}');
  INSERT INTO public.banking_pay_workbench_sessions(
    id,actor_user_id,pay_date,week_ending_cutoff,status,version,
    progress_counter_version,progress_state,source_snapshot_run_id,
    session_signature,scope_change_generation_target,scope_change_generation_applied,
    scope_change_generation_shadow_checked,authority_fence_generation
  ) VALUES (
    v_session_id,v_actor_id,'2026-09-04','2026-09-06','OPEN',1,1,'READY',
    '10000000-0000-0000-0000-000000000207',
    pg_catalog.repeat('f',64),1,1,1,1
  );
  INSERT INTO public.banking_pay_operations(
    id,operation_type,status,phase,actor_user_id,workbench_session_id,idempotency_key,input_json,
    lease_owner,lease_expires_at_utc,frozen_selected_row_count
  ) VALUES (
    v_operation_id,'DRAFT_CREATE','RUNNING','ASSERT_INTEGRITY',v_actor_id,v_session_id,
    'h2-bounded-all-empty',
    '{"pay_date":"2026-09-04","week_start":"2026-08-31","batch_kind":"STANDARD_PAYRUN","rail_provider_snapshot":"REVOLUT","rail_env_snapshot":"PROD","same_week_paye_override":{"pay_date":"2026-09-04","pay_week_start":"2026-08-31"}}',
    'h2-test-worker',pg_catalog.clock_timestamp()+interval '5 minutes',1
  );
  PERFORM pg_temp.h2_bounded_insert_certificate(
    v_cert,v_session_id,v_actor_id,'10000000-0000-0000-0000-000000000207',
    pg_catalog.repeat('f',64),1,1,'10.00',pg_catalog.repeat('9',64));
  INSERT INTO private.banking_pay_workbench_settled_certificate_partitions_v8(
    certificate_uuid,partition_ordinal,candidate_id,resolved_pay_channel,constituent_count,
    canonical_amount_ex_vat_total,partition_digest_sha256
  ) VALUES (v_cert,0,v_candidate,'PAYE',1,'10.00',pg_catalog.repeat('c',64));
  INSERT INTO private.banking_pay_draft_frozen_certificate_scopes_v8(
    operation_id,certificate_uuid,pay_channel_scope,constituent_count,partition_count,
    canonical_amount_ex_vat_total,selected_constituents_digest_sha256,
    selected_partitions_digest_sha256,manifest_digest_sha256,freeze_state,frozen_at_utc
  ) VALUES (
    v_operation_id,v_cert,'PAYE',1,1,'10.00',pg_catalog.repeat('e',64),
    pg_catalog.repeat('f',64),pg_catalog.repeat('0',64),'FROZEN',pg_catalog.clock_timestamp()
  );
  INSERT INTO private.banking_pay_draft_frozen_stage_receipts_v8(
    operation_id,stage_kind,page_sequence,after_ordinal,requested_limit,
    expected_previous_receipt_sha256,request_preimage_digest_sha256,row_count,
    canonical_byte_count,next_after_ordinal,has_more,terminal_sentinel_present,
    receipt_digest_sha256,stage_status
  ) VALUES (
    v_operation_id,'CERTIFICATE_PARTITION_REFS',0,NULL,256,NULL,
    pg_catalog.repeat('6',64),1,1,NULL,false,true,pg_catalog.repeat('3',64),'TERMINAL'
  );
  INSERT INTO private.banking_pay_draft_frozen_candidate_scopes_v8(
    operation_id,candidate_scope_ordinal,certificate_uuid,partition_ordinal,candidate_id,
    resolved_pay_channel,constituent_count,canonical_amount_ex_vat_total,scope_digest_sha256,
    pay_batch_id,scope_state
  ) VALUES (
    v_operation_id,0,v_cert,0,v_candidate,'PAYE',1,'10.00',pg_catalog.repeat('1',64),v_batch,'BATCH_LINKED'
  );
  INSERT INTO private.banking_pay_draft_operation_created_batches_v8(
    operation_id,batch_ordinal,pay_batch_id,candidate_scope_ordinal
  ) VALUES (v_operation_id,0,v_batch,0);

  v_result := public.banking_pay_draft_advance_bounded_v8(v_operation_id,'h2-test-worker',pg_catalog.repeat('3',64));
  IF v_result->>'integrity_state' <> 'SKIPPED_EMPTY_RESERVED' THEN
    RAISE EXCEPTION 'all-empty shell did not retain established classification: %',v_result;
  END IF;
  v_result := public.banking_pay_draft_advance_bounded_v8(v_operation_id,'h2-test-worker',pg_catalog.repeat('3',64));
  IF v_result->>'work_kind' <> 'EMPTY_RESERVED_BATCH_CLEANUP_REQUIRED' THEN
    RAISE EXCEPTION 'all-empty shell skipped cleanup: %',v_result;
  END IF;
  UPDATE public.pay_batches SET status = 'CANCELLED' WHERE id = v_batch;
  v_result := public.banking_pay_draft_advance_bounded_v8(v_operation_id,'h2-test-worker',pg_catalog.repeat('3',64));
  IF v_result->>'work_kind' <> 'TERMINAL_FAILURE_REQUIRED'
     OR v_result->>'code' <> 'PAY_DRAFT_ALL_SELECTED_PAYMENTS_ALREADY_RESERVED' THEN
    RAISE EXCEPTION 'all-empty operation did not preserve established failure: %',v_result;
  END IF;
  IF EXISTS (SELECT 1 FROM private.banking_pay_draft_operation_terminal_results_v8 WHERE operation_id=v_operation_id) THEN
    RAISE EXCEPTION 'bounded adapter published an unauthorised terminal result';
  END IF;
END;
$all_empty_integrity_fixture$;

ROLLBACK;
