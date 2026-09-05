-- Test-only helpers for rollback-contained V8 certificate consumer fixtures.
-- These helpers mirror the current mandatory normalized certificate shape.

CREATE OR REPLACE FUNCTION pg_temp.h2_seed_workbench_context_v8(
  p_actor_id uuid,
  p_session_id uuid,
  p_snapshot_run_id uuid,
  p_session_signature text,
  p_session_version bigint,
  p_progress_counter_version bigint,
  p_generation bigint
)
RETURNS void
LANGUAGE plpgsql
SET search_path TO ''
AS $function$
BEGIN
  INSERT INTO public.tms_users(id,email,role,password_hash)
  VALUES (
    p_actor_id,'h2-'||p_actor_id::text||'@example.invalid','admin','test-only'
  ) ON CONFLICT (id) DO NOTHING;

  INSERT INTO public.banking_pay_snapshot_runs(
    id,pay_date,week_ending_cutoff,pay_week_start,
    eligibility_from_date,eligibility_to_date,status,is_active
  ) VALUES (
    p_snapshot_run_id,'2026-09-04','2026-09-06','2026-08-31',
    '2026-08-31','2026-09-06','READY',false
  ) ON CONFLICT (id) DO NOTHING;

  INSERT INTO public.banking_pay_workbench_sessions(
    id,actor_user_id,pay_date,week_ending_cutoff,status,version,
    progress_counter_version,progress_state,source_snapshot_run_id,session_signature,
    scope_change_generation_target,scope_change_generation_applied,
    scope_change_generation_shadow_checked,authority_fence_generation
  ) VALUES (
    p_session_id,p_actor_id,'2026-09-04','2026-09-06','OPEN',p_session_version,
    p_progress_counter_version,'READY',p_snapshot_run_id,p_session_signature,
    p_generation,p_generation,p_generation,1
  );
END;
$function$;

CREATE OR REPLACE FUNCTION pg_temp.h2_seed_candidate_v8(
  p_candidate_id uuid,
  p_pay_channel text
)
RETURNS void
LANGUAGE plpgsql
SET search_path TO ''
AS $function$
BEGIN
  INSERT INTO public.candidates(id,pay_method)
  VALUES (p_candidate_id,p_pay_channel)
  ON CONFLICT (id) DO NOTHING;
END;
$function$;

CREATE OR REPLACE FUNCTION pg_temp.h2_seed_certificate_v8(
  p_certificate_uuid uuid,
  p_session_id uuid,
  p_actor_id uuid,
  p_snapshot_run_id uuid,
  p_session_signature text,
  p_session_version bigint,
  p_progress_counter_version bigint,
  p_generation bigint,
  p_constituent_count integer,
  p_partition_count integer,
  p_publication_count integer,
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
    selected_constituent_count,selected_canonical_amount_ex_vat_total,
    selected_constituents_digest_sha256,selected_partition_count,
    selected_partitions_digest_sha256,selected_partitions_ordering,policy_contract_version,
    before_policy_projection_digest_sha256,after_policy_projection_digest_sha256,
    policy_digests_equal,execution_recovery_delta_only,forbidden_policy_delta_count,
    no_payment_policy_change,build_id,build_idempotency_key,created_by_user_id,sealed_at_utc,
    filter_binding_mode,filter_context_digest_sha256
  ) VALUES (
    p_certificate_uuid,'WORKBENCH_SETTLED_CERTIFICATION_V2',
    'WORKBENCH_SETTLED_CERTIFICATION_V2:'||p_overall_digest_sha256,
    p_overall_digest_sha256,'SEALED_CURRENT',p_session_id,p_session_version,
    p_progress_counter_version,'READY',p_snapshot_run_id,p_session_signature,
    '2026-09-04','2026-09-06',pg_catalog.repeat('1',64),
    p_generation,p_generation,p_generation,1,p_publication_count,pg_catalog.repeat('2',64),
    p_publication_count,p_publication_count,p_publication_count,0,0,
    p_constituent_count,p_constituent_count,0,0,p_constituent_count,
    'row_ordinal asc, preview_row_id asc',256,
    ((p_constituent_count + 255) / 256),true,false,true,true,true,true,true,true,true,
    0,0,0,0,0,true,true,true,p_constituent_count,0,pg_catalog.repeat('3',64),
    p_constituent_count,p_total_ex_vat,pg_catalog.repeat('4',64),p_partition_count,
    pg_catalog.repeat('5',64),
    'minimum constituent ordinal asc, candidate_id asc, resolved_pay_channel asc',
    'WORKBENCH_PAYMENT_POLICY_PARITY_V2',pg_catalog.repeat('6',64),pg_catalog.repeat('6',64),
    true,true,0,true,p_certificate_uuid,'h2-runtime-'||p_certificate_uuid::text,
    p_actor_id,pg_catalog.clock_timestamp(),'EXACT_CERTIFIED_SELECTED_UNIVERSE',
    pg_catalog.repeat('7',64)
  );
END;
$function$;

CREATE OR REPLACE FUNCTION pg_temp.h2_seed_certificate_entry_v8(
  p_certificate_uuid uuid,
  p_session_id uuid,
  p_constituent_ordinal integer,
  p_candidate_publication_ordinal integer,
  p_candidate_id uuid,
  p_preview_row_id uuid,
  p_pay_channel text,
  p_amount_ex_vat text,
  p_source_identity_digest_sha256 text,
  p_certified_session_version bigint
)
RETURNS void
LANGUAGE plpgsql
SET search_path TO ''
AS $function$
DECLARE
  v_source_publication_id uuid := pg_catalog.md5(
    p_certificate_uuid::text||':publication:'||p_candidate_publication_ordinal::text)::uuid;
  v_source_build_run_id uuid := pg_catalog.md5(
    p_certificate_uuid::text||':build:'||p_candidate_publication_ordinal::text)::uuid;
BEGIN
  PERFORM pg_temp.h2_seed_candidate_v8(p_candidate_id,p_pay_channel);

  INSERT INTO public.banking_pay_workbench_session_candidate_state(
    id,session_id,candidate_id,status,effective_candidate_fragment_json,
    effective_summary_fragment_json,effective_paye_candidate_json,
    effective_non_paye_payee_json,effective_payees_json,
    effective_case_resolution_states_json,effective_canonical_preview_lines_json,
    source_change_seq,session_version,last_recomputed_at_utc
  ) VALUES (
    pg_catalog.md5(p_certificate_uuid::text||':state:'||p_candidate_publication_ordinal::text)::uuid,
    p_session_id,p_candidate_id,'READY',
    pg_catalog.jsonb_build_object('candidate_id',p_candidate_id::text),
    '{}'::jsonb,CASE WHEN p_pay_channel='PAYE' THEN '{}'::jsonb ELSE NULL END,
    CASE WHEN p_pay_channel='UMBRELLA' THEN '{}'::jsonb ELSE NULL END,
    '[]'::jsonb,'[]'::jsonb,'[]'::jsonb,1,p_certified_session_version,
    pg_catalog.clock_timestamp()
  ) ON CONFLICT (session_id,candidate_id) DO NOTHING;

  INSERT INTO public.banking_pay_workbench_session_scope(
    session_id,candidate_id,scope_ordinal,status,seeded,dirty,
    certified_preview_publication_required,certified_preview_publication_parity_ok,
    certified_preview_publication_session_version,certified_preview_publication_source_change_seq,
    certified_preview_publication_source_build_run_id,
    certified_preview_publication_attestation_json,
    certified_preview_publication_attested_at_utc,
    certified_preview_publication_source_publication_id
  ) VALUES (
    p_session_id,p_candidate_id,p_candidate_publication_ordinal,'READY',true,false,true,true,
    p_certified_session_version,1,v_source_build_run_id,
    pg_catalog.jsonb_build_object(
      'source_publication_id',v_source_publication_id::text,
      'source_build_run_id',v_source_build_run_id::text,
      'parity_ok',true),
    pg_catalog.clock_timestamp(),v_source_publication_id
  ) ON CONFLICT (session_id,candidate_id) DO NOTHING;

  INSERT INTO public.banking_pay_workbench_preview_rows(
    id,session_id,candidate_id,section,row_key,row_ordinal,row_json,
    key_type,key_value,selected,selection_state,status,session_version
  ) VALUES (
    p_preview_row_id,p_session_id,p_candidate_id,'canonical_preview_lines',
    'fixture:'||p_constituent_ordinal::text,p_constituent_ordinal,
    pg_catalog.jsonb_build_object(
      'line_type','TS_TOTAL','amount_ex_vat',p_amount_ex_vat::numeric,
      'key_type','TS_TOTAL','key_value',p_preview_row_id::text),
    'TS_TOTAL',p_preview_row_id::text,true,'SELECTED','READY',p_certified_session_version
  );

  INSERT INTO private.banking_pay_workbench_settled_certificate_publications_v8(
    certificate_uuid,scope_ordinal,candidate_id,candidate_state_id,candidate_state_status,
    source_change_seq,source_build_run_id,source_publication_id,
    certified_publication_session_version,publication_attestation_version,
    publication_attestation_digest_sha256,publication_parity_ok,publication_attested_at_utc
  ) VALUES (
    p_certificate_uuid,p_candidate_publication_ordinal,p_candidate_id,
    pg_catalog.md5(p_certificate_uuid::text||':state:'||p_candidate_publication_ordinal::text)::uuid,
    'READY',1,v_source_build_run_id,v_source_publication_id,p_certified_session_version,1,
    pg_catalog.repeat('8',64),true,pg_catalog.clock_timestamp()
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
    p_source_identity_digest_sha256,p_candidate_publication_ordinal,p_candidate_id,p_pay_channel,
    'BANK_TRANSFER',CASE WHEN p_amount_ex_vat::numeric < 0 THEN 'NEGATIVE'
      WHEN p_amount_ex_vat::numeric > 0 THEN 'POSITIVE' ELSE 'ZERO' END,
    'WORKED_TIME','TS_TOTAL',p_preview_row_id::text,p_amount_ex_vat,'UNPAID','0.00',
    pg_catalog.repeat('9',64),'CURRENT',pg_catalog.repeat('a',64),'NOT_APPLICABLE',
    'STANDARD','EXPECTED','WORKED_TIME',p_source_identity_digest_sha256,p_amount_ex_vat,
    pg_catalog.repeat('b',64),'NOT_APPLICABLE',0,pg_catalog.repeat('c',64),0,
    pg_catalog.repeat('d',64),0,pg_catalog.repeat('e',64),'READY','SELECTED',true,true,true,
    p_source_identity_digest_sha256
  );
END;
$function$;

CREATE OR REPLACE FUNCTION pg_temp.h2_seed_certificate_operation_link_v8(
  p_operation_id uuid,
  p_certificate_uuid uuid,
  p_overall_digest_sha256 text,
  p_pay_channel_scope text,
  p_idempotency_key text,
  p_channel_manifest_digest_sha256 text,
  p_link_state text
)
RETURNS void
LANGUAGE plpgsql
SET search_path TO ''
AS $function$
BEGIN
  INSERT INTO private.banking_pay_workbench_settled_certificate_operation_links_v8(
    operation_id,certificate_uuid,certification_id,overall_digest_sha256,pay_channel_scope,
    idempotency_key,admission_request_digest_sha256,channel_manifest_digest_sha256,link_state,
    filter_context_digest_sha256,filter_scope_manifest_digest_sha256,
    same_week_paye_override_continue,same_week_paye_override_verified,
    same_week_paye_override_used,same_week_paye_override_pay_date,
    same_week_paye_override_pay_week_start,same_week_paye_override_pay_week_end,
    same_week_paye_override_digest_sha256
  ) VALUES (
    p_operation_id,p_certificate_uuid,
    'WORKBENCH_SETTLED_CERTIFICATION_V2:'||p_overall_digest_sha256,
    p_overall_digest_sha256,p_pay_channel_scope,p_idempotency_key,pg_catalog.repeat('1',64),
    p_channel_manifest_digest_sha256,p_link_state,pg_catalog.repeat('7',64),pg_catalog.repeat('3',64),
    false,false,false,'2026-09-04','2026-08-31','2026-09-06',pg_catalog.repeat('4',64)
  );
END;
$function$;
