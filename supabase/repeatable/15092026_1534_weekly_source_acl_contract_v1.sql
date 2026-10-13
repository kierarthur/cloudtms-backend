-- Repeatable CloudTMS function/view authority: weekly_source_acl_contract_v1
-- Plan 6 tables are server-owned implementation details. Browser and service
-- roles may use only the deliberately granted RPC surface; they may never
-- write, read or enumerate the backing tables directly.

\set ON_ERROR_STOP on

begin;

-- One explicit inventory prevents a newly added Weekly Source relation from
-- silently inheriting a permissive default. IMMUTABLE_FACTS_WITH_LIFECYCLE
-- permits updates only to the separately allowlisted lifecycle columns and
-- still prohibits deletion. STATEFUL_SERVER_OWNED means that a reviewed owner
-- may perform bounded lifecycle transitions; neither class grants a role
-- direct table access.
create or replace function private._weekly_source_acl_table_contract_v1()
returns table(table_name text, record_class text)
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  values
    ('office_action_notifications', 'IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_candidate_app_mutation_receipts', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_candidate_cohorts', 'STATEFUL_SERVER_OWNED'),
    ('weekly_candidate_message_notifications', 'IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_candidate_outreach_generations', 'STATEFUL_SERVER_OWNED'),
    ('weekly_candidate_outreach_memberships', 'STATEFUL_SERVER_OWNED'),
    ('weekly_candidate_response_draft_items', 'STATEFUL_SERVER_OWNED'),
    ('weekly_candidate_response_drafts', 'STATEFUL_SERVER_OWNED'),
    ('weekly_completed_pack_copy_events', 'IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_discrepancy_events', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_discrepancy_incidents', 'STATEFUL_SERVER_OWNED'),
    ('weekly_exceptional_c1_component_records', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_exceptional_c1_publication_checkpoints', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_exceptional_c1_publication_requests', 'STATEFUL_SERVER_OWNED'),
    ('weekly_exceptional_c1_source_parts', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_exceptional_c1_source_records', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_exceptional_c1_unknown_outcomes', 'IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_exceptional_orchestration_runs', 'STATEFUL_SERVER_OWNED'),
    ('weekly_exceptional_orchestration_steps', 'STATEFUL_SERVER_OWNED'),
    ('weekly_exceptional_pay_family_events', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_exceptional_pay_generations', 'STATEFUL_SERVER_OWNED'),
    ('weekly_exceptional_pay_target_events', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_exceptional_pay_target_families', 'STATEFUL_SERVER_OWNED'),
    ('weekly_exceptional_payment_approvals', 'IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_exceptional_payment_events', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_exceptional_pending_reconciliation_targets', 'STATEFUL_SERVER_OWNED'),
    ('weekly_expense_authority_generations', 'STATEFUL_SERVER_OWNED'),
    ('weekly_final_source_correction_root_impacts', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_final_source_correction_sessions', 'STATEFUL_SERVER_OWNED'),
    ('weekly_issue_comparison_revisions', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_manager_cohort_due_events', 'STATEFUL_SERVER_OWNED'),
    ('weekly_manager_recipient_generations', 'STATEFUL_SERVER_OWNED'),
    ('weekly_manager_recipient_memberships', 'STATEFUL_SERVER_OWNED'),
    ('weekly_manager_recipient_routes', 'STATEFUL_SERVER_OWNED'),
    ('weekly_manager_review_batches', 'STATEFUL_SERVER_OWNED'),
    ('weekly_manager_review_items', 'STATEFUL_SERVER_OWNED'),
    ('weekly_manager_route_preparations', 'IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_manager_route_receipts', 'IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_message_delivery_failures', 'IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_message_dispatch_commands', 'STATEFUL_SERVER_OWNED'),
    ('weekly_message_dispatch_targets', 'STATEFUL_SERVER_OWNED'),
    ('weekly_message_intents', 'STATEFUL_SERVER_OWNED'),
    ('weekly_message_provider_attempts', 'IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_message_renders', 'IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_message_target_attempts', 'IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_route_activations', 'STATEFUL_SERVER_OWNED'),
    ('weekly_source_billing_movements', 'IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_source_charge_acceptances', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_charge_checks', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_client_cycle_completions', 'STATEFUL_SERVER_OWNED'),
    ('weekly_source_client_manifests', 'STATEFUL_SERVER_OWNED'),
    ('weekly_source_client_policies', 'STATEFUL_SERVER_OWNED'),
    ('weekly_source_contract_policies', 'STATEFUL_SERVER_OWNED'),
    ('weekly_source_contract_qualification_observations', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_cycles', 'STATEFUL_SERVER_OWNED'),
    ('weekly_source_entitlement_decision_bundles', 'IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_source_entitlement_head_components', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_entitlement_heads', 'IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_source_expense_cell_evidence', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_expense_materialisations', 'IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_source_expense_pay_materialisations', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_final_revisions', 'STATEFUL_SERVER_OWNED'),
    ('weekly_source_final_snapshot_lines', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_finalisation_pay_runs', 'STATEFUL_SERVER_OWNED'),
    ('weekly_source_finalisation_pay_tasks', 'STATEFUL_SERVER_OWNED'),
    ('weekly_source_format_profiles', 'STATEFUL_SERVER_OWNED'),
    ('weekly_source_global_settings', 'STATEFUL_SERVER_OWNED'),
    ('weekly_source_group_clients', 'STATEFUL_SERVER_OWNED'),
    ('weekly_source_group_rule_assignments', 'STATEFUL_SERVER_OWNED'),
    ('weekly_source_groups', 'STATEFUL_SERVER_OWNED'),
    ('weekly_source_invoice_hold_releases', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_invoice_line_bindings', 'STATEFUL_SERVER_OWNED'),
    ('weekly_source_invoice_placements', 'STATEFUL_SERVER_OWNED'),
    ('weekly_source_invoice_presentation_lines', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_manifest_movements', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_money_cell_evidence', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_nhsp_backing_reports', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_ordinary_pay_projection_receipts', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_pending_entitlement_bundles', 'IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_source_physical_rows', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_projection_publications', 'STATEFUL_SERVER_OWNED'),
    ('weekly_source_report_scopes', 'STATEFUL_SERVER_OWNED'),
    ('weekly_source_root_authorisations', 'IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_source_row_economic_snapshots', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_row_expense_policy_snapshots', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_row_qualification_evidence', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_row_resolutions', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_row_timesheet_lineages', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_rule_set_versions', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_rule_sets', 'STATEFUL_SERVER_OWNED'),
    ('weekly_source_self_bill_report_line_bindings', 'STATEFUL_SERVER_OWNED'),
    ('weekly_source_self_bill_reports', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_state_transitions', 'IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_source_upload_attempts', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_upload_rows', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_upload_supersessions', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_uploads', 'STATEFUL_SERVER_OWNED'),
    ('weekly_timesheet_authority_resolutions', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_timesheet_reference_apply_items', 'IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_timesheet_reference_apply_operations', 'STATEFUL_SERVER_OWNED'),
    ('weekly_timesheet_source_comparisons', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_timesheet_submission_request_memberships', 'STATEFUL_SERVER_OWNED'),
    ('weekly_timesheet_submission_requests', 'STATEFUL_SERVER_OWNED'),
    ('weekly_work_event_source_links', 'IMMUTABLE_APPEND_ONLY'),
    ('weekly_work_events', 'IMMUTABLE_APPEND_ONLY');
$function$;

-- Lifecycle columns are intentionally narrow. Every other column in these
-- records is an immutable identity, source, economic, content or audit fact.
-- The owning service routine remains responsible for validating the legal
-- state-transition arc; this contract prevents it (or a future owner) from
-- rewriting the evidence alongside that transition.
create or replace function private._weekly_source_acl_lifecycle_column_contract_v1()
returns table(table_name text, column_name text, column_ordinal integer)
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  values
    ('weekly_source_state_transitions','ordinary_source_entitlement_projection_state',1),
    ('weekly_source_billing_movements','placement_state',1),
    ('weekly_source_expense_materialisations','state',1),
    ('weekly_message_renders','state',1),
    ('weekly_message_provider_attempts','completed_at_utc',1),
    ('weekly_message_provider_attempts','outcome',2),
    ('weekly_message_provider_attempts','bounded_provider_receipt_json',3),
    ('weekly_message_provider_attempts','bounded_error_json',4),
    ('weekly_message_target_attempts','completed_at_utc',1),
    ('weekly_message_target_attempts','outcome',2),
    ('weekly_message_target_attempts','provider_message_id',3),
    ('weekly_message_target_attempts','bounded_provider_receipt_json',4),
    ('weekly_message_target_attempts','bounded_error_json',5),
    ('weekly_message_target_attempts','result_hash',6),
    ('weekly_message_delivery_failures','state',1),
    ('weekly_message_delivery_failures','acknowledged_at_utc',2),
    ('weekly_message_delivery_failures','resolved_at_utc',3),
    ('weekly_candidate_message_notifications','retired_at_utc',1),
    ('weekly_manager_route_preparations','state',1),
    ('weekly_manager_route_preparations','bound_at_utc',2),
    ('weekly_manager_route_preparations','retired_at_utc',3),
    ('weekly_manager_route_receipts','state',1),
    ('weekly_manager_route_receipts','revoked_at_utc',2),
    ('weekly_completed_pack_copy_events','provider_command_id',1),
    ('weekly_completed_pack_copy_events','state',2),
    ('office_action_notifications','operational_state',1),
    ('office_action_notifications','read_at_utc',2),
    ('office_action_notifications','resolved_at_utc',3),
    ('weekly_timesheet_reference_apply_items','applied_at_utc',1),
    ('weekly_exceptional_c1_unknown_outcomes','state',1),
    ('weekly_exceptional_c1_unknown_outcomes','recovery_result_json',2),
    ('weekly_exceptional_c1_unknown_outcomes','recovered_at_utc',3),
    ('weekly_exceptional_c1_unknown_outcomes','recovery_idempotency_key',4),
    -- Decision D8, proof/34 section 4 and proof/36 section 5.6: these three
    -- columns moved off the per-source-row binding onto
    -- weekly_source_root_authorisations below. The head-publication
    -- coordinator writes current_entitlement_head_id on the live generation and
    -- nothing else; the withdrawal owner sets the two withdrawal columns and
    -- clears the head in the same statement. Family, physical root, version,
    -- signature, generation, actor and time stay immutable on every generation,
    -- and updated_at_utc is the audit clock the writers touch with them.
    ('weekly_source_root_authorisations','current_entitlement_head_id',1),
    ('weekly_source_root_authorisations','withdrawn_at_utc',2),
    ('weekly_source_root_authorisations','withdrawn_by_user_id',3),
    ('weekly_source_root_authorisations','updated_at_utc',4),
    -- proof/36 section 5 step 7: the approved protected-hours decision is
    -- marked withdrawn, never deleted.
    ('weekly_exceptional_payment_approvals','withdrawn_at_utc',1),
    ('weekly_exceptional_payment_approvals','withdrawn_by_user_id',2),
    ('weekly_exceptional_payment_approvals','withdrawal_kind',3),
    -- 24 section 4.5 step 5: bundles and heads are staged and then activated.
    -- Only the activation lifecycle may move; every economic fact, digest,
    -- inventory count and certified-zero flag is frozen when written (WB-005).
    ('weekly_source_entitlement_decision_bundles','state',1),
    ('weekly_source_entitlement_decision_bundles','committed_at_utc',2),
    ('weekly_source_entitlement_decision_bundles','superseded_at_utc',3),
    ('weekly_source_entitlement_heads','state',1),
    ('weekly_source_entitlement_heads','committed_at_utc',2),
    ('weekly_source_entitlement_heads','superseded_at_utc',3),
    ('weekly_source_entitlement_heads','superseded_by_head_id',4),
    ('weekly_source_entitlement_heads','publication_receipt_digest',5),
    ('weekly_source_entitlement_heads','scope_change_tx_token',6),
    -- HANDOVER 2 round-5 ruling A3 step 3 (package WP-07c): the Office
    -- change-of-mind withdrawal supersedes a committed head atomically with the
    -- withdrawal, "with an explicit withdrawal reason and immutable predecessor
    -- link".  Those are the two supersession columns added by migration
    -- 18092026_0900_weekly_source_withdrawal_supersession.sql.  They are part
    -- of the SAME activation lifecycle as superseded_at_utc and
    -- superseded_by_head_id above -- a head still becomes non-current exactly
    -- once and never returns -- so they belong here and nowhere else.  No
    -- economic fact, digest, inventory count or certified-zero flag moves.
    ('weekly_source_entitlement_heads','superseded_reason',7),
    ('weekly_source_entitlement_heads','superseded_by_withdrawal_id',8),
    -- proof/32 section 2 copies decision_id and decided_by_user_id once from
    -- the immutable accepted Office decision, and section 7 revalidates the
    -- stored member set and request digest as fixed facts.  Only the lease, the
    -- backoff clock, the failure count, the census blob and the release record
    -- may move on a tick (section 2, section 8 step 5, section 10).
    ('weekly_source_pending_entitlement_bundles','state',1),
    ('weekly_source_pending_entitlement_bundles','pending_revision',2),
    ('weekly_source_pending_entitlement_bundles','lease_owner',3),
    ('weekly_source_pending_entitlement_bundles','lease_token',4),
    ('weekly_source_pending_entitlement_bundles','lease_worker_run_id',5),
    ('weekly_source_pending_entitlement_bundles','lease_expires_at_utc',6),
    ('weekly_source_pending_entitlement_bundles','next_check_at_utc',7),
    ('weekly_source_pending_entitlement_bundles','technical_failure_count',8),
    ('weekly_source_pending_entitlement_bundles','manual_review_reason',9),
    ('weekly_source_pending_entitlement_bundles','last_census_json',10),
    ('weekly_source_pending_entitlement_bundles','released_receipt_id',11),
    ('weekly_source_pending_entitlement_bundles','released_receipt_digest',12),
    ('weekly_source_pending_entitlement_bundles','released_by_worker_id',13),
    ('weekly_source_pending_entitlement_bundles','released_by_worker_run_id',14),
    ('weekly_source_pending_entitlement_bundles','released_at_utc',15),
    ('weekly_source_pending_entitlement_bundles','updated_at_utc',16);
$function$;

-- The registered Weekly Source private helpers.
--
-- This is an inventory of owner-internal routines, NOT a grant list and NOT an
-- exception list: every signature here is revoked from PUBLIC, anon,
-- authenticated AND service_role, exactly like every other Weekly Source
-- routine that is not in the service RPC contract. Listing a routine here
-- grants it nothing. Its purpose is that a helper is declared once, centrally,
-- with its authority, so that it is neither mistaken for part of the granted
-- RPC surface nor left undeclared; the verifier then re-proves for every entry
-- that it exists, is owned by the release owner and is executable by no role.
--
-- Two rules:
--   * a routine granted execute to service_role belongs in
--     private._weekly_source_acl_service_rpc_contract_v1(), never here;
--   * a signature here must never also appear there. The closure below and the
--     verifier each refuse that independently.
--
-- The inventory is a floor, not a closed set: packages are still landing, so a
-- Weekly Source private routine that is absent from this list is not an error,
-- while a listed one that loses its revocations is. It can be sealed as an
-- exact set once every package has reported.
create or replace function private._weekly_source_acl_private_helper_contract_v1()
returns table(function_signature text)
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  values
    ('private._weekly_source_completed_pack_copy_eligibility_v1(uuid)'),
    ('private._weekly_source_rule_group_context_v1(uuid,date,date,jsonb,boolean)'),
    ('private._weekly_source_rule_values_v1(jsonb)'),
    ('private.weekly_source_accepted_publication_action_immutable_v1()'),
    ('private.weekly_source_accepted_publication_action_record_v1(jsonb,jsonb,jsonb)'),
    ('private.weekly_source_accepted_publication_action_verify_v1(uuid,uuid,bigint,bytea,uuid)'),
    ('private.weekly_source_accepted_publication_context_v1(jsonb)'),
    ('private.weekly_source_accepted_removal_query_evidence_v1(uuid,uuid,uuid,uuid,uuid)'),
    ('private.weekly_source_actual_timesheet_submission_v1(uuid)'),
    ('private.weekly_source_agency_invoice_check_only_v1(uuid)'),
    ('private.weekly_source_approval_duty_v1(uuid)'),
    ('private.weekly_source_authority_subset_v1(jsonb)'),
    ('private.weekly_source_automatic_reconcile_capability_assert_v1(uuid)'),
    ('private.weekly_source_automatic_reconcile_certificate_v1(uuid,uuid,uuid)'),
    ('private.weekly_source_automatic_reconcile_selection_v1(uuid,uuid)'),
    ('private.weekly_source_automatic_reconcile_stage_proof_v1(jsonb)'),
    ('private.weekly_source_automatic_reconcile_stage_replay_assert_v1(uuid,jsonb)'),
    ('private.weekly_source_billing_movement_hash_v1(jsonb)'),
    ('private.weekly_source_bulk_own_stage_context_v1(uuid,uuid,text,text,text)'),
    ('private.weekly_source_bulk_own_stage_event_excluded_v1(uuid,uuid,jsonb)'),
    ('private.weekly_source_bulk_own_stage_v1(uuid,uuid,uuid,text,text,text)'),
    ('private.weekly_source_bulk_signed_claim_v1(uuid)'),
    ('private.weekly_source_bulk_source_cas_assert_v1(uuid,uuid[])'),
    ('private.weekly_source_bulk_stage_proof_v1(jsonb,uuid)'),
    ('private.weekly_source_bulk_stage_replay_assert_v1(uuid,jsonb)'),
    ('private.weekly_source_candidate_approved_clock_row_v2(jsonb,jsonb,text)'),
    ('private.weekly_source_candidate_current_rows_v1(uuid,uuid,uuid,uuid,date)'),
    ('private.weekly_source_candidate_generation_current_v1(uuid)'),
    ('private.weekly_source_candidate_head_hours_v2(uuid)'),
    ('private.weekly_source_candidate_initial_approval_v2(uuid)'),
    ('private.weekly_source_candidate_prefinal_publish_recheck_v1(uuid,uuid)'),
    ('private.weekly_source_candidate_saved_local_hours_v2(uuid)'),
    ('private.weekly_source_candidate_serial_gate_v1(uuid,text,uuid,text)'),
    ('private.weekly_source_candidate_submission_compare_sync_v1(uuid,uuid,uuid,uuid,bytea,timestamp with time zone)'),
    ('private.weekly_source_canonical_json_text_v1(jsonb)'),
    ('private.weekly_source_check_only_query_rows_v1(uuid,uuid,jsonb)'),
    ('private.weekly_source_checking_client_scope_v1(uuid)'),
    ('private.weekly_source_completed_rows_v1(uuid,uuid,uuid)'),
    ('private.weekly_source_configured_agency_invoice_v1(uuid,uuid,uuid,text,date)'),
    ('private.weekly_source_configured_upload_profile_v1(uuid,uuid,text,text)'),
    ('private.weekly_source_correct_final_office_preview_v1(uuid)'),
    ('private.weekly_source_correct_final_preconditions_v1(uuid,uuid,uuid,boolean)'),
    ('private.weekly_source_covered_hours_incident_v1(uuid)'),
    ('private.weekly_source_current_publication_guard_v1(uuid,text,uuid,uuid,uuid,bigint)'),
    ('private.weekly_source_cutoff_admission_assert_v1(uuid,text,timestamp with time zone)'),
    ('private.weekly_source_effective_inventory_v1(uuid)'),
    ('private.weekly_source_effective_rule_set_v1(uuid,uuid)'),
    ('private.weekly_source_entitlement_component_id_v1(text,text,text,text)'),
    ('private.weekly_source_entitlement_components_v1(jsonb,jsonb)'),
    ('private.weekly_source_entitlement_derived_uuid_v1(text,text)'),
    ('private.weekly_source_entitlement_head_inventory_assert_v1()'),
    ('private.weekly_source_entitlement_head_receipt_assert_v1()'),
    ('private.weekly_source_entitlement_head_root_identity_v1()'),
    ('private.weekly_source_entitlement_proposal_record_v1(jsonb,uuid,uuid,date,uuid)'),
    ('private.weekly_source_entitlement_proposal_request_v1(uuid,uuid,text,uuid,bigint,uuid,uuid,jsonb,text)'),
    ('private.weekly_source_entitlement_proposal_request_v2(uuid,jsonb,text,uuid,bigint,uuid,uuid,jsonb,text)'),
    ('private.weekly_source_entitlement_publication_receipt_immutable_v1()'),
    ('private.weekly_source_entitlement_publish_core_v1(jsonb,text,jsonb,uuid,text,uuid,jsonb,jsonb)'),
    ('private.weekly_source_entitlement_publish_immediate_v1(jsonb)'),
    ('private.weekly_source_expected_week_question_v1(uuid)'),
    ('private.weekly_source_expected_week_unsigned_materialise_v1(uuid)'),
    ('private.weekly_source_final_rule_version_guard_v1()'),
    ('private.weekly_source_final_upload_assert_v1(uuid)'),
    ('private.weekly_source_final_upload_facts_v1(text,date,timestamp with time zone,timestamp with time zone,text,date,date,jsonb,date)'),
    ('private.weekly_source_final_upload_gate_v1(uuid)'),
    ('private.weekly_source_final_value_vector_v1(jsonb)'),
    ('private.weekly_source_frozen_rule_values_v1(uuid)'),
    ('private.weekly_source_has_self_bill_rules_v1(uuid,uuid)'),
    ('private.weekly_source_head_origin_guard_v2()'),
    ('private.weekly_source_healthroster_checklist_leaf_v1(jsonb)'),
    ('private.weekly_source_import_is_prepared_v1(uuid)'),
    ('private.weekly_source_import_outreach_v1(uuid)'),
    ('private.weekly_source_incident_contact_retire_v1(uuid)'),
    ('private.weekly_source_initial_additional_components_v2(jsonb,jsonb,numeric,numeric,text)'),
    ('private.weekly_source_inventory_approval_basis_v2(uuid,jsonb)'),
    ('private.weekly_source_invoice_adjustment_authority_v1(uuid,uuid[])'),
    ('private.weekly_source_invoice_adjustment_line_valid_v1(uuid,uuid)'),
    ('private.weekly_source_invoice_adjustment_payload_admit_v1(uuid,jsonb)'),
    ('private.weekly_source_invoice_adjustments_apply_v1(uuid,jsonb,uuid)'),
    ('private.weekly_source_invoice_admission_receipt_map_v1(uuid)'),
    ('private.weekly_source_invoice_admitted_member_money_v1(uuid,uuid)'),
    ('private.weekly_source_invoice_correction_insert_v1(uuid,uuid,uuid)'),
    ('private.weekly_source_invoice_correction_unwind_v1(uuid,uuid,uuid)'),
    ('private.weekly_source_invoice_duty_v1(uuid)'),
    ('private.weekly_source_invoice_economics_fingerprint_v1(jsonb,text,numeric)'),
    ('private.weekly_source_invoice_family_timesheet_ids_v1(uuid)'),
    ('private.weekly_source_invoice_issue_blockers_v1(uuid,text[])'),
    ('private.weekly_source_invoice_issue_reasons_v1(jsonb,text[])'),
    ('private.weekly_source_invoice_issue_skippable_code_v1(text)'),
    ('private.weekly_source_invoice_issue_skippable_reason_v1(text)'),
    ('private.weekly_source_invoice_issue_validate_v1(uuid)'),
    ('private.weekly_source_invoice_legacy_render_support_v1(uuid)'),
    ('private.weekly_source_invoice_line_amounts_v1(uuid,uuid)'),
    ('private.weekly_source_invoice_manifest_admission_money_v1(uuid)'),
    ('private.weekly_source_invoice_movement_money_v1(uuid,uuid)'),
    ('private.weekly_source_invoice_movement_window_id_v1(uuid)'),
    ('private.weekly_source_invoice_only_vector_v1(text,bigint)'),
    ('private.weekly_source_invoice_placement_identity_guard_v1()'),
    ('private.weekly_source_invoice_presentation_expected_source_hash_v1(uuid)'),
    ('private.weekly_source_invoice_presentation_release_authority_v1(uuid[],uuid)'),
    ('private.weekly_source_invoice_presentation_source_hash_v1(uuid[],uuid)'),
    ('private.weekly_source_invoice_receipt_projection_assert_v1(uuid)'),
    ('private.weekly_source_invoice_receipt_support_v1(uuid)'),
    ('private.weekly_source_invoice_rounding_authority_rebuild_v1(uuid)'),
    ('private.weekly_source_invoice_rounding_receipt_set_v1(uuid,jsonb)'),
    ('private.weekly_source_invoice_rounding_suppression_witness_v1(uuid)'),
    ('private.weekly_source_invoice_source_amount_movement_valid_v1(uuid)'),
    ('private.weekly_source_invoice_vat_target_pence_v1(numeric,numeric,text,numeric)'),
    ('private.weekly_source_invoice_window_cycle_v1(uuid,uuid)'),
    ('private.weekly_source_invoice_window_lock_inventory_v1(uuid,bigint,uuid,uuid)'),
    ('private.weekly_source_invoice_window_scope_v1(uuid,uuid,uuid,uuid)'),
    ('private.weekly_source_is_import_authoritative_v1(uuid,uuid,date)'),
    ('private.weekly_source_issued_signed_query_evidence_v1(uuid,uuid)'),
    ('private.weekly_source_local_origin_canonical_v2(jsonb)'),
    ('private.weekly_source_local_publication_context_v2(uuid,uuid,uuid)'),
    ('private.weekly_source_local_publication_origin_guard_v2()'),
    ('private.weekly_source_local_saved_status_v1(uuid,uuid,uuid)'),
    ('private.weekly_source_lock_and_resolve_families_v1(uuid,uuid[],text,uuid,text)'),
    ('private.weekly_source_lock_family_rows_v1(uuid[],uuid)'),
    ('private.weekly_source_magnit_bucket_assert_v1(jsonb,jsonb)'),
    ('private.weekly_source_magnit_contract_price_assert_v1(jsonb,jsonb)'),
    ('private.weekly_source_managed_root_guard_v1(uuid)'),
    ('private.weekly_source_manager_due_event_covers_v1(uuid,uuid)'),
    ('private.weekly_source_manual_review_command_immutable_v1()'),
    ('private.weekly_source_manual_review_final_rows_v2(uuid)'),
    ('private.weekly_source_manual_review_source_v2(uuid)'),
    ('private.weekly_source_manual_reviews_finalised_v1(uuid,uuid)'),
    ('private.weekly_source_mode_a_operation_key_v1(uuid,uuid)'),
    ('private.weekly_source_mode_a_refuse_operation_v1(uuid,uuid,uuid,uuid,uuid,text)'),
    ('private.weekly_source_mode_a_role_type_v1(text,text)'),
    ('private.weekly_source_not_finalised_dispatch_v1(uuid)'),
    ('private.weekly_source_not_finalised_due_scope_guard_v1()'),
    ('private.weekly_source_office_next_owner_v1()'),
    ('private.weekly_source_office_next_paid_evidence_page_v1(uuid,uuid,text,uuid,jsonb,integer)'),
    ('private.weekly_source_office_query_groups_v1(uuid,uuid,jsonb)'),
    ('private.weekly_source_operational_empty_v1(uuid,uuid)'),
    ('private.weekly_source_overlap_admission_assert_v1(uuid)'),
    ('private.weekly_source_pay_query_c1_receipt_v1(uuid,uuid,uuid)'),
    ('private.weekly_source_pay_query_facts_v1(uuid)'),
    ('private.weekly_source_pay_query_gate_v1(uuid)'),
    ('private.weekly_source_pay_query_local_receipt_v1(uuid,uuid,uuid)'),
    ('private.weekly_source_pay_query_next_receipt_v1(uuid,uuid,uuid)'),
    ('private.weekly_source_pay_query_scope_v1(uuid)'),
    ('private.weekly_source_protect_all_signed_selection_v1(uuid,uuid,jsonb)'),
    ('private.weekly_source_protected_contact_retire_v1(uuid,uuid)'),
    ('private.weekly_source_protected_family_lock_v1(uuid)'),
    ('private.weekly_source_protected_final_source_context_v1(uuid,uuid,uuid)'),
    ('private.weekly_source_protected_group_lock_v1(uuid)'),
    ('private.weekly_source_protected_match_candidates_v1(uuid,uuid,uuid)'),
    ('private.weekly_source_protected_query_rows_v1(uuid,uuid,boolean)'),
    ('private.weekly_source_protected_saved_prepare_route_v1(jsonb,text,text)'),
    ('private.weekly_source_protected_wait_receipt_v1(uuid,uuid)'),
    ('private.weekly_source_protected_wait_witness_v1(uuid,uuid)'),
    ('private.weekly_source_publication_comparison_record_v1(uuid,uuid,text,text)'),
    ('private.weekly_source_publication_component_canonical_v1(jsonb,text)'),
    ('private.weekly_source_publication_receipt_json_v1(uuid)'),
    ('private.weekly_source_publication_request_canonical_v1(jsonb,text,uuid)'),
    ('private.weekly_source_publication_request_digest_v1(jsonb)'),
    ('private.weekly_source_publication_require_keys_v1(jsonb,text[],text)'),
    ('private.weekly_source_publication_scalar_v1(jsonb,text,text,integer,boolean)'),
    ('private.weekly_source_publication_signed_family_fence_v1(uuid,jsonb)'),
    ('private.weekly_source_publication_signed_roots_v1(uuid)'),
    ('private.weekly_source_query_comparison_fingerprint_v1(uuid,integer,uuid,jsonb)'),
    ('private.weekly_source_query_manager_generation_v1(uuid,text,timestamp with time zone,boolean)'),
    ('private.weekly_source_query_manager_row_owned_v1(uuid,uuid,integer)'),
    ('private.weekly_source_query_resolved_decision_matches_v1(uuid,uuid,jsonb)'),
    ('private.weekly_source_rebill_movement_insert_v1(uuid,uuid)'),
    ('private.weekly_source_report_shift_facts_v1(uuid,uuid)'),
    ('private.weekly_source_report_shift_facts_v1(uuid,uuid,jsonb)'),
    ('private.weekly_source_resolve_root_identity_v1(uuid)'),
    ('private.weekly_source_retain_approved_additional_v2(uuid,jsonb)'),
    ('private.weekly_source_root_authorisation_identity_v1()'),
    ('private.weekly_source_root_authorisation_withdrawal_once_v1()'),
    ('private.weekly_source_root_integrity_assert_v1(uuid,text,integer)'),
    ('private.weekly_source_rule_coverage_floor_v1(uuid,uuid)'),
    ('private.weekly_source_schedule_compatible_v1(jsonb,date)'),
    ('private.weekly_source_selected_source_witness_v1(uuid,uuid,uuid)'),
    ('private.weekly_source_self_bill_binding_guard_v1()'),
    ('private.weekly_source_self_bill_report_bind_v1(uuid)'),
    ('private.weekly_source_self_bill_report_duplicate_hold_v1(uuid)'),
    ('private.weekly_source_self_bill_report_duplicate_prior_v1(uuid)'),
    ('private.weekly_source_self_bill_report_finalise_v1(jsonb,text)'),
    ('private.weekly_source_self_bill_report_manifest_v1(jsonb)'),
    ('private.weekly_source_self_bill_report_movement_insert_v1(uuid,uuid)'),
    ('private.weekly_source_self_bill_report_pay_witness_v1(uuid)'),
    ('private.weekly_source_self_bill_report_prior_binding_v1(uuid)'),
    ('private.weekly_source_self_bill_report_projection_rows_v1(uuid,uuid,jsonb)'),
    ('private.weekly_source_self_bill_report_row_dto_v1(jsonb)'),
    ('private.weekly_source_self_bill_report_row_hash_v1(jsonb)'),
    ('private.weekly_source_self_bill_report_rules_assert_v1(uuid,uuid)'),
    ('private.weekly_source_self_bill_report_seal_assert_v1(uuid)'),
    ('private.weekly_source_self_bill_report_stored_rows_v1(uuid)'),
    ('private.weekly_source_self_bill_report_workspace_v1(jsonb)'),
    ('private.weekly_source_shift_on_self_bill_context_v1(uuid,uuid,jsonb)'),
    ('private.weekly_source_shift_on_self_bill_v1(uuid,uuid)'),
    ('private.weekly_source_signed_check_only_v1(uuid)'),
    ('private.weekly_source_signed_submission_read_context_v1(uuid,uuid)'),
    ('private.weekly_source_snapshot_authority_subset_v1(uuid)'),
    ('private.weekly_source_snapshot_value_fingerprint_v1(uuid,jsonb)'),
    ('private.weekly_source_source_amount_acceptance_policy_fingerprint_v1()'),
    ('private.weekly_source_source_amount_warnings_v1(uuid,uuid)'),
    ('private.weekly_source_target_family_for_root_v1(uuid)'),
    ('private.weekly_source_target_family_root_identity_v1()'),
    ('private.weekly_source_timesheet_category_v2(uuid)'),
    ('private.weekly_source_unprotected_not_finalised_close_v1(uuid,uuid)'),
    ('private.weekly_source_upload_coverage_context_v1(uuid,uuid)'),
    ('private.weekly_source_upload_coverage_floor_assert_v1(uuid,uuid,text,date)'),
    ('private.weekly_source_upload_seal_core_v1(uuid,uuid)'),
    ('private.weekly_source_uuid_array_is_distinct_v1(uuid[])'),
    ('private.weekly_source_uuid_set_difference_v1(uuid[],uuid[])'),
    ('private.weekly_source_uuid_set_equals_v1(uuid[],uuid[])'),
    ('private.weekly_source_uuid_set_intersect_v1(uuid[],uuid[])'),
    ('private.weekly_source_uuid_set_union_v1(uuid[],uuid[])'),
    ('private.weekly_source_vat_rounding_movement_insert_v1(uuid,uuid,uuid)'),
    ('private.weekly_source_vat_rounding_movement_insert_v1(uuid,uuid,uuid,uuid)'),
    ('private.weekly_source_verified_role_band_match_v1(text,text,uuid,uuid,uuid,text,date)'),
    ('private.weekly_source_waiting_requested_week_v1(uuid,uuid,uuid,uuid,date)'),
    ('private.weekly_source_work_event_schedule_compatible_v1(uuid,timestamp without time zone,timestamp without time zone)');
$function$;

-- Only these routines may be executed by service_role. A signature may be
-- listed before its owning repeatable is installed; when it exists, this ACL
-- closure and its verifier require the exact service-only grant.
create or replace function private._weekly_source_acl_service_rpc_contract_v1()
returns table(function_signature text)
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  values
    ('private._weekly_source_completed_pack_copy_claim_event_valid_v1(public.mail_outbox)'),
    ('private.weekly_source_invoice_batch_rows_v1(jsonb,jsonb)'),
    ('private.weekly_source_invoice_batch_snapshot_v1()'),
    ('private.weekly_source_local_preauthorisation_write_allowed_v1(uuid,jsonb)'),
    ('private.weekly_source_managed_root_guard_decision_v1(uuid)'),
    ('private.weekly_source_office_authority_v1(uuid,text,uuid,uuid,date)'),
    ('private.weekly_source_pay_actor_allowed_v1(uuid)'),
    ('private.weekly_source_summary_pay_delayed_v1(uuid,uuid,uuid,date)'),
    ('public.timesheet_correction_credit_candidates_v1(jsonb)'),
    ('public.weekly_exceptional_pay_action_context_v1(jsonb)'),
    ('public.weekly_exceptional_pay_action_publication_status_v1(jsonb)'),
    ('public.weekly_exceptional_pay_complete_c1_publication_v1(jsonb)'),
    ('public.weekly_exceptional_pay_complete_local_v1(jsonb)'),
    ('public.weekly_exceptional_pay_next_publication_status_v1(jsonb)'),
    ('public.weekly_exceptional_pay_prepare_action_v1(jsonb)'),
    ('public.weekly_exceptional_pay_prepare_family_v1(jsonb)'),
    ('public.weekly_exceptional_pay_publish_next_v1(jsonb)'),
    ('public.weekly_exceptional_pay_read_c1_request_v1(jsonb)'),
    ('public.weekly_exceptional_pay_record_c1_checkpoint_v1(jsonb)'),
    ('public.weekly_exceptional_pay_record_c1_recovery_v1(jsonb)'),
    ('public.weekly_exceptional_pay_record_c1_unknown_v1(jsonb)'),
    ('public.weekly_exceptional_pay_stage_c1_request_v1(jsonb)'),
    ('public.weekly_exceptional_pay_wait_atomic_v1(jsonb)'),
    ('public.weekly_source_audit_guard_refusal_record_v1(jsonb)'),
    ('public.weekly_source_automatic_reconcile_atomic_v1(jsonb)'),
    ('public.weekly_source_candidate_app_draft_save_atomic_v1(uuid,text,uuid,jsonb,timestamp with time zone)'),
    ('public.weekly_source_candidate_app_request_get_v1(uuid,text,uuid,timestamp with time zone)'),
    ('public.weekly_source_candidate_app_submit_atomic_v1(uuid,text,uuid,jsonb,timestamp with time zone)'),
    ('public.weekly_source_candidate_check_materialise_atomic_v1(jsonb,timestamp with time zone)'),
    ('public.weekly_source_candidate_hours_evidence_v1(jsonb)'),
    ('public.weekly_source_candidate_hours_push_v1(jsonb)'),
    ('public.weekly_source_candidate_query_get_v1(jsonb)'),
    ('public.weekly_source_candidate_reminder_atomic_v1(jsonb)'),
    ('public.weekly_source_candidate_response_submit_atomic_v1(jsonb)'),
    ('public.weekly_source_candidate_self_submit_atomic_v1(uuid,text,jsonb,timestamp with time zone)'),
    ('public.weekly_source_charge_accept_atomic_v1(jsonb)'),
    ('public.weekly_source_client_cycle_resolve_atomic_v1(jsonb)'),
    ('public.weekly_source_client_settings_get_v1(jsonb)'),
    ('public.weekly_source_client_settings_save_atomic_v1(jsonb)'),
    ('public.weekly_source_combined_finalise_workspace_v1(jsonb)'),
    ('public.weekly_source_combined_review_workspace_v1(jsonb)'),
    ('public.weekly_source_completed_pack_copy_commit_atomic_v1(jsonb)'),
    ('public.weekly_source_completed_pack_copy_due_list_v1(jsonb)'),
    ('public.weekly_source_completed_pack_copy_status_sync_v1(jsonb)'),
    ('public.weekly_source_contract_settings_get_v1(jsonb)'),
    ('public.weekly_source_contract_settings_save_atomic_v1(jsonb)'),
    ('public.weekly_source_correct_final_apply_atomic_v1(jsonb)'),
    ('public.weekly_source_correct_final_cancel_atomic_v1(jsonb)'),
    ('public.weekly_source_correct_final_open_atomic_v1(jsonb)'),
    ('public.weekly_source_correct_final_prepare_atomic_v1(jsonb)'),
    ('public.weekly_source_correct_final_review_atomic_v1(jsonb)'),
    ('public.weekly_source_external_publication_arrival_v1(jsonb)'),
    ('public.weekly_source_external_publication_pending_inputs_v1(jsonb)'),
    ('public.weekly_source_finalisation_pay_open_atomic_v1(jsonb)'),
    ('public.weekly_source_finalisation_pay_task_finish_atomic_v1(jsonb)'),
    ('public.weekly_source_finalisation_pay_task_recover_atomic_v1(jsonb)'),
    ('public.weekly_source_finalisation_pay_task_start_atomic_v1(jsonb)'),
    ('public.weekly_source_finalisation_pay_task_unknown_atomic_v1(jsonb)'),
    ('public.weekly_source_finalise_atomic_v1(jsonb)'),
    ('public.weekly_source_first_authorisation_withdraw_available_v1(uuid)'),
    ('public.weekly_source_first_authorisation_withdraw_request_v1(jsonb)'),
    ('public.weekly_source_first_authorisation_withdraw_v1(uuid,uuid,text,uuid)'),
    ('public.weekly_source_first_authorise_v1(uuid,uuid,text,uuid)'),
    ('public.weekly_source_global_settings_get_v1(jsonb)'),
    ('public.weekly_source_global_settings_save_atomic_v1(jsonb)'),
    ('public.weekly_source_group_rule_assign_atomic_v1(jsonb)'),
    ('public.weekly_source_group_rule_feature_set_atomic_v1(jsonb)'),
    ('public.weekly_source_guard_refusal_record_after_rollback_v1(jsonb)'),
    ('public.weekly_source_import_prepare_atomic_v1(jsonb)'),
    ('public.weekly_source_invoice_admit_atomic_v1(jsonb)'),
    ('public.weekly_source_invoice_batch_admit_atomic_v1(jsonb)'),
    ('public.weekly_source_invoice_batch_candidates_v1(jsonb)'),
    ('public.weekly_source_invoice_edit_context_v1(jsonb)'),
    ('public.weekly_source_invoice_evidence_v1(jsonb)'),
    ('public.weekly_source_invoice_hold_release_atomic_v1(jsonb)'),
    ('public.weekly_source_invoice_move_atomic_v1(jsonb)'),
    ('public.weekly_source_invoice_report_rows_v1(jsonb)'),
    ('public.weekly_source_later_change_decide_atomic_v1(jsonb)'),
    ('public.weekly_source_manager_review_get_v1(jsonb)'),
    ('public.weekly_source_manager_review_respond_atomic_v1(jsonb)'),
    ('public.weekly_source_manager_route_prepare_atomic_v1(jsonb)'),
    ('public.weekly_source_manual_review_open_v1(jsonb)'),
    ('public.weekly_source_manual_review_resolve_v1(jsonb)'),
    ('public.weekly_source_message_dispatch_claim_v1(jsonb)'),
    ('public.weekly_source_message_dispatch_result_atomic_v1(jsonb)'),
    ('public.weekly_source_message_dispatch_snapshot_failure_atomic_v1(jsonb)'),
    ('public.weekly_source_message_dispatch_submission_start_atomic_v1(jsonb)'),
    ('public.weekly_source_message_dispatch_target_claim_v1(jsonb)'),
    ('public.weekly_source_message_dispatch_target_result_atomic_v1(jsonb)'),
    ('public.weekly_source_message_dispatch_target_start_atomic_v1(jsonb)'),
    ('public.weekly_source_message_render_due_list_v1(jsonb)'),
    ('public.weekly_source_message_render_input_v1(jsonb)'),
    ('public.weekly_source_message_render_stage_atomic_v1(jsonb)'),
    ('public.weekly_source_message_targets_register_atomic_v1(jsonb)'),
    ('public.weekly_source_mode_a_dispatch_atomic_v1(jsonb)'),
    ('public.weekly_source_mode_a_reference_apply_atomic_v1(jsonb)'),
    ('public.weekly_source_nhsp_report_scope_resolve_atomic_v1(jsonb)'),
    ('public.weekly_source_no_shifts_attest_atomic_v1(jsonb)'),
    ('public.weekly_source_office_authorise_scope_v1(jsonb)'),
    ('public.weekly_source_office_bulk_query_action_atomic_v1(jsonb)'),
    ('public.weekly_source_office_notification_ack_atomic_v1(jsonb)'),
    ('public.weekly_source_office_notifications_list_v1(jsonb)'),
    ('public.weekly_source_office_recheck_begin_v1(jsonb)'),
    ('public.weekly_source_office_summary_rows_v1(jsonb)'),
    ('public.weekly_source_office_timesheet_presentation_v1(jsonb)'),
    ('public.weekly_source_office_workspace_v1(jsonb)'),
    ('public.weekly_source_ordinary_pay_projection_apply_atomic_v1(jsonb)'),
    ('public.weekly_source_pending_entitlement_bundle_reopen_v1(uuid,text,uuid)'),
    ('public.weekly_source_pending_entitlement_release_apply_v1(jsonb)'),
    ('public.weekly_source_pending_entitlement_release_claim_page_v1(jsonb)'),
    ('public.weekly_source_pending_entitlement_release_record_failure_v1(jsonb)'),
    ('public.weekly_source_projection_begin_atomic_v1(jsonb)'),
    ('public.weekly_source_projection_publish_atomic_v1(jsonb)'),
    ('public.weekly_source_projection_rows_apply_atomic_v1(uuid,uuid,jsonb)'),
    ('public.weekly_source_protect_all_signed_atomic_v1(jsonb)'),
    ('public.weekly_source_protected_editor_context_v1(jsonb)'),
    ('public.weekly_source_protected_editor_prepare_v1(jsonb)'),
    ('public.weekly_source_query_accept_system_hours_atomic_v1(jsonb)'),
    ('public.weekly_source_query_ask_candidate_atomic_v1(jsonb)'),
    ('public.weekly_source_query_scheduler_tick_v1(jsonb)'),
    ('public.weekly_source_query_send_manager_now_atomic_v1(jsonb)'),
    ('public.weekly_source_query_sync_atomic_v1(jsonb)'),
    ('public.weekly_source_report_history_v1(jsonb)'),
    ('public.weekly_source_rule_set_version_save_atomic_v1(jsonb)'),
    ('public.weekly_source_rule_sets_get_v1(jsonb)'),
    ('public.weekly_source_self_bill_report_scope_resolve_atomic_v1(jsonb)'),
    ('public.weekly_source_source_group_save_atomic_v1(jsonb)'),
    ('public.weekly_source_source_groups_get_v1(jsonb)'),
    ('public.weekly_source_target_managed_root_prepare_atomic_v1(jsonb)'),
    ('public.weekly_source_timesheet_audit_chronology_v1(jsonb)'),
    ('public.weekly_source_timesheet_hours_export_v1(jsonb)'),
    ('public.weekly_source_timesheet_lineage_ensure_atomic_v1(uuid,uuid)'),
    ('public.weekly_source_timesheet_submission_complete_atomic_v1(jsonb)'),
    ('public.weekly_source_timesheet_submission_request_start_atomic_v1(jsonb)'),
    ('public.weekly_source_upload_abort_atomic_v1(jsonb)'),
    ('public.weekly_source_upload_attempt_record_atomic_v1(jsonb)'),
    ('public.weekly_source_upload_context_v1(jsonb)'),
    ('public.weekly_source_upload_detail_v1(jsonb)'),
    ('public.weekly_source_upload_seal_atomic_v1(jsonb)'),
    ('public.weekly_source_upload_stage_begin_atomic_v1(jsonb)'),
    ('public.weekly_source_upload_stage_rows_atomic_v1(jsonb)'),
    ('public.weekly_source_workspace_scopes_v1(jsonb)');
$function$;

create or replace function private._weekly_source_immutable_record_guard_v1()
returns trigger
language plpgsql
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
begin
  raise exception 'WEEKLY_SOURCE_IMMUTABLE_RECORD'
    using errcode='55000',
          detail=pg_catalog.jsonb_build_object(
            'reason_code','WEEKLY_SOURCE_IMMUTABLE_RECORD',
            'table_name',tg_table_name,
            'operation',tg_op
          )::text;
end;
$function$;

create or replace function private._weekly_source_immutable_fact_guard_v1()
returns trigger
language plpgsql
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_old_facts jsonb;
  v_new_facts jsonb;
  v_index integer;
begin
  if tg_op='DELETE' then
    raise exception 'WEEKLY_SOURCE_IMMUTABLE_RECORD'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'reason_code','WEEKLY_SOURCE_IMMUTABLE_RECORD',
              'table_name',tg_table_name,
              'operation',tg_op
            )::text;
  end if;

  v_old_facts:=pg_catalog.to_jsonb(old);
  v_new_facts:=pg_catalog.to_jsonb(new);
  if tg_nargs>0 then
    for v_index in 0..tg_nargs-1 loop
      v_old_facts:=v_old_facts-tg_argv[v_index];
      v_new_facts:=v_new_facts-tg_argv[v_index];
    end loop;
  end if;

  if v_old_facts is distinct from v_new_facts then
    raise exception 'WEEKLY_SOURCE_IMMUTABLE_FACT'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'reason_code','WEEKLY_SOURCE_IMMUTABLE_FACT',
              'table_name',tg_table_name,
              'operation',tg_op
            )::text;
  end if;

  return new;
end;
$function$;

alter function private._weekly_source_acl_table_contract_v1() owner to postgres;
alter function private._weekly_source_acl_lifecycle_column_contract_v1() owner to postgres;
alter function private._weekly_source_acl_private_helper_contract_v1() owner to postgres;
alter function private._weekly_source_acl_service_rpc_contract_v1() owner to postgres;
alter function private._weekly_source_immutable_record_guard_v1() owner to postgres;
alter function private._weekly_source_immutable_fact_guard_v1() owner to postgres;
revoke all on function private._weekly_source_acl_table_contract_v1()
  from public,anon,authenticated,service_role;
revoke all on function private._weekly_source_acl_lifecycle_column_contract_v1()
  from public,anon,authenticated,service_role;
revoke all on function private._weekly_source_acl_private_helper_contract_v1()
  from public,anon,authenticated,service_role;
revoke all on function private._weekly_source_acl_service_rpc_contract_v1()
  from public,anon,authenticated,service_role;
revoke all on function private._weekly_source_immutable_record_guard_v1()
  from public,anon,authenticated,service_role;
revoke all on function private._weekly_source_immutable_fact_guard_v1()
  from public,anon,authenticated,service_role;

do $weekly_source_acl$
declare
  v_contract_count integer;
  v_distinct_contract_count integer;
  v_table record;
  v_unexpected text;
  v_lifecycle_columns text;
begin
  select count(*),count(distinct table_name)
    into v_contract_count,v_distinct_contract_count
  from private._weekly_source_acl_table_contract_v1();

  if v_contract_count<>105
     or v_distinct_contract_count<>105 then
    raise exception 'WEEKLY_SOURCE_ACL_CONTRACT_INVALID'
      using errcode='55000',
            detail='The table ACL contract must contain exactly 105 unique relations.';
  end if;

  -- A private helper is never part of the granted RPC surface. Listing one in
  -- the service contract would make the ACL verifier's routine sweep fail,
  -- because the closure below revokes execute from service_role for every
  -- Weekly Source routine and re-grants only the service list.
  if exists(
    select 1
    from private._weekly_source_acl_private_helper_contract_v1() helper
    join private._weekly_source_acl_service_rpc_contract_v1() service
      on service.function_signature=helper.function_signature
  ) then
    raise exception 'WEEKLY_SOURCE_ACL_PRIVATE_HELPER_IN_SERVICE_CONTRACT'
      using errcode='55000',
            detail='A registered private helper must not appear in the service RPC contract.';
  end if;

  if exists(
    select 1
    from private._weekly_source_acl_private_helper_contract_v1()
    group by function_signature
    having count(*)<>1
  ) then
    raise exception 'WEEKLY_SOURCE_ACL_PRIVATE_HELPER_DUPLICATE' using errcode='55000';
  end if;

  if exists(
    select 1
    from private._weekly_source_acl_table_contract_v1()
    where record_class not in (
      'IMMUTABLE_APPEND_ONLY',
      'IMMUTABLE_FACTS_WITH_LIFECYCLE',
      'STATEFUL_SERVER_OWNED'
    )
  ) then
    raise exception 'WEEKLY_SOURCE_ACL_CLASS_INVALID' using errcode='55000';
  end if;

  if (
    select count(*)
    from private._weekly_source_acl_lifecycle_column_contract_v1()
  )<>67
  or (
    select count(distinct (table_name,column_name))
    from private._weekly_source_acl_lifecycle_column_contract_v1()
  )<>67 then
    -- 65 + the two head supersession columns of HANDOVER 2 round-5 ruling A3
    -- step 3 (package WP-07c).
    raise exception 'WEEKLY_SOURCE_ACL_LIFECYCLE_CONTRACT_INVALID'
      using errcode='55000',detail='The lifecycle contract must contain exactly 67 unique table/column pairs.';
  end if;

  if exists(
    select 1
    from private._weekly_source_acl_lifecycle_column_contract_v1() allowed
    left join private._weekly_source_acl_table_contract_v1() expected
      on expected.table_name=allowed.table_name
    where expected.record_class is distinct from 'IMMUTABLE_FACTS_WITH_LIFECYCLE'
  )
  or exists(
    select 1
    from private._weekly_source_acl_table_contract_v1() expected
    where expected.record_class='IMMUTABLE_FACTS_WITH_LIFECYCLE'
      and not exists(
        select 1
        from private._weekly_source_acl_lifecycle_column_contract_v1() allowed
        where allowed.table_name=expected.table_name
      )
  ) then
    raise exception 'WEEKLY_SOURCE_ACL_LIFECYCLE_CLASS_MISMATCH' using errcode='55000';
  end if;

  if exists(
    select 1
    from private._weekly_source_acl_lifecycle_column_contract_v1() allowed
    left join pg_catalog.pg_attribute a
      on a.attrelid=pg_catalog.to_regclass(pg_catalog.format('public.%I',allowed.table_name))
     and a.attname=allowed.column_name
     and a.attnum>0
     and not a.attisdropped
    where a.attrelid is null
  )
  or exists(
    select 1
    from private._weekly_source_acl_lifecycle_column_contract_v1() allowed
    group by allowed.table_name
    having pg_catalog.min(allowed.column_ordinal)<>1
       or pg_catalog.max(allowed.column_ordinal)<>count(*)
       or count(distinct allowed.column_ordinal)<>count(*)
  ) then
    raise exception 'WEEKLY_SOURCE_ACL_LIFECYCLE_COLUMN_INVALID' using errcode='55000';
  end if;

  select pg_catalog.string_agg(c.table_name,',' order by c.table_name)
    into v_unexpected
  from private._weekly_source_acl_table_contract_v1() c
  where pg_catalog.to_regclass(pg_catalog.format('public.%I',c.table_name)) is null;
  if v_unexpected is not null then
    raise exception 'WEEKLY_SOURCE_ACL_TABLE_MISSING'
      using errcode='55000',detail=v_unexpected;
  end if;

  select pg_catalog.string_agg(c.relname,',' order by c.relname)
    into v_unexpected
  from pg_catalog.pg_class c
  join pg_catalog.pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public'
    and c.relkind in ('r','p')
    and (
      pg_catalog.left(c.relname,7)='weekly_'
      or c.relname='office_action_notifications'
    )
    and not exists(
      select 1
      from private._weekly_source_acl_table_contract_v1() expected
      where expected.table_name=c.relname
    );
  if v_unexpected is not null then
    raise exception 'WEEKLY_SOURCE_ACL_TABLE_UNCLASSIFIED'
      using errcode='55000',detail=v_unexpected;
  end if;

  for v_table in
    select c.table_name,c.record_class
    from private._weekly_source_acl_table_contract_v1() c
    order by c.table_name
  loop
    execute pg_catalog.format('alter table public.%I enable row level security',v_table.table_name);
    execute pg_catalog.format('alter table public.%I force row level security',v_table.table_name);
    execute pg_catalog.format(
      'revoke all on table public.%I from public,anon,authenticated,service_role',
      v_table.table_name
    );
    if exists(select 1 from pg_catalog.pg_roles where rolname='authenticator') then
      execute pg_catalog.format('revoke all on table public.%I from authenticator',v_table.table_name);
    end if;
    if exists(select 1 from pg_catalog.pg_roles where rolname='supabase_admin') then
      execute pg_catalog.format('revoke all on table public.%I from supabase_admin',v_table.table_name);
    end if;

    if exists(
      select 1
      from pg_catalog.pg_class c
      where c.oid=pg_catalog.to_regclass(pg_catalog.format('public.%I',v_table.table_name))
        and (
          c.relowner<>(current_user::pg_catalog.regrole)::oid
          or exists(
            select 1
            from pg_catalog.aclexplode(
              coalesce(c.relacl,pg_catalog.acldefault('r',c.relowner))
            ) acl
            where acl.grantee<>c.relowner
          )
          or exists(
            select 1
            from pg_catalog.pg_attribute a
            cross join lateral pg_catalog.aclexplode(a.attacl) acl
            where a.attrelid=c.oid
              and a.attnum>0
              and not a.attisdropped
              and acl.grantee<>c.relowner
          )
        )
    ) then
      raise exception 'WEEKLY_SOURCE_ACL_UNEXPECTED_TABLE_GRANTEE: %',v_table.table_name
        using errcode='55000';
    end if;

    if exists(
      select 1
      from pg_catalog.pg_policy p
      where p.polrelid=pg_catalog.to_regclass(pg_catalog.format('public.%I',v_table.table_name))
        and p.polname<>'cloudtms_miget_service_owner_all'
    ) then
      raise exception 'WEEKLY_SOURCE_ACL_UNEXPECTED_POLICY: %',v_table.table_name
        using errcode='55000';
    end if;
    execute pg_catalog.format(
      'drop policy if exists cloudtms_miget_service_owner_all on public.%I',
      v_table.table_name
    );
    execute pg_catalog.format(
      'create policy cloudtms_miget_service_owner_all on public.%I for all to %I, service_role using (true) with check (true)',
      v_table.table_name,current_user
    );

    execute pg_catalog.format(
      'drop trigger if exists weekly_source_immutable_record_guard on public.%I',
      v_table.table_name
    );
    execute pg_catalog.format(
      'drop trigger if exists weekly_source_immutable_fact_guard on public.%I',
      v_table.table_name
    );
    execute pg_catalog.format(
      'drop trigger if exists weekly_source_immutable_truncate_guard on public.%I',
      v_table.table_name
    );
    if v_table.record_class='IMMUTABLE_APPEND_ONLY' then
      execute pg_catalog.format(
        'create trigger weekly_source_immutable_record_guard before update or delete on public.%I for each row execute function private._weekly_source_immutable_record_guard_v1()',
        v_table.table_name
      );
    elsif v_table.record_class='IMMUTABLE_FACTS_WITH_LIFECYCLE' then
      select pg_catalog.string_agg(pg_catalog.format('%L',allowed.column_name),',' order by allowed.column_ordinal)
        into v_lifecycle_columns
      from private._weekly_source_acl_lifecycle_column_contract_v1() allowed
      where allowed.table_name=v_table.table_name;
      execute pg_catalog.format(
        'create trigger weekly_source_immutable_fact_guard before update or delete on public.%I for each row execute function private._weekly_source_immutable_fact_guard_v1(%s)',
        v_table.table_name,v_lifecycle_columns
      );
    end if;
    if v_table.record_class in ('IMMUTABLE_APPEND_ONLY','IMMUTABLE_FACTS_WITH_LIFECYCLE') then
      execute pg_catalog.format(
        'create trigger weekly_source_immutable_truncate_guard before truncate on public.%I for each statement execute function private._weekly_source_immutable_record_guard_v1()',
        v_table.table_name
      );
    end if;
  end loop;
end;
$weekly_source_acl$;

-- Reassert the exact API routine surface for every Plan 6 routine that exists
-- when this closure runs. Each later owning repeatable must make the same
-- grant decision; the final verifier rejects any later drift.
do $weekly_source_function_acl$
declare
  v_function record;
  v_signature text;
begin
  for v_function in
    select p.oid,p.proowner,n.nspname,p.proname,
           pg_catalog.pg_get_function_identity_arguments(p.oid) as identity_arguments
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where n.nspname in ('public','private')
      and (
        p.proname like 'weekly\_source\_%' escape '\'
        or p.proname like '\_weekly\_source\_%' escape '\'
        or p.proname like '\_ctms\_weekly\_source\_%' escape '\'
        or p.proname like 'weekly\_exceptional\_%' escape '\'
      )
  loop
    execute pg_catalog.format(
      'revoke all on function %I.%I(%s) from public,anon,authenticated,service_role',
      v_function.nspname,v_function.proname,v_function.identity_arguments
    );
    if exists(select 1 from pg_catalog.pg_roles where rolname='authenticator') then
      execute pg_catalog.format(
        'revoke all on function %I.%I(%s) from authenticator',
        v_function.nspname,v_function.proname,v_function.identity_arguments
      );
    end if;
    if exists(select 1 from pg_catalog.pg_roles where rolname='supabase_admin') then
      execute pg_catalog.format(
        'revoke all on function %I.%I(%s) from supabase_admin',
        v_function.nspname,v_function.proname,v_function.identity_arguments
      );
    end if;
    if v_function.proowner<>(current_user::pg_catalog.regrole)::oid
       or exists(
         select 1
         from pg_catalog.pg_proc p
         cross join lateral pg_catalog.aclexplode(
           coalesce(p.proacl,pg_catalog.acldefault('f',p.proowner))
         ) acl
         where p.oid=v_function.oid
           and acl.grantee<>p.proowner
       ) then
      raise exception 'WEEKLY_SOURCE_ACL_UNEXPECTED_FUNCTION_GRANTEE: %.%(%)',
        v_function.nspname,v_function.proname,v_function.identity_arguments
        using errcode='55000';
    end if;
  end loop;

  for v_signature in
    select function_signature
    from private._weekly_source_acl_service_rpc_contract_v1()
  loop
    if pg_catalog.to_regprocedure(v_signature) is not null then
      execute 'grant execute on function '||v_signature||' to service_role';
    end if;
  end loop;
end;
$weekly_source_function_acl$;

notify pgrst, 'reload schema';

commit;
