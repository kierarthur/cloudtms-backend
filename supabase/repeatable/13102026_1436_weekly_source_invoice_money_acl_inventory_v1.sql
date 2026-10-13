-- F5 inventory successor based on root reviewed 102-table source.
-- Root reasserts the FINAL combined ACL gate after all report/PP/F8 owners.
\set ON_ERROR_STOP on
begin;

create or replace function private._weekly_source_acl_table_contract_v1()
returns table(table_name text, record_class text)
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  values
    ('weekly_source_groups','STATEFUL_SERVER_OWNED'),
    ('weekly_source_rule_sets','STATEFUL_SERVER_OWNED'),
    ('weekly_source_rule_set_versions','IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_group_rule_assignments','STATEFUL_SERVER_OWNED'),
    ('weekly_source_group_clients','STATEFUL_SERVER_OWNED'),
    ('weekly_source_client_policies','STATEFUL_SERVER_OWNED'),
    ('weekly_source_contract_policies','STATEFUL_SERVER_OWNED'),
    ('weekly_source_format_profiles','STATEFUL_SERVER_OWNED'),
    ('weekly_source_global_settings','STATEFUL_SERVER_OWNED'),
    ('weekly_source_cycles','STATEFUL_SERVER_OWNED'),
    ('weekly_source_report_scopes','STATEFUL_SERVER_OWNED'),
    ('weekly_final_source_correction_sessions','STATEFUL_SERVER_OWNED'),
    ('weekly_final_source_correction_root_impacts','IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_uploads','STATEFUL_SERVER_OWNED'),
    ('weekly_source_upload_attempts','IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_upload_supersessions','IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_physical_rows','IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_upload_rows','IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_money_cell_evidence','IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_expense_cell_evidence','IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_projection_publications','STATEFUL_SERVER_OWNED'),
    ('weekly_source_row_qualification_evidence','IMMUTABLE_APPEND_ONLY'),
    ('weekly_work_events','IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_contract_qualification_observations','IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_row_resolutions','IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_row_economic_snapshots','IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_row_expense_policy_snapshots','IMMUTABLE_APPEND_ONLY'),
    -- Decision D8: the per-source-row binding keeps only binding-time facts.
    -- The authorisation record, and every column that moves after it, live on
    -- public.weekly_source_root_authorisations below.
    ('weekly_source_row_timesheet_lineages','IMMUTABLE_APPEND_ONLY'),
    ('weekly_work_event_source_links','IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_charge_checks','IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_charge_acceptances','IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_final_revisions','STATEFUL_SERVER_OWNED'),
    ('weekly_source_finalisation_pay_runs','STATEFUL_SERVER_OWNED'),
    ('weekly_source_finalisation_pay_tasks','STATEFUL_SERVER_OWNED'),
    ('weekly_source_nhsp_backing_reports','IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_client_cycle_completions','STATEFUL_SERVER_OWNED'),
    ('weekly_source_final_snapshot_lines','IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_state_transitions','IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_source_ordinary_pay_projection_receipts','IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_billing_movements','IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_source_client_manifests','STATEFUL_SERVER_OWNED'),
    ('weekly_source_manifest_movements','IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_invoice_presentation_lines','IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_invoice_line_bindings','STATEFUL_SERVER_OWNED'),
    ('weekly_source_invoice_placements','STATEFUL_SERVER_OWNED'),
    ('weekly_expense_authority_generations','STATEFUL_SERVER_OWNED'),
    ('weekly_source_expense_materialisations','IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_source_expense_pay_materialisations','IMMUTABLE_APPEND_ONLY'),
    ('weekly_discrepancy_incidents','STATEFUL_SERVER_OWNED'),
    ('weekly_issue_comparison_revisions','IMMUTABLE_APPEND_ONLY'),
    ('weekly_route_activations','STATEFUL_SERVER_OWNED'),
    ('weekly_candidate_cohorts','STATEFUL_SERVER_OWNED'),
    ('weekly_candidate_outreach_generations','STATEFUL_SERVER_OWNED'),
    ('weekly_candidate_outreach_memberships','STATEFUL_SERVER_OWNED'),
    ('weekly_timesheet_submission_requests','STATEFUL_SERVER_OWNED'),
    ('weekly_timesheet_submission_request_memberships','STATEFUL_SERVER_OWNED'),
    ('weekly_candidate_response_drafts','STATEFUL_SERVER_OWNED'),
    ('weekly_candidate_response_draft_items','STATEFUL_SERVER_OWNED'),
    ('weekly_candidate_app_mutation_receipts','IMMUTABLE_APPEND_ONLY'),
    ('weekly_candidate_message_notifications','IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_discrepancy_events','IMMUTABLE_APPEND_ONLY'),
    ('weekly_manager_recipient_routes','STATEFUL_SERVER_OWNED'),
    ('weekly_manager_recipient_generations','STATEFUL_SERVER_OWNED'),
    ('weekly_manager_recipient_memberships','STATEFUL_SERVER_OWNED'),
    ('weekly_manager_cohort_due_events','STATEFUL_SERVER_OWNED'),
    ('weekly_message_intents','STATEFUL_SERVER_OWNED'),
    ('weekly_message_renders','IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_message_dispatch_commands','STATEFUL_SERVER_OWNED'),
    ('weekly_message_dispatch_targets','STATEFUL_SERVER_OWNED'),
    ('weekly_message_target_attempts','IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_message_delivery_failures','IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_message_provider_attempts','IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_manager_review_batches','STATEFUL_SERVER_OWNED'),
    ('weekly_manager_review_items','STATEFUL_SERVER_OWNED'),
    ('weekly_manager_route_preparations','IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_manager_route_receipts','IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_completed_pack_copy_events','IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('office_action_notifications','IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_timesheet_authority_resolutions','IMMUTABLE_APPEND_ONLY'),
    ('weekly_timesheet_source_comparisons','IMMUTABLE_APPEND_ONLY'),
    ('weekly_timesheet_reference_apply_operations','STATEFUL_SERVER_OWNED'),
    ('weekly_timesheet_reference_apply_items','IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_exceptional_pay_target_families','STATEFUL_SERVER_OWNED'),
    ('weekly_exceptional_payment_approvals','IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_exceptional_pay_family_events','IMMUTABLE_APPEND_ONLY'),
    ('weekly_exceptional_pay_generations','STATEFUL_SERVER_OWNED'),
    ('weekly_exceptional_pay_target_events','IMMUTABLE_APPEND_ONLY'),
    ('weekly_exceptional_orchestration_runs','STATEFUL_SERVER_OWNED'),
    ('weekly_exceptional_orchestration_steps','STATEFUL_SERVER_OWNED'),
    ('weekly_exceptional_pending_reconciliation_targets','STATEFUL_SERVER_OWNED'),
    ('weekly_exceptional_payment_events','IMMUTABLE_APPEND_ONLY'),
    ('weekly_exceptional_c1_publication_requests','STATEFUL_SERVER_OWNED'),
    ('weekly_exceptional_c1_publication_checkpoints','IMMUTABLE_APPEND_ONLY'),
    ('weekly_exceptional_c1_unknown_outcomes','IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_exceptional_c1_source_records','IMMUTABLE_APPEND_ONLY'),
    ('weekly_exceptional_c1_source_parts','IMMUTABLE_APPEND_ONLY'),
    ('weekly_exceptional_c1_component_records','IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_entitlement_decision_bundles','IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_source_entitlement_heads','IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_source_entitlement_head_components','IMMUTABLE_APPEND_ONLY'),
    ('weekly_source_pending_entitlement_bundles','IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    -- Decision D8: the per-root authorisation record. Family, physical root,
    -- version, signature, generation, actor and time are frozen when written;
    -- only the head pointer and the two withdrawal marks may move, and a
    -- withdrawal is permanent (proof/36 section 6).
    ('weekly_source_root_authorisations','IMMUTABLE_FACTS_WITH_LIFECYCLE'),
    ('weekly_source_invoice_hold_releases','IMMUTABLE_APPEND_ONLY');
$function$;

create or replace function private._weekly_source_acl_private_helper_contract_v1()
returns table(function_signature text)
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  values
    -- proof/32 section 9: the distinct-array proof a CHECK cannot express
    -- (it cannot contain a subquery), and the BEFORE UPDATE OR DELETE /
    -- BEFORE TRUNCATE guard that raises
    -- WEEKLY_SOURCE_PUBLICATION_RECEIPT_IMMUTABLE.
    ('private._weekly_source_rule_values_v1(jsonb)'),
    ('private._weekly_source_rule_group_context_v1(uuid,date,date,jsonb,boolean)'),
    ('private.weekly_source_is_import_authoritative_v1(uuid,uuid,date)'),
    ('private.weekly_source_effective_rule_set_v1(uuid,uuid)'),
    ('private.weekly_source_has_self_bill_rules_v1(uuid,uuid)'),
    ('private.weekly_source_agency_invoice_check_only_v1(uuid)'),
    ('private.weekly_source_signed_check_only_v1(uuid)'),
    ('private.weekly_source_configured_agency_invoice_v1(uuid,uuid,uuid,text,date)'),
    ('private.weekly_source_final_rule_version_guard_v1()'),
    ('private.weekly_source_authority_subset_v1(jsonb)'),
    ('private.weekly_source_final_value_vector_v1(jsonb)'),
    ('private.weekly_source_invoice_economics_fingerprint_v1(jsonb,text,numeric)'),
    ('private.weekly_source_snapshot_authority_subset_v1(uuid)'),
    ('private.weekly_source_snapshot_value_fingerprint_v1(uuid,jsonb)'),
    ('private.weekly_source_import_is_prepared_v1(uuid)'),
    ('private.weekly_source_issued_signed_query_evidence_v1(uuid,uuid)'),
    ('private.weekly_source_configured_upload_profile_v1(uuid,uuid,text,text)'),
    ('private.weekly_source_manager_due_event_covers_v1(uuid,uuid)'),
    ('private.weekly_source_candidate_generation_current_v1(uuid)'),
    ('private.weekly_source_waiting_requested_week_v1(uuid,uuid,uuid,uuid,date)'),
    ('private.weekly_source_uuid_array_is_distinct_v1(uuid[])'),
    ('private.weekly_source_entitlement_publication_receipt_immutable_v1()'),
    -- Gate 1 review fixes: the three constraint-trigger guards that enforce the
    -- head's root identity, its inventory digest and count, and its receipt
    -- digest (WB-005, proof/32 section 9).
    ('private.weekly_source_entitlement_head_root_identity_v1()'),
    ('private.weekly_source_entitlement_head_inventory_assert_v1()'),
    ('private.weekly_source_entitlement_head_receipt_assert_v1()'),
    -- 25 section 7 and 24 section 8: the two Contract-choice facts used by the
    -- upload context owner for schedule compatibility and the verified
    -- band/role tie-break.
    ('private.weekly_source_schedule_compatible_v1(jsonb,date)'),
    ('private.weekly_source_protected_final_source_context_v1(uuid,uuid,uuid)'),
    ('private.weekly_source_completed_rows_v1(uuid,uuid,uuid)'),
    ('private.weekly_source_protected_match_candidates_v1(uuid,uuid,uuid)'),
    ('private.weekly_source_query_resolved_decision_matches_v1(uuid,uuid,jsonb)'),
    ('private.weekly_source_protected_query_rows_v1(uuid,uuid,boolean)'),
    ('private.weekly_source_verified_role_band_match_v1(text,text,uuid,uuid,uuid,text,date)'),
    -- 24 section 9 step 2 (WP-37): durable work identity is a COMPATIBLE
    -- schedule, never the exact Actual start and end. This is the compatibility
    -- fact the projection owner and the upload context owner both read.
    ('private.weekly_source_work_event_schedule_compatible_v1(uuid,timestamp without time zone,timestamp without time zone)'),
    -- 24 section 9 Mode A: the role-type and operation-key facts of the
    -- dispatch owner. The two public Mode A entry points are service RPCs and
    -- are in the service contract, not here.
    ('private.weekly_source_mode_a_role_type_v1(text,text)'),
    ('private.weekly_source_mode_a_operation_key_v1(uuid,uuid)'),
    -- 24 section 9A (WP-37): the refusal record written when a physical
    -- Timesheet id in the comparison set is not the family's canonical current
    -- head, so a superseded head can never block the current one.
    ('private.weekly_source_mode_a_refuse_operation_v1(uuid,uuid,uuid,uuid,uuid,text)'),
    -- proof/32 section 6 and proof/34 sections 3, 6 and 7: the rotation
    -- authority set — the serial-gate mapping, the deadlock-free lock set,
    -- interface I-1, read-only root resolution, the integrity assert and the
    -- managed-root guard.
    ('private.weekly_source_candidate_serial_gate_v1(uuid,text,uuid,text)'),
    ('private.weekly_source_lock_family_rows_v1(uuid[],uuid)'),
    ('private.weekly_source_lock_and_resolve_families_v1(uuid,uuid[],text,uuid,text)'),
    ('private.weekly_source_resolve_root_identity_v1(uuid)'),
    ('private.weekly_source_root_integrity_assert_v1(uuid,text,integer)'),
    ('private.weekly_source_managed_root_guard_v1(uuid)'),
    -- proof/32 sections 6 to 9: the canonical encoder and digest of section 9,
    -- its input helpers, the uuid set algebra the move-set proofs use, the
    -- receipt reader, and interface I-4's two coordinator entry points. The
    -- core is also the single permitted Weekly Source direct caller of
    -- private.pay_workbench_scope_invalidate_v1, sealed separately in
    -- supabase/verification/08092026_0805_pay_workbench_scope_invalidate_pair_arrays_verification.sql.
    ('private.weekly_source_canonical_json_text_v1(jsonb)'),
    ('private.weekly_source_publication_request_digest_v1(jsonb)'),
    ('private.weekly_source_publication_scalar_v1(jsonb,text,text,integer,boolean)'),
    ('private.weekly_source_publication_require_keys_v1(jsonb,text[],text)'),
    ('private.weekly_source_publication_component_canonical_v1(jsonb,text)'),
    ('private.weekly_source_publication_request_canonical_v1(jsonb,text,uuid)'),
    ('private.weekly_source_uuid_set_union_v1(uuid[],uuid[])'),
    ('private.weekly_source_uuid_set_intersect_v1(uuid[],uuid[])'),
    ('private.weekly_source_uuid_set_difference_v1(uuid[],uuid[])'),
    ('private.weekly_source_uuid_set_equals_v1(uuid[],uuid[])'),
    ('private.weekly_source_publication_receipt_json_v1(uuid)'),
    ('private.weekly_source_entitlement_publish_core_v1(jsonb,text,jsonb,uuid,text,uuid,jsonb,jsonb)'),
    ('private.weekly_source_entitlement_publish_immediate_v1(jsonb)'),
    -- 24 sections 10 to 12 and proof/34 section 9: the source-aware invoice
    -- issue validator reached from both real entry points, its skippable-code
    -- and reason helpers, and the family resolver invoice lineage uses.
    ('private.weekly_source_invoice_issue_validate_v1(uuid)'),
    ('private.weekly_source_invoice_issue_skippable_code_v1(text)'),
    ('private.weekly_source_invoice_issue_skippable_reason_v1(text)'),
    ('private.weekly_source_invoice_issue_blockers_v1(uuid,text[])'),
    ('private.weekly_source_invoice_issue_reasons_v1(jsonb,text[])'),
    ('private.weekly_source_invoice_family_timesheet_ids_v1(uuid)'),
    -- Decision D8: the two constraint-trigger guards on the per-root
    -- authorisation record — the identity guard that ties family and version to
    -- the row's own root, and the withdrawal-once guard of proof/36 section 6.
    ('private.weekly_source_root_authorisation_identity_v1()'),
    ('private.weekly_source_root_authorisation_withdrawal_once_v1()'),
    -- Gate 2 (24 section 4, decision D9): the proposal composer's identity,
    -- component and request builders, interface I-7's effective inventory, the
    -- deterministic fail-closed family reader S8 needs, and the target-family
    -- root identity trigger.
    ('private.weekly_source_effective_inventory_v1(uuid)'),
    ('private.weekly_source_pay_query_scope_v1(uuid)'),
    ('private.weekly_source_pay_query_local_receipt_v1(uuid,uuid,uuid)'),
    ('private.weekly_source_pay_query_c1_receipt_v1(uuid,uuid,uuid)'),
    ('private.weekly_source_pay_query_next_receipt_v1(uuid,uuid,uuid)'),
    ('private.weekly_source_pay_query_gate_v1(uuid)'),
    ('private.weekly_source_pay_query_facts_v1(uuid)'),
    ('private.weekly_source_covered_hours_incident_v1(uuid)'),
    ('private.weekly_source_protected_contact_retire_v1(uuid,uuid)'),
    ('private.weekly_source_office_next_owner_v1()'),
    ('private.weekly_source_office_next_paid_evidence_page_v1(uuid,uuid,text,uuid,jsonb,integer)'),
    ('private.weekly_source_protected_saved_prepare_route_v1(jsonb,text,text)'),
    ('private.weekly_source_local_saved_status_v1(uuid,uuid,uuid)'),
    ('private.weekly_source_selected_source_witness_v1(uuid,uuid,uuid)'),
    ('private.weekly_source_accepted_removal_query_evidence_v1(uuid,uuid,uuid,uuid,uuid)'),
    ('private.weekly_source_invoice_duty_v1(uuid)'),
    ('private.weekly_source_approval_duty_v1(uuid)'),
    ('private.weekly_source_manual_review_command_immutable_v1()'),
    ('private.weekly_source_manual_review_final_rows_v2(uuid)'),
    ('private.weekly_source_manual_review_source_v2(uuid)'),
    ('private.weekly_source_manual_reviews_finalised_v1(uuid,uuid)'),
    ('private.weekly_source_operational_empty_v1(uuid,uuid)'),
    ('private.weekly_source_timesheet_category_v2(uuid)'),
    ('private.weekly_source_local_publication_origin_guard_v2()'),
    ('private.weekly_source_local_origin_canonical_v2(jsonb)'),
    ('private.weekly_source_local_publication_context_v2(uuid,uuid,uuid)'),
    ('private.weekly_source_entitlement_proposal_request_v2(uuid,jsonb,text,uuid,bigint,uuid,uuid,jsonb,text)'),
    ('private.weekly_source_inventory_approval_basis_v2(uuid,jsonb)'),
    ('private.weekly_source_head_origin_guard_v2()'),
    ('private.weekly_source_accepted_publication_action_immutable_v1()'),
    ('private.weekly_source_accepted_publication_context_v1(jsonb)'),
    ('private.weekly_source_accepted_publication_action_record_v1(jsonb,jsonb,jsonb)'),
    ('private.weekly_source_accepted_publication_action_verify_v1(uuid,uuid,bigint,bytea,uuid)'),
    ('private.weekly_source_initial_additional_components_v2(jsonb,jsonb,numeric,numeric,text)'),
    ('private.weekly_source_candidate_initial_approval_v2(uuid)'),
    ('private.weekly_source_candidate_head_hours_v2(uuid)'),
    ('private.weekly_source_candidate_saved_local_hours_v2(uuid)'),
    ('private.weekly_source_candidate_approved_clock_row_v2(jsonb,jsonb,text)'),
    ('private.weekly_source_retain_approved_additional_v2(uuid,jsonb)'),
    ('private.weekly_source_entitlement_derived_uuid_v1(text,text)'),
    ('private.weekly_source_entitlement_component_id_v1(text,text,text,text)'),
    ('private.weekly_source_entitlement_components_v1(jsonb,jsonb)'),
    ('private.weekly_source_entitlement_proposal_request_v1(uuid,uuid,text,uuid,bigint,uuid,uuid,jsonb,text)'),
    ('private.weekly_source_entitlement_proposal_record_v1(jsonb,uuid,uuid,date,uuid)'),
    ('private.weekly_source_target_family_for_root_v1(uuid)'),
    ('private.weekly_source_target_family_root_identity_v1()'),
    -- Stage 5 / PHD-017: server-only completed-Timesheet informational-copy
    -- eligibility. The three public entry points are service RPCs below; this
    -- helper remains owner-only and must never be callable through PostgREST.
    ('private._weekly_source_completed_pack_copy_eligibility_v1(uuid)'),
    -- WP-54: the two source-row admission rules the pack states and nothing
    -- enforced -- 14 section 4.1.3 with acceptance row NHSP-BR-006 (a cutoff
    -- may not predate a row's real Actual finish), and 03 section 7 with 14
    -- sections 4.2.8 and 4.1.7 (one person cannot work two intersecting
    -- intervals, and the report is refused as a whole). Both are read-only,
    -- raise or return, and are called by the upload seal; the overlap rule is
    -- called by the finalisation engine as well.
    ('private.weekly_source_cutoff_admission_assert_v1(uuid,text,timestamp with time zone)'),
    ('private.weekly_source_overlap_admission_assert_v1(uuid)'),
    ('private.weekly_source_invoice_source_amount_movement_valid_v1(uuid)'),
    ('private.weekly_source_source_amount_warnings_v1(uuid,uuid)'),
    ('private.weekly_source_billing_movement_hash_v1(jsonb)'),
    ('private.weekly_source_frozen_rule_values_v1(uuid)'),
    ('private.weekly_source_shift_on_self_bill_v1(uuid,uuid)'),
    ('private.weekly_source_invoice_only_vector_v1(text,bigint)'),
    ('private.weekly_source_invoice_line_amounts_v1(uuid,uuid)'),
    ('private.weekly_source_source_amount_acceptance_policy_fingerprint_v1()'),
    ('private.weekly_source_invoice_window_cycle_v1(uuid,uuid)'),
    ('private.weekly_source_invoice_correction_insert_v1(uuid,uuid,uuid)'),
    ('private.weekly_source_invoice_correction_unwind_v1(uuid,uuid,uuid)'),
    ('private.weekly_source_invoice_movement_money_v1(uuid,uuid)'),
    ('private.weekly_source_invoice_presentation_release_authority_v1(uuid[],uuid)'),
    ('private.weekly_source_invoice_presentation_source_hash_v1(uuid[],uuid)'),
    ('private.weekly_source_vat_rounding_movement_insert_v1(uuid,uuid,uuid)'),
    ('private.weekly_source_rebill_movement_insert_v1(uuid,uuid)'),
    ('private.weekly_source_invoice_placement_identity_guard_v1()'),
    ('private.weekly_source_final_upload_facts_v1(text,date,timestamp with time zone,timestamp with time zone,text,date,date,jsonb,date)'),
    ('private.weekly_source_final_upload_gate_v1(uuid)'),
    ('private.weekly_source_final_upload_assert_v1(uuid)'),
    ('private.weekly_source_rule_coverage_floor_v1(uuid,uuid)'),
    ('private.weekly_source_upload_coverage_context_v1(uuid,uuid)'),
    ('private.weekly_source_upload_coverage_floor_assert_v1(uuid,uuid,text,date)');
$function$;

create or replace function private._weekly_source_acl_service_rpc_contract_v1()
returns table(function_signature text)
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  values
    ('public.weekly_source_rule_sets_get_v1(jsonb)'),
    ('public.weekly_source_rule_set_version_save_atomic_v1(jsonb)'),
    ('public.weekly_source_group_rule_assign_atomic_v1(jsonb)'),
    ('public.weekly_source_group_rule_feature_set_atomic_v1(jsonb)'),
    ('private.weekly_source_pay_actor_allowed_v1(uuid)'),
    ('public.weekly_source_import_prepare_atomic_v1(jsonb)'),
    ('public.weekly_source_office_recheck_begin_v1(jsonb)'),
    -- Report-first completed History is a service-only read. Its grant is
    -- intentionally unavailable to browser roles.
    ('public.weekly_source_report_history_v1(jsonb)'),
    ('public.weekly_source_manual_review_open_v1(jsonb)'),
    ('public.weekly_source_manual_review_resolve_v1(jsonb)'),
    ('public.weekly_exceptional_pay_complete_local_v1(jsonb)'),
    ('private.weekly_source_local_preauthorisation_write_allowed_v1(uuid,jsonb)'),
    ('private.weekly_source_invoice_batch_rows_v1(jsonb,jsonb)'),
    ('private.weekly_source_invoice_batch_snapshot_v1()'),
    ('private.weekly_source_summary_pay_delayed_v1(uuid,uuid,uuid,date)'),
    -- Narrow mail-outbox claim validator.  The generic service claimant calls
    -- it without direct table authority, so its deliberate service_role grant
    -- belongs in this exact allowlist.
    ('private._weekly_source_completed_pack_copy_claim_event_valid_v1(public.mail_outbox)'),
    -- Plan 6.2 final seals pass (WP-15d): sixteen routines that carry an
    -- installed service_role EXECUTE grant were absent from this allowlist,
    -- which is the whole of 'ACL/owner/search-path verification failed for
    -- 16 routine(s)'. Each was confirmed on a full NEW build to be
    -- SECURITY DEFINER, owned by postgres, with a fixed search_path and
    -- exactly one foreign grant (service_role EXECUTE) and none for anon or
    -- authenticated. Owners: WP-03, WP-06d, WP-07, WP-07c, WP-08b, WP-14.
    ('private.weekly_source_managed_root_guard_decision_v1(uuid)'),
    ('private.weekly_source_office_authority_v1(uuid,text,uuid,uuid,date)'),
    ('public.weekly_exceptional_pay_action_context_v1(jsonb)'),
    ('public.weekly_exceptional_pay_action_publication_status_v1(jsonb)'),
    -- RESET protected-pay receipts/publication: exact service-only authority.
    ('public.weekly_exceptional_pay_next_publication_status_v1(jsonb)'),
    ('public.weekly_exceptional_pay_publish_next_v1(jsonb)'),
    ('public.weekly_exceptional_pay_complete_c1_publication_v1(jsonb)'),
    ('public.weekly_exceptional_pay_prepare_action_v1(jsonb)'),
    ('public.weekly_exceptional_pay_prepare_family_v1(jsonb)'),
    ('public.weekly_exceptional_pay_read_c1_request_v1(jsonb)'),
    ('public.weekly_exceptional_pay_record_c1_checkpoint_v1(jsonb)'),
    ('public.weekly_exceptional_pay_record_c1_recovery_v1(jsonb)'),
    ('public.weekly_exceptional_pay_record_c1_unknown_v1(jsonb)'),
    ('public.weekly_exceptional_pay_stage_c1_request_v1(jsonb)'),
    ('public.weekly_exceptional_pay_wait_atomic_v1(jsonb)'),
    ('public.weekly_source_audit_guard_refusal_record_v1(jsonb)'),
    ('public.weekly_source_candidate_app_draft_save_atomic_v1(uuid,text,uuid,jsonb,timestamptz)'),
    ('public.weekly_source_candidate_app_request_get_v1(uuid,text,uuid,timestamptz)'),
    ('public.weekly_source_candidate_app_submit_atomic_v1(uuid,text,uuid,jsonb,timestamptz)'),
    ('public.weekly_source_candidate_self_submit_atomic_v1(uuid,text,jsonb,timestamptz)'),
    ('public.weekly_source_candidate_check_materialise_atomic_v1(jsonb,timestamptz)'),
    ('public.weekly_source_candidate_hours_push_v1(jsonb)'),
    ('public.weekly_source_candidate_hours_evidence_v1(jsonb)'),
    ('public.weekly_source_candidate_query_get_v1(jsonb)'),
    ('public.weekly_source_candidate_reminder_atomic_v1(jsonb)'),
    ('public.weekly_source_candidate_response_submit_atomic_v1(jsonb)'),
    ('public.weekly_source_charge_accept_atomic_v1(jsonb)'),
    -- Stage 5 / PHD-017: independent completed-Timesheet informational-copy
    -- producer. These are service-only; no browser role may choose the
    -- recipient, document hash, attachment or delivery state.
    ('public.weekly_source_completed_pack_copy_commit_atomic_v1(jsonb)'),
    ('public.weekly_source_completed_pack_copy_due_list_v1(jsonb)'),
    ('public.weekly_source_completed_pack_copy_status_sync_v1(jsonb)'),
    ('public.weekly_source_client_settings_get_v1(jsonb)'),
    ('public.weekly_source_client_settings_save_atomic_v1(jsonb)'),
    ('public.weekly_source_contract_settings_get_v1(jsonb)'),
    ('public.weekly_source_contract_settings_save_atomic_v1(jsonb)'),
    ('public.weekly_source_correct_final_apply_atomic_v1(jsonb)'),
    -- WP-59, 19 Sep 2026.  The correction-session cancel owner
    -- (19092026_0100_weekly_source_correction_cancel_v1.sql).  The independent
    -- duplicate of this list lives in v_expected_service_rpcs in
    -- supabase/verification/15092026_1534_weekly_source_acl_contract_v1.sql and
    -- is sealed as an exact set in both directions; WP-59 does not own that
    -- file and has written the matching one-line addition to
    -- IMPL\handoffs\WP-59_NEEDS.md (N1).  Until it lands, a full NEW build
    -- stops at that verifier with 'Weekly Source service RPC contract differs
    -- from the independent expected set'.
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
    ('public.weekly_source_guard_refusal_record_after_rollback_v1(jsonb)'),
    ('public.weekly_source_invoice_admit_atomic_v1(jsonb)'),
    ('public.weekly_source_invoice_batch_admit_atomic_v1(jsonb)'),
    ('public.weekly_source_invoice_batch_candidates_v1(jsonb)'),
    ('public.weekly_source_invoice_edit_context_v1(jsonb)'),
    ('public.weekly_source_invoice_evidence_v1(jsonb)'),
    ('public.weekly_source_invoice_move_atomic_v1(jsonb)'),
    -- The source-backed invoice/report projection is read by the broker only.
    -- Keep it on the same service-only allowlist as the invoice edit and move
    -- owners; browser roles never receive direct EXECUTE authority.
    ('public.weekly_source_invoice_report_rows_v1(jsonb)'),
    -- 24 section 4 / Gate 2 G2-5: the Office later-change decision, the only
    -- new public service RPC of the protected-action set.
    ('public.weekly_source_later_change_decide_atomic_v1(jsonb)'),
    ('public.weekly_source_manager_review_get_v1(jsonb)'),
    ('public.weekly_source_manager_review_respond_atomic_v1(jsonb)'),
    ('public.weekly_source_manager_route_prepare_atomic_v1(jsonb)'),
    ('public.weekly_source_message_dispatch_claim_v1(jsonb)'),
    ('public.weekly_source_message_dispatch_result_atomic_v1(jsonb)'),
    -- WP-44 F2: the transient Candidate push snapshot failure owner.
    ('public.weekly_source_message_dispatch_snapshot_failure_atomic_v1(jsonb)'),
    ('public.weekly_source_message_dispatch_submission_start_atomic_v1(jsonb)'),
    ('public.weekly_source_message_dispatch_target_claim_v1(jsonb)'),
    ('public.weekly_source_message_dispatch_target_result_atomic_v1(jsonb)'),
    ('public.weekly_source_message_dispatch_target_start_atomic_v1(jsonb)'),
    ('public.weekly_source_message_render_due_list_v1(jsonb)'),
    ('public.weekly_source_message_render_input_v1(jsonb)'),
    ('public.weekly_source_message_render_stage_atomic_v1(jsonb)'),
    ('public.weekly_source_message_targets_register_atomic_v1(jsonb)'),
    -- 24 section 9 Mode A: the broker dispatches a Weekly Source upload to the
    -- established validation-only comparison owner and applies the reference
    -- outcome. Both are service-only SECURITY DEFINER entry points.
    ('public.weekly_source_mode_a_dispatch_atomic_v1(jsonb)'),
    ('public.weekly_source_mode_a_reference_apply_atomic_v1(jsonb)'),
    ('public.weekly_source_nhsp_report_scope_resolve_atomic_v1(jsonb)'),
    ('public.weekly_source_no_shifts_attest_atomic_v1(jsonb)'),
    ('public.weekly_source_office_bulk_query_action_atomic_v1(jsonb)'),
    ('public.weekly_source_office_notification_ack_atomic_v1(jsonb)'),
    ('public.weekly_source_office_notifications_list_v1(jsonb)'),
    ('public.weekly_source_office_timesheet_presentation_v1(jsonb)'),
    ('public.weekly_source_office_authorise_scope_v1(jsonb)'),
    ('public.weekly_source_office_summary_rows_v1(jsonb)'),
    ('public.weekly_source_office_workspace_v1(jsonb)'),
    ('public.weekly_source_client_cycle_resolve_atomic_v1(jsonb)'),
    ('public.weekly_source_protected_editor_context_v1(jsonb)'),
    ('public.weekly_source_protected_editor_prepare_v1(jsonb)'),
    ('public.weekly_source_workspace_scopes_v1(jsonb)'),
    ('public.weekly_source_combined_finalise_workspace_v1(jsonb)'),
    ('public.weekly_source_combined_review_workspace_v1(jsonb)'),
    ('public.weekly_source_upload_detail_v1(jsonb)'),
    ('public.weekly_source_ordinary_pay_projection_apply_atomic_v1(jsonb)'),
    ('public.weekly_source_pending_entitlement_bundle_reopen_v1(uuid,text,uuid)'),
    ('public.weekly_source_pending_entitlement_release_apply_v1(jsonb)'),
    ('public.weekly_source_pending_entitlement_release_claim_page_v1(jsonb)'),
    ('public.weekly_source_pending_entitlement_release_record_failure_v1(jsonb)'),
    ('public.weekly_source_projection_begin_atomic_v1(jsonb)'),
    ('public.weekly_source_projection_publish_atomic_v1(jsonb)'),
    ('public.weekly_source_projection_rows_apply_atomic_v1(uuid,uuid,jsonb)'),
    ('public.weekly_source_query_accept_system_hours_atomic_v1(jsonb)'),
    ('public.weekly_source_query_ask_candidate_atomic_v1(jsonb)'),
    ('public.weekly_source_query_scheduler_tick_v1(jsonb)'),
    ('public.weekly_source_query_send_manager_now_atomic_v1(jsonb)'),
    ('public.weekly_source_query_sync_atomic_v1(jsonb)'),
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
    ('public.weekly_source_upload_seal_atomic_v1(jsonb)'),
    ('public.weekly_source_upload_stage_begin_atomic_v1(jsonb)'),
    ('public.weekly_source_upload_stage_rows_atomic_v1(jsonb)'),
    ('public.weekly_source_invoice_hold_release_atomic_v1(jsonb)');
$function$;

alter function private._weekly_source_acl_table_contract_v1() owner to postgres;
revoke all on function private._weekly_source_acl_table_contract_v1() from public,anon,authenticated,service_role;
alter function private._weekly_source_acl_private_helper_contract_v1() owner to postgres;
revoke all on function private._weekly_source_acl_private_helper_contract_v1() from public,anon,authenticated,service_role;
alter function private._weekly_source_acl_service_rpc_contract_v1() owner to postgres;
revoke all on function private._weekly_source_acl_service_rpc_contract_v1() from public,anon,authenticated,service_role;
commit;
