\set ON_ERROR_STOP on

begin;

do $verify_weekly_source_acl$
declare
  v_bad integer;
  v_contract_count integer;
  v_distinct_contract_count integer;
  v_immutable_count integer;
  v_lifecycle_count integer;
  v_stateful_count integer;
  v_lifecycle_column_count integer;
  v_names text;
  v_service_role_oid oid;
  v_expected_immutable text[]:=array[
    'weekly_candidate_app_mutation_receipts',
    'weekly_discrepancy_events',
    'weekly_exceptional_c1_component_records',
    'weekly_exceptional_c1_publication_checkpoints',
    'weekly_exceptional_c1_source_parts',
    'weekly_exceptional_c1_source_records',
    'weekly_exceptional_pay_family_events',
    'weekly_exceptional_pay_target_events',
    'weekly_exceptional_payment_events',
    'weekly_final_source_correction_root_impacts',
    'weekly_issue_comparison_revisions',
    'weekly_source_charge_checks',
    'weekly_source_charge_acceptances',
    'weekly_source_contract_qualification_observations',
    'weekly_source_entitlement_head_components',
    'weekly_source_expense_cell_evidence',
    'weekly_source_expense_pay_materialisations',
    'weekly_source_final_snapshot_lines',
    'weekly_source_invoice_presentation_lines',
    'weekly_source_manifest_movements',
    'weekly_source_money_cell_evidence',
    'weekly_source_nhsp_backing_reports',
    'weekly_source_ordinary_pay_projection_receipts',
    'weekly_source_physical_rows',
    'weekly_source_row_economic_snapshots',
    'weekly_source_row_expense_policy_snapshots',
    'weekly_source_row_qualification_evidence',
    'weekly_source_row_resolutions',
    -- Decision D8: the per-source-row binding is append-only again; every
    -- column that moves now lives on weekly_source_root_authorisations.
    'weekly_source_row_timesheet_lineages',
    'weekly_source_upload_attempts',
    'weekly_source_upload_rows',
    'weekly_source_upload_supersessions',
    'weekly_timesheet_authority_resolutions',
    'weekly_timesheet_source_comparisons',
    'weekly_work_event_source_links',
    'weekly_work_events'
  ]::text[];
  v_expected_lifecycle_columns text[]:=array[
    'office_action_notifications.operational_state',
    'office_action_notifications.read_at_utc',
    'office_action_notifications.resolved_at_utc',
    'weekly_completed_pack_copy_events.provider_command_id',
    'weekly_completed_pack_copy_events.state',
    'weekly_candidate_message_notifications.retired_at_utc',
    'weekly_exceptional_c1_unknown_outcomes.recovered_at_utc',
    'weekly_exceptional_c1_unknown_outcomes.recovery_idempotency_key',
    'weekly_exceptional_c1_unknown_outcomes.recovery_result_json',
    'weekly_exceptional_c1_unknown_outcomes.state',
    'weekly_manager_route_preparations.bound_at_utc',
    'weekly_manager_route_preparations.retired_at_utc',
    'weekly_manager_route_preparations.state',
    'weekly_manager_route_receipts.revoked_at_utc',
    'weekly_manager_route_receipts.state',
    'weekly_message_delivery_failures.acknowledged_at_utc',
    'weekly_message_delivery_failures.resolved_at_utc',
    'weekly_message_delivery_failures.state',
    'weekly_message_provider_attempts.bounded_error_json',
    'weekly_message_provider_attempts.bounded_provider_receipt_json',
    'weekly_message_provider_attempts.completed_at_utc',
    'weekly_message_provider_attempts.outcome',
    'weekly_message_renders.state',
    'weekly_message_target_attempts.bounded_error_json',
    'weekly_message_target_attempts.bounded_provider_receipt_json',
    'weekly_message_target_attempts.completed_at_utc',
    'weekly_message_target_attempts.outcome',
    'weekly_message_target_attempts.provider_message_id',
    'weekly_message_target_attempts.result_hash',
    'weekly_source_billing_movements.placement_state',
    'weekly_source_expense_materialisations.state',
    'weekly_source_state_transitions.ordinary_source_entitlement_projection_state',
    'weekly_timesheet_reference_apply_items.applied_at_utc',
    -- Decision D8, proof/34 section 4 and proof/36 section 5.6: on the per-root
    -- authorisation record, the coordinator writes current_entitlement_head_id
    -- on the live generation and the withdrawal owner sets the two withdrawal
    -- columns and clears the head in the same statement. updated_at_utc is the
    -- audit clock those writers touch with them. Everything else on the record
    -- is frozen when written.
    'weekly_source_root_authorisations.current_entitlement_head_id',
    'weekly_source_root_authorisations.withdrawn_at_utc',
    'weekly_source_root_authorisations.withdrawn_by_user_id',
    'weekly_source_root_authorisations.updated_at_utc',
    -- proof/36 section 5 step 7: the approved protected-hours decision is
    -- marked withdrawn, never deleted.
    'weekly_exceptional_payment_approvals.withdrawn_at_utc',
    'weekly_exceptional_payment_approvals.withdrawn_by_user_id',
    'weekly_exceptional_payment_approvals.withdrawal_kind',
    -- 24 section 4.5 step 5: bundles and heads are staged and then activated.
    -- Only the activation lifecycle may move; every economic fact, digest,
    -- inventory count and certified-zero flag is frozen when written (WB-005).
    'weekly_source_entitlement_decision_bundles.state',
    'weekly_source_entitlement_decision_bundles.committed_at_utc',
    'weekly_source_entitlement_decision_bundles.superseded_at_utc',
    'weekly_source_entitlement_heads.state',
    'weekly_source_entitlement_heads.committed_at_utc',
    'weekly_source_entitlement_heads.superseded_at_utc',
    'weekly_source_entitlement_heads.superseded_by_head_id',
    'weekly_source_entitlement_heads.publication_receipt_digest',
    'weekly_source_entitlement_heads.scope_change_tx_token',
    -- HANDOVER 2 round-5 ruling A3 step 3 (package WP-07c): the Office
    -- change-of-mind withdrawal supersedes a committed head atomically with the
    -- withdrawal, with an explicit reason and an immutable predecessor link.
    -- Same activation lifecycle, one more supersession authority.
    'weekly_source_entitlement_heads.superseded_reason',
    'weekly_source_entitlement_heads.superseded_by_withdrawal_id',
    -- proof/32 sections 2 and 10: the pending bundle's lease, backoff clock,
    -- failure counter, census blob and release marks move on every worker
    -- tick; its request, digest and member identity stay frozen.
    'weekly_source_pending_entitlement_bundles.state',
    'weekly_source_pending_entitlement_bundles.pending_revision',
    'weekly_source_pending_entitlement_bundles.lease_owner',
    'weekly_source_pending_entitlement_bundles.lease_token',
    'weekly_source_pending_entitlement_bundles.lease_worker_run_id',
    'weekly_source_pending_entitlement_bundles.lease_expires_at_utc',
    'weekly_source_pending_entitlement_bundles.next_check_at_utc',
    'weekly_source_pending_entitlement_bundles.technical_failure_count',
    'weekly_source_pending_entitlement_bundles.manual_review_reason',
    'weekly_source_pending_entitlement_bundles.last_census_json',
    'weekly_source_pending_entitlement_bundles.released_receipt_id',
    'weekly_source_pending_entitlement_bundles.released_receipt_digest',
    'weekly_source_pending_entitlement_bundles.released_by_worker_id',
    'weekly_source_pending_entitlement_bundles.released_by_worker_run_id',
    'weekly_source_pending_entitlement_bundles.released_at_utc',
    'weekly_source_pending_entitlement_bundles.updated_at_utc'
  ]::text[];
  v_expected_stateful text[]:=array[
    'weekly_candidate_cohorts',
    'weekly_candidate_outreach_generations',
    'weekly_candidate_outreach_memberships',
    'weekly_candidate_response_draft_items',
    'weekly_candidate_response_drafts',
    'weekly_discrepancy_incidents',
    'weekly_exceptional_c1_publication_requests',
    'weekly_exceptional_orchestration_runs',
    'weekly_exceptional_orchestration_steps',
    'weekly_exceptional_pay_generations',
    'weekly_exceptional_pay_target_families',
    'weekly_exceptional_pending_reconciliation_targets',
    'weekly_expense_authority_generations',
    'weekly_final_source_correction_sessions',
    'weekly_manager_cohort_due_events',
    'weekly_manager_recipient_generations',
    'weekly_manager_recipient_memberships',
    'weekly_manager_recipient_routes',
    'weekly_manager_review_batches',
    'weekly_manager_review_items',
    'weekly_message_dispatch_commands',
    'weekly_message_dispatch_targets',
    'weekly_message_intents',
    'weekly_route_activations',
    'weekly_source_client_cycle_completions',
    'weekly_source_client_manifests',
    'weekly_source_client_policies',
    'weekly_source_contract_policies',
    'weekly_source_cycles',
    'weekly_source_final_revisions',
    'weekly_source_finalisation_pay_runs',
    'weekly_source_finalisation_pay_tasks',
    'weekly_source_format_profiles',
    'weekly_source_global_settings',
    'weekly_source_group_clients',
    'weekly_source_groups',
    'weekly_source_invoice_line_bindings',
    'weekly_source_invoice_placements',
    'weekly_source_projection_publications',
    'weekly_source_report_scopes',
    'weekly_source_uploads',
    'weekly_timesheet_reference_apply_operations',
    'weekly_timesheet_submission_request_memberships',
    'weekly_timesheet_submission_requests'
  ]::text[];
  -- Registered Weekly Source private helpers. This array is the independent
  -- expected copy of private._weekly_source_acl_private_helper_contract_v1().
  -- It is an inventory of owner-internal routines, not a grant list and not an
  -- exception list: every entry must exist, be owned by the release owner and
  -- be executable by nobody — PUBLIC, anon, authenticated and service_role
  -- alike. Listing a routine here grants it nothing.
  --
  -- A routine granted execute to service_role belongs in
  -- v_expected_service_rpcs below instead, and no signature may appear in both:
  -- a signature listed in the service array but not actually granted makes the
  -- routine sweep fail, which is how the two lists stay honest.
  v_expected_private_helpers text[]:=array[
    -- Plan 6.2 Gate 1 (proof/32 section 9): the distinct-array proof a CHECK
    -- cannot express, and the publication-receipt immutability guard.
    'private.weekly_source_uuid_array_is_distinct_v1(uuid[])',
    'private.weekly_source_entitlement_publication_receipt_immutable_v1()',
    -- Plan 6.2 Gate 1 review fixes: the three head constraint-trigger guards.
    'private.weekly_source_entitlement_head_root_identity_v1()',
    'private.weekly_source_entitlement_head_inventory_assert_v1()',
    'private.weekly_source_entitlement_head_receipt_assert_v1()',
    -- Plan 6.2 Gate 6 (25 section 7, 24 section 8): Contract choice facts.
    'private.weekly_source_schedule_compatible_v1(jsonb,date)',
    'private.weekly_source_verified_role_band_match_v1(text,text,uuid,uuid,uuid,text,date)',
    -- Plan 6.2 Gate 6 / G6-13 (24 section 9 step 2, WP-37): durable work
    -- identity is a COMPATIBLE schedule, never the exact Actual start and end.
    'private.weekly_source_work_event_schedule_compatible_v1(uuid,timestamp without time zone,timestamp without time zone)',
    -- Plan 6.2 Gate 8 (24 section 9): Mode A dispatch facts.
    'private.weekly_source_mode_a_role_type_v1(text,text)',
    'private.weekly_source_mode_a_operation_key_v1(uuid,uuid)',
    -- Plan 6.2 Gate 8 / 24 section 9A (WP-37): the superseded-head refusal
    -- record, so a rotated Timesheet never blocks the family's current head.
    'private.weekly_source_mode_a_refuse_operation_v1(uuid,uuid,uuid,uuid,uuid,text)',
    -- Plan 6.2 Gate 6 (proof/32 section 6, proof/34 sections 3, 6, 7): the
    -- rotation authority set, including interface I-1 and the managed-root
    -- guard.
    'private.weekly_source_candidate_serial_gate_v1(uuid,text,uuid,text)',
    'private.weekly_source_lock_family_rows_v1(uuid[],uuid)',
    'private.weekly_source_lock_and_resolve_families_v1(uuid,uuid[],text,uuid,text)',
    'private.weekly_source_resolve_root_identity_v1(uuid)',
    'private.weekly_source_root_integrity_assert_v1(uuid,text,integer)',
    'private.weekly_source_managed_root_guard_v1(uuid)',
    -- Plan 6.2 Gates 3 and 5 (proof/32 sections 6 to 9): the canonical encoder
    -- and digest, its input helpers, the uuid set algebra, the receipt reader
    -- and interface I-4's two coordinator entry points.
    'private.weekly_source_canonical_json_text_v1(jsonb)',
    'private.weekly_source_publication_request_digest_v1(jsonb)',
    'private.weekly_source_publication_scalar_v1(jsonb,text,text,integer,boolean)',
    'private.weekly_source_publication_require_keys_v1(jsonb,text[],text)',
    'private.weekly_source_publication_component_canonical_v1(jsonb,text)',
    'private.weekly_source_publication_request_canonical_v1(jsonb,text,uuid)',
    'private.weekly_source_uuid_set_union_v1(uuid[],uuid[])',
    'private.weekly_source_uuid_set_intersect_v1(uuid[],uuid[])',
    'private.weekly_source_uuid_set_difference_v1(uuid[],uuid[])',
    'private.weekly_source_uuid_set_equals_v1(uuid[],uuid[])',
    'private.weekly_source_publication_receipt_json_v1(uuid)',
    'private.weekly_source_entitlement_publish_core_v1(jsonb,text,jsonb,uuid,text,uuid,jsonb,jsonb)',
    'private.weekly_source_entitlement_publish_immediate_v1(jsonb)',
    -- Plan 6.2 Gate 7 (24 sections 10 to 12, proof/34 section 9): the
    -- source-aware invoice issue validator and the invoice family resolver.
    'private.weekly_source_invoice_issue_validate_v1(uuid)',
    'private.weekly_source_invoice_issue_skippable_code_v1(text)',
    'private.weekly_source_invoice_issue_skippable_reason_v1(text)',
    'private.weekly_source_invoice_issue_blockers_v1(uuid,text[])',
    'private.weekly_source_invoice_issue_reasons_v1(jsonb,text[])',
    'private.weekly_source_invoice_family_timesheet_ids_v1(uuid)',
    -- Decision D8: the two constraint-trigger guards on the per-root
    -- authorisation record.
    'private.weekly_source_root_authorisation_identity_v1()',
    'private.weekly_source_root_authorisation_withdrawal_once_v1()',
    -- Plan 6.2 Gate 2 (24 section 4, decision D9): the proposal composer's
    -- identity, component and request builders, interface I-7's effective
    -- inventory, the fail-closed target-family reader and its identity trigger.
    'private.weekly_source_effective_inventory_v1(uuid)',
    'private.weekly_source_entitlement_derived_uuid_v1(text,text)',
    'private.weekly_source_entitlement_component_id_v1(text,text,text,text)',
    'private.weekly_source_entitlement_components_v1(jsonb,jsonb)',
    'private.weekly_source_entitlement_proposal_request_v1(uuid,uuid,text,uuid,bigint,uuid,uuid,jsonb,text)',
    'private.weekly_source_entitlement_proposal_record_v1(jsonb,uuid,uuid,date,uuid)',
    'private.weekly_source_target_family_for_root_v1(uuid)',
    'private.weekly_source_target_family_root_identity_v1()',
    -- Stage 5 / PHD-017: the owner-only completed-pack eligibility resolver.
    'private._weekly_source_completed_pack_copy_eligibility_v1(uuid)',
    -- WP-54: the two source-row admission rules -- 14 section 4.1.3 with
    -- acceptance row NHSP-BR-006, and 03 section 7 with 14 sections 4.2.8 and
    -- 4.1.7. Both are read-only asserts called by the upload seal, and the
    -- overlap rule by the finalisation engine as well; neither is granted to
    -- any role.
    'private.weekly_source_cutoff_admission_assert_v1(uuid,text,timestamp with time zone)',
    'private.weekly_source_overlap_admission_assert_v1(uuid)'
  ]::text[];
  v_expected_service_rpcs text[]:=array[
    'private.weekly_source_invoice_batch_rows_v1(jsonb,jsonb)',
    'private.weekly_source_invoice_batch_snapshot_v1()',
    'private.weekly_source_summary_pay_delayed_v1(uuid,uuid,uuid,date)',
    'private.weekly_source_managed_root_guard_decision_v1(uuid)',
    'private.weekly_source_office_authority_v1(uuid,text,uuid,uuid,date)',
    'public.weekly_exceptional_pay_action_context_v1(jsonb)',
    'public.weekly_exceptional_pay_action_publication_status_v1(jsonb)',
    'public.weekly_exceptional_pay_complete_c1_publication_v1(jsonb)',
    'public.weekly_exceptional_pay_prepare_action_v1(jsonb)',
    'public.weekly_exceptional_pay_prepare_family_v1(jsonb)',
    'public.weekly_exceptional_pay_read_c1_request_v1(jsonb)',
    'public.weekly_exceptional_pay_record_c1_checkpoint_v1(jsonb)',
    'public.weekly_exceptional_pay_record_c1_recovery_v1(jsonb)',
    'public.weekly_exceptional_pay_record_c1_unknown_v1(jsonb)',
    'public.weekly_exceptional_pay_stage_c1_request_v1(jsonb)',
    'public.weekly_exceptional_pay_wait_atomic_v1(jsonb)',
    'public.weekly_source_audit_guard_refusal_record_v1(jsonb)',
    'public.weekly_source_candidate_app_draft_save_atomic_v1(uuid,text,uuid,jsonb,timestamptz)',
    'public.weekly_source_candidate_app_request_get_v1(uuid,text,uuid,timestamptz)',
    'public.weekly_source_candidate_app_submit_atomic_v1(uuid,text,uuid,jsonb,timestamptz)',
    'public.weekly_source_candidate_check_materialise_atomic_v1(jsonb,timestamptz)',
    'public.weekly_source_candidate_hours_push_v1(jsonb)',
    'public.weekly_source_candidate_query_get_v1(jsonb)',
    'public.weekly_source_candidate_reminder_atomic_v1(jsonb)',
    'public.weekly_source_candidate_response_submit_atomic_v1(jsonb)',
    'public.weekly_source_candidate_self_submit_atomic_v1(uuid,text,jsonb,timestamptz)',
    'public.weekly_source_charge_accept_atomic_v1(jsonb)',
    -- Stage 5 / PHD-017: completed-Timesheet informational-copy producer.
    'public.weekly_source_completed_pack_copy_commit_atomic_v1(jsonb)',
    'public.weekly_source_completed_pack_copy_due_list_v1(jsonb)',
    'public.weekly_source_completed_pack_copy_status_sync_v1(jsonb)',
    'public.weekly_source_client_settings_get_v1(jsonb)',
    'public.weekly_source_client_settings_save_atomic_v1(jsonb)',
    'public.weekly_source_contract_settings_get_v1(jsonb)',
    'public.weekly_source_contract_settings_save_atomic_v1(jsonb)',
    'public.weekly_source_correct_final_apply_atomic_v1(jsonb)',
    -- Landed by WP-61 on WP-59's behalf, verbatim from IMPL\handoffs\WP-59_NEEDS.md
    -- N1 including its stated alphabetical position.  WP-59 added
    -- public.weekly_source_correct_final_cancel_atomic_v1(jsonb) to the
    -- installed inventory and handed this line off because this file is owned
    -- by WP-56.  Until it landed, this exact-set seal failed on EVERY build
    -- from empty, measured by WP-61 on banking_modal_v2_release6101_20260919:
    -- 'Weekly Source service RPC contract differs from the independent
    -- expected set'.  Additive; it changes nothing for any other signature.
    'public.weekly_source_correct_final_cancel_atomic_v1(jsonb)',
    'public.weekly_source_correct_final_open_atomic_v1(jsonb)',
    'public.weekly_source_correct_final_prepare_atomic_v1(jsonb)',
    'public.weekly_source_correct_final_review_atomic_v1(jsonb)',
    'public.weekly_source_external_publication_arrival_v1(jsonb)',
    'public.weekly_source_external_publication_pending_inputs_v1(jsonb)',
    'public.weekly_source_finalisation_pay_open_atomic_v1(jsonb)',
    'public.weekly_source_finalisation_pay_task_finish_atomic_v1(jsonb)',
    'public.weekly_source_finalisation_pay_task_recover_atomic_v1(jsonb)',
    'public.weekly_source_finalisation_pay_task_start_atomic_v1(jsonb)',
    'public.weekly_source_finalisation_pay_task_unknown_atomic_v1(jsonb)',
    'public.weekly_source_finalise_atomic_v1(jsonb)',
    'public.weekly_source_first_authorisation_withdraw_available_v1(uuid)',
    'public.weekly_source_first_authorisation_withdraw_request_v1(jsonb)',
    'public.weekly_source_first_authorisation_withdraw_v1(uuid,uuid,text,uuid)',
    'public.weekly_source_first_authorise_v1(uuid,uuid,text,uuid)',
    'public.weekly_source_global_settings_get_v1(jsonb)',
    'public.weekly_source_global_settings_save_atomic_v1(jsonb)',
    'public.weekly_source_guard_refusal_record_after_rollback_v1(jsonb)',
    'public.weekly_source_invoice_admit_atomic_v1(jsonb)',
    'public.weekly_source_invoice_batch_admit_atomic_v1(jsonb)',
    'public.weekly_source_invoice_batch_candidates_v1(jsonb)',
    'public.weekly_source_invoice_edit_context_v1(jsonb)',
    'public.weekly_source_invoice_evidence_v1(jsonb)',
    'public.weekly_source_invoice_move_atomic_v1(jsonb)',
    'public.weekly_source_invoice_report_rows_v1(jsonb)',
    'public.weekly_source_later_change_decide_atomic_v1(jsonb)',
    'public.weekly_source_manager_review_get_v1(jsonb)',
    'public.weekly_source_manager_review_respond_atomic_v1(jsonb)',
    'public.weekly_source_manager_route_prepare_atomic_v1(jsonb)',
    'public.weekly_source_message_dispatch_claim_v1(jsonb)',
    'public.weekly_source_message_dispatch_result_atomic_v1(jsonb)',
    -- WP-44 F2: the transient Candidate push snapshot failure owner.
    'public.weekly_source_message_dispatch_snapshot_failure_atomic_v1(jsonb)',
    'public.weekly_source_message_dispatch_submission_start_atomic_v1(jsonb)',
    'public.weekly_source_message_dispatch_target_claim_v1(jsonb)',
    'public.weekly_source_message_dispatch_target_result_atomic_v1(jsonb)',
    'public.weekly_source_message_dispatch_target_start_atomic_v1(jsonb)',
    'public.weekly_source_message_render_due_list_v1(jsonb)',
    'public.weekly_source_message_render_input_v1(jsonb)',
    'public.weekly_source_message_render_stage_atomic_v1(jsonb)',
    'public.weekly_source_message_targets_register_atomic_v1(jsonb)',
    'public.weekly_source_mode_a_dispatch_atomic_v1(jsonb)',
    'public.weekly_source_mode_a_reference_apply_atomic_v1(jsonb)',
    'public.weekly_source_nhsp_report_scope_resolve_atomic_v1(jsonb)',
    'public.weekly_source_no_shifts_attest_atomic_v1(jsonb)',
    'public.weekly_source_office_bulk_query_action_atomic_v1(jsonb)',
    'public.weekly_source_office_notification_ack_atomic_v1(jsonb)',
    'public.weekly_source_office_notifications_list_v1(jsonb)',
    'public.weekly_source_office_timesheet_presentation_v1(jsonb)',
    'public.weekly_source_office_workspace_v1(jsonb)',
    'public.weekly_source_ordinary_pay_projection_apply_atomic_v1(jsonb)',
    'public.weekly_source_pending_entitlement_bundle_reopen_v1(uuid,text,uuid)',
    'public.weekly_source_pending_entitlement_release_apply_v1(jsonb)',
    'public.weekly_source_pending_entitlement_release_claim_page_v1(jsonb)',
    'public.weekly_source_pending_entitlement_release_record_failure_v1(jsonb)',
    'public.weekly_source_projection_begin_atomic_v1(jsonb)',
    'public.weekly_source_projection_publish_atomic_v1(jsonb)',
    'public.weekly_source_projection_rows_apply_atomic_v1(uuid,uuid,jsonb)',
    'public.weekly_source_query_accept_system_hours_atomic_v1(jsonb)',
    'public.weekly_source_query_ask_candidate_atomic_v1(jsonb)',
    'public.weekly_source_query_scheduler_tick_v1(jsonb)',
    'public.weekly_source_query_send_manager_now_atomic_v1(jsonb)',
    'public.weekly_source_query_sync_atomic_v1(jsonb)',
    'public.weekly_source_source_group_save_atomic_v1(jsonb)',
    'public.weekly_source_source_groups_get_v1(jsonb)',
    'public.weekly_source_target_managed_root_prepare_atomic_v1(jsonb)',
    'public.weekly_source_timesheet_audit_chronology_v1(jsonb)',
    'public.weekly_source_timesheet_hours_export_v1(jsonb)',
    'public.weekly_source_timesheet_lineage_ensure_atomic_v1(uuid,uuid)',
    'public.weekly_source_timesheet_submission_complete_atomic_v1(jsonb)',
    'public.weekly_source_timesheet_submission_request_start_atomic_v1(jsonb)',
    'public.weekly_source_upload_abort_atomic_v1(jsonb)',
    'public.weekly_source_upload_attempt_record_atomic_v1(jsonb)',
    'public.weekly_source_upload_context_v1(jsonb)',
    'public.weekly_source_upload_seal_atomic_v1(jsonb)',
    'public.weekly_source_upload_stage_begin_atomic_v1(jsonb)',
    'public.weekly_source_upload_stage_rows_atomic_v1(jsonb)'
  ]::text[];
begin
  select oid into v_service_role_oid
  from pg_catalog.pg_roles
  where rolname='service_role';
  if v_service_role_oid is null
     or not exists(select 1 from pg_catalog.pg_roles where rolname='anon')
     or not exists(select 1 from pg_catalog.pg_roles where rolname='authenticated') then
    raise exception 'weekly source ACL verification requires anon, authenticated and service_role';
  end if;

  if pg_catalog.to_regprocedure('private._weekly_source_acl_table_contract_v1()') is null
     or pg_catalog.to_regprocedure('private._weekly_source_acl_lifecycle_column_contract_v1()') is null
     or pg_catalog.to_regprocedure('private._weekly_source_acl_private_helper_contract_v1()') is null
     or pg_catalog.to_regprocedure('private._weekly_source_acl_service_rpc_contract_v1()') is null
     or pg_catalog.to_regprocedure('private._weekly_source_immutable_record_guard_v1()') is null
     or pg_catalog.to_regprocedure('private._weekly_source_immutable_fact_guard_v1()') is null then
    raise exception 'weekly source ACL contract helper is missing';
  end if;

  select count(*),count(distinct table_name),
         count(*) filter(where record_class='IMMUTABLE_APPEND_ONLY'),
         count(*) filter(where record_class='IMMUTABLE_FACTS_WITH_LIFECYCLE'),
         count(*) filter(where record_class='STATEFUL_SERVER_OWNED')
    into v_contract_count,v_distinct_contract_count,v_immutable_count,v_lifecycle_count,v_stateful_count
  from private._weekly_source_acl_table_contract_v1();
  -- Plan 6.2 Gate 1 adds four relations (decision bundles, heads, head
  -- components, pending bundles) and moves two existing relations from
  -- IMMUTABLE_APPEND_ONLY to IMMUTABLE_FACTS_WITH_LIFECYCLE
  -- (weekly_source_row_timesheet_lineages per proof/34 section 4,
  -- weekly_exceptional_payment_approvals per proof/36 section 5 step 7).
  -- Gate 5 then classifies weekly_source_pending_entitlement_bundles as
  -- IMMUTABLE_FACTS_WITH_LIFECYCLE rather than STATEFUL_SERVER_OWNED: the
  -- stored request, digest and member identity are frozen, and only the lease,
  -- backoff clock, failure counter, census blob and release marks of
  -- proof/32 sections 2 and 10 may move.
  -- Decision D8 adds weekly_source_root_authorisations and returns
  -- weekly_source_row_timesheet_lineages to IMMUTABLE_APPEND_ONLY because the
  -- authorisation record and its movable columns left it.  The later approved
  -- source-rate-disparity journey adds weekly_source_charge_acceptances as an
  -- immutable append-only ledger.  Stage 5 / PHD-017 does not add a new table:
  -- it uses the pre-existing weekly_completed_pack_copy_events relation and
  -- classifies its provider_command_id/state pair as the only mutable delivery
  -- lifecycle on that otherwise frozen record.  The exact final independent
  -- sets below are therefore 36 immutable, 19 lifecycle and 44 stateful tables
  -- (99 total).  Lifecycle columns remain 67: 64 - 3 + 4 = 65 for the Gate 1
  -- movement, then +2 for the head supersession reason and predecessor link of
  -- HANDOVER 2 round-5 ruling A3 step 3 (package WP-07c), with the completed-
  -- pack pair already represented in the sealed set.
  if v_contract_count<>99
     or v_distinct_contract_count<>99
     or v_immutable_count<>36
     or v_lifecycle_count<>19
     or v_stateful_count<>44 then
    raise exception 'weekly source table contract count mismatch: total %, distinct %, immutable %, lifecycle %, stateful %',
      v_contract_count,v_distinct_contract_count,v_immutable_count,v_lifecycle_count,v_stateful_count;
  end if;

  if exists(
    select expected_name
    from pg_catalog.unnest(v_expected_immutable) expected_name
    where not exists(
      select 1
      from private._weekly_source_acl_table_contract_v1() actual
      where actual.table_name=expected_name
        and actual.record_class='IMMUTABLE_APPEND_ONLY'
    )
  )
  or exists(
    select 1
    from private._weekly_source_acl_table_contract_v1() actual
    where actual.record_class='IMMUTABLE_APPEND_ONLY'
      and actual.table_name<>all(v_expected_immutable)
  ) then
    raise exception 'weekly source immutable table contract differs from the independent expected set';
  end if;

  if exists(
    select expected_name
    from pg_catalog.unnest(v_expected_stateful) expected_name
    where not exists(
      select 1
      from private._weekly_source_acl_table_contract_v1() actual
      where actual.table_name=expected_name
        and actual.record_class='STATEFUL_SERVER_OWNED'
    )
  )
  or exists(
    select 1
    from private._weekly_source_acl_table_contract_v1() actual
    where actual.record_class='STATEFUL_SERVER_OWNED'
      and actual.table_name<>all(v_expected_stateful)
  ) then
    raise exception 'weekly source stateful table contract differs from the independent expected set';
  end if;

  select count(*) into v_lifecycle_column_count
  from private._weekly_source_acl_lifecycle_column_contract_v1();
  if v_lifecycle_column_count<>67
     or (
       select count(distinct (table_name,column_name))
       from private._weekly_source_acl_lifecycle_column_contract_v1()
     )<>67
     or exists(
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
     )
     or exists(
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
    raise exception 'weekly source lifecycle-column contract is malformed';
  end if;


  if exists(
    select expected_key
    from pg_catalog.unnest(v_expected_lifecycle_columns) expected_key
    where not exists(
      select 1
      from private._weekly_source_acl_lifecycle_column_contract_v1() actual
      where actual.table_name||'.'||actual.column_name=expected_key
    )
  )
  or exists(
    select 1
    from private._weekly_source_acl_lifecycle_column_contract_v1() actual
    where actual.table_name||'.'||actual.column_name<>all(v_expected_lifecycle_columns)
  ) then
    raise exception 'weekly source lifecycle-column contract differs from the independent expected set';
  end if;

  select pg_catalog.string_agg(expected.table_name,',' order by expected.table_name)
    into v_names
  from private._weekly_source_acl_table_contract_v1() expected
  where pg_catalog.to_regclass(pg_catalog.format('public.%I',expected.table_name)) is null;
  if v_names is not null then
    raise exception 'weekly source contracted table(s) missing: %',v_names;
  end if;

  select pg_catalog.string_agg(c.relname,',' order by c.relname)
    into v_names
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
  if v_names is not null then
    raise exception 'unclassified weekly source table(s): %',v_names;
  end if;

  -- The role policy is deliberately separate from relation privileges. Miget's
  -- service role needs the canonical policy for SECURITY DEFINER RPCs, while
  -- the absence of every direct table privilege keeps the table surface shut.
  select count(*) into v_bad
  from private._weekly_source_acl_table_contract_v1() expected
  join pg_catalog.pg_class c
    on c.oid=pg_catalog.to_regclass(pg_catalog.format('public.%I',expected.table_name))
  where not c.relrowsecurity
     or not c.relforcerowsecurity
     or c.relowner<>(current_user::pg_catalog.regrole)::oid
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
     or exists(
       select 1
       from pg_catalog.pg_roles r
       cross join (values
         ('SELECT'),('INSERT'),('UPDATE'),('DELETE'),
         ('TRUNCATE'),('REFERENCES'),('TRIGGER')
       ) privilege(privilege_name)
       where r.rolname in ('anon','authenticated','service_role')
         and pg_catalog.has_table_privilege(
           r.oid,c.oid,privilege.privilege_name
         )
     );
  if v_bad<>0 then
    raise exception 'weekly source RLS/owner/direct-table ACL verification failed for % table(s)',v_bad;
  end if;

  select count(*) into v_bad
  from private._weekly_source_acl_table_contract_v1() expected
  join pg_catalog.pg_class c
    on c.oid=pg_catalog.to_regclass(pg_catalog.format('public.%I',expected.table_name))
  where (
    select count(*)
    from pg_catalog.pg_policy p
    where p.polrelid=c.oid
  )<>1
  or not exists(
    select 1
    from pg_catalog.pg_policy p
    where p.polrelid=c.oid
      and p.polname='cloudtms_miget_service_owner_all'
      and p.polcmd='*'
      and p.polpermissive
      and p.polroles @> array[c.relowner,v_service_role_oid]::oid[]
      and not exists(
        select 1
        from pg_catalog.unnest(p.polroles) role_oid
        where role_oid<>c.relowner
          and role_oid<>v_service_role_oid
      )
      and pg_catalog.pg_get_expr(p.polqual,p.polrelid)='true'
      and pg_catalog.pg_get_expr(p.polwithcheck,p.polrelid)='true'
  );
  if v_bad<>0 then
    raise exception 'weekly source canonical Miget policy verification failed for % table(s)',v_bad;
  end if;

  select count(*) into v_bad
  from private._weekly_source_acl_table_contract_v1() expected
  join pg_catalog.pg_class c
    on c.oid=pg_catalog.to_regclass(pg_catalog.format('public.%I',expected.table_name))
  where expected.record_class='IMMUTABLE_APPEND_ONLY'
    and (
      select count(*)
      from pg_catalog.pg_trigger t
      where t.tgrelid=c.oid
        and t.tgname='weekly_source_immutable_record_guard'
        and not t.tgisinternal
        and t.tgenabled='O'
        and t.tgfoid='private._weekly_source_immutable_record_guard_v1()'::pg_catalog.regprocedure
        and t.tgtype::integer=27
    )<>1;
  if v_bad<>0 then
    raise exception 'immutable guard missing or malformed for % table(s)',v_bad;
  end if;

  select count(*) into v_bad
  from private._weekly_source_acl_table_contract_v1() expected
  join pg_catalog.pg_class c
    on c.oid=pg_catalog.to_regclass(pg_catalog.format('public.%I',expected.table_name))
  where expected.record_class='IMMUTABLE_FACTS_WITH_LIFECYCLE'
    and (
      select count(*)
      from pg_catalog.pg_trigger t
      where t.tgrelid=c.oid
        and t.tgname='weekly_source_immutable_fact_guard'
        and not t.tgisinternal
        and t.tgenabled='O'
        and t.tgfoid='private._weekly_source_immutable_fact_guard_v1()'::pg_catalog.regprocedure
        and t.tgtype::integer=27
        and t.tgnargs=(
          select count(*)
          from private._weekly_source_acl_lifecycle_column_contract_v1() allowed
          where allowed.table_name=expected.table_name
        )
        and pg_catalog.encode(t.tgargs,'escape')=(
          select pg_catalog.string_agg(
                   allowed.column_name||pg_catalog.chr(92)||'000',
                   '' order by allowed.column_ordinal
                 )
          from private._weekly_source_acl_lifecycle_column_contract_v1() allowed
          where allowed.table_name=expected.table_name
        )
    )<>1;
  if v_bad<>0 then
    raise exception 'immutable-fact lifecycle guard missing or malformed for % table(s)',v_bad;
  end if;

  select count(*) into v_bad
  from private._weekly_source_acl_table_contract_v1() expected
  join pg_catalog.pg_class c
    on c.oid=pg_catalog.to_regclass(pg_catalog.format('public.%I',expected.table_name))
  where expected.record_class in ('IMMUTABLE_APPEND_ONLY','IMMUTABLE_FACTS_WITH_LIFECYCLE')
    and (
      select count(*)
      from pg_catalog.pg_trigger t
      where t.tgrelid=c.oid
        and t.tgname='weekly_source_immutable_truncate_guard'
        and not t.tgisinternal
        and t.tgenabled='O'
        and t.tgfoid='private._weekly_source_immutable_record_guard_v1()'::pg_catalog.regprocedure
        and t.tgtype::integer=34
        and t.tgnargs=0
    )<>1;
  if v_bad<>0 then
    raise exception 'immutable truncate guard missing or malformed for % table(s)',v_bad;
  end if;

  select count(*) into v_bad
  from private._weekly_source_acl_table_contract_v1() expected
  join pg_catalog.pg_class c
    on c.oid=pg_catalog.to_regclass(pg_catalog.format('public.%I',expected.table_name))
  join pg_catalog.pg_trigger t on t.tgrelid=c.oid
  where expected.record_class='STATEFUL_SERVER_OWNED'
    and not t.tgisinternal
    and (
      t.tgname='weekly_source_immutable_record_guard'
      or t.tgfoid='private._weekly_source_immutable_record_guard_v1()'::pg_catalog.regprocedure
      or t.tgname='weekly_source_immutable_fact_guard'
      or t.tgfoid='private._weekly_source_immutable_fact_guard_v1()'::pg_catalog.regprocedure
      or t.tgname='weekly_source_immutable_truncate_guard'
    );
  if v_bad<>0 then
    raise exception 'immutable guard incorrectly installed on % stateful table(s)',v_bad;
  end if;

  select count(*) into v_bad
  from private._weekly_source_acl_table_contract_v1() expected
  join pg_catalog.pg_class c
    on c.oid=pg_catalog.to_regclass(pg_catalog.format('public.%I',expected.table_name))
  join pg_catalog.pg_trigger t on t.tgrelid=c.oid
  where not t.tgisinternal
    and (
      (
        expected.record_class='IMMUTABLE_APPEND_ONLY'
        and t.tgtype::integer=27
        and (
          t.tgname='weekly_source_immutable_fact_guard'
          or t.tgfoid='private._weekly_source_immutable_fact_guard_v1()'::pg_catalog.regprocedure
        )
      )
      or (
        expected.record_class='IMMUTABLE_FACTS_WITH_LIFECYCLE'
        and t.tgtype::integer=27
        and (
          t.tgname='weekly_source_immutable_record_guard'
          or t.tgfoid='private._weekly_source_immutable_record_guard_v1()'::pg_catalog.regprocedure
        )
      )
    );
  if v_bad<>0 then
    raise exception 'wrong immutable guard class installed on % table(s)',v_bad;
  end if;

  select count(*) into v_bad
  from pg_catalog.pg_proc p
  where p.oid='private._weekly_source_immutable_record_guard_v1()'::pg_catalog.regprocedure
    and (
      not p.prosecdef
      or p.proowner<>(current_user::pg_catalog.regrole)::oid
      or p.prorettype<>'pg_catalog.trigger'::pg_catalog.regtype
      or not coalesce(p.proconfig,'{}'::text[]) @> array['search_path=pg_catalog, pg_temp']::text[]
    );
  if v_bad<>0 then
    raise exception 'immutable guard function shape is unsafe';
  end if;

  select count(*) into v_bad
  from pg_catalog.pg_proc p
  where p.oid='private._weekly_source_immutable_fact_guard_v1()'::pg_catalog.regprocedure
    and (
      not p.prosecdef
      or p.proowner<>(current_user::pg_catalog.regrole)::oid
      or p.prorettype<>'pg_catalog.trigger'::pg_catalog.regtype
      or not coalesce(p.proconfig,'{}'::text[]) @> array['search_path=pg_catalog, pg_temp']::text[]
    );
  if v_bad<>0 then
    raise exception 'immutable-fact guard function shape is unsafe';
  end if;

  if pg_catalog.has_function_privilege('anon','private._weekly_source_immutable_record_guard_v1()','EXECUTE')
     or pg_catalog.has_function_privilege('authenticated','private._weekly_source_immutable_record_guard_v1()','EXECUTE')
     or pg_catalog.has_function_privilege('service_role','private._weekly_source_immutable_record_guard_v1()','EXECUTE')
     or pg_catalog.has_function_privilege('anon','private._weekly_source_immutable_fact_guard_v1()','EXECUTE')
     or pg_catalog.has_function_privilege('authenticated','private._weekly_source_immutable_fact_guard_v1()','EXECUTE')
     or pg_catalog.has_function_privilege('service_role','private._weekly_source_immutable_fact_guard_v1()','EXECUTE') then
    raise exception 'immutable guard is externally executable';
  end if;

  if exists(
    select 1
    from private._weekly_source_acl_service_rpc_contract_v1()
    group by function_signature
    having count(*)<>1
  ) then
    raise exception 'duplicate Weekly Source service RPC signature in ACL contract';
  end if;

  if (
    select count(*)
    from private._weekly_source_acl_service_rpc_contract_v1()
  )<>pg_catalog.cardinality(v_expected_service_rpcs)
  or exists(
    select expected_signature
    from pg_catalog.unnest(v_expected_service_rpcs) expected_signature
    where not exists(
      select 1
      from private._weekly_source_acl_service_rpc_contract_v1() actual
      where actual.function_signature=expected_signature
    )
  )
  or exists(
    select 1
    from private._weekly_source_acl_service_rpc_contract_v1() actual
    where actual.function_signature<>all(v_expected_service_rpcs)
  ) then
    raise exception 'Weekly Source service RPC contract differs from the independent expected set';
  end if;

  -- The registered private-helper inventory must match the independent copy
  -- above exactly, must be disjoint from the service RPC contract, and every
  -- entry must be installed, owned by the release owner and withheld from
  -- every browser and service role.
  if exists(
    select 1
    from private._weekly_source_acl_private_helper_contract_v1()
    group by function_signature
    having count(*)<>1
  ) then
    raise exception 'duplicate Weekly Source private helper signature in ACL contract';
  end if;

  -- WP-56 (19 Sep 2026): count history for v_expected_private_helpers. WP-54's
  -- repeatable (17092026_1500_weekly_source_row_admission_guards_v1.sql) added
  -- two owner-only admission-guard helpers to the installed
  -- private._weekly_source_acl_private_helper_contract_v1() at 23:39Z --
  -- weekly_source_cutoff_admission_assert_v1(uuid,text,timestamp with time
  -- zone) and weekly_source_overlap_admission_assert_v1(uuid) -- without this
  -- verifier's independent expected copy being updated in the same edit (this
  -- file was last touched at 20:48Z). A run against the resulting installed
  -- state (48 registered helpers) with the pre-existing 46-entry expected copy
  -- therefore failed "Weekly Source private helper contract differs from the
  -- independent expected set", reported against build wp52_build4 in
  -- WP-52_NHSP_ROW_ORDER_PAY.md section 8. Both routines were confirmed
  -- installed, owned by the release owner and executable by no role (anon,
  -- authenticated, service_role all denied) before this entry was trusted; the
  -- repeatable was not the stale side. This verifier's expected copy is
  -- therefore corrected here: 46 -> 48, adding exactly those two signatures
  -- (see the "WP-54:" entries in v_expected_private_helpers above). Proved by
  -- re-running this file standalone against codex-ws-plan62-pg1711: it now
  -- passes (registered_private_helper_count=48) against a database built with
  -- the two helpers installed, and a reverted 46-entry copy of this array was
  -- proved to still fail the same way against that same database, and this
  -- unmodified 48-entry copy was proved to still fail against an earlier build
  -- that genuinely lacks both routines -- so the fix is additive, not a
  -- weakening of the check.
  if (
    select count(*)
    from private._weekly_source_acl_private_helper_contract_v1()
  )<>pg_catalog.cardinality(v_expected_private_helpers)
  or exists(
    select expected_signature
    from pg_catalog.unnest(v_expected_private_helpers) expected_signature
    where not exists(
      select 1
      from private._weekly_source_acl_private_helper_contract_v1() actual
      where actual.function_signature=expected_signature
    )
  )
  or exists(
    select 1
    from private._weekly_source_acl_private_helper_contract_v1() actual
    where actual.function_signature<>all(v_expected_private_helpers)
  ) then
    raise exception 'Weekly Source private helper contract differs from the independent expected set';
  end if;

  select pg_catalog.string_agg(helper.function_signature,',' order by helper.function_signature)
    into v_names
  from private._weekly_source_acl_private_helper_contract_v1() helper
  join private._weekly_source_acl_service_rpc_contract_v1() service
    on service.function_signature=helper.function_signature;
  if v_names is not null then
    raise exception 'Weekly Source private helper listed as a service RPC: %',v_names;
  end if;

  select pg_catalog.string_agg(expected_signature,',' order by expected_signature)
    into v_names
  from pg_catalog.unnest(v_expected_private_helpers) expected_signature
  where pg_catalog.to_regprocedure(expected_signature) is null;
  if v_names is not null then
    raise exception 'registered Weekly Source private helper missing: %',v_names;
  end if;

  select pg_catalog.string_agg(expected_signature,',' order by expected_signature)
    into v_names
  from pg_catalog.unnest(v_expected_private_helpers) expected_signature
  cross join lateral (
    select pg_catalog.to_regprocedure(expected_signature)::pg_catalog.oid as helper_oid
  ) resolved
  where (select p.proowner from pg_catalog.pg_proc p where p.oid=resolved.helper_oid)
          <>(current_user::pg_catalog.regrole)::oid
     or pg_catalog.has_function_privilege('anon',resolved.helper_oid,'EXECUTE')
     or pg_catalog.has_function_privilege('authenticated',resolved.helper_oid,'EXECUTE')
     or pg_catalog.has_function_privilege('service_role',resolved.helper_oid,'EXECUTE')
     or exists(
       select 1
       from pg_catalog.pg_proc p
       cross join lateral pg_catalog.aclexplode(
         coalesce(p.proacl,pg_catalog.acldefault('f',p.proowner))
       ) acl
       where p.oid=resolved.helper_oid
         and acl.grantee<>p.proowner
     );
  if v_names is not null then
    raise exception 'registered Weekly Source private helper is not owner-only: %',v_names;
  end if;

  -- Every Plan 6 routine is owner-only unless its exact OID appears in the
  -- service allowlist. PUBLIC, anon and authenticated are always denied.
  select count(*) into v_bad
  from pg_catalog.pg_proc p
  join pg_catalog.pg_namespace n on n.oid=p.pronamespace
  cross join lateral (
    select exists(
      select 1
      from private._weekly_source_acl_service_rpc_contract_v1() allowed
      where pg_catalog.to_regprocedure(allowed.function_signature)=p.oid
    ) as service_allowed
  ) expected
  where n.nspname in ('public','private')
    and (
      p.proname like 'weekly\_source\_%' escape '\'
      or p.proname like '\_weekly\_source\_%' escape '\'
      or p.proname like '\_ctms\_weekly\_source\_%' escape '\'
      or p.proname like 'weekly\_exceptional\_%' escape '\'
    )
    and (
      p.proowner<>(current_user::pg_catalog.regrole)::oid
      or pg_catalog.has_function_privilege('anon',p.oid,'EXECUTE')
      or pg_catalog.has_function_privilege('authenticated',p.oid,'EXECUTE')
      or pg_catalog.has_function_privilege('service_role',p.oid,'EXECUTE')<>expected.service_allowed
      or exists(
        select 1
        from pg_catalog.aclexplode(
          coalesce(p.proacl,pg_catalog.acldefault('f',p.proowner))
        ) acl
        where acl.grantee<>p.proowner
          and not (
            expected.service_allowed
            and acl.grantee=v_service_role_oid
            and acl.privilege_type='EXECUTE'
          )
      )
      or (expected.service_allowed and not p.prosecdef)
      or (p.prosecdef and not exists(
        select 1
        from pg_catalog.unnest(coalesce(p.proconfig,'{}'::text[])) setting
        where setting like 'search_path=%'
      ))
    );
  if v_bad<>0 then
    raise exception 'weekly source function ACL/owner/search-path verification failed for % routine(s)',v_bad;
  end if;

  select pg_catalog.string_agg(allowed.function_signature,',' order by allowed.function_signature)
    into v_names
  from private._weekly_source_acl_service_rpc_contract_v1() allowed
  where pg_catalog.to_regprocedure(allowed.function_signature) is not null
    and not pg_catalog.has_function_privilege(
      'service_role',pg_catalog.to_regprocedure(allowed.function_signature),'EXECUTE'
    );
  if v_names is not null then
    raise exception 'contracted service RPC grant missing: %',v_names;
  end if;

  -- Plan 6 must not rewrite the pre-existing Weekly import API. These five
  -- legacy routines deliberately retain the exact grants they had before this
  -- closure was installed.
  if not pg_catalog.has_function_privilege(
       'service_role','public.weekly_import_apply_phase2(uuid,text)','EXECUTE'
     )
     or not pg_catalog.has_function_privilege(
       'authenticated','public.weekly_import_apply_phase2(uuid,text)','EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'anon','public.weekly_import_apply_phase2(uuid,text)','EXECUTE'
     )
     or not pg_catalog.has_function_privilege(
       'service_role','public.weekly_import_phase2(uuid,text)','EXECUTE'
     )
     or not pg_catalog.has_function_privilege(
       'authenticated','public.weekly_import_phase2(uuid,text)','EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'anon','public.weekly_import_phase2(uuid,text)','EXECUTE'
     )
     or not pg_catalog.has_function_privilege(
       'service_role','public.weekly_import_changed_hours_phase3(uuid,text)','EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'authenticated','public.weekly_import_changed_hours_phase3(uuid,text)','EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'anon','public.weekly_import_changed_hours_phase3(uuid,text)','EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'service_role','public.weekly_import_apply_cancellations(uuid,jsonb,uuid)','EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'authenticated','public.weekly_import_apply_cancellations(uuid,jsonb,uuid)','EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'anon','public.weekly_import_apply_cancellations(uuid,jsonb,uuid)','EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'service_role','public.weekly_import_create_cancellation_corrections(uuid,uuid,uuid)','EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'authenticated','public.weekly_import_create_cancellation_corrections(uuid,uuid,uuid)','EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'anon','public.weekly_import_create_cancellation_corrections(uuid,uuid,uuid)','EXECUTE'
     ) then
    raise exception 'legacy Weekly import routine grants changed';
  end if;
end;
$verify_weekly_source_acl$;

-- Exercise the two guard implementations, independently from the catalog
-- shape checks above. Lifecycle-only updates must succeed; fact rewrites,
-- UPDATE/DELETE on append-only rows and DELETE on lifecycle facts must fail.
create temp table weekly_source_acl_lifecycle_probe(
  immutable_fact text not null,
  ordinary_source_entitlement_projection_state text not null
);
create trigger weekly_source_immutable_fact_guard
before update or delete on weekly_source_acl_lifecycle_probe
for each row execute function private._weekly_source_immutable_fact_guard_v1(
  'ordinary_source_entitlement_projection_state'
);
insert into weekly_source_acl_lifecycle_probe values ('sealed','PENDING');
update weekly_source_acl_lifecycle_probe
set ordinary_source_entitlement_projection_state='PUBLISHED';
do $lifecycle_fact_rewrite$
begin
  begin
    update weekly_source_acl_lifecycle_probe set immutable_fact='changed';
    raise exception 'LIFECYCLE_FACT_REWRITE_WAS_ACCEPTED';
  exception when object_not_in_prerequisite_state then
    if sqlerrm<>'WEEKLY_SOURCE_IMMUTABLE_FACT' then raise; end if;
  end;
  begin
    delete from weekly_source_acl_lifecycle_probe;
    raise exception 'LIFECYCLE_DELETE_WAS_ACCEPTED';
  exception when object_not_in_prerequisite_state then
    if sqlerrm<>'WEEKLY_SOURCE_IMMUTABLE_RECORD' then raise; end if;
  end;
end;
$lifecycle_fact_rewrite$;

create temp table weekly_source_acl_append_probe(immutable_fact text not null);
create trigger weekly_source_immutable_record_guard
before update or delete on weekly_source_acl_append_probe
for each row execute function private._weekly_source_immutable_record_guard_v1();
insert into weekly_source_acl_append_probe values ('sealed');
do $append_only_rewrite$
begin
  begin
    update weekly_source_acl_append_probe set immutable_fact='changed';
    raise exception 'APPEND_ONLY_UPDATE_WAS_ACCEPTED';
  exception when object_not_in_prerequisite_state then
    if sqlerrm<>'WEEKLY_SOURCE_IMMUTABLE_RECORD' then raise; end if;
  end;
  begin
    delete from weekly_source_acl_append_probe;
    raise exception 'APPEND_ONLY_DELETE_WAS_ACCEPTED';
  exception when object_not_in_prerequisite_state then
    if sqlerrm<>'WEEKLY_SOURCE_IMMUTABLE_RECORD' then raise; end if;
  end;
end;
$append_only_rewrite$;

select pg_catalog.jsonb_build_object(
  'ok',true,
  'verification','weekly_source_acl_contract_v1',
  'table_count',(select count(*) from private._weekly_source_acl_table_contract_v1()),
  'immutable_table_count',(
    select count(*)
    from private._weekly_source_acl_table_contract_v1()
    where record_class='IMMUTABLE_APPEND_ONLY'
  ),
  'immutable_facts_with_lifecycle_table_count',(
    select count(*)
    from private._weekly_source_acl_table_contract_v1()
    where record_class='IMMUTABLE_FACTS_WITH_LIFECYCLE'
  ),
  'lifecycle_column_count',(
    select count(*)
    from private._weekly_source_acl_lifecycle_column_contract_v1()
  ),
  'stateful_server_owned_table_count',(
    select count(*)
    from private._weekly_source_acl_table_contract_v1()
    where record_class='STATEFUL_SERVER_OWNED'
  ),
  'weekly_source_routine_count',(
    select count(*)
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where n.nspname in ('public','private')
      and (
        p.proname like 'weekly\_source\_%' escape '\'
        or p.proname like '\_weekly\_source\_%' escape '\'
        or p.proname like '\_ctms\_weekly\_source\_%' escape '\'
        or p.proname like 'weekly\_exceptional\_%' escape '\'
      )
  ),
  'present_service_rpc_count',(
    select count(*)
    from private._weekly_source_acl_service_rpc_contract_v1() allowed
    where pg_catalog.to_regprocedure(allowed.function_signature) is not null
  ),
  'registered_private_helper_count',(
    select count(*)
    from private._weekly_source_acl_private_helper_contract_v1()
  ),
  'direct_table_roles',pg_catalog.jsonb_build_array('PUBLIC','anon','authenticated','service_role'),
  'direct_table_access',false,
  'browser_rpc_access',false
) as result;

rollback;
