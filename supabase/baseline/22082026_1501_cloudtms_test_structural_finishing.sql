-- CloudTMS schema-only baseline finishing authority generated from TEST on 22 August 2026.
-- Applied after baseline routines/views so defaults, constraints, indexes and RLS resolve safely.
-- Contains no application rows or sequence values.

\set ON_ERROR_STOP on
set check_function_bodies = off;
set search_path = pg_catalog, public, extensions;

-- private.banking_pay_workbench_candidate_scope_registry.bootstrap_cursor_json
alter table private.banking_pay_workbench_candidate_scope_registry alter column bootstrap_cursor_json set default '{}'::jsonb;

-- private.banking_pay_workbench_candidate_scope_registry.bootstrap_rows_seen
alter table private.banking_pay_workbench_candidate_scope_registry alter column bootstrap_rows_seen set default 0;

-- private.banking_pay_workbench_candidate_scope_registry.bootstrap_timesheets_registered
alter table private.banking_pay_workbench_candidate_scope_registry alter column bootstrap_timesheets_registered set default 0;

-- private.banking_pay_workbench_candidate_scope_registry.created_at_utc
alter table private.banking_pay_workbench_candidate_scope_registry alter column created_at_utc set default clock_timestamp();

-- private.banking_pay_workbench_candidate_scope_registry.current_source_change_seq
alter table private.banking_pay_workbench_candidate_scope_registry alter column current_source_change_seq set default 0;

-- private.banking_pay_workbench_candidate_scope_registry.dirty_generation
alter table private.banking_pay_workbench_candidate_scope_registry alter column dirty_generation set default 0;

-- private.banking_pay_workbench_candidate_scope_registry.evaluated_generation
alter table private.banking_pay_workbench_candidate_scope_registry alter column evaluated_generation set default 0;

-- private.banking_pay_workbench_candidate_scope_registry.failure_json
alter table private.banking_pay_workbench_candidate_scope_registry alter column failure_json set default '{}'::jsonb;

-- private.banking_pay_workbench_candidate_scope_registry.initialisation_status
alter table private.banking_pay_workbench_candidate_scope_registry alter column initialisation_status set default 'UNINITIALISED'::text;

-- private.banking_pay_workbench_candidate_scope_registry.last_dirtied_at_utc
alter table private.banking_pay_workbench_candidate_scope_registry alter column last_dirtied_at_utc set default clock_timestamp();

-- private.banking_pay_workbench_candidate_scope_registry.last_dirty_reason
alter table private.banking_pay_workbench_candidate_scope_registry alter column last_dirty_reason set default 'INITIAL_UNCLASSIFIED'::text;

-- private.banking_pay_workbench_candidate_scope_registry.updated_at_utc
alter table private.banking_pay_workbench_candidate_scope_registry alter column updated_at_utc set default clock_timestamp();

-- private.banking_pay_workbench_canonical_stage_lines.created_at_utc
alter table private.banking_pay_workbench_canonical_stage_lines alter column created_at_utc set default clock_timestamp();

-- private.banking_pay_workbench_canonical_stage_lines.stage_status
alter table private.banking_pay_workbench_canonical_stage_lines alter column stage_status set default 'STAGED'::text;

-- private.banking_pay_workbench_economic_build_fact_pages.completed_at_utc
alter table private.banking_pay_workbench_economic_build_fact_pages alter column completed_at_utc set default clock_timestamp();

-- private.banking_pay_workbench_economic_build_fact_pages.created_at_utc
alter table private.banking_pay_workbench_economic_build_fact_pages alter column created_at_utc set default clock_timestamp();

-- private.banking_pay_workbench_economic_build_fact_pages.id
alter table private.banking_pay_workbench_economic_build_fact_pages alter column id set default gen_random_uuid();

-- private.banking_pay_workbench_economic_build_fact_pages.is_family_final
alter table private.banking_pay_workbench_economic_build_fact_pages alter column is_family_final set default false;

-- private.banking_pay_workbench_economic_build_fact_pages.status
alter table private.banking_pay_workbench_economic_build_fact_pages alter column status set default 'COMPLETED'::text;

-- private.banking_pay_workbench_economic_build_facts.created_at_utc
alter table private.banking_pay_workbench_economic_build_facts alter column created_at_utc set default clock_timestamp();

-- private.banking_pay_workbench_economic_build_facts.diagnostic_json
alter table private.banking_pay_workbench_economic_build_facts alter column diagnostic_json set default '{}'::jsonb;

-- private.banking_pay_workbench_economic_build_facts.payload_schema_version
alter table private.banking_pay_workbench_economic_build_facts alter column payload_schema_version set default 1;

-- private.banking_pay_workbench_economic_build_facts.source_payload_json
alter table private.banking_pay_workbench_economic_build_facts alter column source_payload_json set default '{}'::jsonb;

-- private.banking_pay_workbench_economic_build_scope.closure_family_ordinal
alter table private.banking_pay_workbench_economic_build_scope alter column closure_family_ordinal set default 1;

-- private.banking_pay_workbench_economic_build_scope.closure_processed_edge_count
alter table private.banking_pay_workbench_economic_build_scope alter column closure_processed_edge_count set default 0;

-- private.banking_pay_workbench_economic_build_scope.closure_processed_emission_count
alter table private.banking_pay_workbench_economic_build_scope alter column closure_processed_emission_count set default 0;

-- private.banking_pay_workbench_economic_build_scope.closure_status
alter table private.banking_pay_workbench_economic_build_scope alter column closure_status set default 'PENDING'::text;

-- private.banking_pay_workbench_economic_build_scope.completed_fact_families
alter table private.banking_pay_workbench_economic_build_scope alter column completed_fact_families set default ARRAY[]::text[];

-- private.banking_pay_workbench_economic_build_scope.created_at_utc
alter table private.banking_pay_workbench_economic_build_scope alter column created_at_utc set default clock_timestamp();

-- private.banking_pay_workbench_economic_build_scope.fact_row_count
alter table private.banking_pay_workbench_economic_build_scope alter column fact_row_count set default 0;

-- private.banking_pay_workbench_economic_build_scope.updated_at_utc
alter table private.banking_pay_workbench_economic_build_scope alter column updated_at_utc set default clock_timestamp();

-- private.banking_pay_workbench_economic_builds.attestation_json
alter table private.banking_pay_workbench_economic_builds alter column attestation_json set default '{}'::jsonb;

-- private.banking_pay_workbench_economic_builds.build_token
alter table private.banking_pay_workbench_economic_builds alter column build_token set default gen_random_uuid();

-- private.banking_pay_workbench_economic_builds.canonical_count
alter table private.banking_pay_workbench_economic_builds alter column canonical_count set default 0;

-- private.banking_pay_workbench_economic_builds.cleanup_cursor_json
alter table private.banking_pay_workbench_economic_builds alter column cleanup_cursor_json set default '{}'::jsonb;

-- private.banking_pay_workbench_economic_builds.closure_cursor_json
alter table private.banking_pay_workbench_economic_builds alter column closure_cursor_json set default '{}'::jsonb;

-- private.banking_pay_workbench_economic_builds.complexity_vector_json
alter table private.banking_pay_workbench_economic_builds alter column complexity_vector_json set default '{}'::jsonb;

-- private.banking_pay_workbench_economic_builds.created_at_utc
alter table private.banking_pay_workbench_economic_builds alter column created_at_utc set default clock_timestamp();

-- private.banking_pay_workbench_economic_builds.dependency_edge_count
alter table private.banking_pay_workbench_economic_builds alter column dependency_edge_count set default 0;

-- private.banking_pay_workbench_economic_builds.dependency_edge_stream_complete
alter table private.banking_pay_workbench_economic_builds alter column dependency_edge_stream_complete set default false;

-- private.banking_pay_workbench_economic_builds.dependency_edge_stream_terminal_key_json
alter table private.banking_pay_workbench_economic_builds alter column dependency_edge_stream_terminal_key_json set default '{}'::jsonb;

-- private.banking_pay_workbench_economic_builds.dependency_node_count
alter table private.banking_pay_workbench_economic_builds alter column dependency_node_count set default 0;

-- private.banking_pay_workbench_economic_builds.edge_tag_stream_complete
alter table private.banking_pay_workbench_economic_builds alter column edge_tag_stream_complete set default false;

-- private.banking_pay_workbench_economic_builds.edge_tag_stream_terminal_key_json
alter table private.banking_pay_workbench_economic_builds alter column edge_tag_stream_terminal_key_json set default '{}'::jsonb;

-- private.banking_pay_workbench_economic_builds.envelope_evidence_json
alter table private.banking_pay_workbench_economic_builds alter column envelope_evidence_json set default '{}'::jsonb;

-- private.banking_pay_workbench_economic_builds.envelope_snapshot_json
alter table private.banking_pay_workbench_economic_builds alter column envelope_snapshot_json set default '{}'::jsonb;

-- private.banking_pay_workbench_economic_builds.fact_count
alter table private.banking_pay_workbench_economic_builds alter column fact_count set default 0;

-- private.banking_pay_workbench_economic_builds.fact_cursor_json
alter table private.banking_pay_workbench_economic_builds alter column fact_cursor_json set default '{}'::jsonb;

-- private.banking_pay_workbench_economic_builds.failure_json
alter table private.banking_pay_workbench_economic_builds alter column failure_json set default '{}'::jsonb;

-- private.banking_pay_workbench_economic_builds.id
alter table private.banking_pay_workbench_economic_builds alter column id set default gen_random_uuid();

-- private.banking_pay_workbench_economic_builds.last_stable_ordinal
alter table private.banking_pay_workbench_economic_builds alter column last_stable_ordinal set default 0;

-- private.banking_pay_workbench_economic_builds.publication_cursor_json
alter table private.banking_pay_workbench_economic_builds alter column publication_cursor_json set default '{}'::jsonb;

-- private.banking_pay_workbench_economic_builds.row_seal_count
alter table private.banking_pay_workbench_economic_builds alter column row_seal_count set default 0;

-- private.banking_pay_workbench_economic_builds.scope_count
alter table private.banking_pay_workbench_economic_builds alter column scope_count set default 0;

-- private.banking_pay_workbench_economic_builds.scope_cursor_json
alter table private.banking_pay_workbench_economic_builds alter column scope_cursor_json set default '{}'::jsonb;

-- private.banking_pay_workbench_economic_builds.seed_scope_count
alter table private.banking_pay_workbench_economic_builds alter column seed_scope_count set default 0;

-- private.banking_pay_workbench_economic_builds.stage_version
alter table private.banking_pay_workbench_economic_builds alter column stage_version set default 1;

-- private.banking_pay_workbench_economic_builds.tagged_edge_count
alter table private.banking_pay_workbench_economic_builds alter column tagged_edge_count set default 0;

-- private.banking_pay_workbench_economic_builds.unit_count
alter table private.banking_pay_workbench_economic_builds alter column unit_count set default 0;

-- private.banking_pay_workbench_economic_builds.updated_at_utc
alter table private.banking_pay_workbench_economic_builds alter column updated_at_utc set default clock_timestamp();

-- private.banking_pay_workbench_queue_scan_state.created_at_utc
alter table private.banking_pay_workbench_queue_scan_state alter column created_at_utc set default clock_timestamp();

-- private.banking_pay_workbench_queue_scan_state.sweep_generation
alter table private.banking_pay_workbench_queue_scan_state alter column sweep_generation set default 0;

-- private.banking_pay_workbench_queue_scan_state.updated_at_utc
alter table private.banking_pay_workbench_queue_scan_state alter column updated_at_utc set default clock_timestamp();

-- private.banking_pay_workbench_stage_attempts.attempt_nonce
alter table private.banking_pay_workbench_stage_attempts alter column attempt_nonce set default gen_random_uuid();

-- private.banking_pay_workbench_stage_attempts.attempt_status
alter table private.banking_pay_workbench_stage_attempts alter column attempt_status set default 'STARTED'::text;

-- private.banking_pay_workbench_stage_attempts.created_at_utc
alter table private.banking_pay_workbench_stage_attempts alter column created_at_utc set default clock_timestamp();

-- private.banking_pay_workbench_stage_attempts.error_json
alter table private.banking_pay_workbench_stage_attempts alter column error_json set default '{}'::jsonb;

-- private.banking_pay_workbench_stage_attempts.id
alter table private.banking_pay_workbench_stage_attempts alter column id set default gen_random_uuid();

-- private.banking_pay_workbench_stage_attempts.updated_at_utc
alter table private.banking_pay_workbench_stage_attempts alter column updated_at_utc set default clock_timestamp();

-- private.banking_pay_workbench_timesheet_scope_state.dirty_generation
alter table private.banking_pay_workbench_timesheet_scope_state alter column dirty_generation set default 0;

-- private.banking_pay_workbench_timesheet_scope_state.economic_state
alter table private.banking_pay_workbench_timesheet_scope_state alter column economic_state set default 'DIRTY'::text;

-- private.banking_pay_workbench_timesheet_scope_state.last_dirtied_at_utc
alter table private.banking_pay_workbench_timesheet_scope_state alter column last_dirtied_at_utc set default clock_timestamp();

-- private.banking_pay_workbench_timesheet_scope_state.registered_at_utc
alter table private.banking_pay_workbench_timesheet_scope_state alter column registered_at_utc set default clock_timestamp();

-- private.banking_pay_workbench_timesheet_scope_state.updated_at_utc
alter table private.banking_pay_workbench_timesheet_scope_state alter column updated_at_utc set default clock_timestamp();

-- private.candidate_daily_authority_scopes.authority_mode
alter table private.candidate_daily_authority_scopes alter column authority_mode set default 'GOOGLE_PRIMARY'::text;

-- private.candidate_daily_authority_scopes.canonical_version
alter table private.candidate_daily_authority_scopes alter column canonical_version set default 0;

-- private.candidate_daily_authority_scopes.created_at_utc
alter table private.candidate_daily_authority_scopes alter column created_at_utc set default now();

-- private.candidate_daily_authority_scopes.transition_in_progress
alter table private.candidate_daily_authority_scopes alter column transition_in_progress set default false;

-- private.candidate_daily_authority_scopes.updated_at_utc
alter table private.candidate_daily_authority_scopes alter column updated_at_utc set default now();

-- private.candidate_daily_authority_transitions.created_at_utc
alter table private.candidate_daily_authority_transitions alter column created_at_utc set default now();

-- private.candidate_daily_authority_transitions.transition_id
alter table private.candidate_daily_authority_transitions alter column transition_id set default gen_random_uuid();

-- private.candidate_daily_batch_receipts.batch_receipt_id
alter table private.candidate_daily_batch_receipts alter column batch_receipt_id set default gen_random_uuid();

-- private.candidate_daily_batch_receipts.created_at_utc
alter table private.candidate_daily_batch_receipts alter column created_at_utc set default now();

-- private.candidate_daily_batch_receipts.item_receipt_ids_json
alter table private.candidate_daily_batch_receipts alter column item_receipt_ids_json set default '[]'::jsonb;

-- private.candidate_daily_batch_receipts.state
alter table private.candidate_daily_batch_receipts alter column state set default 'IN_PROGRESS'::text;

-- private.candidate_daily_batch_receipts.updated_at_utc
alter table private.candidate_daily_batch_receipts alter column updated_at_utc set default now();

-- private.candidate_daily_entitlements.created_at_utc
alter table private.candidate_daily_entitlements alter column created_at_utc set default now();

-- private.candidate_daily_entitlements.enabled
alter table private.candidate_daily_entitlements alter column enabled set default false;

-- private.candidate_daily_entitlements.updated_at_utc
alter table private.candidate_daily_entitlements alter column updated_at_utc set default now();

-- private.candidate_daily_external_effect_receipts.attempt_count
alter table private.candidate_daily_external_effect_receipts alter column attempt_count set default 1;

-- private.candidate_daily_external_effect_receipts.created_at_utc
alter table private.candidate_daily_external_effect_receipts alter column created_at_utc set default now();

-- private.candidate_daily_external_effect_receipts.effect_receipt_id
alter table private.candidate_daily_external_effect_receipts alter column effect_receipt_id set default gen_random_uuid();

-- private.candidate_daily_external_effect_receipts.safe_evidence_json
alter table private.candidate_daily_external_effect_receipts alter column safe_evidence_json set default '{}'::jsonb;

-- private.candidate_daily_external_effect_receipts.state
alter table private.candidate_daily_external_effect_receipts alter column state set default 'IN_PROGRESS'::text;

-- private.candidate_daily_external_effect_receipts.updated_at_utc
alter table private.candidate_daily_external_effect_receipts alter column updated_at_utc set default now();

-- private.candidate_daily_source_links.created_at_utc
alter table private.candidate_daily_source_links alter column created_at_utc set default now();

-- private.candidate_daily_source_links.link_id
alter table private.candidate_daily_source_links alter column link_id set default gen_random_uuid();

-- private.candidate_daily_source_links.updated_at_utc
alter table private.candidate_daily_source_links alter column updated_at_utc set default now();

-- private.candidate_daily_source_links.valid_from_utc
alter table private.candidate_daily_source_links alter column valid_from_utc set default now();

-- private.candidate_daily_sync_state.accepted_canonical_cursor
alter table private.candidate_daily_sync_state alter column accepted_canonical_cursor set default 0;

-- private.candidate_daily_sync_state.created_at_utc
alter table private.candidate_daily_sync_state alter column created_at_utc set default now();

-- private.candidate_daily_sync_state.deferred_count
alter table private.candidate_daily_sync_state alter column deferred_count set default 0;

-- private.candidate_daily_sync_state.delivered_visible_cursor
alter table private.candidate_daily_sync_state alter column delivered_visible_cursor set default 0;

-- private.candidate_daily_sync_state.effective_visible_cursor
alter table private.candidate_daily_sync_state alter column effective_visible_cursor set default 0;

-- private.candidate_daily_sync_state.overlay_proof_cursor
alter table private.candidate_daily_sync_state alter column overlay_proof_cursor set default 0;

-- private.candidate_daily_sync_state.pending_count
alter table private.candidate_daily_sync_state alter column pending_count set default 0;

-- private.candidate_daily_sync_state.required_visible_cursor
alter table private.candidate_daily_sync_state alter column required_visible_cursor set default 0;

-- private.candidate_daily_sync_state.retry_count
alter table private.candidate_daily_sync_state alter column retry_count set default 0;

-- private.candidate_daily_sync_state.state
alter table private.candidate_daily_sync_state alter column state set default 'READY'::text;

-- private.candidate_daily_sync_state.terminal_count
alter table private.candidate_daily_sync_state alter column terminal_count set default 0;

-- private.candidate_daily_sync_state.updated_at_utc
alter table private.candidate_daily_sync_state alter column updated_at_utc set default now();

-- private.invoice_async_snapshot_hmac_keys.created_at_utc
alter table private.invoice_async_snapshot_hmac_keys alter column created_at_utc set default now();

-- private.invoice_async_snapshot_hmac_keys.is_current
alter table private.invoice_async_snapshot_hmac_keys alter column is_current set default false;

-- public.app_change_counters.seq
alter table public.app_change_counters alter column seq set default 0;

-- public.app_change_counters.updated_at
alter table public.app_change_counters alter column updated_at set default now();

-- public.assignment_band_mappings.active
alter table public.assignment_band_mappings alter column active set default true;

-- public.assignment_band_mappings.created_at
alter table public.assignment_band_mappings alter column created_at set default now();

-- public.assignment_band_mappings.id
alter table public.assignment_band_mappings alter column id set default gen_random_uuid();

-- public.assignment_band_mappings.updated_at
alter table public.assignment_band_mappings alter column updated_at set default now();

-- public.audit_events.id
alter table public.audit_events alter column id set default gen_random_uuid();

-- public.audit_events.ts_utc
alter table public.audit_events alter column ts_utc set default now();

-- public.bank_name_checks.created_at_utc
alter table public.bank_name_checks alter column created_at_utc set default now();

-- public.bank_name_checks.id
alter table public.bank_name_checks alter column id set default gen_random_uuid();

-- public.bank_name_checks.updated_at_utc
alter table public.bank_name_checks alter column updated_at_utc set default now();

-- public.bank_payee_map.created_at_utc
alter table public.bank_payee_map alter column created_at_utc set default now();

-- public.bank_payee_map.id
alter table public.bank_payee_map alter column id set default gen_random_uuid();

-- public.bank_payee_map.updated_at_utc
alter table public.bank_payee_map alter column updated_at_utc set default now();

-- public.bank_provider_webhook_configs.created_at_utc
alter table public.bank_provider_webhook_configs alter column created_at_utc set default now();

-- public.bank_provider_webhook_configs.event_types
alter table public.bank_provider_webhook_configs alter column event_types set default '[]'::jsonb;

-- public.bank_provider_webhook_configs.id
alter table public.bank_provider_webhook_configs alter column id set default gen_random_uuid();

-- public.bank_provider_webhook_configs.meta_json
alter table public.bank_provider_webhook_configs alter column meta_json set default '{}'::jsonb;

-- public.bank_provider_webhook_configs.rail_env
alter table public.bank_provider_webhook_configs alter column rail_env set default 'PROD'::text;

-- public.bank_provider_webhook_configs.status
alter table public.bank_provider_webhook_configs alter column status set default 'ACTIVE'::text;

-- public.bank_provider_webhook_configs.updated_at_utc
alter table public.bank_provider_webhook_configs alter column updated_at_utc set default now();

-- public.bank_provider_webhook_receipts.attempt_count
alter table public.bank_provider_webhook_receipts alter column attempt_count set default 1;

-- public.bank_provider_webhook_receipts.created_at_utc
alter table public.bank_provider_webhook_receipts alter column created_at_utc set default now();

-- public.bank_provider_webhook_receipts.id
alter table public.bank_provider_webhook_receipts alter column id set default gen_random_uuid();

-- public.bank_provider_webhook_receipts.ingest_results_json
alter table public.bank_provider_webhook_receipts alter column ingest_results_json set default '[]'::jsonb;

-- public.bank_provider_webhook_receipts.normalised_events_json
alter table public.bank_provider_webhook_receipts alter column normalised_events_json set default '[]'::jsonb;

-- public.bank_provider_webhook_receipts.rail_env
alter table public.bank_provider_webhook_receipts alter column rail_env set default 'PROD'::text;

-- public.bank_provider_webhook_receipts.raw_headers_redacted
alter table public.bank_provider_webhook_receipts alter column raw_headers_redacted set default '{}'::jsonb;

-- public.bank_provider_webhook_receipts.request_received_at_utc
alter table public.bank_provider_webhook_receipts alter column request_received_at_utc set default now();

-- public.bank_provider_webhook_receipts.signature_valid
alter table public.bank_provider_webhook_receipts alter column signature_valid set default false;

-- public.bank_provider_webhook_receipts.status
alter table public.bank_provider_webhook_receipts alter column status set default 'RECEIVED'::text;

-- public.bank_provider_webhook_receipts.updated_at_utc
alter table public.bank_provider_webhook_receipts alter column updated_at_utc set default now();

-- public.banking_alert_acknowledgements.acknowledge_scope
alter table public.banking_alert_acknowledgements alter column acknowledge_scope set default 'USER'::text;

-- public.banking_alert_acknowledgements.acknowledged_at_utc
alter table public.banking_alert_acknowledgements alter column acknowledged_at_utc set default now();

-- public.banking_alert_acknowledgements.alert_payload_json
alter table public.banking_alert_acknowledgements alter column alert_payload_json set default '{}'::jsonb;

-- public.banking_alert_acknowledgements.entity_kind
alter table public.banking_alert_acknowledgements alter column entity_kind set default 'pay_batch'::text;

-- public.banking_alert_acknowledgements.id
alter table public.banking_alert_acknowledgements alter column id set default gen_random_uuid();

-- public.banking_alert_acknowledgements.resolved_at_ack
alter table public.banking_alert_acknowledgements alter column resolved_at_ack set default false;

-- public.banking_alert_display_summary.created_at_utc
alter table public.banking_alert_display_summary alter column created_at_utc set default now();

-- public.banking_alert_display_summary.id
alter table public.banking_alert_display_summary alter column id set default gen_random_uuid();

-- public.banking_alert_display_summary.summary_json
alter table public.banking_alert_display_summary alter column summary_json set default '{}'::jsonb;

-- public.banking_alert_display_summary.unacknowledged_count
alter table public.banking_alert_display_summary alter column unacknowledged_count set default 0;

-- public.banking_alert_display_summary.updated_at_utc
alter table public.banking_alert_display_summary alter column updated_at_utc set default now();

-- public.banking_alert_success_events.created_at_utc
alter table public.banking_alert_success_events alter column created_at_utc set default now();

-- public.banking_alert_success_events.expires_at_utc
alter table public.banking_alert_success_events alter column expires_at_utc set default (now() + '365 days'::interval);

-- public.banking_alert_success_events.id
alter table public.banking_alert_success_events alter column id set default gen_random_uuid();

-- public.banking_alert_success_events.occurred_at_utc
alter table public.banking_alert_success_events alter column occurred_at_utc set default now();

-- public.banking_alert_success_events.payload_json
alter table public.banking_alert_success_events alter column payload_json set default '{}'::jsonb;

-- public.banking_alert_success_events.updated_at_utc
alter table public.banking_alert_success_events alter column updated_at_utc set default now();

-- public.banking_alert_user_preferences.alert_kind_blocklist
alter table public.banking_alert_user_preferences alter column alert_kind_blocklist set default '[]'::jsonb;

-- public.banking_alert_user_preferences.created_at_utc
alter table public.banking_alert_user_preferences alter column created_at_utc set default now();

-- public.banking_alert_user_preferences.enabled
alter table public.banking_alert_user_preferences alter column enabled set default true;

-- public.banking_alert_user_preferences.failure_reason_blocklist
alter table public.banking_alert_user_preferences alter column failure_reason_blocklist set default '[]'::jsonb;

-- public.banking_alert_user_preferences.id
alter table public.banking_alert_user_preferences alter column id set default gen_random_uuid();

-- public.banking_alert_user_preferences.include_action_required
alter table public.banking_alert_user_preferences alter column include_action_required set default true;

-- public.banking_alert_user_preferences.include_informational_alerts
alter table public.banking_alert_user_preferences alter column include_informational_alerts set default false;

-- public.banking_alert_user_preferences.include_progress_alerts
alter table public.banking_alert_user_preferences alter column include_progress_alerts set default true;

-- public.banking_alert_user_preferences.include_success_alerts
alter table public.banking_alert_user_preferences alter column include_success_alerts set default true;

-- public.banking_alert_user_preferences.muted_pay_batch_ids
alter table public.banking_alert_user_preferences alter column muted_pay_batch_ids set default '[]'::jsonb;

-- public.banking_alert_user_preferences.muted_provider_keys
alter table public.banking_alert_user_preferences alter column muted_provider_keys set default '[]'::jsonb;

-- public.banking_alert_user_preferences.severity_min
alter table public.banking_alert_user_preferences alter column severity_min set default 'ACTION_REQUIRED'::text;

-- public.banking_alert_user_preferences.updated_at_utc
alter table public.banking_alert_user_preferences alter column updated_at_utc set default now();

-- public.banking_pay_batch_change_signals.alert_version
alter table public.banking_pay_batch_change_signals alter column alert_version set default 0;

-- public.banking_pay_batch_change_signals.correction_progress_version
alter table public.banking_pay_batch_change_signals alter column correction_progress_version set default 0;

-- public.banking_pay_batch_change_signals.last_change_scope_json
alter table public.banking_pay_batch_change_signals alter column last_change_scope_json set default '{}'::jsonb;

-- public.banking_pay_batch_change_signals.last_changed_at_utc
alter table public.banking_pay_batch_change_signals alter column last_changed_at_utc set default now();

-- public.banking_pay_batch_change_signals.last_changed_candidate_ids
alter table public.banking_pay_batch_change_signals alter column last_changed_candidate_ids set default '[]'::jsonb;

-- public.banking_pay_batch_change_signals.last_changed_pay_batch_item_ids
alter table public.banking_pay_batch_change_signals alter column last_changed_pay_batch_item_ids set default '[]'::jsonb;

-- public.banking_pay_batch_change_signals.last_changed_transfer_ids
alter table public.banking_pay_batch_change_signals alter column last_changed_transfer_ids set default '[]'::jsonb;

-- public.banking_pay_batch_change_signals.overview_version
alter table public.banking_pay_batch_change_signals alter column overview_version set default 0;

-- public.banking_pay_batch_change_signals.payment_status_version
alter table public.banking_pay_batch_change_signals alter column payment_status_version set default 0;

-- public.banking_pay_batch_change_signals.updated_at_utc
alter table public.banking_pay_batch_change_signals alter column updated_at_utc set default now();

-- public.banking_pay_batch_change_signals.version
alter table public.banking_pay_batch_change_signals alter column version set default 0;

-- public.banking_pay_operation_candidate_allocation_rows.allocated_amount
alter table public.banking_pay_operation_candidate_allocation_rows alter column allocated_amount set default 0;

-- public.banking_pay_operation_candidate_allocation_rows.allocation_basis_json
alter table public.banking_pay_operation_candidate_allocation_rows alter column allocation_basis_json set default '{}'::jsonb;

-- public.banking_pay_operation_candidate_allocation_rows.created_at_utc
alter table public.banking_pay_operation_candidate_allocation_rows alter column created_at_utc set default now();

-- public.banking_pay_operation_candidate_allocation_rows.id
alter table public.banking_pay_operation_candidate_allocation_rows alter column id set default gen_random_uuid();

-- public.banking_pay_operation_candidate_allocation_rows.sort_order
alter table public.banking_pay_operation_candidate_allocation_rows alter column sort_order set default 0;

-- public.banking_pay_operation_candidate_allocation_rows.status
alter table public.banking_pay_operation_candidate_allocation_rows alter column status set default 'PENDING'::text;

-- public.banking_pay_operation_candidate_allocation_rows.updated_at_utc
alter table public.banking_pay_operation_candidate_allocation_rows alter column updated_at_utc set default now();

-- public.banking_pay_operation_candidate_scope.allocation_basis_json
alter table public.banking_pay_operation_candidate_scope alter column allocation_basis_json set default '{}'::jsonb;

-- public.banking_pay_operation_candidate_scope.baseline_component_rows_json
alter table public.banking_pay_operation_candidate_scope alter column baseline_component_rows_json set default '[]'::jsonb;

-- public.banking_pay_operation_candidate_scope.candidate_totals_json
alter table public.banking_pay_operation_candidate_scope alter column candidate_totals_json set default '{}'::jsonb;

-- public.banking_pay_operation_candidate_scope.created_at_utc
alter table public.banking_pay_operation_candidate_scope alter column created_at_utc set default now();

-- public.banking_pay_operation_candidate_scope.effective_candidate_fragment_json
alter table public.banking_pay_operation_candidate_scope alter column effective_candidate_fragment_json set default '{}'::jsonb;

-- public.banking_pay_operation_candidate_scope.effective_canonical_preview_lines_json
alter table public.banking_pay_operation_candidate_scope alter column effective_canonical_preview_lines_json set default '[]'::jsonb;

-- public.banking_pay_operation_candidate_scope.effective_case_resolution_states_json
alter table public.banking_pay_operation_candidate_scope alter column effective_case_resolution_states_json set default '{}'::jsonb;

-- public.banking_pay_operation_candidate_scope.effective_non_paye_payee_json
alter table public.banking_pay_operation_candidate_scope alter column effective_non_paye_payee_json set default '{}'::jsonb;

-- public.banking_pay_operation_candidate_scope.effective_paye_candidate_json
alter table public.banking_pay_operation_candidate_scope alter column effective_paye_candidate_json set default '{}'::jsonb;

-- public.banking_pay_operation_candidate_scope.effective_payees_json
alter table public.banking_pay_operation_candidate_scope alter column effective_payees_json set default '[]'::jsonb;

-- public.banking_pay_operation_candidate_scope.effective_summary_fragment_json
alter table public.banking_pay_operation_candidate_scope alter column effective_summary_fragment_json set default '{}'::jsonb;

-- public.banking_pay_operation_candidate_scope.hidden_recovery_template_lines_json
alter table public.banking_pay_operation_candidate_scope alter column hidden_recovery_template_lines_json set default '[]'::jsonb;

-- public.banking_pay_operation_candidate_scope.id
alter table public.banking_pay_operation_candidate_scope alter column id set default gen_random_uuid();

-- public.banking_pay_operation_candidate_scope.selected_canonical_preview_lines_json
alter table public.banking_pay_operation_candidate_scope alter column selected_canonical_preview_lines_json set default '[]'::jsonb;

-- public.banking_pay_operation_candidate_scope.selected_finance_case_ids_json
alter table public.banking_pay_operation_candidate_scope alter column selected_finance_case_ids_json set default '[]'::jsonb;

-- public.banking_pay_operation_candidate_scope.selected_preview_row_ids_json
alter table public.banking_pay_operation_candidate_scope alter column selected_preview_row_ids_json set default '[]'::jsonb;

-- public.banking_pay_operation_candidate_scope.selected_timesheet_ids_json
alter table public.banking_pay_operation_candidate_scope alter column selected_timesheet_ids_json set default '[]'::jsonb;

-- public.banking_pay_operation_candidate_scope.status
alter table public.banking_pay_operation_candidate_scope alter column status set default 'PENDING'::text;

-- public.banking_pay_operation_candidate_scope.updated_at_utc
alter table public.banking_pay_operation_candidate_scope alter column updated_at_utc set default now();

-- public.banking_pay_operation_chunks.completed_count
alter table public.banking_pay_operation_chunks alter column completed_count set default 0;

-- public.banking_pay_operation_chunks.created_at_utc
alter table public.banking_pay_operation_chunks alter column created_at_utc set default now();

-- public.banking_pay_operation_chunks.failed_count
alter table public.banking_pay_operation_chunks alter column failed_count set default 0;

-- public.banking_pay_operation_chunks.id
alter table public.banking_pay_operation_chunks alter column id set default gen_random_uuid();

-- public.banking_pay_operation_chunks.payload_json
alter table public.banking_pay_operation_chunks alter column payload_json set default '{}'::jsonb;

-- public.banking_pay_operation_chunks.status
alter table public.banking_pay_operation_chunks alter column status set default 'PENDING'::text;

-- public.banking_pay_operation_chunks.unit_count
alter table public.banking_pay_operation_chunks alter column unit_count set default 0;

-- public.banking_pay_operation_chunks.updated_at_utc
alter table public.banking_pay_operation_chunks alter column updated_at_utc set default now();

-- public.banking_pay_operation_config.enabled
alter table public.banking_pay_operation_config alter column enabled set default true;

-- public.banking_pay_operation_config.id
alter table public.banking_pay_operation_config alter column id set default gen_random_uuid();

-- public.banking_pay_operation_config.lock_seconds
alter table public.banking_pay_operation_config alter column lock_seconds set default 60;

-- public.banking_pay_operation_config.max_advance_ms
alter table public.banking_pay_operation_config alter column max_advance_ms set default 15000;

-- public.banking_pay_operation_config.phase
alter table public.banking_pay_operation_config alter column phase set default 'ALL'::text;

-- public.banking_pay_operation_config.updated_at_utc
alter table public.banking_pay_operation_config alter column updated_at_utc set default now();

-- public.banking_pay_operation_provider_attempts.created_at_utc
alter table public.banking_pay_operation_provider_attempts alter column created_at_utc set default now();

-- public.banking_pay_operation_provider_attempts.id
alter table public.banking_pay_operation_provider_attempts alter column id set default gen_random_uuid();

-- public.banking_pay_operation_remittance_scope.created_at_utc
alter table public.banking_pay_operation_remittance_scope alter column created_at_utc set default now();

-- public.banking_pay_operation_remittance_scope.id
alter table public.banking_pay_operation_remittance_scope alter column id set default gen_random_uuid();

-- public.banking_pay_operation_remittance_scope.payload_json
alter table public.banking_pay_operation_remittance_scope alter column payload_json set default '{}'::jsonb;

-- public.banking_pay_operation_remittance_scope.status
alter table public.banking_pay_operation_remittance_scope alter column status set default 'PENDING'::text;

-- public.banking_pay_operation_remittance_scope.updated_at_utc
alter table public.banking_pay_operation_remittance_scope alter column updated_at_utc set default now();

-- public.banking_pay_operation_scope_units.created_at_utc
alter table public.banking_pay_operation_scope_units alter column created_at_utc set default now();

-- public.banking_pay_operation_scope_units.id
alter table public.banking_pay_operation_scope_units alter column id set default gen_random_uuid();

-- public.banking_pay_operation_scope_units.status
alter table public.banking_pay_operation_scope_units alter column status set default 'PENDING'::text;

-- public.banking_pay_operation_scope_units.unit_payload_json
alter table public.banking_pay_operation_scope_units alter column unit_payload_json set default '{}'::jsonb;

-- public.banking_pay_operation_scope_units.updated_at_utc
alter table public.banking_pay_operation_scope_units alter column updated_at_utc set default now();

-- public.banking_pay_operation_settlement_scope.created_at_utc
alter table public.banking_pay_operation_settlement_scope alter column created_at_utc set default now();

-- public.banking_pay_operation_settlement_scope.id
alter table public.banking_pay_operation_settlement_scope alter column id set default gen_random_uuid();

-- public.banking_pay_operation_settlement_scope.payload_json
alter table public.banking_pay_operation_settlement_scope alter column payload_json set default '{}'::jsonb;

-- public.banking_pay_operation_settlement_scope.status
alter table public.banking_pay_operation_settlement_scope alter column status set default 'PENDING'::text;

-- public.banking_pay_operation_settlement_scope.updated_at_utc
alter table public.banking_pay_operation_settlement_scope alter column updated_at_utc set default now();

-- public.banking_pay_operation_transfer_scope.amount
alter table public.banking_pay_operation_transfer_scope alter column amount set default 0;

-- public.banking_pay_operation_transfer_scope.candidate_ids_json
alter table public.banking_pay_operation_transfer_scope alter column candidate_ids_json set default '[]'::jsonb;

-- public.banking_pay_operation_transfer_scope.created_at_utc
alter table public.banking_pay_operation_transfer_scope alter column created_at_utc set default now();

-- public.banking_pay_operation_transfer_scope.currency
alter table public.banking_pay_operation_transfer_scope alter column currency set default 'GBP'::text;

-- public.banking_pay_operation_transfer_scope.id
alter table public.banking_pay_operation_transfer_scope alter column id set default gen_random_uuid();

-- public.banking_pay_operation_transfer_scope.pay_batch_item_ids_json
alter table public.banking_pay_operation_transfer_scope alter column pay_batch_item_ids_json set default '[]'::jsonb;

-- public.banking_pay_operation_transfer_scope.prepared_amount_total
alter table public.banking_pay_operation_transfer_scope alter column prepared_amount_total set default 0;

-- public.banking_pay_operation_transfer_scope.prepared_item_count
alter table public.banking_pay_operation_transfer_scope alter column prepared_item_count set default 0;

-- public.banking_pay_operation_transfer_scope.provider_review_required
alter table public.banking_pay_operation_transfer_scope alter column provider_review_required set default false;

-- public.banking_pay_operation_transfer_scope.provider_submit_attempt_count
alter table public.banking_pay_operation_transfer_scope alter column provider_submit_attempt_count set default 0;

-- public.banking_pay_operation_transfer_scope.provider_submit_ready
alter table public.banking_pay_operation_transfer_scope alter column provider_submit_ready set default false;

-- public.banking_pay_operation_transfer_scope.provider_submit_state
alter table public.banking_pay_operation_transfer_scope alter column provider_submit_state set default 'NOT_READY'::text;

-- public.banking_pay_operation_transfer_scope.status
alter table public.banking_pay_operation_transfer_scope alter column status set default 'PENDING'::text;

-- public.banking_pay_operation_transfer_scope.updated_at_utc
alter table public.banking_pay_operation_transfer_scope alter column updated_at_utc set default now();

-- public.banking_pay_operation_transfer_scope_items.created_at_utc
alter table public.banking_pay_operation_transfer_scope_items alter column created_at_utc set default now();

-- public.banking_pay_operation_transfer_scope_items.id
alter table public.banking_pay_operation_transfer_scope_items alter column id set default gen_random_uuid();

-- public.banking_pay_operation_transfer_scope_items.item_amount
alter table public.banking_pay_operation_transfer_scope_items alter column item_amount set default 0;

-- public.banking_pay_operation_transfer_scope_items.item_ordinal
alter table public.banking_pay_operation_transfer_scope_items alter column item_ordinal set default 0;

-- public.banking_pay_operation_transfer_scope_items.item_status
alter table public.banking_pay_operation_transfer_scope_items alter column item_status set default 'PENDING'::text;

-- public.banking_pay_operation_transfer_scope_items.rollup_status
alter table public.banking_pay_operation_transfer_scope_items alter column rollup_status set default 'PENDING'::text;

-- public.banking_pay_operation_transfer_scope_items.updated_at_utc
alter table public.banking_pay_operation_transfer_scope_items alter column updated_at_utc set default now();

-- public.banking_pay_operations.attempt_count
alter table public.banking_pay_operations alter column attempt_count set default 0;

-- public.banking_pay_operations.chunk_count
alter table public.banking_pay_operations alter column chunk_count set default 0;

-- public.banking_pay_operations.completed_units
alter table public.banking_pay_operations alter column completed_units set default 0;

-- public.banking_pay_operations.config_json
alter table public.banking_pay_operations alter column config_json set default '{}'::jsonb;

-- public.banking_pay_operations.created_at_utc
alter table public.banking_pay_operations alter column created_at_utc set default now();

-- public.banking_pay_operations.current_chunk_index
alter table public.banking_pay_operations alter column current_chunk_index set default 0;

-- public.banking_pay_operations.failed_units
alter table public.banking_pay_operations alter column failed_units set default 0;

-- public.banking_pay_operations.id
alter table public.banking_pay_operations alter column id set default gen_random_uuid();

-- public.banking_pay_operations.input_json
alter table public.banking_pay_operations alter column input_json set default '{}'::jsonb;

-- public.banking_pay_operations.max_attempts
alter table public.banking_pay_operations alter column max_attempts set default 20;

-- public.banking_pay_operations.phase
alter table public.banking_pay_operations alter column phase set default 'INITIALISE'::text;

-- public.banking_pay_operations.post_freeze_scope_status
alter table public.banking_pay_operations alter column post_freeze_scope_status set default 'NONE'::text;

-- public.banking_pay_operations.progress_json
alter table public.banking_pay_operations alter column progress_json set default '{}'::jsonb;

-- public.banking_pay_operations.requires_user_action
alter table public.banking_pay_operations alter column requires_user_action set default false;

-- public.banking_pay_operations.runner_state
alter table public.banking_pay_operations alter column runner_state set default 'IDLE'::text;

-- public.banking_pay_operations.scope_freeze_status
alter table public.banking_pay_operations alter column scope_freeze_status set default 'NONE'::text;

-- public.banking_pay_operations.source_scope_seed_complete
alter table public.banking_pay_operations alter column source_scope_seed_complete set default false;

-- public.banking_pay_operations.status
alter table public.banking_pay_operations alter column status set default 'QUEUED'::text;

-- public.banking_pay_operations.total_units
alter table public.banking_pay_operations alter column total_units set default 0;

-- public.banking_pay_operations.updated_at_utc
alter table public.banking_pay_operations alter column updated_at_utc set default now();

-- public.banking_pay_scope_change_transactions.created_at_utc
alter table public.banking_pay_scope_change_transactions alter column created_at_utc set default now();

-- public.banking_pay_scope_change_transactions.state
alter table public.banking_pay_scope_change_transactions alter column state set default 'PENDING'::text;

-- public.banking_pay_snapshot_candidate_state.candidate_fragment_json
alter table public.banking_pay_snapshot_candidate_state alter column candidate_fragment_json set default '{}'::jsonb;

-- public.banking_pay_snapshot_candidate_state.canonical_preview_lines_json
alter table public.banking_pay_snapshot_candidate_state alter column canonical_preview_lines_json set default '[]'::jsonb;

-- public.banking_pay_snapshot_candidate_state.case_resolution_states_json
alter table public.banking_pay_snapshot_candidate_state alter column case_resolution_states_json set default '[]'::jsonb;

-- public.banking_pay_snapshot_candidate_state.created_at_utc
alter table public.banking_pay_snapshot_candidate_state alter column created_at_utc set default now();

-- public.banking_pay_snapshot_candidate_state.id
alter table public.banking_pay_snapshot_candidate_state alter column id set default gen_random_uuid();

-- public.banking_pay_snapshot_candidate_state.payees_json
alter table public.banking_pay_snapshot_candidate_state alter column payees_json set default '[]'::jsonb;

-- public.banking_pay_snapshot_candidate_state.source_change_seq
alter table public.banking_pay_snapshot_candidate_state alter column source_change_seq set default 0;

-- public.banking_pay_snapshot_candidate_state.status
alter table public.banking_pay_snapshot_candidate_state alter column status set default 'PENDING'::text;

-- public.banking_pay_snapshot_candidate_state.summary_fragment_json
alter table public.banking_pay_snapshot_candidate_state alter column summary_fragment_json set default '{}'::jsonb;

-- public.banking_pay_snapshot_candidate_state.updated_at_utc
alter table public.banking_pay_snapshot_candidate_state alter column updated_at_utc set default now();

-- public.banking_pay_snapshot_case_component_state.component_state_json
alter table public.banking_pay_snapshot_case_component_state alter column component_state_json set default '{}'::jsonb;

-- public.banking_pay_snapshot_case_component_state.created_at_utc
alter table public.banking_pay_snapshot_case_component_state alter column created_at_utc set default now();

-- public.banking_pay_snapshot_case_component_state.id
alter table public.banking_pay_snapshot_case_component_state alter column id set default gen_random_uuid();

-- public.banking_pay_snapshot_case_component_state.updated_at_utc
alter table public.banking_pay_snapshot_case_component_state alter column updated_at_utc set default now();

-- public.banking_pay_snapshot_case_state.case_state_json
alter table public.banking_pay_snapshot_case_state alter column case_state_json set default '{}'::jsonb;

-- public.banking_pay_snapshot_case_state.created_at_utc
alter table public.banking_pay_snapshot_case_state alter column created_at_utc set default now();

-- public.banking_pay_snapshot_case_state.id
alter table public.banking_pay_snapshot_case_state alter column id set default gen_random_uuid();

-- public.banking_pay_snapshot_case_state.is_blocked
alter table public.banking_pay_snapshot_case_state alter column is_blocked set default false;

-- public.banking_pay_snapshot_case_state.is_resolved
alter table public.banking_pay_snapshot_case_state alter column is_resolved set default false;

-- public.banking_pay_snapshot_case_state.updated_at_utc
alter table public.banking_pay_snapshot_case_state alter column updated_at_utc set default now();

-- public.banking_pay_snapshot_line_state.canonical_line_json
alter table public.banking_pay_snapshot_line_state alter column canonical_line_json set default '{}'::jsonb;

-- public.banking_pay_snapshot_line_state.created_at_utc
alter table public.banking_pay_snapshot_line_state alter column created_at_utc set default now();

-- public.banking_pay_snapshot_line_state.id
alter table public.banking_pay_snapshot_line_state alter column id set default gen_random_uuid();

-- public.banking_pay_snapshot_line_state.updated_at_utc
alter table public.banking_pay_snapshot_line_state alter column updated_at_utc set default now();

-- public.banking_pay_snapshot_runs.created_at_utc
alter table public.banking_pay_snapshot_runs alter column created_at_utc set default now();

-- public.banking_pay_snapshot_runs.id
alter table public.banking_pay_snapshot_runs alter column id set default gen_random_uuid();

-- public.banking_pay_snapshot_runs.is_active
alter table public.banking_pay_snapshot_runs alter column is_active set default false;

-- public.banking_pay_snapshot_runs.paye_guardrails_json
alter table public.banking_pay_snapshot_runs alter column paye_guardrails_json set default '{}'::jsonb;

-- public.banking_pay_snapshot_runs.status
alter table public.banking_pay_snapshot_runs alter column status set default 'OPEN'::text;

-- public.banking_pay_snapshot_runs.summary_json
alter table public.banking_pay_snapshot_runs alter column summary_json set default '{}'::jsonb;

-- public.banking_pay_snapshot_runs.updated_at_utc
alter table public.banking_pay_snapshot_runs alter column updated_at_utc set default now();

-- public.banking_pay_workbench_candidate_delta_projection_runs.admission_seal_json
alter table public.banking_pay_workbench_candidate_delta_projection_runs alter column admission_seal_json set default '{}'::jsonb;

-- public.banking_pay_workbench_candidate_delta_projection_runs.candidate_state_updated
alter table public.banking_pay_workbench_candidate_delta_projection_runs alter column candidate_state_updated set default false;

-- public.banking_pay_workbench_candidate_delta_projection_runs.cursor_json
alter table public.banking_pay_workbench_candidate_delta_projection_runs alter column cursor_json set default '{}'::jsonb;

-- public.banking_pay_workbench_candidate_delta_projection_runs.diagnostics_json
alter table public.banking_pay_workbench_candidate_delta_projection_runs alter column diagnostics_json set default '{}'::jsonb;

-- public.banking_pay_workbench_candidate_delta_projection_runs.fallback_required
alter table public.banking_pay_workbench_candidate_delta_projection_runs alter column fallback_required set default false;

-- public.banking_pay_workbench_candidate_delta_projection_runs.id
alter table public.banking_pay_workbench_candidate_delta_projection_runs alter column id set default gen_random_uuid();

-- public.banking_pay_workbench_candidate_delta_projection_runs.legacy_compare_json
alter table public.banking_pay_workbench_candidate_delta_projection_runs alter column legacy_compare_json set default '{}'::jsonb;

-- public.banking_pay_workbench_candidate_delta_projection_runs.linked_timesheet_ids
alter table public.banking_pay_workbench_candidate_delta_projection_runs alter column linked_timesheet_ids set default '[]'::jsonb;

-- public.banking_pay_workbench_candidate_delta_projection_runs.phase
alter table public.banking_pay_workbench_candidate_delta_projection_runs alter column phase set default 'INIT_PREFLIGHT'::text;

-- public.banking_pay_workbench_candidate_delta_projection_runs.projected_row_count
alter table public.banking_pay_workbench_candidate_delta_projection_runs alter column projected_row_count set default 0;

-- public.banking_pay_workbench_candidate_delta_projection_runs.projection_class
alter table public.banking_pay_workbench_candidate_delta_projection_runs alter column projection_class set default 'UNKNOWN'::text;

-- public.banking_pay_workbench_candidate_delta_projection_runs.projection_mode
alter table public.banking_pay_workbench_candidate_delta_projection_runs alter column projection_mode set default 'DELTA'::text;

-- public.banking_pay_workbench_candidate_delta_projection_runs.shadow_compare_enforced
alter table public.banking_pay_workbench_candidate_delta_projection_runs alter column shadow_compare_enforced set default false;

-- public.banking_pay_workbench_candidate_delta_projection_runs.shadow_compare_required
alter table public.banking_pay_workbench_candidate_delta_projection_runs alter column shadow_compare_required set default false;

-- public.banking_pay_workbench_candidate_delta_projection_runs.started_at_utc
alter table public.banking_pay_workbench_candidate_delta_projection_runs alter column started_at_utc set default now();

-- public.banking_pay_workbench_candidate_delta_projection_runs.status
alter table public.banking_pay_workbench_candidate_delta_projection_runs alter column status set default 'RUNNING'::text;

-- public.banking_pay_workbench_candidate_delta_projection_runs.targeted_timesheet_ids
alter table public.banking_pay_workbench_candidate_delta_projection_runs alter column targeted_timesheet_ids set default '[]'::jsonb;

-- public.banking_pay_workbench_candidate_delta_projection_runs.updated_at_utc
alter table public.banking_pay_workbench_candidate_delta_projection_runs alter column updated_at_utc set default now();

-- public.banking_pay_workbench_candidate_delta_projection_runs.write_cursor_json
alter table public.banking_pay_workbench_candidate_delta_projection_runs alter column write_cursor_json set default '{}'::jsonb;

-- public.banking_pay_workbench_candidate_delta_projection_runs.written_line_work_count
alter table public.banking_pay_workbench_candidate_delta_projection_runs alter column written_line_work_count set default 0;

-- public.banking_pay_workbench_candidate_delta_projection_runs.written_preview_count
alter table public.banking_pay_workbench_candidate_delta_projection_runs alter column written_preview_count set default 0;

-- public.banking_pay_workbench_candidate_delta_projection_runs.written_source_count
alter table public.banking_pay_workbench_candidate_delta_projection_runs alter column written_source_count set default 0;

-- public.banking_pay_workbench_candidate_line_work.created_at_utc
alter table public.banking_pay_workbench_candidate_line_work alter column created_at_utc set default now();

-- public.banking_pay_workbench_candidate_line_work.id
alter table public.banking_pay_workbench_candidate_line_work alter column id set default gen_random_uuid();

-- public.banking_pay_workbench_candidate_line_work.status
alter table public.banking_pay_workbench_candidate_line_work alter column status set default 'PENDING'::text;

-- public.banking_pay_workbench_candidate_line_work.updated_at_utc
alter table public.banking_pay_workbench_candidate_line_work alter column updated_at_utc set default now();

-- public.banking_pay_workbench_candidate_line_work.work_payload_json
alter table public.banking_pay_workbench_candidate_line_work alter column work_payload_json set default '{}'::jsonb;

-- public.banking_pay_workbench_candidate_source_lines.contract_json
alter table public.banking_pay_workbench_candidate_source_lines alter column contract_json set default '{}'::jsonb;

-- public.banking_pay_workbench_candidate_source_lines.created_at_utc
alter table public.banking_pay_workbench_candidate_source_lines alter column created_at_utc set default now();

-- public.banking_pay_workbench_candidate_source_lines.economic_key_json
alter table public.banking_pay_workbench_candidate_source_lines alter column economic_key_json set default '{}'::jsonb;

-- public.banking_pay_workbench_candidate_source_lines.id
alter table public.banking_pay_workbench_candidate_source_lines alter column id set default gen_random_uuid();

-- public.banking_pay_workbench_candidate_source_lines.pay_channel_scope
alter table public.banking_pay_workbench_candidate_source_lines alter column pay_channel_scope set default 'ALL'::text;

-- public.banking_pay_workbench_candidate_source_lines.refresh_scope_kind
alter table public.banking_pay_workbench_candidate_source_lines alter column refresh_scope_kind set default 'CANDIDATE_FULL_LIVE'::text;

-- public.banking_pay_workbench_candidate_source_lines.source_row_json
alter table public.banking_pay_workbench_candidate_source_lines alter column source_row_json set default '{}'::jsonb;

-- public.banking_pay_workbench_candidate_source_lines.status
alter table public.banking_pay_workbench_candidate_source_lines alter column status set default 'CURRENT'::text;

-- public.banking_pay_workbench_candidate_source_lines.updated_at_utc
alter table public.banking_pay_workbench_candidate_source_lines alter column updated_at_utc set default now();

-- public.banking_pay_workbench_case_resolution_carry_registrations.attempt_count
alter table public.banking_pay_workbench_case_resolution_carry_registrations alter column attempt_count set default 0;

-- public.banking_pay_workbench_case_resolution_carry_registrations.created_at_utc
alter table public.banking_pay_workbench_case_resolution_carry_registrations alter column created_at_utc set default now();

-- public.banking_pay_workbench_case_resolution_carry_registrations.id
alter table public.banking_pay_workbench_case_resolution_carry_registrations alter column id set default gen_random_uuid();

-- public.banking_pay_workbench_case_resolution_carry_registrations.updated_at_utc
alter table public.banking_pay_workbench_case_resolution_carry_registrations alter column updated_at_utc set default now();

-- public.banking_pay_workbench_jobs.attempt_count
alter table public.banking_pay_workbench_jobs alter column attempt_count set default 0;

-- public.banking_pay_workbench_jobs.created_at_utc
alter table public.banking_pay_workbench_jobs alter column created_at_utc set default now();

-- public.banking_pay_workbench_jobs.id
alter table public.banking_pay_workbench_jobs alter column id set default gen_random_uuid();

-- public.banking_pay_workbench_jobs.max_attempts
alter table public.banking_pay_workbench_jobs alter column max_attempts set default 8;

-- public.banking_pay_workbench_jobs.payload_json
alter table public.banking_pay_workbench_jobs alter column payload_json set default '{}'::jsonb;

-- public.banking_pay_workbench_jobs.priority
alter table public.banking_pay_workbench_jobs alter column priority set default 100;

-- public.banking_pay_workbench_jobs.private_cursor_json
alter table public.banking_pay_workbench_jobs alter column private_cursor_json set default '{}'::jsonb;

-- public.banking_pay_workbench_jobs.run_at_utc
alter table public.banking_pay_workbench_jobs alter column run_at_utc set default now();

-- public.banking_pay_workbench_jobs.status
alter table public.banking_pay_workbench_jobs alter column status set default 'QUEUED'::text;

-- public.banking_pay_workbench_jobs.updated_at_utc
alter table public.banking_pay_workbench_jobs alter column updated_at_utc set default now();

-- public.banking_pay_workbench_preview_rows.created_at_utc
alter table public.banking_pay_workbench_preview_rows alter column created_at_utc set default now();

-- public.banking_pay_workbench_preview_rows.id
alter table public.banking_pay_workbench_preview_rows alter column id set default gen_random_uuid();

-- public.banking_pay_workbench_preview_rows.row_json
alter table public.banking_pay_workbench_preview_rows alter column row_json set default '{}'::jsonb;

-- public.banking_pay_workbench_preview_rows.selected
alter table public.banking_pay_workbench_preview_rows alter column selected set default true;

-- public.banking_pay_workbench_preview_rows.selection_state
alter table public.banking_pay_workbench_preview_rows alter column selection_state set default 'SELECTED'::text;

-- public.banking_pay_workbench_preview_rows.status
alter table public.banking_pay_workbench_preview_rows alter column status set default 'READY'::text;

-- public.banking_pay_workbench_preview_rows.updated_at_utc
alter table public.banking_pay_workbench_preview_rows alter column updated_at_utc set default now();

-- public.banking_pay_workbench_selection_carry_registrations.created_at_utc
alter table public.banking_pay_workbench_selection_carry_registrations alter column created_at_utc set default now();

-- public.banking_pay_workbench_selection_carry_registrations.id
alter table public.banking_pay_workbench_selection_carry_registrations alter column id set default gen_random_uuid();

-- public.banking_pay_workbench_selection_carry_registrations.source_priority
alter table public.banking_pay_workbench_selection_carry_registrations alter column source_priority set default 0;

-- public.banking_pay_workbench_selection_carry_registrations.source_row_snapshot_json
alter table public.banking_pay_workbench_selection_carry_registrations alter column source_row_snapshot_json set default '{}'::jsonb;

-- public.banking_pay_workbench_selection_carry_registrations.status
alter table public.banking_pay_workbench_selection_carry_registrations alter column status set default 'PENDING'::text;

-- public.banking_pay_workbench_selection_carry_registrations.updated_at_utc
alter table public.banking_pay_workbench_selection_carry_registrations alter column updated_at_utc set default now();

-- public.banking_pay_workbench_session_candidate_state.created_at_utc
alter table public.banking_pay_workbench_session_candidate_state alter column created_at_utc set default now();

-- public.banking_pay_workbench_session_candidate_state.effective_candidate_fragment_json
alter table public.banking_pay_workbench_session_candidate_state alter column effective_candidate_fragment_json set default '{}'::jsonb;

-- public.banking_pay_workbench_session_candidate_state.effective_canonical_preview_lines_json
alter table public.banking_pay_workbench_session_candidate_state alter column effective_canonical_preview_lines_json set default '[]'::jsonb;

-- public.banking_pay_workbench_session_candidate_state.effective_case_resolution_states_json
alter table public.banking_pay_workbench_session_candidate_state alter column effective_case_resolution_states_json set default '[]'::jsonb;

-- public.banking_pay_workbench_session_candidate_state.effective_payees_json
alter table public.banking_pay_workbench_session_candidate_state alter column effective_payees_json set default '[]'::jsonb;

-- public.banking_pay_workbench_session_candidate_state.effective_summary_fragment_json
alter table public.banking_pay_workbench_session_candidate_state alter column effective_summary_fragment_json set default '{}'::jsonb;

-- public.banking_pay_workbench_session_candidate_state.id
alter table public.banking_pay_workbench_session_candidate_state alter column id set default gen_random_uuid();

-- public.banking_pay_workbench_session_candidate_state.session_version
alter table public.banking_pay_workbench_session_candidate_state alter column session_version set default 1;

-- public.banking_pay_workbench_session_candidate_state.source_change_seq
alter table public.banking_pay_workbench_session_candidate_state alter column source_change_seq set default 0;

-- public.banking_pay_workbench_session_candidate_state.status
alter table public.banking_pay_workbench_session_candidate_state alter column status set default 'PENDING'::text;

-- public.banking_pay_workbench_session_candidate_state.updated_at_utc
alter table public.banking_pay_workbench_session_candidate_state alter column updated_at_utc set default now();

-- public.banking_pay_workbench_session_case_resolutions.created_at_utc
alter table public.banking_pay_workbench_session_case_resolutions alter column created_at_utc set default now();

-- public.banking_pay_workbench_session_case_resolutions.id
alter table public.banking_pay_workbench_session_case_resolutions alter column id set default gen_random_uuid();

-- public.banking_pay_workbench_session_case_resolutions.payload_json
alter table public.banking_pay_workbench_session_case_resolutions alter column payload_json set default '{}'::jsonb;

-- public.banking_pay_workbench_session_case_resolutions.updated_at_utc
alter table public.banking_pay_workbench_session_case_resolutions alter column updated_at_utc set default now();

-- public.banking_pay_workbench_session_overrides.created_at_utc
alter table public.banking_pay_workbench_session_overrides alter column created_at_utc set default now();

-- public.banking_pay_workbench_session_overrides.id
alter table public.banking_pay_workbench_session_overrides alter column id set default gen_random_uuid();

-- public.banking_pay_workbench_session_overrides.payload_json
alter table public.banking_pay_workbench_session_overrides alter column payload_json set default '{}'::jsonb;

-- public.banking_pay_workbench_session_overrides.updated_at_utc
alter table public.banking_pay_workbench_session_overrides alter column updated_at_utc set default now();

-- public.banking_pay_workbench_session_scope.certified_preview_publication_attestation_json
alter table public.banking_pay_workbench_session_scope alter column certified_preview_publication_attestation_json set default '{}'::jsonb;

-- public.banking_pay_workbench_session_scope.certified_preview_publication_parity_ok
alter table public.banking_pay_workbench_session_scope alter column certified_preview_publication_parity_ok set default false;

-- public.banking_pay_workbench_session_scope.certified_preview_publication_required
alter table public.banking_pay_workbench_session_scope alter column certified_preview_publication_required set default false;

-- public.banking_pay_workbench_session_scope.created_at_utc
alter table public.banking_pay_workbench_session_scope alter column created_at_utc set default now();

-- public.banking_pay_workbench_session_scope.dirty
alter table public.banking_pay_workbench_session_scope alter column dirty set default false;

-- public.banking_pay_workbench_session_scope.id
alter table public.banking_pay_workbench_session_scope alter column id set default gen_random_uuid();

-- public.banking_pay_workbench_session_scope.seeded
alter table public.banking_pay_workbench_session_scope alter column seeded set default false;

-- public.banking_pay_workbench_session_scope.status
alter table public.banking_pay_workbench_session_scope alter column status set default 'PENDING'::text;

-- public.banking_pay_workbench_session_scope.updated_at_utc
alter table public.banking_pay_workbench_session_scope alter column updated_at_utc set default now();

-- public.banking_pay_workbench_sessions.candidate_sample_rows_json
alter table public.banking_pay_workbench_sessions alter column candidate_sample_rows_json set default '[]'::jsonb;

-- public.banking_pay_workbench_sessions.created_at_utc
alter table public.banking_pay_workbench_sessions alter column created_at_utc set default now();

-- public.banking_pay_workbench_sessions.filters_json
alter table public.banking_pay_workbench_sessions alter column filters_json set default '{}'::jsonb;

-- public.banking_pay_workbench_sessions.id
alter table public.banking_pay_workbench_sessions alter column id set default gen_random_uuid();

-- public.banking_pay_workbench_sessions.line_units_failed
alter table public.banking_pay_workbench_sessions alter column line_units_failed set default 0;

-- public.banking_pay_workbench_sessions.line_units_pending
alter table public.banking_pay_workbench_sessions alter column line_units_pending set default 0;

-- public.banking_pay_workbench_sessions.line_units_ready
alter table public.banking_pay_workbench_sessions alter column line_units_ready set default 0;

-- public.banking_pay_workbench_sessions.line_units_total
alter table public.banking_pay_workbench_sessions alter column line_units_total set default 0;

-- public.banking_pay_workbench_sessions.preview_row_count
alter table public.banking_pay_workbench_sessions alter column preview_row_count set default 0;

-- public.banking_pay_workbench_sessions.progress_counter_version
alter table public.banking_pay_workbench_sessions alter column progress_counter_version set default 0;

-- public.banking_pay_workbench_sessions.progress_json
alter table public.banking_pay_workbench_sessions alter column progress_json set default '{}'::jsonb;

-- public.banking_pay_workbench_sessions.progress_state
alter table public.banking_pay_workbench_sessions alter column progress_state set default 'PENDING'::text;

-- public.banking_pay_workbench_sessions.progress_updated_at_utc
alter table public.banking_pay_workbench_sessions alter column progress_updated_at_utc set default now();

-- public.banking_pay_workbench_sessions.scope_candidate_ids
alter table public.banking_pay_workbench_sessions alter column scope_candidate_ids set default '{}'::uuid[];

-- public.banking_pay_workbench_sessions.scope_change_generation_applied
alter table public.banking_pay_workbench_sessions alter column scope_change_generation_applied set default 0;

-- public.banking_pay_workbench_sessions.scope_change_generation_shadow_checked
alter table public.banking_pay_workbench_sessions alter column scope_change_generation_shadow_checked set default 0;

-- public.banking_pay_workbench_sessions.scope_change_generation_target
alter table public.banking_pay_workbench_sessions alter column scope_change_generation_target set default 0;

-- public.banking_pay_workbench_sessions.scope_failed_count
alter table public.banking_pay_workbench_sessions alter column scope_failed_count set default 0;

-- public.banking_pay_workbench_sessions.scope_next_cursor_json
alter table public.banking_pay_workbench_sessions alter column scope_next_cursor_json set default '{}'::jsonb;

-- public.banking_pay_workbench_sessions.scope_pending_count
alter table public.banking_pay_workbench_sessions alter column scope_pending_count set default 0;

-- public.banking_pay_workbench_sessions.scope_ready_count
alter table public.banking_pay_workbench_sessions alter column scope_ready_count set default 0;

-- public.banking_pay_workbench_sessions.scope_seed_complete
alter table public.banking_pay_workbench_sessions alter column scope_seed_complete set default false;

-- public.banking_pay_workbench_sessions.scope_seeded_count
alter table public.banking_pay_workbench_sessions alter column scope_seeded_count set default 0;

-- public.banking_pay_workbench_sessions.scope_total_count
alter table public.banking_pay_workbench_sessions alter column scope_total_count set default 0;

-- public.banking_pay_workbench_sessions.section_counts_json
alter table public.banking_pay_workbench_sessions alter column section_counts_json set default '{}'::jsonb;

-- public.banking_pay_workbench_sessions.selected_row_count
alter table public.banking_pay_workbench_sessions alter column selected_row_count set default 0;

-- public.banking_pay_workbench_sessions.server_selected_preview_row_ids
alter table public.banking_pay_workbench_sessions alter column server_selected_preview_row_ids set default '[]'::jsonb;

-- public.banking_pay_workbench_sessions.server_selected_preview_row_ids_provided
alter table public.banking_pay_workbench_sessions alter column server_selected_preview_row_ids_provided set default false;

-- public.banking_pay_workbench_sessions.status
alter table public.banking_pay_workbench_sessions alter column status set default 'OPEN'::text;

-- public.banking_pay_workbench_sessions.updated_at_utc
alter table public.banking_pay_workbench_sessions alter column updated_at_utc set default now();

-- public.banking_pay_workbench_sessions.version
alter table public.banking_pay_workbench_sessions alter column version set default 1;

-- public.candidate_app_accounts.created_at_utc
alter table public.candidate_app_accounts alter column created_at_utc set default now();

-- public.candidate_app_accounts.failed_login_count
alter table public.candidate_app_accounts alter column failed_login_count set default 0;

-- public.candidate_app_accounts.id
alter table public.candidate_app_accounts alter column id set default gen_random_uuid();

-- public.candidate_app_accounts.notification_preferences_json
alter table public.candidate_app_accounts alter column notification_preferences_json set default '{"payment": true, "authorisation": true, "manager_refusal": true, "manager_approval": true, "office_rejection": true, "resubmission_required": true}'::jsonb;

-- public.candidate_app_accounts.password_params_json
alter table public.candidate_app_accounts alter column password_params_json set default '{}'::jsonb;

-- public.candidate_app_accounts.session_version
alter table public.candidate_app_accounts alter column session_version set default 1;

-- public.candidate_app_accounts.status
alter table public.candidate_app_accounts alter column status set default 'SETUP_REQUIRED'::text;

-- public.candidate_app_accounts.updated_at_utc
alter table public.candidate_app_accounts alter column updated_at_utc set default now();

-- public.candidate_app_global_membership_links.linked_at_utc
alter table public.candidate_app_global_membership_links alter column linked_at_utc set default now();

-- public.candidate_app_global_membership_links.state
alter table public.candidate_app_global_membership_links alter column state set default 'ACTIVE'::text;

-- public.candidate_app_global_membership_links.updated_at_utc
alter table public.candidate_app_global_membership_links alter column updated_at_utc set default now();

-- public.candidate_app_sessions.auth_source
alter table public.candidate_app_sessions alter column auth_source set default 'LOCAL'::text;

-- public.candidate_app_sessions.created_at_utc
alter table public.candidate_app_sessions alter column created_at_utc set default now();

-- public.candidate_app_sessions.id
alter table public.candidate_app_sessions alter column id set default gen_random_uuid();

-- public.candidate_app_sessions.issued_at_utc
alter table public.candidate_app_sessions alter column issued_at_utc set default now();

-- public.candidate_app_sessions.last_used_at_utc
alter table public.candidate_app_sessions alter column last_used_at_utc set default now();

-- public.candidate_app_sessions.rotation
alter table public.candidate_app_sessions alter column rotation set default 0;

-- public.candidate_app_sessions.status
alter table public.candidate_app_sessions alter column status set default 'ACTIVE'::text;

-- public.candidate_app_sessions.token_family_id
alter table public.candidate_app_sessions alter column token_family_id set default gen_random_uuid();

-- public.candidate_app_sessions.updated_at_utc
alter table public.candidate_app_sessions alter column updated_at_utc set default now();

-- public.candidate_approval_requests.created_at_utc
alter table public.candidate_approval_requests alter column created_at_utc set default now();

-- public.candidate_approval_requests.id
alter table public.candidate_approval_requests alter column id set default gen_random_uuid();

-- public.candidate_approval_requests.renewal_count
alter table public.candidate_approval_requests alter column renewal_count set default 0;

-- public.candidate_approval_requests.request_generation
alter table public.candidate_approval_requests alter column request_generation set default 1;

-- public.candidate_approval_requests.required_component_ids
alter table public.candidate_approval_requests alter column required_component_ids set default '{}'::uuid[];

-- public.candidate_approval_requests.required_component_manifest_json
alter table public.candidate_approval_requests alter column required_component_manifest_json set default '[]'::jsonb;

-- public.candidate_approval_requests.resend_count
alter table public.candidate_approval_requests alter column resend_count set default 0;

-- public.candidate_approval_requests.review_progress_json
alter table public.candidate_approval_requests alter column review_progress_json set default '{}'::jsonb;

-- public.candidate_approval_requests.state
alter table public.candidate_approval_requests alter column state set default 'PENDING'::text;

-- public.candidate_approval_requests.updated_at_utc
alter table public.candidate_approval_requests alter column updated_at_utc set default now();

-- public.candidate_auth_challenges.attempt_count
alter table public.candidate_auth_challenges alter column attempt_count set default 0;

-- public.candidate_auth_challenges.created_at_utc
alter table public.candidate_auth_challenges alter column created_at_utc set default now();

-- public.candidate_auth_challenges.id
alter table public.candidate_auth_challenges alter column id set default gen_random_uuid();

-- public.candidate_auth_challenges.resend_count
alter table public.candidate_auth_challenges alter column resend_count set default 0;

-- public.candidate_auth_challenges.state
alter table public.candidate_auth_challenges alter column state set default 'PENDING'::text;

-- public.candidate_auth_challenges.updated_at_utc
alter table public.candidate_auth_challenges alter column updated_at_utc set default now();

-- public.candidate_daily_availability_days.changed_at_utc
alter table public.candidate_daily_availability_days alter column changed_at_utc set default now();

-- public.candidate_daily_command_receipts.canonical_version_before
alter table public.candidate_daily_command_receipts alter column canonical_version_before set default 0;

-- public.candidate_daily_command_receipts.command_id
alter table public.candidate_daily_command_receipts alter column command_id set default gen_random_uuid();

-- public.candidate_daily_command_receipts.created_at_utc
alter table public.candidate_daily_command_receipts alter column created_at_utc set default now();

-- public.candidate_daily_command_receipts.state
alter table public.candidate_daily_command_receipts alter column state set default 'IN_PROGRESS'::text;

-- public.candidate_daily_command_receipts.updated_at_utc
alter table public.candidate_daily_command_receipts alter column updated_at_utc set default now();

-- public.candidate_daily_rota_days.created_at_utc
alter table public.candidate_daily_rota_days alter column created_at_utc set default now();

-- public.candidate_daily_rota_days.updated_at_utc
alter table public.candidate_daily_rota_days alter column updated_at_utc set default now();

-- public.candidate_daily_rota_generations.actual_day_count
alter table public.candidate_daily_rota_generations alter column actual_day_count set default 0;

-- public.candidate_daily_rota_generations.created_at_utc
alter table public.candidate_daily_rota_generations alter column created_at_utc set default now();

-- public.candidate_daily_rota_generations.expected_day_count
alter table public.candidate_daily_rota_generations alter column expected_day_count set default 14;

-- public.candidate_daily_rota_generations.generation_id
alter table public.candidate_daily_rota_generations alter column generation_id set default gen_random_uuid();

-- public.candidate_daily_rota_generations.state
alter table public.candidate_daily_rota_generations alter column state set default 'BUILDING'::text;

-- public.candidate_daily_rota_generations.updated_at_utc
alter table public.candidate_daily_rota_generations alter column updated_at_utc set default now();

-- public.candidate_daily_sheet_projection_outbox.created_at_utc
alter table public.candidate_daily_sheet_projection_outbox alter column created_at_utc set default now();

-- public.candidate_daily_sheet_projection_outbox.deferral_count
alter table public.candidate_daily_sheet_projection_outbox alter column deferral_count set default 0;

-- public.candidate_daily_sheet_projection_outbox.delivery_attempt_count
alter table public.candidate_daily_sheet_projection_outbox alter column delivery_attempt_count set default 0;

-- public.candidate_daily_sheet_projection_outbox.next_available_at_utc
alter table public.candidate_daily_sheet_projection_outbox alter column next_available_at_utc set default now();

-- public.candidate_daily_sheet_projection_outbox.operation
alter table public.candidate_daily_sheet_projection_outbox alter column operation set default 'SET_AVAILABILITY'::text;

-- public.candidate_daily_sheet_projection_outbox.outbox_id
alter table public.candidate_daily_sheet_projection_outbox alter column outbox_id set default gen_random_uuid();

-- public.candidate_daily_sheet_projection_outbox.state
alter table public.candidate_daily_sheet_projection_outbox alter column state set default 'PENDING'::text;

-- public.candidate_daily_sheet_projection_outbox.target
alter table public.candidate_daily_sheet_projection_outbox alter column target set default 'MASTER_AVAILABILITY_SHEET'::text;

-- public.candidate_daily_sheet_projection_outbox.updated_at_utc
alter table public.candidate_daily_sheet_projection_outbox alter column updated_at_utc set default now();

-- public.candidate_job_titles.created_at
alter table public.candidate_job_titles alter column created_at set default now();

-- public.candidate_job_titles.id
alter table public.candidate_job_titles alter column id set default gen_random_uuid();

-- public.candidate_job_titles.is_primary
alter table public.candidate_job_titles alter column is_primary set default false;

-- public.candidate_job_titles.updated_at
alter table public.candidate_job_titles alter column updated_at set default now();

-- public.candidate_notifications.created_at_utc
alter table public.candidate_notifications alter column created_at_utc set default now();

-- public.candidate_notifications.deep_link_json
alter table public.candidate_notifications alter column deep_link_json set default '{}'::jsonb;

-- public.candidate_notifications.id
alter table public.candidate_notifications alter column id set default gen_random_uuid();

-- public.candidate_notifications.push_state
alter table public.candidate_notifications alter column push_state set default 'PENDING'::text;

-- public.candidate_notifications.state
alter table public.candidate_notifications alter column state set default 'UNREAD'::text;

-- public.candidate_notifications.template_params
alter table public.candidate_notifications alter column template_params set default '{}'::jsonb;

-- public.candidate_submission_components.created_at_utc
alter table public.candidate_submission_components alter column created_at_utc set default now();

-- public.candidate_submission_components.final_signed_render_state
alter table public.candidate_submission_components alter column final_signed_render_state set default 'NOT_REQUIRED'::text;

-- public.candidate_submission_components.id
alter table public.candidate_submission_components alter column id set default gen_random_uuid();

-- public.candidate_submission_components.required
alter table public.candidate_submission_components alter column required set default false;

-- public.candidate_submission_components.review_render_state
alter table public.candidate_submission_components alter column review_render_state set default 'NOT_REQUIRED'::text;

-- public.candidate_submission_components.state
alter table public.candidate_submission_components alter column state set default 'PENDING'::text;

-- public.candidate_submission_workflows.created_at_utc
alter table public.candidate_submission_workflows alter column created_at_utc set default now();

-- public.candidate_submission_workflows.generation
alter table public.candidate_submission_workflows alter column generation set default 1;

-- public.candidate_submission_workflows.id
alter table public.candidate_submission_workflows alter column id set default gen_random_uuid();

-- public.candidate_submission_workflows.input_snapshot_json
alter table public.candidate_submission_workflows alter column input_snapshot_json set default '{}'::jsonb;

-- public.candidate_submission_workflows.issue_codes
alter table public.candidate_submission_workflows alter column issue_codes set default '[]'::jsonb;

-- public.candidate_submission_workflows.policy_snapshot_json
alter table public.candidate_submission_workflows alter column policy_snapshot_json set default '{}'::jsonb;

-- public.candidate_submission_workflows.state
alter table public.candidate_submission_workflows alter column state set default 'WORKER_DRAFT'::text;

-- public.candidate_submission_workflows.updated_at_utc
alter table public.candidate_submission_workflows alter column updated_at_utc set default now();

-- public.candidates.active
alter table public.candidates alter column active set default true;

-- public.candidates.created_at
alter table public.candidates alter column created_at set default now();

-- public.candidates.id
alter table public.candidates alter column id set default gen_random_uuid();

-- public.candidates.min_take_home_wtd
alter table public.candidates alter column min_take_home_wtd set default 0;

-- public.candidates.nhsp_hr_name_aliases
alter table public.candidates alter column nhsp_hr_name_aliases set default '[]'::jsonb;

-- public.candidates.opt_in_email
alter table public.candidates alter column opt_in_email set default false;

-- public.candidates.opt_in_sms
alter table public.candidates alter column opt_in_sms set default false;

-- public.candidates.opt_in_whatsapp
alter table public.candidates alter column opt_in_whatsapp set default false;

-- public.candidates.pay_method
alter table public.candidates alter column pay_method set default 'PAYE'::text;

-- public.candidates.remittance_overrides_enabled
alter table public.candidates alter column remittance_overrides_enabled set default false;

-- public.candidates.remittance_receive_enabled
alter table public.candidates alter column remittance_receive_enabled set default false;

-- public.candidates.remittance_receive_when_umbrella_paid
alter table public.candidates alter column remittance_receive_when_umbrella_paid set default false;

-- public.candidates.remittances_detailed_breakdown
alter table public.candidates alter column remittances_detailed_breakdown set default false;

-- public.candidates.updated_at
alter table public.candidates alter column updated_at set default now();

-- public.candidates_tombstones.deleted_at
alter table public.candidates_tombstones alter column deleted_at set default now();

-- public.client_hospitals.created_at
alter table public.client_hospitals alter column created_at set default now();

-- public.client_hospitals.id
alter table public.client_hospitals alter column id set default gen_random_uuid();

-- public.client_hospitals.updated_at
alter table public.client_hospitals alter column updated_at set default now();

-- public.client_settings.allow_daily_manager_authorise_by_email
alter table public.client_settings alter column allow_daily_manager_authorise_by_email set default false;

-- public.client_settings.allow_daily_manager_authorise_on_phone
alter table public.client_settings alter column allow_daily_manager_authorise_on_phone set default true;

-- public.client_settings.auto_invoice_default
alter table public.client_settings alter column auto_invoice_default set default false;

-- public.client_settings.autoprocess_hr
alter table public.client_settings alter column autoprocess_hr set default false;

-- public.client_settings.candidate_expenses_require_separate_timesheet
alter table public.client_settings alter column candidate_expenses_require_separate_timesheet set default false;

-- public.client_settings.candidate_manager_approval_policy_json
alter table public.client_settings alter column candidate_manager_approval_policy_json set default '{"approved_emails": [], "approved_domains": [], "allow_free_business_email": false}'::jsonb;

-- public.client_settings.candidate_paper_submission_enabled
alter table public.client_settings alter column candidate_paper_submission_enabled set default false;

-- public.client_settings.created_at
alter table public.client_settings alter column created_at set default now();

-- public.client_settings.daily_calc_of_invoices
alter table public.client_settings alter column daily_calc_of_invoices set default false;

-- public.client_settings.default_submission_mode
alter table public.client_settings alter column default_submission_mode set default 'ELECTRONIC'::submission_mode_enum;

-- public.client_settings.group_nightsat_sunbh
alter table public.client_settings alter column group_nightsat_sunbh set default false;

-- public.client_settings.healthroster_import_auto_authorise
alter table public.client_settings alter column healthroster_import_auto_authorise set default true;

-- public.client_settings.hr_attach_to_invoice
alter table public.client_settings alter column hr_attach_to_invoice set default true;

-- public.client_settings.hr_validation_required
alter table public.client_settings alter column hr_validation_required set default false;

-- public.client_settings.id
alter table public.client_settings alter column id set default gen_random_uuid();

-- public.client_settings.invoice_consolidation_mode
alter table public.client_settings alter column invoice_consolidation_mode set default 'NONE'::invoice_consolidation_mode_enum;

-- public.client_settings.invoice_reference_required
alter table public.client_settings alter column invoice_reference_required set default false;

-- public.client_settings.is_nhsp
alter table public.client_settings alter column is_nhsp set default false;

-- public.client_settings.nhsp_import_auto_authorise
alter table public.client_settings alter column nhsp_import_auto_authorise set default false;

-- public.client_settings.no_timesheet_required
alter table public.client_settings alter column no_timesheet_required set default false;

-- public.client_settings.opt_in_email
alter table public.client_settings alter column opt_in_email set default true;

-- public.client_settings.opt_in_sms
alter table public.client_settings alter column opt_in_sms set default true;

-- public.client_settings.opt_in_whatsapp
alter table public.client_settings alter column opt_in_whatsapp set default true;

-- public.client_settings.pay_reference_required
alter table public.client_settings alter column pay_reference_required set default false;

-- public.client_settings.reference_number_required_to_issue_invoice
alter table public.client_settings alter column reference_number_required_to_issue_invoice set default false;

-- public.client_settings.requires_hr
alter table public.client_settings alter column requires_hr set default false;

-- public.client_settings.self_bill_no_invoices_sent
alter table public.client_settings alter column self_bill_no_invoices_sent set default false;

-- public.client_settings.send_manual_invoices_to_different_email
alter table public.client_settings alter column send_manual_invoices_to_different_email set default false;

-- public.client_settings.ts_attach_to_invoice
alter table public.client_settings alter column ts_attach_to_invoice set default true;

-- public.client_settings.ts_reference_required
alter table public.client_settings alter column ts_reference_required set default false;

-- public.client_settings.timesheet_break_entry_mode
alter table public.client_settings alter column timesheet_break_entry_mode set default 'START_END_TIMES'::timesheet_break_entry_mode_enum;

-- public.client_settings.updated_at
alter table public.client_settings alter column updated_at set default now();

-- public.clients.created_at
alter table public.clients alter column created_at set default now();

-- public.clients.id
alter table public.clients alter column id set default gen_random_uuid();

-- public.clients.payment_terms_days
alter table public.clients alter column payment_terms_days set default 30;

-- public.clients.updated_at
alter table public.clients alter column updated_at set default now();

-- public.clients.vat_chargeable
alter table public.clients alter column vat_chargeable set default true;

-- public.clients_tombstones.deleted_at
alter table public.clients_tombstones alter column deleted_at set default now();

-- public.comms_outbox.created_at_utc
alter table public.comms_outbox alter column created_at_utc set default now();

-- public.comms_outbox.id
alter table public.comms_outbox alter column id set default gen_random_uuid();

-- public.comms_outbox.provider_payload_json
alter table public.comms_outbox alter column provider_payload_json set default '{}'::jsonb;

-- public.comms_outbox.provider_response_json
alter table public.comms_outbox alter column provider_response_json set default '{}'::jsonb;

-- public.contract_weeks.additional_seq
alter table public.contract_weeks alter column additional_seq set default 0;

-- public.contract_weeks.created_at
alter table public.contract_weeks alter column created_at set default now();

-- public.contract_weeks.enforce_day_partition
alter table public.contract_weeks alter column enforce_day_partition set default false;

-- public.contract_weeks.id
alter table public.contract_weeks alter column id set default gen_random_uuid();

-- public.contract_weeks.is_adjustment
alter table public.contract_weeks alter column is_adjustment set default false;

-- public.contract_weeks.status
alter table public.contract_weeks alter column status set default 'OPEN'::contract_week_status_enum;

-- public.contract_weeks.submission_mode_snapshot
alter table public.contract_weeks alter column submission_mode_snapshot set default 'ELECTRONIC'::submission_mode_enum;

-- public.contract_weeks.updated_at
alter table public.contract_weeks alter column updated_at set default now();

-- public.contracts.auto_invoice
alter table public.contracts alter column auto_invoice set default false;

-- public.contracts.candidate_manager_approval_policy_json
alter table public.contracts alter column candidate_manager_approval_policy_json set default '{"mode": "INHERIT"}'::jsonb;

-- public.contracts.created_at
alter table public.contracts alter column created_at set default now();

-- public.contracts.default_submission_mode
alter table public.contracts alter column default_submission_mode set default 'ELECTRONIC'::submission_mode_enum;

-- public.contracts.id
alter table public.contracts alter column id set default gen_random_uuid();

-- public.contracts.is_ad_hoc
alter table public.contracts alter column is_ad_hoc set default false;

-- public.contracts.overrideclientsettings
alter table public.contracts alter column overrideclientsettings set default false;

-- public.contracts.rates_json
alter table public.contracts alter column rates_json set default '{}'::jsonb;

-- public.contracts.require_reference_to_invoice
alter table public.contracts alter column require_reference_to_invoice set default false;

-- public.contracts.require_reference_to_pay
alter table public.contracts alter column require_reference_to_pay set default false;

-- public.contracts.self_bill
alter table public.contracts alter column self_bill set default false;

-- public.contracts.send_ts_queries_to_different_email
alter table public.contracts alter column send_ts_queries_to_different_email set default false;

-- public.contracts.updated_at
alter table public.contracts alter column updated_at set default now();

-- public.contracts.week_ending_weekday_snapshot
alter table public.contracts alter column week_ending_weekday_snapshot set default 0;

comment on column public.client_settings.timesheet_break_entry_mode is
  'Office-configured break entry presentation for standard validation timesheets.';

comment on column public.contracts.timesheet_break_entry_mode is
  'Optional contract override for break entry presentation; null inherits the client setting.';

-- public.default_job_titles.active
alter table public.default_job_titles alter column active set default true;

-- public.default_job_titles.created_at
alter table public.default_job_titles alter column created_at set default now();

-- public.default_job_titles.depth
alter table public.default_job_titles alter column depth set default 0;

-- public.default_job_titles.id
alter table public.default_job_titles alter column id set default gen_random_uuid();

-- public.default_job_titles.is_role
alter table public.default_job_titles alter column is_role set default false;

-- public.default_job_titles.requires_prof_reg
alter table public.default_job_titles alter column requires_prof_reg set default false;

-- public.default_job_titles.updated_at
alter table public.default_job_titles alter column updated_at set default now();

-- public.document_templates.created_at_utc
alter table public.document_templates alter column created_at_utc set default now();

-- public.document_templates.id
alter table public.document_templates alter column id set default gen_random_uuid();

-- public.document_templates.selected_field_keys
alter table public.document_templates alter column selected_field_keys set default '{}'::text[];

-- public.document_templates.template_content_json
alter table public.document_templates alter column template_content_json set default '{}'::jsonb;

-- public.document_templates.updated_at_utc
alter table public.document_templates alter column updated_at_utc set default now();

-- public.hr_daily_grade_role_mappings.active
alter table public.hr_daily_grade_role_mappings alter column active set default true;

-- public.hr_daily_grade_role_mappings.created_at
alter table public.hr_daily_grade_role_mappings alter column created_at set default now();

-- public.hr_daily_grade_role_mappings.id
alter table public.hr_daily_grade_role_mappings alter column id set default gen_random_uuid();

-- public.hr_daily_grade_role_mappings.updated_at
alter table public.hr_daily_grade_role_mappings alter column updated_at set default now();

-- public.hr_imports.created_at
alter table public.hr_imports alter column created_at set default now();

-- public.hr_imports.id
alter table public.hr_imports alter column id set default gen_random_uuid();

-- public.hr_imports.parse_summary_json
alter table public.hr_imports alter column parse_summary_json set default '{}'::jsonb;

-- public.hr_imports.source_system
alter table public.hr_imports alter column source_system set default 'HEALTHROSTER'::hr_source_enum;

-- public.hr_imports.tz_assumption
alter table public.hr_imports alter column tz_assumption set default 'Europe/London'::text;

-- public.hr_imports.uploaded_at_utc
alter table public.hr_imports alter column uploaded_at_utc set default now();

-- public.hr_issue_email_deliveries.created_at_utc
alter table public.hr_issue_email_deliveries alter column created_at_utc set default now();

-- public.hr_issue_email_deliveries.id
alter table public.hr_issue_email_deliveries alter column id set default gen_random_uuid();

-- public.hr_issue_email_deliveries.reminder_sequence
alter table public.hr_issue_email_deliveries alter column reminder_sequence set default 0;

-- public.hr_issue_email_deliveries.status
alter table public.hr_issue_email_deliveries alter column status set default 'QUEUED'::text;

-- public.hr_issue_email_deliveries.updated_at_utc
alter table public.hr_issue_email_deliveries alter column updated_at_utc set default now();

-- public.hr_issue_email_delivery_items.created_at_utc
alter table public.hr_issue_email_delivery_items alter column created_at_utc set default now();

-- public.hr_issue_email_delivery_items.id
alter table public.hr_issue_email_delivery_items alter column id set default gen_random_uuid();

-- public.hr_issue_emails.created_at
alter table public.hr_issue_emails alter column created_at set default now();

-- public.hr_issue_emails.delivery_history_status
alter table public.hr_issue_emails alter column delivery_history_status set default 'LEGACY_UNVERIFIED'::text;

-- public.hr_issue_emails.id
alter table public.hr_issue_emails alter column id set default gen_random_uuid();

-- public.hr_issue_emails.sent_count
alter table public.hr_issue_emails alter column sent_count set default 0;

-- public.hr_issue_emails.updated_at
alter table public.hr_issue_emails alter column updated_at set default now();

-- public.hr_name_mappings.active
alter table public.hr_name_mappings alter column active set default true;

-- public.hr_name_mappings.created_at
alter table public.hr_name_mappings alter column created_at set default now();

-- public.hr_name_mappings.id
alter table public.hr_name_mappings alter column id set default gen_random_uuid();

-- public.hr_results.created_at_utc
alter table public.hr_results alter column created_at_utc set default now();

-- public.hr_results.id
alter table public.hr_results alter column id set default gen_random_uuid();

-- public.hr_rows.created_at
alter table public.hr_rows alter column created_at set default now();

-- public.hr_rows.id
alter table public.hr_rows alter column id set default gen_random_uuid();

-- public.id_consolidation_runs.created_at_utc
alter table public.id_consolidation_runs alter column created_at_utc set default now();

-- public.id_consolidation_runs.total_delta_ex_vat
alter table public.id_consolidation_runs alter column total_delta_ex_vat set default 0;

-- public.id_consolidation_runs.total_delta_vat
alter table public.id_consolidation_runs alter column total_delta_vat set default 0;

-- public.id_invoice_ledger.current_ex_vat
alter table public.id_invoice_ledger alter column current_ex_vat set default 0;

-- public.id_invoice_ledger.current_inc_vat
alter table public.id_invoice_ledger alter column current_inc_vat set default 0;

-- public.id_invoice_ledger.current_vat
alter table public.id_invoice_ledger alter column current_vat set default 0;

-- public.id_invoice_ledger.last_reported_ex_vat
alter table public.id_invoice_ledger alter column last_reported_ex_vat set default 0;

-- public.id_invoice_ledger.last_reported_inc_vat
alter table public.id_invoice_ledger alter column last_reported_inc_vat set default 0;

-- public.id_invoice_ledger.last_reported_vat
alter table public.id_invoice_ledger alter column last_reported_vat set default 0;

-- public.id_invoice_ledger.updated_at_utc
alter table public.id_invoice_ledger alter column updated_at_utc set default now();

-- public.import_apply_operations.created_at_utc
alter table public.import_apply_operations alter column created_at_utc set default now();

-- public.import_apply_operations.id
alter table public.import_apply_operations alter column id set default gen_random_uuid();

-- public.import_apply_operations.response_json
alter table public.import_apply_operations alter column response_json set default '{}'::jsonb;

-- public.import_apply_operations.state
alter table public.import_apply_operations alter column state set default 'PREPARED'::text;

-- public.import_apply_operations.updated_at_utc
alter table public.import_apply_operations alter column updated_at_utc set default now();

-- public.import_column_aliases.active
alter table public.import_column_aliases alter column active set default true;

-- public.import_column_aliases.created_at
alter table public.import_column_aliases alter column created_at set default now();

-- public.import_column_aliases.id
alter table public.import_column_aliases alter column id set default gen_random_uuid();

-- public.import_column_aliases.updated_at
alter table public.import_column_aliases alter column updated_at set default now();

-- public.import_review_action_outcomes.applied_at_utc
alter table public.import_review_action_outcomes alter column applied_at_utc set default now();

-- public.import_review_action_outcomes.summary_json
alter table public.import_review_action_outcomes alter column summary_json set default '{}'::jsonb;

-- public.import_review_daily_timesheet_resolutions.created_at_utc
alter table public.import_review_daily_timesheet_resolutions alter column created_at_utc set default now();

-- public.import_review_daily_timesheet_resolutions.id
alter table public.import_review_daily_timesheet_resolutions alter column id set default gen_random_uuid();

-- public.import_review_daily_timesheet_resolutions.selected_at_utc
alter table public.import_review_daily_timesheet_resolutions alter column selected_at_utc set default now();

-- public.import_review_daily_timesheet_resolutions.status
alter table public.import_review_daily_timesheet_resolutions alter column status set default 'CURRENT'::text;

-- public.import_review_daily_timesheet_resolutions.updated_at_utc
alter table public.import_review_daily_timesheet_resolutions alter column updated_at_utc set default now();

-- public.import_review_decisions.blocking
alter table public.import_review_decisions alter column blocking set default false;

-- public.import_review_decisions.created_at_utc
alter table public.import_review_decisions alter column created_at_utc set default now();

-- public.import_review_decisions.default_selected
alter table public.import_review_decisions alter column default_selected set default false;

-- public.import_review_decisions.is_current
alter table public.import_review_decisions alter column is_current set default true;

-- public.import_review_decisions.refreshed_at_utc
alter table public.import_review_decisions alter column refreshed_at_utc set default now();

-- public.import_review_decisions.requires_reconfirmation
alter table public.import_review_decisions alter column requires_reconfirmation set default false;

-- public.import_review_decisions.selectable
alter table public.import_review_decisions alter column selectable set default false;

-- public.import_review_decisions.selected
alter table public.import_review_decisions alter column selected set default false;

-- public.import_review_decisions.summary_json
alter table public.import_review_decisions alter column summary_json set default '{}'::jsonb;

-- public.import_review_events.created_at_utc
alter table public.import_review_events alter column created_at_utc set default now();

-- public.import_review_events.event_context_json
alter table public.import_review_events alter column event_context_json set default '{}'::jsonb;

-- public.import_review_scope_candidates.created_at_utc
alter table public.import_review_scope_candidates alter column created_at_utc set default now();

-- public.import_review_scope_candidates.id
alter table public.import_review_scope_candidates alter column id set default gen_random_uuid();

-- public.import_review_scope_clients.created_at_utc
alter table public.import_review_scope_clients alter column created_at_utc set default now();

-- public.import_review_scope_clients.id
alter table public.import_review_scope_clients alter column id set default gen_random_uuid();

-- public.import_review_states.created_at_utc
alter table public.import_review_states alter column created_at_utc set default now();

-- public.import_review_states.follow_up_retry_count
alter table public.import_review_states alter column follow_up_retry_count set default 0;

-- public.import_review_states.follow_up_status
alter table public.import_review_states alter column follow_up_status set default 'NOT_REQUIRED'::text;

-- public.import_review_states.preview_generation
alter table public.import_review_states alter column preview_generation set default 0;

-- public.import_review_states.schema_contract_version
alter table public.import_review_states alter column schema_contract_version set default 'IMPORT_REVIEW_DB_V1'::text;

-- public.import_review_states.state_version
alter table public.import_review_states alter column state_version set default 1;

-- public.import_review_states.status
alter table public.import_review_states alter column status set default 'STAGED'::text;

-- public.import_review_states.ui_state_json
alter table public.import_review_states alter column ui_state_json set default '{}'::jsonb;

-- public.import_review_states.updated_at_utc
alter table public.import_review_states alter column updated_at_utc set default now();

-- public.import_review_weekly_validation_resolutions.created_at_utc
alter table public.import_review_weekly_validation_resolutions alter column created_at_utc set default now();

-- public.import_review_weekly_validation_resolutions.id
alter table public.import_review_weekly_validation_resolutions alter column id set default gen_random_uuid();

-- public.import_review_weekly_validation_resolutions.selected_at_utc
alter table public.import_review_weekly_validation_resolutions alter column selected_at_utc set default now();

-- public.import_review_weekly_validation_resolutions.status
alter table public.import_review_weekly_validation_resolutions alter column status set default 'CURRENT'::text;

-- public.import_review_weekly_validation_resolutions.updated_at_utc
alter table public.import_review_weekly_validation_resolutions alter column updated_at_utc set default now();

-- public.invoice_document_assets.created_at_utc
alter table public.invoice_document_assets alter column created_at_utc set default now();

-- public.invoice_document_assets.id
alter table public.invoice_document_assets alter column id set default gen_random_uuid();

-- public.invoice_document_assets.normalised_manifest_json
alter table public.invoice_document_assets alter column normalised_manifest_json set default '[]'::jsonb;

-- public.invoice_document_assets.status
alter table public.invoice_document_assets alter column status set default 'DISCOVERED'::text;

-- public.invoice_document_assets.updated_at_utc
alter table public.invoice_document_assets alter column updated_at_utc set default now();

-- public.invoice_document_versions.created_at_utc
alter table public.invoice_document_versions alter column created_at_utc set default now();

-- public.invoice_document_versions.id
alter table public.invoice_document_versions alter column id set default gen_random_uuid();

-- public.invoice_document_versions.manifest_json
alter table public.invoice_document_versions alter column manifest_json set default '[]'::jsonb;

-- public.invoice_document_versions.snapshot_json
alter table public.invoice_document_versions alter column snapshot_json set default '{}'::jsonb;

-- public.invoice_document_versions.status
alter table public.invoice_document_versions alter column status set default 'PLANNING'::text;

-- public.invoice_jobs_outbox.attempt_count
alter table public.invoice_jobs_outbox alter column attempt_count set default 0;

-- public.invoice_jobs_outbox.created_at
alter table public.invoice_jobs_outbox alter column created_at set default now();

-- public.invoice_jobs_outbox.id
alter table public.invoice_jobs_outbox alter column id set default gen_random_uuid();

-- public.invoice_lines.created_at
alter table public.invoice_lines alter column created_at set default now();

-- public.invoice_lines.hours_bh
alter table public.invoice_lines alter column hours_bh set default 0;

-- public.invoice_lines.hours_day
alter table public.invoice_lines alter column hours_day set default 0;

-- public.invoice_lines.hours_night
alter table public.invoice_lines alter column hours_night set default 0;

-- public.invoice_lines.hours_sat
alter table public.invoice_lines alter column hours_sat set default 0;

-- public.invoice_lines.hours_sun
alter table public.invoice_lines alter column hours_sun set default 0;

-- public.invoice_lines.id
alter table public.invoice_lines alter column id set default gen_random_uuid();

-- public.invoice_lines.margin_ex_vat
alter table public.invoice_lines alter column margin_ex_vat set default 0;

-- public.invoice_lines.meta_json
alter table public.invoice_lines alter column meta_json set default '{}'::jsonb;

-- public.invoice_lines.total_charge_ex_vat
alter table public.invoice_lines alter column total_charge_ex_vat set default 0;

-- public.invoice_lines.total_inc_vat
alter table public.invoice_lines alter column total_inc_vat set default 0;

-- public.invoice_lines.total_pay_ex_vat
alter table public.invoice_lines alter column total_pay_ex_vat set default 0;

-- public.invoice_lines.vat_amount
alter table public.invoice_lines alter column vat_amount set default 0;

-- public.invoice_lines.vat_rate_pct
alter table public.invoice_lines alter column vat_rate_pct set default 20.00;

-- public.invoice_operation_chunks.attempt_count
alter table public.invoice_operation_chunks alter column attempt_count set default 0;

-- public.invoice_operation_chunks.created_at_utc
alter table public.invoice_operation_chunks alter column created_at_utc set default now();

-- public.invoice_operation_chunks.fence_token
alter table public.invoice_operation_chunks alter column fence_token set default 0;

-- public.invoice_operation_chunks.id
alter table public.invoice_operation_chunks alter column id set default gen_random_uuid();

-- public.invoice_operation_chunks.is_manifest_member
alter table public.invoice_operation_chunks alter column is_manifest_member set default false;

-- public.invoice_operation_chunks.level_no
alter table public.invoice_operation_chunks alter column level_no set default 0;

-- public.invoice_operation_chunks.manifest_committed
alter table public.invoice_operation_chunks alter column manifest_committed set default true;

-- public.invoice_operation_chunks.manifest_generation
alter table public.invoice_operation_chunks alter column manifest_generation set default 0;

-- public.invoice_operation_chunks.max_attempts
alter table public.invoice_operation_chunks alter column max_attempts set default 5;

-- public.invoice_operation_chunks.operation_control_version
alter table public.invoice_operation_chunks alter column operation_control_version set default 1;

-- public.invoice_operation_chunks.payload_json
alter table public.invoice_operation_chunks alter column payload_json set default '{}'::jsonb;

-- public.invoice_operation_chunks.plan_generation
alter table public.invoice_operation_chunks alter column plan_generation set default 1;

-- public.invoice_operation_chunks.priority
alter table public.invoice_operation_chunks alter column priority set default 200;

-- public.invoice_operation_chunks.progress_json
alter table public.invoice_operation_chunks alter column progress_json set default '{}'::jsonb;

-- public.invoice_operation_chunks.replacement_required
alter table public.invoice_operation_chunks alter column replacement_required set default false;

-- public.invoice_operation_chunks.result_visible
alter table public.invoice_operation_chunks alter column result_visible set default false;

-- public.invoice_operation_chunks.run_after_utc
alter table public.invoice_operation_chunks alter column run_after_utc set default now();

-- public.invoice_operation_chunks.status
alter table public.invoice_operation_chunks alter column status set default 'QUEUED'::text;

-- public.invoice_operation_chunks.updated_at_utc
alter table public.invoice_operation_chunks alter column updated_at_utc set default now();

-- public.invoice_operations.change_seq
alter table public.invoice_operations alter column change_seq set default nextval('invoice_operation_change_seq'::regclass);

-- public.invoice_operations.chunk_count
alter table public.invoice_operations alter column chunk_count set default 0;

-- public.invoice_operations.completed_units
alter table public.invoice_operations alter column completed_units set default 0;

-- public.invoice_operations.config_json
alter table public.invoice_operations alter column config_json set default '{}'::jsonb;

-- public.invoice_operations.control_version
alter table public.invoice_operations alter column control_version set default 1;

-- public.invoice_operations.created_at_utc
alter table public.invoice_operations alter column created_at_utc set default now();

-- public.invoice_operations.failed_units
alter table public.invoice_operations alter column failed_units set default 0;

-- public.invoice_operations.id
alter table public.invoice_operations alter column id set default gen_random_uuid();

-- public.invoice_operations.input_json
alter table public.invoice_operations alter column input_json set default '{}'::jsonb;

-- public.invoice_operations.manifest_committed
alter table public.invoice_operations alter column manifest_committed set default true;

-- public.invoice_operations.manifest_generation
alter table public.invoice_operations alter column manifest_generation set default 0;

-- public.invoice_operations.phase
alter table public.invoice_operations alter column phase set default 'SUBMITTED'::text;

-- public.invoice_operations.priority
alter table public.invoice_operations alter column priority set default 200;

-- public.invoice_operations.progress_json
alter table public.invoice_operations alter column progress_json set default '{}'::jsonb;

-- public.invoice_operations.release_complete
alter table public.invoice_operations alter column release_complete set default true;

-- public.invoice_operations.requires_user_action
alter table public.invoice_operations alter column requires_user_action set default false;

-- public.invoice_operations.result_page_revision
alter table public.invoice_operations alter column result_page_revision set default 0;

-- public.invoice_operations.status
alter table public.invoice_operations alter column status set default 'QUEUED'::text;

-- public.invoice_operations.total_units
alter table public.invoice_operations alter column total_units set default 0;

-- public.invoice_operations.updated_at_utc
alter table public.invoice_operations alter column updated_at_utc set default now();

-- public.invoice_pdf_outbox.attempt_count
alter table public.invoice_pdf_outbox alter column attempt_count set default 0;

-- public.invoice_pdf_outbox.created_at
alter table public.invoice_pdf_outbox alter column created_at set default now();

-- public.invoice_pdf_outbox.force_regen
alter table public.invoice_pdf_outbox alter column force_regen set default false;

-- public.invoice_pdf_outbox.id
alter table public.invoice_pdf_outbox alter column id set default gen_random_uuid();

-- public.invoices.created_at
alter table public.invoices alter column created_at set default now();

-- public.invoices.do_not_send
alter table public.invoices alter column do_not_send set default false;

-- public.invoices.document_revision
alter table public.invoices alter column document_revision set default 1;

-- public.invoices.document_state
alter table public.invoices alter column document_state set default 'NOT_REQUESTED'::text;

-- public.invoices.header_snapshot_json
alter table public.invoices alter column header_snapshot_json set default '{}'::jsonb;

-- public.invoices.id
alter table public.invoices alter column id set default gen_random_uuid();

-- public.invoices.issue_state
alter table public.invoices alter column issue_state set default 'NOT_STARTED'::text;

-- public.invoices.status
alter table public.invoices alter column status set default 'DRAFT'::invoice_status_enum;

-- public.invoices.status_date_utc
alter table public.invoices alter column status_date_utc set default now();

-- public.invoices.subtotal_ex_vat
alter table public.invoices alter column subtotal_ex_vat set default 0;

-- public.invoices.total_inc_vat
alter table public.invoices alter column total_inc_vat set default 0;

-- public.invoices.type
alter table public.invoices alter column type set default 'INVOICE'::invoice_type_enum;

-- public.invoices.updated_at
alter table public.invoices alter column updated_at set default now();

-- public.invoices.vat_amount
alter table public.invoices alter column vat_amount set default 0;

-- public.legacy_contract_rate_lines.created_at
alter table public.legacy_contract_rate_lines alter column created_at set default now();

-- public.legacy_contract_rate_lines.id
alter table public.legacy_contract_rate_lines alter column id set default gen_random_uuid();

-- public.legacy_contracts.created_at
alter table public.legacy_contracts alter column created_at set default now();

-- public.legacy_contracts.id
alter table public.legacy_contracts alter column id set default gen_random_uuid();

-- public.legacy_contracts.source_system
alter table public.legacy_contracts alter column source_system set default 'ECLIPSE'::text;

-- public.legacy_eclipse_candidate_map.imported_at
alter table public.legacy_eclipse_candidate_map alter column imported_at set default now();

-- public.legacy_eclipse_client_map.imported_at
alter table public.legacy_eclipse_client_map alter column imported_at set default now();

-- public.mail_outbox.attachments_ready
alter table public.mail_outbox alter column attachments_ready set default true;

-- public.mail_outbox.created_at_utc
alter table public.mail_outbox alter column created_at_utc set default now();

-- public.mail_outbox.id
alter table public.mail_outbox alter column id set default gen_random_uuid();

-- public.mail_outbox.payment_scope_json
alter table public.mail_outbox alter column payment_scope_json set default '{}'::jsonb;

-- public.mail_outbox.status
alter table public.mail_outbox alter column status set default 'QUEUED'::mail_status_enum;

-- public.mailshot_field_overrides.enabled_local
alter table public.mailshot_field_overrides alter column enabled_local set default true;

-- public.mailshot_field_overrides.id
alter table public.mailshot_field_overrides alter column id set default gen_random_uuid();

-- public.mailshot_fields.allowed_entity_types
alter table public.mailshot_fields alter column allowed_entity_types set default '{}'::text[];

-- public.mailshot_fields.created_at_utc
alter table public.mailshot_fields alter column created_at_utc set default now();

-- public.mailshot_fields.enabled_global
alter table public.mailshot_fields alter column enabled_global set default true;

-- public.mailshot_fields.id
alter table public.mailshot_fields alter column id set default gen_random_uuid();

-- public.mailshot_fields.resolver_spec_json
alter table public.mailshot_fields alter column resolver_spec_json set default '{}'::jsonb;

-- public.mailshot_fields.updated_at_utc
alter table public.mailshot_fields alter column updated_at_utc set default now();

-- public.mailshot_runs.created_at_utc
alter table public.mailshot_runs alter column created_at_utc set default now();

-- public.mailshot_runs.delivery_timing_json
alter table public.mailshot_runs alter column delivery_timing_json set default '{}'::jsonb;

-- public.mailshot_runs.id
alter table public.mailshot_runs alter column id set default gen_random_uuid();

-- public.mailshot_runs.result_json
alter table public.mailshot_runs alter column result_json set default '{}'::jsonb;

-- public.manual_timesheet_queue.id
alter table public.manual_timesheet_queue alter column id set default gen_random_uuid();

-- public.manual_timesheet_queue.last_rotation_deg
alter table public.manual_timesheet_queue alter column last_rotation_deg set default 0;

-- public.manual_timesheet_queue.meta_json
alter table public.manual_timesheet_queue alter column meta_json set default '{}'::jsonb;

-- public.manual_timesheet_queue.status
alter table public.manual_timesheet_queue alter column status set default 'QUEUED'::text;

-- public.manual_timesheet_queue.uploaded_at_utc
alter table public.manual_timesheet_queue alter column uploaded_at_utc set default now();

-- public.migration_smoke_once_only.created_at
alter table public.migration_smoke_once_only alter column created_at set default now();

-- public.migration_smoke_once_only.id
alter table public.migration_smoke_once_only alter column id set default nextval('migration_smoke_once_only_id_seq'::regclass);

-- public.nhsp_shifts.break_mins
alter table public.nhsp_shifts alter column break_mins set default 0;

-- public.nhsp_shifts.created_at
alter table public.nhsp_shifts alter column created_at set default now();

-- public.nhsp_shifts.id
alter table public.nhsp_shifts alter column id set default gen_random_uuid();

-- public.nhsp_shifts.invoice_status
alter table public.nhsp_shifts alter column invoice_status set default 'PENDING'::text;

-- public.nhsp_shifts.pay_minutes
alter table public.nhsp_shifts alter column pay_minutes set default 0;

-- public.nhsp_shifts.source_system
alter table public.nhsp_shifts alter column source_system set default 'NHSP'::hr_source_enum;

-- public.nhsp_shifts.updated_at
alter table public.nhsp_shifts alter column updated_at set default now();

-- public.pay_advance_patches.id
alter table public.pay_advance_patches alter column id set default gen_random_uuid();

-- public.pay_advance_patches.patched_at_utc
alter table public.pay_advance_patches alter column patched_at_utc set default now();

-- public.pay_advance_reservations.created_at_utc
alter table public.pay_advance_reservations alter column created_at_utc set default now();

-- public.pay_advance_reservations.id
alter table public.pay_advance_reservations alter column id set default gen_random_uuid();

-- public.pay_advances.advance_kind
alter table public.pay_advances alter column advance_kind set default 'LEGACY_ADVANCE'::pay_advance_kind_enum;

-- public.pay_advances.created_at
alter table public.pay_advances alter column created_at set default now();

-- public.pay_advances.id
alter table public.pay_advances alter column id set default gen_random_uuid();

-- public.pay_advances.oneoff_bank_details_required
alter table public.pay_advances alter column oneoff_bank_details_required set default false;

-- public.pay_advances.schedule_json
alter table public.pay_advances alter column schedule_json set default '[]'::jsonb;

-- public.pay_advances.status
alter table public.pay_advances alter column status set default 'ACTIVE'::pay_advance_status_enum;

-- public.pay_advances.updated_at
alter table public.pay_advances alter column updated_at set default now();

-- public.pay_bank_transfer_events.created_at_utc
alter table public.pay_bank_transfer_events alter column created_at_utc set default now();

-- public.pay_bank_transfer_events.currency
alter table public.pay_bank_transfer_events alter column currency set default 'GBP'::text;

-- public.pay_bank_transfer_events.id
alter table public.pay_bank_transfer_events alter column id set default gen_random_uuid();

-- public.pay_bank_transfer_events.mapping_hints_json
alter table public.pay_bank_transfer_events alter column mapping_hints_json set default '{}'::jsonb;

-- public.pay_bank_transfer_events.raw_payload
alter table public.pay_bank_transfer_events alter column raw_payload set default '{}'::jsonb;

-- public.pay_bank_transfer_events.received_at_utc
alter table public.pay_bank_transfer_events alter column received_at_utc set default now();

-- public.pay_bank_transfers.created_at_utc
alter table public.pay_bank_transfers alter column created_at_utc set default now();

-- public.pay_bank_transfers.currency
alter table public.pay_bank_transfers alter column currency set default 'GBP'::text;

-- public.pay_bank_transfers.id
alter table public.pay_bank_transfers alter column id set default gen_random_uuid();

-- public.pay_bank_transfers.payee_entity_kind
alter table public.pay_bank_transfers alter column payee_entity_kind set default 'CANDIDATE'::text;

-- public.pay_bank_transfers.rail_env
alter table public.pay_bank_transfers alter column rail_env set default 'PROD'::text;

-- public.pay_bank_transfers.rail_provider
alter table public.pay_bank_transfers alter column rail_provider set default 'CSV'::text;

-- public.pay_bank_transfers.status
alter table public.pay_bank_transfers alter column status set default 'PENDING'::text;

-- public.pay_batch_auth_actions.action_at_utc
alter table public.pay_batch_auth_actions alter column action_at_utc set default now();

-- public.pay_batch_auth_actions.id
alter table public.pay_batch_auth_actions alter column id set default gen_random_uuid();

-- public.pay_batch_auth_requests.created_at_utc
alter table public.pay_batch_auth_requests alter column created_at_utc set default now();

-- public.pay_batch_auth_requests.golden_key_used
alter table public.pay_batch_auth_requests alter column golden_key_used set default false;

-- public.pay_batch_auth_requests.id
alter table public.pay_batch_auth_requests alter column id set default gen_random_uuid();

-- public.pay_batch_auth_requests.state
alter table public.pay_batch_auth_requests alter column state set default 'AWAITING'::text;

-- public.pay_batch_auth_tokens.created_at_utc
alter table public.pay_batch_auth_tokens alter column created_at_utc set default now();

-- public.pay_batch_candidates.awaiting_net_amount
alter table public.pay_batch_candidates alter column awaiting_net_amount set default false;

-- public.pay_batch_candidates.id
alter table public.pay_batch_candidates alter column id set default gen_random_uuid();

-- public.pay_batch_candidates.overpayment_recovery_taken
alter table public.pay_batch_candidates alter column overpayment_recovery_taken set default 0;

-- public.pay_batch_candidates.updated_at
alter table public.pay_batch_candidates alter column updated_at set default now();

-- public.pay_batch_display_summary.candidate_count
alter table public.pay_batch_display_summary alter column candidate_count set default 0;

-- public.pay_batch_display_summary.created_at_utc
alter table public.pay_batch_display_summary alter column created_at_utc set default now();

-- public.pay_batch_display_summary.id
alter table public.pay_batch_display_summary alter column id set default gen_random_uuid();

-- public.pay_batch_display_summary.issue_summary_counts
alter table public.pay_batch_display_summary alter column issue_summary_counts set default '{}'::jsonb;

-- public.pay_batch_display_summary.item_count
alter table public.pay_batch_display_summary alter column item_count set default 0;

-- public.pay_batch_display_summary.stale_summary_json
alter table public.pay_batch_display_summary alter column stale_summary_json set default '{}'::jsonb;

-- public.pay_batch_display_summary.summary_version
alter table public.pay_batch_display_summary alter column summary_version set default 1;

-- public.pay_batch_display_summary.total_bank_out
alter table public.pay_batch_display_summary alter column total_bank_out set default 0;

-- public.pay_batch_display_summary.total_payable
alter table public.pay_batch_display_summary alter column total_payable set default 0;

-- public.pay_batch_display_summary.transfer_count
alter table public.pay_batch_display_summary alter column transfer_count set default 0;

-- public.pay_batch_display_summary.updated_at_utc
alter table public.pay_batch_display_summary alter column updated_at_utc set default now();

-- public.pay_batch_item_breakdowns.amount_vat
alter table public.pay_batch_item_breakdowns alter column amount_vat set default 0;

-- public.pay_batch_item_breakdowns.created_at_utc
alter table public.pay_batch_item_breakdowns alter column created_at_utc set default now();

-- public.pay_batch_item_breakdowns.id
alter table public.pay_batch_item_breakdowns alter column id set default gen_random_uuid();

-- public.pay_batch_item_breakdowns.meta_json
alter table public.pay_batch_item_breakdowns alter column meta_json set default '{}'::jsonb;

-- public.pay_batch_items.created_at
alter table public.pay_batch_items alter column created_at set default now();

-- public.pay_batch_items.id
alter table public.pay_batch_items alter column id set default gen_random_uuid();

-- public.pay_batch_items.is_mismatch
alter table public.pay_batch_items alter column is_mismatch set default false;

-- public.pay_batch_items.is_voided
alter table public.pay_batch_items alter column is_voided set default false;

-- public.pay_batch_items.updated_at
alter table public.pay_batch_items alter column updated_at set default now();

-- public.pay_batch_paye_net_inputs.id
alter table public.pay_batch_paye_net_inputs alter column id set default gen_random_uuid();

-- public.pay_batch_paye_net_inputs.imported_at_utc
alter table public.pay_batch_paye_net_inputs alter column imported_at_utc set default now();

-- public.pay_batch_timesheet_snapshots.created_at_utc
alter table public.pay_batch_timesheet_snapshots alter column created_at_utc set default now();

-- public.pay_batch_timesheet_snapshots.id
alter table public.pay_batch_timesheet_snapshots alter column id set default gen_random_uuid();

-- public.pay_batches.created_at_utc
alter table public.pay_batches alter column created_at_utc set default now();

-- public.pay_batches.execution_commit_state
alter table public.pay_batches alter column execution_commit_state set default 'NOT_SUBMITTED'::text;

-- public.pay_batches.id
alter table public.pay_batches alter column id set default gen_random_uuid();

-- public.pay_batches.rail_env_snapshot
alter table public.pay_batches alter column rail_env_snapshot set default 'PROD'::text;

-- public.pay_batches.rail_provider_snapshot
alter table public.pay_batches alter column rail_provider_snapshot set default 'CSV'::text;

-- public.pay_batches.same_week_paye_override_used
alter table public.pay_batches alter column same_week_paye_override_used set default false;

-- public.pay_finance_case_components.allocation_priority_group
alter table public.pay_finance_case_components alter column allocation_priority_group set default 0;

-- public.pay_finance_case_components.allocation_priority_order
alter table public.pay_finance_case_components alter column allocation_priority_order set default 0;

-- public.pay_finance_case_components.created_at_utc
alter table public.pay_finance_case_components alter column created_at_utc set default now();

-- public.pay_finance_case_components.id
alter table public.pay_finance_case_components alter column id set default gen_random_uuid();

-- public.pay_finance_case_components.is_resolution_stale
alter table public.pay_finance_case_components alter column is_resolution_stale set default false;

-- public.pay_finance_case_components.remaining_source_amount
alter table public.pay_finance_case_components alter column remaining_source_amount set default 0;

-- public.pay_finance_case_components.source_amount
alter table public.pay_finance_case_components alter column source_amount set default 0;

-- public.pay_finance_case_components.source_basis_json
alter table public.pay_finance_case_components alter column source_basis_json set default '{}'::jsonb;

-- public.pay_finance_case_components.updated_at_utc
alter table public.pay_finance_case_components alter column updated_at_utc set default now();

-- public.pay_finance_case_events.event_at_utc
alter table public.pay_finance_case_events alter column event_at_utc set default now();

-- public.pay_finance_case_events.id
alter table public.pay_finance_case_events alter column id set default gen_random_uuid();

-- public.pay_finance_case_oneoff_payout_bank_details.created_at_utc
alter table public.pay_finance_case_oneoff_payout_bank_details alter column created_at_utc set default now();

-- public.pay_finance_case_oneoff_payout_bank_details.updated_at_utc
alter table public.pay_finance_case_oneoff_payout_bank_details alter column updated_at_utc set default now();

-- public.pay_item_snoozes.created_at_utc
alter table public.pay_item_snoozes alter column created_at_utc set default now();

-- public.pay_item_snoozes.id
alter table public.pay_item_snoozes alter column id set default gen_random_uuid();

-- public.pay_item_snoozes.snooze_kind
alter table public.pay_item_snoozes alter column snooze_kind set default 'DO_NOT_PAY'::text;

-- public.pay_manual_adjustment_carry_forwards.created_at_utc
alter table public.pay_manual_adjustment_carry_forwards alter column created_at_utc set default now();

-- public.pay_manual_adjustment_carry_forwards.id
alter table public.pay_manual_adjustment_carry_forwards alter column id set default gen_random_uuid();

-- public.pay_manual_adjustment_carry_forwards.source_snapshot_json
alter table public.pay_manual_adjustment_carry_forwards alter column source_snapshot_json set default '{}'::jsonb;

-- public.pay_manual_adjustment_carry_forwards.status
alter table public.pay_manual_adjustment_carry_forwards alter column status set default 'PENDING_CARRY_FORWARD'::text;

-- public.pay_manual_adjustment_carry_forwards.tax_treatment_json
alter table public.pay_manual_adjustment_carry_forwards alter column tax_treatment_json set default '{}'::jsonb;

-- public.pay_manual_adjustment_carry_forwards.updated_at_utc
alter table public.pay_manual_adjustment_carry_forwards alter column updated_at_utc set default now();

-- public.pay_payment_correction_actions.action_at_utc
alter table public.pay_payment_correction_actions alter column action_at_utc set default now();

-- public.pay_payment_correction_actions.id
alter table public.pay_payment_correction_actions alter column id set default gen_random_uuid();

-- public.pay_payment_correction_actions.metadata_json
alter table public.pay_payment_correction_actions alter column metadata_json set default '{}'::jsonb;

-- public.pay_payment_correction_items.created_at_utc
alter table public.pay_payment_correction_items alter column created_at_utc set default now();

-- public.pay_payment_correction_items.id
alter table public.pay_payment_correction_items alter column id set default gen_random_uuid();

-- public.pay_payment_correction_items.status
alter table public.pay_payment_correction_items alter column status set default 'APPLIED'::text;

-- public.pay_payment_correction_request_candidates.created_at_utc
alter table public.pay_payment_correction_request_candidates alter column created_at_utc set default clock_timestamp();

-- public.pay_payment_correction_requests.approved_count
alter table public.pay_payment_correction_requests alter column approved_count set default 0;

-- public.pay_payment_correction_requests.auto_requested
alter table public.pay_payment_correction_requests alter column auto_requested set default false;

-- public.pay_payment_correction_requests.created_at_utc
alter table public.pay_payment_correction_requests alter column created_at_utc set default now();

-- public.pay_payment_correction_requests.golden_key_used
alter table public.pay_payment_correction_requests alter column golden_key_used set default false;

-- public.pay_payment_correction_requests.id
alter table public.pay_payment_correction_requests alter column id set default gen_random_uuid();

-- public.pay_payment_correction_requests.plan_json
alter table public.pay_payment_correction_requests alter column plan_json set default '{}'::jsonb;

-- public.pay_payment_correction_requests.requested_at_utc
alter table public.pay_payment_correction_requests alter column requested_at_utc set default now();

-- public.pay_payment_correction_requests.required_quantity
alter table public.pay_payment_correction_requests alter column required_quantity set default 1;

-- public.pay_payment_correction_requests.selection_json
alter table public.pay_payment_correction_requests alter column selection_json set default '{}'::jsonb;

-- public.pay_payment_correction_requests.updated_at_utc
alter table public.pay_payment_correction_requests alter column updated_at_utc set default now();

-- public.pay_payment_correction_work_items.attempt_count
alter table public.pay_payment_correction_work_items alter column attempt_count set default 0;

-- public.pay_payment_correction_work_items.created_at_utc
alter table public.pay_payment_correction_work_items alter column created_at_utc set default now();

-- public.pay_payment_correction_work_items.id
alter table public.pay_payment_correction_work_items alter column id set default gen_random_uuid();

-- public.pay_payment_correction_work_items.result_json
alter table public.pay_payment_correction_work_items alter column result_json set default '{}'::jsonb;

-- public.pay_payment_correction_work_items.selection_json
alter table public.pay_payment_correction_work_items alter column selection_json set default '{}'::jsonb;

-- public.pay_payment_correction_work_items.status
alter table public.pay_payment_correction_work_items alter column status set default 'PENDING'::text;

-- public.pay_payment_return_notice_groups.created_at_utc
alter table public.pay_payment_return_notice_groups alter column created_at_utc set default now();

-- public.pay_payment_return_notice_groups.id
alter table public.pay_payment_return_notice_groups alter column id set default gen_random_uuid();

-- public.pay_payment_return_notice_groups.mail_outbox_ids
alter table public.pay_payment_return_notice_groups alter column mail_outbox_ids set default '[]'::jsonb;

-- public.pay_payment_return_notice_groups.status
alter table public.pay_payment_return_notice_groups alter column status set default 'OPEN'::text;

-- public.pay_payment_return_notice_groups.summary_json
alter table public.pay_payment_return_notice_groups alter column summary_json set default '{}'::jsonb;

-- public.pay_payment_return_notice_groups.updated_at_utc
alter table public.pay_payment_return_notice_groups alter column updated_at_utc set default now();

-- public.pay_snooze_warning_acknowledgements.created_at_utc
alter table public.pay_snooze_warning_acknowledgements alter column created_at_utc set default now();

-- public.pay_snooze_warning_acknowledgements.id
alter table public.pay_snooze_warning_acknowledgements alter column id set default gen_random_uuid();

-- public.rates_candidate_overrides.created_at
alter table public.rates_candidate_overrides alter column created_at set default now();

-- public.rates_candidate_overrides.id
alter table public.rates_candidate_overrides alter column id set default gen_random_uuid();

-- public.rates_candidate_overrides.updated_at
alter table public.rates_candidate_overrides alter column updated_at set default now();

-- public.rates_client_defaults.created_at
alter table public.rates_client_defaults alter column created_at set default now();

-- public.rates_client_defaults.id
alter table public.rates_client_defaults alter column id set default gen_random_uuid();

-- public.rates_client_defaults.updated_at
alter table public.rates_client_defaults alter column updated_at set default now();

-- public.rates_presets.created_at
alter table public.rates_presets alter column created_at set default now();

-- public.rates_presets.enable_paye
alter table public.rates_presets alter column enable_paye set default false;

-- public.rates_presets.enable_umbrella
alter table public.rates_presets alter column enable_umbrella set default false;

-- public.rates_presets.id
alter table public.rates_presets alter column id set default gen_random_uuid();

-- public.rates_presets.updated_at
alter table public.rates_presets alter column updated_at set default now();

-- public.report_presets.created_at
alter table public.report_presets alter column created_at set default now();

-- public.report_presets.filters_json
alter table public.report_presets alter column filters_json set default '{}'::jsonb;

-- public.report_presets.id
alter table public.report_presets alter column id set default gen_random_uuid();

-- public.report_presets.is_default
alter table public.report_presets alter column is_default set default false;

-- public.report_presets.is_shared
alter table public.report_presets alter column is_shared set default false;

-- public.report_presets.kind
alter table public.report_presets alter column kind set default 'search'::text;

-- public.report_presets.updated_at
alter table public.report_presets alter column updated_at set default now();

-- public.schema_migrations.applied_at
alter table public.schema_migrations alter column applied_at set default now();

-- public.schema_repeatables.applied_at
alter table public.schema_repeatables alter column applied_at set default now();

-- public.settings_defaults.auto_authorise_on_validation
alter table public.settings_defaults alter column auto_authorise_on_validation set default false;

-- public.settings_defaults.banking_pay_auto_unwind_terminal_no_money
alter table public.settings_defaults alter column banking_pay_auto_unwind_terminal_no_money set default false;

-- public.settings_defaults.banking_pay_cancellation_reversion_exact_empty_v1_enabled
alter table public.settings_defaults alter column banking_pay_cancellation_reversion_exact_empty_v1_enabled set default false;

-- public.settings_defaults.banking_pay_cancellation_reversion_observe_v1_enabled
alter table public.settings_defaults alter column banking_pay_cancellation_reversion_observe_v1_enabled set default false;

-- public.settings_defaults.banking_pay_cancellation_reversion_publish_v1_enabled
alter table public.settings_defaults alter column banking_pay_cancellation_reversion_publish_v1_enabled set default false;

-- public.settings_defaults.banking_pay_candidate_cancellation_enabled
alter table public.settings_defaults alter column banking_pay_candidate_cancellation_enabled set default false;

-- public.settings_defaults.banking_pay_correction_held_dirty_route_absorption_v1_enabled
alter table public.settings_defaults alter column banking_pay_correction_held_dirty_route_absorption_v1_enabled set default false;

-- public.settings_defaults.banking_pay_correction_max_active_items
alter table public.settings_defaults alter column banking_pay_correction_max_active_items set default 250000;

-- public.settings_defaults.banking_pay_correction_max_active_items_per_candidate
alter table public.settings_defaults alter column banking_pay_correction_max_active_items_per_candidate set default 128;

-- public.settings_defaults.banking_pay_correction_max_candidates
alter table public.settings_defaults alter column banking_pay_correction_max_candidates set default 10000;

-- public.settings_defaults.banking_pay_correction_max_source_rows_per_candidate
alter table public.settings_defaults alter column banking_pay_correction_max_source_rows_per_candidate set default 512;

-- public.settings_defaults.banking_pay_correction_request_dirty_deferral_v1_enabled
alter table public.settings_defaults alter column banking_pay_correction_request_dirty_deferral_v1_enabled set default false;

-- public.settings_defaults.banking_pay_draft_create_adoption_v1_enabled
alter table public.settings_defaults alter column banking_pay_draft_create_adoption_v1_enabled set default false;

-- public.settings_defaults.banking_pay_draft_expected_effects_v1_enabled
alter table public.settings_defaults alter column banking_pay_draft_expected_effects_v1_enabled set default false;

-- public.settings_defaults.banking_pay_draft_overlay_fast_cancel_v1_enabled
alter table public.settings_defaults alter column banking_pay_draft_overlay_fast_cancel_v1_enabled set default false;

-- public.settings_defaults.banking_pay_draft_self_invalidation_claim_deferral_v1_enabled
alter table public.settings_defaults alter column banking_pay_draft_self_invalidation_claim_deferral_v1_enabled set default false;

-- public.settings_defaults.banking_pay_draft_step_rpc_v1_enabled
alter table public.settings_defaults alter column banking_pay_draft_step_rpc_v1_enabled set default false;

-- public.settings_defaults.banking_pay_execution_refresh_owner_bridge_v1_observe_enabled
alter table public.settings_defaults alter column banking_pay_execution_refresh_owner_bridge_v1_observe_enabled set default false;

-- public.settings_defaults.banking_pay_execution_refresh_owner_bridge_v1_publish_enabled
alter table public.settings_defaults alter column banking_pay_execution_refresh_owner_bridge_v1_publish_enabled set default false;

-- public.settings_defaults.banking_pay_official_pay_weekday
alter table public.settings_defaults alter column banking_pay_official_pay_weekday set default 5;

-- public.settings_defaults.banking_pay_pre_bank_cancel_set_page_v1_enabled
alter table public.settings_defaults alter column banking_pay_pre_bank_cancel_set_page_v1_enabled set default false;

-- public.settings_defaults.banking_pay_same_authority_build_election_v1_enabled
alter table public.settings_defaults alter column banking_pay_same_authority_build_election_v1_enabled set default false;

-- public.settings_defaults.banking_pay_scheduled_cancellation_reversion_v2_observe_enabled
alter table public.settings_defaults alter column banking_pay_scheduled_cancellation_reversion_v2_observe_enabled set default false;

-- public.settings_defaults.banking_pay_scheduled_cancellation_reversion_v2_publish_enabled
alter table public.settings_defaults alter column banking_pay_scheduled_cancellation_reversion_v2_publish_enabled set default false;

-- public.settings_defaults.banking_pay_selection_intent_identity_v1_enabled
alter table public.settings_defaults alter column banking_pay_selection_intent_identity_v1_enabled set default false;

-- public.settings_defaults.banking_pay_source_publication_identity_enforce_v1_enabled
alter table public.settings_defaults alter column banking_pay_source_publication_identity_enforce_v1_enabled set default false;

-- public.settings_defaults.banking_pay_source_publication_identity_write_v1_enabled
alter table public.settings_defaults alter column banking_pay_source_publication_identity_write_v1_enabled set default false;

-- public.settings_defaults.banking_pay_workbench_auto_continuation_claim_limit_max
alter table public.settings_defaults alter column banking_pay_workbench_auto_continuation_claim_limit_max set default 10;

-- public.settings_defaults.banking_pay_workbench_auto_continuation_max_bursts
alter table public.settings_defaults alter column banking_pay_workbench_auto_continuation_max_bursts set default 3;

-- public.settings_defaults.banking_pay_workbench_auto_continuation_max_jobs
alter table public.settings_defaults alter column banking_pay_workbench_auto_continuation_max_jobs set default 50;

-- public.settings_defaults.banking_pay_workbench_auto_continuation_max_passes
alter table public.settings_defaults alter column banking_pay_workbench_auto_continuation_max_passes set default 1;

-- public.settings_defaults.banking_pay_workbench_auto_continuation_max_rows
alter table public.settings_defaults alter column banking_pay_workbench_auto_continuation_max_rows set default 500;

-- public.settings_defaults.banking_pay_workbench_auto_continuation_max_runtime_ms
alter table public.settings_defaults alter column banking_pay_workbench_auto_continuation_max_runtime_ms set default 28000;

-- public.settings_defaults.banking_pay_workbench_auto_continuation_min_runtime_ms
alter table public.settings_defaults alter column banking_pay_workbench_auto_continuation_min_runtime_ms set default 10000;

-- public.settings_defaults.banking_pay_workbench_auto_continuation_per_burst_max_runtime_m
alter table public.settings_defaults alter column banking_pay_workbench_auto_continuation_per_burst_max_runtime_m set default 10000;

-- public.settings_defaults.banking_pay_workbench_clone_bounded_reuse_v2_enabled
alter table public.settings_defaults alter column banking_pay_workbench_clone_bounded_reuse_v2_enabled set default false;

-- public.settings_defaults.banking_pay_workbench_clone_rebase_budget_ms
alter table public.settings_defaults alter column banking_pay_workbench_clone_rebase_budget_ms set default 3000;

-- public.settings_defaults.banking_pay_workbench_clone_rebase_enabled
alter table public.settings_defaults alter column banking_pay_workbench_clone_rebase_enabled set default false;

-- public.settings_defaults.banking_pay_workbench_clone_rebase_units_per_job
alter table public.settings_defaults alter column banking_pay_workbench_clone_rebase_units_per_job set default 100;

-- public.settings_defaults.banking_pay_workbench_clone_source_empty_reuse_enabled
alter table public.settings_defaults alter column banking_pay_workbench_clone_source_empty_reuse_enabled set default false;

-- public.settings_defaults.banking_pay_workbench_cron_claim_limit
alter table public.settings_defaults alter column banking_pay_workbench_cron_claim_limit set default 50;

-- public.settings_defaults.banking_pay_workbench_cron_clone_rebase_units_per_job
alter table public.settings_defaults alter column banking_pay_workbench_cron_clone_rebase_units_per_job set default 250;

-- public.settings_defaults.banking_pay_workbench_cron_delta_units_per_job
alter table public.settings_defaults alter column banking_pay_workbench_cron_delta_units_per_job set default 50;

-- public.settings_defaults.banking_pay_workbench_cron_enabled
alter table public.settings_defaults alter column banking_pay_workbench_cron_enabled set default true;

-- public.settings_defaults.banking_pay_workbench_cron_line_process_units_per_job
alter table public.settings_defaults alter column banking_pay_workbench_cron_line_process_units_per_job set default 50;

-- public.settings_defaults.banking_pay_workbench_cron_line_seed_units_per_job
alter table public.settings_defaults alter column banking_pay_workbench_cron_line_seed_units_per_job set default 50;

-- public.settings_defaults.banking_pay_workbench_cron_max_jobs
alter table public.settings_defaults alter column banking_pay_workbench_cron_max_jobs set default 110;

-- public.settings_defaults.banking_pay_workbench_cron_max_passes
alter table public.settings_defaults alter column banking_pay_workbench_cron_max_passes set default 2;

-- public.settings_defaults.banking_pay_workbench_cron_max_rows
alter table public.settings_defaults alter column banking_pay_workbench_cron_max_rows set default 750;

-- public.settings_defaults.banking_pay_workbench_cron_max_runtime_ms
alter table public.settings_defaults alter column banking_pay_workbench_cron_max_runtime_ms set default 15000;

-- public.settings_defaults.banking_pay_workbench_cron_preview_mat_units_per_job
alter table public.settings_defaults alter column banking_pay_workbench_cron_preview_mat_units_per_job set default 50;

-- public.settings_defaults.banking_pay_workbench_cron_scope_seed_units_per_job
alter table public.settings_defaults alter column banking_pay_workbench_cron_scope_seed_units_per_job set default 50;

-- public.settings_defaults.banking_pay_workbench_cron_source_build_lane_claim_limit
alter table public.settings_defaults alter column banking_pay_workbench_cron_source_build_lane_claim_limit set default 1;

-- public.settings_defaults.banking_pay_workbench_cron_source_build_parallel_bursts
alter table public.settings_defaults alter column banking_pay_workbench_cron_source_build_parallel_bursts set default 4;

-- public.settings_defaults.banking_pay_workbench_cron_source_build_parallelism
alter table public.settings_defaults alter column banking_pay_workbench_cron_source_build_parallelism set default 2;

-- public.settings_defaults.banking_pay_workbench_cron_source_build_runtime_floor_ms
alter table public.settings_defaults alter column banking_pay_workbench_cron_source_build_runtime_floor_ms set default 8000;

-- public.settings_defaults.banking_pay_workbench_cron_source_build_units_per_job
alter table public.settings_defaults alter column banking_pay_workbench_cron_source_build_units_per_job set default 25;

-- public.settings_defaults.banking_pay_workbench_db_idle_tx_timeout_ms
alter table public.settings_defaults alter column banking_pay_workbench_db_idle_tx_timeout_ms set default 30000;

-- public.settings_defaults.banking_pay_workbench_db_lock_timeout_ms
alter table public.settings_defaults alter column banking_pay_workbench_db_lock_timeout_ms set default 1500;

-- public.settings_defaults.banking_pay_workbench_db_statement_timeout_ms
alter table public.settings_defaults alter column banking_pay_workbench_db_statement_timeout_ms set default 15000;

-- public.settings_defaults.banking_pay_workbench_db_worker_lease_seconds
alter table public.settings_defaults alter column banking_pay_workbench_db_worker_lease_seconds set default 25;

-- public.settings_defaults.banking_pay_workbench_db_worker_max_runtime_ms
alter table public.settings_defaults alter column banking_pay_workbench_db_worker_max_runtime_ms set default 8000;

-- public.settings_defaults.banking_pay_workbench_db_worker_min_phase_budget_ms
alter table public.settings_defaults alter column banking_pay_workbench_db_worker_min_phase_budget_ms set default 2500;

-- public.settings_defaults.banking_pay_workbench_delta_budget_ms
alter table public.settings_defaults alter column banking_pay_workbench_delta_budget_ms set default 3000;

-- public.settings_defaults.banking_pay_workbench_delta_enable_exact_import_family
alter table public.settings_defaults alter column banking_pay_workbench_delta_enable_exact_import_family set default false;

-- public.settings_defaults.banking_pay_workbench_delta_enable_normal_timesheet
alter table public.settings_defaults alter column banking_pay_workbench_delta_enable_normal_timesheet set default false;

-- public.settings_defaults.banking_pay_workbench_delta_enable_readiness_only
alter table public.settings_defaults alter column banking_pay_workbench_delta_enable_readiness_only set default false;

-- public.settings_defaults.banking_pay_workbench_delta_enable_reservation_only
alter table public.settings_defaults alter column banking_pay_workbench_delta_enable_reservation_only set default false;

-- public.settings_defaults.banking_pay_workbench_delta_enable_simple_authorise
alter table public.settings_defaults alter column banking_pay_workbench_delta_enable_simple_authorise set default false;

-- public.settings_defaults.banking_pay_workbench_delta_enable_simple_unauthorise
alter table public.settings_defaults alter column banking_pay_workbench_delta_enable_simple_unauthorise set default false;

-- public.settings_defaults.banking_pay_workbench_delta_fallback_on_mismatch
alter table public.settings_defaults alter column banking_pay_workbench_delta_fallback_on_mismatch set default true;

-- public.settings_defaults.banking_pay_workbench_delta_refresh_enabled
alter table public.settings_defaults alter column banking_pay_workbench_delta_refresh_enabled set default false;

-- public.settings_defaults.banking_pay_workbench_delta_shadow_mode
alter table public.settings_defaults alter column banking_pay_workbench_delta_shadow_mode set default true;

-- public.settings_defaults.banking_pay_workbench_delta_units_per_job
alter table public.settings_defaults alter column banking_pay_workbench_delta_units_per_job set default 25;

-- public.settings_defaults.banking_pay_workbench_job_retry_base_seconds
alter table public.settings_defaults alter column banking_pay_workbench_job_retry_base_seconds set default 30;

-- public.settings_defaults.banking_pay_workbench_job_retry_max_seconds
alter table public.settings_defaults alter column banking_pay_workbench_job_retry_max_seconds set default 900;

-- public.settings_defaults.banking_pay_workbench_line_process_units_per_job
alter table public.settings_defaults alter column banking_pay_workbench_line_process_units_per_job set default 25;

-- public.settings_defaults.banking_pay_workbench_line_seed_units_per_job
alter table public.settings_defaults alter column banking_pay_workbench_line_seed_units_per_job set default 50;

-- public.settings_defaults.banking_pay_workbench_minimum_rpc_budget_ms
alter table public.settings_defaults alter column banking_pay_workbench_minimum_rpc_budget_ms set default 10000;

-- public.settings_defaults.banking_pay_workbench_nudge_claim_limit
alter table public.settings_defaults alter column banking_pay_workbench_nudge_claim_limit set default 50;

-- public.settings_defaults.banking_pay_workbench_nudge_clone_rebase_units_per_job
alter table public.settings_defaults alter column banking_pay_workbench_nudge_clone_rebase_units_per_job set default 100;

-- public.settings_defaults.banking_pay_workbench_nudge_delta_units_per_job
alter table public.settings_defaults alter column banking_pay_workbench_nudge_delta_units_per_job set default 25;

-- public.settings_defaults.banking_pay_workbench_nudge_enabled
alter table public.settings_defaults alter column banking_pay_workbench_nudge_enabled set default true;

-- public.settings_defaults.banking_pay_workbench_nudge_line_process_units_per_job
alter table public.settings_defaults alter column banking_pay_workbench_nudge_line_process_units_per_job set default 25;

-- public.settings_defaults.banking_pay_workbench_nudge_line_seed_units_per_job
alter table public.settings_defaults alter column banking_pay_workbench_nudge_line_seed_units_per_job set default 25;

-- public.settings_defaults.banking_pay_workbench_nudge_max_jobs
alter table public.settings_defaults alter column banking_pay_workbench_nudge_max_jobs set default 110;

-- public.settings_defaults.banking_pay_workbench_nudge_max_passes
alter table public.settings_defaults alter column banking_pay_workbench_nudge_max_passes set default 2;

-- public.settings_defaults.banking_pay_workbench_nudge_max_rows
alter table public.settings_defaults alter column banking_pay_workbench_nudge_max_rows set default 750;

-- public.settings_defaults.banking_pay_workbench_nudge_max_runtime_ms
alter table public.settings_defaults alter column banking_pay_workbench_nudge_max_runtime_ms set default 15000;

-- public.settings_defaults.banking_pay_workbench_nudge_preview_mat_units_per_job
alter table public.settings_defaults alter column banking_pay_workbench_nudge_preview_mat_units_per_job set default 25;

-- public.settings_defaults.banking_pay_workbench_nudge_scope_seed_units_per_job
alter table public.settings_defaults alter column banking_pay_workbench_nudge_scope_seed_units_per_job set default 25;

-- public.settings_defaults.banking_pay_workbench_nudge_source_build_lane_claim_limit
alter table public.settings_defaults alter column banking_pay_workbench_nudge_source_build_lane_claim_limit set default 1;

-- public.settings_defaults.banking_pay_workbench_nudge_source_build_parallel_bursts
alter table public.settings_defaults alter column banking_pay_workbench_nudge_source_build_parallel_bursts set default 12;

-- public.settings_defaults.banking_pay_workbench_nudge_source_build_parallelism
alter table public.settings_defaults alter column banking_pay_workbench_nudge_source_build_parallelism set default 4;

-- public.settings_defaults.banking_pay_workbench_nudge_source_build_runtime_floor_ms
alter table public.settings_defaults alter column banking_pay_workbench_nudge_source_build_runtime_floor_ms set default 8000;

-- public.settings_defaults.banking_pay_workbench_nudge_source_build_units_per_job
alter table public.settings_defaults alter column banking_pay_workbench_nudge_source_build_units_per_job set default 25;

-- public.settings_defaults.banking_pay_workbench_patch_after_batch_mutation_enabled
alter table public.settings_defaults alter column banking_pay_workbench_patch_after_batch_mutation_enabled set default true;

-- public.settings_defaults.banking_pay_workbench_preview_mat_units_per_job
alter table public.settings_defaults alter column banking_pay_workbench_preview_mat_units_per_job set default 25;

-- public.settings_defaults.banking_pay_workbench_reconciliation_envelope_evidence_json
alter table public.settings_defaults alter column banking_pay_workbench_reconciliation_envelope_evidence_json set default '{}'::jsonb;

-- public.settings_defaults.banking_pay_workbench_reconciliation_envelope_json
alter table public.settings_defaults alter column banking_pay_workbench_reconciliation_envelope_json set default '{}'::jsonb;

-- public.settings_defaults.banking_pay_workbench_reconciliation_envelope_version
alter table public.settings_defaults alter column banking_pay_workbench_reconciliation_envelope_version set default 1;

-- public.settings_defaults.banking_pay_workbench_reconciliation_optimization_version
alter table public.settings_defaults alter column banking_pay_workbench_reconciliation_optimization_version set default 0;

-- public.settings_defaults.banking_pay_workbench_rollover_enabled
alter table public.settings_defaults alter column banking_pay_workbench_rollover_enabled set default true;

-- public.settings_defaults.banking_pay_workbench_rollover_max_sessions_per_tick
alter table public.settings_defaults alter column banking_pay_workbench_rollover_max_sessions_per_tick set default 3;

-- public.settings_defaults.banking_pay_workbench_rollover_nudge_after_create
alter table public.settings_defaults alter column banking_pay_workbench_rollover_nudge_after_create set default true;

-- public.settings_defaults.banking_pay_workbench_rpc_safety_buffer_ms
alter table public.settings_defaults alter column banking_pay_workbench_rpc_safety_buffer_ms set default 1000;

-- public.settings_defaults.banking_pay_workbench_scope_reconcile_enabled
alter table public.settings_defaults alter column banking_pay_workbench_scope_reconcile_enabled set default true;

-- public.settings_defaults.banking_pay_workbench_scope_reconcile_shadow_mode
alter table public.settings_defaults alter column banking_pay_workbench_scope_reconcile_shadow_mode set default false;

-- public.settings_defaults.banking_pay_workbench_scope_seed_units_per_job
alter table public.settings_defaults alter column banking_pay_workbench_scope_seed_units_per_job set default 25;

-- public.settings_defaults.banking_pay_workbench_semantic_ready_draft_guard_v2_enabled
alter table public.settings_defaults alter column banking_pay_workbench_semantic_ready_draft_guard_v2_enabled set default false;

-- public.settings_defaults.banking_pay_workbench_semantic_ready_observe_v2_enabled
alter table public.settings_defaults alter column banking_pay_workbench_semantic_ready_observe_v2_enabled set default false;

-- public.settings_defaults.banking_pay_workbench_semantic_ready_publication_v3_enabled
alter table public.settings_defaults alter column banking_pay_workbench_semantic_ready_publication_v3_enabled set default false;

-- public.settings_defaults.banking_pay_workbench_source_build_execution_profile_version
alter table public.settings_defaults alter column banking_pay_workbench_source_build_execution_profile_version set default 1;

-- public.settings_defaults.banking_pay_workbench_source_build_units_per_job
alter table public.settings_defaults alter column banking_pay_workbench_source_build_units_per_job set default 25;

-- public.settings_defaults.banking_pay_workbench_stage_work_units_per_job
alter table public.settings_defaults alter column banking_pay_workbench_stage_work_units_per_job set default 25;

-- public.settings_defaults.banking_system
alter table public.settings_defaults alter column banking_system set default 'MONZO_CSV'::text;

-- public.settings_defaults.bh_end
alter table public.settings_defaults alter column bh_end set default '00:00:00'::time without time zone;

-- public.settings_defaults.bh_list
alter table public.settings_defaults alter column bh_list set default '[]'::jsonb;

-- public.settings_defaults.bh_source
alter table public.settings_defaults alter column bh_source set default 'MANUAL'::text;

-- public.settings_defaults.bh_start
alter table public.settings_defaults alter column bh_start set default '00:00:00'::time without time zone;

-- public.settings_defaults.candidate_app_environment
alter table public.settings_defaults alter column candidate_app_environment set default 'TEST'::text;

-- public.settings_defaults.candidate_app_feature_flags_json
alter table public.settings_defaults alter column candidate_app_feature_flags_json set default '{"candidate_paper_qr": false, "candidate_settings": false, "candidate_app_reads": false, "candidate_app_writes": false, "candidate_notifications": false, "candidate_manager_approval": false, "candidate_daily_finalisation": false, "candidate_route_confirmation": false, "candidate_account_registration": false, "candidate_expense_atomic_placement": false, "candidate_record_role_capabilities": false, "candidate_expense_invoice_routing_v1": false}'::jsonb;

-- public.settings_defaults.candidate_barred_manager_email_domains
alter table public.settings_defaults alter column candidate_barred_manager_email_domains set default '[]'::jsonb;

-- public.settings_defaults.candidate_electronic_auto_authorise_default
alter table public.settings_defaults alter column candidate_electronic_auto_authorise_default set default false;

-- public.settings_defaults.candidate_hours_deviation_pct
alter table public.settings_defaults alter column candidate_hours_deviation_pct set default 30;

-- public.settings_defaults.comms_adaptors_json
alter table public.settings_defaults alter column comms_adaptors_json set default '{}'::jsonb;

-- public.settings_defaults.created_at
alter table public.settings_defaults alter column created_at set default now();

-- public.settings_defaults.day_end
alter table public.settings_defaults alter column day_end set default '20:00:00'::time without time zone;

-- public.settings_defaults.day_start
alter table public.settings_defaults alter column day_start set default '06:00:00'::time without time zone;

-- public.settings_defaults.default_schedule_paye_local
alter table public.settings_defaults alter column default_schedule_paye_local set default 'Fri 02:00'::text;

-- public.settings_defaults.default_schedule_umbrella_local
alter table public.settings_defaults alter column default_schedule_umbrella_local set default 'Thu 02:00'::text;

-- public.settings_defaults.external_paye_system
alter table public.settings_defaults alter column external_paye_system set default 'SAGE'::text;

-- public.settings_defaults.finance_email_settings
alter table public.settings_defaults alter column finance_email_settings set default '{}'::jsonb;

-- public.settings_defaults.funds_warning_hours_json
alter table public.settings_defaults alter column funds_warning_hours_json set default '[24, 12]'::jsonb;

-- public.settings_defaults.healthroster_import_auto_authorise_default
alter table public.settings_defaults alter column healthroster_import_auto_authorise_default set default true;

-- public.settings_defaults.hr_attach_to_invoice
alter table public.settings_defaults alter column hr_attach_to_invoice set default true;

-- public.settings_defaults.id
alter table public.settings_defaults alter column id set default 1;

-- public.settings_defaults.import_config_json
alter table public.settings_defaults alter column import_config_json set default '{}'::jsonb;

-- public.settings_defaults.invoice_debug
alter table public.settings_defaults alter column invoice_debug set default false;

-- public.settings_defaults.invoice_document_presentation_json
alter table public.settings_defaults alter column invoice_document_presentation_json set default '{}'::jsonb;

-- public.settings_defaults.max_attachments_per_email
alter table public.settings_defaults alter column max_attachments_per_email set default 30;

-- public.settings_defaults.nhsp_import_auto_authorise_default
alter table public.settings_defaults alter column nhsp_import_auto_authorise_default set default false;

-- public.settings_defaults.night_end
alter table public.settings_defaults alter column night_end set default '06:00:00'::time without time zone;

-- public.settings_defaults.night_start
alter table public.settings_defaults alter column night_start set default '20:00:00'::time without time zone;

-- public.settings_defaults.pay_eligibility_months_back
alter table public.settings_defaults alter column pay_eligibility_months_back set default 6;

-- public.settings_defaults.pay_eligibility_weeks_ahead
alter table public.settings_defaults alter column pay_eligibility_weeks_ahead set default 2;

-- public.settings_defaults.pay_export_csv_columns_json
alter table public.settings_defaults alter column pay_export_csv_columns_json set default '["payment_reference", "payee_name", "sort_code", "account_number", "account_type", "amount"]'::jsonb;

-- public.settings_defaults.pay_export_csv_format_json
alter table public.settings_defaults alter column pay_export_csv_format_json set default '{}'::jsonb;

-- public.settings_defaults.paye_remittances_enabled
alter table public.settings_defaults alter column paye_remittances_enabled set default false;

-- public.settings_defaults.payment_authoriser_quantity
alter table public.settings_defaults alter column payment_authoriser_quantity set default 1;

-- public.settings_defaults.payment_remittance_send_timing
alter table public.settings_defaults alter column payment_remittance_send_timing set default 'ON_EXECUTION'::text;

-- public.settings_defaults.payment_return_admin_notice_max_wait_minutes
alter table public.settings_defaults alter column payment_return_admin_notice_max_wait_minutes set default 60;

-- public.settings_defaults.payment_return_admin_notice_quiet_minutes
alter table public.settings_defaults alter column payment_return_admin_notice_quiet_minutes set default 10;

-- public.settings_defaults.payment_return_admin_recipient_role
alter table public.settings_defaults alter column payment_return_admin_recipient_role set default 'ADMIN'::text;

-- public.settings_defaults.payment_return_auto_reverse_timesheets
alter table public.settings_defaults alter column payment_return_auto_reverse_timesheets set default false;

-- public.settings_defaults.payroll_testing
alter table public.settings_defaults alter column payroll_testing set default false;

-- public.settings_defaults.rail_env_default
alter table public.settings_defaults alter column rail_env_default set default 'PROD'::text;

-- public.settings_defaults.rail_provider_default
alter table public.settings_defaults alter column rail_provider_default set default 'CSV'::text;

-- public.settings_defaults.rail_supports_auto_execute
alter table public.settings_defaults alter column rail_supports_auto_execute set default false;

-- public.settings_defaults.rail_supports_name_check
alter table public.settings_defaults alter column rail_supports_name_check set default false;

-- public.settings_defaults.rail_supports_scheduling
alter table public.settings_defaults alter column rail_supports_scheduling set default false;

-- public.settings_defaults.remittance_receive_when_umbrella_paid
alter table public.settings_defaults alter column remittance_receive_when_umbrella_paid set default false;

-- public.settings_defaults.remittances_detailed_breakdown
alter table public.settings_defaults alter column remittances_detailed_breakdown set default false;

-- public.settings_defaults.reversal_complete_financials_date
alter table public.settings_defaults alter column reversal_complete_financials_date set default 'PAID_DATE'::correction_financials_date_basis_enum;

-- public.settings_defaults.reversal_replacement_financials_date
alter table public.settings_defaults alter column reversal_replacement_financials_date set default 'PAID_DATE'::correction_financials_date_basis_enum;

-- public.settings_defaults.system_emails
alter table public.settings_defaults alter column system_emails set default '{}'::jsonb;

-- public.settings_defaults.temp_log
alter table public.settings_defaults alter column temp_log set default false;

-- public.settings_defaults.timezone_id
alter table public.settings_defaults alter column timezone_id set default 'Europe/London'::text;

-- public.settings_defaults.ts_attach_to_invoice
alter table public.settings_defaults alter column ts_attach_to_invoice set default true;

-- public.settings_defaults.ts_reference_required
alter table public.settings_defaults alter column ts_reference_required set default false;

-- public.settings_defaults.updated_at
alter table public.settings_defaults alter column updated_at set default now();

-- public.settings_finance_windows.created_at
alter table public.settings_finance_windows alter column created_at set default now();

-- public.settings_finance_windows.id
alter table public.settings_finance_windows alter column id set default gen_random_uuid();

-- public.settings_finance_windows.updated_at
alter table public.settings_finance_windows alter column updated_at set default now();

-- public.sheets_outbox.attempt_count
alter table public.sheets_outbox alter column attempt_count set default 0;

-- public.sheets_outbox.created_at
alter table public.sheets_outbox alter column created_at set default now();

-- public.sheets_outbox.id
alter table public.sheets_outbox alter column id set default gen_random_uuid();

-- public.sheets_outbox.status
alter table public.sheets_outbox alter column status set default 'PENDING'::outbox_status_enum;

-- public.sheets_outbox.updated_at
alter table public.sheets_outbox alter column updated_at set default now();

-- public.timesheet_archive_transition_capability.created_at_utc
alter table public.timesheet_archive_transition_capability alter column created_at_utc set default clock_timestamp();

-- public.timesheet_evidence.created_at
alter table public.timesheet_evidence alter column created_at set default now();

-- public.timesheet_evidence.document_role
alter table public.timesheet_evidence alter column document_role set default 'SOURCE_EVIDENCE'::text;

-- public.timesheet_evidence.id
alter table public.timesheet_evidence alter column id set default gen_random_uuid();

-- public.timesheet_evidence.processing_state
alter table public.timesheet_evidence alter column processing_state set default 'DISCOVERED'::text;

-- public.timesheet_financial_retention.first_retained_at_utc
alter table public.timesheet_financial_retention alter column first_retained_at_utc set default clock_timestamp();

-- public.timesheet_lifecycle_bulk_operation_items.affected_rows_json
alter table public.timesheet_lifecycle_bulk_operation_items alter column affected_rows_json set default '[]'::jsonb;

-- public.timesheet_lifecycle_bulk_operation_items.attempt_count
alter table public.timesheet_lifecycle_bulk_operation_items alter column attempt_count set default 0;

-- public.timesheet_lifecycle_bulk_operation_items.created_at_utc
alter table public.timesheet_lifecycle_bulk_operation_items alter column created_at_utc set default now();

-- public.timesheet_lifecycle_bulk_operation_items.error_json
alter table public.timesheet_lifecycle_bulk_operation_items alter column error_json set default '{}'::jsonb;

-- public.timesheet_lifecycle_bulk_operation_items.id
alter table public.timesheet_lifecycle_bulk_operation_items alter column id set default gen_random_uuid();

-- public.timesheet_lifecycle_bulk_operation_items.result_json
alter table public.timesheet_lifecycle_bulk_operation_items alter column result_json set default '{}'::jsonb;

-- public.timesheet_lifecycle_bulk_operation_items.status
alter table public.timesheet_lifecycle_bulk_operation_items alter column status set default 'QUEUED'::text;

-- public.timesheet_lifecycle_bulk_operation_items.updated_at_utc
alter table public.timesheet_lifecycle_bulk_operation_items alter column updated_at_utc set default now();

-- public.timesheet_lifecycle_bulk_operations.context
alter table public.timesheet_lifecycle_bulk_operations alter column context set default 'bulk_authorise'::text;

-- public.timesheet_lifecycle_bulk_operations.created_at_utc
alter table public.timesheet_lifecycle_bulk_operations alter column created_at_utc set default now();

-- public.timesheet_lifecycle_bulk_operations.error_json
alter table public.timesheet_lifecycle_bulk_operations alter column error_json set default '{}'::jsonb;

-- public.timesheet_lifecycle_bulk_operations.failure_count
alter table public.timesheet_lifecycle_bulk_operations alter column failure_count set default 0;

-- public.timesheet_lifecycle_bulk_operations.id
alter table public.timesheet_lifecycle_bulk_operations alter column id set default gen_random_uuid();

-- public.timesheet_lifecycle_bulk_operations.progress_json
alter table public.timesheet_lifecycle_bulk_operations alter column progress_json set default '{}'::jsonb;

-- public.timesheet_lifecycle_bulk_operations.request_json
alter table public.timesheet_lifecycle_bulk_operations alter column request_json set default '{}'::jsonb;

-- public.timesheet_lifecycle_bulk_operations.requested_count
alter table public.timesheet_lifecycle_bulk_operations alter column requested_count set default 0;

-- public.timesheet_lifecycle_bulk_operations.result_json
alter table public.timesheet_lifecycle_bulk_operations alter column result_json set default '{}'::jsonb;

-- public.timesheet_lifecycle_bulk_operations.run_after_utc
alter table public.timesheet_lifecycle_bulk_operations alter column run_after_utc set default now();

-- public.timesheet_lifecycle_bulk_operations.status
alter table public.timesheet_lifecycle_bulk_operations alter column status set default 'QUEUED'::text;

-- public.timesheet_lifecycle_bulk_operations.success_count
alter table public.timesheet_lifecycle_bulk_operations alter column success_count set default 0;

-- public.timesheet_lifecycle_bulk_operations.updated_at_utc
alter table public.timesheet_lifecycle_bulk_operations alter column updated_at_utc set default now();

-- public.timesheet_pay_state_history.id
alter table public.timesheet_pay_state_history alter column id set default gen_random_uuid();

-- public.timesheet_pay_state_history.settled_at_utc
alter table public.timesheet_pay_state_history alter column settled_at_utc set default now();

-- public.timesheet_payment_overrides.created_at_utc
alter table public.timesheet_payment_overrides alter column created_at_utc set default now();

-- public.timesheet_payment_overrides.id
alter table public.timesheet_payment_overrides alter column id set default gen_random_uuid();

-- public.timesheet_payment_overrides.override_type
alter table public.timesheet_payment_overrides alter column override_type set default 'ADVANCE_THIS_PAYMENT'::text;

-- public.timesheet_r2_cleanup_queue.attempt_count
alter table public.timesheet_r2_cleanup_queue alter column attempt_count set default 1;

-- public.timesheet_r2_cleanup_queue.deleted_timesheet_ids
alter table public.timesheet_r2_cleanup_queue alter column deleted_timesheet_ids set default ARRAY[]::uuid[];

-- public.timesheet_r2_cleanup_queue.first_failed_at_utc
alter table public.timesheet_r2_cleanup_queue alter column first_failed_at_utc set default clock_timestamp();

-- public.timesheet_r2_cleanup_queue.last_attempt_at_utc
alter table public.timesheet_r2_cleanup_queue alter column last_attempt_at_utc set default clock_timestamp();

-- public.timesheet_r2_cleanup_queue.next_attempt_at_utc
alter table public.timesheet_r2_cleanup_queue alter column next_attempt_at_utc set default clock_timestamp();

-- public.timesheet_r2_cleanup_queue.status
alter table public.timesheet_r2_cleanup_queue alter column status set default 'PENDING'::text;

-- public.timesheet_summary_pay_state_cache.active_advance
alter table public.timesheet_summary_pay_state_cache alter column active_advance set default false;

-- public.timesheet_summary_pay_state_cache.active_processing
alter table public.timesheet_summary_pay_state_cache alter column active_processing set default false;

-- public.timesheet_summary_pay_state_cache.net_delta_ex_vat
alter table public.timesheet_summary_pay_state_cache alter column net_delta_ex_vat set default 0;

-- public.timesheet_summary_pay_state_cache.outstanding_ex_vat
alter table public.timesheet_summary_pay_state_cache alter column outstanding_ex_vat set default 0;

-- public.timesheet_summary_pay_state_cache.paid_to_date_ex_vat
alter table public.timesheet_summary_pay_state_cache alter column paid_to_date_ex_vat set default 0;

-- public.timesheet_summary_pay_state_cache.refreshed_at_utc
alter table public.timesheet_summary_pay_state_cache alter column refreshed_at_utc set default now();

-- public.timesheet_summary_pay_state_cache.reserved_ex_vat
alter table public.timesheet_summary_pay_state_cache alter column reserved_ex_vat set default 0;

-- public.timesheet_summary_pay_state_cache.summary_badge_codes
alter table public.timesheet_summary_pay_state_cache alter column summary_badge_codes set default ARRAY[]::text[];

-- public.timesheet_summary_pay_state_cache.summary_pay_icon_code
alter table public.timesheet_summary_pay_state_cache alter column summary_pay_icon_code set default 'NONE'::text;

-- public.timesheet_summary_pay_state_cache.summary_pay_status_code
alter table public.timesheet_summary_pay_state_cache alter column summary_pay_status_code set default 'UNPAID'::text;

-- public.timesheet_summary_pay_state_cache.summary_state_applies
alter table public.timesheet_summary_pay_state_cache alter column summary_state_applies set default false;

-- public.timesheet_validations.created_at
alter table public.timesheet_validations alter column created_at set default now();

-- public.timesheet_validations.id
alter table public.timesheet_validations alter column id set default gen_random_uuid();

-- public.timesheet_validations.pre_validated
alter table public.timesheet_validations alter column pre_validated set default false;

-- public.timesheet_validations.status
alter table public.timesheet_validations alter column status set default 'PENDING'::validation_status_enum;

-- public.timesheet_validations.updated_at
alter table public.timesheet_validations alter column updated_at set default now();

-- public.timesheets.additional_units_per_day
alter table public.timesheets alter column additional_units_per_day set default '{}'::jsonb;

-- public.timesheets.additional_units_week
alter table public.timesheets alter column additional_units_week set default '{}'::jsonb;

-- public.timesheets.created_at
alter table public.timesheets alter column created_at set default now();

-- public.timesheets.document_revision
alter table public.timesheets alter column document_revision set default 1;

-- public.timesheets.document_state
alter table public.timesheets alter column document_state set default 'NOT_REQUESTED'::text;

-- public.timesheets.is_adjustment
alter table public.timesheets alter column is_adjustment set default false;

-- public.timesheets.is_current
alter table public.timesheets alter column is_current set default true;

-- public.timesheets.line_type
alter table public.timesheets alter column line_type set default 'HOURS'::timesheet_line_type_enum;

-- public.timesheets.manual_pdf_rotation_degrees
alter table public.timesheets alter column manual_pdf_rotation_degrees set default 0;

-- public.timesheets.qr_payload_json
alter table public.timesheets alter column qr_payload_json set default '{}'::jsonb;

-- public.timesheets.sheet_scope
alter table public.timesheets alter column sheet_scope set default 'DAILY'::timesheet_scope_enum;

-- public.timesheets.status
alter table public.timesheets alter column status set default 'RECEIVED'::timesheet_status_enum;

-- public.timesheets.timesheet_id
alter table public.timesheets alter column timesheet_id set default gen_random_uuid();

-- public.timesheets.updated_at
alter table public.timesheets alter column updated_at set default now();

-- public.timesheets.version
alter table public.timesheets alter column version set default 1;

-- public.timesheets_financials.accommodation_charge_ex_vat
alter table public.timesheets_financials alter column accommodation_charge_ex_vat set default 0;

-- public.timesheets_financials.accommodation_pay_ex_vat
alter table public.timesheets_financials alter column accommodation_pay_ex_vat set default 0;

-- public.timesheets_financials.additional_charge_ex_vat
alter table public.timesheets_financials alter column additional_charge_ex_vat set default 0;

-- public.timesheets_financials.additional_margin_ex_vat
alter table public.timesheets_financials alter column additional_margin_ex_vat set default 0;

-- public.timesheets_financials.additional_pay_ex_vat
alter table public.timesheets_financials alter column additional_pay_ex_vat set default 0;

-- public.timesheets_financials.additional_units_json
alter table public.timesheets_financials alter column additional_units_json set default '{}'::jsonb;

-- public.timesheets_financials.basis
alter table public.timesheets_financials alter column basis set default 'SELF_REPORTED'::timesheet_fin_basis_enum;

-- public.timesheets_financials.candidate_assignment
alter table public.timesheets_financials alter column candidate_assignment set default 'UNASSIGNED'::candidate_assignment_enum;

-- public.timesheets_financials.computed_at_utc
alter table public.timesheets_financials alter column computed_at_utc set default now();

-- public.timesheets_financials.created_at
alter table public.timesheets_financials alter column created_at set default now();

-- public.timesheets_financials.expenses_charge_ex_vat
alter table public.timesheets_financials alter column expenses_charge_ex_vat set default 0;

-- public.timesheets_financials.expenses_pay_ex_vat
alter table public.timesheets_financials alter column expenses_pay_ex_vat set default 0;

-- public.timesheets_financials.has_pay_channel_issue
alter table public.timesheets_financials alter column has_pay_channel_issue set default false;

-- public.timesheets_financials.has_rate_issue
alter table public.timesheets_financials alter column has_rate_issue set default false;

-- public.timesheets_financials.hours_bh
alter table public.timesheets_financials alter column hours_bh set default 0;

-- public.timesheets_financials.hours_day
alter table public.timesheets_financials alter column hours_day set default 0;

-- public.timesheets_financials.hours_night
alter table public.timesheets_financials alter column hours_night set default 0;

-- public.timesheets_financials.hours_sat
alter table public.timesheets_financials alter column hours_sat set default 0;

-- public.timesheets_financials.hours_sun
alter table public.timesheets_financials alter column hours_sun set default 0;

-- public.timesheets_financials.id
alter table public.timesheets_financials alter column id set default gen_random_uuid();

-- public.timesheets_financials.invoice_breakdown_json
alter table public.timesheets_financials alter column invoice_breakdown_json set default '{}'::jsonb;

-- public.timesheets_financials.is_current
alter table public.timesheets_financials alter column is_current set default true;

-- public.timesheets_financials.is_stale
alter table public.timesheets_financials alter column is_stale set default false;

-- public.timesheets_financials.margin_ex_vat
alter table public.timesheets_financials alter column margin_ex_vat set default 0;

-- public.timesheets_financials.mileage_charge_ex_vat
alter table public.timesheets_financials alter column mileage_charge_ex_vat set default 0;

-- public.timesheets_financials.mileage_pay_ex_vat
alter table public.timesheets_financials alter column mileage_pay_ex_vat set default 0;

-- public.timesheets_financials.mileage_units
alter table public.timesheets_financials alter column mileage_units set default 0;

-- public.timesheets_financials.other_charge_ex_vat
alter table public.timesheets_financials alter column other_charge_ex_vat set default 0;

-- public.timesheets_financials.other_pay_ex_vat
alter table public.timesheets_financials alter column other_pay_ex_vat set default 0;

-- public.timesheets_financials.pay_on_hold
alter table public.timesheets_financials alter column pay_on_hold set default false;

-- public.timesheets_financials.pay_total_inc_vat_snapshot
alter table public.timesheets_financials alter column pay_total_inc_vat_snapshot set default 0;

-- public.timesheets_financials.pay_vat_amount_snapshot
alter table public.timesheets_financials alter column pay_vat_amount_snapshot set default 0;

-- public.timesheets_financials.policy_snapshot_json
alter table public.timesheets_financials alter column policy_snapshot_json set default '{}'::jsonb;

-- public.timesheets_financials.processing_status
alter table public.timesheets_financials alter column processing_status set default 'UNASSIGNED'::ts_fin_processing_status_enum;

-- public.timesheets_financials.rate_source_refs_json
alter table public.timesheets_financials alter column rate_source_refs_json set default '{}'::jsonb;

-- public.timesheets_financials.remittance_send_count
alter table public.timesheets_financials alter column remittance_send_count set default 0;

-- public.timesheets_financials.total_charge_ex_vat
alter table public.timesheets_financials alter column total_charge_ex_vat set default 0;

-- public.timesheets_financials.total_hours
alter table public.timesheets_financials alter column total_hours set default 0;

-- public.timesheets_financials.total_pay_ex_vat
alter table public.timesheets_financials alter column total_pay_ex_vat set default 0;

-- public.timesheets_financials.travel_charge_ex_vat
alter table public.timesheets_financials alter column travel_charge_ex_vat set default 0;

-- public.timesheets_financials.travel_pay_ex_vat
alter table public.timesheets_financials alter column travel_pay_ex_vat set default 0;

-- public.timesheets_financials.updated_at
alter table public.timesheets_financials alter column updated_at set default now();

-- public.tms_login_2fa_challenges.attempt_count
alter table public.tms_login_2fa_challenges alter column attempt_count set default 0;

-- public.tms_login_2fa_challenges.created_at
alter table public.tms_login_2fa_challenges alter column created_at set default now();

-- public.tms_login_2fa_challenges.id
alter table public.tms_login_2fa_challenges alter column id set default gen_random_uuid();

-- public.tms_login_2fa_challenges.last_sent_at_utc
alter table public.tms_login_2fa_challenges alter column last_sent_at_utc set default now();

-- public.tms_login_2fa_challenges.purpose
alter table public.tms_login_2fa_challenges alter column purpose set default 'PAYMENT_SCHEDULE'::text;

-- public.tms_login_2fa_challenges.resend_count
alter table public.tms_login_2fa_challenges alter column resend_count set default 0;

-- public.tms_password_resets.created_at
alter table public.tms_password_resets alter column created_at set default now();

-- public.tms_password_resets.id
alter table public.tms_password_resets alter column id set default gen_random_uuid();

-- public.tms_user_2fa_trust.created_at
alter table public.tms_user_2fa_trust alter column created_at set default now();

-- public.tms_user_2fa_trust.id
alter table public.tms_user_2fa_trust alter column id set default gen_random_uuid();

-- public.tms_user_2fa_trust.updated_at
alter table public.tms_user_2fa_trust alter column updated_at set default now();

-- public.tms_user_2fa_trust.verified_at_utc
alter table public.tms_user_2fa_trust alter column verified_at_utc set default now();

-- public.tms_users.created_at
alter table public.tms_users alter column created_at set default now();

-- public.tms_users.email_settings
alter table public.tms_users alter column email_settings set default '{}'::jsonb;

-- public.tms_users.grid_prefs_json
alter table public.tms_users alter column grid_prefs_json set default '{}'::jsonb;

-- public.tms_users.id
alter table public.tms_users alter column id set default gen_random_uuid();

-- public.tms_users.is_active
alter table public.tms_users alter column is_active set default true;

-- public.tms_users.payment_authoriser
alter table public.tms_users alter column payment_authoriser set default false;

-- public.tms_users.payment_golden_key
alter table public.tms_users alter column payment_golden_key set default false;

-- public.tms_users.role
alter table public.tms_users alter column role set default 'user'::text;

-- public.tms_users.session_version
alter table public.tms_users alter column session_version set default 1;

-- public.tms_users.updated_at
alter table public.tms_users alter column updated_at set default now();

-- public.ts_financials_outbox.attempt_count
alter table public.ts_financials_outbox alter column attempt_count set default 0;

-- public.ts_financials_outbox.created_at
alter table public.ts_financials_outbox alter column created_at set default now();

-- public.ts_financials_outbox.id
alter table public.ts_financials_outbox alter column id set default gen_random_uuid();

-- public.ts_pay_adjustments.as_advance
alter table public.ts_pay_adjustments alter column as_advance set default false;

-- public.ts_pay_adjustments.created_at
alter table public.ts_pay_adjustments alter column created_at set default now();

-- public.ts_pay_adjustments.id
alter table public.ts_pay_adjustments alter column id set default gen_random_uuid();

-- public.ts_pay_adjustments.meta_json
alter table public.ts_pay_adjustments alter column meta_json set default '{}'::jsonb;

-- public.ts_pay_adjustments.updated_at
alter table public.ts_pay_adjustments alter column updated_at set default now();

-- public.ts_pdfs_outbox.attempt_count
alter table public.ts_pdfs_outbox alter column attempt_count set default 0;

-- public.ts_pdfs_outbox.created_at
alter table public.ts_pdfs_outbox alter column created_at set default now();

-- public.ts_pdfs_outbox.force_regen
alter table public.ts_pdfs_outbox alter column force_regen set default false;

-- public.ts_pdfs_outbox.id
alter table public.ts_pdfs_outbox alter column id set default gen_random_uuid();

-- public.ts_pdfs_outbox.prefer_generated
alter table public.ts_pdfs_outbox alter column prefer_generated set default false;

-- public.umbrellas.created_at
alter table public.umbrellas alter column created_at set default now();

-- public.umbrellas.enabled
alter table public.umbrellas alter column enabled set default true;

-- public.umbrellas.id
alter table public.umbrellas alter column id set default gen_random_uuid();

-- public.umbrellas.remittance_overrides_enabled
alter table public.umbrellas alter column remittance_overrides_enabled set default false;

-- public.umbrellas.remittances_detailed_breakdown
alter table public.umbrellas alter column remittances_detailed_breakdown set default false;

-- public.umbrellas.updated_at
alter table public.umbrellas alter column updated_at set default now();

-- public.umbrellas.vat_chargeable
alter table public.umbrellas alter column vat_chargeable set default false;

-- Constraints are dependency-ordered: referenced keys, exclusions and checks precede foreign keys.

-- private.banking_pay_workbench_candidate_scope_registry.bpay_wb_scope_registry_pkey
alter table private.banking_pay_workbench_candidate_scope_registry add constraint bpay_wb_scope_registry_pkey PRIMARY KEY (candidate_id);

-- private.banking_pay_workbench_canonical_stage_lines.bpay_wb_canonical_stage_lines_pkey
alter table private.banking_pay_workbench_canonical_stage_lines add constraint bpay_wb_canonical_stage_lines_pkey PRIMARY KEY (build_id, source_ordinal);

-- private.banking_pay_workbench_economic_build_fact_pages.bpay_wb_economic_build_fact_pages_pkey
alter table private.banking_pay_workbench_economic_build_fact_pages add constraint bpay_wb_economic_build_fact_pages_pkey PRIMARY KEY (id);

-- private.banking_pay_workbench_economic_build_facts.bpay_wb_economic_build_facts_pkey
alter table private.banking_pay_workbench_economic_build_facts add constraint bpay_wb_economic_build_facts_pkey PRIMARY KEY (build_id, fact_family, natural_key);

-- private.banking_pay_workbench_economic_build_scope.bpay_wb_economic_build_scope_pkey
alter table private.banking_pay_workbench_economic_build_scope add constraint bpay_wb_economic_build_scope_pkey PRIMARY KEY (build_id, timesheet_id);

-- private.banking_pay_workbench_economic_builds.bpay_wb_economic_builds_pkey
alter table private.banking_pay_workbench_economic_builds add constraint bpay_wb_economic_builds_pkey PRIMARY KEY (id);

-- private.banking_pay_workbench_queue_scan_state.bpay_wb_queue_scan_state_pkey
alter table private.banking_pay_workbench_queue_scan_state add constraint bpay_wb_queue_scan_state_pkey PRIMARY KEY (lane_identity, scan_kind, scan_scope_key);

-- private.banking_pay_workbench_stage_attempts.bpay_wb_stage_attempts_pkey
alter table private.banking_pay_workbench_stage_attempts add constraint bpay_wb_stage_attempts_pkey PRIMARY KEY (id);

-- private.banking_pay_workbench_timesheet_scope_state.bpay_wb_timesheet_scope_state_pkey
alter table private.banking_pay_workbench_timesheet_scope_state add constraint bpay_wb_timesheet_scope_state_pkey PRIMARY KEY (timesheet_id);

-- private.candidate_daily_authority_scopes.candidate_daily_authority_scopes_pkey
alter table private.candidate_daily_authority_scopes add constraint candidate_daily_authority_scopes_pkey PRIMARY KEY (environment, candidate_id);

-- private.candidate_daily_authority_transitions.candidate_daily_authority_transitions_pkey
alter table private.candidate_daily_authority_transitions add constraint candidate_daily_authority_transitions_pkey PRIMARY KEY (transition_id);

-- private.candidate_daily_batch_receipts.candidate_daily_batch_receipts_pkey
alter table private.candidate_daily_batch_receipts add constraint candidate_daily_batch_receipts_pkey PRIMARY KEY (batch_receipt_id);

-- private.candidate_daily_entitlements.candidate_daily_entitlements_pkey
alter table private.candidate_daily_entitlements add constraint candidate_daily_entitlements_pkey PRIMARY KEY (environment, candidate_id);

-- private.candidate_daily_external_effect_receipts.candidate_daily_external_effect_receipts_pkey
alter table private.candidate_daily_external_effect_receipts add constraint candidate_daily_external_effect_receipts_pkey PRIMARY KEY (effect_receipt_id);

-- private.candidate_daily_source_links.candidate_daily_source_links_pkey
alter table private.candidate_daily_source_links add constraint candidate_daily_source_links_pkey PRIMARY KEY (link_id);

-- private.candidate_daily_sync_state.candidate_daily_sync_state_pkey
alter table private.candidate_daily_sync_state add constraint candidate_daily_sync_state_pkey PRIMARY KEY (environment, candidate_id, target);

-- private.invoice_async_snapshot_hmac_keys.invoice_async_snapshot_hmac_keys_pkey
alter table private.invoice_async_snapshot_hmac_keys add constraint invoice_async_snapshot_hmac_keys_pkey PRIMARY KEY (key_id);

-- public.app_change_counters.app_change_counters_pkey
alter table public.app_change_counters add constraint app_change_counters_pkey PRIMARY KEY (entity_key);

-- public.assignment_band_mappings.assignment_band_mappings_pkey
alter table public.assignment_band_mappings add constraint assignment_band_mappings_pkey PRIMARY KEY (id);

-- public.audit_events.audit_events_pkey
alter table public.audit_events add constraint audit_events_pkey PRIMARY KEY (id);

-- public.bank_name_checks.bank_name_checks_pkey
alter table public.bank_name_checks add constraint bank_name_checks_pkey PRIMARY KEY (id);

-- public.bank_payee_map.bank_payee_map_pkey
alter table public.bank_payee_map add constraint bank_payee_map_pkey PRIMARY KEY (id);

-- public.bank_provider_webhook_configs.bank_provider_webhook_configs_pkey
alter table public.bank_provider_webhook_configs add constraint bank_provider_webhook_configs_pkey PRIMARY KEY (id);

-- public.bank_provider_webhook_receipts.bank_provider_webhook_receipts_pkey
alter table public.bank_provider_webhook_receipts add constraint bank_provider_webhook_receipts_pkey PRIMARY KEY (id);

-- public.banking_alert_acknowledgements.banking_alert_acknowledgements_pkey
alter table public.banking_alert_acknowledgements add constraint banking_alert_acknowledgements_pkey PRIMARY KEY (id);

-- public.banking_alert_display_summary.banking_alert_display_summary_pkey
alter table public.banking_alert_display_summary add constraint banking_alert_display_summary_pkey PRIMARY KEY (id);

-- public.banking_alert_success_events.banking_alert_success_events_pkey
alter table public.banking_alert_success_events add constraint banking_alert_success_events_pkey PRIMARY KEY (id);

-- public.banking_alert_user_preferences.banking_alert_user_preferences_pkey
alter table public.banking_alert_user_preferences add constraint banking_alert_user_preferences_pkey PRIMARY KEY (id);

-- public.banking_pay_batch_change_signals.banking_pay_batch_change_signals_pkey
alter table public.banking_pay_batch_change_signals add constraint banking_pay_batch_change_signals_pkey PRIMARY KEY (pay_batch_id);

-- public.banking_pay_operation_candidate_allocation_rows.banking_pay_operation_candidate_allocation_rows_pkey
alter table public.banking_pay_operation_candidate_allocation_rows add constraint banking_pay_operation_candidate_allocation_rows_pkey PRIMARY KEY (id);

-- public.banking_pay_operation_candidate_scope.banking_pay_operation_candidate_scope_pkey
alter table public.banking_pay_operation_candidate_scope add constraint banking_pay_operation_candidate_scope_pkey PRIMARY KEY (id);

-- public.banking_pay_operation_chunks.banking_pay_operation_chunks_pkey
alter table public.banking_pay_operation_chunks add constraint banking_pay_operation_chunks_pkey PRIMARY KEY (id);

-- public.banking_pay_operation_config.banking_pay_operation_config_pkey
alter table public.banking_pay_operation_config add constraint banking_pay_operation_config_pkey PRIMARY KEY (id);

-- public.banking_pay_operation_provider_attempts.banking_pay_operation_provider_attempts_pkey
alter table public.banking_pay_operation_provider_attempts add constraint banking_pay_operation_provider_attempts_pkey PRIMARY KEY (id);

-- public.banking_pay_operation_remittance_scope.banking_pay_operation_remittance_scope_pkey
alter table public.banking_pay_operation_remittance_scope add constraint banking_pay_operation_remittance_scope_pkey PRIMARY KEY (id);

-- public.banking_pay_operation_scope_units.banking_pay_operation_scope_units_pkey
alter table public.banking_pay_operation_scope_units add constraint banking_pay_operation_scope_units_pkey PRIMARY KEY (id);

-- public.banking_pay_operation_settlement_scope.banking_pay_operation_settlement_scope_pkey
alter table public.banking_pay_operation_settlement_scope add constraint banking_pay_operation_settlement_scope_pkey PRIMARY KEY (id);

-- public.banking_pay_operation_transfer_scope_items.banking_pay_operation_transfer_scope_items_pkey
alter table public.banking_pay_operation_transfer_scope_items add constraint banking_pay_operation_transfer_scope_items_pkey PRIMARY KEY (id);

-- public.banking_pay_operation_transfer_scope.banking_pay_operation_transfer_scope_pkey
alter table public.banking_pay_operation_transfer_scope add constraint banking_pay_operation_transfer_scope_pkey PRIMARY KEY (id);

-- public.banking_pay_operations.banking_pay_operations_pkey
alter table public.banking_pay_operations add constraint banking_pay_operations_pkey PRIMARY KEY (id);

-- public.banking_pay_scope_change_transactions.banking_pay_scope_change_transactions_pkey
alter table public.banking_pay_scope_change_transactions add constraint banking_pay_scope_change_transactions_pkey PRIMARY KEY (tx_token);

-- public.banking_pay_snapshot_candidate_state.banking_pay_snapshot_candidate_state_pkey
alter table public.banking_pay_snapshot_candidate_state add constraint banking_pay_snapshot_candidate_state_pkey PRIMARY KEY (id);

-- public.banking_pay_snapshot_case_component_state.banking_pay_snapshot_case_component_state_pkey
alter table public.banking_pay_snapshot_case_component_state add constraint banking_pay_snapshot_case_component_state_pkey PRIMARY KEY (id);

-- public.banking_pay_snapshot_case_state.banking_pay_snapshot_case_state_pkey
alter table public.banking_pay_snapshot_case_state add constraint banking_pay_snapshot_case_state_pkey PRIMARY KEY (id);

-- public.banking_pay_snapshot_line_state.banking_pay_snapshot_line_state_pkey
alter table public.banking_pay_snapshot_line_state add constraint banking_pay_snapshot_line_state_pkey PRIMARY KEY (id);

-- public.banking_pay_snapshot_runs.banking_pay_snapshot_runs_pkey
alter table public.banking_pay_snapshot_runs add constraint banking_pay_snapshot_runs_pkey PRIMARY KEY (id);

-- public.banking_pay_workbench_candidate_delta_projection_runs.banking_pay_workbench_candidate_delta_projection_runs_pkey
alter table public.banking_pay_workbench_candidate_delta_projection_runs add constraint banking_pay_workbench_candidate_delta_projection_runs_pkey PRIMARY KEY (id);

-- public.banking_pay_workbench_candidate_line_work.banking_pay_workbench_candidate_line_work_pkey
alter table public.banking_pay_workbench_candidate_line_work add constraint banking_pay_workbench_candidate_line_work_pkey PRIMARY KEY (id);

-- public.banking_pay_workbench_candidate_source_lines.bpay_wb_candidate_source_lines_pkey
alter table public.banking_pay_workbench_candidate_source_lines add constraint bpay_wb_candidate_source_lines_pkey PRIMARY KEY (id);

-- public.banking_pay_workbench_case_resolution_carry_registrations.banking_pay_workbench_case_resolution_carry_registrations_pkey
alter table public.banking_pay_workbench_case_resolution_carry_registrations add constraint banking_pay_workbench_case_resolution_carry_registrations_pkey PRIMARY KEY (id);

-- public.banking_pay_workbench_jobs.banking_pay_workbench_jobs_pkey
alter table public.banking_pay_workbench_jobs add constraint banking_pay_workbench_jobs_pkey PRIMARY KEY (id);

-- public.banking_pay_workbench_preview_rows.banking_pay_workbench_preview_rows_pkey
alter table public.banking_pay_workbench_preview_rows add constraint banking_pay_workbench_preview_rows_pkey PRIMARY KEY (id);

-- public.banking_pay_workbench_selection_carry_registrations.banking_pay_workbench_selection_carry_registrations_pkey
alter table public.banking_pay_workbench_selection_carry_registrations add constraint banking_pay_workbench_selection_carry_registrations_pkey PRIMARY KEY (id);

-- public.banking_pay_workbench_session_candidate_state.banking_pay_workbench_session_candidate_state_pkey
alter table public.banking_pay_workbench_session_candidate_state add constraint banking_pay_workbench_session_candidate_state_pkey PRIMARY KEY (id);

-- public.banking_pay_workbench_session_case_resolutions.banking_pay_workbench_session_case_resolutions_pkey
alter table public.banking_pay_workbench_session_case_resolutions add constraint banking_pay_workbench_session_case_resolutions_pkey PRIMARY KEY (id);

-- public.banking_pay_workbench_session_overrides.banking_pay_workbench_session_overrides_pkey
alter table public.banking_pay_workbench_session_overrides add constraint banking_pay_workbench_session_overrides_pkey PRIMARY KEY (id);

-- public.banking_pay_workbench_session_scope.banking_pay_workbench_session_scope_pkey
alter table public.banking_pay_workbench_session_scope add constraint banking_pay_workbench_session_scope_pkey PRIMARY KEY (id);

-- public.banking_pay_workbench_sessions.banking_pay_workbench_sessions_pkey
alter table public.banking_pay_workbench_sessions add constraint banking_pay_workbench_sessions_pkey PRIMARY KEY (id);

-- public.candidate_app_accounts.candidate_app_accounts_pkey
alter table public.candidate_app_accounts add constraint candidate_app_accounts_pkey PRIMARY KEY (id);

-- public.candidate_app_global_membership_links.candidate_app_global_membership_links_pkey
alter table public.candidate_app_global_membership_links add constraint candidate_app_global_membership_links_pkey PRIMARY KEY (membership_id);

-- public.candidate_app_sessions.candidate_app_sessions_pkey
alter table public.candidate_app_sessions add constraint candidate_app_sessions_pkey PRIMARY KEY (id);

-- public.candidate_approval_requests.candidate_approval_requests_pkey
alter table public.candidate_approval_requests add constraint candidate_approval_requests_pkey PRIMARY KEY (id);

-- public.candidate_auth_challenges.candidate_auth_challenges_pkey
alter table public.candidate_auth_challenges add constraint candidate_auth_challenges_pkey PRIMARY KEY (id);

-- public.candidate_daily_availability_days.candidate_daily_availability_days_pkey
alter table public.candidate_daily_availability_days add constraint candidate_daily_availability_days_pkey PRIMARY KEY (environment, candidate_id, availability_date);

-- public.candidate_daily_command_receipts.candidate_daily_command_receipts_pkey
alter table public.candidate_daily_command_receipts add constraint candidate_daily_command_receipts_pkey PRIMARY KEY (command_id);

-- public.candidate_daily_rota_days.candidate_daily_rota_days_pkey
alter table public.candidate_daily_rota_days add constraint candidate_daily_rota_days_pkey PRIMARY KEY (generation_id, rota_date);

-- public.candidate_daily_rota_generations.candidate_daily_rota_generations_pkey
alter table public.candidate_daily_rota_generations add constraint candidate_daily_rota_generations_pkey PRIMARY KEY (generation_id);

-- public.candidate_daily_sheet_projection_outbox.candidate_daily_sheet_projection_outbox_pkey
alter table public.candidate_daily_sheet_projection_outbox add constraint candidate_daily_sheet_projection_outbox_pkey PRIMARY KEY (outbox_id);

-- public.candidate_job_titles.candidate_job_titles_pkey
alter table public.candidate_job_titles add constraint candidate_job_titles_pkey PRIMARY KEY (id);

-- public.candidate_notifications.candidate_notifications_pkey
alter table public.candidate_notifications add constraint candidate_notifications_pkey PRIMARY KEY (id);

-- public.candidate_submission_components.candidate_submission_components_pkey
alter table public.candidate_submission_components add constraint candidate_submission_components_pkey PRIMARY KEY (id);

-- public.candidate_submission_workflows.candidate_submission_workflows_pkey
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_pkey PRIMARY KEY (id);

-- public.candidates_tombstones.candidates_tombstones_pkey
alter table public.candidates_tombstones add constraint candidates_tombstones_pkey PRIMARY KEY (id);

-- public.candidates.candidates_pkey
alter table public.candidates add constraint candidates_pkey PRIMARY KEY (id);

-- public.client_hospitals.client_hospitals_pkey
alter table public.client_hospitals add constraint client_hospitals_pkey PRIMARY KEY (id);

-- public.client_settings.client_settings_pkey
alter table public.client_settings add constraint client_settings_pkey PRIMARY KEY (id);

-- public.clients_tombstones.clients_tombstones_pkey
alter table public.clients_tombstones add constraint clients_tombstones_pkey PRIMARY KEY (id);

-- public.clients.clients_pkey
alter table public.clients add constraint clients_pkey PRIMARY KEY (id);

-- public.comms_outbox.comms_outbox_pkey
alter table public.comms_outbox add constraint comms_outbox_pkey PRIMARY KEY (id);

-- public.contract_weeks.contract_weeks_pkey
alter table public.contract_weeks add constraint contract_weeks_pkey PRIMARY KEY (id);

-- public.contracts.contracts_pkey
alter table public.contracts add constraint contracts_pkey PRIMARY KEY (id);

-- public.default_job_titles.default_job_titles_pkey
alter table public.default_job_titles add constraint default_job_titles_pkey PRIMARY KEY (id);

-- public.document_templates.document_templates_pkey
alter table public.document_templates add constraint document_templates_pkey PRIMARY KEY (id);

-- public.hr_daily_grade_role_mappings.hr_daily_grade_role_mappings_pkey
alter table public.hr_daily_grade_role_mappings add constraint hr_daily_grade_role_mappings_pkey PRIMARY KEY (id);

-- public.hr_imports.hr_imports_pkey
alter table public.hr_imports add constraint hr_imports_pkey PRIMARY KEY (id);

-- public.hr_issue_email_deliveries.hr_issue_email_deliveries_pkey
alter table public.hr_issue_email_deliveries add constraint hr_issue_email_deliveries_pkey PRIMARY KEY (id);

-- public.hr_issue_email_delivery_items.hr_issue_email_delivery_items_pkey
alter table public.hr_issue_email_delivery_items add constraint hr_issue_email_delivery_items_pkey PRIMARY KEY (id);

-- public.hr_issue_emails.hr_issue_emails_pkey
alter table public.hr_issue_emails add constraint hr_issue_emails_pkey PRIMARY KEY (id);

-- public.hr_name_mappings.hr_name_mappings_pkey
alter table public.hr_name_mappings add constraint hr_name_mappings_pkey PRIMARY KEY (id);

-- public.hr_results.hr_results_pkey
alter table public.hr_results add constraint hr_results_pkey PRIMARY KEY (id);

-- public.hr_rows.hr_rows_pkey
alter table public.hr_rows add constraint hr_rows_pkey PRIMARY KEY (id);

-- public.id_consolidation_run_lines.id_consolidation_run_lines_pkey
alter table public.id_consolidation_run_lines add constraint id_consolidation_run_lines_pkey PRIMARY KEY (id_ref, invoice_id);

-- public.id_consolidation_runs.id_consolidation_runs_pkey
alter table public.id_consolidation_runs add constraint id_consolidation_runs_pkey PRIMARY KEY (id_ref);

-- public.id_invoice_ledger.id_invoice_ledger_pkey
alter table public.id_invoice_ledger add constraint id_invoice_ledger_pkey PRIMARY KEY (invoice_id);

-- public.import_apply_operations.import_apply_operations_pkey
alter table public.import_apply_operations add constraint import_apply_operations_pkey PRIMARY KEY (id);

-- public.import_column_aliases.import_column_aliases_pkey
alter table public.import_column_aliases add constraint import_column_aliases_pkey PRIMARY KEY (id);

-- public.import_review_action_outcomes.import_review_action_outcomes_pkey
alter table public.import_review_action_outcomes add constraint import_review_action_outcomes_pkey PRIMARY KEY (action_id);

-- public.import_review_daily_timesheet_resolutions.import_review_daily_timesheet_resolutions_pkey
alter table public.import_review_daily_timesheet_resolutions add constraint import_review_daily_timesheet_resolutions_pkey PRIMARY KEY (id);

-- public.import_review_decisions.import_review_decisions_pkey
alter table public.import_review_decisions add constraint import_review_decisions_pkey PRIMARY KEY (action_id);

-- public.import_review_events.import_review_events_pkey
alter table public.import_review_events add constraint import_review_events_pkey PRIMARY KEY (id);

-- public.import_review_scope_candidates.import_review_scope_candidates_pkey
alter table public.import_review_scope_candidates add constraint import_review_scope_candidates_pkey PRIMARY KEY (id);

-- public.import_review_scope_clients.import_review_scope_clients_pkey
alter table public.import_review_scope_clients add constraint import_review_scope_clients_pkey PRIMARY KEY (id);

-- public.import_review_states.import_review_states_pkey
alter table public.import_review_states add constraint import_review_states_pkey PRIMARY KEY (import_id);

-- public.import_review_weekly_validation_resolutions.import_review_weekly_validation_resolutions_pkey
alter table public.import_review_weekly_validation_resolutions add constraint import_review_weekly_validation_resolutions_pkey PRIMARY KEY (id);

-- public.invoice_document_assets.invoice_document_assets_pkey
alter table public.invoice_document_assets add constraint invoice_document_assets_pkey PRIMARY KEY (id);

-- public.invoice_document_versions.invoice_document_versions_pkey
alter table public.invoice_document_versions add constraint invoice_document_versions_pkey PRIMARY KEY (id);

-- public.invoice_hr_source_rows.invoice_hr_source_rows_pkey
alter table public.invoice_hr_source_rows add constraint invoice_hr_source_rows_pkey PRIMARY KEY (invoice_id, source_system, import_id);

-- public.invoice_jobs_outbox.invoice_jobs_outbox_pkey
alter table public.invoice_jobs_outbox add constraint invoice_jobs_outbox_pkey PRIMARY KEY (id);

-- public.invoice_lines.invoice_lines_pkey
alter table public.invoice_lines add constraint invoice_lines_pkey PRIMARY KEY (id);

-- public.invoice_operation_chunks.invoice_operation_chunks_pkey
alter table public.invoice_operation_chunks add constraint invoice_operation_chunks_pkey PRIMARY KEY (id);

-- public.invoice_operations.invoice_operations_pkey
alter table public.invoice_operations add constraint invoice_operations_pkey PRIMARY KEY (id);

-- public.invoice_pdf_outbox.invoice_pdf_outbox_pkey
alter table public.invoice_pdf_outbox add constraint invoice_pdf_outbox_pkey PRIMARY KEY (id);

-- public.invoices.invoices_pkey
alter table public.invoices add constraint invoices_pkey PRIMARY KEY (id);

-- public.legacy_contract_rate_lines.legacy_contract_rate_lines_pkey
alter table public.legacy_contract_rate_lines add constraint legacy_contract_rate_lines_pkey PRIMARY KEY (id);

-- public.legacy_contracts.legacy_contracts_pkey
alter table public.legacy_contracts add constraint legacy_contracts_pkey PRIMARY KEY (id);

-- public.legacy_eclipse_candidate_map.legacy_eclipse_candidate_map_pkey
alter table public.legacy_eclipse_candidate_map add constraint legacy_eclipse_candidate_map_pkey PRIMARY KEY (legacy_candidate_ref);

-- public.legacy_eclipse_client_map.legacy_eclipse_client_map_pkey
alter table public.legacy_eclipse_client_map add constraint legacy_eclipse_client_map_pkey PRIMARY KEY (legacy_client_ref);

-- public.mail_outbox.mail_outbox_pkey
alter table public.mail_outbox add constraint mail_outbox_pkey PRIMARY KEY (id);

-- public.mailshot_field_overrides.mailshot_field_overrides_pkey
alter table public.mailshot_field_overrides add constraint mailshot_field_overrides_pkey PRIMARY KEY (id);

-- public.mailshot_fields.mailshot_fields_pkey
alter table public.mailshot_fields add constraint mailshot_fields_pkey PRIMARY KEY (id);

-- public.mailshot_runs.mailshot_runs_pkey
alter table public.mailshot_runs add constraint mailshot_runs_pkey PRIMARY KEY (id);

-- public.manual_timesheet_queue.manual_timesheet_queue_pkey
alter table public.manual_timesheet_queue add constraint manual_timesheet_queue_pkey PRIMARY KEY (id);

-- public.migration_smoke_once_only.migration_smoke_once_only_pkey
alter table public.migration_smoke_once_only add constraint migration_smoke_once_only_pkey PRIMARY KEY (id);

-- public.nhsp_shifts.nhsp_shifts_pkey
alter table public.nhsp_shifts add constraint nhsp_shifts_pkey PRIMARY KEY (id);

-- public.pay_advance_patches.pay_advance_patches_pkey
alter table public.pay_advance_patches add constraint pay_advance_patches_pkey PRIMARY KEY (id);

-- public.pay_advance_reservations.pay_advance_reservations_pkey
alter table public.pay_advance_reservations add constraint pay_advance_reservations_pkey PRIMARY KEY (id);

-- public.pay_advances.pay_advances_pkey
alter table public.pay_advances add constraint pay_advances_pkey PRIMARY KEY (id);

-- public.pay_bank_transfer_events.pay_bank_transfer_events_pkey
alter table public.pay_bank_transfer_events add constraint pay_bank_transfer_events_pkey PRIMARY KEY (id);

-- public.pay_bank_transfers.pay_bank_transfers_pkey
alter table public.pay_bank_transfers add constraint pay_bank_transfers_pkey PRIMARY KEY (id);

-- public.pay_batch_auth_actions.pay_batch_auth_actions_pkey
alter table public.pay_batch_auth_actions add constraint pay_batch_auth_actions_pkey PRIMARY KEY (id);

-- public.pay_batch_auth_requests.pay_batch_auth_requests_pkey
alter table public.pay_batch_auth_requests add constraint pay_batch_auth_requests_pkey PRIMARY KEY (id);

-- public.pay_batch_auth_tokens.pay_batch_auth_tokens_pkey
alter table public.pay_batch_auth_tokens add constraint pay_batch_auth_tokens_pkey PRIMARY KEY (token);

-- public.pay_batch_candidates.pay_batch_candidates_pkey
alter table public.pay_batch_candidates add constraint pay_batch_candidates_pkey PRIMARY KEY (id);

-- public.pay_batch_display_summary.pay_batch_display_summary_pkey
alter table public.pay_batch_display_summary add constraint pay_batch_display_summary_pkey PRIMARY KEY (id);

-- public.pay_batch_item_breakdowns.pay_batch_item_breakdowns_pkey
alter table public.pay_batch_item_breakdowns add constraint pay_batch_item_breakdowns_pkey PRIMARY KEY (id);

-- public.pay_batch_items.pay_batch_items_pkey
alter table public.pay_batch_items add constraint pay_batch_items_pkey PRIMARY KEY (id);

-- public.pay_batch_paye_net_inputs.pay_batch_paye_net_inputs_pkey
alter table public.pay_batch_paye_net_inputs add constraint pay_batch_paye_net_inputs_pkey PRIMARY KEY (id);

-- public.pay_batch_timesheet_snapshots.pay_batch_timesheet_snapshots_pkey
alter table public.pay_batch_timesheet_snapshots add constraint pay_batch_timesheet_snapshots_pkey PRIMARY KEY (id);

-- public.pay_batches.pay_batches_pkey
alter table public.pay_batches add constraint pay_batches_pkey PRIMARY KEY (id);

-- public.pay_finance_case_components.pay_finance_case_components_pkey
alter table public.pay_finance_case_components add constraint pay_finance_case_components_pkey PRIMARY KEY (id);

-- public.pay_finance_case_events.pay_finance_case_events_pkey
alter table public.pay_finance_case_events add constraint pay_finance_case_events_pkey PRIMARY KEY (id);

-- public.pay_finance_case_oneoff_payout_bank_details.pay_finance_case_oneoff_payout_bank_details_pkey
alter table public.pay_finance_case_oneoff_payout_bank_details add constraint pay_finance_case_oneoff_payout_bank_details_pkey PRIMARY KEY (finance_case_id);

-- public.pay_item_snoozes.pay_item_snoozes_pkey
alter table public.pay_item_snoozes add constraint pay_item_snoozes_pkey PRIMARY KEY (id);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_forwards_pkey
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_forwards_pkey PRIMARY KEY (id);

-- public.pay_payment_correction_actions.pay_payment_correction_actions_pkey
alter table public.pay_payment_correction_actions add constraint pay_payment_correction_actions_pkey PRIMARY KEY (id);

-- public.pay_payment_correction_items.pay_payment_correction_items_pkey
alter table public.pay_payment_correction_items add constraint pay_payment_correction_items_pkey PRIMARY KEY (id);

-- public.pay_payment_correction_request_candidates.pay_payment_correction_request_candidates_pkey
alter table public.pay_payment_correction_request_candidates add constraint pay_payment_correction_request_candidates_pkey PRIMARY KEY (correction_request_id, pay_batch_candidate_id);

-- public.pay_payment_correction_requests.pay_payment_correction_requests_pkey
alter table public.pay_payment_correction_requests add constraint pay_payment_correction_requests_pkey PRIMARY KEY (id);

-- public.pay_payment_correction_work_items.pay_payment_correction_work_items_pkey
alter table public.pay_payment_correction_work_items add constraint pay_payment_correction_work_items_pkey PRIMARY KEY (id);

-- public.pay_payment_return_notice_groups.pay_payment_return_notice_groups_pkey
alter table public.pay_payment_return_notice_groups add constraint pay_payment_return_notice_groups_pkey PRIMARY KEY (id);

-- public.pay_snooze_warning_acknowledgements.pay_snooze_warning_acknowledgements_pkey
alter table public.pay_snooze_warning_acknowledgements add constraint pay_snooze_warning_acknowledgements_pkey PRIMARY KEY (id);

-- public.rates_candidate_overrides.rates_candidate_overrides_pkey
alter table public.rates_candidate_overrides add constraint rates_candidate_overrides_pkey PRIMARY KEY (id);

-- public.rates_client_defaults.rates_client_defaults_pkey
alter table public.rates_client_defaults add constraint rates_client_defaults_pkey PRIMARY KEY (id);

-- public.rates_presets.rates_presets_pkey
alter table public.rates_presets add constraint rates_presets_pkey PRIMARY KEY (id);

-- public.report_presets.report_presets_pkey
alter table public.report_presets add constraint report_presets_pkey PRIMARY KEY (id);

-- public.schema_migrations.schema_migrations_pkey
alter table public.schema_migrations add constraint schema_migrations_pkey PRIMARY KEY (filename);

-- public.schema_repeatables.schema_repeatables_pkey
alter table public.schema_repeatables add constraint schema_repeatables_pkey PRIMARY KEY (filename);

-- public.settings_defaults.settings_defaults_pkey
alter table public.settings_defaults add constraint settings_defaults_pkey PRIMARY KEY (id);

-- public.settings_finance_windows.settings_finance_windows_pkey
alter table public.settings_finance_windows add constraint settings_finance_windows_pkey PRIMARY KEY (id);

-- public.sheets_outbox.sheets_outbox_pkey
alter table public.sheets_outbox add constraint sheets_outbox_pkey PRIMARY KEY (id);

-- public.timesheet_archive_transition_capability.timesheet_archive_transition_capability_pk
alter table public.timesheet_archive_transition_capability add constraint timesheet_archive_transition_capability_pk PRIMARY KEY (capability_token, timesheet_id);

-- public.timesheet_evidence.timesheet_evidence_pkey
alter table public.timesheet_evidence add constraint timesheet_evidence_pkey PRIMARY KEY (id);

-- public.timesheet_financial_retention.timesheet_financial_retention_pkey
alter table public.timesheet_financial_retention add constraint timesheet_financial_retention_pkey PRIMARY KEY (timesheet_id);

-- public.timesheet_lifecycle_bulk_operation_items.timesheet_lifecycle_bulk_operation_items_pkey
alter table public.timesheet_lifecycle_bulk_operation_items add constraint timesheet_lifecycle_bulk_operation_items_pkey PRIMARY KEY (id);

-- public.timesheet_lifecycle_bulk_operations.timesheet_lifecycle_bulk_operations_pkey
alter table public.timesheet_lifecycle_bulk_operations add constraint timesheet_lifecycle_bulk_operations_pkey PRIMARY KEY (id);

-- public.timesheet_pay_state_history.timesheet_pay_state_history_pkey
alter table public.timesheet_pay_state_history add constraint timesheet_pay_state_history_pkey PRIMARY KEY (id);

-- public.timesheet_pay_state.timesheet_pay_state_pkey
alter table public.timesheet_pay_state add constraint timesheet_pay_state_pkey PRIMARY KEY (timesheet_id);

-- public.timesheet_payment_overrides.timesheet_payment_overrides_pkey
alter table public.timesheet_payment_overrides add constraint timesheet_payment_overrides_pkey PRIMARY KEY (id);

-- public.timesheet_r2_cleanup_queue.timesheet_r2_cleanup_queue_pk
alter table public.timesheet_r2_cleanup_queue add constraint timesheet_r2_cleanup_queue_pk PRIMARY KEY (delete_operation_id, r2_key);

-- public.timesheet_summary_pay_state_cache.timesheet_summary_pay_state_cache_pkey
alter table public.timesheet_summary_pay_state_cache add constraint timesheet_summary_pay_state_cache_pkey PRIMARY KEY (timesheet_id);

-- public.timesheet_validations.timesheet_validations_pkey
alter table public.timesheet_validations add constraint timesheet_validations_pkey PRIMARY KEY (id);

-- public.timesheets_financials.timesheets_financials_pkey
alter table public.timesheets_financials add constraint timesheets_financials_pkey PRIMARY KEY (id);

-- public.timesheets.timesheets_pkey
alter table public.timesheets add constraint timesheets_pkey PRIMARY KEY (timesheet_id);

-- public.tms_login_2fa_challenges.tms_login_2fa_challenges_pkey
alter table public.tms_login_2fa_challenges add constraint tms_login_2fa_challenges_pkey PRIMARY KEY (id);

-- public.tms_password_resets.tms_password_resets_pkey
alter table public.tms_password_resets add constraint tms_password_resets_pkey PRIMARY KEY (id);

-- public.tms_user_2fa_trust.tms_user_2fa_trust_pkey
alter table public.tms_user_2fa_trust add constraint tms_user_2fa_trust_pkey PRIMARY KEY (id);

-- public.tms_users.tms_users_pkey
alter table public.tms_users add constraint tms_users_pkey PRIMARY KEY (id);

-- public.ts_financials_outbox.ts_financials_outbox_pkey
alter table public.ts_financials_outbox add constraint ts_financials_outbox_pkey PRIMARY KEY (id);

-- public.ts_pay_adjustments.ts_pay_adjustments_pkey
alter table public.ts_pay_adjustments add constraint ts_pay_adjustments_pkey PRIMARY KEY (id);

-- public.ts_pdfs_outbox.ts_pdfs_outbox_pkey
alter table public.ts_pdfs_outbox add constraint ts_pdfs_outbox_pkey PRIMARY KEY (id);

-- public.umbrellas.umbrellas_pkey
alter table public.umbrellas add constraint umbrellas_pkey PRIMARY KEY (id);

-- private.banking_pay_workbench_canonical_stage_lines.bpay_wb_canonical_stage_identity_uq
alter table private.banking_pay_workbench_canonical_stage_lines add constraint bpay_wb_canonical_stage_identity_uq UNIQUE NULLS NOT DISTINCT (build_id, timesheet_id, line_key);

-- private.banking_pay_workbench_economic_build_fact_pages.bpay_wb_economic_build_fact_pages_cursor_uq
alter table private.banking_pay_workbench_economic_build_fact_pages add constraint bpay_wb_economic_build_fact_pages_cursor_uq UNIQUE (build_id, dependency_unit_key, fact_family, cursor_start_hash);

-- private.banking_pay_workbench_economic_build_fact_pages.bpay_wb_economic_build_fact_pages_number_uq
alter table private.banking_pay_workbench_economic_build_fact_pages add constraint bpay_wb_economic_build_fact_pages_number_uq UNIQUE (build_id, dependency_unit_key, fact_family, page_number);

-- private.banking_pay_workbench_economic_builds.bpay_wb_economic_builds_token_uq
alter table private.banking_pay_workbench_economic_builds add constraint bpay_wb_economic_builds_token_uq UNIQUE (build_token);

-- private.banking_pay_workbench_stage_attempts.bpay_wb_stage_attempts_job_number_uq
alter table private.banking_pay_workbench_stage_attempts add constraint bpay_wb_stage_attempts_job_number_uq UNIQUE (job_id, attempt_number);

-- private.banking_pay_workbench_stage_attempts.bpay_wb_stage_attempts_nonce_uq
alter table private.banking_pay_workbench_stage_attempts add constraint bpay_wb_stage_attempts_nonce_uq UNIQUE (attempt_nonce);

-- private.candidate_daily_authority_transitions.candidate_daily_authority_transitions_batch_candidate_uq
alter table private.candidate_daily_authority_transitions add constraint candidate_daily_authority_transitions_batch_candidate_uq UNIQUE (batch_receipt_id, candidate_id);

-- private.candidate_daily_batch_receipts.candidate_daily_batch_receipts_scope_uq
alter table private.candidate_daily_batch_receipts add constraint candidate_daily_batch_receipts_scope_uq UNIQUE (environment, actor_class, operation_class, idempotency_key);

-- private.candidate_daily_external_effect_receipts.candidate_daily_external_effects_effect_uq
alter table private.candidate_daily_external_effect_receipts add constraint candidate_daily_external_effects_effect_uq UNIQUE (environment, effect_key);

-- private.candidate_daily_external_effect_receipts.candidate_daily_external_effects_idempotency_uq
alter table private.candidate_daily_external_effect_receipts add constraint candidate_daily_external_effects_idempotency_uq UNIQUE (environment, candidate_id, operation, idempotency_key);

-- private.invoice_async_snapshot_hmac_keys.invoice_async_snapshot_hmac_keys_vault_secret_id_key
alter table private.invoice_async_snapshot_hmac_keys add constraint invoice_async_snapshot_hmac_keys_vault_secret_id_key UNIQUE (vault_secret_id);

-- public.banking_alert_success_events.banking_alert_success_events_batch_kind_key_uk
alter table public.banking_alert_success_events add constraint banking_alert_success_events_batch_kind_key_uk UNIQUE (pay_batch_id, alert_kind, event_key);

-- public.candidate_app_accounts.candidate_app_accounts_environment_email_uq
alter table public.candidate_app_accounts add constraint candidate_app_accounts_environment_email_uq UNIQUE (environment, email_normalized);

-- public.candidate_app_global_membership_links.candidate_app_global_membersh_global_account_identity_hmac__key
alter table public.candidate_app_global_membership_links add constraint candidate_app_global_membersh_global_account_identity_hmac__key UNIQUE (global_account_identity_hmac, candidate_id);

-- public.candidate_app_sessions.candidate_app_sessions_refresh_hash_uq
alter table public.candidate_app_sessions add constraint candidate_app_sessions_refresh_hash_uq UNIQUE (refresh_token_hash);

-- public.candidate_auth_challenges.candidate_auth_challenges_outbox_key_uq
alter table public.candidate_auth_challenges add constraint candidate_auth_challenges_outbox_key_uq UNIQUE (deterministic_outbox_key);

-- public.candidate_auth_challenges.candidate_auth_challenges_token_hash_uq
alter table public.candidate_auth_challenges add constraint candidate_auth_challenges_token_hash_uq UNIQUE (token_hash);

-- public.candidate_daily_availability_days.candidate_daily_availability_days_evidence_uq
alter table public.candidate_daily_availability_days add constraint candidate_daily_availability_days_evidence_uq UNIQUE (environment, candidate_id, availability_version, availability_date);

-- public.candidate_daily_command_receipts.candidate_daily_command_receipts_scope_uq
alter table public.candidate_daily_command_receipts add constraint candidate_daily_command_receipts_scope_uq UNIQUE (environment, candidate_id, actor_class, idempotency_key);

-- public.candidate_daily_rota_days.candidate_daily_rota_days_identity_uq
alter table public.candidate_daily_rota_days add constraint candidate_daily_rota_days_identity_uq UNIQUE (generation_id, environment, candidate_id, rota_date);

-- public.candidate_daily_rota_generations.candidate_daily_rota_generations_identity_uq
alter table public.candidate_daily_rota_generations add constraint candidate_daily_rota_generations_identity_uq UNIQUE (generation_id, environment, candidate_id);

-- public.candidate_daily_rota_generations.candidate_daily_rota_generations_source_uq
alter table public.candidate_daily_rota_generations add constraint candidate_daily_rota_generations_source_uq UNIQUE (environment, source_system, source_event_id, item_key);

-- public.candidate_daily_rota_generations.candidate_daily_rota_generations_version_uq
alter table public.candidate_daily_rota_generations add constraint candidate_daily_rota_generations_version_uq UNIQUE (environment, candidate_id, generation_version);

-- public.candidate_daily_sheet_projection_outbox.candidate_daily_sheet_projection_outbox_dedupe_uq
alter table public.candidate_daily_sheet_projection_outbox add constraint candidate_daily_sheet_projection_outbox_dedupe_uq UNIQUE (environment, target, candidate_id, availability_date, availability_version, operation);

-- public.candidate_job_titles.candidate_job_titles_candidate_role_unique
alter table public.candidate_job_titles add constraint candidate_job_titles_candidate_role_unique UNIQUE (candidate_id, job_title_id);

-- public.candidate_notifications.candidate_notifications_dedupe_uq
alter table public.candidate_notifications add constraint candidate_notifications_dedupe_uq UNIQUE (dedupe_key);

-- public.candidate_submission_components.candidate_submission_components_workflow_component_uq
alter table public.candidate_submission_components add constraint candidate_submission_components_workflow_component_uq UNIQUE (workflow_id, workflow_generation, component_no);

-- public.candidate_submission_workflows.candidate_submission_workflows_account_idempotency_uq
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_account_idempotency_uq UNIQUE (account_id, idempotency_key);

-- public.candidates.candidates_tms_ref_key
alter table public.candidates add constraint candidates_tms_ref_key UNIQUE (tms_ref);

-- public.clients.clients_cli_ref_key
alter table public.clients add constraint clients_cli_ref_key UNIQUE (cli_ref);

-- public.contract_weeks.uq_contract_week
alter table public.contract_weeks add constraint uq_contract_week UNIQUE (contract_id, week_ending_date, additional_seq);

-- public.hr_issue_email_deliveries.hr_issue_email_deliveries_deterministic_outbox_key_key
alter table public.hr_issue_email_deliveries add constraint hr_issue_email_deliveries_deterministic_outbox_key_key UNIQUE (deterministic_outbox_key);

-- public.hr_issue_email_deliveries.hr_issue_email_deliveries_operation_id_recipient_scope_key__key
alter table public.hr_issue_email_deliveries add constraint hr_issue_email_deliveries_operation_id_recipient_scope_key__key UNIQUE (operation_id, recipient_scope_key, issue_set_fingerprint, reminder_sequence);

-- public.hr_issue_email_delivery_items.hr_issue_email_delivery_items_delivery_id_action_id_key
alter table public.hr_issue_email_delivery_items add constraint hr_issue_email_delivery_items_delivery_id_action_id_key UNIQUE (delivery_id, action_id);

-- public.hr_issue_email_delivery_items.hr_issue_email_delivery_items_delivery_id_issue_id_key
alter table public.hr_issue_email_delivery_items add constraint hr_issue_email_delivery_items_delivery_id_issue_id_key UNIQUE (delivery_id, issue_id);

-- public.hr_name_mappings.uq_hr_name_mappings_cols
alter table public.hr_name_mappings add constraint uq_hr_name_mappings_cols UNIQUE (hr_name_norm, hospital_or_trust);

-- public.import_apply_operations.uq_import_apply_operations_request
alter table public.import_apply_operations add constraint uq_import_apply_operations_request UNIQUE (import_id, import_revision, request_hash);

-- public.import_review_action_outcomes.import_review_action_outcomes_operation_id_action_id_key
alter table public.import_review_action_outcomes add constraint import_review_action_outcomes_operation_id_action_id_key UNIQUE (operation_id, action_id);

-- public.import_review_daily_timesheet_resolutions.import_review_daily_timesheet_resolutio_import_id_hr_row_id_key
alter table public.import_review_daily_timesheet_resolutions add constraint import_review_daily_timesheet_resolutio_import_id_hr_row_id_key UNIQUE (import_id, hr_row_id);

-- public.import_review_decisions.import_review_decisions_import_id_action_kind_target_key_key
alter table public.import_review_decisions add constraint import_review_decisions_import_id_action_kind_target_key_key UNIQUE (import_id, action_kind, target_key);

-- public.import_review_scope_candidates.import_review_scope_candidate_import_id_source_candidate_ke_key
alter table public.import_review_scope_candidates add constraint import_review_scope_candidate_import_id_source_candidate_ke_key UNIQUE (import_id, source_candidate_key);

-- public.import_review_scope_clients.import_review_scope_clients_import_id_source_client_key_key
alter table public.import_review_scope_clients add constraint import_review_scope_clients_import_id_source_client_key_key UNIQUE (import_id, source_client_key);

-- public.import_review_weekly_validation_resolutions.import_review_weekly_validation_resolut_import_id_hr_row_id_key
alter table public.import_review_weekly_validation_resolutions add constraint import_review_weekly_validation_resolut_import_id_hr_row_id_key UNIQUE (import_id, hr_row_id);

-- public.invoice_operation_chunks.invoice_operation_chunks_unique_slot
alter table public.invoice_operation_chunks add constraint invoice_operation_chunks_unique_slot UNIQUE (operation_id, chunk_type, level_no, sequence_no, work_key);

-- public.invoices.invoices_invoice_no_key
alter table public.invoices add constraint invoices_invoice_no_key UNIQUE (invoice_no);

-- public.legacy_contract_rate_lines.legacy_contract_rate_lines_uq
alter table public.legacy_contract_rate_lines add constraint legacy_contract_rate_lines_uq UNIQUE (legacy_contract_id, line_no);

-- public.mailshot_field_overrides.mailshot_field_overrides_field_id_entity_type_key
alter table public.mailshot_field_overrides add constraint mailshot_field_overrides_field_id_entity_type_key UNIQUE (field_id, entity_type);

-- public.mailshot_fields.mailshot_fields_field_key_key
alter table public.mailshot_fields add constraint mailshot_fields_field_key_key UNIQUE (field_key);

-- public.nhsp_shifts.nhsp_shifts_external_row_key_key
alter table public.nhsp_shifts add constraint nhsp_shifts_external_row_key_key UNIQUE (external_row_key);

-- public.pay_batch_auth_actions.ux_pay_batch_auth_actions_one_per_user
alter table public.pay_batch_auth_actions add constraint ux_pay_batch_auth_actions_one_per_user UNIQUE (auth_request_id, actor_user_id);

-- public.pay_batch_auth_tokens.ux_pay_batch_auth_tokens_one_per_target
alter table public.pay_batch_auth_tokens add constraint ux_pay_batch_auth_tokens_one_per_target UNIQUE (auth_request_id, target_user_id);

-- public.pay_payment_correction_request_candidates.pay_payment_correction_request_candidates_ordinal_key
alter table public.pay_payment_correction_request_candidates add constraint pay_payment_correction_request_candidates_ordinal_key UNIQUE (correction_request_id, selection_ordinal);

-- public.report_presets.report_presets_unique_name_per_scope
alter table public.report_presets add constraint report_presets_unique_name_per_scope UNIQUE (user_id, section, kind, name);

-- public.sheets_outbox.uq_sheets_outbox_booking_target
alter table public.sheets_outbox add constraint uq_sheets_outbox_booking_target UNIQUE (booking_id, target);

-- public.timesheet_lifecycle_bulk_operation_items.timesheet_lifecycle_bulk_operation_items_unique_ordinal
alter table public.timesheet_lifecycle_bulk_operation_items add constraint timesheet_lifecycle_bulk_operation_items_unique_ordinal UNIQUE (operation_id, ordinal);

-- public.tms_password_resets.tms_password_resets_token_key
alter table public.tms_password_resets add constraint tms_password_resets_token_key UNIQUE (token);

-- public.tms_user_2fa_trust.tms_user_2fa_trust_user_ip_key
alter table public.tms_user_2fa_trust add constraint tms_user_2fa_trust_user_ip_key UNIQUE (user_id, ip_address);

-- public.tms_users.tms_users_email_key
alter table public.tms_users add constraint tms_users_email_key UNIQUE (email);

-- public.ts_financials_outbox.uq_tsfin_outbox
alter table public.ts_financials_outbox add constraint uq_tsfin_outbox UNIQUE (timesheet_id, reason);

-- public.rates_candidate_overrides.rco_no_overlap
alter table public.rates_candidate_overrides add constraint rco_no_overlap EXCLUDE USING gist (candidate_id WITH =, client_id WITH =, role WITH =, COALESCE(band, ''::text) WITH =, rate_type WITH =, daterange(date_from, COALESCE(date_to + 1, 'infinity'::date), '[)'::text) WITH &&);

-- public.rates_client_defaults.rcd_enabled_no_overlap
alter table public.rates_client_defaults add constraint rcd_enabled_no_overlap EXCLUDE USING gist (client_id WITH =, role WITH =, COALESCE(band, ''::text) WITH =, daterange(date_from, COALESCE(date_to + 1, 'infinity'::date), '[)'::text) WITH &&) WHERE (disabled_at_utc IS NULL);

-- public.settings_finance_windows.settings_finance_windows_no_overlap_excl
alter table public.settings_finance_windows add constraint settings_finance_windows_no_overlap_excl EXCLUDE USING gist (daterange(date_from, COALESCE(date_to, 'infinity'::date), '[]'::text) WITH &&);

-- private.banking_pay_workbench_candidate_scope_registry.bpay_wb_scope_registry_bootstrap_chk
alter table private.banking_pay_workbench_candidate_scope_registry add constraint bpay_wb_scope_registry_bootstrap_chk CHECK ((initialisation_status <> ALL (ARRAY['DISCOVERING'::text, 'CLASSIFYING'::text])) OR bootstrap_id IS NOT NULL AND NULLIF(btrim(bootstrap_stream), ''::text) IS NOT NULL AND bootstrap_captured_generation IS NOT NULL AND bootstrap_captured_source_change_seq IS NOT NULL);

-- private.banking_pay_workbench_candidate_scope_registry.bpay_wb_scope_registry_counts_chk
alter table private.banking_pay_workbench_candidate_scope_registry add constraint bpay_wb_scope_registry_counts_chk CHECK (dirty_generation >= 0 AND evaluated_generation >= 0 AND evaluated_generation <= dirty_generation AND current_source_change_seq >= 0 AND bootstrap_rows_seen >= 0 AND bootstrap_timesheets_registered >= 0 AND (bootstrap_captured_generation IS NULL OR bootstrap_captured_generation >= 0) AND (bootstrap_captured_source_change_seq IS NULL OR bootstrap_captured_source_change_seq >= 0));

-- private.banking_pay_workbench_candidate_scope_registry.bpay_wb_scope_registry_json_chk
alter table private.banking_pay_workbench_candidate_scope_registry add constraint bpay_wb_scope_registry_json_chk CHECK (jsonb_typeof(bootstrap_cursor_json) = 'object'::text AND jsonb_typeof(failure_json) = 'object'::text AND btrim(last_dirty_reason) <> ''::text);

-- private.banking_pay_workbench_candidate_scope_registry.bpay_wb_scope_registry_status_chk
alter table private.banking_pay_workbench_candidate_scope_registry add constraint bpay_wb_scope_registry_status_chk CHECK (initialisation_status = ANY (ARRAY['UNINITIALISED'::text, 'DISCOVERING'::text, 'CLASSIFYING'::text, 'READY'::text, 'FAILED'::text]));

-- private.banking_pay_workbench_candidate_scope_registry.bpay_wb_scope_registry_terminal_chk
alter table private.banking_pay_workbench_candidate_scope_registry add constraint bpay_wb_scope_registry_terminal_chk CHECK ((initialisation_status <> 'READY'::text OR initialised_at_utc IS NOT NULL AND failure_json = '{}'::jsonb) AND (initialisation_status <> 'FAILED'::text OR failure_json <> '{}'::jsonb) AND updated_at_utc >= created_at_utc AND last_dirtied_at_utc >= created_at_utc AND (last_evaluated_at_utc IS NULL OR last_evaluated_at_utc >= created_at_utc) AND (initialised_at_utc IS NULL OR initialised_at_utc >= created_at_utc));

-- private.banking_pay_workbench_canonical_stage_lines.bpay_wb_canonical_stage_lines_authority_chk
alter table private.banking_pay_workbench_canonical_stage_lines add constraint bpay_wb_canonical_stage_lines_authority_chk CHECK (session_version > 0 AND source_change_seq >= 0 AND btrim(pay_channel_scope) <> ''::text AND btrim(refresh_scope_kind) <> ''::text AND (stage_status = 'VERIFIED'::text) = (verified_at_utc IS NOT NULL) AND (verified_at_utc IS NULL OR verified_at_utc >= created_at_utc));

-- private.banking_pay_workbench_canonical_stage_lines.bpay_wb_canonical_stage_lines_identity_chk
alter table private.banking_pay_workbench_canonical_stage_lines add constraint bpay_wb_canonical_stage_lines_identity_chk CHECK (source_ordinal > 0 AND btrim(line_key) <> ''::text);

-- private.banking_pay_workbench_canonical_stage_lines.bpay_wb_canonical_stage_lines_json_chk
alter table private.banking_pay_workbench_canonical_stage_lines add constraint bpay_wb_canonical_stage_lines_json_chk CHECK (jsonb_typeof(source_row_json) = 'object'::text AND jsonb_typeof(economic_key_json) = 'object'::text AND jsonb_typeof(contract_json) = 'object'::text AND row_digest ~ '^[0-9a-f]{32}$'::text);

-- private.banking_pay_workbench_canonical_stage_lines.bpay_wb_canonical_stage_lines_status_chk
alter table private.banking_pay_workbench_canonical_stage_lines add constraint bpay_wb_canonical_stage_lines_status_chk CHECK (stage_status = ANY (ARRAY['STAGED'::text, 'VERIFIED'::text]));

-- private.banking_pay_workbench_economic_build_fact_pages.bpay_wb_economic_build_fact_pages_completed_chk
alter table private.banking_pay_workbench_economic_build_fact_pages add constraint bpay_wb_economic_build_fact_pages_completed_chk CHECK (completed_at_utc >= created_at_utc);

-- private.banking_pay_workbench_economic_build_fact_pages.bpay_wb_economic_build_fact_pages_counts_chk
alter table private.banking_pay_workbench_economic_build_fact_pages add constraint bpay_wb_economic_build_fact_pages_counts_chk CHECK (page_number > 0 AND (expected_source_count IS NULL OR expected_source_count >= 0) AND actual_fact_count >= 0 AND cumulative_fact_count >= actual_fact_count);

-- private.banking_pay_workbench_economic_build_fact_pages.bpay_wb_economic_build_fact_pages_cursor_digest_chk
alter table private.banking_pay_workbench_economic_build_fact_pages add constraint bpay_wb_economic_build_fact_pages_cursor_digest_chk CHECK (jsonb_typeof(cursor_start_json) = 'object'::text AND jsonb_typeof(cursor_end_json) = 'object'::text AND cursor_start_hash ~ '^[0-9a-f]{32}$'::text AND cursor_end_hash ~ '^[0-9a-f]{32}$'::text AND page_digest ~ '^[0-9a-f]{32}$'::text AND cumulative_digest ~ '^[0-9a-f]{32}$'::text);

-- private.banking_pay_workbench_economic_build_fact_pages.bpay_wb_economic_build_fact_pages_final_chk
alter table private.banking_pay_workbench_economic_build_fact_pages add constraint bpay_wb_economic_build_fact_pages_final_chk CHECK (NOT is_family_final OR cursor_end_json @> '{"terminal": true}'::jsonb);

-- private.banking_pay_workbench_economic_build_fact_pages.bpay_wb_economic_build_fact_pages_status_chk
alter table private.banking_pay_workbench_economic_build_fact_pages add constraint bpay_wb_economic_build_fact_pages_status_chk CHECK (status = ANY (ARRAY['COMPLETED'::text, 'CONFLICTED'::text]));

-- private.banking_pay_workbench_economic_build_fact_pages.bpay_wb_economic_build_fact_pages_unit_chk
alter table private.banking_pay_workbench_economic_build_fact_pages add constraint bpay_wb_economic_build_fact_pages_unit_chk CHECK ((fact_family = ANY (ARRAY['FINANCE_ITEM_AUTHORITY'::text, 'RESERVATION_COMPONENT'::text, 'FINANCE_CASE_IDENTITY'::text, 'FINANCE_COMPONENT_IDENTITY'::text, 'PROTECTION_EVIDENCE'::text, 'ALLOCATION_INPUT'::text])) AND dependency_unit_key = 'GLOBAL'::text OR (fact_family = ANY (ARRAY['FROZEN_SETTLED_COMPONENT'::text, 'LIVE_ENTITLEMENT_INPUT'::text, 'ENTITLEMENT_COMPONENT'::text, 'PAY_STATE_FALLBACK'::text, 'PAYEE_BASELINE_INPUT'::text, 'CANONICAL_INPUT'::text])) AND btrim(dependency_unit_key) <> ''::text AND dependency_unit_key <> 'GLOBAL'::text);

-- private.banking_pay_workbench_economic_build_facts.bpay_wb_economic_build_facts_authority_chk
alter table private.banking_pay_workbench_economic_build_facts add constraint bpay_wb_economic_build_facts_authority_chk CHECK ((fact_family <> 'FINANCE_ITEM_AUTHORITY'::text OR source_id IS NOT NULL) AND (fact_family <> 'FROZEN_SETTLED_COMPONENT'::text OR economic_key_type IS NOT NULL AND (amount_ex_vat IS NOT NULL OR amount_inc_vat IS NOT NULL)) AND (fact_family <> 'ENTITLEMENT_COMPONENT'::text OR economic_key_type IS NOT NULL AND (truth_ex_vat IS NOT NULL OR truth_inc_vat IS NOT NULL) AND (baseline_ex_vat IS NOT NULL OR baseline_inc_vat IS NOT NULL)) AND (fact_family <> 'RESERVATION_COMPONENT'::text OR reservation_id IS NOT NULL AND economic_key_type IS NOT NULL AND reserved_source_amount IS NOT NULL) AND (fact_family <> 'FINANCE_CASE_IDENTITY'::text OR finance_case_id IS NOT NULL) AND (fact_family <> 'FINANCE_COMPONENT_IDENTITY'::text OR finance_case_id IS NOT NULL AND finance_component_id IS NOT NULL) AND (fact_family <> 'PROTECTION_EVIDENCE'::text OR finance_case_id IS NOT NULL OR finance_component_id IS NOT NULL OR reservation_id IS NOT NULL OR source_id IS NOT NULL) AND (fact_family <> 'PAYEE_BASELINE_INPUT'::text OR economic_key_type IS NOT NULL AND (truth_ex_vat IS NOT NULL OR truth_inc_vat IS NOT NULL OR baseline_ex_vat IS NOT NULL OR baseline_inc_vat IS NOT NULL)) AND (fact_family <> 'ALLOCATION_INPUT'::text OR economic_key_type IS NOT NULL AND (amount_ex_vat IS NOT NULL OR amount_inc_vat IS NOT NULL OR reserved_source_amount IS NOT NULL)) AND (fact_family <> 'CANONICAL_INPUT'::text OR economic_key_type IS NOT NULL AND (amount_ex_vat IS NOT NULL OR amount_inc_vat IS NOT NULL OR truth_ex_vat IS NOT NULL OR truth_inc_vat IS NOT NULL OR baseline_ex_vat IS NOT NULL OR baseline_inc_vat IS NOT NULL)) AND (fact_family <> 'EXPECTED_FINANCE_EFFECT'::text OR source_id IS NOT NULL AND (source_relation = ANY (ARRAY['pay_advances'::text, 'pay_finance_case_components'::text, 'pay_finance_case_events'::text])) AND source_payload_json ? 'operation'::text AND source_payload_json ? 'expected_before_digest'::text AND source_payload_json ? 'expected_after_digest'::text));

-- private.banking_pay_workbench_economic_build_facts.bpay_wb_economic_build_facts_economic_key_chk
alter table private.banking_pay_workbench_economic_build_facts add constraint bpay_wb_economic_build_facts_economic_key_chk CHECK (economic_key_type IS NULL OR (economic_key_type = ANY (ARRAY['TS_DAY'::text, 'TS_TOTAL'::text, 'ADDITIONAL_CODE'::text, 'ADJUSTMENT_CODE'::text, 'EXPENSE_CODE'::text, 'MANUAL_CARRY_FORWARD'::text])) AND NULLIF(btrim(economic_key_value), ''::text) IS NOT NULL AND (economic_key_type <> 'TS_DAY'::text OR economic_key_value ~ '^\d{4}-\d{2}-\d{2}$'::text AND to_char(economic_key_value::date::timestamp with time zone, 'YYYY-MM-DD'::text) = economic_key_value));

-- private.banking_pay_workbench_economic_build_facts.bpay_wb_economic_build_facts_edge_chk
alter table private.banking_pay_workbench_economic_build_facts add constraint bpay_wb_economic_build_facts_edge_chk CHECK (fact_family <> 'DEPENDENCY_EDGE'::text OR NULLIF(btrim(edge_kind), ''::text) IS NOT NULL AND edge_from_timesheet_id IS NOT NULL AND edge_to_timesheet_id IS NOT NULL AND edge_source_id IS NOT NULL AND cardinality(subject_timesheet_ids) > 0);

-- private.banking_pay_workbench_economic_build_facts.bpay_wb_economic_build_facts_family_chk
alter table private.banking_pay_workbench_economic_build_facts add constraint bpay_wb_economic_build_facts_family_chk CHECK (fact_family = ANY (ARRAY['DEPENDENCY_EDGE'::text, 'FINANCE_ITEM_AUTHORITY'::text, 'FROZEN_SETTLED_COMPONENT'::text, 'LIVE_ENTITLEMENT_INPUT'::text, 'ENTITLEMENT_COMPONENT'::text, 'RESERVATION_COMPONENT'::text, 'PAY_STATE_FALLBACK'::text, 'FINANCE_CASE_IDENTITY'::text, 'FINANCE_COMPONENT_IDENTITY'::text, 'PROTECTION_EVIDENCE'::text, 'PAYEE_BASELINE_INPUT'::text, 'ALLOCATION_INPUT'::text, 'CANONICAL_INPUT'::text, 'EXPECTED_FINANCE_EFFECT'::text]));

-- private.banking_pay_workbench_economic_build_facts.bpay_wb_economic_build_facts_payload_chk
alter table private.banking_pay_workbench_economic_build_facts add constraint bpay_wb_economic_build_facts_payload_chk CHECK (payload_schema_version > 0 AND NULLIF(btrim(natural_key), ''::text) IS NOT NULL AND NULLIF(btrim(source_relation), ''::text) IS NOT NULL AND jsonb_typeof(source_payload_json) = 'object'::text AND jsonb_typeof(diagnostic_json) = 'object'::text AND financial_digest ~ '^[0-9a-f]{32}$'::text AND (source_ordinal IS NULL OR source_ordinal > 0));

-- private.banking_pay_workbench_economic_build_facts.bpay_wb_economic_build_facts_subjects_chk
alter table private.banking_pay_workbench_economic_build_facts add constraint bpay_wb_economic_build_facts_subjects_chk CHECK (array_position(subject_timesheet_ids, NULL::uuid) IS NULL);

-- private.banking_pay_workbench_economic_build_facts.bpay_wb_economic_build_facts_unit_chk
alter table private.banking_pay_workbench_economic_build_facts add constraint bpay_wb_economic_build_facts_unit_chk CHECK (fact_family = 'DEPENDENCY_EDGE'::text AND (dependency_unit_key IS NULL OR dependency_unit_key <> 'GLOBAL'::text AND btrim(dependency_unit_key) <> ''::text) OR (fact_family = ANY (ARRAY['FINANCE_ITEM_AUTHORITY'::text, 'RESERVATION_COMPONENT'::text, 'FINANCE_CASE_IDENTITY'::text, 'FINANCE_COMPONENT_IDENTITY'::text, 'PROTECTION_EVIDENCE'::text, 'ALLOCATION_INPUT'::text, 'EXPECTED_FINANCE_EFFECT'::text])) AND dependency_unit_key = 'GLOBAL'::text OR (fact_family = ANY (ARRAY['FROZEN_SETTLED_COMPONENT'::text, 'LIVE_ENTITLEMENT_INPUT'::text, 'ENTITLEMENT_COMPONENT'::text, 'PAY_STATE_FALLBACK'::text, 'PAYEE_BASELINE_INPUT'::text, 'CANONICAL_INPUT'::text])) AND NULLIF(btrim(dependency_unit_key), ''::text) IS NOT NULL AND dependency_unit_key <> 'GLOBAL'::text);

-- private.banking_pay_workbench_economic_build_scope.bpay_wb_economic_build_scope_arrays_chk
alter table private.banking_pay_workbench_economic_build_scope add constraint bpay_wb_economic_build_scope_arrays_chk CHECK (array_position(seed_reasons, NULL::text) IS NULL AND array_position(dependency_reasons, NULL::text) IS NULL AND array_position(required_fact_families, NULL::text) IS NULL AND array_position(completed_fact_families, NULL::text) IS NULL);

-- private.banking_pay_workbench_economic_build_scope.bpay_wb_economic_build_scope_counts_chk
alter table private.banking_pay_workbench_economic_build_scope add constraint bpay_wb_economic_build_scope_counts_chk CHECK (captured_dirty_generation >= 0 AND closure_family_ordinal >= 1 AND closure_processed_edge_count >= 0 AND closure_processed_emission_count >= 0 AND fact_row_count >= 0);

-- private.banking_pay_workbench_economic_build_scope.bpay_wb_economic_build_scope_expanded_chk
alter table private.banking_pay_workbench_economic_build_scope add constraint bpay_wb_economic_build_scope_expanded_chk CHECK (closure_status <> 'EXPANDED'::text OR closure_family_ordinal = 11 AND stable_ordinal IS NULL AND dependency_unit_key IS NULL AND dependency_unit_digest IS NULL AND captured_input_fingerprint IS NULL AND seal_prepared_at_utc IS NULL AND cardinality(completed_fact_families) = 0 AND fact_row_count = 0 AND fact_digest IS NULL);

-- private.banking_pay_workbench_economic_build_scope.bpay_wb_economic_build_scope_fact_completion_chk
alter table private.banking_pay_workbench_economic_build_scope add constraint bpay_wb_economic_build_scope_fact_completion_chk CHECK (completed_fact_families <@ required_fact_families AND (closure_status = 'SEALED'::text OR cardinality(completed_fact_families) = 0 AND fact_row_count = 0 AND fact_digest IS NULL));

-- private.banking_pay_workbench_economic_build_scope.bpay_wb_economic_build_scope_family_ordinal_chk
alter table private.banking_pay_workbench_economic_build_scope add constraint bpay_wb_economic_build_scope_family_ordinal_chk CHECK (closure_family_ordinal >= 1 AND closure_family_ordinal <= 11);

-- private.banking_pay_workbench_economic_build_scope.bpay_wb_economic_build_scope_identity_chk
alter table private.banking_pay_workbench_economic_build_scope add constraint bpay_wb_economic_build_scope_identity_chk CHECK ((dependency_unit_key IS NULL OR btrim(dependency_unit_key) <> ''::text AND dependency_unit_key <> 'GLOBAL'::text) AND (dependency_unit_digest IS NULL OR dependency_unit_digest ~ '^[0-9a-f]{32}$'::text) AND (captured_input_fingerprint IS NULL OR captured_input_fingerprint ~ '^[0-9a-f]{32}$'::text) AND (fact_digest IS NULL OR fact_digest ~ '^[0-9a-f]{32}$'::text) AND updated_at_utc >= created_at_utc);

-- private.banking_pay_workbench_economic_build_scope.bpay_wb_economic_build_scope_pending_chk
alter table private.banking_pay_workbench_economic_build_scope add constraint bpay_wb_economic_build_scope_pending_chk CHECK (closure_status <> 'PENDING'::text OR stable_ordinal IS NULL AND dependency_unit_anchor_timesheet_id IS NULL AND dependency_unit_key IS NULL AND dependency_unit_digest IS NULL AND captured_input_fingerprint IS NULL AND seal_prepared_at_utc IS NULL AND cardinality(completed_fact_families) = 0 AND fact_row_count = 0 AND fact_digest IS NULL);

-- private.banking_pay_workbench_economic_build_scope.bpay_wb_economic_build_scope_sealed_chk
alter table private.banking_pay_workbench_economic_build_scope add constraint bpay_wb_economic_build_scope_sealed_chk CHECK (closure_status <> 'SEALED'::text OR stable_ordinal > 0 AND dependency_unit_anchor_timesheet_id IS NOT NULL AND NULLIF(btrim(dependency_unit_key), ''::text) IS NOT NULL AND dependency_unit_digest ~ '^[0-9a-f]{32}$'::text AND captured_input_fingerprint ~ '^[0-9a-f]{32}$'::text AND seal_prepared_at_utc IS NOT NULL);

-- private.banking_pay_workbench_economic_build_scope.bpay_wb_economic_build_scope_sealing_chk
alter table private.banking_pay_workbench_economic_build_scope add constraint bpay_wb_economic_build_scope_sealing_chk CHECK (closure_status <> 'SEALING'::text OR dependency_unit_anchor_timesheet_id IS NOT NULL AND stable_ordinal IS NULL AND captured_input_fingerprint IS NULL AND seal_prepared_at_utc IS NULL AND cardinality(completed_fact_families) = 0 AND fact_row_count = 0 AND fact_digest IS NULL AND (dependency_unit_digest IS NULL OR timesheet_id = dependency_unit_anchor_timesheet_id));

-- private.banking_pay_workbench_economic_build_scope.bpay_wb_economic_build_scope_status_chk
alter table private.banking_pay_workbench_economic_build_scope add constraint bpay_wb_economic_build_scope_status_chk CHECK (closure_status = ANY (ARRAY['PENDING'::text, 'EXPANDED'::text, 'SEALING'::text, 'SEALED'::text]));

-- private.banking_pay_workbench_economic_builds.bpay_wb_economic_build_authority_fingerprint_ck
alter table private.banking_pay_workbench_economic_builds add constraint bpay_wb_economic_build_authority_fingerprint_ck CHECK (authority_fingerprint_version IS NULL AND authority_fingerprint IS NULL OR authority_fingerprint_version > 0 AND authority_fingerprint ~ '^[0-9a-f]{64}$'::text) NOT VALID;

-- private.banking_pay_workbench_economic_builds.bpay_wb_economic_builds_complete_chk
alter table private.banking_pay_workbench_economic_builds add constraint bpay_wb_economic_builds_complete_chk CHECK (status <> 'COMPLETE'::text OR private_stage = 'COMPLETE'::text AND canonical_digest IS NOT NULL AND attestation_json <> '{}'::jsonb AND completed_at_utc IS NOT NULL);

-- private.banking_pay_workbench_economic_builds.bpay_wb_economic_builds_counts_chk
alter table private.banking_pay_workbench_economic_builds add constraint bpay_wb_economic_builds_counts_chk CHECK (session_version > 0 AND stage_version > 0 AND captured_candidate_generation >= 0 AND source_change_seq >= 0 AND seed_scope_count >= 0 AND scope_count >= 0 AND dependency_node_count >= 0 AND dependency_edge_count >= 0 AND tagged_edge_count >= 0 AND unit_count >= 0 AND row_seal_count >= 0 AND last_stable_ordinal >= 0 AND fact_count >= 0 AND canonical_count >= 0 AND (envelope_version IS NULL OR envelope_version > 0));

-- private.banking_pay_workbench_economic_builds.bpay_wb_economic_builds_dependency_seal_chk
alter table private.banking_pay_workbench_economic_builds add constraint bpay_wb_economic_builds_dependency_seal_chk CHECK (dependency_closure_sealed_at_utc IS NULL OR seed_scope_sealed_at_utc IS NOT NULL AND dependency_edge_stream_complete AND edge_tag_stream_complete AND tagged_edge_count = dependency_edge_count AND unit_digest IS NOT NULL AND edge_tag_digest IS NOT NULL AND scope_digest IS NOT NULL AND dependency_digest IS NOT NULL AND sealed_fingerprint_digest IS NOT NULL AND row_seal_count = scope_count AND last_stable_ordinal = row_seal_count AND closure_cursor_json @> '{"terminal": true, "seal_phase": "COMPLETE"}'::jsonb);

-- private.banking_pay_workbench_economic_builds.bpay_wb_economic_builds_digest_chk
alter table private.banking_pay_workbench_economic_builds add constraint bpay_wb_economic_builds_digest_chk CHECK ((seed_scope_digest IS NULL OR seed_scope_digest ~ '^[0-9a-f]{32}$'::text) AND (dependency_edge_stream_digest IS NULL OR dependency_edge_stream_digest ~ '^[0-9a-f]{32}$'::text) AND (edge_tag_digest IS NULL OR edge_tag_digest ~ '^[0-9a-f]{32}$'::text) AND (unit_digest IS NULL OR unit_digest ~ '^[0-9a-f]{32}$'::text) AND (sealed_fingerprint_digest IS NULL OR sealed_fingerprint_digest ~ '^[0-9a-f]{32}$'::text) AND (scope_digest IS NULL OR scope_digest ~ '^[0-9a-f]{32}$'::text) AND (dependency_digest IS NULL OR dependency_digest ~ '^[0-9a-f]{32}$'::text) AND (pre_sync_digest IS NULL OR pre_sync_digest ~ '^[0-9a-f]{32}$'::text) AND (post_sync_digest IS NULL OR post_sync_digest ~ '^[0-9a-f]{32}$'::text) AND (canonical_digest IS NULL OR canonical_digest ~ '^[0-9a-f]{32}$'::text));

-- private.banking_pay_workbench_economic_builds.bpay_wb_economic_builds_job_identity_chk
alter table private.banking_pay_workbench_economic_builds add constraint bpay_wb_economic_builds_job_identity_chk CHECK ((status = ANY (ARRAY['COMPLETE'::text, 'OBSOLETE'::text, 'FAILED'::text, 'CLEANING'::text])) OR source_job_id IS NOT NULL);

-- private.banking_pay_workbench_economic_builds.bpay_wb_economic_builds_json_chk
alter table private.banking_pay_workbench_economic_builds add constraint bpay_wb_economic_builds_json_chk CHECK (jsonb_typeof(scope_cursor_json) = 'object'::text AND jsonb_typeof(closure_cursor_json) = 'object'::text AND jsonb_typeof(fact_cursor_json) = 'object'::text AND jsonb_typeof(publication_cursor_json) = 'object'::text AND jsonb_typeof(cleanup_cursor_json) = 'object'::text AND jsonb_typeof(dependency_edge_stream_terminal_key_json) = 'object'::text AND jsonb_typeof(edge_tag_stream_terminal_key_json) = 'object'::text AND jsonb_typeof(complexity_vector_json) = 'object'::text AND jsonb_typeof(envelope_snapshot_json) = 'object'::text AND jsonb_typeof(envelope_evidence_json) = 'object'::text AND jsonb_typeof(attestation_json) = 'object'::text AND jsonb_typeof(failure_json) = 'object'::text);

-- private.banking_pay_workbench_economic_builds.bpay_wb_economic_builds_lifecycle_chk
alter table private.banking_pay_workbench_economic_builds add constraint bpay_wb_economic_builds_lifecycle_chk CHECK (updated_at_utc >= created_at_utc AND (ready_at_utc IS NULL OR ready_at_utc >= created_at_utc) AND (reconciled_at_utc IS NULL OR reconciled_at_utc >= COALESCE(ready_at_utc, created_at_utc)) AND (completed_at_utc IS NULL OR completed_at_utc >= COALESCE(reconciled_at_utc, created_at_utc)) AND (obsolete_at_utc IS NULL OR obsolete_at_utc >= created_at_utc) AND (failed_at_utc IS NULL OR failed_at_utc >= created_at_utc) AND (status <> 'READY_FOR_RECONCILIATION'::text OR ready_at_utc IS NOT NULL) AND ((status <> ALL (ARRAY['RECONCILED'::text, 'PUBLISHING'::text, 'COMPLETE'::text])) OR reconciled_at_utc IS NOT NULL) AND (status <> 'COMPLETE'::text OR completed_at_utc IS NOT NULL) AND (status <> 'OBSOLETE'::text OR obsolete_at_utc IS NOT NULL) AND (status <> 'FAILED'::text OR failed_at_utc IS NOT NULL) AND (status <> 'CLEANING'::text OR cleanup_not_before_utc IS NOT NULL));

-- private.banking_pay_workbench_economic_builds.bpay_wb_economic_builds_scale_block_chk
alter table private.banking_pay_workbench_economic_builds add constraint bpay_wb_economic_builds_scale_block_chk CHECK (status <> 'BLOCKED_UNVALIDATED_RECONCILIATION_SCALE'::text OR complexity_vector_json <> '{}'::jsonb AND envelope_version IS NOT NULL AND envelope_snapshot_json <> '{}'::jsonb AND reconciled_at_utc IS NULL AND completed_at_utc IS NULL AND canonical_count = 0 AND canonical_digest IS NULL);

-- private.banking_pay_workbench_economic_builds.bpay_wb_economic_builds_seed_seal_chk
alter table private.banking_pay_workbench_economic_builds add constraint bpay_wb_economic_builds_seed_seal_chk CHECK (seed_scope_sealed_at_utc IS NULL AND seed_scope_digest IS NULL OR seed_scope_sealed_at_utc IS NOT NULL AND seed_scope_digest IS NOT NULL AND seed_scope_count >= 0 AND scope_cursor_json @> '{"terminal": true}'::jsonb);

-- private.banking_pay_workbench_economic_builds.bpay_wb_economic_builds_stage_chk
alter table private.banking_pay_workbench_economic_builds add constraint bpay_wb_economic_builds_stage_chk CHECK (private_stage = ANY (ARRAY['PREPARE_SCOPE'::text, 'DEPENDENCY_CLOSURE'::text, 'WORKSPACE_FACT'::text, 'RECONCILE_EXECUTE'::text, 'SOURCE_PUBLISH'::text, 'BOOTSTRAP_DISCOVERY'::text, 'BUILD_CLEANUP'::text, 'COMPLETE'::text]));

-- private.banking_pay_workbench_economic_builds.bpay_wb_economic_builds_status_chk
alter table private.banking_pay_workbench_economic_builds add constraint bpay_wb_economic_builds_status_chk CHECK (status = ANY (ARRAY['COLLECTING'::text, 'READY_FOR_RECONCILIATION'::text, 'RECONCILING'::text, 'RECONCILED'::text, 'PUBLISHING'::text, 'COMPLETE'::text, 'OBSOLETE'::text, 'FAILED'::text, 'BLOCKED_UNVALIDATED_RECONCILIATION_SCALE'::text, 'CLEANING'::text]));

-- private.banking_pay_workbench_queue_scan_state.bpay_wb_queue_scan_state_cursor_chk
alter table private.banking_pay_workbench_queue_scan_state add constraint bpay_wb_queue_scan_state_cursor_chk CHECK (cursor_object_id IS NULL AND cursor_generation IS NULL AND cursor_chain_rank IS NULL AND cursor_priority IS NULL AND cursor_due_at IS NULL AND cursor_created_at IS NULL OR scan_kind = 'CLAIM'::text AND cursor_object_id IS NOT NULL AND cursor_generation IS NOT NULL AND cursor_generation >= 0 AND (cursor_chain_rank = ANY (ARRAY[0, 1])) AND cursor_priority IS NOT NULL AND cursor_due_at IS NOT NULL AND cursor_created_at IS NOT NULL OR scan_kind = 'RECOVERY'::text AND cursor_object_id IS NOT NULL AND cursor_generation IS NOT NULL AND cursor_generation >= 0 AND cursor_chain_rank IS NULL AND cursor_priority IS NULL AND cursor_due_at IS NOT NULL AND cursor_created_at IS NULL);

-- private.banking_pay_workbench_queue_scan_state.bpay_wb_queue_scan_state_identity_chk
alter table private.banking_pay_workbench_queue_scan_state add constraint bpay_wb_queue_scan_state_identity_chk CHECK (NULLIF(btrim(lane_identity), ''::text) IS NOT NULL AND char_length(lane_identity) <= 200 AND (scan_kind = ANY (ARRAY['CLAIM'::text, 'RECOVERY'::text])) AND NULLIF(btrim(scan_scope_key), ''::text) IS NOT NULL AND char_length(scan_scope_key) <= 160);

-- private.banking_pay_workbench_queue_scan_state.bpay_wb_queue_scan_state_sweep_chk
alter table private.banking_pay_workbench_queue_scan_state add constraint bpay_wb_queue_scan_state_sweep_chk CHECK (sweep_generation >= 0);

-- private.banking_pay_workbench_stage_attempts.bpay_wb_stage_attempts_counts_chk
alter table private.banking_pay_workbench_stage_attempts add constraint bpay_wb_stage_attempts_counts_chk CHECK (attempt_number > 0 AND execution_profile_version > 0 AND captured_candidate_generation >= 0 AND captured_source_change_seq >= 0);

-- private.banking_pay_workbench_stage_attempts.bpay_wb_stage_attempts_lease_chk
alter table private.banking_pay_workbench_stage_attempts add constraint bpay_wb_stage_attempts_lease_chk CHECK (lease_expires_at_utc > started_at_utc AND created_at_utc <= started_at_utc AND updated_at_utc >= created_at_utc);

-- private.banking_pay_workbench_stage_attempts.bpay_wb_stage_attempts_outcome_chk
alter table private.banking_pay_workbench_stage_attempts add constraint bpay_wb_stage_attempts_outcome_chk CHECK (attempt_status = 'STARTED'::text AND completed_at_utc IS NULL AND failed_at_utc IS NULL AND expired_at_utc IS NULL AND obsolete_at_utc IS NULL OR attempt_status = 'COMPLETED'::text AND completed_at_utc IS NOT NULL AND failed_at_utc IS NULL AND expired_at_utc IS NULL AND obsolete_at_utc IS NULL OR attempt_status = 'FAILED'::text AND completed_at_utc IS NULL AND failed_at_utc IS NOT NULL AND expired_at_utc IS NULL AND obsolete_at_utc IS NULL OR attempt_status = 'EXPIRED'::text AND completed_at_utc IS NULL AND failed_at_utc IS NULL AND expired_at_utc IS NOT NULL AND obsolete_at_utc IS NULL OR attempt_status = 'OBSOLETE'::text AND completed_at_utc IS NULL AND failed_at_utc IS NULL AND expired_at_utc IS NULL AND obsolete_at_utc IS NOT NULL);

-- private.banking_pay_workbench_stage_attempts.bpay_wb_stage_attempts_result_chk
alter table private.banking_pay_workbench_stage_attempts add constraint bpay_wb_stage_attempts_result_chk CHECK (jsonb_typeof(error_json) = 'object'::text AND (result_digest IS NULL OR result_digest ~ '^[0-9a-f]{32}$'::text) AND (result_code IS NULL OR NULLIF(btrim(result_code), ''::text) IS NOT NULL) AND (error_class IS NULL OR error_class ~ '^[A-Z][A-Z0-9_]*$'::text));

-- private.banking_pay_workbench_stage_attempts.bpay_wb_stage_attempts_status_stage_chk
alter table private.banking_pay_workbench_stage_attempts add constraint bpay_wb_stage_attempts_status_stage_chk CHECK ((attempt_status = ANY (ARRAY['STARTED'::text, 'COMPLETED'::text, 'FAILED'::text, 'EXPIRED'::text, 'OBSOLETE'::text])) AND (private_stage = ANY (ARRAY['PREPARE_SCOPE'::text, 'DEPENDENCY_CLOSURE'::text, 'WORKSPACE_FACT'::text, 'RECONCILE_EXECUTE'::text, 'SOURCE_PUBLISH'::text, 'BOOTSTRAP_DISCOVERY'::text, 'BUILD_CLEANUP'::text, 'COMPLETE'::text])) AND btrim(worker_id) <> ''::text AND btrim(lane_identity) <> ''::text);

-- private.banking_pay_workbench_timesheet_scope_state.bpay_wb_timesheet_scope_state_evaluated_chk
alter table private.banking_pay_workbench_timesheet_scope_state add constraint bpay_wb_timesheet_scope_state_evaluated_chk CHECK (economic_state = 'DIRTY'::text OR evaluated_generation = dirty_generation AND current_input_fingerprint IS NOT NULL AND evaluated_input_fingerprint = current_input_fingerprint AND last_evaluated_at_utc IS NOT NULL);

-- private.banking_pay_workbench_timesheet_scope_state.bpay_wb_timesheet_scope_state_fingerprint_chk
alter table private.banking_pay_workbench_timesheet_scope_state add constraint bpay_wb_timesheet_scope_state_fingerprint_chk CHECK ((current_input_fingerprint IS NULL OR current_input_fingerprint ~ '^[0-9a-f]{32}$'::text) AND (evaluated_input_fingerprint IS NULL OR evaluated_input_fingerprint ~ '^[0-9a-f]{32}$'::text));

-- private.banking_pay_workbench_timesheet_scope_state.bpay_wb_timesheet_scope_state_generation_chk
alter table private.banking_pay_workbench_timesheet_scope_state add constraint bpay_wb_timesheet_scope_state_generation_chk CHECK (dirty_generation >= 0 AND (evaluated_generation IS NULL OR evaluated_generation >= 0 AND evaluated_generation <= dirty_generation));

-- private.banking_pay_workbench_timesheet_scope_state.bpay_wb_timesheet_scope_state_lifecycle_chk
alter table private.banking_pay_workbench_timesheet_scope_state add constraint bpay_wb_timesheet_scope_state_lifecycle_chk CHECK (btrim(last_dirty_reason) <> ''::text AND (economic_state = 'CLOSED'::text) = (closed_at_utc IS NOT NULL) AND last_dirtied_at_utc >= registered_at_utc AND updated_at_utc >= registered_at_utc AND (last_evaluated_at_utc IS NULL OR last_evaluated_at_utc >= registered_at_utc) AND (closed_at_utc IS NULL OR closed_at_utc >= registered_at_utc));

-- private.banking_pay_workbench_timesheet_scope_state.bpay_wb_timesheet_scope_state_status_chk
alter table private.banking_pay_workbench_timesheet_scope_state add constraint bpay_wb_timesheet_scope_state_status_chk CHECK (economic_state = ANY (ARRAY['DIRTY'::text, 'LIVE'::text, 'CLOSED'::text]));

-- private.candidate_daily_authority_scopes.candidate_daily_authority_scopes_environment_ck
alter table private.candidate_daily_authority_scopes add constraint candidate_daily_authority_scopes_environment_ck CHECK (environment = ANY (ARRAY['TEST'::text, 'LIVE'::text]));

-- private.candidate_daily_authority_scopes.candidate_daily_authority_scopes_mode_ck
alter table private.candidate_daily_authority_scopes add constraint candidate_daily_authority_scopes_mode_ck CHECK (authority_mode = ANY (ARRAY['GOOGLE_PRIMARY'::text, 'ROLLBACK_PENDING'::text, 'SUPABASE_PRIMARY'::text]));

-- private.candidate_daily_authority_scopes.candidate_daily_authority_scopes_version_ck
alter table private.candidate_daily_authority_scopes add constraint candidate_daily_authority_scopes_version_ck CHECK (canonical_version >= 0);

-- private.candidate_daily_authority_transitions.candidate_daily_authority_transitions_environment_ck
alter table private.candidate_daily_authority_transitions add constraint candidate_daily_authority_transitions_environment_ck CHECK (environment = ANY (ARRAY['TEST'::text, 'LIVE'::text]));

-- private.candidate_daily_authority_transitions.candidate_daily_authority_transitions_evidence_ck
alter table private.candidate_daily_authority_transitions add constraint candidate_daily_authority_transitions_evidence_ck CHECK (evidence_sha256 ~ '^[a-f0-9]{64}$'::text);

-- private.candidate_daily_authority_transitions.candidate_daily_authority_transitions_inflight_ck
alter table private.candidate_daily_authority_transitions add constraint candidate_daily_authority_transitions_inflight_ck CHECK (in_flight_disposition = ANY (ARRAY['DRAINED'::text, 'CANCELLED'::text, 'RECONCILED'::text, 'NONE'::text]));

-- private.candidate_daily_authority_transitions.candidate_daily_authority_transitions_modes_ck
alter table private.candidate_daily_authority_transitions add constraint candidate_daily_authority_transitions_modes_ck CHECK ((prior_authority_mode = ANY (ARRAY['GOOGLE_PRIMARY'::text, 'ROLLBACK_PENDING'::text, 'SUPABASE_PRIMARY'::text])) AND (new_authority_mode = ANY (ARRAY['GOOGLE_PRIMARY'::text, 'ROLLBACK_PENDING'::text, 'SUPABASE_PRIMARY'::text])));

-- private.candidate_daily_authority_transitions.candidate_daily_authority_transitions_outcome_ck
alter table private.candidate_daily_authority_transitions add constraint candidate_daily_authority_transitions_outcome_ck CHECK (outcome = ANY (ARRAY['COMMITTED'::text, 'REJECTED'::text]));

-- private.candidate_daily_authority_transitions.candidate_daily_authority_transitions_reason_ck
alter table private.candidate_daily_authority_transitions add constraint candidate_daily_authority_transitions_reason_ck CHECK (length(btrim(reason)) >= 1 AND length(btrim(reason)) <= 500);

-- private.candidate_daily_authority_transitions.candidate_daily_authority_transitions_sync_ck
alter table private.candidate_daily_authority_transitions add constraint candidate_daily_authority_transitions_sync_ck CHECK (jsonb_typeof(sync_snapshot_json) = 'object'::text);

-- private.candidate_daily_authority_transitions.candidate_daily_authority_transitions_version_ck
alter table private.candidate_daily_authority_transitions add constraint candidate_daily_authority_transitions_version_ck CHECK (canonical_version_snapshot >= 0);

-- private.candidate_daily_batch_receipts.candidate_daily_batch_receipts_actor_ck
alter table private.candidate_daily_batch_receipts add constraint candidate_daily_batch_receipts_actor_ck CHECK (actor_class = ANY (ARRAY['LEGACY_ADAPTER'::text, 'SIGNED_SYSTEM'::text, 'OFFICE_ADMIN'::text]));

-- private.candidate_daily_batch_receipts.candidate_daily_batch_receipts_environment_ck
alter table private.candidate_daily_batch_receipts add constraint candidate_daily_batch_receipts_environment_ck CHECK (environment = ANY (ARRAY['TEST'::text, 'LIVE'::text]));

-- private.candidate_daily_batch_receipts.candidate_daily_batch_receipts_hash_ck
alter table private.candidate_daily_batch_receipts add constraint candidate_daily_batch_receipts_hash_ck CHECK (request_hash ~ '^[a-f0-9]{64}$'::text);

-- private.candidate_daily_batch_receipts.candidate_daily_batch_receipts_item_receipts_ck
alter table private.candidate_daily_batch_receipts add constraint candidate_daily_batch_receipts_item_receipts_ck CHECK (jsonb_typeof(item_receipt_ids_json) = 'array'::text);

-- private.candidate_daily_batch_receipts.candidate_daily_batch_receipts_items_ck
alter table private.candidate_daily_batch_receipts add constraint candidate_daily_batch_receipts_items_ck CHECK (jsonb_typeof(item_keys_json) = 'array'::text AND jsonb_array_length(item_keys_json) = item_count AND item_count >= 1 AND item_count <= 100);

-- private.candidate_daily_batch_receipts.candidate_daily_batch_receipts_key_ck
alter table private.candidate_daily_batch_receipts add constraint candidate_daily_batch_receipts_key_ck CHECK (length(idempotency_key) >= 16 AND length(idempotency_key) <= 160);

-- private.candidate_daily_batch_receipts.candidate_daily_batch_receipts_operation_ck
alter table private.candidate_daily_batch_receipts add constraint candidate_daily_batch_receipts_operation_ck CHECK (operation_class = ANY (ARRAY['ROTA_GENERATION_PUBLISH'::text, 'SHEET_EDIT_INGEST'::text, 'PROJECTION_CLAIM'::text, 'PROJECTION_COMPLETE'::text, 'RECONCILIATION'::text, 'AUTHORITY_TRANSITION'::text]));

-- private.candidate_daily_batch_receipts.candidate_daily_batch_receipts_state_ck
alter table private.candidate_daily_batch_receipts add constraint candidate_daily_batch_receipts_state_ck CHECK (state = ANY (ARRAY['IN_PROGRESS'::text, 'COMPLETED'::text, 'FAILED_FINAL'::text]));

-- private.candidate_daily_batch_receipts.candidate_daily_batch_receipts_terminal_ck
alter table private.candidate_daily_batch_receipts add constraint candidate_daily_batch_receipts_terminal_ck CHECK (state = 'IN_PROGRESS'::text AND terminal_response_body IS NULL AND terminal_response_sha256 IS NULL AND completed_at_utc IS NULL OR (state = ANY (ARRAY['COMPLETED'::text, 'FAILED_FINAL'::text])) AND terminal_response_body IS NOT NULL AND terminal_response_sha256 ~ '^[a-f0-9]{64}$'::text AND completed_at_utc IS NOT NULL);

-- private.candidate_daily_entitlements.candidate_daily_entitlements_environment_ck
alter table private.candidate_daily_entitlements add constraint candidate_daily_entitlements_environment_ck CHECK (environment = ANY (ARRAY['TEST'::text, 'LIVE'::text]));

-- private.candidate_daily_entitlements.candidate_daily_entitlements_evidence_ck
alter table private.candidate_daily_entitlements add constraint candidate_daily_entitlements_evidence_ck CHECK (evidence_sha256 ~ '^[a-f0-9]{64}$'::text);

-- private.candidate_daily_entitlements.candidate_daily_entitlements_reason_ck
alter table private.candidate_daily_entitlements add constraint candidate_daily_entitlements_reason_ck CHECK (length(btrim(reason)) >= 1 AND length(btrim(reason)) <= 500);

-- private.candidate_daily_entitlements.candidate_daily_entitlements_validity_ck
alter table private.candidate_daily_entitlements add constraint candidate_daily_entitlements_validity_ck CHECK (valid_to_utc IS NULL OR valid_from_utc IS NULL OR valid_to_utc > valid_from_utc);

-- private.candidate_daily_external_effect_receipts.candidate_daily_external_effects_attempt_ck
alter table private.candidate_daily_external_effect_receipts add constraint candidate_daily_external_effects_attempt_ck CHECK (attempt_count >= 1 AND attempt_count <= 100);

-- private.candidate_daily_external_effect_receipts.candidate_daily_external_effects_environment_ck
alter table private.candidate_daily_external_effect_receipts add constraint candidate_daily_external_effects_environment_ck CHECK (environment = ANY (ARRAY['TEST'::text, 'LIVE'::text]));

-- private.candidate_daily_external_effect_receipts.candidate_daily_external_effects_hash_ck
alter table private.candidate_daily_external_effect_receipts add constraint candidate_daily_external_effects_hash_ck CHECK (request_hash ~ '^[a-f0-9]{64}$'::text);

-- private.candidate_daily_external_effect_receipts.candidate_daily_external_effects_idempotency_ck
alter table private.candidate_daily_external_effect_receipts add constraint candidate_daily_external_effects_idempotency_ck CHECK (length(idempotency_key) >= 16 AND length(idempotency_key) <= 160);

-- private.candidate_daily_external_effect_receipts.candidate_daily_external_effects_key_ck
alter table private.candidate_daily_external_effect_receipts add constraint candidate_daily_external_effects_key_ck CHECK (length(effect_key) >= 16 AND length(effect_key) <= 256);

-- private.candidate_daily_external_effect_receipts.candidate_daily_external_effects_operation_ck
alter table private.candidate_daily_external_effect_receipts add constraint candidate_daily_external_effects_operation_ck CHECK (operation = ANY (ARRAY['RUNNING_LATE_SEND'::text, 'CANNOT_ATTEND'::text, 'LEAVE_EARLY'::text, 'DNA'::text, 'MESSAGE_SEEN'::text, 'ESCALATION_STEP'::text, 'ACKNOWLEDGEMENT'::text]));

-- private.candidate_daily_external_effect_receipts.candidate_daily_external_effects_provider_hash_ck
alter table private.candidate_daily_external_effect_receipts add constraint candidate_daily_external_effects_provider_hash_ck CHECK (provider_reference_hash IS NULL OR provider_reference_hash ~ '^[a-f0-9]{64}$'::text);

-- private.candidate_daily_external_effect_receipts.candidate_daily_external_effects_safe_evidence_ck
alter table private.candidate_daily_external_effect_receipts add constraint candidate_daily_external_effects_safe_evidence_ck CHECK (jsonb_typeof(safe_evidence_json) = 'object'::text);

-- private.candidate_daily_external_effect_receipts.candidate_daily_external_effects_state_ck
alter table private.candidate_daily_external_effect_receipts add constraint candidate_daily_external_effects_state_ck CHECK (state = ANY (ARRAY['IN_PROGRESS'::text, 'COMPLETED'::text, 'FAILED_FINAL'::text, 'UNKNOWN'::text]));

-- private.candidate_daily_external_effect_receipts.candidate_daily_external_effects_terminal_ck
alter table private.candidate_daily_external_effect_receipts add constraint candidate_daily_external_effects_terminal_ck CHECK (state = 'IN_PROGRESS'::text AND terminal_result_json IS NULL AND terminal_body_sha256 IS NULL AND completed_at_utc IS NULL OR (state = ANY (ARRAY['COMPLETED'::text, 'FAILED_FINAL'::text, 'UNKNOWN'::text])) AND terminal_result_json IS NOT NULL AND terminal_body_sha256 ~ '^[a-f0-9]{64}$'::text AND completed_at_utc IS NOT NULL);

-- private.candidate_daily_source_links.candidate_daily_source_links_canonical_ck
alter table private.candidate_daily_source_links add constraint candidate_daily_source_links_canonical_ck CHECK (canonicalization_version = 'SOURCE_IDENTITY_V1'::text);

-- private.candidate_daily_source_links.candidate_daily_source_links_environment_ck
alter table private.candidate_daily_source_links add constraint candidate_daily_source_links_environment_ck CHECK (environment = ANY (ARRAY['TEST'::text, 'LIVE'::text]));

-- private.candidate_daily_source_links.candidate_daily_source_links_evidence_ck
alter table private.candidate_daily_source_links add constraint candidate_daily_source_links_evidence_ck CHECK (evidence_sha256 ~ '^[a-f0-9]{64}$'::text);

-- private.candidate_daily_source_links.candidate_daily_source_links_hmac_ck
alter table private.candidate_daily_source_links add constraint candidate_daily_source_links_hmac_ck CHECK (identifier_hmac ~ '^[a-f0-9]{64}$'::text);

-- private.candidate_daily_source_links.candidate_daily_source_links_key_version_ck
alter table private.candidate_daily_source_links add constraint candidate_daily_source_links_key_version_ck CHECK (hmac_key_version > 0);

-- private.candidate_daily_source_links.candidate_daily_source_links_source_ck
alter table private.candidate_daily_source_links add constraint candidate_daily_source_links_source_ck CHECK (source_system = 'GOOGLE_CREDENTIALLY_PUBLIC_ID'::text);

-- private.candidate_daily_source_links.candidate_daily_source_links_state_ck
alter table private.candidate_daily_source_links add constraint candidate_daily_source_links_state_ck CHECK (state = ANY (ARRAY['PRIMARY'::text, 'OVERLAP'::text, 'RETIRED'::text, 'REJECTED'::text]));

-- private.candidate_daily_source_links.candidate_daily_source_links_validity_ck
alter table private.candidate_daily_source_links add constraint candidate_daily_source_links_validity_ck CHECK (valid_to_utc IS NULL OR valid_to_utc > valid_from_utc);

-- private.candidate_daily_sync_state.candidate_daily_sync_state_counts_ck
alter table private.candidate_daily_sync_state add constraint candidate_daily_sync_state_counts_ck CHECK (pending_count >= 0 AND retry_count >= 0 AND deferred_count >= 0 AND terminal_count >= 0);

-- private.candidate_daily_sync_state.candidate_daily_sync_state_cursor_ck
alter table private.candidate_daily_sync_state add constraint candidate_daily_sync_state_cursor_ck CHECK (accepted_canonical_cursor >= 0 AND required_visible_cursor >= 0 AND delivered_visible_cursor >= 0 AND overlay_proof_cursor >= 0 AND effective_visible_cursor >= 0 AND effective_visible_cursor <= accepted_canonical_cursor AND delivered_visible_cursor <= accepted_canonical_cursor AND overlay_proof_cursor <= accepted_canonical_cursor);

-- private.candidate_daily_sync_state.candidate_daily_sync_state_environment_ck
alter table private.candidate_daily_sync_state add constraint candidate_daily_sync_state_environment_ck CHECK (environment = ANY (ARRAY['TEST'::text, 'LIVE'::text]));

-- private.candidate_daily_sync_state.candidate_daily_sync_state_state_ck
alter table private.candidate_daily_sync_state add constraint candidate_daily_sync_state_state_ck CHECK (state = ANY (ARRAY['READY'::text, 'LAGGING'::text, 'BLOCKED'::text, 'ERROR'::text]));

-- private.candidate_daily_sync_state.candidate_daily_sync_state_target_ck
alter table private.candidate_daily_sync_state add constraint candidate_daily_sync_state_target_ck CHECK (target = 'MASTER_AVAILABILITY_SHEET'::text);

-- private.invoice_async_snapshot_hmac_keys.invoice_async_snapshot_hmac_keys_key_id_ck
alter table private.invoice_async_snapshot_hmac_keys add constraint invoice_async_snapshot_hmac_keys_key_id_ck CHECK (key_id ~ '^[a-z0-9][a-z0-9._-]{0,63}$'::text);

-- private.invoice_async_snapshot_hmac_keys.invoice_async_snapshot_hmac_keys_window_ck
alter table private.invoice_async_snapshot_hmac_keys add constraint invoice_async_snapshot_hmac_keys_window_ck CHECK ((active_to_utc IS NULL OR active_to_utc > active_from_utc) AND (verify_until_utc IS NULL OR verify_until_utc >= COALESCE(active_to_utc, active_from_utc)));

-- public.assignment_band_mappings.abm_target_contract_scope_check
alter table public.assignment_band_mappings add constraint abm_target_contract_scope_check CHECK (target_contract_id IS NULL OR candidate_id IS NOT NULL AND client_id IS NOT NULL);

-- public.assignment_band_mappings.assignment_band_mappings_system_type_check
alter table public.assignment_band_mappings add constraint assignment_band_mappings_system_type_check CHECK (system_type = ANY (ARRAY['NHSP'::text, 'HR_WEEKLY'::text]));

-- public.bank_name_checks.bank_name_checks_entity_kind_chk
alter table public.bank_name_checks add constraint bank_name_checks_entity_kind_chk CHECK (entity_kind = ANY (ARRAY['CANDIDATE'::text, 'UMBRELLA'::text]));

-- public.bank_name_checks.bank_name_checks_override_hash_chk
alter table public.bank_name_checks add constraint bank_name_checks_override_hash_chk CHECK (override_reason IS NULL OR override_hash = bank_details_hash);

-- public.bank_name_checks.bank_name_checks_provider_env_chk
alter table public.bank_name_checks add constraint bank_name_checks_provider_env_chk CHECK ((rail_provider = ANY (ARRAY['REVOLUT'::text, 'CSV'::text])) AND (rail_env = ANY (ARRAY['PROD'::text, 'SANDBOX'::text])));

-- public.bank_name_checks.bank_name_checks_status_chk
alter table public.bank_name_checks add constraint bank_name_checks_status_chk CHECK (status = ANY (ARRAY['UNVERIFIED'::text, 'PASS'::text, 'NEAR_MATCH'::text, 'FAIL'::text, 'UNAVAILABLE'::text]));

-- public.bank_payee_map.bank_payee_map_entity_kind_chk
alter table public.bank_payee_map add constraint bank_payee_map_entity_kind_chk CHECK (entity_kind = ANY (ARRAY['CANDIDATE'::text, 'UMBRELLA'::text]));

-- public.bank_payee_map.bank_payee_map_provider_env_chk
alter table public.bank_payee_map add constraint bank_payee_map_provider_env_chk CHECK ((rail_provider = ANY (ARRAY['REVOLUT'::text, 'CSV'::text])) AND (rail_env = ANY (ARRAY['PROD'::text, 'SANDBOX'::text])));

-- public.bank_provider_webhook_configs.bank_provider_webhook_configs_event_types_array_chk
alter table public.bank_provider_webhook_configs add constraint bank_provider_webhook_configs_event_types_array_chk CHECK (jsonb_typeof(event_types) = 'array'::text);

-- public.bank_provider_webhook_configs.bank_provider_webhook_configs_meta_object_chk
alter table public.bank_provider_webhook_configs add constraint bank_provider_webhook_configs_meta_object_chk CHECK (jsonb_typeof(meta_json) = 'object'::text);

-- public.bank_provider_webhook_configs.bank_provider_webhook_configs_status_chk
alter table public.bank_provider_webhook_configs add constraint bank_provider_webhook_configs_status_chk CHECK (status = ANY (ARRAY['ACTIVE'::text, 'DISABLED'::text, 'ROTATING_SECRET'::text, 'ERROR'::text, 'DELETED'::text]));

-- public.bank_provider_webhook_receipts.bank_provider_webhook_receipts_attempt_count_chk
alter table public.bank_provider_webhook_receipts add constraint bank_provider_webhook_receipts_attempt_count_chk CHECK (attempt_count >= 1);

-- public.bank_provider_webhook_receipts.bank_provider_webhook_receipts_ingest_results_array_chk
alter table public.bank_provider_webhook_receipts add constraint bank_provider_webhook_receipts_ingest_results_array_chk CHECK (jsonb_typeof(ingest_results_json) = 'array'::text);

-- public.bank_provider_webhook_receipts.bank_provider_webhook_receipts_normalised_events_array_chk
alter table public.bank_provider_webhook_receipts add constraint bank_provider_webhook_receipts_normalised_events_array_chk CHECK (jsonb_typeof(normalised_events_json) = 'array'::text);

-- public.bank_provider_webhook_receipts.bank_provider_webhook_receipts_raw_headers_object_chk
alter table public.bank_provider_webhook_receipts add constraint bank_provider_webhook_receipts_raw_headers_object_chk CHECK (jsonb_typeof(raw_headers_redacted) = 'object'::text);

-- public.bank_provider_webhook_receipts.bank_provider_webhook_receipts_status_chk
alter table public.bank_provider_webhook_receipts add constraint bank_provider_webhook_receipts_status_chk CHECK (status = ANY (ARRAY['RECEIVED'::text, 'DUPLICATE'::text, 'SIGNATURE_INVALID'::text, 'VERIFIED'::text, 'NORMALISED'::text, 'INGESTED'::text, 'UNMATCHED_REVIEW_REQUIRED'::text, 'FAILED_RETRYABLE'::text, 'FAILED_FINAL'::text, 'IGNORED_EVENT_TYPE'::text]));

-- public.banking_alert_acknowledgements.banking_alert_acknowledgements_entity_kind_chk
alter table public.banking_alert_acknowledgements add constraint banking_alert_acknowledgements_entity_kind_chk CHECK (btrim(entity_kind) <> ''::text);

-- public.banking_alert_acknowledgements.banking_alert_acknowledgements_fingerprint_chk
alter table public.banking_alert_acknowledgements add constraint banking_alert_acknowledgements_fingerprint_chk CHECK (btrim(alert_fingerprint) <> ''::text);

-- public.banking_alert_acknowledgements.banking_alert_acknowledgements_kind_chk
alter table public.banking_alert_acknowledgements add constraint banking_alert_acknowledgements_kind_chk CHECK (btrim(alert_kind) <> ''::text);

-- public.banking_alert_acknowledgements.banking_alert_acknowledgements_scope_chk
alter table public.banking_alert_acknowledgements add constraint banking_alert_acknowledgements_scope_chk CHECK (btrim(acknowledge_scope) <> ''::text);

-- public.banking_alert_success_events.banking_alert_success_events_event_key_chk
alter table public.banking_alert_success_events add constraint banking_alert_success_events_event_key_chk CHECK (NULLIF(btrim(event_key), ''::text) IS NOT NULL);

-- public.banking_alert_success_events.banking_alert_success_events_expiry_chk
alter table public.banking_alert_success_events add constraint banking_alert_success_events_expiry_chk CHECK (expires_at_utc > occurred_at_utc);

-- public.banking_alert_success_events.banking_alert_success_events_kind_chk
alter table public.banking_alert_success_events add constraint banking_alert_success_events_kind_chk CHECK (upper(btrim(alert_kind)) = ANY (ARRAY['BATCH_SCHEDULED_SUCCESS'::text, 'BATCH_SETTLED_SUCCESS'::text, 'BATCH_CANCELLATION_SUCCESS'::text]));

-- public.banking_alert_user_preferences.banking_alert_user_preferences_alert_allowlist_array_chk
alter table public.banking_alert_user_preferences add constraint banking_alert_user_preferences_alert_allowlist_array_chk CHECK (alert_kind_allowlist IS NULL OR jsonb_typeof(alert_kind_allowlist) = 'array'::text);

-- public.banking_alert_user_preferences.banking_alert_user_preferences_alert_blocklist_array_chk
alter table public.banking_alert_user_preferences add constraint banking_alert_user_preferences_alert_blocklist_array_chk CHECK (jsonb_typeof(alert_kind_blocklist) = 'array'::text);

-- public.banking_alert_user_preferences.banking_alert_user_preferences_muted_batches_array_chk
alter table public.banking_alert_user_preferences add constraint banking_alert_user_preferences_muted_batches_array_chk CHECK (jsonb_typeof(muted_pay_batch_ids) = 'array'::text);

-- public.banking_alert_user_preferences.banking_alert_user_preferences_muted_providers_array_chk
alter table public.banking_alert_user_preferences add constraint banking_alert_user_preferences_muted_providers_array_chk CHECK (jsonb_typeof(muted_provider_keys) = 'array'::text);

-- public.banking_alert_user_preferences.banking_alert_user_preferences_reason_allowlist_array_chk
alter table public.banking_alert_user_preferences add constraint banking_alert_user_preferences_reason_allowlist_array_chk CHECK (failure_reason_allowlist IS NULL OR jsonb_typeof(failure_reason_allowlist) = 'array'::text);

-- public.banking_alert_user_preferences.banking_alert_user_preferences_reason_blocklist_array_chk
alter table public.banking_alert_user_preferences add constraint banking_alert_user_preferences_reason_blocklist_array_chk CHECK (jsonb_typeof(failure_reason_blocklist) = 'array'::text);

-- public.banking_alert_user_preferences.banking_alert_user_preferences_severity_chk
alter table public.banking_alert_user_preferences add constraint banking_alert_user_preferences_severity_chk CHECK (severity_min = ANY (ARRAY['INFO'::text, 'PROGRESS'::text, 'ACTION_REQUIRED'::text, 'CRITICAL'::text]));

-- public.banking_pay_batch_change_signals.banking_pay_batch_change_signals_candidate_ids_array_chk
alter table public.banking_pay_batch_change_signals add constraint banking_pay_batch_change_signals_candidate_ids_array_chk CHECK (jsonb_typeof(last_changed_candidate_ids) = 'array'::text);

-- public.banking_pay_batch_change_signals.banking_pay_batch_change_signals_item_ids_array_chk
alter table public.banking_pay_batch_change_signals add constraint banking_pay_batch_change_signals_item_ids_array_chk CHECK (jsonb_typeof(last_changed_pay_batch_item_ids) = 'array'::text);

-- public.banking_pay_batch_change_signals.banking_pay_batch_change_signals_scope_object_chk
alter table public.banking_pay_batch_change_signals add constraint banking_pay_batch_change_signals_scope_object_chk CHECK (jsonb_typeof(last_change_scope_json) = 'object'::text);

-- public.banking_pay_batch_change_signals.banking_pay_batch_change_signals_transfer_ids_array_chk
alter table public.banking_pay_batch_change_signals add constraint banking_pay_batch_change_signals_transfer_ids_array_chk CHECK (jsonb_typeof(last_changed_transfer_ids) = 'array'::text);

-- public.banking_pay_batch_change_signals.banking_pay_batch_change_signals_versions_chk
alter table public.banking_pay_batch_change_signals add constraint banking_pay_batch_change_signals_versions_chk CHECK (version >= 0 AND payment_status_version >= 0 AND correction_progress_version >= 0 AND alert_version >= 0 AND overview_version >= 0);

-- public.banking_pay_operation_candidate_allocation_rows.banking_pay_operation_candidate_allocation_json_shape_chk
alter table public.banking_pay_operation_candidate_allocation_rows add constraint banking_pay_operation_candidate_allocation_json_shape_chk CHECK (jsonb_typeof(allocation_basis_json) = 'object'::text);

-- public.banking_pay_operation_candidate_allocation_rows.banking_pay_operation_candidate_allocation_status_chk
alter table public.banking_pay_operation_candidate_allocation_rows add constraint banking_pay_operation_candidate_allocation_status_chk CHECK (status = ANY (ARRAY['PENDING'::text, 'ITEM_CREATED'::text, 'SKIPPED'::text, 'FAILED'::text]));

-- public.banking_pay_operation_candidate_scope.banking_pay_operation_candidate_scope_json_shape_chk
alter table public.banking_pay_operation_candidate_scope add constraint banking_pay_operation_candidate_scope_json_shape_chk CHECK (jsonb_typeof(selected_preview_row_ids_json) = 'array'::text AND jsonb_typeof(selected_timesheet_ids_json) = 'array'::text AND jsonb_typeof(selected_finance_case_ids_json) = 'array'::text AND jsonb_typeof(effective_candidate_fragment_json) = 'object'::text AND jsonb_typeof(effective_summary_fragment_json) = 'object'::text AND jsonb_typeof(effective_paye_candidate_json) = 'object'::text AND jsonb_typeof(effective_non_paye_payee_json) = 'object'::text AND jsonb_typeof(effective_payees_json) = 'array'::text AND jsonb_typeof(effective_case_resolution_states_json) = 'object'::text AND jsonb_typeof(effective_canonical_preview_lines_json) = 'array'::text AND jsonb_typeof(selected_canonical_preview_lines_json) = 'array'::text AND jsonb_typeof(baseline_component_rows_json) = 'array'::text AND jsonb_typeof(hidden_recovery_template_lines_json) = 'array'::text AND jsonb_typeof(candidate_totals_json) = 'object'::text AND jsonb_typeof(allocation_basis_json) = 'object'::text);

-- public.banking_pay_operation_candidate_scope.banking_pay_operation_candidate_scope_status_chk
alter table public.banking_pay_operation_candidate_scope add constraint banking_pay_operation_candidate_scope_status_chk CHECK (status = ANY (ARRAY['PENDING'::text, 'SCOPED'::text, 'ALLOCATED'::text, 'DRAFTED'::text, 'FAILED'::text]));

-- public.banking_pay_operation_chunks.banking_pay_operation_chunks_chunk_type_chk
alter table public.banking_pay_operation_chunks add constraint banking_pay_operation_chunks_chunk_type_chk CHECK (chunk_type = ANY (ARRAY['CANDIDATE_SCOPE'::text, 'TSFIN'::text, 'PAYEE_READINESS'::text, 'TRANSFER_GROUP'::text, 'TRANSFER_SCOPE_ITEM_SEED'::text, 'TRANSFER_SCOPE_ROLLUP'::text, 'TRANSFER_SUBMIT'::text, 'RAIL_UPDATE'::text, 'SETTLEMENT'::text, 'REMITTANCE'::text, 'PAYOUT_NOTICE'::text, 'PREVIEW_PAGE'::text, 'FRESHNESS_VALIDATE'::text]));

-- public.banking_pay_operation_chunks.banking_pay_operation_chunks_counts_nonneg_chk
alter table public.banking_pay_operation_chunks add constraint banking_pay_operation_chunks_counts_nonneg_chk CHECK (unit_count >= 0 AND completed_count >= 0 AND failed_count >= 0);

-- public.banking_pay_operation_chunks.banking_pay_operation_chunks_json_shape_chk
alter table public.banking_pay_operation_chunks add constraint banking_pay_operation_chunks_json_shape_chk CHECK (jsonb_typeof(payload_json) = 'object'::text AND (result_json IS NULL OR jsonb_typeof(result_json) = 'object'::text) AND (error_json IS NULL OR jsonb_typeof(error_json) = 'object'::text));

-- public.banking_pay_operation_chunks.banking_pay_operation_chunks_sequence_chk
alter table public.banking_pay_operation_chunks add constraint banking_pay_operation_chunks_sequence_chk CHECK (sequence_no > 0);

-- public.banking_pay_operation_chunks.banking_pay_operation_chunks_status_chk
alter table public.banking_pay_operation_chunks add constraint banking_pay_operation_chunks_status_chk CHECK (status = ANY (ARRAY['PENDING'::text, 'RUNNING'::text, 'COMPLETE'::text, 'FAILED'::text, 'SKIPPED'::text]));

-- public.banking_pay_operation_config.banking_pay_operation_config_chunk_type_chk
alter table public.banking_pay_operation_config add constraint banking_pay_operation_config_chunk_type_chk CHECK (chunk_type = ANY (ARRAY['CANDIDATE_SCOPE'::text, 'TSFIN'::text, 'PAYEE_READINESS'::text, 'TRANSFER_GROUP'::text, 'TRANSFER_SUBMIT'::text, 'RAIL_UPDATE'::text, 'SETTLEMENT'::text, 'REMITTANCE'::text, 'PAYOUT_NOTICE'::text, 'PREVIEW_PAGE'::text, 'FRESHNESS_VALIDATE'::text]));

-- public.banking_pay_operation_config.banking_pay_operation_config_operation_type_chk
alter table public.banking_pay_operation_config add constraint banking_pay_operation_config_operation_type_chk CHECK (operation_type = ANY (ARRAY['ALL'::text, 'DRAFT_CREATE'::text, 'PAYMENT_EXECUTE'::text, 'PAYMENT_RETRY_BLOCKED_FUNDS'::text, 'PAYMENT_SETTLEMENT'::text, 'REMITTANCE_QUEUE'::text, 'PREVIEW_REFRESH'::text, 'PAYMENT_CORRECTION'::text]));

-- public.banking_pay_operation_config.banking_pay_operation_config_sizes_chk
alter table public.banking_pay_operation_config add constraint banking_pay_operation_config_sizes_chk CHECK (min_chunk_size > 0 AND default_chunk_size > 0 AND max_chunk_size >= min_chunk_size AND default_chunk_size >= min_chunk_size AND default_chunk_size <= max_chunk_size AND max_advance_ms >= 1000 AND max_advance_ms <= 120000 AND lock_seconds >= 5 AND lock_seconds <= 3600);

-- public.banking_pay_operation_remittance_scope.banking_pay_operation_remittance_scope_payload_shape_chk
alter table public.banking_pay_operation_remittance_scope add constraint banking_pay_operation_remittance_scope_payload_shape_chk CHECK (jsonb_typeof(payload_json) = 'object'::text);

-- public.banking_pay_operation_remittance_scope.banking_pay_operation_remittance_scope_status_chk
alter table public.banking_pay_operation_remittance_scope add constraint banking_pay_operation_remittance_scope_status_chk CHECK (status = ANY (ARRAY['PENDING'::text, 'QUEUED'::text, 'SKIPPED'::text, 'FAILED'::text]));

-- public.banking_pay_operation_settlement_scope.banking_pay_operation_settlement_scope_payload_shape_chk
alter table public.banking_pay_operation_settlement_scope add constraint banking_pay_operation_settlement_scope_payload_shape_chk CHECK (jsonb_typeof(payload_json) = 'object'::text);

-- public.banking_pay_operation_settlement_scope.banking_pay_operation_settlement_scope_status_chk
alter table public.banking_pay_operation_settlement_scope add constraint banking_pay_operation_settlement_scope_status_chk CHECK (status = ANY (ARRAY['PENDING'::text, 'SETTLED'::text, 'SKIPPED'::text, 'FAILED'::text]));

-- public.banking_pay_operation_transfer_scope.banking_pay_operation_transfer_scope_amount_chk
alter table public.banking_pay_operation_transfer_scope add constraint banking_pay_operation_transfer_scope_amount_chk CHECK (amount >= 0::numeric);

-- public.banking_pay_operation_transfer_scope.banking_pay_operation_transfer_scope_json_shape_chk
alter table public.banking_pay_operation_transfer_scope add constraint banking_pay_operation_transfer_scope_json_shape_chk CHECK (jsonb_typeof(pay_batch_item_ids_json) = 'array'::text AND jsonb_typeof(candidate_ids_json) = 'array'::text);

-- public.banking_pay_operation_transfer_scope.banking_pay_operation_transfer_scope_status_chk
alter table public.banking_pay_operation_transfer_scope add constraint banking_pay_operation_transfer_scope_status_chk CHECK (status = ANY (ARRAY['PENDING'::text, 'ROLLED_UP'::text, 'PREPARED'::text, 'SUBMITTED'::text, 'FAILED'::text, 'SKIPPED'::text]));

-- public.banking_pay_operation_transfer_scope.chk_bpay_transfer_scope_provider_state
alter table public.banking_pay_operation_transfer_scope add constraint chk_bpay_transfer_scope_provider_state CHECK (provider_submit_state = ANY (ARRAY['NOT_READY'::text, 'READY'::text, 'CLAIMED'::text, 'REQUEST_PREPARING'::text, 'REQUEST_SENDING'::text, 'REQUEST_SENT_LOCAL'::text, 'PROVIDER_ACCEPTED'::text, 'PROVIDER_REJECTED'::text, 'PROVIDER_UNKNOWN'::text, 'REVIEW_REQUIRED'::text, 'CHUNK_FINALISED'::text]));

-- public.banking_pay_operations.banking_pay_operations_json_shape_chk
alter table public.banking_pay_operations add constraint banking_pay_operations_json_shape_chk CHECK (jsonb_typeof(input_json) = 'object'::text AND jsonb_typeof(config_json) = 'object'::text AND jsonb_typeof(progress_json) = 'object'::text AND (result_json IS NULL OR jsonb_typeof(result_json) = 'object'::text) AND (error_json IS NULL OR jsonb_typeof(error_json) = 'object'::text));

-- public.banking_pay_operations.banking_pay_operations_operation_type_chk
alter table public.banking_pay_operations add constraint banking_pay_operations_operation_type_chk CHECK (operation_type = ANY (ARRAY['DRAFT_CREATE'::text, 'PAYMENT_EXECUTE'::text, 'PAYMENT_RETRY_BLOCKED_FUNDS'::text, 'PAYMENT_SETTLEMENT'::text, 'REMITTANCE_QUEUE'::text, 'PREVIEW_REFRESH'::text, 'PAYMENT_CORRECTION'::text]));

-- public.banking_pay_operations.banking_pay_operations_post_freeze_scope_status_chk
alter table public.banking_pay_operations add constraint banking_pay_operations_post_freeze_scope_status_chk CHECK (post_freeze_scope_status = ANY (ARRAY['NONE'::text, 'PENDING_RELEVANCE'::text, 'RELEVANT'::text, 'IRRELEVANT'::text]));

-- public.banking_pay_operations.banking_pay_operations_scope_freeze_status_chk
alter table public.banking_pay_operations add constraint banking_pay_operations_scope_freeze_status_chk CHECK (scope_freeze_status = ANY (ARRAY['NONE'::text, 'SEEDING'::text, 'FROZEN'::text]));

-- public.banking_pay_operations.banking_pay_operations_status_chk
alter table public.banking_pay_operations add constraint banking_pay_operations_status_chk CHECK (status = ANY (ARRAY['QUEUED'::text, 'RUNNING'::text, 'WAITING'::text, 'WAITING_AUTHORISATION'::text, 'WAITING_PROVIDER'::text, 'COMPLETE'::text, 'FAILED'::text, 'CANCELLED'::text, 'REVIEW_REQUIRED'::text]));

-- public.banking_pay_operations.banking_pay_operations_units_nonneg_chk
alter table public.banking_pay_operations add constraint banking_pay_operations_units_nonneg_chk CHECK (total_units >= 0 AND completed_units >= 0 AND failed_units >= 0 AND current_chunk_index >= 0 AND chunk_count >= 0);

-- public.banking_pay_scope_change_transactions.banking_pay_scope_change_transactions_finalized_chk
alter table public.banking_pay_scope_change_transactions add constraint banking_pay_scope_change_transactions_finalized_chk CHECK (state = 'PENDING'::text AND allocated_generation IS NULL AND finalized_at_utc IS NULL OR state = 'NOOP'::text AND allocated_generation IS NULL AND finalized_at_utc IS NOT NULL OR state = 'FINALIZED'::text AND allocated_generation IS NOT NULL AND finalized_at_utc IS NOT NULL);

-- public.banking_pay_scope_change_transactions.banking_pay_scope_change_transactions_state_chk
alter table public.banking_pay_scope_change_transactions add constraint banking_pay_scope_change_transactions_state_chk CHECK (state = ANY (ARRAY['PENDING'::text, 'FINALIZED'::text, 'NOOP'::text]));

-- public.banking_pay_snapshot_candidate_state.banking_pay_snapshot_candidate_state_status_chk
alter table public.banking_pay_snapshot_candidate_state add constraint banking_pay_snapshot_candidate_state_status_chk CHECK (status = ANY (ARRAY['PENDING'::text, 'READY'::text, 'FAILED'::text]));

-- public.banking_pay_snapshot_runs.banking_pay_snapshot_runs_status_chk
alter table public.banking_pay_snapshot_runs add constraint banking_pay_snapshot_runs_status_chk CHECK (status = ANY (ARRAY['OPEN'::text, 'READY'::text, 'FAILED'::text, 'ARCHIVED'::text]));

-- public.banking_pay_workbench_candidate_delta_projection_runs.banking_pay_workbench_delta_projection_runs_admission_seal_json
alter table public.banking_pay_workbench_candidate_delta_projection_runs add constraint banking_pay_workbench_delta_projection_runs_admission_seal_json CHECK (jsonb_typeof(admission_seal_json) = 'object'::text);

-- public.banking_pay_workbench_candidate_delta_projection_runs.banking_pay_workbench_delta_projection_runs_admission_seal_shap
alter table public.banking_pay_workbench_candidate_delta_projection_runs add constraint banking_pay_workbench_delta_projection_runs_admission_seal_shap CHECK (admission_seal_version IS NULL AND admission_seal_json = '{}'::jsonb AND admission_seal_digest IS NULL AND admission_sealed_at_utc IS NULL OR admission_seal_version = 1 AND admission_seal_json <> '{}'::jsonb AND admission_seal_digest ~ '^[0-9a-f]{64}$'::text AND admission_sealed_at_utc IS NOT NULL);

-- public.banking_pay_workbench_candidate_delta_projection_runs.chk_bpay_delta_projection_runs_mode
alter table public.banking_pay_workbench_candidate_delta_projection_runs add constraint chk_bpay_delta_projection_runs_mode CHECK (projection_mode = ANY (ARRAY['DELTA'::text, 'READINESS_PATCH'::text, 'RESERVATION_PATCH'::text, 'POST_DRAFT_OVERLAY'::text, 'CLONE_REBASE'::text, 'SHADOW_COMPARE'::text]));

-- public.banking_pay_workbench_candidate_delta_projection_runs.chk_bpay_delta_projection_runs_status
alter table public.banking_pay_workbench_candidate_delta_projection_runs add constraint chk_bpay_delta_projection_runs_status CHECK (status = ANY (ARRAY['RUNNING'::text, 'COMPLETED'::text, 'FAILED'::text, 'FALLBACK_REQUIRED'::text, 'BLOCKED'::text]));

-- public.banking_pay_workbench_candidate_delta_projection_runs.chk_bpay_delta_runs_non_negative_counts
alter table public.banking_pay_workbench_candidate_delta_projection_runs add constraint chk_bpay_delta_runs_non_negative_counts CHECK (projected_row_count >= 0 AND written_source_count >= 0 AND written_line_work_count >= 0 AND written_preview_count >= 0);

-- public.banking_pay_workbench_candidate_delta_projection_runs.chk_bpay_delta_runs_write_cursor_object
alter table public.banking_pay_workbench_candidate_delta_projection_runs add constraint chk_bpay_delta_runs_write_cursor_object CHECK (jsonb_typeof(write_cursor_json) = 'object'::text);

-- public.banking_pay_workbench_candidate_source_lines.bpay_wb_source_lines_json_shape_chk
alter table public.banking_pay_workbench_candidate_source_lines add constraint bpay_wb_source_lines_json_shape_chk CHECK (jsonb_typeof(source_row_json) = 'object'::text AND jsonb_typeof(economic_key_json) = 'object'::text AND jsonb_typeof(contract_json) = 'object'::text);

-- public.banking_pay_workbench_candidate_source_lines.bpay_wb_source_lines_status_chk
alter table public.banking_pay_workbench_candidate_source_lines add constraint bpay_wb_source_lines_status_chk CHECK (status = ANY (ARRAY['CURRENT'::text, 'SUPERSEDED'::text, 'DIRTY'::text, 'ERROR'::text]));

-- public.banking_pay_workbench_candidate_source_lines.bpay_wb_source_publication_identity_ck
alter table public.banking_pay_workbench_candidate_source_lines add constraint bpay_wb_source_publication_identity_ck CHECK (source_publication_id IS NULL OR source_build_run_id IS NOT NULL AND session_version > 0 AND source_change_seq >= 0 AND source_ordinal > 0) NOT VALID;

-- public.banking_pay_workbench_case_resolution_carry_registrations.banking_pay_case_resolution_carry_scope_kind_chk
alter table public.banking_pay_workbench_case_resolution_carry_registrations add constraint banking_pay_case_resolution_carry_scope_kind_chk CHECK (resolution_scope_kind = ANY (ARRAY['CORRECTION_COMPONENT'::text, 'NON_CORRECTION'::text]));

-- public.banking_pay_workbench_case_resolution_carry_registrations.banking_pay_case_resolution_carry_status_chk
alter table public.banking_pay_workbench_case_resolution_carry_registrations add constraint banking_pay_case_resolution_carry_status_chk CHECK (status = ANY (ARRAY['PENDING'::text, 'CARRIED'::text, 'STALE'::text, 'INCOMPATIBLE'::text, 'SUPERSEDED'::text]));

-- public.banking_pay_workbench_case_resolution_carry_registrations.banking_pay_case_resolution_carry_terminal_shape_chk
alter table public.banking_pay_workbench_case_resolution_carry_registrations add constraint banking_pay_case_resolution_carry_terminal_shape_chk CHECK (status = 'PENDING'::text AND completed_at_utc IS NULL AND target_resolution_id IS NULL OR status = 'CARRIED'::text AND completed_at_utc IS NOT NULL AND target_resolution_id IS NOT NULL AND target_economic_fingerprint IS NOT NULL OR (status = ANY (ARRAY['STALE'::text, 'INCOMPATIBLE'::text, 'SUPERSEDED'::text])) AND completed_at_utc IS NOT NULL AND state_reason_code IS NOT NULL);

-- public.banking_pay_workbench_jobs.banking_pay_workbench_jobs_coordinator_token_chk
alter table public.banking_pay_workbench_jobs add constraint banking_pay_workbench_jobs_coordinator_token_chk CHECK (upper(btrim(COALESCE(job_type, ''::text))) <> 'WORKBENCH_SCOPE_RECONCILE'::text OR scope_change_tx_token IS NULL);

-- public.banking_pay_workbench_jobs.banking_pay_workbench_jobs_status_chk
alter table public.banking_pay_workbench_jobs add constraint banking_pay_workbench_jobs_status_chk CHECK (status = ANY (ARRAY['QUEUED'::text, 'RUNNING'::text, 'SUCCEEDED'::text, 'FAILED'::text, 'DEAD'::text]));

-- public.banking_pay_workbench_jobs.bpay_wb_jobs_build_identity_chk
alter table public.banking_pay_workbench_jobs add constraint bpay_wb_jobs_build_identity_chk CHECK (private_stage IS NULL AND economic_build_id IS NULL AND private_cursor_kind IS NULL AND private_stage_version IS NULL AND private_cursor_json = '{}'::jsonb OR economic_build_id IS NULL AND job_type = 'WORKBENCH_CANDIDATE_DIRTY_APPLY'::text AND private_stage IS NULL AND private_cursor_kind = 'DIRTY_SESSION_SCAN_V1'::text AND private_stage_version = 1 OR economic_build_id IS NULL AND job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'::text AND status = 'QUEUED'::text AND private_stage = 'BUILD_INITIALISE'::text AND private_cursor_kind = 'BUILD_INITIALISE'::text AND attempt_count = 0 AND private_stage_version = 1 OR economic_build_id IS NOT NULL AND job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'::text AND private_stage IS NOT NULL AND private_stage <> 'BUILD_INITIALISE'::text AND private_cursor_kind IS NOT NULL AND private_stage_version IS NOT NULL);

-- public.banking_pay_workbench_jobs.bpay_wb_jobs_private_cursor_chk
alter table public.banking_pay_workbench_jobs add constraint bpay_wb_jobs_private_cursor_chk CHECK (jsonb_typeof(private_cursor_json) = 'object'::text AND (private_stage_version IS NULL OR private_stage_version > 0));

-- public.banking_pay_workbench_jobs.bpay_wb_jobs_private_stage_cursor_chk
alter table public.banking_pay_workbench_jobs add constraint bpay_wb_jobs_private_stage_cursor_chk CHECK (private_stage IS NULL AND private_cursor_kind IS NULL AND private_stage_version IS NULL OR job_type = 'WORKBENCH_CANDIDATE_DIRTY_APPLY'::text AND private_stage IS NULL AND private_cursor_kind = 'DIRTY_SESSION_SCAN_V1'::text AND private_stage_version = 1 OR private_stage = 'BUILD_INITIALISE'::text AND private_cursor_kind = 'BUILD_INITIALISE'::text OR private_stage = 'PREPARE_SCOPE'::text AND (private_cursor_kind = ANY (ARRAY['SCOPE_SELECT'::text, 'SEED_SCOPE_SEAL'::text])) OR private_stage = 'DEPENDENCY_CLOSURE'::text AND (private_cursor_kind = ANY (ARRAY['DEPENDENCY_CLOSURE'::text, 'DEPENDENCY_SCOPE_SEAL'::text])) OR private_stage = 'WORKSPACE_FACT'::text AND private_cursor_kind = 'WORKSPACE_FACT'::text OR private_stage = 'RECONCILE_EXECUTE'::text AND private_cursor_kind = 'RECONCILE_EXECUTE'::text OR private_stage = 'SOURCE_PUBLISH'::text AND private_cursor_kind = 'SOURCE_PUBLISH'::text OR private_stage = 'BOOTSTRAP_DISCOVERY'::text AND private_cursor_kind = 'BOOTSTRAP_DISCOVERY'::text OR private_stage = 'BUILD_CLEANUP'::text AND private_cursor_kind = 'BUILD_CLEANUP'::text OR private_stage = 'COMPLETE'::text AND private_cursor_kind = 'COMPLETE'::text);

-- public.banking_pay_workbench_selection_carry_registrations.banking_pay_selection_carry_state_chk
alter table public.banking_pay_workbench_selection_carry_registrations add constraint banking_pay_selection_carry_state_chk CHECK (selection_state = ANY (ARRAY['SELECTED'::text, 'UNSELECTED'::text]));

-- public.banking_pay_workbench_selection_carry_registrations.banking_pay_selection_carry_status_chk
alter table public.banking_pay_workbench_selection_carry_registrations add constraint banking_pay_selection_carry_status_chk CHECK (status = ANY (ARRAY['PENDING'::text, 'APPLIED'::text, 'SUPERSEDED'::text, 'AMBIGUOUS'::text]));

-- public.banking_pay_workbench_selection_carry_registrations.banking_pay_selection_carry_terminal_shape_chk
alter table public.banking_pay_workbench_selection_carry_registrations add constraint banking_pay_selection_carry_terminal_shape_chk CHECK (status = 'PENDING'::text AND completed_at_utc IS NULL AND target_preview_row_id IS NULL AND source_preview_row_id IS NOT NULL OR status = 'APPLIED'::text AND completed_at_utc IS NOT NULL OR (status = ANY (ARRAY['SUPERSEDED'::text, 'AMBIGUOUS'::text])) AND completed_at_utc IS NOT NULL AND state_reason_code IS NOT NULL);

-- public.banking_pay_workbench_session_candidate_state.banking_pay_workbench_session_candidate_state_status_chk
alter table public.banking_pay_workbench_session_candidate_state add constraint banking_pay_workbench_session_candidate_state_status_chk CHECK (status = ANY (ARRAY['PENDING'::text, 'READY'::text, 'FAILED'::text]));

-- public.banking_pay_workbench_session_scope.bpay_wb_scope_certified_preview_attestation_ck
alter table public.banking_pay_workbench_session_scope add constraint bpay_wb_scope_certified_preview_attestation_ck CHECK (certified_preview_publication_parity_ok IS NOT TRUE OR certified_preview_publication_required IS TRUE AND certified_preview_publication_session_version > 0 AND certified_preview_publication_source_change_seq >= 0 AND certified_preview_publication_source_build_run_id IS NOT NULL AND certified_preview_publication_attested_at_utc IS NOT NULL AND jsonb_typeof(certified_preview_publication_attestation_json) = 'object'::text AND (certified_preview_publication_attestation_json ->> 'parity_complete'::text) = 'true'::text AND ((certified_preview_publication_attestation_json ->> 'attestation_version'::text) = 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V1'::text AND (certified_preview_publication_attestation_json ->> 'authority_kind'::text) = 'BOUNDED_FULL_SOURCE_BUILD'::text OR (certified_preview_publication_attestation_json ->> 'attestation_version'::text) = 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V2'::text AND (certified_preview_publication_attestation_json ->> 'contract_version'::text) = '2'::text AND (certified_preview_publication_attestation_json ->> 'authority_kind'::text) = 'CERTIFIED_CLONE'::text AND ((certified_preview_publication_attestation_json ->> 'final_state'::text) = ANY (ARRAY['READY'::text, 'SOURCE_EMPTY'::text])) OR (certified_preview_publication_attestation_json ->> 'attestation_version'::text) = 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V2'::text AND (certified_preview_publication_attestation_json ->> 'contract_version'::text) = '2'::text AND (certified_preview_publication_attestation_json ->> 'authority_kind'::text) = 'TARGETED_DELTA'::text AND ((certified_preview_publication_attestation_json ->> 'final_state'::text) = ANY (ARRAY['READY'::text, 'SOURCE_EMPTY'::text])) OR (certified_preview_publication_attestation_json ->> 'attestation_version'::text) = 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3'::text AND (certified_preview_publication_attestation_json ->> 'contract_version'::text) = '3'::text AND (certified_preview_publication_attestation_json ->> 'semantic_contract_version'::text) = 'READY_TO_PAY_SEMANTIC_V2'::text AND ((certified_preview_publication_attestation_json ->> 'authority_kind'::text) = ANY (ARRAY['BOUNDED_FULL_SOURCE_BUILD'::text, 'CERTIFIED_CLONE'::text, 'TARGETED_DELTA'::text, 'CERTIFIED_CANCELLATION_REVERSION'::text])) AND ((certified_preview_publication_attestation_json ->> 'final_state'::text) = ANY (ARRAY['READY'::text, 'SOURCE_EMPTY'::text])) AND (certified_preview_publication_attestation_json ->> 'semantic_ready'::text) = 'true'::text AND COALESCE((certified_preview_publication_attestation_json ->> 'invalid_selectable_row_count'::text)::integer, '-1'::integer) = 0 AND ((certified_preview_publication_attestation_json ->> 'candidate_ready_amount'::text)::numeric) >= 0::numeric AND NULLIF(certified_preview_publication_attestation_json ->> 'semantic_proof_digest'::text, ''::text) IS NOT NULL));

-- public.banking_pay_workbench_session_scope.bpay_wb_scope_source_publication_identity_ck
alter table public.banking_pay_workbench_session_scope add constraint bpay_wb_scope_source_publication_identity_ck CHECK (certified_preview_publication_source_publication_id IS NULL OR certified_preview_publication_source_build_run_id IS NOT NULL AND certified_preview_publication_session_version > 0 AND certified_preview_publication_source_change_seq >= 0) NOT VALID;

-- public.banking_pay_workbench_sessions.banking_pay_workbench_sessions_scope_generation_chk
alter table public.banking_pay_workbench_sessions add constraint banking_pay_workbench_sessions_scope_generation_chk CHECK (scope_change_generation_target >= 0 AND scope_change_generation_applied >= 0);

-- public.banking_pay_workbench_sessions.banking_pay_workbench_sessions_status_chk
alter table public.banking_pay_workbench_sessions add constraint banking_pay_workbench_sessions_status_chk CHECK (status = ANY (ARRAY['OPEN'::text, 'DISCARDED'::text]));

-- public.banking_pay_workbench_sessions.bpay_workbench_sessions_progress_counts_nonnegative_chk
alter table public.banking_pay_workbench_sessions add constraint bpay_workbench_sessions_progress_counts_nonnegative_chk CHECK (scope_total_count >= 0 AND scope_seeded_count >= 0 AND scope_ready_count >= 0 AND scope_pending_count >= 0 AND scope_failed_count >= 0 AND line_units_total >= 0 AND line_units_ready >= 0 AND line_units_pending >= 0 AND line_units_failed >= 0 AND preview_row_count >= 0 AND selected_row_count >= 0 AND progress_counter_version >= 0);

-- public.banking_pay_workbench_sessions.bpay_workbench_sessions_progress_json_shape_chk
alter table public.banking_pay_workbench_sessions add constraint bpay_workbench_sessions_progress_json_shape_chk CHECK (jsonb_typeof(scope_next_cursor_json) = 'object'::text AND jsonb_typeof(section_counts_json) = 'object'::text AND jsonb_typeof(candidate_sample_rows_json) = 'array'::text AND jsonb_typeof(progress_json) = 'object'::text);

-- public.candidate_app_accounts.candidate_app_accounts_email_normalized_ck
alter table public.candidate_app_accounts add constraint candidate_app_accounts_email_normalized_ck CHECK (email_normalized = lower(btrim(email_normalized)) AND email_normalized ~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$'::text);

-- public.candidate_app_accounts.candidate_app_accounts_environment_ck
alter table public.candidate_app_accounts add constraint candidate_app_accounts_environment_ck CHECK (environment = ANY (ARRAY['TEST'::text, 'LIVE'::text]));

-- public.candidate_app_accounts.candidate_app_accounts_failed_login_ck
alter table public.candidate_app_accounts add constraint candidate_app_accounts_failed_login_ck CHECK (failed_login_count >= 0 AND failed_login_count <= 1000);

-- public.candidate_app_accounts.candidate_app_accounts_password_group_ck
alter table public.candidate_app_accounts add constraint candidate_app_accounts_password_group_ck CHECK (password_scheme IS NULL AND password_scheme_version IS NULL AND password_salt IS NULL AND password_digest IS NULL AND password_changed_at_utc IS NULL OR NULLIF(btrim(password_scheme), ''::text) IS NOT NULL AND password_scheme_version IS NOT NULL AND password_scheme_version > 0 AND password_salt IS NOT NULL AND octet_length(password_salt) >= 16 AND octet_length(password_salt) <= 64 AND password_digest IS NOT NULL AND octet_length(password_digest) >= 32 AND octet_length(password_digest) <= 128 AND password_changed_at_utc IS NOT NULL);

-- public.candidate_app_accounts.candidate_app_accounts_password_params_ck
alter table public.candidate_app_accounts add constraint candidate_app_accounts_password_params_ck CHECK (jsonb_typeof(password_params_json) = 'object'::text);

-- public.candidate_app_accounts.candidate_app_accounts_preferences_ck
alter table public.candidate_app_accounts add constraint candidate_app_accounts_preferences_ck CHECK (jsonb_typeof(notification_preferences_json) = 'object'::text);

-- public.candidate_app_accounts.candidate_app_accounts_session_version_ck
alter table public.candidate_app_accounts add constraint candidate_app_accounts_session_version_ck CHECK (session_version > 0);

-- public.candidate_app_accounts.candidate_app_accounts_status_ck
alter table public.candidate_app_accounts add constraint candidate_app_accounts_status_ck CHECK (status = ANY (ARRAY['SETUP_REQUIRED'::text, 'ACTIVE'::text, 'LOCKED'::text, 'DISABLED'::text]));

-- public.candidate_app_global_membership_links.candidate_app_global_membership_links_account_hmac_ck
alter table public.candidate_app_global_membership_links add constraint candidate_app_global_membership_links_account_hmac_ck CHECK (octet_length(global_account_identity_hmac) = 32);

-- public.candidate_app_global_membership_links.candidate_app_global_membership_links_generation_ck
alter table public.candidate_app_global_membership_links add constraint candidate_app_global_membership_links_generation_ck CHECK (membership_generation >= 1);

-- public.candidate_app_global_membership_links.candidate_app_global_membership_links_state_ck
alter table public.candidate_app_global_membership_links add constraint candidate_app_global_membership_links_state_ck CHECK (state = ANY (ARRAY['PENDING'::text, 'ACTIVE'::text, 'DISABLED'::text, 'REVOKED'::text]));

-- public.candidate_app_global_membership_links.candidate_app_global_membership_links_state_time_ck
alter table public.candidate_app_global_membership_links add constraint candidate_app_global_membership_links_state_time_ck CHECK (state = 'REVOKED'::text AND revoked_at_utc IS NOT NULL OR state <> 'REVOKED'::text);

-- public.candidate_app_sessions.candidate_app_sessions_auth_source_ck
alter table public.candidate_app_sessions add constraint candidate_app_sessions_auth_source_ck CHECK (auth_source = ANY (ARRAY['LOCAL'::text, 'CONTROL_PLANE'::text]));

-- public.candidate_app_sessions.candidate_app_sessions_device_hash_ck
alter table public.candidate_app_sessions add constraint candidate_app_sessions_device_hash_ck CHECK (device_id_hash IS NULL OR octet_length(device_id_hash) = 32);

-- public.candidate_app_sessions.candidate_app_sessions_environment_ck
alter table public.candidate_app_sessions add constraint candidate_app_sessions_environment_ck CHECK (environment = ANY (ARRAY['TEST'::text, 'LIVE'::text]));

-- public.candidate_app_sessions.candidate_app_sessions_expiry_ck
alter table public.candidate_app_sessions add constraint candidate_app_sessions_expiry_ck CHECK (expires_at_utc <= absolute_expires_at_utc);

-- public.candidate_app_sessions.candidate_app_sessions_federated_context_ck
alter table public.candidate_app_sessions add constraint candidate_app_sessions_federated_context_ck CHECK (auth_source = 'LOCAL'::text AND global_account_identity_hmac IS NULL AND global_session_identity_hmac IS NULL AND membership_id IS NULL AND membership_generation IS NULL AND route_version IS NULL AND session_epoch IS NULL OR auth_source = 'CONTROL_PLANE'::text AND octet_length(global_account_identity_hmac) = 32 AND octet_length(global_session_identity_hmac) = 32 AND membership_id IS NOT NULL AND membership_generation >= 1 AND route_version >= 1 AND session_epoch >= 1);

-- public.candidate_app_sessions.candidate_app_sessions_refresh_hash_ck
alter table public.candidate_app_sessions add constraint candidate_app_sessions_refresh_hash_ck CHECK (octet_length(refresh_token_hash) = 32);

-- public.candidate_app_sessions.candidate_app_sessions_rotation_ck
alter table public.candidate_app_sessions add constraint candidate_app_sessions_rotation_ck CHECK (rotation >= 0);

-- public.candidate_app_sessions.candidate_app_sessions_status_ck
alter table public.candidate_app_sessions add constraint candidate_app_sessions_status_ck CHECK (status = ANY (ARRAY['ACTIVE'::text, 'ROTATED'::text, 'REVOKED'::text, 'EXPIRED'::text]));

-- public.candidate_approval_requests.candidate_approval_requests_email_ck
alter table public.candidate_approval_requests add constraint candidate_approval_requests_email_ck CHECK (manager_email_normalized IS NULL OR manager_email_normalized = lower(btrim(manager_email_normalized)) AND manager_email_normalized ~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$'::text);

-- public.candidate_approval_requests.candidate_approval_requests_email_method_ck
alter table public.candidate_approval_requests add constraint candidate_approval_requests_email_method_ck CHECK (method <> 'EMAIL'::text OR manager_email_normalized IS NOT NULL AND token_hash IS NOT NULL AND expires_at_utc IS NOT NULL);

-- public.candidate_approval_requests.candidate_approval_requests_generation_ck
alter table public.candidate_approval_requests add constraint candidate_approval_requests_generation_ck CHECK (workflow_generation > 0);

-- public.candidate_approval_requests.candidate_approval_requests_live_binding_ck
alter table public.candidate_approval_requests add constraint candidate_approval_requests_live_binding_ck CHECK ((state <> ALL (ARRAY['PENDING'::text, 'APPROVED'::text])) OR review_manifest_sha256 IS NOT NULL AND cardinality(required_component_ids) > 0 AND jsonb_array_length(required_component_manifest_json) > 0);

-- public.candidate_approval_requests.candidate_approval_requests_method_ck
alter table public.candidate_approval_requests add constraint candidate_approval_requests_method_ck CHECK (method = ANY (ARRAY['PHONE'::text, 'EMAIL'::text]));

-- public.candidate_approval_requests.candidate_approval_requests_renewal_ck
alter table public.candidate_approval_requests add constraint candidate_approval_requests_renewal_ck CHECK (renewal_count >= 0 AND renewal_count <= 100);

-- public.candidate_approval_requests.candidate_approval_requests_request_generation_ck
alter table public.candidate_approval_requests add constraint candidate_approval_requests_request_generation_ck CHECK (request_generation > 0);

-- public.candidate_approval_requests.candidate_approval_requests_required_manifest_ck
alter table public.candidate_approval_requests add constraint candidate_approval_requests_required_manifest_ck CHECK (jsonb_typeof(required_component_manifest_json) = 'array'::text AND jsonb_typeof(review_progress_json) = 'object'::text);

-- public.candidate_approval_requests.candidate_approval_requests_resend_ck
alter table public.candidate_approval_requests add constraint candidate_approval_requests_resend_ck CHECK (resend_count >= 0 AND resend_count <= 5);

-- public.candidate_approval_requests.candidate_approval_requests_review_manifest_hash_ck
alter table public.candidate_approval_requests add constraint candidate_approval_requests_review_manifest_hash_ck CHECK (review_manifest_sha256 IS NULL OR octet_length(review_manifest_sha256) = 32);

-- public.candidate_approval_requests.candidate_approval_requests_review_timesheet_hash_ck
alter table public.candidate_approval_requests add constraint candidate_approval_requests_review_timesheet_hash_ck CHECK (manager_review_timesheet_sha256 IS NULL OR octet_length(manager_review_timesheet_sha256) = 32);

-- public.candidate_approval_requests.candidate_approval_requests_state_ck
alter table public.candidate_approval_requests add constraint candidate_approval_requests_state_ck CHECK (state = ANY (ARRAY['PENDING'::text, 'APPROVED'::text, 'REFUSED'::text, 'CANCELLED'::text, 'EXPIRED'::text, 'SUPERSEDED'::text]));

-- public.candidate_approval_requests.candidate_approval_requests_token_ck
alter table public.candidate_approval_requests add constraint candidate_approval_requests_token_ck CHECK (token_hash IS NULL OR octet_length(token_hash) = 32);

-- public.candidate_auth_challenges.candidate_auth_challenges_attempt_ck
alter table public.candidate_auth_challenges add constraint candidate_auth_challenges_attempt_ck CHECK (attempt_count >= 0 AND attempt_count <= 5);

-- public.candidate_auth_challenges.candidate_auth_challenges_email_ck
alter table public.candidate_auth_challenges add constraint candidate_auth_challenges_email_ck CHECK (email_normalized = lower(btrim(email_normalized)) AND email_normalized ~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$'::text);

-- public.candidate_auth_challenges.candidate_auth_challenges_environment_ck
alter table public.candidate_auth_challenges add constraint candidate_auth_challenges_environment_ck CHECK (environment = ANY (ARRAY['TEST'::text, 'LIVE'::text]));

-- public.candidate_auth_challenges.candidate_auth_challenges_purpose_ck
alter table public.candidate_auth_challenges add constraint candidate_auth_challenges_purpose_ck CHECK (purpose = ANY (ARRAY['ACTIVATE'::text, 'RESET'::text, 'RECOVERY'::text]));

-- public.candidate_auth_challenges.candidate_auth_challenges_resend_ck
alter table public.candidate_auth_challenges add constraint candidate_auth_challenges_resend_ck CHECK (resend_count >= 0 AND resend_count <= 5);

-- public.candidate_auth_challenges.candidate_auth_challenges_state_ck
alter table public.candidate_auth_challenges add constraint candidate_auth_challenges_state_ck CHECK (state = ANY (ARRAY['PENDING'::text, 'VERIFIED'::text, 'CONSUMED'::text, 'EXPIRED'::text, 'SUPERSEDED'::text]));

-- public.candidate_auth_challenges.candidate_auth_challenges_token_hash_ck
alter table public.candidate_auth_challenges add constraint candidate_auth_challenges_token_hash_ck CHECK (octet_length(token_hash) = 32);

-- public.candidate_daily_availability_days.candidate_daily_availability_days_changed_by_ck
alter table public.candidate_daily_availability_days add constraint candidate_daily_availability_days_changed_by_ck CHECK (changed_by_class = ANY (ARRAY['CANDIDATE'::text, 'LEGACY_ADAPTER'::text, 'SIGNED_SYSTEM'::text]));

-- public.candidate_daily_availability_days.candidate_daily_availability_days_environment_ck
alter table public.candidate_daily_availability_days add constraint candidate_daily_availability_days_environment_ck CHECK (environment = ANY (ARRAY['TEST'::text, 'LIVE'::text]));

-- public.candidate_daily_availability_days.candidate_daily_availability_days_hash_ck
alter table public.candidate_daily_availability_days add constraint candidate_daily_availability_days_hash_ck CHECK (row_hash ~ '^[a-f0-9]{64}$'::text);

-- public.candidate_daily_availability_days.candidate_daily_availability_days_preference_ck
alter table public.candidate_daily_availability_days add constraint candidate_daily_availability_days_preference_ck CHECK (preference = ANY (ARRAY['PENDING'::text, 'NOT_AVAILABLE'::text, 'LONG_DAY'::text, 'NIGHT'::text, 'LONG_DAY_OR_NIGHT'::text]));

-- public.candidate_daily_availability_days.candidate_daily_availability_days_source_ck
alter table public.candidate_daily_availability_days add constraint candidate_daily_availability_days_source_ck CHECK (source_class = ANY (ARRAY['CANDIDATE'::text, 'LEGACY_ADAPTER'::text, 'SIGNED_SYSTEM'::text]));

-- public.candidate_daily_availability_days.candidate_daily_availability_days_version_ck
alter table public.candidate_daily_availability_days add constraint candidate_daily_availability_days_version_ck CHECK (availability_version > 0);

-- public.candidate_daily_command_receipts.candidate_daily_command_receipts_actor_ck
alter table public.candidate_daily_command_receipts add constraint candidate_daily_command_receipts_actor_ck CHECK (actor_class = ANY (ARRAY['CANDIDATE'::text, 'LEGACY_ADAPTER'::text, 'SIGNED_SYSTEM'::text]));

-- public.candidate_daily_command_receipts.candidate_daily_command_receipts_environment_ck
alter table public.candidate_daily_command_receipts add constraint candidate_daily_command_receipts_environment_ck CHECK (environment = ANY (ARRAY['TEST'::text, 'LIVE'::text]));

-- public.candidate_daily_command_receipts.candidate_daily_command_receipts_key_ck
alter table public.candidate_daily_command_receipts add constraint candidate_daily_command_receipts_key_ck CHECK (length(idempotency_key) >= 16 AND length(idempotency_key) <= 160);

-- public.candidate_daily_command_receipts.candidate_daily_command_receipts_request_ck
alter table public.candidate_daily_command_receipts add constraint candidate_daily_command_receipts_request_ck CHECK (request_sha256 ~ '^[a-f0-9]{64}$'::text);

-- public.candidate_daily_command_receipts.candidate_daily_command_receipts_source_ck
alter table public.candidate_daily_command_receipts add constraint candidate_daily_command_receipts_source_ck CHECK (source_system IS NULL OR NULLIF(btrim(source_event_id), ''::text) IS NOT NULL AND NULLIF(btrim(source_revision), ''::text) IS NOT NULL AND source_event_time IS NOT NULL AND NULLIF(btrim(item_key), ''::text) IS NOT NULL);

-- public.candidate_daily_command_receipts.candidate_daily_command_receipts_state_ck
alter table public.candidate_daily_command_receipts add constraint candidate_daily_command_receipts_state_ck CHECK (state = ANY (ARRAY['IN_PROGRESS'::text, 'COMPLETED'::text, 'FAILED_FINAL'::text]));

-- public.candidate_daily_command_receipts.candidate_daily_command_receipts_terminal_ck
alter table public.candidate_daily_command_receipts add constraint candidate_daily_command_receipts_terminal_ck CHECK (state = 'IN_PROGRESS'::text AND terminal_body_json IS NULL AND terminal_body_sha256 IS NULL AND completed_at_utc IS NULL OR (state = ANY (ARRAY['COMPLETED'::text, 'FAILED_FINAL'::text])) AND terminal_body_json IS NOT NULL AND terminal_body_sha256 ~ '^[a-f0-9]{64}$'::text AND completed_at_utc IS NOT NULL);

-- public.candidate_daily_command_receipts.candidate_daily_command_receipts_version_ck
alter table public.candidate_daily_command_receipts add constraint candidate_daily_command_receipts_version_ck CHECK (canonical_version_before >= 0 AND (canonical_version_after IS NULL OR canonical_version_after >= canonical_version_before));

-- public.candidate_daily_rota_days.candidate_daily_rota_days_action_kind_ck
alter table public.candidate_daily_rota_days add constraint candidate_daily_rota_days_action_kind_ck CHECK (action_target_kind IS NULL OR (action_target_kind = ANY (ARRAY['TIMESHEET_DETAIL'::text, 'CONTRACT_WEEK_DETAIL'::text, 'WORKFLOW_DETAIL'::text])));

-- public.candidate_daily_rota_days.candidate_daily_rota_days_action_shape_ck
alter table public.candidate_daily_rota_days add constraint candidate_daily_rota_days_action_shape_ck CHECK (action_target_kind IS NULL AND action_timesheet_id IS NULL AND action_contract_week_id IS NULL AND action_workflow_id IS NULL AND action_workflow_generation IS NULL AND action_row_signature IS NULL OR action_target_kind = 'TIMESHEET_DETAIL'::text AND action_timesheet_id IS NOT NULL AND action_contract_week_id IS NULL AND action_workflow_generation IS NULL AND action_row_signature ~ '^[a-f0-9]{32}$'::text OR action_target_kind = 'CONTRACT_WEEK_DETAIL'::text AND action_contract_week_id IS NOT NULL AND action_workflow_generation IS NULL AND action_row_signature ~ '^[a-f0-9]{32}$'::text OR action_target_kind = 'WORKFLOW_DETAIL'::text AND action_workflow_id IS NOT NULL AND action_workflow_generation > 0 AND action_timesheet_id IS NULL AND action_contract_week_id IS NULL AND action_row_signature ~ '^[a-f0-9]{32}$'::text);

-- public.candidate_daily_rota_days.candidate_daily_rota_days_booking_ck
alter table public.candidate_daily_rota_days add constraint candidate_daily_rota_days_booking_ck CHECK (booked AND NULLIF(btrim(booking_id), ''::text) IS NOT NULL AND shift_starts_at IS NOT NULL AND shift_ends_at IS NOT NULL AND shift_ends_at > shift_starts_at OR NOT booked AND booking_id IS NULL AND shift_starts_at IS NULL AND shift_ends_at IS NULL);

-- public.candidate_daily_rota_days.candidate_daily_rota_days_hash_ck
alter table public.candidate_daily_rota_days add constraint candidate_daily_rota_days_hash_ck CHECK (source_row_hash ~ '^[a-f0-9]{64}$'::text);

-- public.candidate_daily_rota_days.candidate_daily_rota_days_text_bounds_ck
alter table public.candidate_daily_rota_days add constraint candidate_daily_rota_days_text_bounds_ck CHECK (length(COALESCE(booking_id, ''::text)) <= 128 AND length(COALESCE(shift_info, ''::text)) <= 256 AND length(COALESCE(hospital, ''::text)) <= 160 AND length(COALESCE(ward, ''::text)) <= 160 AND length(COALESCE(job_title, ''::text)) <= 160 AND length(COALESCE(booking_ref, ''::text)) <= 128 AND length(COALESCE(shift_type, ''::text)) <= 80);

-- public.candidate_daily_rota_generations.candidate_daily_rota_generations_day_count_ck
alter table public.candidate_daily_rota_generations add constraint candidate_daily_rota_generations_day_count_ck CHECK (expected_day_count = 14 AND actual_day_count >= 0 AND actual_day_count <= 14 AND (state <> 'ACTIVE'::text OR actual_day_count = 14));

-- public.candidate_daily_rota_generations.candidate_daily_rota_generations_environment_ck
alter table public.candidate_daily_rota_generations add constraint candidate_daily_rota_generations_environment_ck CHECK (environment = ANY (ARRAY['TEST'::text, 'LIVE'::text]));

-- public.candidate_daily_rota_generations.candidate_daily_rota_generations_hash_ck
alter table public.candidate_daily_rota_generations add constraint candidate_daily_rota_generations_hash_ck CHECK (source_hash ~ '^[a-f0-9]{64}$'::text AND generation_row_hash ~ '^[a-f0-9]{64}$'::text);

-- public.candidate_daily_rota_generations.candidate_daily_rota_generations_state_ck
alter table public.candidate_daily_rota_generations add constraint candidate_daily_rota_generations_state_ck CHECK (state = ANY (ARRAY['BUILDING'::text, 'ACTIVE'::text, 'SUPERSEDED'::text, 'REJECTED'::text]));

-- public.candidate_daily_rota_generations.candidate_daily_rota_generations_version_ck
alter table public.candidate_daily_rota_generations add constraint candidate_daily_rota_generations_version_ck CHECK (generation_version > 0);

-- public.candidate_daily_rota_generations.candidate_daily_rota_generations_window_ck
alter table public.candidate_daily_rota_generations add constraint candidate_daily_rota_generations_window_ck CHECK ((window_end - window_start) = 13);

-- public.candidate_daily_sheet_projection_outbox.candidate_daily_sheet_projection_outbox_attempts_ck
alter table public.candidate_daily_sheet_projection_outbox add constraint candidate_daily_sheet_projection_outbox_attempts_ck CHECK (delivery_attempt_count >= 0 AND delivery_attempt_count <= 12 AND deferral_count >= 0);

-- public.candidate_daily_sheet_projection_outbox.candidate_daily_sheet_projection_outbox_environment_ck
alter table public.candidate_daily_sheet_projection_outbox add constraint candidate_daily_sheet_projection_outbox_environment_ck CHECK (environment = ANY (ARRAY['TEST'::text, 'LIVE'::text]));

-- public.candidate_daily_sheet_projection_outbox.candidate_daily_sheet_projection_outbox_lease_ck
alter table public.candidate_daily_sheet_projection_outbox add constraint candidate_daily_sheet_projection_outbox_lease_ck CHECK (state = 'CLAIMED'::text AND NULLIF(btrim(lease_owner), ''::text) IS NOT NULL AND NULLIF(btrim(lease_token), ''::text) IS NOT NULL AND lease_expires_at_utc IS NOT NULL OR state <> 'CLAIMED'::text);

-- public.candidate_daily_sheet_projection_outbox.candidate_daily_sheet_projection_outbox_operation_ck
alter table public.candidate_daily_sheet_projection_outbox add constraint candidate_daily_sheet_projection_outbox_operation_ck CHECK (operation = 'SET_AVAILABILITY'::text);

-- public.candidate_daily_sheet_projection_outbox.candidate_daily_sheet_projection_outbox_overlay_ck
alter table public.candidate_daily_sheet_projection_outbox add constraint candidate_daily_sheet_projection_outbox_overlay_ck CHECK (state = 'DEFERRED_OVERLAY'::text AND overlay_generation_id IS NOT NULL AND overlay_generation_version IS NOT NULL AND overlay_source_row_hash ~ '^[a-f0-9]{64}$'::text OR state <> 'DEFERRED_OVERLAY'::text);

-- public.candidate_daily_sheet_projection_outbox.candidate_daily_sheet_projection_outbox_preference_ck
alter table public.candidate_daily_sheet_projection_outbox add constraint candidate_daily_sheet_projection_outbox_preference_ck CHECK (preference = ANY (ARRAY['PENDING'::text, 'NOT_AVAILABLE'::text, 'LONG_DAY'::text, 'NIGHT'::text, 'LONG_DAY_OR_NIGHT'::text]));

-- public.candidate_daily_sheet_projection_outbox.candidate_daily_sheet_projection_outbox_state_ck
alter table public.candidate_daily_sheet_projection_outbox add constraint candidate_daily_sheet_projection_outbox_state_ck CHECK (state = ANY (ARRAY['PENDING'::text, 'CLAIMED'::text, 'RETRY'::text, 'DEFERRED_OVERLAY'::text, 'DELIVERED'::text, 'TERMINAL'::text]));

-- public.candidate_daily_sheet_projection_outbox.candidate_daily_sheet_projection_outbox_target_ck
alter table public.candidate_daily_sheet_projection_outbox add constraint candidate_daily_sheet_projection_outbox_target_ck CHECK (target = 'MASTER_AVAILABILITY_SHEET'::text);

-- public.candidate_daily_sheet_projection_outbox.candidate_daily_sheet_projection_outbox_version_ck
alter table public.candidate_daily_sheet_projection_outbox add constraint candidate_daily_sheet_projection_outbox_version_ck CHECK (availability_version > 0);

-- public.candidate_notifications.candidate_notifications_dedupe_ck
alter table public.candidate_notifications add constraint candidate_notifications_dedupe_ck CHECK (NULLIF(btrim(dedupe_key), ''::text) IS NOT NULL);

-- public.candidate_notifications.candidate_notifications_deep_link_ck
alter table public.candidate_notifications add constraint candidate_notifications_deep_link_ck CHECK (jsonb_typeof(deep_link_json) = 'object'::text);

-- public.candidate_notifications.candidate_notifications_push_state_ck
alter table public.candidate_notifications add constraint candidate_notifications_push_state_ck CHECK (push_state = ANY (ARRAY['PENDING'::text, 'CLAIMED'::text, 'SENT'::text, 'SKIPPED'::text, 'FAILED'::text]));

-- public.candidate_notifications.candidate_notifications_state_ck
alter table public.candidate_notifications add constraint candidate_notifications_state_ck CHECK (state = ANY (ARRAY['UNREAD'::text, 'READ'::text, 'DISMISSED'::text]));

-- public.candidate_notifications.candidate_notifications_template_params_ck
alter table public.candidate_notifications add constraint candidate_notifications_template_params_ck CHECK (jsonb_typeof(template_params) = 'object'::text);

-- public.candidate_submission_components.candidate_submission_components_approval_request_role_ck
alter table public.candidate_submission_components add constraint candidate_submission_components_approval_request_role_ck CHECK (component_kind = 'MANAGER_SIGNATURE'::text AND approval_request_id IS NOT NULL OR component_kind <> 'MANAGER_SIGNATURE'::text AND approval_request_id IS NULL);

-- public.candidate_submission_components.candidate_submission_components_category_ck
alter table public.candidate_submission_components add constraint candidate_submission_components_category_ck CHECK (component_kind = 'EXPENSE_EVIDENCE'::text AND expense_category IS NOT NULL AND (expense_category = ANY (ARRAY['TRAVEL'::text, 'ACCOMMODATION'::text, 'OTHER'::text, 'MILEAGE'::text])) OR component_kind = 'MILEAGE_FORM'::text AND expense_category IS NOT NULL AND expense_category = 'MILEAGE'::text OR (component_kind <> ALL (ARRAY['EXPENSE_EVIDENCE'::text, 'MILEAGE_FORM'::text])) AND expense_category IS NULL);

-- public.candidate_submission_components.candidate_submission_components_digest_ck
alter table public.candidate_submission_components add constraint candidate_submission_components_digest_ck CHECK (source_content_sha256 IS NULL OR octet_length(source_content_sha256) = 32);

-- public.candidate_submission_components.candidate_submission_components_final_hash_ck
alter table public.candidate_submission_components add constraint candidate_submission_components_final_hash_ck CHECK ((final_signed_content_sha256 IS NULL OR octet_length(final_signed_content_sha256) = 32) AND (final_signed_render_input_sha256 IS NULL OR octet_length(final_signed_render_input_sha256) = 32));

-- public.candidate_submission_components.candidate_submission_components_final_ready_ck
alter table public.candidate_submission_components add constraint candidate_submission_components_final_ready_ck CHECK (final_signed_render_state <> 'READY'::text OR final_signed_storage_key IS NOT NULL AND final_signed_content_sha256 IS NOT NULL AND final_signed_media_type = 'application/pdf'::text AND final_signed_byte_size IS NOT NULL AND final_signed_page_count = 1 AND final_signed_render_input_sha256 IS NOT NULL AND NULLIF(btrim(final_signed_renderer_contract_version), ''::text) IS NOT NULL AND jsonb_typeof(final_signed_renderer_receipt_json) = 'object'::text AND final_signed_generated_at_utc IS NOT NULL);

-- public.candidate_submission_components.candidate_submission_components_final_size_ck
alter table public.candidate_submission_components add constraint candidate_submission_components_final_size_ck CHECK (final_signed_byte_size IS NULL OR final_signed_byte_size >= 1 AND final_signed_byte_size <= 52428800);

-- public.candidate_submission_components.candidate_submission_components_final_state_ck
alter table public.candidate_submission_components add constraint candidate_submission_components_final_state_ck CHECK (final_signed_render_state = ANY (ARRAY['NOT_REQUIRED'::text, 'PENDING'::text, 'READY'::text, 'FAILED'::text, 'SUPERSEDED'::text]));

-- public.candidate_submission_components.candidate_submission_components_generation_ck
alter table public.candidate_submission_components add constraint candidate_submission_components_generation_ck CHECK (workflow_generation > 0);

-- public.candidate_submission_components.candidate_submission_components_hours_role_ck
alter table public.candidate_submission_components add constraint candidate_submission_components_hours_role_ck CHECK (component_kind <> 'HOURS_TIMESHEET'::text OR document_role = 'ELECTRONIC_TIMESHEET_MANAGER_REVIEW'::text AND required = true AND review_ordinal IS NOT NULL AND review_render_state <> 'NOT_REQUIRED'::text AND final_signed_render_state <> 'NOT_REQUIRED'::text);

-- public.candidate_submission_components.candidate_submission_components_immutable_ck
alter table public.candidate_submission_components add constraint candidate_submission_components_immutable_ck CHECK (state <> 'IMMUTABLE'::text OR immutable_at_utc IS NOT NULL AND ((component_kind = ANY (ARRAY['HOURS_TIMESHEET'::text, 'EXPENSE_SUMMARY'::text])) OR storage_key IS NOT NULL AND byte_size IS NOT NULL AND source_content_sha256 IS NOT NULL OR source_component_id IS NOT NULL));

-- public.candidate_submission_components.candidate_submission_components_kind_ck
alter table public.candidate_submission_components add constraint candidate_submission_components_kind_ck CHECK (component_kind = ANY (ARRAY['HOURS_TIMESHEET'::text, 'EXPENSE_SUMMARY'::text, 'MILEAGE_FORM'::text, 'EXPENSE_EVIDENCE'::text, 'SIGNED_RETURN'::text, 'MANAGER_SIGNATURE'::text, 'CANDIDATE_SIGNATURE'::text, 'PAPER_DOCUMENT'::text]));

-- public.candidate_submission_components.candidate_submission_components_number_ck
alter table public.candidate_submission_components add constraint candidate_submission_components_number_ck CHECK (component_no > 0);

-- public.candidate_submission_components.candidate_submission_components_paper_page_ck
alter table public.candidate_submission_components add constraint candidate_submission_components_paper_page_ck CHECK (component_kind = 'SIGNED_RETURN'::text AND NULLIF(btrim(paper_return_page_key), ''::text) IS NOT NULL OR component_kind <> 'SIGNED_RETURN'::text AND paper_return_page_key IS NULL);

-- public.candidate_submission_components.candidate_submission_components_render_input_match_ck
alter table public.candidate_submission_components add constraint candidate_submission_components_render_input_match_ck CHECK (review_render_state <> 'READY'::text OR final_signed_render_state <> 'READY'::text OR review_render_input_sha256 = final_signed_render_input_sha256);

-- public.candidate_submission_components.candidate_submission_components_required_render_ck
alter table public.candidate_submission_components add constraint candidate_submission_components_required_render_ck CHECK (required = false OR review_ordinal IS NOT NULL AND review_render_state <> 'NOT_REQUIRED'::text AND final_signed_render_state <> 'NOT_REQUIRED'::text);

-- public.candidate_submission_components.candidate_submission_components_review_hash_ck
alter table public.candidate_submission_components add constraint candidate_submission_components_review_hash_ck CHECK ((review_content_sha256 IS NULL OR octet_length(review_content_sha256) = 32) AND (review_render_input_sha256 IS NULL OR octet_length(review_render_input_sha256) = 32));

-- public.candidate_submission_components.candidate_submission_components_review_ordinal_ck
alter table public.candidate_submission_components add constraint candidate_submission_components_review_ordinal_ck CHECK (review_ordinal IS NULL OR review_ordinal > 0);

-- public.candidate_submission_components.candidate_submission_components_review_ready_ck
alter table public.candidate_submission_components add constraint candidate_submission_components_review_ready_ck CHECK (review_render_state <> 'READY'::text OR review_storage_key IS NOT NULL AND review_content_sha256 IS NOT NULL AND (review_media_type = 'application/pdf'::text OR review_media_type ~~ 'image/%'::text) AND review_byte_size IS NOT NULL AND review_page_count = 1 AND review_render_input_sha256 IS NOT NULL AND NULLIF(btrim(review_renderer_contract_version), ''::text) IS NOT NULL AND jsonb_typeof(review_renderer_receipt_json) = 'object'::text AND review_generated_at_utc IS NOT NULL);

-- public.candidate_submission_components.candidate_submission_components_review_size_ck
alter table public.candidate_submission_components add constraint candidate_submission_components_review_size_ck CHECK (review_byte_size IS NULL OR review_byte_size >= 1 AND review_byte_size <= 52428800);

-- public.candidate_submission_components.candidate_submission_components_review_state_ck
alter table public.candidate_submission_components add constraint candidate_submission_components_review_state_ck CHECK (review_render_state = ANY (ARRAY['NOT_REQUIRED'::text, 'PENDING'::text, 'READY'::text, 'FAILED'::text, 'SUPERSEDED'::text]));

-- public.candidate_submission_components.candidate_submission_components_role_ck
alter table public.candidate_submission_components add constraint candidate_submission_components_role_ck CHECK (document_role = ANY (ARRAY['SOURCE_EVIDENCE'::text, 'MILEAGE_CLAIM_FORM'::text, 'EXPENSE_MILEAGE_APPROVAL_SUMMARY'::text, 'SIGNED_TIMESHEET'::text, 'MANAGER_SIGNATURE'::text, 'CANDIDATE_SIGNATURE'::text, 'ELECTRONIC_TIMESHEET_MANAGER_REVIEW'::text, 'SIGNED_RETURN'::text]));

-- public.candidate_submission_components.candidate_submission_components_size_ck
alter table public.candidate_submission_components add constraint candidate_submission_components_size_ck CHECK (byte_size IS NULL OR byte_size >= 1 AND byte_size <= 15728640);

-- public.candidate_submission_components.candidate_submission_components_state_ck
alter table public.candidate_submission_components add constraint candidate_submission_components_state_ck CHECK (state = ANY (ARRAY['PENDING'::text, 'UPLOADED'::text, 'IMMUTABLE'::text, 'SUPERSEDED'::text, 'REJECTED'::text, 'ABANDONED'::text]));

-- public.candidate_submission_workflows.candidate_submission_workflows_canonical_save_ck
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_canonical_save_ck CHECK (canonical_saved_at_utc IS NULL AND canonical_save_input_sha256 IS NULL AND canonical_save_row_signature IS NULL AND canonical_save_financials_id IS NULL AND canonical_save_receipt_json IS NULL OR canonical_saved_at_utc IS NOT NULL AND canonical_save_input_sha256 IS NOT NULL AND NULLIF(btrim(canonical_save_row_signature), ''::text) IS NOT NULL AND canonical_save_financials_id IS NOT NULL AND jsonb_typeof(canonical_save_receipt_json) = 'object'::text);

-- public.candidate_submission_workflows.candidate_submission_workflows_creation_identity_ck
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_creation_identity_ck CHECK (creation_identity_json IS NULL OR jsonb_typeof(creation_identity_json) = 'object'::text);

-- public.candidate_submission_workflows.candidate_submission_workflows_creation_identity_group_ck
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_creation_identity_group_ck CHECK (creation_request_sha256 IS NULL AND creation_identity_json IS NULL OR creation_request_sha256 IS NOT NULL AND creation_identity_json IS NOT NULL);

-- public.candidate_submission_workflows.candidate_submission_workflows_creation_sha256_ck
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_creation_sha256_ck CHECK (creation_request_sha256 IS NULL OR octet_length(creation_request_sha256) = 32);

-- public.candidate_submission_workflows.candidate_submission_workflows_environment_ck
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_environment_ck CHECK (environment = ANY (ARRAY['TEST'::text, 'LIVE'::text]));

-- public.candidate_submission_workflows.candidate_submission_workflows_generation_ck
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_generation_ck CHECK (generation > 0);

-- public.candidate_submission_workflows.candidate_submission_workflows_hashes_ck
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_hashes_ck CHECK ((immutable_submission_sha256 IS NULL OR octet_length(immutable_submission_sha256) = 32) AND (policy_snapshot_sha256 IS NULL OR octet_length(policy_snapshot_sha256) = 32) AND (candidate_signature_sha256 IS NULL OR octet_length(candidate_signature_sha256) = 32) AND (review_manifest_sha256 IS NULL OR octet_length(review_manifest_sha256) = 32) AND (paper_return_manifest_sha256 IS NULL OR octet_length(paper_return_manifest_sha256) = 32) AND (manager_signature_sha256 IS NULL OR octet_length(manager_signature_sha256) = 32) AND (daily_context_sha256 IS NULL OR octet_length(daily_context_sha256) = 32) AND (canonical_financial_sha256 IS NULL OR octet_length(canonical_financial_sha256) = 32) AND (canonical_save_input_sha256 IS NULL OR octet_length(canonical_save_input_sha256) = 32));

-- public.candidate_submission_workflows.candidate_submission_workflows_idempotency_ck
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_idempotency_ck CHECK (NULLIF(btrim(idempotency_key), ''::text) IS NOT NULL);

-- public.candidate_submission_workflows.candidate_submission_workflows_identity_shape_ck
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_identity_shape_ck CHECK ((workflow_kind = ANY (ARRAY['CONTRACT_HOURS'::text, 'CONTRACT_EXPENSE'::text, 'CONTRACT_COMBINED'::text])) AND contract_id IS NOT NULL AND contract_week_id IS NOT NULL AND week_ending_date IS NOT NULL OR workflow_kind = 'DAILY'::text AND target_timesheet_id IS NOT NULL AND anchor_timesheet_id = target_timesheet_id AND work_date IS NOT NULL AND contract_week_id IS NULL AND week_ending_date IS NULL);

-- public.candidate_submission_workflows.candidate_submission_workflows_immutable_submission_ck
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_immutable_submission_ck CHECK (immutable_submission_json IS NULL OR jsonb_typeof(immutable_submission_json) = 'object'::text);

-- public.candidate_submission_workflows.candidate_submission_workflows_input_ck
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_input_ck CHECK (jsonb_typeof(input_snapshot_json) = 'object'::text);

-- public.candidate_submission_workflows.candidate_submission_workflows_issues_ck
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_issues_ck CHECK (jsonb_typeof(issue_codes) = 'array'::text);

-- public.candidate_submission_workflows.candidate_submission_workflows_kind_ck
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_kind_ck CHECK (workflow_kind = ANY (ARRAY['CONTRACT_HOURS'::text, 'CONTRACT_EXPENSE'::text, 'CONTRACT_COMBINED'::text, 'DAILY'::text]));

-- public.candidate_submission_workflows.candidate_submission_workflows_kind_scope_route_ck
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_kind_scope_route_ck CHECK ((workflow_kind = ANY (ARRAY['CONTRACT_HOURS'::text, 'CONTRACT_EXPENSE'::text, 'CONTRACT_COMBINED'::text])) AND scope = 'WEEKLY'::text OR workflow_kind = 'DAILY'::text AND scope = 'DAILY'::text AND (route = ANY (ARRAY['PHONE'::text, 'EMAIL'::text])));

-- public.candidate_submission_workflows.candidate_submission_workflows_manager_signature_ck
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_manager_signature_ck CHECK (manager_signature_component_id IS NULL OR manager_signature_sha256 IS NOT NULL AND NULLIF(btrim(manager_name), ''::text) IS NOT NULL AND NULLIF(btrim(manager_position), ''::text) IS NOT NULL AND manager_approved_at_utc IS NOT NULL);

-- public.candidate_submission_workflows.candidate_submission_workflows_paper_manifest_ck
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_paper_manifest_ck CHECK (paper_return_manifest_json IS NULL AND paper_return_manifest_sha256 IS NULL OR jsonb_typeof(paper_return_manifest_json) = 'object'::text AND jsonb_typeof(paper_return_manifest_json -> 'pages'::text) = 'array'::text AND jsonb_array_length(paper_return_manifest_json -> 'pages'::text) > 0 AND paper_return_manifest_sha256 IS NOT NULL);

-- public.candidate_submission_workflows.candidate_submission_workflows_policy_ck
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_policy_ck CHECK (jsonb_typeof(policy_snapshot_json) = 'object'::text);

-- public.candidate_submission_workflows.candidate_submission_workflows_replacement_not_self_ck
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_replacement_not_self_ck CHECK (replacement_of_workflow_id IS NULL OR replacement_of_workflow_id <> id);

-- public.candidate_submission_workflows.candidate_submission_workflows_route_ck
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_route_ck CHECK (route = ANY (ARRAY['ELECTRONIC'::text, 'PHONE'::text, 'EMAIL'::text, 'PAPER'::text]));

-- public.candidate_submission_workflows.candidate_submission_workflows_scope_ck
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_scope_ck CHECK (scope = ANY (ARRAY['WEEKLY'::text, 'DAILY'::text]));

-- public.candidate_submission_workflows.candidate_submission_workflows_state_ck
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_state_ck CHECK (state = ANY (ARRAY['CREATED'::text, 'WORKER_DRAFT'::text, 'WORKER_SUBMITTED'::text, 'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT'::text, 'READY_FOR_MANAGER_APPROVAL'::text, 'AWAITING_MANAGER_APPROVAL'::text, 'MANAGER_APPROVED'::text, 'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT'::text, 'READY_TO_FINALISE'::text, 'AWAITING_PAPER_RETURN'::text, 'RECEIVED'::text, 'FINALISED'::text, 'REFUSED'::text, 'REJECTED'::text, 'CANCELLED'::text, 'EXPIRED'::text, 'SUPERSEDED'::text]));

-- public.candidate_submission_workflows.candidate_submission_workflows_worker_signature_ck
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_worker_signature_ck CHECK (candidate_signature_component_id IS NULL OR candidate_signature_sha256 IS NOT NULL AND candidate_signed_at_utc IS NOT NULL);

-- public.candidates.candidates_pay_method_check
alter table public.candidates add constraint candidates_pay_method_check CHECK (pay_method = ANY (ARRAY['PAYE'::text, 'UMBRELLA'::text]));

-- public.candidates.chk_candidates_tms_ref_format
alter table public.candidates add constraint chk_candidates_tms_ref_format CHECK (tms_ref IS NULL OR tms_ref ~ '^CCR-[0-9]{5,}$'::text) NOT VALID;

-- public.client_settings.chk_client_settings_wew
alter table public.client_settings add constraint chk_client_settings_wew CHECK (week_ending_weekday IS NULL OR week_ending_weekday >= 0 AND week_ending_weekday <= 6);

-- public.client_settings.client_settings_apply_erni_to_check
alter table public.client_settings add constraint client_settings_apply_erni_to_check CHECK (apply_erni_to = ANY (ARRAY['PAYE_ONLY'::text, 'ALL'::text, 'NONE'::text]));

-- public.client_settings.client_settings_apply_holiday_to_check
alter table public.client_settings add constraint client_settings_apply_holiday_to_check CHECK (apply_holiday_to = ANY (ARRAY['PAYE_ONLY'::text, 'ALL'::text, 'NONE'::text]));

-- public.client_settings.client_settings_bh_source_check
alter table public.client_settings add constraint client_settings_bh_source_check CHECK (bh_source = ANY (ARRAY['MANUAL'::text, 'FEED'::text]));

-- public.client_settings.client_settings_candidate_daily_method_ck
alter table public.client_settings add constraint client_settings_candidate_daily_method_ck CHECK (allow_daily_manager_authorise_on_phone OR allow_daily_manager_authorise_by_email);

-- public.client_settings.client_settings_candidate_expense_email_ck
alter table public.client_settings add constraint client_settings_candidate_expense_email_ck CHECK (candidate_expense_invoice_email IS NULL OR btrim(candidate_expense_invoice_email) = ''::text OR candidate_expense_invoice_email ~* '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$'::text);

-- public.client_settings.client_settings_candidate_manager_policy_object_ck
alter table public.client_settings add constraint client_settings_candidate_manager_policy_object_ck CHECK (jsonb_typeof(candidate_manager_approval_policy_json) = 'object'::text);

-- public.clients.chk_clients_cli_ref_format
alter table public.clients add constraint chk_clients_cli_ref_format CHECK (cli_ref IS NULL OR cli_ref ~ '^CLI-[0-9]{5,}$'::text);

-- public.comms_outbox.comms_outbox_channel_chk
alter table public.comms_outbox add constraint comms_outbox_channel_chk CHECK (channel = ANY (ARRAY['WHATSAPP'::text, 'SMS'::text, 'VOICE'::text]));

-- public.comms_outbox.comms_outbox_status_chk
alter table public.comms_outbox add constraint comms_outbox_status_chk CHECK (status = ANY (ARRAY['QUEUED'::text, 'SENT'::text, 'DELIVERED'::text, 'READ'::text, 'FAILED'::text]));

-- public.contract_weeks.contract_weeks_additional_seq_check
alter table public.contract_weeks add constraint contract_weeks_additional_seq_check CHECK (additional_seq >= 0);

-- public.contracts.contracts_bucket_labels_json_chk
alter table public.contracts add constraint contracts_bucket_labels_json_chk CHECK (bucket_labels_json IS NULL OR jsonb_typeof(bucket_labels_json) = 'object'::text AND (bucket_labels_json - ARRAY['day'::text, 'night'::text, 'sat'::text, 'sun'::text, 'bh'::text]) = '{}'::jsonb AND bucket_labels_json ? 'day'::text AND bucket_labels_json ? 'night'::text AND bucket_labels_json ? 'sat'::text AND bucket_labels_json ? 'sun'::text AND bucket_labels_json ? 'bh'::text AND NULLIF(btrim(bucket_labels_json ->> 'day'::text), ''::text) IS NOT NULL AND NULLIF(btrim(bucket_labels_json ->> 'night'::text), ''::text) IS NOT NULL AND NULLIF(btrim(bucket_labels_json ->> 'sat'::text), ''::text) IS NOT NULL AND NULLIF(btrim(bucket_labels_json ->> 'sun'::text), ''::text) IS NOT NULL AND NULLIF(btrim(bucket_labels_json ->> 'bh'::text), ''::text) IS NOT NULL);

-- public.contracts.contracts_candidate_expense_email_ck
alter table public.contracts add constraint contracts_candidate_expense_email_ck CHECK (candidate_expense_invoice_email_override IS NULL OR btrim(candidate_expense_invoice_email_override) = ''::text OR candidate_expense_invoice_email_override ~* '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$'::text);

-- public.contracts.contracts_candidate_manager_policy_object_ck
alter table public.contracts add constraint contracts_candidate_manager_policy_object_ck CHECK (jsonb_typeof(candidate_manager_approval_policy_json) = 'object'::text);

-- public.contracts.contracts_mileage_nonneg_chk
alter table public.contracts add constraint contracts_mileage_nonneg_chk CHECK ((mileage_pay_rate IS NULL OR mileage_pay_rate >= 0::numeric) AND (mileage_charge_rate IS NULL OR mileage_charge_rate >= 0::numeric));

-- public.contracts.contracts_no_ts_requires_hr_chk
alter table public.contracts add constraint contracts_no_ts_requires_hr_chk CHECK (COALESCE(no_timesheet_required, false) = false OR COALESCE(autoprocess_hr, false) = true);

-- public.contracts.contracts_pay_method_snapshot_check
alter table public.contracts add constraint contracts_pay_method_snapshot_check CHECK (pay_method_snapshot = ANY (ARRAY['PAYE'::text, 'UMBRELLA'::text]));

-- public.contracts.contracts_route_one_source_chk
alter table public.contracts add constraint contracts_route_one_source_chk CHECK ((
CASE
    WHEN COALESCE(is_nhsp, false) THEN 1
    ELSE 0
END +
CASE
    WHEN COALESCE(autoprocess_hr, false) THEN 1
    ELSE 0
END) <= 1);

-- public.contracts.contracts_ts_query_email_override_ck
alter table public.contracts add constraint contracts_ts_query_email_override_ck CHECK (NOT send_ts_queries_to_different_email OR NULLIF(btrim(ts_queries_alt_email_address), ''::text) IS NOT NULL AND length(btrim(ts_queries_alt_email_address)) <= 320);

-- public.contracts.contracts_week_ending_weekday_snapshot_check
alter table public.contracts add constraint contracts_week_ending_weekday_snapshot_check CHECK (week_ending_weekday_snapshot >= 0 AND week_ending_weekday_snapshot <= 6);

-- public.contracts.contracts_weekly_source_no_ts_chk
alter table public.contracts add constraint contracts_weekly_source_no_ts_chk CHECK (weekly_timesheet_source IS NULL OR weekly_timesheet_source = 'HEALTHROSTER'::weekly_timesheet_source_enum OR COALESCE(no_timesheet_required, false) = false);

-- public.hr_imports.hr_imports_coverage_bundle_ck
alter table public.hr_imports add constraint hr_imports_coverage_bundle_ck CHECK (coverage_locked_at IS NULL OR coverage_mode IS NOT NULL AND coverage_start_date IS NOT NULL AND coverage_end_date IS NOT NULL AND coverage_fingerprint IS NOT NULL AND length(coverage_fingerprint) = 64 AND coverage_operation_key IS NOT NULL AND coverage_request_hash IS NOT NULL);

-- public.hr_imports.hr_imports_coverage_dates_ck
alter table public.hr_imports add constraint hr_imports_coverage_dates_ck CHECK (coverage_start_date IS NULL OR coverage_end_date IS NULL OR coverage_start_date <= coverage_end_date);

-- public.hr_imports.hr_imports_coverage_mode_ck
alter table public.hr_imports add constraint hr_imports_coverage_mode_ck CHECK (coverage_mode IS NULL OR (coverage_mode = ANY (ARRAY['COMPLETE_ALL'::text, 'COMPLETE_SELECTED_CANDIDATES'::text, 'PARTIAL'::text])));

-- public.hr_imports.hr_imports_no_self_supersede_ck
alter table public.hr_imports add constraint hr_imports_no_self_supersede_ck CHECK (supersedes_import_id IS NULL OR supersedes_import_id <> id);

-- public.hr_imports.hr_imports_revision_no_positive_ck
alter table public.hr_imports add constraint hr_imports_revision_no_positive_ck CHECK (revision_no IS NULL OR revision_no > 0);

-- public.hr_imports.hr_imports_source_hash_ck
alter table public.hr_imports add constraint hr_imports_source_hash_ck CHECK (source_file_sha256 IS NULL OR source_file_sha256 ~ '^[0-9a-f]{64}$'::text);

-- public.hr_issue_email_deliveries.hr_issue_email_deliveries_scope_ck
alter table public.hr_issue_email_deliveries add constraint hr_issue_email_deliveries_scope_ck CHECK ((recipient_scope = ANY (ARRAY['CLIENT_DEFAULT'::text, 'CONTRACT_OVERRIDE'::text])) AND reminder_sequence >= 0 AND length(recipient_email) >= 3 AND length(recipient_email) <= 320 AND length(recipient_route_fingerprint) = 64 AND recipient_route_fingerprint ~ '^[0-9a-f]{64}$'::text AND length(issue_set_fingerprint) = 64 AND issue_set_fingerprint ~ '^[0-9a-f]{64}$'::text AND (status = ANY (ARRAY['QUEUED'::text, 'ACCEPTED'::text, 'SENT'::text, 'FAILED'::text])));

-- public.hr_issue_emails.hr_issue_emails_history_status_ck
alter table public.hr_issue_emails add constraint hr_issue_emails_history_status_ck CHECK (delivery_history_status = ANY (ARRAY['LEGACY_UNVERIFIED'::text, 'PENDING'::text, 'SENT_VERIFIED'::text]));

-- public.hr_issue_emails.hr_issue_emails_sent_count_ck
alter table public.hr_issue_emails add constraint hr_issue_emails_sent_count_ck CHECK (sent_count >= 0);

-- public.id_consolidation_runs.id_consolidation_runs_id_ref_6digits_chk
alter table public.id_consolidation_runs add constraint id_consolidation_runs_id_ref_6digits_chk CHECK (id_ref ~ '^[0-9]{6}$'::text);

-- public.import_apply_operations.import_apply_operations_import_revision_chk
alter table public.import_apply_operations add constraint import_apply_operations_import_revision_chk CHECK (btrim(import_revision) <> ''::text);

-- public.import_apply_operations.import_apply_operations_request_hash_chk
alter table public.import_apply_operations add constraint import_apply_operations_request_hash_chk CHECK (btrim(request_hash) <> ''::text);

-- public.import_apply_operations.import_apply_operations_response_json_chk
alter table public.import_apply_operations add constraint import_apply_operations_response_json_chk CHECK (jsonb_typeof(response_json) = 'object'::text);

-- public.import_apply_operations.import_apply_operations_state_chk
alter table public.import_apply_operations add constraint import_apply_operations_state_chk CHECK (state = ANY (ARRAY['PREPARED'::text, 'SOURCE_COMMITTED_TSFIN_PENDING'::text, 'FINANCIALISED_PENDING_FINALISATION'::text, 'COMPLETE'::text, 'BLOCKED'::text, 'FAILED_BEFORE_COMMIT'::text]));

-- public.import_column_aliases.import_column_aliases_field_key_check
alter table public.import_column_aliases add constraint import_column_aliases_field_key_check CHECK (field_key = ANY (ARRAY['ASSIGNMENT'::text, 'GRADE'::text, 'REQUEST_ID'::text, 'DATE'::text, 'START'::text, 'END'::text, 'UNIT'::text, 'STAFF'::text, 'BREAK'::text, 'BOOKED_BREAK'::text, 'ACTUAL_BREAK'::text, 'ACTUAL_HOURS'::text, 'BOOKED_HOURS'::text, 'RATE'::text, 'FINALISED_DATE'::text, 'SUBMITTED_DATE'::text, 'TIMESHEET_REASON'::text, 'SKILL'::text, 'AGENCY'::text, 'REF'::text, 'UNIQUE_ID'::text, 'TRUST'::text, 'WARD'::text, 'HOURS'::text]));

-- public.import_column_aliases.import_column_aliases_system_type_check
alter table public.import_column_aliases add constraint import_column_aliases_system_type_check CHECK (system_type = ANY (ARRAY['NHSP'::text, 'HR_WEEKLY'::text, 'HR_DAILY'::text]));

-- public.import_review_action_outcomes.import_review_action_outcomes_identity_ck
alter table public.import_review_action_outcomes add constraint import_review_action_outcomes_identity_ck CHECK (length(action_id) = 64 AND action_id ~ '^[0-9a-f]{64}$'::text AND length(source_identity) >= 1 AND length(source_identity) <= 1024 AND length(evidence_fingerprint) = 64 AND evidence_fingerprint ~ '^[0-9a-f]{64}$'::text AND length(completed_label) >= 1 AND length(completed_label) <= 160 AND pg_column_size(summary_json) <= 32768);

-- public.import_review_daily_timesheet_resolutions.import_review_daily_resolution_bounds_ck
alter table public.import_review_daily_timesheet_resolutions add constraint import_review_daily_resolution_bounds_ck CHECK (length(evidence_fingerprint) = 64 AND evidence_fingerprint ~ '^[0-9a-f]{64}$'::text AND preview_generation >= 0 AND state_version > 0 AND ((status = ANY (ARRAY['CLEARED'::text, 'STALE'::text])) OR resolved_timesheet_id IS NOT NULL) AND (status <> 'APPLIED'::text OR applied_operation_id IS NOT NULL AND applied_at_utc IS NOT NULL));

-- public.import_review_daily_timesheet_resolutions.import_review_daily_resolution_method_ck
alter table public.import_review_daily_timesheet_resolutions add constraint import_review_daily_resolution_method_ck CHECK (resolution_method = ANY (ARRAY['AUTO_MATCHED'::text, 'USER_SELECTED'::text]));

-- public.import_review_daily_timesheet_resolutions.import_review_daily_resolution_status_ck
alter table public.import_review_daily_timesheet_resolutions add constraint import_review_daily_resolution_status_ck CHECK (status = ANY (ARRAY['CURRENT'::text, 'STALE'::text, 'CLEARED'::text, 'APPLIED'::text]));

-- public.import_review_decisions.import_review_decisions_bounds_ck
alter table public.import_review_decisions add constraint import_review_decisions_bounds_ck CHECK (length(action_id) = 64 AND action_id ~ '^[0-9a-f]{64}$'::text AND length(evidence_fingerprint) = 64 AND evidence_fingerprint ~ '^[0-9a-f]{64}$'::text AND preview_generation >= 0 AND pg_column_size(summary_json) <= 32768 AND (NOT selected OR selectable));

-- public.import_review_decisions.import_review_decisions_category_ck
alter table public.import_review_decisions add constraint import_review_decisions_category_ck CHECK (action_category = ANY (ARRAY['PENDING'::text, 'READY'::text, 'NO_ACTION'::text, 'EMAIL'::text, 'BLOCKED'::text]));

-- public.import_review_decisions.import_review_decisions_kind_ck
alter table public.import_review_decisions add constraint import_review_decisions_kind_ck CHECK (action_kind = ANY (ARRAY['INCLUDE_SHIFT'::text, 'EXCLUDE_SHIFT'::text, 'APPLY_AMENDMENT'::text, 'APPLY_CANCELLATION'::text, 'EMAIL_ISSUE'::text, 'EMAIL_REMINDER'::text, 'INVALIDATE_REFERENCE'::text, 'DAILY_TIMESHEET_RESOLUTION'::text, 'MARK_VALIDATION_ERROR'::text, 'NO_ACTION'::text, 'ADVISORY'::text]));

-- public.import_review_events.import_review_events_bounds_ck
alter table public.import_review_events add constraint import_review_events_bounds_ck CHECK (state_version > 0 AND length(event_code) >= 1 AND length(event_code) <= 128 AND pg_column_size(event_context_json) <= 16384);

-- public.import_review_states.import_review_states_bounds_ck
alter table public.import_review_states add constraint import_review_states_bounds_ck CHECK (state_version > 0 AND preview_generation >= 0 AND follow_up_retry_count >= 0 AND pg_column_size(ui_state_json) <= 65536 AND COALESCE(length(follow_up_error_code), 0) <= 128 AND COALESCE(length(follow_up_error_message), 0) <= 1000 AND COALESCE(length(abandoned_reason), 0) <= 500);

-- public.import_review_states.import_review_states_contract_version_ck
alter table public.import_review_states add constraint import_review_states_contract_version_ck CHECK (schema_contract_version = 'IMPORT_REVIEW_DB_V1'::text);

-- public.import_review_states.import_review_states_follow_up_ck
alter table public.import_review_states add constraint import_review_states_follow_up_ck CHECK (follow_up_status = ANY (ARRAY['NOT_REQUIRED'::text, 'PENDING'::text, 'COMPLETE'::text, 'FAILED_RETRYABLE'::text]));

-- public.import_review_states.import_review_states_status_ck
alter table public.import_review_states add constraint import_review_states_status_ck CHECK (status = ANY (ARRAY['STAGED'::text, 'IN_REVIEW'::text, 'BLOCKED'::text, 'READY'::text, 'APPLYING'::text, 'APPLIED'::text, 'ABANDONED'::text, 'SUPERSEDED'::text]));

-- public.import_review_weekly_validation_resolutions.import_review_weekly_validation_resolution_bounds_ck
alter table public.import_review_weekly_validation_resolutions add constraint import_review_weekly_validation_resolution_bounds_ck CHECK (length(evidence_fingerprint) = 64 AND evidence_fingerprint ~ '^[0-9a-f]{64}$'::text AND preview_generation >= 0 AND state_version > 0 AND (status <> 'APPLIED'::text OR applied_operation_id IS NOT NULL AND applied_at_utc IS NOT NULL));

-- public.import_review_weekly_validation_resolutions.import_review_weekly_validation_resolution_code_ck
alter table public.import_review_weekly_validation_resolutions add constraint import_review_weekly_validation_resolution_code_ck CHECK (resolution_code = 'CANDIDATE_DID_NOT_WORK'::text);

-- public.import_review_weekly_validation_resolutions.import_review_weekly_validation_resolution_status_ck
alter table public.import_review_weekly_validation_resolutions add constraint import_review_weekly_validation_resolution_status_ck CHECK (status = ANY (ARRAY['CURRENT'::text, 'STALE'::text, 'CLEARED'::text, 'APPLIED'::text]));

-- public.invoice_document_assets.invoice_document_assets_counts_ck
alter table public.invoice_document_assets add constraint invoice_document_assets_counts_ck CHECK ((original_size_bytes IS NULL OR original_size_bytes >= 0) AND (width_pixels IS NULL OR width_pixels > 0) AND (height_pixels IS NULL OR height_pixels > 0) AND (source_page_count IS NULL OR source_page_count >= 0) AND (normalised_size_bytes IS NULL OR normalised_size_bytes >= 0) AND (normalised_page_count IS NULL OR normalised_page_count >= 0));

-- public.invoice_document_assets.invoice_document_assets_error_history_ck
alter table public.invoice_document_assets add constraint invoice_document_assets_error_history_ck CHECK (error_json IS NULL OR jsonb_typeof(error_json) = 'object'::text AND (NOT error_json ? 'history'::text OR jsonb_typeof(error_json -> 'history'::text) = 'array'::text AND jsonb_array_length(error_json -> 'history'::text) <= 8));

-- public.invoice_document_assets.invoice_document_assets_hash_format_ck
alter table public.invoice_document_assets add constraint invoice_document_assets_hash_format_ck CHECK ((original_sha256 IS NULL OR original_sha256 ~ '^[0-9a-f]{64}$'::text) AND (normalised_sha256 IS NULL OR normalised_sha256 ~ '^[0-9a-f]{64}$'::text) AND (normalised_manifest_hash IS NULL OR normalised_manifest_hash ~ '^[0-9a-f]{64}$'::text));

-- public.invoice_document_assets.invoice_document_assets_manifest_array_ck
alter table public.invoice_document_assets add constraint invoice_document_assets_manifest_array_ck CHECK (jsonb_typeof(normalised_manifest_json) = 'array'::text);

-- public.invoice_document_assets.invoice_document_assets_ready_ck
alter table public.invoice_document_assets add constraint invoice_document_assets_ready_ck CHECK (status <> 'READY'::text OR normalised_size_bytes > 0 AND normalised_page_count > 0 AND ready_at_utc IS NOT NULL AND (normalised_r2_key IS NOT NULL AND normalised_sha256 IS NOT NULL AND jsonb_array_length(normalised_manifest_json) = 0 AND normalised_manifest_hash IS NULL OR normalised_r2_key IS NULL AND normalised_sha256 IS NULL AND jsonb_array_length(normalised_manifest_json) > 0 AND normalised_manifest_hash IS NOT NULL));

-- public.invoice_document_assets.invoice_document_assets_rotation_ck
alter table public.invoice_document_assets add constraint invoice_document_assets_rotation_ck CHECK (orientation_degrees IS NULL OR (orientation_degrees = ANY (ARRAY[0, 90, 180, 270])));

-- public.invoice_document_assets.invoice_document_assets_status_ck
alter table public.invoice_document_assets add constraint invoice_document_assets_status_ck CHECK (status = ANY (ARRAY['DISCOVERED'::text, 'INSPECTING'::text, 'NORMALISING'::text, 'READY'::text, 'UNSUPPORTED'::text, 'CORRUPT'::text, 'MISSING'::text, 'FAILED'::text, 'SUPERSEDED'::text]));

-- public.invoice_document_versions.invoice_document_versions_counts_ck
alter table public.invoice_document_versions add constraint invoice_document_versions_counts_ck CHECK ((size_bytes IS NULL OR size_bytes >= 0) AND (expected_page_count IS NULL OR expected_page_count >= 0) AND (page_count IS NULL OR page_count >= 0) AND (core_page_count IS NULL OR core_page_count >= 0) AND (supporting_page_count IS NULL OR supporting_page_count >= 0));

-- public.invoice_document_versions.invoice_document_versions_error_history_ck
alter table public.invoice_document_versions add constraint invoice_document_versions_error_history_ck CHECK (error_json IS NULL OR jsonb_typeof(error_json) = 'object'::text AND (NOT error_json ? 'history'::text OR jsonb_typeof(error_json -> 'history'::text) = 'array'::text AND jsonb_array_length(error_json -> 'history'::text) <= 8));

-- public.invoice_document_versions.invoice_document_versions_hash_format_ck
alter table public.invoice_document_versions add constraint invoice_document_versions_hash_format_ck CHECK (snapshot_hash ~ '^[0-9a-f]{64}$'::text AND manifest_hash ~ '^[0-9a-f]{64}$'::text AND (sha256 IS NULL OR sha256 ~ '^[0-9a-f]{64}$'::text));

-- public.invoice_document_versions.invoice_document_versions_manifest_array_ck
alter table public.invoice_document_versions add constraint invoice_document_versions_manifest_array_ck CHECK (jsonb_typeof(manifest_json) = 'array'::text);

-- public.invoice_document_versions.invoice_document_versions_purpose_ck
alter table public.invoice_document_versions add constraint invoice_document_versions_purpose_ck CHECK (purpose = ANY (ARRAY['DRAFT_PREVIEW'::text, 'FINAL_ISSUE'::text, 'TIMESHEET'::text]));

-- public.invoice_document_versions.invoice_document_versions_ready_ck
alter table public.invoice_document_versions add constraint invoice_document_versions_ready_ck CHECK (status <> 'READY'::text OR r2_key IS NOT NULL AND sha256 IS NOT NULL AND size_bytes > 0 AND page_count > 0 AND ready_at_utc IS NOT NULL AND verified_at_utc IS NOT NULL);

-- public.invoice_document_versions.invoice_document_versions_snapshot_object_ck
alter table public.invoice_document_versions add constraint invoice_document_versions_snapshot_object_ck CHECK (jsonb_typeof(snapshot_json) = 'object'::text);

-- public.invoice_document_versions.invoice_document_versions_status_ck
alter table public.invoice_document_versions add constraint invoice_document_versions_status_ck CHECK (status = ANY (ARRAY['PLANNING'::text, 'WAITING_FOR_INPUTS'::text, 'RENDERING'::text, 'ASSEMBLING'::text, 'VERIFYING'::text, 'READY'::text, 'FAILED'::text, 'SUPERSEDED'::text, 'CANCELLED'::text]));

-- public.invoice_jobs_outbox.invoice_jobs_outbox_kind_not_blank
alter table public.invoice_jobs_outbox add constraint invoice_jobs_outbox_kind_not_blank CHECK (length(btrim(kind)) > 0);

-- public.invoice_operation_chunks.invoice_operation_chunks_counts_ck
alter table public.invoice_operation_chunks add constraint invoice_operation_chunks_counts_ck CHECK (sequence_no >= 0 AND level_no >= 0 AND priority >= 0 AND priority <= 2000 AND attempt_count >= 0 AND max_attempts > 0 AND fence_token >= 0 AND operation_control_version > 0 AND (expected_page_count IS NULL OR expected_page_count >= 0) AND (actual_page_count IS NULL OR actual_page_count >= 0) AND (expected_byte_count IS NULL OR expected_byte_count >= 0) AND (actual_byte_count IS NULL OR actual_byte_count >= 0));

-- public.invoice_operation_chunks.invoice_operation_chunks_dependency_ck
alter table public.invoice_operation_chunks add constraint invoice_operation_chunks_dependency_ck CHECK (chunk_type <> 'DOCUMENT_INPUT'::text OR (status = ANY (ARRAY['WAITING'::text, 'COMPLETE'::text, 'BLOCKED'::text, 'CANCELLED'::text, 'SUPERSEDED'::text])));

-- public.invoice_operation_chunks.invoice_operation_chunks_generation_ck
alter table public.invoice_operation_chunks add constraint invoice_operation_chunks_generation_ck CHECK (plan_generation > 0);

-- public.invoice_operation_chunks.invoice_operation_chunks_json_ck
alter table public.invoice_operation_chunks add constraint invoice_operation_chunks_json_ck CHECK (jsonb_typeof(payload_json) = 'object'::text AND jsonb_typeof(progress_json) = 'object'::text AND (result_json IS NULL OR jsonb_typeof(result_json) = 'object'::text) AND (error_json IS NULL OR jsonb_typeof(error_json) = 'object'::text AND (NOT error_json ? 'history'::text OR jsonb_typeof(error_json -> 'history'::text) = 'array'::text AND jsonb_array_length(error_json -> 'history'::text) <= 8)));

-- public.invoice_operation_chunks.invoice_operation_chunks_manifest_generation_ck
alter table public.invoice_operation_chunks add constraint invoice_operation_chunks_manifest_generation_ck CHECK (manifest_generation >= 0);

-- public.invoice_operation_chunks.invoice_operation_chunks_manifest_member_ck
alter table public.invoice_operation_chunks add constraint invoice_operation_chunks_manifest_member_ck CHECK (NOT is_manifest_member OR manifest_generation > 0);

-- public.invoice_operation_chunks.invoice_operation_chunks_replacement_shape_ck
alter table public.invoice_operation_chunks add constraint invoice_operation_chunks_replacement_shape_ck CHECK (replaced_by_chunk_id IS DISTINCT FROM id AND (replaced_by_chunk_id IS NULL OR status = 'SUPERSEDED'::text) AND replacement_required = (replaced_by_chunk_id IS NOT NULL));

-- public.invoice_operation_chunks.invoice_operation_chunks_result_visible_ck
alter table public.invoice_operation_chunks add constraint invoice_operation_chunks_result_visible_ck CHECK (NOT result_visible OR selection_key IS NOT NULL);

-- public.invoice_operation_chunks.invoice_operation_chunks_running_lease_ck
alter table public.invoice_operation_chunks add constraint invoice_operation_chunks_running_lease_ck CHECK (status = 'RUNNING'::text AND lease_owner IS NOT NULL AND lease_token IS NOT NULL AND lease_expires_at_utc IS NOT NULL AND started_at_utc IS NOT NULL OR status <> 'RUNNING'::text AND lease_owner IS NULL AND lease_token IS NULL AND lease_expires_at_utc IS NULL);

-- public.invoice_operation_chunks.invoice_operation_chunks_status_ck
alter table public.invoice_operation_chunks add constraint invoice_operation_chunks_status_ck CHECK (status = ANY (ARRAY['QUEUED'::text, 'RUNNING'::text, 'WAITING'::text, 'RETRY_WAIT'::text, 'BLOCKED'::text, 'COMPLETE'::text, 'FAILED'::text, 'DEAD_LETTER'::text, 'CANCELLED'::text, 'SUPERSEDED'::text]));

-- public.invoice_operation_chunks.invoice_operation_chunks_type_ck
alter table public.invoice_operation_chunks add constraint invoice_operation_chunks_type_ck CHECK (chunk_type = ANY (ARRAY['GENERATION_GROUP'::text, 'DOCUMENT_PLAN'::text, 'DOCUMENT_INPUT'::text, 'ASSET_INSPECT'::text, 'ASSET_NORMALISE'::text, 'SOURCE_RENDER'::text, 'INVOICE_CORE_RENDER'::text, 'PDF_MERGE'::text, 'DOCUMENT_VERIFY'::text, 'ISSUE_INVOICE'::text, 'DELIVERY_PREPARE'::text, 'RECONCILE'::text]));

-- public.invoice_operation_chunks.invoice_operation_chunks_work_key_ck
alter table public.invoice_operation_chunks add constraint invoice_operation_chunks_work_key_ck CHECK (work_key ~ '^[0-9a-f]{64}$'::text);

-- public.invoice_operations.invoice_operations_counts_ck
alter table public.invoice_operations add constraint invoice_operations_counts_ck CHECK (total_units >= 0 AND completed_units >= 0 AND failed_units >= 0 AND (completed_units + failed_units) <= total_units AND chunk_count >= 0 AND control_version > 0 AND change_seq > 0 AND (total_units > 0 OR completed_units = 0 AND failed_units = 0 AND (phase = ANY (ARRAY['SUBMITTED'::text, 'PLANNING'::text, 'VALIDATING'::text]))));

-- public.invoice_operations.invoice_operations_json_ck
alter table public.invoice_operations add constraint invoice_operations_json_ck CHECK (jsonb_typeof(input_json) = 'object'::text AND jsonb_typeof(config_json) = 'object'::text AND jsonb_typeof(progress_json) = 'object'::text AND jsonb_typeof(config_json -> 'processor_policy'::text) = 'object'::text AND (config_json #>> '{processor_policy,version}'::text[]) = 'INVOICE_PROCESSOR_LIMITS_V4'::text AND (config_json #>> '{processor_policy,policy_version}'::text[]) = 'INVOICE_PROCESSOR_LIMITS_V4'::text AND jsonb_typeof(config_json #> '{processor_policy,context}'::text[]) = 'object'::text AND jsonb_typeof(config_json #> '{processor_policy,result}'::text[]) = 'object'::text AND jsonb_typeof(config_json #> '{processor_policy,asset}'::text[]) = 'object'::text AND jsonb_typeof(config_json #> '{processor_policy,merge}'::text[]) = 'object'::text AND jsonb_typeof(config_json #> '{processor_policy,attachment_index}'::text[]) = 'object'::text AND jsonb_typeof(config_json #> '{processor_policy,verify}'::text[]) = 'object'::text AND jsonb_typeof(config_json #> '{processor_policy,delivery}'::text[]) = 'object'::text AND (config_json #> '{processor_policy,asset,allowed_media_types}'::text[]) = '["application/pdf", "image/jpeg", "image/png"]'::jsonb AND (config_json #>> '{processor_policy,merge,bin_packing_version}'::text[]) = 'SEQUENTIAL_FIRST_FIT_V1'::text AND (config_json #>> '{processor_policy,asset,image_normalisation_profile}'::text[]) = 'INVOICE_IMAGE_NORMALISATION_V1'::text AND (config_json #>> '{processor_policy,asset,pdf_normalisation_profile}'::text[]) = 'INVOICE_PDF_NORMALISATION_V1'::text AND (config_json #>> '{processor_policy,verify,object_receipt_contract}'::text[]) = 'ACTUAL_BYTES_OBJECT_RECEIPT_V3'::text AND (config_json #>> '{processor_policy,verify,logical_receipt_contract}'::text[]) = 'LOGICAL_SOURCE_RECEIPT_V3'::text AND (config_json #>> '{processor_policy,verify,merge_receipt_contract}'::text[]) = 'ACTUAL_BYTES_MERGE_RECEIPT_V3'::text AND (config_json #>> '{processor_policy,verify,document_root_receipt_contract}'::text[]) = 'DOCUMENT_ROOT_RECEIPT_V3'::text AND (config_json #>> '{processor_policy,verify,ordered_input_hash_contract}'::text[]) = 'ACTUAL_ORDERED_INPUT_V1'::text AND (config_json #>> '{processor_policy,verify,receipt_hash_algorithm}'::text[]) = 'SHA256'::text AND (config_json #> '{processor_policy,delivery,allowed_policies}'::text[]) = '["ATTACH", "SPLIT", "SECURE_LINK"]'::jsonb AND ((config_json #>> '{processor_policy,verify,require_parse_success}'::text[])::boolean) AND ((config_json #>> '{processor_policy,verify,require_ordered_input_receipts}'::text[])::boolean) AND ((config_json #>> '{processor_policy,verify,require_logical_coverage_hash}'::text[])::boolean) AND ((config_json #>> '{processor_policy,verify,require_physical_receipt_hash}'::text[])::boolean) AND ((config_json #>> '{processor_policy,context,ASSET_INSPECT}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,context,ASSET_INSPECT}'::text[])::bigint) <= 16777216 AND ((config_json #>> '{processor_policy,context,ASSET_NORMALISE}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,context,ASSET_NORMALISE}'::text[])::bigint) <= 16777216 AND ((config_json #>> '{processor_policy,context,SOURCE_RENDER}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,context,SOURCE_RENDER}'::text[])::bigint) <= 16777216 AND ((config_json #>> '{processor_policy,context,INVOICE_CORE_RENDER}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,context,INVOICE_CORE_RENDER}'::text[])::bigint) <= 16777216 AND ((config_json #>> '{processor_policy,context,PDF_MERGE}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,context,PDF_MERGE}'::text[])::bigint) <= 16777216 AND ((config_json #>> '{processor_policy,context,DOCUMENT_VERIFY}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,context,DOCUMENT_VERIFY}'::text[])::bigint) <= 16777216 AND ((config_json #>> '{processor_policy,result,ASSET_INSPECT}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,result,ASSET_INSPECT}'::text[])::bigint) <= 16777216 AND ((config_json #>> '{processor_policy,result,ASSET_NORMALISE}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,result,ASSET_NORMALISE}'::text[])::bigint) <= 16777216 AND ((config_json #>> '{processor_policy,result,SOURCE_RENDER}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,result,SOURCE_RENDER}'::text[])::bigint) <= 16777216 AND ((config_json #>> '{processor_policy,result,INVOICE_CORE_RENDER}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,result,INVOICE_CORE_RENDER}'::text[])::bigint) <= 16777216 AND ((config_json #>> '{processor_policy,result,PDF_MERGE}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,result,PDF_MERGE}'::text[])::bigint) <= 16777216 AND ((config_json #>> '{processor_policy,result,DOCUMENT_VERIFY}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,result,DOCUMENT_VERIFY}'::text[])::bigint) <= 16777216 AND ((config_json #>> '{processor_policy,asset,max_pixels}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,asset,max_pixels}'::text[])::bigint) <= 1000000000 AND ((config_json #>> '{processor_policy,asset,max_decoded_bytes}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,asset,max_decoded_bytes}'::text[])::bigint) <= '4294967296'::bigint AND ((config_json #>> '{processor_policy,asset,max_source_bytes}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,asset,max_source_bytes}'::text[])::bigint) <= '2147483648'::bigint AND ((config_json #>> '{processor_policy,asset,max_pdf_part_pages}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,asset,max_pdf_part_pages}'::text[])::bigint) <= 10000 AND ((config_json #>> '{processor_policy,asset,max_pdf_part_bytes}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,asset,max_pdf_part_bytes}'::text[])::bigint) <= 1073741824 AND ((config_json #>> '{processor_policy,asset,max_part_input_bytes}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,asset,max_part_input_bytes}'::text[])::bigint) <= '2147483648'::bigint AND ((config_json #>> '{processor_policy,asset,max_part_estimated_decoded_bytes}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,asset,max_part_estimated_decoded_bytes}'::text[])::bigint) <= '4294967296'::bigint AND ((config_json #>> '{processor_policy,merge,max_inputs}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,merge,max_inputs}'::text[])::bigint) <= 1000 AND ((config_json #>> '{processor_policy,merge,max_pages}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,merge,max_pages}'::text[])::bigint) <= 10000 AND ((config_json #>> '{processor_policy,merge,max_input_bytes}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,merge,max_input_bytes}'::text[])::bigint) <= '2147483648'::bigint AND ((config_json #>> '{processor_policy,merge,max_estimated_decoded_bytes}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,merge,max_estimated_decoded_bytes}'::text[])::bigint) <= '8589934592'::bigint AND ((config_json #>> '{processor_policy,merge,max_levels}'::text[])::integer) >= 1 AND ((config_json #>> '{processor_policy,merge,max_levels}'::text[])::integer) <= 32 AND ((config_json #>> '{processor_policy,attachment_index,max_render_passes}'::text[])::integer) >= 1 AND ((config_json #>> '{processor_policy,attachment_index,max_render_passes}'::text[])::integer) <= 3 AND ((config_json #>> '{processor_policy,verify,max_receipts}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,verify,max_receipts}'::text[])::bigint) <= 10000 AND ((config_json #>> '{processor_policy,delivery,max_attachments_per_message}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,delivery,max_attachments_per_message}'::text[])::bigint) <= 1000 AND ((config_json #>> '{processor_policy,delivery,max_cumulative_attachment_bytes}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,delivery,max_cumulative_attachment_bytes}'::text[])::bigint) <= 1073741824 AND ((config_json #>> '{processor_policy,delivery,max_individual_attachment_bytes}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,delivery,max_individual_attachment_bytes}'::text[])::bigint) <= 1073741824 AND ((config_json #>> '{processor_policy,delivery,secure_link_threshold_bytes}'::text[])::bigint) >= 1 AND ((config_json #>> '{processor_policy,delivery,secure_link_threshold_bytes}'::text[])::bigint) <= '2147483648'::bigint AND (result_json IS NULL OR jsonb_typeof(result_json) = 'object'::text) AND (error_json IS NULL OR jsonb_typeof(error_json) = 'object'::text AND (NOT error_json ? 'history'::text OR jsonb_typeof(error_json -> 'history'::text) = 'array'::text AND jsonb_array_length(error_json -> 'history'::text) <= 8)));

-- public.invoice_operations.invoice_operations_manifest_generation_ck
alter table public.invoice_operations add constraint invoice_operations_manifest_generation_ck CHECK (manifest_generation >= 0);

-- public.invoice_operations.invoice_operations_manifest_release_ck
alter table public.invoice_operations add constraint invoice_operations_manifest_release_ck CHECK (NOT release_complete OR manifest_committed);

-- public.invoice_operations.invoice_operations_priority_ck
alter table public.invoice_operations add constraint invoice_operations_priority_ck CHECK (priority >= 0 AND priority <= 2000);

-- public.invoice_operations.invoice_operations_result_page_revision_ck
alter table public.invoice_operations add constraint invoice_operations_result_page_revision_ck CHECK (result_page_revision >= 0);

-- public.invoice_operations.invoice_operations_status_ck
alter table public.invoice_operations add constraint invoice_operations_status_ck CHECK (status = ANY (ARRAY['QUEUED'::text, 'RUNNING'::text, 'WAITING'::text, 'RETRY_WAIT'::text, 'BLOCKED'::text, 'COMPLETE'::text, 'FAILED'::text, 'DEAD_LETTER'::text, 'CANCELLED'::text, 'SUPERSEDED'::text]));

-- public.invoice_operations.invoice_operations_type_ck
alter table public.invoice_operations add constraint invoice_operations_type_ck CHECK (operation_type = ANY (ARRAY['GENERATE_INVOICES'::text, 'BUILD_DOCUMENT'::text, 'PREPARE_ASSET'::text, 'ISSUE_INVOICES'::text, 'DELIVER_INVOICES'::text, 'RECONCILE_INVOICE_WORK'::text, 'OPERATION_CONTROL_REQUEST'::text]));

-- public.invoices.chk_invoices_issued_have_timestamp
alter table public.invoices add constraint chk_invoices_issued_have_timestamp CHECK (status <> 'ISSUED'::invoice_status_enum OR issued_at_utc IS NOT NULL) NOT VALID;

-- public.invoices.invoices_document_state_ck
alter table public.invoices add constraint invoices_document_state_ck CHECK (document_state = ANY (ARRAY['NOT_REQUESTED'::text, 'QUEUED'::text, 'STALE'::text, 'PREPARING'::text, 'READY'::text, 'FAILED'::text]));

-- public.invoices.invoices_issue_state_ck
alter table public.invoices add constraint invoices_issue_state_ck CHECK (issue_state = ANY (ARRAY['NOT_STARTED'::text, 'VALIDATING'::text, 'PREPARING_DOCUMENT'::text, 'READY_TO_FINALISE'::text, 'ISSUED'::text, 'BLOCKED'::text, 'FAILED'::text, 'CANCELLED'::text, 'SUPERSEDED'::text]));

-- public.legacy_contract_rate_lines.legacy_contract_rate_lines_any_value_chk
alter table public.legacy_contract_rate_lines add constraint legacy_contract_rate_lines_any_value_chk CHECK (pay_rate IS NOT NULL OR margin IS NOT NULL OR charge_rate IS NOT NULL);

-- public.legacy_contract_rate_lines.legacy_contract_rate_lines_line_no_chk
alter table public.legacy_contract_rate_lines add constraint legacy_contract_rate_lines_line_no_chk CHECK (line_no >= 1 AND line_no <= 4);

-- public.legacy_contracts.legacy_contracts_pay_method_chk
alter table public.legacy_contracts add constraint legacy_contracts_pay_method_chk CHECK (pay_method = ANY (ARRAY['PAYE'::text, 'UMBRELLA'::text]));

-- public.mail_outbox.mail_outbox_attachment_bytes_ck
alter table public.mail_outbox add constraint mail_outbox_attachment_bytes_ck CHECK (attachment_total_bytes IS NULL OR attachment_total_bytes >= 0);

-- public.mail_outbox.mail_outbox_attachment_delivery_policy_ck
alter table public.mail_outbox add constraint mail_outbox_attachment_delivery_policy_ck CHECK (attachment_delivery_policy IS NULL OR (attachment_delivery_policy = ANY (ARRAY['ATTACH'::text, 'SPLIT'::text, 'SECURE_LINK'::text])));

-- public.mail_outbox.mail_outbox_payment_scope_json_object_chk
alter table public.mail_outbox add constraint mail_outbox_payment_scope_json_object_chk CHECK (jsonb_typeof(payment_scope_json) = 'object'::text);

-- public.mail_outbox.mail_outbox_type_check
alter table public.mail_outbox add constraint mail_outbox_type_check CHECK ((type = ANY (ARRAY['INVOICE'::text, 'REMITTANCE'::text, 'TSO_FAILURE'::text, 'BROADCAST'::text, 'TIMESHEET_QR'::text, 'TIMESHEET_REFUSAL'::text, 'TIMESHEET_GENERAL'::text, 'TIMESHEET_QUERY'::text])) OR type = 'MAILSHOT_EMAIL'::text);

-- public.manual_timesheet_queue.chk_manual_timesheet_queue_active_staged_timesheet_storage_key
alter table public.manual_timesheet_queue add constraint chk_manual_timesheet_queue_active_staged_timesheet_storage_key CHECK (NOT (status = 'STAGED'::text AND NULLIF(btrim(meta_json ->> 'contract_week_id'::text), ''::text) IS NOT NULL AND upper(COALESCE(NULLIF(btrim(meta_json ->> 'staged_kind'::text), ''::text), NULLIF(btrim(meta_json ->> 'kind'::text), ''::text), NULLIF(btrim(meta_json ->> 'attached_kind'::text), ''::text), 'TIMESHEET'::text)) = 'TIMESHEET'::text AND NULLIF(regexp_replace(COALESCE(NULLIF(btrim(COALESCE(r2_key, ''::text)), ''::text), NULLIF(btrim(COALESCE(meta_json ->> 'r2_key'::text, ''::text)), ''::text), NULLIF(btrim(COALESCE(meta_json ->> 'storage_key'::text, ''::text)), ''::text), NULLIF(btrim(COALESCE(meta_json ->> 'file_key'::text, ''::text)), ''::text), NULLIF(btrim(COALESCE(meta_json ->> 'canonical_key'::text, ''::text)), ''::text), ''::text), '^/+'::text, ''::text), ''::text) IS NULL));

-- public.manual_timesheet_queue.manual_timesheet_queue_rotation_chk
alter table public.manual_timesheet_queue add constraint manual_timesheet_queue_rotation_chk CHECK (last_rotation_deg = ANY (ARRAY[0, 90, 180, 270]));

-- public.manual_timesheet_queue.manual_timesheet_queue_status_chk
alter table public.manual_timesheet_queue add constraint manual_timesheet_queue_status_chk CHECK (status = ANY (ARRAY['QUEUED'::text, 'ATTACHED'::text, 'DISCARDED'::text, 'STAGED'::text]));

-- public.pay_advance_reservations.pay_advance_reservations_status_chk
alter table public.pay_advance_reservations add constraint pay_advance_reservations_status_chk CHECK (status = ANY (ARRAY['RESERVED'::text, 'COMMITTED'::text, 'SETTLED'::text, 'RELEASED'::text]));

-- public.pay_bank_transfer_events.pay_bank_transfer_events_correction_disposition_chk
alter table public.pay_bank_transfer_events add constraint pay_bank_transfer_events_correction_disposition_chk CHECK (correction_disposition IS NULL OR (correction_disposition = ANY (ARRAY['NO_CORRECTION_REQUIRED'::text, 'ACTION_REQUIRED'::text, 'AMBIGUOUS'::text, 'BLOCKED'::text, 'FAILED'::text, 'AUTO_PROCESSING'::text, 'AUTO_APPLIED'::text])));

-- public.pay_bank_transfer_events.pay_bank_transfer_events_event_source_chk
alter table public.pay_bank_transfer_events add constraint pay_bank_transfer_events_event_source_chk CHECK (event_source = ANY (ARRAY['PROVIDER_WEBHOOK'::text, 'PROVIDER_POLL'::text, 'PROVIDER_RESPONSE'::text, 'LOCAL_STATE'::text, 'MANUAL_CONFIRM'::text, 'MANUAL_EVIDENCE'::text, 'SYSTEM'::text]));

-- public.pay_bank_transfer_events.pay_bank_transfer_events_failure_reason_group_chk
alter table public.pay_bank_transfer_events add constraint pay_bank_transfer_events_failure_reason_group_chk CHECK (provider_failure_reason_group IS NULL OR (provider_failure_reason_group = ANY (ARRAY['INSUFFICIENT_FUNDS'::text, 'UNKNOWN_RECIPIENT'::text, 'INVALID_ACCOUNT'::text, 'ACCOUNT_CLOSED'::text, 'BANK_REJECTED'::text, 'PROVIDER_OUTAGE'::text, 'PROVIDER_UNKNOWN'::text, 'COMPLIANCE_REVIEW'::text, 'DUPLICATE_RISK'::text, 'PAID_RECOVERY_REQUIRED'::text, 'MANUAL_ADJUSTMENT_BLOCKER'::text, 'WEBHOOK_UNMATCHED'::text, 'PROVIDER_FAILED_UNSPECIFIED'::text])));

-- public.pay_bank_transfer_events.pay_bank_transfer_events_mapping_hints_object_chk
alter table public.pay_bank_transfer_events add constraint pay_bank_transfer_events_mapping_hints_object_chk CHECK (jsonb_typeof(mapping_hints_json) = 'object'::text);

-- public.pay_bank_transfer_events.pay_bank_transfer_events_mapping_method_chk
alter table public.pay_bank_transfer_events add constraint pay_bank_transfer_events_mapping_method_chk CHECK (mapping_method IS NULL OR (mapping_method = ANY (ARRAY['TRANSFER_ID'::text, 'PROVIDER_EVENT_ID'::text, 'PROVIDER_REFERENCE'::text, 'PROVIDER_TRANSACTION_ID'::text, 'REQUEST_ID'::text, 'RAIL_TX_ID'::text, 'PAYMENT_REFERENCE'::text, 'OPERATION_TRANSFER_SCOPE'::text, 'MANUAL_TRANSFER_SELECTION'::text, 'AMOUNT_ONLY_UNIQUE'::text, 'UNMATCHED'::text, 'AMBIGUOUS'::text, 'LEGACY_NO_ARTIFACT'::text])));

-- public.pay_bank_transfer_events.pay_bank_transfer_events_mapping_status_chk
alter table public.pay_bank_transfer_events add constraint pay_bank_transfer_events_mapping_status_chk CHECK (mapping_status = ANY (ARRAY['MATCHED'::text, 'AMBIGUOUS'::text, 'UNMATCHED'::text, 'LEGACY_NO_ARTIFACT'::text]));

-- public.pay_bank_transfer_events.pay_bank_transfer_events_movement_classification_chk
alter table public.pay_bank_transfer_events add constraint pay_bank_transfer_events_movement_classification_chk CHECK (movement_classification IS NULL OR (movement_classification = ANY (ARRAY['PRE_BANK_CANCEL'::text, 'NO_MONEY_UNWIND'::text, 'TRUE_SETTLED_REVERSAL_REQUIRED'::text, 'AMBIGUOUS_REVIEW_REQUIRED'::text])));

-- public.pay_bank_transfer_events.pay_bank_transfer_events_normalised_state_chk
alter table public.pay_bank_transfer_events add constraint pay_bank_transfer_events_normalised_state_chk CHECK (normalised_state = ANY (ARRAY['SUBMITTED'::text, 'PENDING'::text, 'PROCESSING'::text, 'COMPLETED'::text, 'FAILED'::text, 'DECLINED'::text, 'REJECTED'::text, 'CANCELLED'::text, 'RETURNED'::text, 'REVERTED'::text, 'UNKNOWN'::text]));

-- public.pay_bank_transfer_events.pay_bank_transfer_events_provider_transport_chk
alter table public.pay_bank_transfer_events add constraint pay_bank_transfer_events_provider_transport_chk CHECK (provider_event_transport IS NULL OR (provider_event_transport = ANY (ARRAY['PROVIDER_RESPONSE'::text, 'PROVIDER_POLL'::text, 'PROVIDER_WEBHOOK'::text, 'FAILED_WEBHOOK_REPLAY'::text, 'MANUAL_CONFIRM'::text, 'LOCAL_STATE'::text])));

-- public.pay_bank_transfers.pay_bank_transfers_account_type_chk
alter table public.pay_bank_transfers add constraint pay_bank_transfers_account_type_chk CHECK (account_type IS NULL OR (account_type = ANY (ARRAY['Business'::text, 'Personal'::text])));

-- public.pay_bank_transfers.pay_bank_transfers_amount_check
alter table public.pay_bank_transfers add constraint pay_bank_transfers_amount_check CHECK (amount >= 0::numeric);

-- public.pay_bank_transfers.pay_bank_transfers_pay_channel_check
alter table public.pay_bank_transfers add constraint pay_bank_transfers_pay_channel_check CHECK (pay_channel = ANY (ARRAY['PAYE'::text, 'UMBRELLA'::text]));

-- public.pay_bank_transfers.pay_bank_transfers_payee_entity_kind_chk
alter table public.pay_bank_transfers add constraint pay_bank_transfers_payee_entity_kind_chk CHECK (payee_entity_kind = ANY (ARRAY['CANDIDATE'::text, 'UMBRELLA'::text]));

-- public.pay_bank_transfers.pay_bank_transfers_payee_present_chk
alter table public.pay_bank_transfers add constraint pay_bank_transfers_payee_present_chk CHECK (candidate_id IS NOT NULL OR umbrella_id IS NOT NULL);

-- public.pay_bank_transfers.pay_bank_transfers_rail_env_chk
alter table public.pay_bank_transfers add constraint pay_bank_transfers_rail_env_chk CHECK (rail_env = ANY (ARRAY['PROD'::text, 'SANDBOX'::text]));

-- public.pay_bank_transfers.pay_bank_transfers_rail_provider_chk
alter table public.pay_bank_transfers add constraint pay_bank_transfers_rail_provider_chk CHECK (rail_provider = ANY (ARRAY['REVOLUT'::text, 'CSV'::text]));

-- public.pay_bank_transfers.pay_bank_transfers_sort_code_fmt_chk
alter table public.pay_bank_transfers add constraint pay_bank_transfers_sort_code_fmt_chk CHECK (sort_code IS NULL OR sort_code ~ '^[0-9]{2}-[0-9]{2}-[0-9]{2}$'::text);

-- public.pay_bank_transfers.pay_bank_transfers_status_chk_v3
alter table public.pay_bank_transfers add constraint pay_bank_transfers_status_chk_v3 CHECK (status = ANY (ARRAY['PENDING'::text, 'PROCESSING'::text, 'UNKNOWN'::text, 'COMPLETED'::text, 'FAILED'::text, 'DECLINED'::text, 'REJECTED'::text, 'CANCELLED'::text, 'VOIDED'::text, 'RETURNED'::text, 'REVERTED'::text, 'BLOCKED'::text, 'SUBMISSION_FAILED'::text, 'FAILED_BEFORE_COMMIT'::text]));

-- public.pay_batch_auth_actions.pay_batch_auth_actions_action_chk
alter table public.pay_batch_auth_actions add constraint pay_batch_auth_actions_action_chk CHECK (action = ANY (ARRAY['AUTHORISE'::text, 'USE_GOLDEN_KEY'::text, 'REJECT'::text]));

-- public.pay_batch_auth_requests.pay_batch_auth_requests_execution_intent_json_shape_chk
alter table public.pay_batch_auth_requests add constraint pay_batch_auth_requests_execution_intent_json_shape_chk CHECK (execution_intent_json IS NULL OR jsonb_typeof(execution_intent_json) = 'object'::text AND (NOT execution_intent_json ? 'execution_mode'::text OR ((execution_intent_json ->> 'execution_mode'::text) = ANY (ARRAY['STANDARD_BANK'::text, 'CSV_SETTLEMENT'::text, 'EXTERNAL_SETTLEMENT'::text]))) AND (NOT execution_intent_json ? 'suppress_remittances'::text OR jsonb_typeof(execution_intent_json -> 'suppress_remittances'::text) = 'boolean'::text) AND (NOT execution_intent_json ? 'csv_uploaded_confirmed'::text OR jsonb_typeof(execution_intent_json -> 'csv_uploaded_confirmed'::text) = 'boolean'::text));

-- public.pay_batch_auth_requests.pay_batch_auth_requests_state_chk
alter table public.pay_batch_auth_requests add constraint pay_batch_auth_requests_state_chk CHECK (state = ANY (ARRAY['AWAITING'::text, 'AUTHORISED'::text, 'REJECTED'::text, 'CANCELLED'::text]));

-- public.pay_batch_candidates.pay_batch_candidates_mismatch_choice_check
alter table public.pay_batch_candidates add constraint pay_batch_candidates_mismatch_choice_check CHECK (mismatch_settlement_choice IS NULL OR (mismatch_settlement_choice = ANY (ARRAY['SETTLE_VIA_PAYE'::text, 'SETTLE_VIA_UMBRELLA'::text])));

-- public.pay_batch_candidates.pay_batch_candidates_net_bank_nonneg_chk
alter table public.pay_batch_candidates add constraint pay_batch_candidates_net_bank_nonneg_chk CHECK (net_bank_amount IS NULL OR net_bank_amount >= 0::numeric);

-- public.pay_batch_candidates.pay_batch_candidates_paye_state_check
alter table public.pay_batch_candidates add constraint pay_batch_candidates_paye_state_check CHECK (paye_state IS NULL OR (paye_state = ANY (ARRAY['PENDING_NET'::text, 'READY'::text, 'SETTLED'::text])));

-- public.pay_batch_candidates.pay_batch_candidates_settlement_status_check
alter table public.pay_batch_candidates add constraint pay_batch_candidates_settlement_status_check CHECK (settlement_status IS NULL OR (settlement_status = ANY (ARRAY['PENDING'::text, 'SETTLED'::text, 'PARTIAL'::text, 'FAILED'::text, 'UNPAID'::text])));

-- public.pay_batch_items.pay_batch_items_pay_channel_check
alter table public.pay_batch_items add constraint pay_batch_items_pay_channel_check CHECK (pay_channel = ANY (ARRAY['PAYE'::text, 'UMBRELLA'::text]));

-- public.pay_batch_items.pay_batch_items_paye_treatment_chk
alter table public.pay_batch_items add constraint pay_batch_items_paye_treatment_chk CHECK (paye_treatment IS NULL OR (paye_treatment = ANY (ARRAY['GROSS_ADD'::text, 'GROSS_DEDUCT'::text, 'NET_ADD'::text, 'NET_DEDUCT'::text, 'NONE'::text])));

-- public.pay_batch_paye_net_inputs.pay_batch_paye_net_inputs_net_amount_check
alter table public.pay_batch_paye_net_inputs add constraint pay_batch_paye_net_inputs_net_amount_check CHECK (net_amount >= 0::numeric);

-- public.pay_batch_paye_net_inputs.pay_batch_paye_net_inputs_source_check
alter table public.pay_batch_paye_net_inputs add constraint pay_batch_paye_net_inputs_source_check CHECK (source = ANY (ARRAY['SAGE_IMPORT'::text, 'MANUAL_ENTRY'::text, 'CSV_IMPORT'::text]));

-- public.pay_batch_timesheet_snapshots.pay_batch_timesheet_snapshots_pay_channel_chk
alter table public.pay_batch_timesheet_snapshots add constraint pay_batch_timesheet_snapshots_pay_channel_chk CHECK (pay_channel = ANY (ARRAY['PAYE'::text, 'UMBRELLA'::text]));

-- public.pay_batches.pay_batches_bank_csv_export_json_object_chk
alter table public.pay_batches add constraint pay_batches_bank_csv_export_json_object_chk CHECK (bank_csv_export_json IS NULL OR jsonb_typeof(bank_csv_export_json) = 'object'::text);

-- public.pay_batches.pay_batches_banking_system_snapshot_check
alter table public.pay_batches add constraint pay_batches_banking_system_snapshot_check CHECK (banking_system_snapshot = ANY (ARRAY['REVOLUT_API'::text, 'MONZO_CSV'::text, 'REVOLUT_CSV'::text]));

-- public.pay_batches.pay_batches_batch_kind_fixed_chk
alter table public.pay_batches add constraint pay_batches_batch_kind_fixed_chk CHECK (batch_kind_fixed IS NULL OR (upper(batch_kind_fixed) = ANY (ARRAY['PAYE'::text, 'UMBRELLA'::text, 'MIXED'::text, 'LOANS'::text])));

-- public.pay_batches.pay_batches_bulk_ref_num_range_chk
alter table public.pay_batches add constraint pay_batches_bulk_ref_num_range_chk CHECK (bulk_ref_num IS NULL OR bulk_ref_num >= 1000000 AND bulk_ref_num <= 9999999);

-- public.pay_batches.pay_batches_execution_commit_state_chk
alter table public.pay_batches add constraint pay_batches_execution_commit_state_chk CHECK (execution_commit_state = ANY (ARRAY['NOT_SUBMITTED'::text, 'SUBMITTED_NOT_COMMITTED'::text, 'COMMITTED'::text]));

-- public.pay_batches.pay_batches_execution_intent_json_shape_chk
alter table public.pay_batches add constraint pay_batches_execution_intent_json_shape_chk CHECK (execution_intent_json IS NULL OR jsonb_typeof(execution_intent_json) = 'object'::text AND (NOT execution_intent_json ? 'execution_mode'::text OR ((execution_intent_json ->> 'execution_mode'::text) = ANY (ARRAY['STANDARD_BANK'::text, 'CSV_SETTLEMENT'::text, 'EXTERNAL_SETTLEMENT'::text]))) AND (NOT execution_intent_json ? 'suppress_remittances'::text OR jsonb_typeof(execution_intent_json -> 'suppress_remittances'::text) = 'boolean'::text) AND (NOT execution_intent_json ? 'suppress_remittances_confirmed'::text OR jsonb_typeof(execution_intent_json -> 'suppress_remittances_confirmed'::text) = 'boolean'::text) AND (NOT execution_intent_json ? 'csv_uploaded_confirmed'::text OR jsonb_typeof(execution_intent_json -> 'csv_uploaded_confirmed'::text) = 'boolean'::text));

-- public.pay_batches.pay_batches_external_paye_system_snapshot_check
alter table public.pay_batches add constraint pay_batches_external_paye_system_snapshot_check CHECK (external_paye_system_snapshot = ANY (ARRAY['SAGE'::text, 'CSV'::text]));

-- public.pay_batches.pay_batches_freshness_validation_status_check
alter table public.pay_batches add constraint pay_batches_freshness_validation_status_check CHECK (freshness_validation_status IS NULL OR (freshness_validation_status = ANY (ARRAY['PENDING'::text, 'PASSED'::text, 'STALE'::text, 'FAILED'::text, 'NOT_REQUIRED'::text, 'VALID_AT_SCOPE_FREEZE'::text, 'STALE_POST_SCOPE_FREEZE'::text, 'PENDING_SCOPE_CHANGE_RELEVANCE'::text])));

-- public.pay_batches.pay_batches_funds_warning_hours_is_array_chk
alter table public.pay_batches add constraint pay_batches_funds_warning_hours_is_array_chk CHECK (funds_warning_hours_json IS NULL OR jsonb_typeof(funds_warning_hours_json) = 'array'::text);

-- public.pay_batches.pay_batches_rail_env_chk
alter table public.pay_batches add constraint pay_batches_rail_env_chk CHECK (rail_env_snapshot = ANY (ARRAY['PROD'::text, 'SANDBOX'::text]));

-- public.pay_batches.pay_batches_rail_provider_chk
alter table public.pay_batches add constraint pay_batches_rail_provider_chk CHECK (rail_provider_snapshot = ANY (ARRAY['REVOLUT'::text, 'CSV'::text]));

-- public.pay_batches.pay_batches_schedule_kind_chk
alter table public.pay_batches add constraint pay_batches_schedule_kind_chk CHECK (schedule_kind IS NULL OR (schedule_kind = ANY (ARRAY['IMMEDIATE'::text, 'SCHEDULED'::text])));

-- public.pay_batches.pay_batches_settlement_confirmation_json_shape_chk
alter table public.pay_batches add constraint pay_batches_settlement_confirmation_json_shape_chk CHECK (settlement_confirmation_json IS NULL OR jsonb_typeof(settlement_confirmation_json) = 'object'::text AND (NOT settlement_confirmation_json ? 'settlement_mode'::text OR ((settlement_confirmation_json ->> 'settlement_mode'::text) = ANY (ARRAY['STANDARD_BANK'::text, 'CSV_SETTLEMENT'::text, 'EXTERNAL_SETTLEMENT'::text]))) AND (NOT settlement_confirmation_json ? 'suppress_remittances'::text OR jsonb_typeof(settlement_confirmation_json -> 'suppress_remittances'::text) = 'boolean'::text));

-- public.pay_batches.pay_batches_status_chk_v2
alter table public.pay_batches add constraint pay_batches_status_chk_v2 CHECK ((status = ANY (ARRAY['DRAFT'::text, 'WAITING_BANK_CONFIRM'::text, 'DRAFT_CREATED'::text, 'PARTIAL'::text, 'FAILED'::text, 'SETTLED'::text, 'READY'::text, 'SCHEDULED'::text, 'EXECUTING'::text, 'CANCELLED'::text, 'BLOCKED_FUNDS'::text])) OR (status = ANY (ARRAY['AWAITING_AUTHORISATION'::text, 'AUTHORISED_FOR_PAYMENT'::text])));

-- public.pay_finance_case_components.pay_finance_case_components_remaining_source_amount_nonneg
alter table public.pay_finance_case_components add constraint pay_finance_case_components_remaining_source_amount_nonneg CHECK (remaining_source_amount >= 0::numeric);

-- public.pay_finance_case_components.pay_finance_case_components_source_amount_nonneg
alter table public.pay_finance_case_components add constraint pay_finance_case_components_source_amount_nonneg CHECK (source_amount >= 0::numeric);

-- public.pay_finance_case_oneoff_payout_bank_details.pay_finance_case_oneoff_payout_bank_details_account_number_chk
alter table public.pay_finance_case_oneoff_payout_bank_details add constraint pay_finance_case_oneoff_payout_bank_details_account_number_chk CHECK (NULLIF(btrim(account_number), ''::text) IS NOT NULL);

-- public.pay_finance_case_oneoff_payout_bank_details.pay_finance_case_oneoff_payout_bank_details_bank_details_hash_c
alter table public.pay_finance_case_oneoff_payout_bank_details add constraint pay_finance_case_oneoff_payout_bank_details_bank_details_hash_c CHECK (NULLIF(btrim(bank_details_hash), ''::text) IS NOT NULL);

-- public.pay_finance_case_oneoff_payout_bank_details.pay_finance_case_oneoff_payout_bank_details_beneficiary_name_ch
alter table public.pay_finance_case_oneoff_payout_bank_details add constraint pay_finance_case_oneoff_payout_bank_details_beneficiary_name_ch CHECK (NULLIF(btrim(beneficiary_name), ''::text) IS NOT NULL);

-- public.pay_finance_case_oneoff_payout_bank_details.pay_finance_case_oneoff_payout_bank_details_sort_code_chk
alter table public.pay_finance_case_oneoff_payout_bank_details add constraint pay_finance_case_oneoff_payout_bank_details_sort_code_chk CHECK (NULLIF(btrim(sort_code), ''::text) IS NOT NULL);

-- public.pay_item_snoozes.pay_item_snoozes_identity_check
alter table public.pay_item_snoozes add constraint pay_item_snoozes_identity_check CHECK (source_ref IS NOT NULL OR booking_id IS NOT NULL OR timesheet_id IS NOT NULL);

-- public.pay_item_snoozes.pay_item_snoozes_kind_check
alter table public.pay_item_snoozes add constraint pay_item_snoozes_kind_check CHECK (snooze_kind = ANY (ARRAY['DO_NOT_PAY'::text, 'BLOCKED_TIMESHEET'::text, 'TIMESHEET_PAYMENT'::text, 'OVERPAYMENT_RECOVERY'::text, 'PAYMENT_ADVANCE_REPAYMENT'::text, 'MANUAL_DEBT_RECOVERY'::text]));

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_forwards_amount_nonzero_chk
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_forwards_amount_nonzero_chk CHECK (amount_inc_vat <> 0::numeric);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_forwards_direction_chk
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_forwards_direction_chk CHECK (adjustment_direction = ANY (ARRAY['CREDIT'::text, 'DEBIT'::text]));

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_forwards_signed_direction_chk
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_forwards_signed_direction_chk CHECK (amount_inc_vat > 0::numeric AND adjustment_direction = 'CREDIT'::text OR amount_inc_vat < 0::numeric AND adjustment_direction = 'DEBIT'::text);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_forwards_status_chk
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_forwards_status_chk CHECK (status = ANY (ARRAY['PENDING_CARRY_FORWARD'::text, 'RESERVED_IN_DRAFT'::text, 'CONSUMED_IN_BATCH'::text, 'CANCELLED'::text, 'SUPERSEDED'::text, 'NEEDS_REVIEW'::text]));

-- public.pay_payment_correction_actions.pay_payment_correction_actions_action_chk
alter table public.pay_payment_correction_actions add constraint pay_payment_correction_actions_action_chk CHECK (action = ANY (ARRAY['REQUEST'::text, 'AUTHORISE'::text, 'USE_GOLDEN_KEY'::text, 'REJECT'::text, 'CANCEL'::text, 'EXPAND_WORK'::text, 'APPLY'::text, 'APPLY_FAILED'::text, 'BLOCK'::text, 'RETRY'::text]));

-- public.pay_payment_correction_actions.pay_payment_correction_actions_actor_kind_chk
alter table public.pay_payment_correction_actions add constraint pay_payment_correction_actions_actor_kind_chk CHECK (actor_kind = ANY (ARRAY['USER'::text, 'SYSTEM'::text]));

-- public.pay_payment_correction_actions.pay_payment_correction_actions_metadata_obj_chk
alter table public.pay_payment_correction_actions add constraint pay_payment_correction_actions_metadata_obj_chk CHECK (jsonb_typeof(metadata_json) = 'object'::text);

-- public.pay_payment_correction_items.pay_payment_correction_items_kind_chk
alter table public.pay_payment_correction_items add constraint pay_payment_correction_items_kind_chk CHECK (correction_item_kind = ANY (ARRAY['PRE_BANK_CANCEL'::text, 'NO_MONEY_UNWIND'::text, 'SETTLED_REVERSAL'::text]));

-- public.pay_payment_correction_items.pay_payment_correction_items_status_chk
alter table public.pay_payment_correction_items add constraint pay_payment_correction_items_status_chk CHECK (status = ANY (ARRAY['PLANNED'::text, 'APPLIED'::text, 'SKIPPED'::text, 'BLOCKED'::text, 'FAILED'::text]));

-- public.pay_payment_correction_request_candidates.pay_payment_correction_request_candidates_amount_chk
alter table public.pay_payment_correction_request_candidates add constraint pay_payment_correction_request_candidates_amount_chk CHECK (active_amount >= 0::numeric);

-- public.pay_payment_correction_request_candidates.pay_payment_correction_request_candidates_candidate_hash_chk
alter table public.pay_payment_correction_request_candidates add constraint pay_payment_correction_request_candidates_candidate_hash_chk CHECK (candidate_scope_hash ~ '^[0-9a-f]{64}$'::text);

-- public.pay_payment_correction_request_candidates.pay_payment_correction_request_candidates_instruction_hash_chk
alter table public.pay_payment_correction_request_candidates add constraint pay_payment_correction_request_candidates_instruction_hash_chk CHECK (shared_instruction_scope_hash IS NULL OR shared_instruction_scope_hash ~ '^[0-9a-f]{64}$'::text);

-- public.pay_payment_correction_request_candidates.pay_payment_correction_request_candidates_item_count_chk
alter table public.pay_payment_correction_request_candidates add constraint pay_payment_correction_request_candidates_item_count_chk CHECK (active_item_count >= 1 AND active_item_count <= 128 AND active_item_count = cardinality(pay_batch_item_ids));

-- public.pay_payment_correction_request_candidates.pay_payment_correction_request_candidates_ordinal_chk
alter table public.pay_payment_correction_request_candidates add constraint pay_payment_correction_request_candidates_ordinal_chk CHECK (selection_ordinal > 0);

-- public.pay_payment_correction_request_candidates.pay_payment_correction_request_candidates_source_count_chk
alter table public.pay_payment_correction_request_candidates add constraint pay_payment_correction_request_candidates_source_count_chk CHECK (source_row_count >= 1 AND source_row_count <= 512);

-- public.pay_payment_correction_requests.pay_payment_correction_requests_approved_count_chk
alter table public.pay_payment_correction_requests add constraint pay_payment_correction_requests_approved_count_chk CHECK (approved_count >= 0);

-- public.pay_payment_correction_requests.pay_payment_correction_requests_kind_chk
alter table public.pay_payment_correction_requests add constraint pay_payment_correction_requests_kind_chk CHECK (correction_kind = ANY (ARRAY['PRE_BANK_CANCEL'::text, 'NO_MONEY_UNWIND'::text, 'SETTLED_REVERSAL'::text, 'MANUAL_EVIDENCE_NO_MONEY'::text, 'MANUAL_EVIDENCE_SETTLED_RETURN'::text]));

-- public.pay_payment_correction_requests.pay_payment_correction_requests_plan_obj_chk
alter table public.pay_payment_correction_requests add constraint pay_payment_correction_requests_plan_obj_chk CHECK (jsonb_typeof(plan_json) = 'object'::text);

-- public.pay_payment_correction_requests.pay_payment_correction_requests_reauth_consumed_chk
alter table public.pay_payment_correction_requests add constraint pay_payment_correction_requests_reauth_consumed_chk CHECK (reauth_consumed_at_utc IS NULL OR reauth_proof_hash IS NOT NULL);

-- public.pay_payment_correction_requests.pay_payment_correction_requests_reauth_hash_chk
alter table public.pay_payment_correction_requests add constraint pay_payment_correction_requests_reauth_hash_chk CHECK (reauth_proof_hash IS NULL OR reauth_proof_hash ~ '^[0-9a-f]{64}$'::text);

-- public.pay_payment_correction_requests.pay_payment_correction_requests_required_qty_chk
alter table public.pay_payment_correction_requests add constraint pay_payment_correction_requests_required_qty_chk CHECK (required_quantity >= 1);

-- public.pay_payment_correction_requests.pay_payment_correction_requests_selection_obj_chk
alter table public.pay_payment_correction_requests add constraint pay_payment_correction_requests_selection_obj_chk CHECK (jsonb_typeof(selection_json) = 'object'::text);

-- public.pay_payment_correction_requests.pay_payment_correction_requests_status_chk
alter table public.pay_payment_correction_requests add constraint pay_payment_correction_requests_status_chk CHECK (status = ANY (ARRAY['PLANNING'::text, 'PLANNED'::text, 'REQUESTED'::text, 'AWAITING_AUTHORISATION'::text, 'AUTHORISED'::text, 'EXPANDED'::text, 'PROCESSING'::text, 'APPLIED'::text, 'APPLIED_WITH_BLOCKERS'::text, 'BLOCKED'::text, 'FAILED'::text, 'REJECTED'::text, 'CANCELLED'::text]));

-- public.pay_payment_correction_work_items.pay_payment_correction_work_items_attempt_count_chk
alter table public.pay_payment_correction_work_items add constraint pay_payment_correction_work_items_attempt_count_chk CHECK (attempt_count >= 0);

-- public.pay_payment_correction_work_items.pay_payment_correction_work_items_kind_chk
alter table public.pay_payment_correction_work_items add constraint pay_payment_correction_work_items_kind_chk CHECK (work_kind = ANY (ARRAY['PRE_BANK_CANCEL'::text, 'NO_MONEY_UNWIND'::text, 'SETTLED_REVERSAL'::text]));

-- public.pay_payment_correction_work_items.pay_payment_correction_work_items_result_obj_chk
alter table public.pay_payment_correction_work_items add constraint pay_payment_correction_work_items_result_obj_chk CHECK (jsonb_typeof(result_json) = 'object'::text);

-- public.pay_payment_correction_work_items.pay_payment_correction_work_items_selection_obj_chk
alter table public.pay_payment_correction_work_items add constraint pay_payment_correction_work_items_selection_obj_chk CHECK (jsonb_typeof(selection_json) = 'object'::text);

-- public.pay_payment_correction_work_items.pay_payment_correction_work_items_status_chk
alter table public.pay_payment_correction_work_items add constraint pay_payment_correction_work_items_status_chk CHECK (status = ANY (ARRAY['PENDING'::text, 'PROCESSING'::text, 'APPLIED'::text, 'SKIPPED'::text, 'BLOCKED'::text, 'FAILED_RETRYABLE'::text, 'FAILED_FINAL'::text, 'CANCELLED'::text]));

-- public.pay_payment_return_notice_groups.pay_payment_return_notice_groups_alert_fingerprint_chk
alter table public.pay_payment_return_notice_groups add constraint pay_payment_return_notice_groups_alert_fingerprint_chk CHECK (alert_fingerprint IS NULL OR btrim(alert_fingerprint) <> ''::text);

-- public.pay_payment_return_notice_groups.pay_payment_return_notice_groups_kind_chk
alter table public.pay_payment_return_notice_groups add constraint pay_payment_return_notice_groups_kind_chk CHECK (notice_kind = ANY (ARRAY['BANK_FAILURE_DETECTED'::text, 'BLOCKED_FUNDS'::text, 'NO_MONEY_UNWIND_REQUIRED'::text, 'NO_MONEY_UNWIND_APPLIED'::text, 'SETTLED_RETURN_DETECTED'::text, 'SETTLED_REVERSAL_REQUIRED'::text, 'SETTLED_REVERSAL_APPLIED'::text, 'AUTO_CORRECTION_BLOCKED'::text, 'MANUAL_CORRECTION_APPLIED'::text]));

-- public.pay_payment_return_notice_groups.pay_payment_return_notice_groups_mail_ids_arr_chk
alter table public.pay_payment_return_notice_groups add constraint pay_payment_return_notice_groups_mail_ids_arr_chk CHECK (jsonb_typeof(mail_outbox_ids) = 'array'::text);

-- public.pay_payment_return_notice_groups.pay_payment_return_notice_groups_send_window_chk
alter table public.pay_payment_return_notice_groups add constraint pay_payment_return_notice_groups_send_window_chk CHECK (max_send_at_utc >= quiet_until_utc);

-- public.pay_payment_return_notice_groups.pay_payment_return_notice_groups_status_chk
alter table public.pay_payment_return_notice_groups add constraint pay_payment_return_notice_groups_status_chk CHECK (status = ANY (ARRAY['OPEN'::text, 'READY'::text, 'SENT'::text, 'CANCELLED'::text, 'FAILED'::text]));

-- public.pay_payment_return_notice_groups.pay_payment_return_notice_groups_summary_obj_chk
alter table public.pay_payment_return_notice_groups add constraint pay_payment_return_notice_groups_summary_obj_chk CHECK (jsonb_typeof(summary_json) = 'object'::text);

-- public.pay_snooze_warning_acknowledgements.pay_snooze_warning_ack_code_chk
alter table public.pay_snooze_warning_acknowledgements add constraint pay_snooze_warning_ack_code_chk CHECK (warning_code = ANY (ARRAY['RATE_RESOLUTION_REQUIRES_REVIEW_AFTER_UNSNOOZE'::text, 'SNOOZE_DATE_BEFORE_NEXT_PAY_RUN'::text, 'SNOOZE_INCLUDES_NEXT_PAY_RUN'::text]));

-- public.pay_snooze_warning_acknowledgements.pay_snooze_warning_ack_expiry_chk
alter table public.pay_snooze_warning_acknowledgements add constraint pay_snooze_warning_ack_expiry_chk CHECK (expires_at_utc > created_at_utc);

-- public.pay_snooze_warning_acknowledgements.pay_snooze_warning_ack_kind_chk
alter table public.pay_snooze_warning_acknowledgements add constraint pay_snooze_warning_ack_kind_chk CHECK (snooze_kind = ANY (ARRAY['DO_NOT_PAY'::text, 'BLOCKED_TIMESHEET'::text, 'TIMESHEET_PAYMENT'::text, 'OVERPAYMENT_RECOVERY'::text, 'PAYMENT_ADVANCE_REPAYMENT'::text, 'MANUAL_DEBT_RECOVERY'::text]));

-- public.pay_snooze_warning_acknowledgements.pay_snooze_warning_ack_phase_chk
alter table public.pay_snooze_warning_acknowledgements add constraint pay_snooze_warning_ack_phase_chk CHECK (validation_phase = ANY (ARRAY['PRE_OPEN'::text, 'PRE_SAVE'::text]));

-- public.pay_snooze_warning_acknowledgements.pay_snooze_warning_ack_scope_chk
alter table public.pay_snooze_warning_acknowledgements add constraint pay_snooze_warning_ack_scope_chk CHECK ((snooze_kind = ANY (ARRAY['OVERPAYMENT_RECOVERY'::text, 'PAYMENT_ADVANCE_REPAYMENT'::text, 'MANUAL_DEBT_RECOVERY'::text])) AND source_ref ~* '^advance:[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'::text AND timesheet_id IS NULL AND segment_id IS NULL AND segment_stable_key IS NULL OR snooze_kind = 'DO_NOT_PAY'::text AND lower(btrim(source_ref)) ~ '^timesheet-expense:[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}:(expenses|travel|accommodation|other|mileage):[0-9a-f]{32}$'::text AND timesheet_id IS NOT NULL AND split_part(lower(btrim(source_ref)), ':'::text, 2) = lower(timesheet_id::text) AND segment_id IS NULL AND segment_stable_key IS NULL OR (snooze_kind = ANY (ARRAY['DO_NOT_PAY'::text, 'BLOCKED_TIMESHEET'::text, 'TIMESHEET_PAYMENT'::text])) AND source_ref IS NULL);

-- public.pay_snooze_warning_acknowledgements.pay_snooze_warning_ack_token_hash_chk
alter table public.pay_snooze_warning_acknowledgements add constraint pay_snooze_warning_ack_token_hash_chk CHECK (token_hash ~ '^[0-9a-f]{32}$'::text);

-- public.rates_candidate_overrides.rates_candidate_overrides_date_order_chk
alter table public.rates_candidate_overrides add constraint rates_candidate_overrides_date_order_chk CHECK (date_to IS NULL OR date_to >= date_from);

-- public.rates_candidate_overrides.rates_candidate_overrides_rate_type_chk
alter table public.rates_candidate_overrides add constraint rates_candidate_overrides_rate_type_chk CHECK (rate_type = ANY (ARRAY['PAYE'::text, 'UMBRELLA'::text]));

-- public.rates_client_defaults.rates_client_defaults_date_order_chk
alter table public.rates_client_defaults add constraint rates_client_defaults_date_order_chk CHECK (date_to IS NULL OR date_to >= date_from);

-- public.rates_presets.chk_preset_scope_client
alter table public.rates_presets add constraint chk_preset_scope_client CHECK (scope = 'GLOBAL'::preset_scope_enum AND client_id IS NULL OR scope = 'CLIENT'::preset_scope_enum AND client_id IS NOT NULL);

-- public.rates_presets.rates_presets_bucket_labels_json_chk
alter table public.rates_presets add constraint rates_presets_bucket_labels_json_chk CHECK (bucket_labels_json IS NULL OR jsonb_typeof(bucket_labels_json) = 'object'::text AND (bucket_labels_json - ARRAY['day'::text, 'night'::text, 'sat'::text, 'sun'::text, 'bh'::text]) = '{}'::jsonb AND bucket_labels_json ? 'day'::text AND bucket_labels_json ? 'night'::text AND bucket_labels_json ? 'sat'::text AND bucket_labels_json ? 'sun'::text AND bucket_labels_json ? 'bh'::text AND NULLIF(btrim(bucket_labels_json ->> 'day'::text), ''::text) IS NOT NULL AND NULLIF(btrim(bucket_labels_json ->> 'night'::text), ''::text) IS NOT NULL AND NULLIF(btrim(bucket_labels_json ->> 'sat'::text), ''::text) IS NOT NULL AND NULLIF(btrim(bucket_labels_json ->> 'sun'::text), ''::text) IS NOT NULL AND NULLIF(btrim(bucket_labels_json ->> 'bh'::text), ''::text) IS NOT NULL);

-- public.rates_presets.rates_presets_mileage_nonneg_chk
alter table public.rates_presets add constraint rates_presets_mileage_nonneg_chk CHECK ((mileage_pay_rate IS NULL OR mileage_pay_rate >= 0::numeric) AND (mileage_charge_rate IS NULL OR mileage_charge_rate >= 0::numeric));

-- public.report_presets.report_presets_kind_check
alter table public.report_presets add constraint report_presets_kind_check CHECK (kind = ANY (ARRAY['search'::text, 'report'::text, 'dashboard'::text, 'selection'::text]));

-- public.report_presets.report_presets_section_check
alter table public.report_presets add constraint report_presets_section_check CHECK (section = ANY (ARRAY['candidates'::text, 'clients'::text, 'contracts'::text, 'umbrellas'::text, 'timesheets'::text, 'invoices'::text]));

-- public.settings_defaults.settings_bpay_execution_refresh_owner_bridge_v1_dependency_ck
alter table public.settings_defaults add constraint settings_bpay_execution_refresh_owner_bridge_v1_dependency_ck CHECK (banking_pay_execution_refresh_owner_bridge_v1_publish_enabled IS NOT TRUE OR banking_pay_execution_refresh_owner_bridge_v1_observe_enabled IS TRUE AND banking_pay_scheduled_cancellation_reversion_v2_observe_enabled IS TRUE AND banking_pay_scheduled_cancellation_reversion_v2_publish_enabled IS TRUE AND banking_pay_cancellation_reversion_observe_v1_enabled IS TRUE AND banking_pay_cancellation_reversion_publish_v1_enabled IS TRUE AND banking_pay_workbench_semantic_ready_publication_v3_enabled IS TRUE AND banking_pay_workbench_semantic_ready_draft_guard_v2_enabled IS TRUE);

-- public.settings_defaults.settings_bpay_reconciliation_optimization_version_ck
alter table public.settings_defaults add constraint settings_bpay_reconciliation_optimization_version_ck CHECK (banking_pay_workbench_reconciliation_optimization_version = ANY (ARRAY[0, 1]));

-- public.settings_defaults.settings_bpay_scheduled_reversion_v2_dependency_ck
alter table public.settings_defaults add constraint settings_bpay_scheduled_reversion_v2_dependency_ck CHECK (banking_pay_scheduled_cancellation_reversion_v2_publish_enabled IS NOT TRUE OR banking_pay_scheduled_cancellation_reversion_v2_observe_enabled IS TRUE AND banking_pay_cancellation_reversion_observe_v1_enabled IS TRUE AND banking_pay_cancellation_reversion_publish_v1_enabled IS TRUE AND banking_pay_workbench_semantic_ready_publication_v3_enabled IS TRUE AND banking_pay_workbench_semantic_ready_draft_guard_v2_enabled IS TRUE);

-- public.settings_defaults.settings_bpay_semantic_cancellation_dependency_ck
alter table public.settings_defaults add constraint settings_bpay_semantic_cancellation_dependency_ck CHECK (banking_pay_cancellation_reversion_publish_v1_enabled IS NOT TRUE OR banking_pay_workbench_semantic_ready_publication_v3_enabled IS TRUE AND banking_pay_workbench_semantic_ready_draft_guard_v2_enabled IS TRUE);

-- public.settings_defaults.settings_bpay_wb_reconciliation_envelope_json_chk
alter table public.settings_defaults add constraint settings_bpay_wb_reconciliation_envelope_json_chk CHECK (jsonb_typeof(banking_pay_workbench_reconciliation_envelope_json) = 'object'::text AND jsonb_typeof(banking_pay_workbench_reconciliation_envelope_evidence_json) = 'object'::text);

-- public.settings_defaults.settings_bpay_wb_reconciliation_envelope_version_chk
alter table public.settings_defaults add constraint settings_bpay_wb_reconciliation_envelope_version_chk CHECK (banking_pay_workbench_reconciliation_envelope_version > 0);

-- public.settings_defaults.settings_defaults_banking_pay_official_pay_weekday_chk
alter table public.settings_defaults add constraint settings_defaults_banking_pay_official_pay_weekday_chk CHECK (banking_pay_official_pay_weekday >= 0 AND banking_pay_official_pay_weekday <= 6);

-- public.settings_defaults.settings_defaults_banking_pay_workbench_source_build_profile_ch
alter table public.settings_defaults add constraint settings_defaults_banking_pay_workbench_source_build_profile_ch CHECK (banking_pay_workbench_source_build_execution_profile_version = ANY (ARRAY[1, 2]));

-- public.settings_defaults.settings_defaults_banking_system_check
alter table public.settings_defaults add constraint settings_defaults_banking_system_check CHECK (banking_system = ANY (ARRAY['REVOLUT_API'::text, 'MONZO_CSV'::text, 'REVOLUT_CSV'::text]));

-- public.settings_defaults.settings_defaults_bh_source_check
alter table public.settings_defaults add constraint settings_defaults_bh_source_check CHECK (bh_source = ANY (ARRAY['MANUAL'::text, 'FEED'::text]));

-- public.settings_defaults.settings_defaults_bpay_cancel_candidate_items_chk
alter table public.settings_defaults add constraint settings_defaults_bpay_cancel_candidate_items_chk CHECK (banking_pay_correction_max_active_items_per_candidate >= 1 AND banking_pay_correction_max_active_items_per_candidate <= 128);

-- public.settings_defaults.settings_defaults_bpay_cancel_candidate_rows_chk
alter table public.settings_defaults add constraint settings_defaults_bpay_cancel_candidate_rows_chk CHECK (banking_pay_correction_max_source_rows_per_candidate >= 1 AND banking_pay_correction_max_source_rows_per_candidate <= 512);

-- public.settings_defaults.settings_defaults_bpay_cancel_max_candidates_chk
alter table public.settings_defaults add constraint settings_defaults_bpay_cancel_max_candidates_chk CHECK (banking_pay_correction_max_candidates >= 1 AND banking_pay_correction_max_candidates <= 10000);

-- public.settings_defaults.settings_defaults_bpay_cancel_max_items_chk
alter table public.settings_defaults add constraint settings_defaults_bpay_cancel_max_items_chk CHECK (banking_pay_correction_max_active_items >= 1 AND banking_pay_correction_max_active_items <= 250000);

-- public.settings_defaults.settings_defaults_bpw_auto_continuation_claim_limit_max_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_auto_continuation_claim_limit_max_chk CHECK (banking_pay_workbench_auto_continuation_claim_limit_max >= 1 AND banking_pay_workbench_auto_continuation_claim_limit_max <= 50);

-- public.settings_defaults.settings_defaults_bpw_auto_continuation_max_bursts_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_auto_continuation_max_bursts_chk CHECK (banking_pay_workbench_auto_continuation_max_bursts >= 0 AND banking_pay_workbench_auto_continuation_max_bursts <= 8);

-- public.settings_defaults.settings_defaults_bpw_auto_continuation_max_jobs_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_auto_continuation_max_jobs_chk CHECK (banking_pay_workbench_auto_continuation_max_jobs >= 1 AND banking_pay_workbench_auto_continuation_max_jobs <= 150);

-- public.settings_defaults.settings_defaults_bpw_auto_continuation_max_passes_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_auto_continuation_max_passes_chk CHECK (banking_pay_workbench_auto_continuation_max_passes >= 1 AND banking_pay_workbench_auto_continuation_max_passes <= 2);

-- public.settings_defaults.settings_defaults_bpw_auto_continuation_max_rows_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_auto_continuation_max_rows_chk CHECK (banking_pay_workbench_auto_continuation_max_rows >= 1 AND banking_pay_workbench_auto_continuation_max_rows <= 5000);

-- public.settings_defaults.settings_defaults_bpw_auto_continuation_max_runtime_ms_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_auto_continuation_max_runtime_ms_chk CHECK (banking_pay_workbench_auto_continuation_max_runtime_ms >= 5000 AND banking_pay_workbench_auto_continuation_max_runtime_ms <= 60000);

-- public.settings_defaults.settings_defaults_bpw_auto_continuation_min_runtime_ms_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_auto_continuation_min_runtime_ms_chk CHECK (banking_pay_workbench_auto_continuation_min_runtime_ms >= 1000 AND banking_pay_workbench_auto_continuation_min_runtime_ms <= 15000 AND banking_pay_workbench_auto_continuation_min_runtime_ms <= banking_pay_workbench_auto_continuation_per_burst_max_runtime_m);

-- public.settings_defaults.settings_defaults_bpw_auto_continuation_per_burst_max_runtime_m
alter table public.settings_defaults add constraint settings_defaults_bpw_auto_continuation_per_burst_max_runtime_m CHECK (banking_pay_workbench_auto_continuation_per_burst_max_runtime_m >= 1000 AND banking_pay_workbench_auto_continuation_per_burst_max_runtime_m <= 15000);

-- public.settings_defaults.settings_defaults_bpw_clone_rebase_budget_ms_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_clone_rebase_budget_ms_chk CHECK (banking_pay_workbench_clone_rebase_budget_ms >= 500 AND banking_pay_workbench_clone_rebase_budget_ms <= 30000);

-- public.settings_defaults.settings_defaults_bpw_clone_rebase_units_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_clone_rebase_units_chk CHECK (banking_pay_workbench_clone_rebase_units_per_job >= 1 AND banking_pay_workbench_clone_rebase_units_per_job <= 1000);

-- public.settings_defaults.settings_defaults_bpw_cron_claim_limit_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_cron_claim_limit_chk CHECK (banking_pay_workbench_cron_claim_limit >= 1 AND banking_pay_workbench_cron_claim_limit <= 100);

-- public.settings_defaults.settings_defaults_bpw_cron_clone_rebase_units_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_cron_clone_rebase_units_chk CHECK (banking_pay_workbench_cron_clone_rebase_units_per_job >= 1 AND banking_pay_workbench_cron_clone_rebase_units_per_job <= 2000);

-- public.settings_defaults.settings_defaults_bpw_cron_line_process_units_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_cron_line_process_units_chk CHECK (banking_pay_workbench_cron_line_process_units_per_job >= 1 AND banking_pay_workbench_cron_line_process_units_per_job <= 100);

-- public.settings_defaults.settings_defaults_bpw_cron_line_seed_units_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_cron_line_seed_units_chk CHECK (banking_pay_workbench_cron_line_seed_units_per_job >= 1 AND banking_pay_workbench_cron_line_seed_units_per_job <= 100);

-- public.settings_defaults.settings_defaults_bpw_cron_max_jobs_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_cron_max_jobs_chk CHECK (banking_pay_workbench_cron_max_jobs >= 1 AND banking_pay_workbench_cron_max_jobs <= 150);

-- public.settings_defaults.settings_defaults_bpw_cron_max_passes_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_cron_max_passes_chk CHECK (banking_pay_workbench_cron_max_passes >= 1 AND banking_pay_workbench_cron_max_passes <= 2);

-- public.settings_defaults.settings_defaults_bpw_cron_max_rows_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_cron_max_rows_chk CHECK (banking_pay_workbench_cron_max_rows >= 1 AND banking_pay_workbench_cron_max_rows <= 5000);

-- public.settings_defaults.settings_defaults_bpw_cron_max_runtime_ms_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_cron_max_runtime_ms_chk CHECK (banking_pay_workbench_cron_max_runtime_ms >= 1000 AND banking_pay_workbench_cron_max_runtime_ms <= 30000);

-- public.settings_defaults.settings_defaults_bpw_cron_preview_mat_units_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_cron_preview_mat_units_chk CHECK (banking_pay_workbench_cron_preview_mat_units_per_job >= 1 AND banking_pay_workbench_cron_preview_mat_units_per_job <= 100);

-- public.settings_defaults.settings_defaults_bpw_cron_scope_seed_units_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_cron_scope_seed_units_chk CHECK (banking_pay_workbench_cron_scope_seed_units_per_job >= 1 AND banking_pay_workbench_cron_scope_seed_units_per_job <= 100);

-- public.settings_defaults.settings_defaults_bpw_cron_source_build_units_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_cron_source_build_units_chk CHECK (banking_pay_workbench_cron_source_build_units_per_job >= 1 AND banking_pay_workbench_cron_source_build_units_per_job <= 100);

-- public.settings_defaults.settings_defaults_bpw_cron_source_bursts_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_cron_source_bursts_chk CHECK (banking_pay_workbench_cron_source_build_parallel_bursts >= 0 AND banking_pay_workbench_cron_source_build_parallel_bursts <= 100);

-- public.settings_defaults.settings_defaults_bpw_cron_source_lane_claim_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_cron_source_lane_claim_chk CHECK (banking_pay_workbench_cron_source_build_lane_claim_limit >= 1 AND banking_pay_workbench_cron_source_build_lane_claim_limit <= 10);

-- public.settings_defaults.settings_defaults_bpw_cron_source_parallelism_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_cron_source_parallelism_chk CHECK (banking_pay_workbench_cron_source_build_parallelism >= 0 AND banking_pay_workbench_cron_source_build_parallelism <= 32);

-- public.settings_defaults.settings_defaults_bpw_cron_source_runtime_floor_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_cron_source_runtime_floor_chk CHECK (banking_pay_workbench_cron_source_build_runtime_floor_ms >= 0 AND banking_pay_workbench_cron_source_build_runtime_floor_ms <= 300000);

-- public.settings_defaults.settings_defaults_bpw_db_idle_tx_timeout_ms_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_db_idle_tx_timeout_ms_chk CHECK (banking_pay_workbench_db_idle_tx_timeout_ms >= 5000 AND banking_pay_workbench_db_idle_tx_timeout_ms <= 60000);

-- public.settings_defaults.settings_defaults_bpw_db_lock_timeout_ms_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_db_lock_timeout_ms_chk CHECK (banking_pay_workbench_db_lock_timeout_ms >= 100 AND banking_pay_workbench_db_lock_timeout_ms <= 5000);

-- public.settings_defaults.settings_defaults_bpw_db_statement_timeout_ms_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_db_statement_timeout_ms_chk CHECK (banking_pay_workbench_db_statement_timeout_ms >= 1000 AND banking_pay_workbench_db_statement_timeout_ms <= 30000);

-- public.settings_defaults.settings_defaults_bpw_db_worker_lease_seconds_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_db_worker_lease_seconds_chk CHECK (banking_pay_workbench_db_worker_lease_seconds IS NULL OR banking_pay_workbench_db_worker_lease_seconds >= 25 AND banking_pay_workbench_db_worker_lease_seconds <= 3600);

-- public.settings_defaults.settings_defaults_bpw_db_worker_max_runtime_ms_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_db_worker_max_runtime_ms_chk CHECK (banking_pay_workbench_db_worker_max_runtime_ms >= 1000 AND banking_pay_workbench_db_worker_max_runtime_ms <= 30000);

-- public.settings_defaults.settings_defaults_bpw_db_worker_min_phase_budget_ms_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_db_worker_min_phase_budget_ms_chk CHECK (banking_pay_workbench_db_worker_min_phase_budget_ms >= 250 AND banking_pay_workbench_db_worker_min_phase_budget_ms <= 15000 AND banking_pay_workbench_db_worker_min_phase_budget_ms < banking_pay_workbench_db_worker_max_runtime_ms);

-- public.settings_defaults.settings_defaults_bpw_job_retry_base_seconds_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_job_retry_base_seconds_chk CHECK (banking_pay_workbench_job_retry_base_seconds >= 5 AND banking_pay_workbench_job_retry_base_seconds <= 3600);

-- public.settings_defaults.settings_defaults_bpw_job_retry_max_seconds_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_job_retry_max_seconds_chk CHECK (banking_pay_workbench_job_retry_max_seconds >= 5 AND banking_pay_workbench_job_retry_max_seconds <= 86400 AND banking_pay_workbench_job_retry_max_seconds >= banking_pay_workbench_job_retry_base_seconds);

-- public.settings_defaults.settings_defaults_bpw_line_process_units_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_line_process_units_chk CHECK (banking_pay_workbench_line_process_units_per_job >= 1 AND banking_pay_workbench_line_process_units_per_job <= 100);

-- public.settings_defaults.settings_defaults_bpw_line_seed_units_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_line_seed_units_chk CHECK (banking_pay_workbench_line_seed_units_per_job >= 1 AND banking_pay_workbench_line_seed_units_per_job <= 100);

-- public.settings_defaults.settings_defaults_bpw_minimum_rpc_budget_ms_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_minimum_rpc_budget_ms_chk CHECK (banking_pay_workbench_minimum_rpc_budget_ms IS NULL OR banking_pay_workbench_minimum_rpc_budget_ms >= 1000 AND banking_pay_workbench_minimum_rpc_budget_ms <= 30000);

-- public.settings_defaults.settings_defaults_bpw_nudge_claim_limit_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_nudge_claim_limit_chk CHECK (banking_pay_workbench_nudge_claim_limit >= 1 AND banking_pay_workbench_nudge_claim_limit <= 100);

-- public.settings_defaults.settings_defaults_bpw_nudge_clone_rebase_units_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_nudge_clone_rebase_units_chk CHECK (banking_pay_workbench_nudge_clone_rebase_units_per_job >= 1 AND banking_pay_workbench_nudge_clone_rebase_units_per_job <= 1000);

-- public.settings_defaults.settings_defaults_bpw_nudge_line_process_units_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_nudge_line_process_units_chk CHECK (banking_pay_workbench_nudge_line_process_units_per_job >= 1 AND banking_pay_workbench_nudge_line_process_units_per_job <= 100);

-- public.settings_defaults.settings_defaults_bpw_nudge_line_seed_units_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_nudge_line_seed_units_chk CHECK (banking_pay_workbench_nudge_line_seed_units_per_job >= 1 AND banking_pay_workbench_nudge_line_seed_units_per_job <= 100);

-- public.settings_defaults.settings_defaults_bpw_nudge_max_jobs_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_nudge_max_jobs_chk CHECK (banking_pay_workbench_nudge_max_jobs >= 1 AND banking_pay_workbench_nudge_max_jobs <= 150);

-- public.settings_defaults.settings_defaults_bpw_nudge_max_passes_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_nudge_max_passes_chk CHECK (banking_pay_workbench_nudge_max_passes >= 1 AND banking_pay_workbench_nudge_max_passes <= 2);

-- public.settings_defaults.settings_defaults_bpw_nudge_max_rows_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_nudge_max_rows_chk CHECK (banking_pay_workbench_nudge_max_rows >= 1 AND banking_pay_workbench_nudge_max_rows <= 5000);

-- public.settings_defaults.settings_defaults_bpw_nudge_max_runtime_ms_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_nudge_max_runtime_ms_chk CHECK (banking_pay_workbench_nudge_max_runtime_ms >= 1000 AND banking_pay_workbench_nudge_max_runtime_ms <= 30000);

-- public.settings_defaults.settings_defaults_bpw_nudge_preview_mat_units_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_nudge_preview_mat_units_chk CHECK (banking_pay_workbench_nudge_preview_mat_units_per_job >= 1 AND banking_pay_workbench_nudge_preview_mat_units_per_job <= 100);

-- public.settings_defaults.settings_defaults_bpw_nudge_scope_seed_units_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_nudge_scope_seed_units_chk CHECK (banking_pay_workbench_nudge_scope_seed_units_per_job >= 1 AND banking_pay_workbench_nudge_scope_seed_units_per_job <= 100);

-- public.settings_defaults.settings_defaults_bpw_nudge_source_build_units_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_nudge_source_build_units_chk CHECK (banking_pay_workbench_nudge_source_build_units_per_job >= 1 AND banking_pay_workbench_nudge_source_build_units_per_job <= 100);

-- public.settings_defaults.settings_defaults_bpw_nudge_source_bursts_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_nudge_source_bursts_chk CHECK (banking_pay_workbench_nudge_source_build_parallel_bursts >= 0 AND banking_pay_workbench_nudge_source_build_parallel_bursts <= 100);

-- public.settings_defaults.settings_defaults_bpw_nudge_source_lane_claim_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_nudge_source_lane_claim_chk CHECK (banking_pay_workbench_nudge_source_build_lane_claim_limit >= 1 AND banking_pay_workbench_nudge_source_build_lane_claim_limit <= 10);

-- public.settings_defaults.settings_defaults_bpw_nudge_source_parallelism_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_nudge_source_parallelism_chk CHECK (banking_pay_workbench_nudge_source_build_parallelism >= 0 AND banking_pay_workbench_nudge_source_build_parallelism <= 32);

-- public.settings_defaults.settings_defaults_bpw_nudge_source_runtime_floor_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_nudge_source_runtime_floor_chk CHECK (banking_pay_workbench_nudge_source_build_runtime_floor_ms >= 0 AND banking_pay_workbench_nudge_source_build_runtime_floor_ms <= 300000);

-- public.settings_defaults.settings_defaults_bpw_preview_mat_units_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_preview_mat_units_chk CHECK (banking_pay_workbench_preview_mat_units_per_job >= 1 AND banking_pay_workbench_preview_mat_units_per_job <= 100);

-- public.settings_defaults.settings_defaults_bpw_rollover_max_sessions_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_rollover_max_sessions_chk CHECK (banking_pay_workbench_rollover_max_sessions_per_tick >= 0 AND banking_pay_workbench_rollover_max_sessions_per_tick <= 50);

-- public.settings_defaults.settings_defaults_bpw_rpc_safety_buffer_ms_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_rpc_safety_buffer_ms_chk CHECK (banking_pay_workbench_rpc_safety_buffer_ms >= 100 AND banking_pay_workbench_rpc_safety_buffer_ms <= 5000);

-- public.settings_defaults.settings_defaults_bpw_scope_seed_units_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_scope_seed_units_chk CHECK (banking_pay_workbench_scope_seed_units_per_job >= 1 AND banking_pay_workbench_scope_seed_units_per_job <= 100);

-- public.settings_defaults.settings_defaults_bpw_source_build_units_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_source_build_units_chk CHECK (banking_pay_workbench_source_build_units_per_job >= 1 AND banking_pay_workbench_source_build_units_per_job <= 100);

-- public.settings_defaults.settings_defaults_bpw_stage_work_units_per_job_chk
alter table public.settings_defaults add constraint settings_defaults_bpw_stage_work_units_per_job_chk CHECK (banking_pay_workbench_stage_work_units_per_job >= 1 AND banking_pay_workbench_stage_work_units_per_job <= 100);

-- public.settings_defaults.settings_defaults_candidate_app_environment_ck
alter table public.settings_defaults add constraint settings_defaults_candidate_app_environment_ck CHECK (candidate_app_environment = ANY (ARRAY['TEST'::text, 'LIVE'::text]));

-- public.settings_defaults.settings_defaults_candidate_app_flags_object_ck
alter table public.settings_defaults add constraint settings_defaults_candidate_app_flags_object_ck CHECK (jsonb_typeof(candidate_app_feature_flags_json) = 'object'::text);

-- public.settings_defaults.settings_defaults_candidate_barred_domains_array_ck
alter table public.settings_defaults add constraint settings_defaults_candidate_barred_domains_array_ck CHECK (jsonb_typeof(candidate_barred_manager_email_domains) = 'array'::text);

-- public.settings_defaults.settings_defaults_candidate_deviation_ck
alter table public.settings_defaults add constraint settings_defaults_candidate_deviation_ck CHECK (candidate_hours_deviation_pct >= 0::numeric AND candidate_hours_deviation_pct <= 500::numeric);

-- public.settings_defaults.settings_defaults_comms_adaptors_json_is_object_chk
alter table public.settings_defaults add constraint settings_defaults_comms_adaptors_json_is_object_chk CHECK (jsonb_typeof(comms_adaptors_json) = 'object'::text);

-- public.settings_defaults.settings_defaults_external_paye_system_check
alter table public.settings_defaults add constraint settings_defaults_external_paye_system_check CHECK (external_paye_system = ANY (ARRAY['SAGE'::text, 'CSV'::text]));

-- public.settings_defaults.settings_defaults_funds_warning_hours_json_is_array_chk
alter table public.settings_defaults add constraint settings_defaults_funds_warning_hours_json_is_array_chk CHECK (jsonb_typeof(funds_warning_hours_json) = 'array'::text);

-- public.settings_defaults.settings_defaults_id_check
alter table public.settings_defaults add constraint settings_defaults_id_check CHECK (id = 1);

-- public.settings_defaults.settings_defaults_invoice_document_presentation_json_chk
alter table public.settings_defaults add constraint settings_defaults_invoice_document_presentation_json_chk CHECK (invoice_document_presentation_json IS NOT NULL AND jsonb_typeof(invoice_document_presentation_json) = 'object'::text AND ((invoice_document_presentation_json -> 'branding'::text) IS NULL OR (invoice_document_presentation_json -> 'branding'::text) = 'null'::jsonb OR jsonb_typeof(invoice_document_presentation_json -> 'branding'::text) = 'object'::text) AND ((invoice_document_presentation_json #> '{branding,logo}'::text[]) IS NULL OR (invoice_document_presentation_json #> '{branding,logo}'::text[]) = 'null'::jsonb OR jsonb_typeof(invoice_document_presentation_json #> '{branding,logo}'::text[]) = 'object'::text AND NULLIF(btrim(COALESCE(invoice_document_presentation_json #>> '{branding,logo,r2_key}'::text[], ''::text)), ''::text) IS NOT NULL AND COALESCE(invoice_document_presentation_json #>> '{branding,logo,sha256}'::text[], ''::text) ~ '^[0-9a-f]{64}$'::text AND COALESCE(invoice_document_presentation_json #>> '{branding,logo,size_bytes}'::text[], ''::text) ~ '^[1-9][0-9]{0,18}$'::text AND (lower(COALESCE(invoice_document_presentation_json #>> '{branding,logo,media_type}'::text[], ''::text)) = ANY (ARRAY['image/jpeg'::text, 'image/png'::text]))) AND ((invoice_document_presentation_json -> 'self_bill_legal_wording'::text) IS NULL OR (invoice_document_presentation_json -> 'self_bill_legal_wording'::text) = 'null'::jsonb OR jsonb_typeof(invoice_document_presentation_json -> 'self_bill_legal_wording'::text) = 'string'::text) AND ((invoice_document_presentation_json -> 'legal_wording'::text) IS NULL OR (invoice_document_presentation_json -> 'legal_wording'::text) = 'null'::jsonb OR jsonb_typeof(invoice_document_presentation_json -> 'legal_wording'::text) = 'array'::text) AND ((invoice_document_presentation_json -> 'hide_bank_footer_default'::text) IS NULL OR (invoice_document_presentation_json -> 'hide_bank_footer_default'::text) = 'null'::jsonb OR jsonb_typeof(invoice_document_presentation_json -> 'hide_bank_footer_default'::text) = 'boolean'::text) AND ((invoice_document_presentation_json -> 'payment_instructions'::text) IS NULL OR (invoice_document_presentation_json -> 'payment_instructions'::text) = 'null'::jsonb OR jsonb_typeof(invoice_document_presentation_json -> 'payment_instructions'::text) = 'string'::text) AND ((invoice_document_presentation_json -> 'remittance_email'::text) IS NULL OR (invoice_document_presentation_json -> 'remittance_email'::text) = 'null'::jsonb OR jsonb_typeof(invoice_document_presentation_json -> 'remittance_email'::text) = 'string'::text) AND ((invoice_document_presentation_json -> 'locale'::text) IS NULL OR (invoice_document_presentation_json -> 'locale'::text) = 'null'::jsonb OR jsonb_typeof(invoice_document_presentation_json -> 'locale'::text) = 'string'::text) AND ((invoice_document_presentation_json -> 'page_geometry'::text) IS NULL OR (invoice_document_presentation_json -> 'page_geometry'::text) = 'null'::jsonb OR jsonb_typeof(invoice_document_presentation_json -> 'page_geometry'::text) = 'string'::text));

-- public.settings_defaults.settings_defaults_max_attachments_per_email_range
alter table public.settings_defaults add constraint settings_defaults_max_attachments_per_email_range CHECK (max_attachments_per_email >= 1 AND max_attachments_per_email <= 100);

-- public.settings_defaults.settings_defaults_pay_eligibility_months_back_range_chk
alter table public.settings_defaults add constraint settings_defaults_pay_eligibility_months_back_range_chk CHECK (pay_eligibility_months_back >= 0 AND pay_eligibility_months_back <= 120);

-- public.settings_defaults.settings_defaults_pay_eligibility_weeks_ahead_range_chk
alter table public.settings_defaults add constraint settings_defaults_pay_eligibility_weeks_ahead_range_chk CHECK (pay_eligibility_weeks_ahead >= 0 AND pay_eligibility_weeks_ahead <= 52);

-- public.settings_defaults.settings_defaults_pay_export_csv_columns_json_is_array_chk
alter table public.settings_defaults add constraint settings_defaults_pay_export_csv_columns_json_is_array_chk CHECK (pay_export_csv_columns_json IS NULL OR jsonb_typeof(pay_export_csv_columns_json) = 'array'::text);

-- public.settings_defaults.settings_defaults_pay_export_csv_format_json_shape_chk
alter table public.settings_defaults add constraint settings_defaults_pay_export_csv_format_json_shape_chk CHECK (jsonb_typeof(pay_export_csv_format_json) = 'object'::text AND (NOT pay_export_csv_format_json ? 'headers'::text OR jsonb_typeof(pay_export_csv_format_json -> 'headers'::text) = 'object'::text) AND (NOT pay_export_csv_format_json ? 'include_header'::text OR jsonb_typeof(pay_export_csv_format_json -> 'include_header'::text) = 'boolean'::text) AND (NOT pay_export_csv_format_json ? 'bom'::text OR jsonb_typeof(pay_export_csv_format_json -> 'bom'::text) = 'boolean'::text) AND (NOT pay_export_csv_format_json ? 'delimiter'::text OR jsonb_typeof(pay_export_csv_format_json -> 'delimiter'::text) = 'string'::text) AND (NOT pay_export_csv_format_json ? 'empty_value'::text OR jsonb_typeof(pay_export_csv_format_json -> 'empty_value'::text) = 'string'::text) AND (NOT pay_export_csv_format_json ? 'line_ending'::text OR ((pay_export_csv_format_json ->> 'line_ending'::text) = ANY (ARRAY['LF'::text, 'CRLF'::text]))) AND (NOT pay_export_csv_format_json ? 'quote_mode'::text OR ((pay_export_csv_format_json ->> 'quote_mode'::text) = ANY (ARRAY['MINIMAL'::text, 'ALL'::text]))) AND (NOT pay_export_csv_format_json ? 'sort_code_format'::text OR ((pay_export_csv_format_json ->> 'sort_code_format'::text) = ANY (ARRAY['AS_STORED'::text, 'DIGITS'::text, 'HYPHENATED'::text]))) AND (NOT pay_export_csv_format_json ? 'account_number_format'::text OR ((pay_export_csv_format_json ->> 'account_number_format'::text) = ANY (ARRAY['AS_STORED'::text, 'DIGITS'::text]))) AND (NOT pay_export_csv_format_json ? 'amount_decimals'::text OR jsonb_typeof(pay_export_csv_format_json -> 'amount_decimals'::text) = 'number'::text AND (pay_export_csv_format_json ->> 'amount_decimals'::text) ~ '^[0-4]$'::text));

-- public.settings_defaults.settings_defaults_payment_authoriser_quantity_range_chk
alter table public.settings_defaults add constraint settings_defaults_payment_authoriser_quantity_range_chk CHECK (payment_authoriser_quantity >= 1 AND payment_authoriser_quantity <= 10);

-- public.settings_defaults.settings_defaults_payment_remittance_send_timing_chk
alter table public.settings_defaults add constraint settings_defaults_payment_remittance_send_timing_chk CHECK (payment_remittance_send_timing = ANY (ARRAY['ON_EXECUTION'::text, 'ON_PAYMENT_CONFIRMED'::text]));

-- public.settings_defaults.settings_defaults_payment_return_admin_max_wait_minutes_chk
alter table public.settings_defaults add constraint settings_defaults_payment_return_admin_max_wait_minutes_chk CHECK (payment_return_admin_notice_max_wait_minutes >= payment_return_admin_notice_quiet_minutes AND payment_return_admin_notice_max_wait_minutes <= 1440);

-- public.settings_defaults.settings_defaults_payment_return_admin_quiet_minutes_chk
alter table public.settings_defaults add constraint settings_defaults_payment_return_admin_quiet_minutes_chk CHECK (payment_return_admin_notice_quiet_minutes >= 0 AND payment_return_admin_notice_quiet_minutes <= 1440);

-- public.settings_defaults.settings_defaults_rail_env_default_check
alter table public.settings_defaults add constraint settings_defaults_rail_env_default_check CHECK (rail_env_default = ANY (ARRAY['PROD'::text, 'SANDBOX'::text]));

-- public.settings_defaults.settings_defaults_rail_provider_default_check
alter table public.settings_defaults add constraint settings_defaults_rail_provider_default_check CHECK (rail_provider_default = ANY (ARRAY['REVOLUT'::text, 'CSV'::text]));

-- public.settings_defaults.settings_defaults_remittance_includes_json_chk
alter table public.settings_defaults add constraint settings_defaults_remittance_includes_json_chk CHECK (remittance_includes_json IS NULL OR jsonb_typeof(remittance_includes_json) = 'object'::text AND remittance_includes_json ? 'weekly'::text AND remittance_includes_json ? 'daily'::text);

-- public.settings_finance_windows.chk_finwin_mileage_charge_nonneg
alter table public.settings_finance_windows add constraint chk_finwin_mileage_charge_nonneg CHECK (mileage_charge_defaults IS NULL OR mileage_charge_defaults >= 0::numeric);

-- public.settings_finance_windows.chk_finwin_mileage_pay_nonneg
alter table public.settings_finance_windows add constraint chk_finwin_mileage_pay_nonneg CHECK (mileage_pay_defaults IS NULL OR mileage_pay_defaults >= 0::numeric);

-- public.settings_finance_windows.settings_finance_windows_date_range_chk
alter table public.settings_finance_windows add constraint settings_finance_windows_date_range_chk CHECK (date_to IS NULL OR date_to >= date_from);

-- public.timesheet_archive_transition_capability.timesheet_archive_transition_capability_action_ck
alter table public.timesheet_archive_transition_capability add constraint timesheet_archive_transition_capability_action_ck CHECK (action = ANY (ARRAY['ARCHIVE'::text, 'UNARCHIVE'::text]));

-- public.timesheet_evidence.timesheet_evidence_asset_revision_ck
alter table public.timesheet_evidence add constraint timesheet_evidence_asset_revision_ck CHECK (document_asset_id IS NULL OR NULLIF(btrim(source_revision), ''::text) IS NOT NULL);

-- public.timesheet_evidence.timesheet_evidence_document_role_ck
alter table public.timesheet_evidence add constraint timesheet_evidence_document_role_ck CHECK (document_role = ANY (ARRAY['SOURCE_EVIDENCE'::text, 'MILEAGE_CLAIM_FORM'::text, 'EXPENSE_MILEAGE_APPROVAL_SUMMARY'::text, 'SIGNED_TIMESHEET'::text, 'MANAGER_SIGNATURE'::text, 'CANDIDATE_SIGNATURE'::text]));

-- public.timesheet_evidence.timesheet_evidence_processing_state_ck
alter table public.timesheet_evidence add constraint timesheet_evidence_processing_state_ck CHECK (processing_state = ANY (ARRAY['DISCOVERED'::text, 'INSPECTING'::text, 'NORMALISING'::text, 'READY'::text, 'UNSUPPORTED'::text, 'CORRUPT'::text, 'MISSING'::text, 'FAILED'::text, 'SUPERSEDED'::text]));

-- public.timesheet_lifecycle_bulk_operation_items.timesheet_lifecycle_bulk_operation_items_action_check
alter table public.timesheet_lifecycle_bulk_operation_items add constraint timesheet_lifecycle_bulk_operation_items_action_check CHECK (action = ANY (ARRAY['AUTHORISE'::text, 'UNAUTHORISE'::text]));

-- public.timesheet_lifecycle_bulk_operation_items.timesheet_lifecycle_bulk_operation_items_attempt_count_check
alter table public.timesheet_lifecycle_bulk_operation_items add constraint timesheet_lifecycle_bulk_operation_items_attempt_count_check CHECK (attempt_count >= 0);

-- public.timesheet_lifecycle_bulk_operation_items.timesheet_lifecycle_bulk_operation_items_ordinal_check
alter table public.timesheet_lifecycle_bulk_operation_items add constraint timesheet_lifecycle_bulk_operation_items_ordinal_check CHECK (ordinal >= 0);

-- public.timesheet_lifecycle_bulk_operation_items.timesheet_lifecycle_bulk_operation_items_status_check
alter table public.timesheet_lifecycle_bulk_operation_items add constraint timesheet_lifecycle_bulk_operation_items_status_check CHECK (status = ANY (ARRAY['QUEUED'::text, 'PROCESSING'::text, 'COMPLETED'::text, 'FAILED'::text, 'RETRY'::text, 'CANCELLED'::text]));

-- public.timesheet_lifecycle_bulk_operations.timesheet_lifecycle_bulk_operations_action_check
alter table public.timesheet_lifecycle_bulk_operations add constraint timesheet_lifecycle_bulk_operations_action_check CHECK (action = ANY (ARRAY['AUTHORISE'::text, 'UNAUTHORISE'::text]));

-- public.timesheet_lifecycle_bulk_operations.timesheet_lifecycle_bulk_operations_failure_count_check
alter table public.timesheet_lifecycle_bulk_operations add constraint timesheet_lifecycle_bulk_operations_failure_count_check CHECK (failure_count >= 0);

-- public.timesheet_lifecycle_bulk_operations.timesheet_lifecycle_bulk_operations_requested_count_check
alter table public.timesheet_lifecycle_bulk_operations add constraint timesheet_lifecycle_bulk_operations_requested_count_check CHECK (requested_count >= 0);

-- public.timesheet_lifecycle_bulk_operations.timesheet_lifecycle_bulk_operations_status_check
alter table public.timesheet_lifecycle_bulk_operations add constraint timesheet_lifecycle_bulk_operations_status_check CHECK (status = ANY (ARRAY['QUEUED'::text, 'RUNNING'::text, 'COMPLETED'::text, 'FAILED'::text, 'CANCELLED'::text]));

-- public.timesheet_lifecycle_bulk_operations.timesheet_lifecycle_bulk_operations_success_count_check
alter table public.timesheet_lifecycle_bulk_operations add constraint timesheet_lifecycle_bulk_operations_success_count_check CHECK (success_count >= 0);

-- public.timesheet_pay_state.timesheet_pay_state_summary_pay_icon_code_chk
alter table public.timesheet_pay_state add constraint timesheet_pay_state_summary_pay_icon_code_chk CHECK (summary_pay_icon_code IS NULL OR (summary_pay_icon_code = ANY (ARRAY['COIN'::text, 'HALF_COIN'::text, 'CLOCK'::text, 'RED_COIN'::text, 'NONE'::text])));

-- public.timesheet_pay_state.timesheet_pay_state_summary_pay_status_code_chk
alter table public.timesheet_pay_state add constraint timesheet_pay_state_summary_pay_status_code_chk CHECK (summary_pay_status_code IS NULL OR (summary_pay_status_code = ANY (ARRAY['PAID'::text, 'PARTIALLY_PAID'::text, 'PROCESSING'::text, 'ADVANCED'::text, 'UNPAID'::text])));

-- public.timesheet_payment_overrides.timesheet_payment_overrides_override_type_chk
alter table public.timesheet_payment_overrides add constraint timesheet_payment_overrides_override_type_chk CHECK (override_type = 'ADVANCE_THIS_PAYMENT'::text);

-- public.timesheet_r2_cleanup_queue.timesheet_r2_cleanup_queue_attempt_ck
alter table public.timesheet_r2_cleanup_queue add constraint timesheet_r2_cleanup_queue_attempt_ck CHECK (attempt_count >= 1);

-- public.timesheet_r2_cleanup_queue.timesheet_r2_cleanup_queue_key_nonblank
alter table public.timesheet_r2_cleanup_queue add constraint timesheet_r2_cleanup_queue_key_nonblank CHECK (btrim(r2_key) <> ''::text);

-- public.timesheet_r2_cleanup_queue.timesheet_r2_cleanup_queue_status_ck
alter table public.timesheet_r2_cleanup_queue add constraint timesheet_r2_cleanup_queue_status_ck CHECK (status = ANY (ARRAY['PENDING'::text, 'IN_PROGRESS'::text, 'COMPLETE'::text]));

-- public.timesheet_summary_pay_state_cache.timesheet_summary_pay_state_cache_badges_chk
alter table public.timesheet_summary_pay_state_cache add constraint timesheet_summary_pay_state_cache_badges_chk CHECK (summary_badge_codes <@ ARRAY['__PAY_BADGE_ADV__'::text, '__PAY_BADGE_OVERPAID__'::text, '__PAY_BADGE_PROCESSING__'::text]);

-- public.timesheet_summary_pay_state_cache.timesheet_summary_pay_state_cache_icon_chk
alter table public.timesheet_summary_pay_state_cache add constraint timesheet_summary_pay_state_cache_icon_chk CHECK (summary_pay_icon_code = ANY (ARRAY['COIN'::text, 'HALF_COIN'::text, 'CLOCK'::text, 'RED_COIN'::text, 'NONE'::text]));

-- public.timesheet_summary_pay_state_cache.timesheet_summary_pay_state_cache_status_chk
alter table public.timesheet_summary_pay_state_cache add constraint timesheet_summary_pay_state_cache_status_chk CHECK (summary_pay_status_code = ANY (ARRAY['PAID'::text, 'PARTIALLY_PAID'::text, 'PROCESSING'::text, 'OVERPAID'::text, 'UNPAID'::text]));

-- public.timesheet_validations.chk_tsv_hr_request_fields
alter table public.timesheet_validations add constraint chk_tsv_hr_request_fields CHECK (hr_request_id IS NULL AND hr_request_set_at_utc IS NULL AND hr_request_source IS NULL OR hr_request_id IS NOT NULL AND hr_request_set_at_utc IS NOT NULL AND hr_request_source IS NOT NULL) NOT VALID;

-- public.timesheets_financials.chk_tsfin_lock_pair
alter table public.timesheets_financials add constraint chk_tsfin_lock_pair CHECK (locked_by_invoice_id IS NULL AND locked_at_utc IS NULL OR locked_by_invoice_id IS NOT NULL AND locked_at_utc IS NOT NULL);

-- public.timesheets_financials.chk_tsfin_mileage_units_nonneg
alter table public.timesheets_financials add constraint chk_tsfin_mileage_units_nonneg CHECK (mileage_units >= 0::numeric);

-- public.timesheets_financials.timesheets_financials_pay_method_check
alter table public.timesheets_financials add constraint timesheets_financials_pay_method_check CHECK (pay_method = ANY (ARRAY['PAYE'::text, 'UMBRELLA'::text]));

-- public.timesheets.chk_timesheets_minutes_positive
alter table public.timesheets add constraint chk_timesheets_minutes_positive CHECK ((break_minutes IS NULL OR break_minutes >= 0) AND (worked_minutes IS NULL OR worked_minutes >= 0));

-- public.timesheets.chk_ts_signatures_for_electronic
alter table public.timesheets add constraint chk_ts_signatures_for_electronic CHECK (submission_mode IS NULL OR submission_mode <> 'ELECTRONIC'::submission_mode_enum OR r2_nurse_key IS NOT NULL AND r2_auth_key IS NOT NULL);

-- public.timesheets.timesheets_archive_metadata_coherent_chk
alter table public.timesheets add constraint timesheets_archive_metadata_coherent_chk CHECK (archived_at_utc IS NULL AND archived_by_user_id IS NULL AND archived_reason_code IS NULL OR archived_at_utc IS NOT NULL AND archived_by_user_id IS NOT NULL AND NULLIF(btrim(archived_reason_code), ''::text) IS NOT NULL);

-- public.timesheets.timesheets_archived_not_authorised_chk
alter table public.timesheets add constraint timesheets_archived_not_authorised_chk CHECK (archived_at_utc IS NULL OR authorised_at_server IS NULL);

-- public.timesheets.timesheets_candidate_hint_text_is_object
alter table public.timesheets add constraint timesheets_candidate_hint_text_is_object CHECK (candidate_hint_text IS NULL OR jsonb_typeof(candidate_hint_text) = 'object'::text);

-- public.timesheets.timesheets_candidate_submission_route_intent_ck
alter table public.timesheets add constraint timesheets_candidate_submission_route_intent_ck CHECK (candidate_submission_route_intent IS NULL OR candidate_submission_route_intent = 'ELECTRONIC'::text AND submission_mode = 'MANUAL'::submission_mode_enum AND sheet_scope = 'DAILY'::timesheet_scope_enum OR candidate_submission_route_intent = 'PAPER'::text AND submission_mode = 'MANUAL'::submission_mode_enum AND sheet_scope = 'WEEKLY'::timesheet_scope_enum);

-- public.timesheets.timesheets_candidate_workflow_generation_ck
alter table public.timesheets add constraint timesheets_candidate_workflow_generation_ck CHECK (candidate_workflow_generation IS NULL OR candidate_workflow_generation > 0);

-- public.timesheets.timesheets_candidate_workflow_pair_ck
alter table public.timesheets add constraint timesheets_candidate_workflow_pair_ck CHECK (candidate_workflow_id IS NULL AND candidate_workflow_generation IS NULL OR candidate_workflow_id IS NOT NULL AND candidate_workflow_generation IS NOT NULL);

-- public.timesheets.timesheets_document_state_ck
alter table public.timesheets add constraint timesheets_document_state_ck CHECK (document_state = ANY (ARRAY['NOT_REQUESTED'::text, 'QUEUED'::text, 'STALE'::text, 'PREPARING'::text, 'READY'::text, 'FAILED'::text]));

-- public.timesheets.timesheets_manual_pdf_rotation_chk
alter table public.timesheets add constraint timesheets_manual_pdf_rotation_chk CHECK (manual_pdf_rotation_degrees = ANY (ARRAY[0, 90, 180, 270]));

-- public.timesheets.timesheets_parent_timesheet_id_adjustment_chk
alter table public.timesheets add constraint timesheets_parent_timesheet_id_adjustment_chk CHECK (parent_timesheet_id IS NULL OR is_adjustment = true);

-- public.tms_login_2fa_challenges.tms_login_2fa_challenges_purpose_chk
alter table public.tms_login_2fa_challenges add constraint tms_login_2fa_challenges_purpose_chk CHECK (purpose = ANY (ARRAY['PAYMENT_SCHEDULE'::text, 'PAYMENT_REVERSAL'::text, 'PAYE_SAME_WEEK_OVERRIDE'::text]));

-- public.tms_users.tms_users_payment_golden_implies_authoriser_chk
alter table public.tms_users add constraint tms_users_payment_golden_implies_authoriser_chk CHECK (payment_golden_key = false OR payment_authoriser = true);

-- private.banking_pay_workbench_candidate_scope_registry.bpay_wb_scope_registry_candidate_fk
alter table private.banking_pay_workbench_candidate_scope_registry add constraint bpay_wb_scope_registry_candidate_fk FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE CASCADE;

-- private.banking_pay_workbench_candidate_scope_registry.bpay_wb_scope_registry_current_build_fk
alter table private.banking_pay_workbench_candidate_scope_registry add constraint bpay_wb_scope_registry_current_build_fk FOREIGN KEY (current_build_id) REFERENCES private.banking_pay_workbench_economic_builds(id) ON DELETE SET NULL DEFERRABLE;

-- private.banking_pay_workbench_canonical_stage_lines.bpay_wb_canonical_stage_lines_build_fk
alter table private.banking_pay_workbench_canonical_stage_lines add constraint bpay_wb_canonical_stage_lines_build_fk FOREIGN KEY (build_id) REFERENCES private.banking_pay_workbench_economic_builds(id) ON DELETE CASCADE;

-- private.banking_pay_workbench_economic_build_fact_pages.bpay_wb_economic_build_fact_pages_attempt_fk
alter table private.banking_pay_workbench_economic_build_fact_pages add constraint bpay_wb_economic_build_fact_pages_attempt_fk FOREIGN KEY (attempt_id) REFERENCES private.banking_pay_workbench_stage_attempts(id) DEFERRABLE INITIALLY DEFERRED;

-- private.banking_pay_workbench_economic_build_fact_pages.bpay_wb_economic_build_fact_pages_build_fk
alter table private.banking_pay_workbench_economic_build_fact_pages add constraint bpay_wb_economic_build_fact_pages_build_fk FOREIGN KEY (build_id) REFERENCES private.banking_pay_workbench_economic_builds(id) ON DELETE CASCADE;

-- private.banking_pay_workbench_economic_build_facts.bpay_wb_economic_build_facts_build_fk
alter table private.banking_pay_workbench_economic_build_facts add constraint bpay_wb_economic_build_facts_build_fk FOREIGN KEY (build_id) REFERENCES private.banking_pay_workbench_economic_builds(id) ON DELETE CASCADE;

-- private.banking_pay_workbench_economic_build_scope.bpay_wb_economic_build_scope_build_fk
alter table private.banking_pay_workbench_economic_build_scope add constraint bpay_wb_economic_build_scope_build_fk FOREIGN KEY (build_id) REFERENCES private.banking_pay_workbench_economic_builds(id) ON DELETE CASCADE;

-- private.banking_pay_workbench_economic_builds.bpay_wb_economic_builds_candidate_fk
alter table private.banking_pay_workbench_economic_builds add constraint bpay_wb_economic_builds_candidate_fk FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE CASCADE;

-- private.banking_pay_workbench_economic_builds.bpay_wb_economic_builds_session_fk
alter table private.banking_pay_workbench_economic_builds add constraint bpay_wb_economic_builds_session_fk FOREIGN KEY (session_id) REFERENCES banking_pay_workbench_sessions(id) ON DELETE RESTRICT;

-- private.banking_pay_workbench_economic_builds.bpay_wb_economic_builds_source_job_fk
alter table private.banking_pay_workbench_economic_builds add constraint bpay_wb_economic_builds_source_job_fk FOREIGN KEY (source_job_id) REFERENCES banking_pay_workbench_jobs(id) ON DELETE SET NULL DEFERRABLE;

-- private.banking_pay_workbench_stage_attempts.bpay_wb_stage_attempts_build_fk
alter table private.banking_pay_workbench_stage_attempts add constraint bpay_wb_stage_attempts_build_fk FOREIGN KEY (build_id) REFERENCES private.banking_pay_workbench_economic_builds(id) ON DELETE CASCADE;

-- private.banking_pay_workbench_stage_attempts.bpay_wb_stage_attempts_job_fk
alter table private.banking_pay_workbench_stage_attempts add constraint bpay_wb_stage_attempts_job_fk FOREIGN KEY (job_id) REFERENCES banking_pay_workbench_jobs(id) ON DELETE RESTRICT;

-- private.banking_pay_workbench_timesheet_scope_state.bpay_wb_timesheet_scope_state_candidate_fk
alter table private.banking_pay_workbench_timesheet_scope_state add constraint bpay_wb_timesheet_scope_state_candidate_fk FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE CASCADE;

-- private.banking_pay_workbench_timesheet_scope_state.bpay_wb_timesheet_scope_state_current_build_fk
alter table private.banking_pay_workbench_timesheet_scope_state add constraint bpay_wb_timesheet_scope_state_current_build_fk FOREIGN KEY (current_build_id) REFERENCES private.banking_pay_workbench_economic_builds(id) ON DELETE SET NULL DEFERRABLE;

-- private.banking_pay_workbench_timesheet_scope_state.bpay_wb_timesheet_scope_state_timesheet_fk
alter table private.banking_pay_workbench_timesheet_scope_state add constraint bpay_wb_timesheet_scope_state_timesheet_fk FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id) ON DELETE CASCADE;

-- private.candidate_daily_authority_scopes.candidate_daily_authority_scopes_active_generation_fk
alter table private.candidate_daily_authority_scopes add constraint candidate_daily_authority_scopes_active_generation_fk FOREIGN KEY (active_generation_id, environment, candidate_id) REFERENCES candidate_daily_rota_generations(generation_id, environment, candidate_id) DEFERRABLE INITIALLY DEFERRED;

-- private.candidate_daily_authority_scopes.candidate_daily_authority_scopes_candidate_id_fkey
alter table private.candidate_daily_authority_scopes add constraint candidate_daily_authority_scopes_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE RESTRICT;

-- private.candidate_daily_authority_scopes.candidate_daily_authority_scopes_last_transition_fk
alter table private.candidate_daily_authority_scopes add constraint candidate_daily_authority_scopes_last_transition_fk FOREIGN KEY (last_transition_id) REFERENCES private.candidate_daily_authority_transitions(transition_id) DEFERRABLE INITIALLY DEFERRED;

-- private.candidate_daily_authority_transitions.candidate_daily_authority_transit_supersedes_transition_id_fkey
alter table private.candidate_daily_authority_transitions add constraint candidate_daily_authority_transit_supersedes_transition_id_fkey FOREIGN KEY (supersedes_transition_id) REFERENCES private.candidate_daily_authority_transitions(transition_id) ON DELETE RESTRICT;

-- private.candidate_daily_authority_transitions.candidate_daily_authority_transitions_batch_receipt_id_fkey
alter table private.candidate_daily_authority_transitions add constraint candidate_daily_authority_transitions_batch_receipt_id_fkey FOREIGN KEY (batch_receipt_id) REFERENCES private.candidate_daily_batch_receipts(batch_receipt_id) ON DELETE RESTRICT;

-- private.candidate_daily_authority_transitions.candidate_daily_authority_transitions_candidate_id_fkey
alter table private.candidate_daily_authority_transitions add constraint candidate_daily_authority_transitions_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE RESTRICT;

-- private.candidate_daily_entitlements.candidate_daily_entitlements_candidate_id_fkey
alter table private.candidate_daily_entitlements add constraint candidate_daily_entitlements_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE RESTRICT;

-- private.candidate_daily_external_effect_receipts.candidate_daily_external_effect_receipts_candidate_id_fkey
alter table private.candidate_daily_external_effect_receipts add constraint candidate_daily_external_effect_receipts_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE RESTRICT;

-- private.candidate_daily_source_links.candidate_daily_source_links_candidate_id_fkey
alter table private.candidate_daily_source_links add constraint candidate_daily_source_links_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE RESTRICT;

-- private.candidate_daily_sync_state.candidate_daily_sync_state_scope_fk
alter table private.candidate_daily_sync_state add constraint candidate_daily_sync_state_scope_fk FOREIGN KEY (environment, candidate_id) REFERENCES private.candidate_daily_authority_scopes(environment, candidate_id) ON DELETE RESTRICT;

-- public.app_change_counters.app_change_counters_scope_change_tx_token_fkey
alter table public.app_change_counters add constraint app_change_counters_scope_change_tx_token_fkey FOREIGN KEY (scope_change_tx_token) REFERENCES banking_pay_scope_change_transactions(tx_token) DEFERRABLE INITIALLY DEFERRED;

-- public.assignment_band_mappings.assignment_band_mappings_candidate_id_fkey
alter table public.assignment_band_mappings add constraint assignment_band_mappings_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE CASCADE;

-- public.assignment_band_mappings.assignment_band_mappings_client_id_fkey
alter table public.assignment_band_mappings add constraint assignment_band_mappings_client_id_fkey FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE;

-- public.assignment_band_mappings.assignment_band_mappings_target_contract_id_fkey
alter table public.assignment_band_mappings add constraint assignment_band_mappings_target_contract_id_fkey FOREIGN KEY (target_contract_id) REFERENCES contracts(id) ON DELETE SET NULL;

-- public.audit_events.audit_events_actor_user_id_fkey
alter table public.audit_events add constraint audit_events_actor_user_id_fkey FOREIGN KEY (actor_user_id) REFERENCES tms_users(id) ON DELETE SET NULL;

-- public.bank_provider_webhook_receipts.bank_provider_webhook_receipts_config_fkey
alter table public.bank_provider_webhook_receipts add constraint bank_provider_webhook_receipts_config_fkey FOREIGN KEY (webhook_config_id) REFERENCES bank_provider_webhook_configs(id);

-- public.bank_provider_webhook_receipts.bank_provider_webhook_receipts_webhook_config_id_fkey
alter table public.bank_provider_webhook_receipts add constraint bank_provider_webhook_receipts_webhook_config_id_fkey FOREIGN KEY (webhook_config_id) REFERENCES bank_provider_webhook_configs(id);

-- public.banking_alert_acknowledgements.banking_alert_acknowledgements_ack_user_fkey
alter table public.banking_alert_acknowledgements add constraint banking_alert_acknowledgements_ack_user_fkey FOREIGN KEY (acknowledged_by_user_id) REFERENCES tms_users(id) ON DELETE CASCADE;

-- public.banking_alert_success_events.banking_alert_success_events_pay_batch_id_fkey
alter table public.banking_alert_success_events add constraint banking_alert_success_events_pay_batch_id_fkey FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id) ON DELETE CASCADE;

-- public.banking_alert_user_preferences.banking_alert_user_preferences_user_fkey
alter table public.banking_alert_user_preferences add constraint banking_alert_user_preferences_user_fkey FOREIGN KEY (user_id) REFERENCES tms_users(id) ON DELETE CASCADE;

-- public.banking_pay_batch_change_signals.banking_pay_batch_change_signals_batch_fkey
alter table public.banking_pay_batch_change_signals add constraint banking_pay_batch_change_signals_batch_fkey FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id) ON DELETE CASCADE;

-- public.banking_pay_batch_change_signals.banking_pay_batch_change_signals_pay_batch_id_fkey
alter table public.banking_pay_batch_change_signals add constraint banking_pay_batch_change_signals_pay_batch_id_fkey FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id) ON DELETE CASCADE;

-- public.banking_pay_operation_candidate_allocation_rows.banking_pay_operation_candidate_alloc_finance_component_id_fkey
alter table public.banking_pay_operation_candidate_allocation_rows add constraint banking_pay_operation_candidate_alloc_finance_component_id_fkey FOREIGN KEY (finance_component_id) REFERENCES pay_finance_case_components(id) ON DELETE SET NULL;

-- public.banking_pay_operation_candidate_allocation_rows.banking_pay_operation_candidate_allocat_candidate_scope_id_fkey
alter table public.banking_pay_operation_candidate_allocation_rows add constraint banking_pay_operation_candidate_allocat_candidate_scope_id_fkey FOREIGN KEY (candidate_scope_id) REFERENCES banking_pay_operation_candidate_scope(id) ON DELETE CASCADE;

-- public.banking_pay_operation_candidate_allocation_rows.banking_pay_operation_candidate_allocati_pay_batch_item_id_fkey
alter table public.banking_pay_operation_candidate_allocation_rows add constraint banking_pay_operation_candidate_allocati_pay_batch_item_id_fkey FOREIGN KEY (pay_batch_item_id) REFERENCES pay_batch_items(id) ON DELETE SET NULL;

-- public.banking_pay_operation_candidate_allocation_rows.banking_pay_operation_candidate_allocation_ro_candidate_id_fkey
alter table public.banking_pay_operation_candidate_allocation_rows add constraint banking_pay_operation_candidate_allocation_ro_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE RESTRICT;

-- public.banking_pay_operation_candidate_allocation_rows.banking_pay_operation_candidate_allocation_ro_operation_id_fkey
alter table public.banking_pay_operation_candidate_allocation_rows add constraint banking_pay_operation_candidate_allocation_ro_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES banking_pay_operations(id) ON DELETE CASCADE;

-- public.banking_pay_operation_candidate_allocation_rows.banking_pay_operation_candidate_allocation_ro_pay_batch_id_fkey
alter table public.banking_pay_operation_candidate_allocation_rows add constraint banking_pay_operation_candidate_allocation_ro_pay_batch_id_fkey FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id) ON DELETE SET NULL;

-- public.banking_pay_operation_candidate_scope.banking_pay_operation_candidate_sco_source_snapshot_run_id_fkey
alter table public.banking_pay_operation_candidate_scope add constraint banking_pay_operation_candidate_sco_source_snapshot_run_id_fkey FOREIGN KEY (source_snapshot_run_id) REFERENCES banking_pay_snapshot_runs(id) ON DELETE SET NULL;

-- public.banking_pay_operation_candidate_scope.banking_pay_operation_candidate_scope_candidate_id_fkey
alter table public.banking_pay_operation_candidate_scope add constraint banking_pay_operation_candidate_scope_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE RESTRICT;

-- public.banking_pay_operation_candidate_scope.banking_pay_operation_candidate_scope_candidate_state_id_fkey
alter table public.banking_pay_operation_candidate_scope add constraint banking_pay_operation_candidate_scope_candidate_state_id_fkey FOREIGN KEY (candidate_state_id) REFERENCES banking_pay_workbench_session_candidate_state(id) ON DELETE SET NULL;

-- public.banking_pay_operation_candidate_scope.banking_pay_operation_candidate_scope_operation_id_fkey
alter table public.banking_pay_operation_candidate_scope add constraint banking_pay_operation_candidate_scope_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES banking_pay_operations(id) ON DELETE CASCADE;

-- public.banking_pay_operation_candidate_scope.banking_pay_operation_candidate_scope_pay_batch_id_fkey
alter table public.banking_pay_operation_candidate_scope add constraint banking_pay_operation_candidate_scope_pay_batch_id_fkey FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id) ON DELETE SET NULL;

-- public.banking_pay_operation_candidate_scope.banking_pay_operation_candidate_scope_workbench_session_id_fkey
alter table public.banking_pay_operation_candidate_scope add constraint banking_pay_operation_candidate_scope_workbench_session_id_fkey FOREIGN KEY (workbench_session_id) REFERENCES banking_pay_workbench_sessions(id) ON DELETE RESTRICT;

-- public.banking_pay_operation_chunks.banking_pay_operation_chunks_operation_id_fkey
alter table public.banking_pay_operation_chunks add constraint banking_pay_operation_chunks_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES banking_pay_operations(id) ON DELETE CASCADE;

-- public.banking_pay_operation_provider_attempts.bpay_provider_attempts_batch_fk
alter table public.banking_pay_operation_provider_attempts add constraint bpay_provider_attempts_batch_fk FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id) ON DELETE CASCADE;

-- public.banking_pay_operation_provider_attempts.bpay_provider_attempts_chunk_fk
alter table public.banking_pay_operation_provider_attempts add constraint bpay_provider_attempts_chunk_fk FOREIGN KEY (provider_chunk_id) REFERENCES banking_pay_operation_chunks(id) ON DELETE SET NULL;

-- public.banking_pay_operation_provider_attempts.bpay_provider_attempts_operation_fk
alter table public.banking_pay_operation_provider_attempts add constraint bpay_provider_attempts_operation_fk FOREIGN KEY (operation_id) REFERENCES banking_pay_operations(id) ON DELETE CASCADE;

-- public.banking_pay_operation_provider_attempts.bpay_provider_attempts_transfer_scope_fk
alter table public.banking_pay_operation_provider_attempts add constraint bpay_provider_attempts_transfer_scope_fk FOREIGN KEY (transfer_scope_id) REFERENCES banking_pay_operation_transfer_scope(id) ON DELETE SET NULL;

-- public.banking_pay_operation_remittance_scope.banking_pay_operation_remittance_sc_pay_batch_candidate_id_fkey
alter table public.banking_pay_operation_remittance_scope add constraint banking_pay_operation_remittance_sc_pay_batch_candidate_id_fkey FOREIGN KEY (pay_batch_candidate_id) REFERENCES pay_batch_candidates(id) ON DELETE SET NULL;

-- public.banking_pay_operation_remittance_scope.banking_pay_operation_remittance_scope_candidate_id_fkey
alter table public.banking_pay_operation_remittance_scope add constraint banking_pay_operation_remittance_scope_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE SET NULL;

-- public.banking_pay_operation_remittance_scope.banking_pay_operation_remittance_scope_operation_id_fkey
alter table public.banking_pay_operation_remittance_scope add constraint banking_pay_operation_remittance_scope_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES banking_pay_operations(id) ON DELETE CASCADE;

-- public.banking_pay_operation_remittance_scope.banking_pay_operation_remittance_scope_pay_batch_id_fkey
alter table public.banking_pay_operation_remittance_scope add constraint banking_pay_operation_remittance_scope_pay_batch_id_fkey FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id) ON DELETE CASCADE;

-- public.banking_pay_operation_scope_units.bpay_op_scope_units_batch_fk
alter table public.banking_pay_operation_scope_units add constraint bpay_op_scope_units_batch_fk FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id) ON DELETE CASCADE;

-- public.banking_pay_operation_scope_units.bpay_op_scope_units_chunk_fk
alter table public.banking_pay_operation_scope_units add constraint bpay_op_scope_units_chunk_fk FOREIGN KEY (chunk_id) REFERENCES banking_pay_operation_chunks(id) ON DELETE SET NULL;

-- public.banking_pay_operation_scope_units.bpay_op_scope_units_operation_fk
alter table public.banking_pay_operation_scope_units add constraint bpay_op_scope_units_operation_fk FOREIGN KEY (operation_id) REFERENCES banking_pay_operations(id) ON DELETE CASCADE;

-- public.banking_pay_operation_settlement_scope.banking_pay_operation_settlement_sc_pay_batch_candidate_id_fkey
alter table public.banking_pay_operation_settlement_scope add constraint banking_pay_operation_settlement_sc_pay_batch_candidate_id_fkey FOREIGN KEY (pay_batch_candidate_id) REFERENCES pay_batch_candidates(id) ON DELETE SET NULL;

-- public.banking_pay_operation_settlement_scope.banking_pay_operation_settlement_scope_candidate_id_fkey
alter table public.banking_pay_operation_settlement_scope add constraint banking_pay_operation_settlement_scope_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE SET NULL;

-- public.banking_pay_operation_settlement_scope.banking_pay_operation_settlement_scope_operation_id_fkey
alter table public.banking_pay_operation_settlement_scope add constraint banking_pay_operation_settlement_scope_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES banking_pay_operations(id) ON DELETE CASCADE;

-- public.banking_pay_operation_settlement_scope.banking_pay_operation_settlement_scope_pay_batch_id_fkey
alter table public.banking_pay_operation_settlement_scope add constraint banking_pay_operation_settlement_scope_pay_batch_id_fkey FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id) ON DELETE CASCADE;

-- public.banking_pay_operation_transfer_scope_items.bpay_op_transfer_items_batch_candidate_fk
alter table public.banking_pay_operation_transfer_scope_items add constraint bpay_op_transfer_items_batch_candidate_fk FOREIGN KEY (pay_batch_candidate_id) REFERENCES pay_batch_candidates(id) ON DELETE SET NULL;

-- public.banking_pay_operation_transfer_scope_items.bpay_op_transfer_items_batch_fk
alter table public.banking_pay_operation_transfer_scope_items add constraint bpay_op_transfer_items_batch_fk FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id) ON DELETE CASCADE;

-- public.banking_pay_operation_transfer_scope_items.bpay_op_transfer_items_candidate_fk
alter table public.banking_pay_operation_transfer_scope_items add constraint bpay_op_transfer_items_candidate_fk FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE SET NULL;

-- public.banking_pay_operation_transfer_scope_items.bpay_op_transfer_items_item_fk
alter table public.banking_pay_operation_transfer_scope_items add constraint bpay_op_transfer_items_item_fk FOREIGN KEY (pay_batch_item_id) REFERENCES pay_batch_items(id) ON DELETE CASCADE;

-- public.banking_pay_operation_transfer_scope_items.bpay_op_transfer_items_operation_fk
alter table public.banking_pay_operation_transfer_scope_items add constraint bpay_op_transfer_items_operation_fk FOREIGN KEY (operation_id) REFERENCES banking_pay_operations(id) ON DELETE CASCADE;

-- public.banking_pay_operation_transfer_scope_items.bpay_op_transfer_items_scope_fk
alter table public.banking_pay_operation_transfer_scope_items add constraint bpay_op_transfer_items_scope_fk FOREIGN KEY (transfer_scope_id) REFERENCES banking_pay_operation_transfer_scope(id) ON DELETE CASCADE;

-- public.banking_pay_operation_transfer_scope.banking_pay_operation_transfer_scope_candidate_id_fkey
alter table public.banking_pay_operation_transfer_scope add constraint banking_pay_operation_transfer_scope_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE SET NULL;

-- public.banking_pay_operation_transfer_scope.banking_pay_operation_transfer_scope_operation_id_fkey
alter table public.banking_pay_operation_transfer_scope add constraint banking_pay_operation_transfer_scope_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES banking_pay_operations(id) ON DELETE CASCADE;

-- public.banking_pay_operation_transfer_scope.banking_pay_operation_transfer_scope_pay_bank_transfer_id_fkey
alter table public.banking_pay_operation_transfer_scope add constraint banking_pay_operation_transfer_scope_pay_bank_transfer_id_fkey FOREIGN KEY (pay_bank_transfer_id) REFERENCES pay_bank_transfers(id) ON DELETE SET NULL;

-- public.banking_pay_operation_transfer_scope.banking_pay_operation_transfer_scope_pay_batch_id_fkey
alter table public.banking_pay_operation_transfer_scope add constraint banking_pay_operation_transfer_scope_pay_batch_id_fkey FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id) ON DELETE CASCADE;

-- public.banking_pay_operation_transfer_scope.banking_pay_operation_transfer_scope_umbrella_id_fkey
alter table public.banking_pay_operation_transfer_scope add constraint banking_pay_operation_transfer_scope_umbrella_id_fkey FOREIGN KEY (umbrella_id) REFERENCES umbrellas(id) ON DELETE SET NULL;

-- public.banking_pay_operation_transfer_scope.bpay_transfer_scope_provider_chunk_fk
alter table public.banking_pay_operation_transfer_scope add constraint bpay_transfer_scope_provider_chunk_fk FOREIGN KEY (provider_submit_chunk_id) REFERENCES banking_pay_operation_chunks(id) ON DELETE SET NULL;

-- public.banking_pay_operations.banking_pay_operations_pay_batch_id_fkey
alter table public.banking_pay_operations add constraint banking_pay_operations_pay_batch_id_fkey FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id) ON DELETE SET NULL;

-- public.banking_pay_operations.banking_pay_operations_root_operation_id_fkey
alter table public.banking_pay_operations add constraint banking_pay_operations_root_operation_id_fkey FOREIGN KEY (root_operation_id) REFERENCES banking_pay_operations(id) ON DELETE SET NULL;

-- public.banking_pay_operations.banking_pay_operations_workbench_session_id_fkey
alter table public.banking_pay_operations add constraint banking_pay_operations_workbench_session_id_fkey FOREIGN KEY (workbench_session_id) REFERENCES banking_pay_workbench_sessions(id) ON DELETE SET NULL;

-- public.banking_pay_snapshot_candidate_state.banking_pay_snapshot_candidate_state_candidate_id_fkey
alter table public.banking_pay_snapshot_candidate_state add constraint banking_pay_snapshot_candidate_state_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id);

-- public.banking_pay_snapshot_candidate_state.banking_pay_snapshot_candidate_state_snapshot_run_id_fkey
alter table public.banking_pay_snapshot_candidate_state add constraint banking_pay_snapshot_candidate_state_snapshot_run_id_fkey FOREIGN KEY (snapshot_run_id) REFERENCES banking_pay_snapshot_runs(id);

-- public.banking_pay_snapshot_case_component_state.banking_pay_snapshot_case_component_state_candidate_id_fkey
alter table public.banking_pay_snapshot_case_component_state add constraint banking_pay_snapshot_case_component_state_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id);

-- public.banking_pay_snapshot_case_component_state.banking_pay_snapshot_case_component_state_snapshot_run_id_fkey
alter table public.banking_pay_snapshot_case_component_state add constraint banking_pay_snapshot_case_component_state_snapshot_run_id_fkey FOREIGN KEY (snapshot_run_id) REFERENCES banking_pay_snapshot_runs(id);

-- public.banking_pay_snapshot_case_state.banking_pay_snapshot_case_state_candidate_id_fkey
alter table public.banking_pay_snapshot_case_state add constraint banking_pay_snapshot_case_state_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id);

-- public.banking_pay_snapshot_case_state.banking_pay_snapshot_case_state_snapshot_run_id_fkey
alter table public.banking_pay_snapshot_case_state add constraint banking_pay_snapshot_case_state_snapshot_run_id_fkey FOREIGN KEY (snapshot_run_id) REFERENCES banking_pay_snapshot_runs(id);

-- public.banking_pay_snapshot_line_state.banking_pay_snapshot_line_state_candidate_id_fkey
alter table public.banking_pay_snapshot_line_state add constraint banking_pay_snapshot_line_state_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id);

-- public.banking_pay_snapshot_line_state.banking_pay_snapshot_line_state_snapshot_run_id_fkey
alter table public.banking_pay_snapshot_line_state add constraint banking_pay_snapshot_line_state_snapshot_run_id_fkey FOREIGN KEY (snapshot_run_id) REFERENCES banking_pay_snapshot_runs(id);

-- public.banking_pay_workbench_candidate_line_work.bpay_wb_line_work_candidate_fk
alter table public.banking_pay_workbench_candidate_line_work add constraint bpay_wb_line_work_candidate_fk FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE CASCADE;

-- public.banking_pay_workbench_candidate_line_work.bpay_wb_line_work_session_fk
alter table public.banking_pay_workbench_candidate_line_work add constraint bpay_wb_line_work_session_fk FOREIGN KEY (session_id) REFERENCES banking_pay_workbench_sessions(id) ON DELETE CASCADE;

-- public.banking_pay_workbench_candidate_line_work.bpay_wb_line_work_timesheet_fk
alter table public.banking_pay_workbench_candidate_line_work add constraint bpay_wb_line_work_timesheet_fk FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id) ON DELETE SET NULL;

-- public.banking_pay_workbench_candidate_source_lines.bpay_wb_source_lines_candidate_fk
alter table public.banking_pay_workbench_candidate_source_lines add constraint bpay_wb_source_lines_candidate_fk FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE CASCADE;

-- public.banking_pay_workbench_candidate_source_lines.bpay_wb_source_lines_session_fk
alter table public.banking_pay_workbench_candidate_source_lines add constraint bpay_wb_source_lines_session_fk FOREIGN KEY (session_id) REFERENCES banking_pay_workbench_sessions(id) ON DELETE CASCADE;

-- public.banking_pay_workbench_candidate_source_lines.bpay_wb_source_lines_timesheet_fk
alter table public.banking_pay_workbench_candidate_source_lines add constraint bpay_wb_source_lines_timesheet_fk FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id) ON DELETE SET NULL;

-- public.banking_pay_workbench_case_resolution_carry_registrations.banking_pay_case_resolution_carry_candidate_fkey
alter table public.banking_pay_workbench_case_resolution_carry_registrations add constraint banking_pay_case_resolution_carry_candidate_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE RESTRICT;

-- public.banking_pay_workbench_case_resolution_carry_registrations.banking_pay_case_resolution_carry_source_resolution_fkey
alter table public.banking_pay_workbench_case_resolution_carry_registrations add constraint banking_pay_case_resolution_carry_source_resolution_fkey FOREIGN KEY (source_resolution_id) REFERENCES banking_pay_workbench_session_case_resolutions(id) ON DELETE RESTRICT;

-- public.banking_pay_workbench_case_resolution_carry_registrations.banking_pay_case_resolution_carry_source_session_fkey
alter table public.banking_pay_workbench_case_resolution_carry_registrations add constraint banking_pay_case_resolution_carry_source_session_fkey FOREIGN KEY (source_session_id) REFERENCES banking_pay_workbench_sessions(id) ON DELETE RESTRICT;

-- public.banking_pay_workbench_case_resolution_carry_registrations.banking_pay_case_resolution_carry_target_resolution_fkey
alter table public.banking_pay_workbench_case_resolution_carry_registrations add constraint banking_pay_case_resolution_carry_target_resolution_fkey FOREIGN KEY (target_resolution_id) REFERENCES banking_pay_workbench_session_case_resolutions(id) ON DELETE SET NULL;

-- public.banking_pay_workbench_case_resolution_carry_registrations.banking_pay_case_resolution_carry_target_session_fkey
alter table public.banking_pay_workbench_case_resolution_carry_registrations add constraint banking_pay_case_resolution_carry_target_session_fkey FOREIGN KEY (target_session_id) REFERENCES banking_pay_workbench_sessions(id) ON DELETE RESTRICT;

-- public.banking_pay_workbench_jobs.banking_pay_workbench_jobs_candidate_id_fkey
alter table public.banking_pay_workbench_jobs add constraint banking_pay_workbench_jobs_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id);

-- public.banking_pay_workbench_jobs.banking_pay_workbench_jobs_scope_change_tx_token_fkey
alter table public.banking_pay_workbench_jobs add constraint banking_pay_workbench_jobs_scope_change_tx_token_fkey FOREIGN KEY (scope_change_tx_token) REFERENCES banking_pay_scope_change_transactions(tx_token) DEFERRABLE INITIALLY DEFERRED;

-- public.banking_pay_workbench_jobs.banking_pay_workbench_jobs_session_id_fkey
alter table public.banking_pay_workbench_jobs add constraint banking_pay_workbench_jobs_session_id_fkey FOREIGN KEY (session_id) REFERENCES banking_pay_workbench_sessions(id);

-- public.banking_pay_workbench_jobs.banking_pay_workbench_jobs_snapshot_run_id_fkey
alter table public.banking_pay_workbench_jobs add constraint banking_pay_workbench_jobs_snapshot_run_id_fkey FOREIGN KEY (snapshot_run_id) REFERENCES banking_pay_snapshot_runs(id);

-- public.banking_pay_workbench_jobs.bpay_wb_jobs_economic_build_fk
alter table public.banking_pay_workbench_jobs add constraint bpay_wb_jobs_economic_build_fk FOREIGN KEY (economic_build_id) REFERENCES private.banking_pay_workbench_economic_builds(id) ON DELETE SET NULL;

-- public.banking_pay_workbench_preview_rows.bpay_wb_preview_rows_candidate_fk
alter table public.banking_pay_workbench_preview_rows add constraint bpay_wb_preview_rows_candidate_fk FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE CASCADE;

-- public.banking_pay_workbench_preview_rows.bpay_wb_preview_rows_session_fk
alter table public.banking_pay_workbench_preview_rows add constraint bpay_wb_preview_rows_session_fk FOREIGN KEY (session_id) REFERENCES banking_pay_workbench_sessions(id) ON DELETE CASCADE;

-- public.banking_pay_workbench_preview_rows.bpay_wb_preview_rows_timesheet_fk
alter table public.banking_pay_workbench_preview_rows add constraint bpay_wb_preview_rows_timesheet_fk FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id) ON DELETE SET NULL;

-- public.banking_pay_workbench_selection_carry_registrations.banking_pay_selection_carry_candidate_fkey
alter table public.banking_pay_workbench_selection_carry_registrations add constraint banking_pay_selection_carry_candidate_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE RESTRICT;

-- public.banking_pay_workbench_selection_carry_registrations.banking_pay_selection_carry_source_preview_fkey
alter table public.banking_pay_workbench_selection_carry_registrations add constraint banking_pay_selection_carry_source_preview_fkey FOREIGN KEY (source_preview_row_id) REFERENCES banking_pay_workbench_preview_rows(id) ON DELETE SET NULL;

-- public.banking_pay_workbench_selection_carry_registrations.banking_pay_selection_carry_source_session_fkey
alter table public.banking_pay_workbench_selection_carry_registrations add constraint banking_pay_selection_carry_source_session_fkey FOREIGN KEY (source_session_id) REFERENCES banking_pay_workbench_sessions(id) ON DELETE RESTRICT;

-- public.banking_pay_workbench_selection_carry_registrations.banking_pay_selection_carry_target_preview_fkey
alter table public.banking_pay_workbench_selection_carry_registrations add constraint banking_pay_selection_carry_target_preview_fkey FOREIGN KEY (target_preview_row_id) REFERENCES banking_pay_workbench_preview_rows(id) ON DELETE SET NULL;

-- public.banking_pay_workbench_selection_carry_registrations.banking_pay_selection_carry_target_session_fkey
alter table public.banking_pay_workbench_selection_carry_registrations add constraint banking_pay_selection_carry_target_session_fkey FOREIGN KEY (target_session_id) REFERENCES banking_pay_workbench_sessions(id) ON DELETE RESTRICT;

-- public.banking_pay_workbench_session_candidate_state.banking_pay_workbench_session_candidate_state_candidate_id_fkey
alter table public.banking_pay_workbench_session_candidate_state add constraint banking_pay_workbench_session_candidate_state_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id);

-- public.banking_pay_workbench_session_candidate_state.banking_pay_workbench_session_candidate_state_session_id_fkey
alter table public.banking_pay_workbench_session_candidate_state add constraint banking_pay_workbench_session_candidate_state_session_id_fkey FOREIGN KEY (session_id) REFERENCES banking_pay_workbench_sessions(id);

-- public.banking_pay_workbench_session_case_resolutions.banking_pay_workbench_session_case_resolutions_candidate_id_fke
alter table public.banking_pay_workbench_session_case_resolutions add constraint banking_pay_workbench_session_case_resolutions_candidate_id_fke FOREIGN KEY (candidate_id) REFERENCES candidates(id);

-- public.banking_pay_workbench_session_case_resolutions.banking_pay_workbench_session_case_resolutions_session_id_fkey
alter table public.banking_pay_workbench_session_case_resolutions add constraint banking_pay_workbench_session_case_resolutions_session_id_fkey FOREIGN KEY (session_id) REFERENCES banking_pay_workbench_sessions(id);

-- public.banking_pay_workbench_session_case_resolutions.bpay_wb_case_resolution_origin_session_fk
alter table public.banking_pay_workbench_session_case_resolutions add constraint bpay_wb_case_resolution_origin_session_fk FOREIGN KEY (resolution_origin_session_id) REFERENCES banking_pay_workbench_sessions(id) ON DELETE RESTRICT;

-- public.banking_pay_workbench_session_overrides.banking_pay_workbench_session_overrides_candidate_id_fkey
alter table public.banking_pay_workbench_session_overrides add constraint banking_pay_workbench_session_overrides_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id);

-- public.banking_pay_workbench_session_overrides.banking_pay_workbench_session_overrides_session_id_fkey
alter table public.banking_pay_workbench_session_overrides add constraint banking_pay_workbench_session_overrides_session_id_fkey FOREIGN KEY (session_id) REFERENCES banking_pay_workbench_sessions(id);

-- public.banking_pay_workbench_session_scope.bpay_wb_scope_candidate_fk
alter table public.banking_pay_workbench_session_scope add constraint bpay_wb_scope_candidate_fk FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE CASCADE;

-- public.banking_pay_workbench_session_scope.bpay_wb_scope_pending_job_fk
alter table public.banking_pay_workbench_session_scope add constraint bpay_wb_scope_pending_job_fk FOREIGN KEY (pending_job_id) REFERENCES banking_pay_workbench_jobs(id) ON DELETE SET NULL;

-- public.banking_pay_workbench_session_scope.bpay_wb_scope_session_fk
alter table public.banking_pay_workbench_session_scope add constraint bpay_wb_scope_session_fk FOREIGN KEY (session_id) REFERENCES banking_pay_workbench_sessions(id) ON DELETE CASCADE;

-- public.banking_pay_workbench_sessions.banking_pay_workbench_sessions_source_snapshot_run_id_fkey
alter table public.banking_pay_workbench_sessions add constraint banking_pay_workbench_sessions_source_snapshot_run_id_fkey FOREIGN KEY (source_snapshot_run_id) REFERENCES banking_pay_snapshot_runs(id);

-- public.banking_pay_workbench_sessions.bpay_workbench_sessions_replacement_session_id_fkey
alter table public.banking_pay_workbench_sessions add constraint bpay_workbench_sessions_replacement_session_id_fkey FOREIGN KEY (replacement_session_id) REFERENCES banking_pay_workbench_sessions(id) ON DELETE SET NULL;

-- public.candidate_app_global_membership_links.candidate_app_global_membership_links_account_id_fkey
alter table public.candidate_app_global_membership_links add constraint candidate_app_global_membership_links_account_id_fkey FOREIGN KEY (account_id) REFERENCES candidate_app_accounts(id) ON DELETE RESTRICT;

-- public.candidate_app_global_membership_links.candidate_app_global_membership_links_candidate_id_fkey
alter table public.candidate_app_global_membership_links add constraint candidate_app_global_membership_links_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE RESTRICT;

-- public.candidate_app_sessions.candidate_app_sessions_account_id_fkey
alter table public.candidate_app_sessions add constraint candidate_app_sessions_account_id_fkey FOREIGN KEY (account_id) REFERENCES candidate_app_accounts(id) ON DELETE CASCADE;

-- public.candidate_app_sessions.candidate_app_sessions_replaced_by_fk
alter table public.candidate_app_sessions add constraint candidate_app_sessions_replaced_by_fk FOREIGN KEY (replaced_by_session_id) REFERENCES candidate_app_sessions(id) ON DELETE SET NULL;

-- public.candidate_app_sessions.candidate_app_sessions_selected_candidate_id_fkey
alter table public.candidate_app_sessions add constraint candidate_app_sessions_selected_candidate_id_fkey FOREIGN KEY (selected_candidate_id) REFERENCES candidates(id) ON DELETE SET NULL;

-- public.candidate_approval_requests.candidate_approval_requests_review_timesheet_fk
alter table public.candidate_approval_requests add constraint candidate_approval_requests_review_timesheet_fk FOREIGN KEY (manager_review_timesheet_component_id) REFERENCES candidate_submission_components(id) ON DELETE RESTRICT;

-- public.candidate_approval_requests.candidate_approval_requests_signature_component_id_fkey
alter table public.candidate_approval_requests add constraint candidate_approval_requests_signature_component_id_fkey FOREIGN KEY (signature_component_id) REFERENCES candidate_submission_components(id) ON DELETE RESTRICT;

-- public.candidate_approval_requests.candidate_approval_requests_workflow_id_fkey
alter table public.candidate_approval_requests add constraint candidate_approval_requests_workflow_id_fkey FOREIGN KEY (workflow_id) REFERENCES candidate_submission_workflows(id) ON DELETE RESTRICT;

-- public.candidate_auth_challenges.candidate_auth_challenges_account_id_fkey
alter table public.candidate_auth_challenges add constraint candidate_auth_challenges_account_id_fkey FOREIGN KEY (account_id) REFERENCES candidate_app_accounts(id) ON DELETE CASCADE;

-- public.candidate_auth_challenges.candidate_auth_challenges_mail_outbox_id_fkey
alter table public.candidate_auth_challenges add constraint candidate_auth_challenges_mail_outbox_id_fkey FOREIGN KEY (mail_outbox_id) REFERENCES mail_outbox(id) ON DELETE SET NULL;

-- public.candidate_auth_challenges.candidate_auth_challenges_superseded_by_id_fkey
alter table public.candidate_auth_challenges add constraint candidate_auth_challenges_superseded_by_id_fkey FOREIGN KEY (superseded_by_id) REFERENCES candidate_auth_challenges(id) ON DELETE SET NULL;

-- public.candidate_daily_availability_days.candidate_daily_availability_days_scope_fk
alter table public.candidate_daily_availability_days add constraint candidate_daily_availability_days_scope_fk FOREIGN KEY (environment, candidate_id) REFERENCES private.candidate_daily_authority_scopes(environment, candidate_id) ON DELETE RESTRICT;

-- public.candidate_daily_availability_days.candidate_daily_availability_days_source_command_id_fkey
alter table public.candidate_daily_availability_days add constraint candidate_daily_availability_days_source_command_id_fkey FOREIGN KEY (source_command_id) REFERENCES candidate_daily_command_receipts(command_id) ON DELETE RESTRICT;

-- public.candidate_daily_command_receipts.candidate_daily_command_receipts_candidate_id_fkey
alter table public.candidate_daily_command_receipts add constraint candidate_daily_command_receipts_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE RESTRICT;

-- public.candidate_daily_rota_days.candidate_daily_rota_days_generation_fk
alter table public.candidate_daily_rota_days add constraint candidate_daily_rota_days_generation_fk FOREIGN KEY (generation_id, environment, candidate_id) REFERENCES candidate_daily_rota_generations(generation_id, environment, candidate_id) ON DELETE CASCADE;

-- public.candidate_daily_rota_generations.candidate_daily_rota_generations_batch_receipt_id_fkey
alter table public.candidate_daily_rota_generations add constraint candidate_daily_rota_generations_batch_receipt_id_fkey FOREIGN KEY (batch_receipt_id) REFERENCES private.candidate_daily_batch_receipts(batch_receipt_id) ON DELETE RESTRICT;

-- public.candidate_daily_rota_generations.candidate_daily_rota_generations_scope_fk
alter table public.candidate_daily_rota_generations add constraint candidate_daily_rota_generations_scope_fk FOREIGN KEY (environment, candidate_id) REFERENCES private.candidate_daily_authority_scopes(environment, candidate_id) ON DELETE RESTRICT;

-- public.candidate_daily_sheet_projection_outbox.candidate_daily_sheet_projection_outbox_command_id_fkey
alter table public.candidate_daily_sheet_projection_outbox add constraint candidate_daily_sheet_projection_outbox_command_id_fkey FOREIGN KEY (command_id) REFERENCES candidate_daily_command_receipts(command_id) ON DELETE RESTRICT;

-- public.candidate_daily_sheet_projection_outbox.candidate_daily_sheet_projection_outbox_scope_fk
alter table public.candidate_daily_sheet_projection_outbox add constraint candidate_daily_sheet_projection_outbox_scope_fk FOREIGN KEY (environment, candidate_id) REFERENCES private.candidate_daily_authority_scopes(environment, candidate_id) ON DELETE RESTRICT;

-- public.candidate_job_titles.candidate_job_titles_candidate_id_fkey
alter table public.candidate_job_titles add constraint candidate_job_titles_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE CASCADE;

-- public.candidate_job_titles.candidate_job_titles_job_title_id_fkey
alter table public.candidate_job_titles add constraint candidate_job_titles_job_title_id_fkey FOREIGN KEY (job_title_id) REFERENCES default_job_titles(id) ON DELETE CASCADE;

-- public.candidate_notifications.candidate_notifications_account_id_fkey
alter table public.candidate_notifications add constraint candidate_notifications_account_id_fkey FOREIGN KEY (account_id) REFERENCES candidate_app_accounts(id) ON DELETE CASCADE;

-- public.candidate_notifications.candidate_notifications_candidate_id_fkey
alter table public.candidate_notifications add constraint candidate_notifications_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE RESTRICT;

-- public.candidate_notifications.candidate_notifications_timesheet_id_fkey
alter table public.candidate_notifications add constraint candidate_notifications_timesheet_id_fkey FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id) ON DELETE RESTRICT;

-- public.candidate_notifications.candidate_notifications_workflow_id_fkey
alter table public.candidate_notifications add constraint candidate_notifications_workflow_id_fkey FOREIGN KEY (workflow_id) REFERENCES candidate_submission_workflows(id) ON DELETE RESTRICT;

-- public.candidate_submission_components.candidate_submission_components_approval_request_fk
alter table public.candidate_submission_components add constraint candidate_submission_components_approval_request_fk FOREIGN KEY (approval_request_id) REFERENCES candidate_approval_requests(id) ON DELETE RESTRICT;

-- public.candidate_submission_components.candidate_submission_components_source_component_id_fkey
alter table public.candidate_submission_components add constraint candidate_submission_components_source_component_id_fkey FOREIGN KEY (source_component_id) REFERENCES candidate_submission_components(id) ON DELETE RESTRICT;

-- public.candidate_submission_components.candidate_submission_components_timesheet_id_fkey
alter table public.candidate_submission_components add constraint candidate_submission_components_timesheet_id_fkey FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id) ON DELETE RESTRICT;

-- public.candidate_submission_components.candidate_submission_components_workflow_id_fkey
alter table public.candidate_submission_components add constraint candidate_submission_components_workflow_id_fkey FOREIGN KEY (workflow_id) REFERENCES candidate_submission_workflows(id) ON DELETE RESTRICT;

-- public.candidate_submission_workflows.candidate_submission_workflow_canonical_save_financials_id_fkey
alter table public.candidate_submission_workflows add constraint candidate_submission_workflow_canonical_save_financials_id_fkey FOREIGN KEY (canonical_save_financials_id) REFERENCES timesheets_financials(id) ON DELETE RESTRICT;

-- public.candidate_submission_workflows.candidate_submission_workflows_account_id_fkey
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_account_id_fkey FOREIGN KEY (account_id) REFERENCES candidate_app_accounts(id) ON DELETE RESTRICT;

-- public.candidate_submission_workflows.candidate_submission_workflows_anchor_timesheet_id_fkey
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_anchor_timesheet_id_fkey FOREIGN KEY (anchor_timesheet_id) REFERENCES timesheets(timesheet_id) ON DELETE RESTRICT;

-- public.candidate_submission_workflows.candidate_submission_workflows_candidate_id_fkey
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE RESTRICT;

-- public.candidate_submission_workflows.candidate_submission_workflows_candidate_signature_fk
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_candidate_signature_fk FOREIGN KEY (candidate_signature_component_id) REFERENCES candidate_submission_components(id) ON DELETE RESTRICT;

-- public.candidate_submission_workflows.candidate_submission_workflows_contract_id_fkey
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_contract_id_fkey FOREIGN KEY (contract_id) REFERENCES contracts(id) ON DELETE RESTRICT;

-- public.candidate_submission_workflows.candidate_submission_workflows_contract_week_id_fkey
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_contract_week_id_fkey FOREIGN KEY (contract_week_id) REFERENCES contract_weeks(id) ON DELETE RESTRICT;

-- public.candidate_submission_workflows.candidate_submission_workflows_manager_signature_fk
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_manager_signature_fk FOREIGN KEY (manager_signature_component_id) REFERENCES candidate_submission_components(id) ON DELETE RESTRICT;

-- public.candidate_submission_workflows.candidate_submission_workflows_replacement_of_fk
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_replacement_of_fk FOREIGN KEY (replacement_of_workflow_id) REFERENCES candidate_submission_workflows(id) ON DELETE RESTRICT;

-- public.candidate_submission_workflows.candidate_submission_workflows_target_timesheet_id_fkey
alter table public.candidate_submission_workflows add constraint candidate_submission_workflows_target_timesheet_id_fkey FOREIGN KEY (target_timesheet_id) REFERENCES timesheets(timesheet_id) ON DELETE RESTRICT;

-- public.candidates.candidates_job_title_id_fkey
alter table public.candidates add constraint candidates_job_title_id_fkey FOREIGN KEY (job_title_id) REFERENCES default_job_titles(id);

-- public.candidates.candidates_umbrella_id_fkey
alter table public.candidates add constraint candidates_umbrella_id_fkey FOREIGN KEY (umbrella_id) REFERENCES umbrellas(id) ON DELETE SET NULL;

-- public.client_hospitals.client_hospitals_client_id_fkey
alter table public.client_hospitals add constraint client_hospitals_client_id_fkey FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE;

-- public.client_settings.client_settings_client_id_fkey
alter table public.client_settings add constraint client_settings_client_id_fkey FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE;

-- public.comms_outbox.comms_outbox_created_by_fkey
alter table public.comms_outbox add constraint comms_outbox_created_by_fkey FOREIGN KEY (created_by) REFERENCES tms_users(id);

-- public.comms_outbox.comms_outbox_document_template_id_fkey
alter table public.comms_outbox add constraint comms_outbox_document_template_id_fkey FOREIGN KEY (document_template_id) REFERENCES document_templates(id);

-- public.comms_outbox.comms_outbox_mailshot_run_id_fkey
alter table public.comms_outbox add constraint comms_outbox_mailshot_run_id_fkey FOREIGN KEY (mailshot_run_id) REFERENCES mailshot_runs(id);

-- public.contract_weeks.contract_weeks_contract_id_fkey
alter table public.contract_weeks add constraint contract_weeks_contract_id_fkey FOREIGN KEY (contract_id) REFERENCES contracts(id) ON DELETE CASCADE;

-- public.contract_weeks.contract_weeks_timesheet_id_fkey
alter table public.contract_weeks add constraint contract_weeks_timesheet_id_fkey FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id) ON DELETE SET NULL;

-- public.contracts.contracts_candidate_id_fkey
alter table public.contracts add constraint contracts_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE RESTRICT;

-- public.contracts.contracts_client_id_fkey
alter table public.contracts add constraint contracts_client_id_fkey FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE RESTRICT;

-- public.default_job_titles.default_job_titles_parent_id_fkey
alter table public.default_job_titles add constraint default_job_titles_parent_id_fkey FOREIGN KEY (parent_id) REFERENCES default_job_titles(id) ON DELETE RESTRICT;

-- public.document_templates.document_templates_created_by_fkey
alter table public.document_templates add constraint document_templates_created_by_fkey FOREIGN KEY (created_by) REFERENCES tms_users(id);

-- public.hr_daily_grade_role_mappings.hr_daily_grade_role_mappings_client_id_fkey
alter table public.hr_daily_grade_role_mappings add constraint hr_daily_grade_role_mappings_client_id_fkey FOREIGN KEY (client_id) REFERENCES clients(id);

-- public.hr_imports.hr_imports_client_id_fkey
alter table public.hr_imports add constraint hr_imports_client_id_fkey FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE SET NULL;

-- public.hr_imports.hr_imports_supersedes_import_id_fkey
alter table public.hr_imports add constraint hr_imports_supersedes_import_id_fkey FOREIGN KEY (supersedes_import_id) REFERENCES hr_imports(id) ON DELETE RESTRICT;

-- public.hr_imports.hr_imports_uploaded_by_fkey
alter table public.hr_imports add constraint hr_imports_uploaded_by_fkey FOREIGN KEY (uploaded_by) REFERENCES tms_users(id) ON DELETE SET NULL;

-- public.hr_issue_email_deliveries.hr_issue_email_deliveries_import_id_fkey
alter table public.hr_issue_email_deliveries add constraint hr_issue_email_deliveries_import_id_fkey FOREIGN KEY (import_id) REFERENCES import_review_states(import_id) ON DELETE RESTRICT;

-- public.hr_issue_email_deliveries.hr_issue_email_deliveries_mail_outbox_id_fkey
alter table public.hr_issue_email_deliveries add constraint hr_issue_email_deliveries_mail_outbox_id_fkey FOREIGN KEY (mail_outbox_id) REFERENCES mail_outbox(id) ON DELETE RESTRICT;

-- public.hr_issue_email_deliveries.hr_issue_email_deliveries_operation_id_fkey
alter table public.hr_issue_email_deliveries add constraint hr_issue_email_deliveries_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES import_apply_operations(id) ON DELETE RESTRICT;

-- public.hr_issue_email_delivery_items.hr_issue_email_delivery_items_action_id_fkey
alter table public.hr_issue_email_delivery_items add constraint hr_issue_email_delivery_items_action_id_fkey FOREIGN KEY (action_id) REFERENCES import_review_decisions(action_id) ON DELETE RESTRICT;

-- public.hr_issue_email_delivery_items.hr_issue_email_delivery_items_delivery_id_fkey
alter table public.hr_issue_email_delivery_items add constraint hr_issue_email_delivery_items_delivery_id_fkey FOREIGN KEY (delivery_id) REFERENCES hr_issue_email_deliveries(id) ON DELETE CASCADE;

-- public.hr_issue_email_delivery_items.hr_issue_email_delivery_items_issue_id_fkey
alter table public.hr_issue_email_delivery_items add constraint hr_issue_email_delivery_items_issue_id_fkey FOREIGN KEY (issue_id) REFERENCES hr_issue_emails(id) ON DELETE RESTRICT;

-- public.hr_issue_emails.hr_issue_emails_contract_id_fkey
alter table public.hr_issue_emails add constraint hr_issue_emails_contract_id_fkey FOREIGN KEY (contract_id) REFERENCES contracts(id) ON DELETE RESTRICT;

-- public.hr_issue_emails.hr_issue_emails_last_successful_delivery_id_fkey
alter table public.hr_issue_emails add constraint hr_issue_emails_last_successful_delivery_id_fkey FOREIGN KEY (last_successful_delivery_id) REFERENCES hr_issue_email_deliveries(id) ON DELETE RESTRICT;

-- public.hr_name_mappings.hr_name_mappings_candidate_id_fkey
alter table public.hr_name_mappings add constraint hr_name_mappings_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE CASCADE;

-- public.hr_name_mappings.hr_name_mappings_created_by_fkey
alter table public.hr_name_mappings add constraint hr_name_mappings_created_by_fkey FOREIGN KEY (created_by) REFERENCES tms_users(id) ON DELETE SET NULL;

-- public.hr_results.hr_results_candidate_id_fkey
alter table public.hr_results add constraint hr_results_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE SET NULL;

-- public.hr_results.hr_results_row_id_fkey
alter table public.hr_results add constraint hr_results_row_id_fkey FOREIGN KEY (row_id) REFERENCES hr_rows(id) ON DELETE CASCADE;

-- public.hr_rows.hr_rows_import_id_fkey
alter table public.hr_rows add constraint hr_rows_import_id_fkey FOREIGN KEY (import_id) REFERENCES hr_imports(id) ON DELETE CASCADE;

-- public.id_consolidation_run_lines.id_consolidation_run_lines_id_ref_fkey
alter table public.id_consolidation_run_lines add constraint id_consolidation_run_lines_id_ref_fkey FOREIGN KEY (id_ref) REFERENCES id_consolidation_runs(id_ref) ON DELETE CASCADE;

-- public.id_consolidation_runs.id_consolidation_runs_created_by_user_id_fkey
alter table public.id_consolidation_runs add constraint id_consolidation_runs_created_by_user_id_fkey FOREIGN KEY (created_by_user_id) REFERENCES tms_users(id) ON DELETE SET NULL;

-- public.import_apply_operations.import_apply_operations_actor_fk
alter table public.import_apply_operations add constraint import_apply_operations_actor_fk FOREIGN KEY (actor_user_id) REFERENCES tms_users(id) ON DELETE RESTRICT;

-- public.import_apply_operations.import_apply_operations_import_fk
alter table public.import_apply_operations add constraint import_apply_operations_import_fk FOREIGN KEY (import_id) REFERENCES hr_imports(id) ON DELETE RESTRICT;

-- public.import_review_action_outcomes.import_review_action_outcomes_applied_by_user_id_fkey
alter table public.import_review_action_outcomes add constraint import_review_action_outcomes_applied_by_user_id_fkey FOREIGN KEY (applied_by_user_id) REFERENCES tms_users(id) ON DELETE RESTRICT;

-- public.import_review_action_outcomes.import_review_action_outcomes_candidate_id_fkey
alter table public.import_review_action_outcomes add constraint import_review_action_outcomes_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE RESTRICT;

-- public.import_review_action_outcomes.import_review_action_outcomes_client_id_fkey
alter table public.import_review_action_outcomes add constraint import_review_action_outcomes_client_id_fkey FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE RESTRICT;

-- public.import_review_action_outcomes.import_review_action_outcomes_contract_id_fkey
alter table public.import_review_action_outcomes add constraint import_review_action_outcomes_contract_id_fkey FOREIGN KEY (contract_id) REFERENCES contracts(id) ON DELETE RESTRICT;

-- public.import_review_action_outcomes.import_review_action_outcomes_hr_row_id_fkey
alter table public.import_review_action_outcomes add constraint import_review_action_outcomes_hr_row_id_fkey FOREIGN KEY (hr_row_id) REFERENCES hr_rows(id) ON DELETE RESTRICT;

-- public.import_review_action_outcomes.import_review_action_outcomes_import_id_fkey
alter table public.import_review_action_outcomes add constraint import_review_action_outcomes_import_id_fkey FOREIGN KEY (import_id) REFERENCES import_review_states(import_id) ON DELETE RESTRICT;

-- public.import_review_action_outcomes.import_review_action_outcomes_operation_id_fkey
alter table public.import_review_action_outcomes add constraint import_review_action_outcomes_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES import_apply_operations(id) ON DELETE RESTRICT;

-- public.import_review_action_outcomes.import_review_action_outcomes_shift_id_fkey
alter table public.import_review_action_outcomes add constraint import_review_action_outcomes_shift_id_fkey FOREIGN KEY (shift_id) REFERENCES nhsp_shifts(id) ON DELETE RESTRICT;

-- public.import_review_action_outcomes.import_review_action_outcomes_timesheet_id_fkey
alter table public.import_review_action_outcomes add constraint import_review_action_outcomes_timesheet_id_fkey FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id) ON DELETE RESTRICT;

-- public.import_review_daily_timesheet_resolutions.import_review_daily_resolution_applied_operation_id_fkey
alter table public.import_review_daily_timesheet_resolutions add constraint import_review_daily_resolution_applied_operation_id_fkey FOREIGN KEY (applied_operation_id) REFERENCES import_apply_operations(id) ON DELETE RESTRICT;

-- public.import_review_daily_timesheet_resolutions.import_review_daily_timesheet_resolu_resolved_timesheet_id_fkey
alter table public.import_review_daily_timesheet_resolutions add constraint import_review_daily_timesheet_resolu_resolved_timesheet_id_fkey FOREIGN KEY (resolved_timesheet_id) REFERENCES timesheets(timesheet_id);

-- public.import_review_daily_timesheet_resolutions.import_review_daily_timesheet_resolutions_hr_row_id_fkey
alter table public.import_review_daily_timesheet_resolutions add constraint import_review_daily_timesheet_resolutions_hr_row_id_fkey FOREIGN KEY (hr_row_id) REFERENCES hr_rows(id) ON DELETE CASCADE;

-- public.import_review_daily_timesheet_resolutions.import_review_daily_timesheet_resolutions_import_id_fkey
alter table public.import_review_daily_timesheet_resolutions add constraint import_review_daily_timesheet_resolutions_import_id_fkey FOREIGN KEY (import_id) REFERENCES import_review_states(import_id) ON DELETE CASCADE;

-- public.import_review_decisions.import_review_decisions_candidate_id_fkey
alter table public.import_review_decisions add constraint import_review_decisions_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id);

-- public.import_review_decisions.import_review_decisions_client_id_fkey
alter table public.import_review_decisions add constraint import_review_decisions_client_id_fkey FOREIGN KEY (client_id) REFERENCES clients(id);

-- public.import_review_decisions.import_review_decisions_contract_id_fkey
alter table public.import_review_decisions add constraint import_review_decisions_contract_id_fkey FOREIGN KEY (contract_id) REFERENCES contracts(id);

-- public.import_review_decisions.import_review_decisions_hr_row_id_fkey
alter table public.import_review_decisions add constraint import_review_decisions_hr_row_id_fkey FOREIGN KEY (hr_row_id) REFERENCES hr_rows(id);

-- public.import_review_decisions.import_review_decisions_import_id_fkey
alter table public.import_review_decisions add constraint import_review_decisions_import_id_fkey FOREIGN KEY (import_id) REFERENCES import_review_states(import_id) ON DELETE CASCADE;

-- public.import_review_decisions.import_review_decisions_issue_id_fkey
alter table public.import_review_decisions add constraint import_review_decisions_issue_id_fkey FOREIGN KEY (issue_id) REFERENCES hr_issue_emails(id) ON DELETE RESTRICT;

-- public.import_review_decisions.import_review_decisions_shift_id_fkey
alter table public.import_review_decisions add constraint import_review_decisions_shift_id_fkey FOREIGN KEY (shift_id) REFERENCES nhsp_shifts(id);

-- public.import_review_decisions.import_review_decisions_timesheet_id_fkey
alter table public.import_review_decisions add constraint import_review_decisions_timesheet_id_fkey FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id);

-- public.import_review_events.import_review_events_import_id_fkey
alter table public.import_review_events add constraint import_review_events_import_id_fkey FOREIGN KEY (import_id) REFERENCES import_review_states(import_id) ON DELETE CASCADE;

-- public.import_review_scope_candidates.import_review_scope_candidates_candidate_id_fkey
alter table public.import_review_scope_candidates add constraint import_review_scope_candidates_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id);

-- public.import_review_scope_candidates.import_review_scope_candidates_import_id_fkey
alter table public.import_review_scope_candidates add constraint import_review_scope_candidates_import_id_fkey FOREIGN KEY (import_id) REFERENCES hr_imports(id) ON DELETE CASCADE;

-- public.import_review_scope_clients.import_review_scope_clients_client_id_fkey
alter table public.import_review_scope_clients add constraint import_review_scope_clients_client_id_fkey FOREIGN KEY (client_id) REFERENCES clients(id);

-- public.import_review_scope_clients.import_review_scope_clients_import_id_fkey
alter table public.import_review_scope_clients add constraint import_review_scope_clients_import_id_fkey FOREIGN KEY (import_id) REFERENCES hr_imports(id) ON DELETE CASCADE;

-- public.import_review_states.import_review_states_import_id_fkey
alter table public.import_review_states add constraint import_review_states_import_id_fkey FOREIGN KEY (import_id) REFERENCES hr_imports(id) ON DELETE CASCADE;

-- public.import_review_states.import_review_states_last_operation_id_fkey
alter table public.import_review_states add constraint import_review_states_last_operation_id_fkey FOREIGN KEY (last_operation_id) REFERENCES import_apply_operations(id) ON DELETE RESTRICT;

-- public.import_review_weekly_validation_resolutions.import_review_weekly_validation_resol_applied_operation_id_fkey
alter table public.import_review_weekly_validation_resolutions add constraint import_review_weekly_validation_resol_applied_operation_id_fkey FOREIGN KEY (applied_operation_id) REFERENCES import_apply_operations(id) ON DELETE RESTRICT;

-- public.import_review_weekly_validation_resolutions.import_review_weekly_validation_resolutions_hr_row_id_fkey
alter table public.import_review_weekly_validation_resolutions add constraint import_review_weekly_validation_resolutions_hr_row_id_fkey FOREIGN KEY (hr_row_id) REFERENCES hr_rows(id) ON DELETE CASCADE;

-- public.import_review_weekly_validation_resolutions.import_review_weekly_validation_resolutions_import_id_fkey
alter table public.import_review_weekly_validation_resolutions add constraint import_review_weekly_validation_resolutions_import_id_fkey FOREIGN KEY (import_id) REFERENCES import_review_states(import_id) ON DELETE CASCADE;

-- public.import_review_weekly_validation_resolutions.import_review_weekly_validation_resolutions_timesheet_id_fkey
alter table public.import_review_weekly_validation_resolutions add constraint import_review_weekly_validation_resolutions_timesheet_id_fkey FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id) ON DELETE RESTRICT;

-- public.invoice_document_assets.invoice_document_assets_operation_id_fkey
alter table public.invoice_document_assets add constraint invoice_document_assets_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES invoice_operations(id) ON DELETE SET NULL;

-- public.invoice_document_versions.invoice_document_versions_operation_id_fkey
alter table public.invoice_document_versions add constraint invoice_document_versions_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES invoice_operations(id) ON DELETE RESTRICT;

-- public.invoice_hr_source_rows.invoice_hr_source_rows_import_id_fkey
alter table public.invoice_hr_source_rows add constraint invoice_hr_source_rows_import_id_fkey FOREIGN KEY (import_id) REFERENCES hr_imports(id);

-- public.invoice_hr_source_rows.invoice_hr_source_rows_invoice_id_fkey
alter table public.invoice_hr_source_rows add constraint invoice_hr_source_rows_invoice_id_fkey FOREIGN KEY (invoice_id) REFERENCES invoices(id);

-- public.invoice_lines.invoice_lines_invoice_id_fkey
alter table public.invoice_lines add constraint invoice_lines_invoice_id_fkey FOREIGN KEY (invoice_id) REFERENCES invoices(id) ON DELETE CASCADE;

-- public.invoice_operation_chunks.invoice_operation_chunks_document_asset_id_fkey
alter table public.invoice_operation_chunks add constraint invoice_operation_chunks_document_asset_id_fkey FOREIGN KEY (document_asset_id) REFERENCES invoice_document_assets(id) ON DELETE RESTRICT;

-- public.invoice_operation_chunks.invoice_operation_chunks_document_version_id_fkey
alter table public.invoice_operation_chunks add constraint invoice_operation_chunks_document_version_id_fkey FOREIGN KEY (document_version_id) REFERENCES invoice_document_versions(id) ON DELETE RESTRICT;

-- public.invoice_operation_chunks.invoice_operation_chunks_input_document_version_id_fkey
alter table public.invoice_operation_chunks add constraint invoice_operation_chunks_input_document_version_id_fkey FOREIGN KEY (input_document_version_id) REFERENCES invoice_document_versions(id) ON DELETE RESTRICT;

-- public.invoice_operation_chunks.invoice_operation_chunks_operation_id_fkey
alter table public.invoice_operation_chunks add constraint invoice_operation_chunks_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES invoice_operations(id) ON DELETE CASCADE;

-- public.invoice_operation_chunks.invoice_operation_chunks_replaced_by_fk
alter table public.invoice_operation_chunks add constraint invoice_operation_chunks_replaced_by_fk FOREIGN KEY (replaced_by_chunk_id) REFERENCES invoice_operation_chunks(id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

-- public.invoice_operations.invoice_operations_actor_user_id_fkey
alter table public.invoice_operations add constraint invoice_operations_actor_user_id_fkey FOREIGN KEY (actor_user_id) REFERENCES tms_users(id) ON DELETE SET NULL;

-- public.invoice_operations.invoice_operations_parent_operation_id_fkey
alter table public.invoice_operations add constraint invoice_operations_parent_operation_id_fkey FOREIGN KEY (parent_operation_id) REFERENCES invoice_operations(id) ON DELETE SET NULL;

-- public.invoice_pdf_outbox.invoice_pdf_outbox_invoice_id_fkey
alter table public.invoice_pdf_outbox add constraint invoice_pdf_outbox_invoice_id_fkey FOREIGN KEY (invoice_id) REFERENCES invoices(id) ON DELETE CASCADE;

-- public.invoices.invoices_active_document_operation_fk
alter table public.invoices add constraint invoices_active_document_operation_fk FOREIGN KEY (active_document_operation_id) REFERENCES invoice_operations(id) ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED;

-- public.invoices.invoices_active_issue_operation_fk
alter table public.invoices add constraint invoices_active_issue_operation_fk FOREIGN KEY (active_issue_operation_id) REFERENCES invoice_operations(id) ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED;

-- public.invoices.invoices_client_id_fkey
alter table public.invoices add constraint invoices_client_id_fkey FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE RESTRICT;

-- public.invoices.invoices_issued_document_version_fk
alter table public.invoices add constraint invoices_issued_document_version_fk FOREIGN KEY (issued_document_version_id) REFERENCES invoice_document_versions(id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

-- public.invoices.invoices_original_invoice_id_fkey
alter table public.invoices add constraint invoices_original_invoice_id_fkey FOREIGN KEY (original_invoice_id) REFERENCES invoices(id) ON DELETE SET NULL;

-- public.invoices.invoices_preview_document_version_fk
alter table public.invoices add constraint invoices_preview_document_version_fk FOREIGN KEY (preview_document_version_id) REFERENCES invoice_document_versions(id) ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED;

-- public.legacy_contract_rate_lines.legacy_contract_rate_lines_legacy_contract_id_fkey
alter table public.legacy_contract_rate_lines add constraint legacy_contract_rate_lines_legacy_contract_id_fkey FOREIGN KEY (legacy_contract_id) REFERENCES legacy_contracts(id) ON DELETE CASCADE;

-- public.legacy_contracts.legacy_contracts_candidate_id_fkey
alter table public.legacy_contracts add constraint legacy_contracts_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE CASCADE;

-- public.legacy_contracts.legacy_contracts_client_id_fkey
alter table public.legacy_contracts add constraint legacy_contracts_client_id_fkey FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE;

-- public.legacy_eclipse_candidate_map.legacy_eclipse_candidate_map_candidate_id_fkey
alter table public.legacy_eclipse_candidate_map add constraint legacy_eclipse_candidate_map_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE CASCADE;

-- public.legacy_eclipse_client_map.legacy_eclipse_client_map_client_id_fkey
alter table public.legacy_eclipse_client_map add constraint legacy_eclipse_client_map_client_id_fkey FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE;

-- public.mail_outbox.mail_outbox_created_by_fkey
alter table public.mail_outbox add constraint mail_outbox_created_by_fkey FOREIGN KEY (created_by) REFERENCES tms_users(id) ON DELETE SET NULL;

-- public.mail_outbox.mail_outbox_document_template_id_fkey
alter table public.mail_outbox add constraint mail_outbox_document_template_id_fkey FOREIGN KEY (document_template_id) REFERENCES document_templates(id);

-- public.mail_outbox.mail_outbox_mailshot_run_id_fkey
alter table public.mail_outbox add constraint mail_outbox_mailshot_run_id_fkey FOREIGN KEY (mailshot_run_id) REFERENCES mailshot_runs(id);

-- public.mail_outbox.mail_outbox_waiting_invoice_operation_fk
alter table public.mail_outbox add constraint mail_outbox_waiting_invoice_operation_fk FOREIGN KEY (waiting_invoice_operation_id) REFERENCES invoice_operations(id) ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED;

-- public.mailshot_field_overrides.mailshot_field_overrides_field_id_fkey
alter table public.mailshot_field_overrides add constraint mailshot_field_overrides_field_id_fkey FOREIGN KEY (field_id) REFERENCES mailshot_fields(id) ON DELETE CASCADE;

-- public.mailshot_runs.mailshot_runs_created_by_fkey
alter table public.mailshot_runs add constraint mailshot_runs_created_by_fkey FOREIGN KEY (created_by) REFERENCES tms_users(id);

-- public.mailshot_runs.mailshot_runs_document_template_id_fkey
alter table public.mailshot_runs add constraint mailshot_runs_document_template_id_fkey FOREIGN KEY (document_template_id) REFERENCES document_templates(id);

-- public.manual_timesheet_queue.manual_timesheet_queue_timesheet_id_fkey
alter table public.manual_timesheet_queue add constraint manual_timesheet_queue_timesheet_id_fkey FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id) ON DELETE SET NULL;

-- public.manual_timesheet_queue.manual_timesheet_queue_uploaded_by_user_id_fkey
alter table public.manual_timesheet_queue add constraint manual_timesheet_queue_uploaded_by_user_id_fkey FOREIGN KEY (uploaded_by_user_id) REFERENCES tms_users(id) ON DELETE RESTRICT;

-- public.nhsp_shifts.nhsp_shifts_cancelled_by_import_id_fkey
alter table public.nhsp_shifts add constraint nhsp_shifts_cancelled_by_import_id_fkey FOREIGN KEY (cancelled_by_import_id) REFERENCES hr_imports(id) ON DELETE SET NULL;

-- public.nhsp_shifts.nhsp_shifts_candidate_id_fkey
alter table public.nhsp_shifts add constraint nhsp_shifts_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE SET NULL;

-- public.nhsp_shifts.nhsp_shifts_client_id_fkey
alter table public.nhsp_shifts add constraint nhsp_shifts_client_id_fkey FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE SET NULL;

-- public.nhsp_shifts.nhsp_shifts_contract_id_fkey
alter table public.nhsp_shifts add constraint nhsp_shifts_contract_id_fkey FOREIGN KEY (contract_id) REFERENCES contracts(id) ON DELETE SET NULL;

-- public.nhsp_shifts.nhsp_shifts_invoice_id_fkey
alter table public.nhsp_shifts add constraint nhsp_shifts_invoice_id_fkey FOREIGN KEY (invoice_id) REFERENCES invoices(id) ON DELETE SET NULL;

-- public.nhsp_shifts.nhsp_shifts_latest_import_id_fkey
alter table public.nhsp_shifts add constraint nhsp_shifts_latest_import_id_fkey FOREIGN KEY (latest_import_id) REFERENCES hr_imports(id) ON DELETE SET NULL;

-- public.nhsp_shifts.nhsp_shifts_timesheet_id_fkey
alter table public.nhsp_shifts add constraint nhsp_shifts_timesheet_id_fkey FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id) ON DELETE SET NULL;

-- public.pay_advance_patches.pay_advance_patches_advance_fkey
alter table public.pay_advance_patches add constraint pay_advance_patches_advance_fkey FOREIGN KEY (advance_id) REFERENCES pay_advances(id) ON DELETE RESTRICT;

-- public.pay_advance_patches.pay_advance_patches_batch_fkey
alter table public.pay_advance_patches add constraint pay_advance_patches_batch_fkey FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id) ON DELETE CASCADE;

-- public.pay_advance_reservations.pay_advance_reservations_created_by_user_id_fkey
alter table public.pay_advance_reservations add constraint pay_advance_reservations_created_by_user_id_fkey FOREIGN KEY (created_by_user_id) REFERENCES tms_users(id);

-- public.pay_advance_reservations.pay_advance_reservations_finance_case_id_fkey
alter table public.pay_advance_reservations add constraint pay_advance_reservations_finance_case_id_fkey FOREIGN KEY (finance_case_id) REFERENCES pay_advances(id);

-- public.pay_advance_reservations.pay_advance_reservations_finance_component_id_fkey
alter table public.pay_advance_reservations add constraint pay_advance_reservations_finance_component_id_fkey FOREIGN KEY (finance_component_id) REFERENCES pay_finance_case_components(id) ON DELETE SET NULL;

-- public.pay_advance_reservations.pay_advance_reservations_pay_batch_candidate_id_fkey
alter table public.pay_advance_reservations add constraint pay_advance_reservations_pay_batch_candidate_id_fkey FOREIGN KEY (pay_batch_candidate_id) REFERENCES pay_batch_candidates(id);

-- public.pay_advance_reservations.pay_advance_reservations_pay_batch_id_fkey
alter table public.pay_advance_reservations add constraint pay_advance_reservations_pay_batch_id_fkey FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id);

-- public.pay_advance_reservations.pay_advance_reservations_pay_batch_item_id_fkey
alter table public.pay_advance_reservations add constraint pay_advance_reservations_pay_batch_item_id_fkey FOREIGN KEY (pay_batch_item_id) REFERENCES pay_batch_items(id);

-- public.pay_advance_reservations.pay_advance_reservations_updated_by_user_id_fkey
alter table public.pay_advance_reservations add constraint pay_advance_reservations_updated_by_user_id_fkey FOREIGN KEY (updated_by_user_id) REFERENCES tms_users(id);

-- public.pay_advances.pay_advances_candidate_id_fkey
alter table public.pay_advances add constraint pay_advances_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id);

-- public.pay_advances.pay_advances_cleared_by_user_id_fkey
alter table public.pay_advances add constraint pay_advances_cleared_by_user_id_fkey FOREIGN KEY (cleared_by_user_id) REFERENCES tms_users(id);

-- public.pay_advances.pay_advances_client_id_fkey
alter table public.pay_advances add constraint pay_advances_client_id_fkey FOREIGN KEY (client_id) REFERENCES clients(id);

-- public.pay_advances.pay_advances_created_by_fkey
alter table public.pay_advances add constraint pay_advances_created_by_fkey FOREIGN KEY (created_by) REFERENCES tms_users(id);

-- public.pay_advances.pay_advances_linked_timesheet_id_fkey
alter table public.pay_advances add constraint pay_advances_linked_timesheet_id_fkey FOREIGN KEY (linked_timesheet_id) REFERENCES timesheets(timesheet_id) ON DELETE SET NULL;

-- public.pay_advances.pay_advances_payout_pay_batch_id_fkey
alter table public.pay_advances add constraint pay_advances_payout_pay_batch_id_fkey FOREIGN KEY (payout_pay_batch_id) REFERENCES pay_batches(id) ON DELETE SET NULL;

-- public.pay_advances.pay_advances_payout_transfer_id_fkey
alter table public.pay_advances add constraint pay_advances_payout_transfer_id_fkey FOREIGN KEY (payout_transfer_id) REFERENCES pay_bank_transfers(id) ON DELETE SET NULL;

-- public.pay_advances.pay_advances_written_off_by_user_id_fkey
alter table public.pay_advances add constraint pay_advances_written_off_by_user_id_fkey FOREIGN KEY (written_off_by_user_id) REFERENCES tms_users(id);

-- public.pay_bank_transfer_events.pay_bank_transfer_events_candidate_id_fkey
alter table public.pay_bank_transfer_events add constraint pay_bank_transfer_events_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id);

-- public.pay_bank_transfer_events.pay_bank_transfer_events_pay_bank_transfer_id_fkey
alter table public.pay_bank_transfer_events add constraint pay_bank_transfer_events_pay_bank_transfer_id_fkey FOREIGN KEY (pay_bank_transfer_id) REFERENCES pay_bank_transfers(id);

-- public.pay_bank_transfer_events.pay_bank_transfer_events_pay_batch_id_fkey
alter table public.pay_bank_transfer_events add constraint pay_bank_transfer_events_pay_batch_id_fkey FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id);

-- public.pay_bank_transfer_events.pay_bank_transfer_events_umbrella_id_fkey
alter table public.pay_bank_transfer_events add constraint pay_bank_transfer_events_umbrella_id_fkey FOREIGN KEY (umbrella_id) REFERENCES umbrellas(id);

-- public.pay_bank_transfer_events.pay_bank_transfer_events_webhook_receipt_fkey
alter table public.pay_bank_transfer_events add constraint pay_bank_transfer_events_webhook_receipt_fkey FOREIGN KEY (provider_webhook_receipt_id) REFERENCES bank_provider_webhook_receipts(id);

-- public.pay_bank_transfers.pay_bank_transfers_batch_fkey
alter table public.pay_bank_transfers add constraint pay_bank_transfers_batch_fkey FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id) ON DELETE CASCADE;

-- public.pay_bank_transfers.pay_bank_transfers_candidate_fkey
alter table public.pay_bank_transfers add constraint pay_bank_transfers_candidate_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE SET NULL;

-- public.pay_bank_transfers.pay_bank_transfers_umbrella_fkey
alter table public.pay_bank_transfers add constraint pay_bank_transfers_umbrella_fkey FOREIGN KEY (umbrella_id) REFERENCES umbrellas(id) ON DELETE SET NULL;

-- public.pay_batch_auth_actions.pay_batch_auth_actions_actor_user_id_fkey
alter table public.pay_batch_auth_actions add constraint pay_batch_auth_actions_actor_user_id_fkey FOREIGN KEY (actor_user_id) REFERENCES tms_users(id) ON DELETE RESTRICT;

-- public.pay_batch_auth_actions.pay_batch_auth_actions_auth_request_id_fkey
alter table public.pay_batch_auth_actions add constraint pay_batch_auth_actions_auth_request_id_fkey FOREIGN KEY (auth_request_id) REFERENCES pay_batch_auth_requests(id) ON DELETE CASCADE;

-- public.pay_batch_auth_actions.pay_batch_auth_actions_pay_batch_id_fkey
alter table public.pay_batch_auth_actions add constraint pay_batch_auth_actions_pay_batch_id_fkey FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id) ON DELETE CASCADE;

-- public.pay_batch_auth_requests.pay_batch_auth_requests_finalised_by_user_id_fkey
alter table public.pay_batch_auth_requests add constraint pay_batch_auth_requests_finalised_by_user_id_fkey FOREIGN KEY (finalised_by_user_id) REFERENCES tms_users(id) ON DELETE RESTRICT;

-- public.pay_batch_auth_requests.pay_batch_auth_requests_golden_key_user_id_fkey
alter table public.pay_batch_auth_requests add constraint pay_batch_auth_requests_golden_key_user_id_fkey FOREIGN KEY (golden_key_user_id) REFERENCES tms_users(id) ON DELETE RESTRICT;

-- public.pay_batch_auth_requests.pay_batch_auth_requests_pay_batch_id_fkey
alter table public.pay_batch_auth_requests add constraint pay_batch_auth_requests_pay_batch_id_fkey FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id) ON DELETE CASCADE;

-- public.pay_batch_auth_requests.pay_batch_auth_requests_requested_by_user_id_fkey
alter table public.pay_batch_auth_requests add constraint pay_batch_auth_requests_requested_by_user_id_fkey FOREIGN KEY (requested_by_user_id) REFERENCES tms_users(id) ON DELETE RESTRICT;

-- public.pay_batch_auth_tokens.pay_batch_auth_tokens_auth_request_id_fkey
alter table public.pay_batch_auth_tokens add constraint pay_batch_auth_tokens_auth_request_id_fkey FOREIGN KEY (auth_request_id) REFERENCES pay_batch_auth_requests(id) ON DELETE CASCADE;

-- public.pay_batch_auth_tokens.pay_batch_auth_tokens_target_user_id_fkey
alter table public.pay_batch_auth_tokens add constraint pay_batch_auth_tokens_target_user_id_fkey FOREIGN KEY (target_user_id) REFERENCES tms_users(id) ON DELETE RESTRICT;

-- public.pay_batch_candidates.pay_batch_candidates_candidate_id_fkey
alter table public.pay_batch_candidates add constraint pay_batch_candidates_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE RESTRICT;

-- public.pay_batch_candidates.pay_batch_candidates_pay_batch_id_fkey
alter table public.pay_batch_candidates add constraint pay_batch_candidates_pay_batch_id_fkey FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id) ON DELETE CASCADE;

-- public.pay_batch_candidates.pay_batch_candidates_remittance_sent_by_user_id_fkey
alter table public.pay_batch_candidates add constraint pay_batch_candidates_remittance_sent_by_user_id_fkey FOREIGN KEY (remittance_sent_by_user_id) REFERENCES tms_users(id);

-- public.pay_batch_display_summary.pay_batch_display_summary_batch_fk
alter table public.pay_batch_display_summary add constraint pay_batch_display_summary_batch_fk FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id) ON DELETE CASCADE;

-- public.pay_batch_display_summary.pay_batch_display_summary_latest_op_fk
alter table public.pay_batch_display_summary add constraint pay_batch_display_summary_latest_op_fk FOREIGN KEY (latest_operation_id) REFERENCES banking_pay_operations(id) ON DELETE SET NULL;

-- public.pay_batch_item_breakdowns.pay_batch_item_breakdowns_pay_batch_item_id_fkey
alter table public.pay_batch_item_breakdowns add constraint pay_batch_item_breakdowns_pay_batch_item_id_fkey FOREIGN KEY (pay_batch_item_id) REFERENCES pay_batch_items(id) ON DELETE CASCADE;

-- public.pay_batch_items.pay_batch_items_candidate_fkey
alter table public.pay_batch_items add constraint pay_batch_items_candidate_fkey FOREIGN KEY (pay_batch_candidate_id) REFERENCES pay_batch_candidates(id) ON DELETE CASCADE;

-- public.pay_batch_items.pay_batch_items_finance_case_id_fkey
alter table public.pay_batch_items add constraint pay_batch_items_finance_case_id_fkey FOREIGN KEY (finance_case_id) REFERENCES pay_advances(id);

-- public.pay_batch_items.pay_batch_items_finance_component_id_fkey
alter table public.pay_batch_items add constraint pay_batch_items_finance_component_id_fkey FOREIGN KEY (finance_component_id) REFERENCES pay_finance_case_components(id) ON DELETE SET NULL;

-- public.pay_batch_items.pay_batch_items_pay_bank_transfer_id_fkey
alter table public.pay_batch_items add constraint pay_batch_items_pay_bank_transfer_id_fkey FOREIGN KEY (pay_bank_transfer_id) REFERENCES pay_bank_transfers(id);

-- public.pay_batch_items.pay_batch_items_reservation_id_fkey
alter table public.pay_batch_items add constraint pay_batch_items_reservation_id_fkey FOREIGN KEY (reservation_id) REFERENCES pay_advance_reservations(id);

-- public.pay_batch_items.pay_batch_items_timesheet_id_fkey
alter table public.pay_batch_items add constraint pay_batch_items_timesheet_id_fkey FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id) ON DELETE SET NULL;

-- public.pay_batch_items.pay_batch_items_umbrella_id_fkey
alter table public.pay_batch_items add constraint pay_batch_items_umbrella_id_fkey FOREIGN KEY (umbrella_id) REFERENCES umbrellas(id) ON DELETE SET NULL;

-- public.pay_batch_paye_net_inputs.pay_batch_paye_net_inputs_candidate_fkey
alter table public.pay_batch_paye_net_inputs add constraint pay_batch_paye_net_inputs_candidate_fkey FOREIGN KEY (pay_batch_candidate_id) REFERENCES pay_batch_candidates(id) ON DELETE CASCADE;

-- public.pay_batch_timesheet_snapshots.pay_batch_timesheet_snapshots_candidate_id_fkey
alter table public.pay_batch_timesheet_snapshots add constraint pay_batch_timesheet_snapshots_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE RESTRICT;

-- public.pay_batch_timesheet_snapshots.pay_batch_timesheet_snapshots_pay_batch_id_fkey
alter table public.pay_batch_timesheet_snapshots add constraint pay_batch_timesheet_snapshots_pay_batch_id_fkey FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id) ON DELETE CASCADE;

-- public.pay_batch_timesheet_snapshots.pay_batch_timesheet_snapshots_timesheet_id_fkey
alter table public.pay_batch_timesheet_snapshots add constraint pay_batch_timesheet_snapshots_timesheet_id_fkey FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id) ON DELETE CASCADE;

-- public.pay_batches.pay_batches_cancelled_by_user_id_fkey
alter table public.pay_batches add constraint pay_batches_cancelled_by_user_id_fkey FOREIGN KEY (cancelled_by_user_id) REFERENCES tms_users(id);

-- public.pay_batches.pay_batches_created_by_user_id_fkey
alter table public.pay_batches add constraint pay_batches_created_by_user_id_fkey FOREIGN KEY (created_by_user_id) REFERENCES tms_users(id) ON DELETE SET NULL;

-- public.pay_batches.pay_batches_freshness_operation_id_fkey
alter table public.pay_batches add constraint pay_batches_freshness_operation_id_fkey FOREIGN KEY (freshness_operation_id) REFERENCES banking_pay_operations(id) ON DELETE SET NULL;

-- public.pay_batches.pay_batches_monzo_confirmed_by_user_id_fkey
alter table public.pay_batches add constraint pay_batches_monzo_confirmed_by_user_id_fkey FOREIGN KEY (monzo_confirmed_by_user_id) REFERENCES tms_users(id) ON DELETE SET NULL;

-- public.pay_batches.pay_batches_same_week_paye_override_verified_by_user_id_fkey
alter table public.pay_batches add constraint pay_batches_same_week_paye_override_verified_by_user_id_fkey FOREIGN KEY (same_week_paye_override_verified_by_user_id) REFERENCES tms_users(id);

-- public.pay_batches.pay_batches_scheduled_by_user_id_fkey
alter table public.pay_batches add constraint pay_batches_scheduled_by_user_id_fkey FOREIGN KEY (scheduled_by_user_id) REFERENCES tms_users(id);

-- public.pay_finance_case_components.pay_finance_case_components_candidate_id_fkey
alter table public.pay_finance_case_components add constraint pay_finance_case_components_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id);

-- public.pay_finance_case_components.pay_finance_case_components_client_id_fkey
alter table public.pay_finance_case_components add constraint pay_finance_case_components_client_id_fkey FOREIGN KEY (client_id) REFERENCES clients(id);

-- public.pay_finance_case_components.pay_finance_case_components_finance_case_id_fkey
alter table public.pay_finance_case_components add constraint pay_finance_case_components_finance_case_id_fkey FOREIGN KEY (finance_case_id) REFERENCES pay_advances(id) ON DELETE CASCADE;

-- public.pay_finance_case_components.pay_finance_case_components_linked_timesheet_id_fkey
alter table public.pay_finance_case_components add constraint pay_finance_case_components_linked_timesheet_id_fkey FOREIGN KEY (linked_timesheet_id) REFERENCES timesheets(timesheet_id);

-- public.pay_finance_case_events.pay_finance_case_events_actor_user_id_fkey
alter table public.pay_finance_case_events add constraint pay_finance_case_events_actor_user_id_fkey FOREIGN KEY (actor_user_id) REFERENCES tms_users(id);

-- public.pay_finance_case_events.pay_finance_case_events_finance_case_id_fkey
alter table public.pay_finance_case_events add constraint pay_finance_case_events_finance_case_id_fkey FOREIGN KEY (finance_case_id) REFERENCES pay_advances(id);

-- public.pay_finance_case_events.pay_finance_case_events_finance_component_id_fkey
alter table public.pay_finance_case_events add constraint pay_finance_case_events_finance_component_id_fkey FOREIGN KEY (finance_component_id) REFERENCES pay_finance_case_components(id) ON DELETE SET NULL;

-- public.pay_finance_case_events.pay_finance_case_events_pay_batch_id_fkey
alter table public.pay_finance_case_events add constraint pay_finance_case_events_pay_batch_id_fkey FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id);

-- public.pay_finance_case_events.pay_finance_case_events_reservation_id_fkey
alter table public.pay_finance_case_events add constraint pay_finance_case_events_reservation_id_fkey FOREIGN KEY (reservation_id) REFERENCES pay_advance_reservations(id);

-- public.pay_finance_case_oneoff_payout_bank_details.pay_finance_case_oneoff_payout_bank_detail_finance_case_id_fkey
alter table public.pay_finance_case_oneoff_payout_bank_details add constraint pay_finance_case_oneoff_payout_bank_detail_finance_case_id_fkey FOREIGN KEY (finance_case_id) REFERENCES pay_advances(id) ON DELETE CASCADE;

-- public.pay_finance_case_oneoff_payout_bank_details.pay_finance_case_oneoff_payout_bank_details_candidate_id_fkey
alter table public.pay_finance_case_oneoff_payout_bank_details add constraint pay_finance_case_oneoff_payout_bank_details_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE RESTRICT;

-- public.pay_item_snoozes.pay_item_snoozes_candidate_id_fkey
alter table public.pay_item_snoozes add constraint pay_item_snoozes_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE CASCADE;

-- public.pay_item_snoozes.pay_item_snoozes_cleared_by_user_id_fkey
alter table public.pay_item_snoozes add constraint pay_item_snoozes_cleared_by_user_id_fkey FOREIGN KEY (cleared_by_user_id) REFERENCES tms_users(id) ON DELETE SET NULL;

-- public.pay_item_snoozes.pay_item_snoozes_created_by_user_id_fkey
alter table public.pay_item_snoozes add constraint pay_item_snoozes_created_by_user_id_fkey FOREIGN KEY (created_by_user_id) REFERENCES tms_users(id) ON DELETE SET NULL;

-- public.pay_item_snoozes.pay_item_snoozes_timesheet_id_fkey
alter table public.pay_item_snoozes add constraint pay_item_snoozes_timesheet_id_fkey FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id) ON DELETE CASCADE;

-- public.pay_item_snoozes.pay_item_snoozes_updated_by_user_id_fkey
alter table public.pay_item_snoozes add constraint pay_item_snoozes_updated_by_user_id_fkey FOREIGN KEY (updated_by_user_id) REFERENCES tms_users(id);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_f_source_correction_request_id_fkey
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_f_source_correction_request_id_fkey FOREIGN KEY (source_correction_request_id) REFERENCES pay_payment_correction_requests(id);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_f_source_correction_work_item__fkey
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_f_source_correction_work_item__fkey FOREIGN KEY (source_correction_work_item_id) REFERENCES pay_payment_correction_work_items(id);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_f_source_pay_batch_candidate_i_fkey
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_f_source_pay_batch_candidate_i_fkey FOREIGN KEY (source_pay_batch_candidate_id) REFERENCES pay_batch_candidates(id);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_fo_source_pay_bank_transfer_id_fkey
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_fo_source_pay_bank_transfer_id_fkey FOREIGN KEY (source_pay_bank_transfer_id) REFERENCES pay_bank_transfers(id);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_forwa_source_pay_batch_item_id_fkey
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_forwa_source_pay_batch_item_id_fkey FOREIGN KEY (source_pay_batch_item_id) REFERENCES pay_batch_items(id);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_forwa_target_pay_batch_item_id_fkey
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_forwa_target_pay_batch_item_id_fkey FOREIGN KEY (target_pay_batch_item_id) REFERENCES pay_batch_items(id);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_forwards_candidate_fkey
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_forwards_candidate_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_forwards_candidate_id_fkey
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_forwards_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_forwards_client_fkey
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_forwards_client_fkey FOREIGN KEY (client_id) REFERENCES clients(id);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_forwards_client_id_fkey
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_forwards_client_id_fkey FOREIGN KEY (client_id) REFERENCES clients(id);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_forwards_created_by_fkey
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_forwards_created_by_fkey FOREIGN KEY (created_by_user_id) REFERENCES auth.users(id);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_forwards_created_by_user_id_fkey
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_forwards_created_by_user_id_fkey FOREIGN KEY (created_by_user_id) REFERENCES auth.users(id);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_forwards_source_batch_fkey
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_forwards_source_batch_fkey FOREIGN KEY (source_pay_batch_id) REFERENCES pay_batches(id);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_forwards_source_candidate_fkey
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_forwards_source_candidate_fkey FOREIGN KEY (source_pay_batch_candidate_id) REFERENCES pay_batch_candidates(id);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_forwards_source_item_fkey
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_forwards_source_item_fkey FOREIGN KEY (source_pay_batch_item_id) REFERENCES pay_batch_items(id);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_forwards_source_pay_batch_id_fkey
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_forwards_source_pay_batch_id_fkey FOREIGN KEY (source_pay_batch_id) REFERENCES pay_batches(id);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_forwards_source_request_fkey
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_forwards_source_request_fkey FOREIGN KEY (source_correction_request_id) REFERENCES pay_payment_correction_requests(id);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_forwards_source_transfer_fkey
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_forwards_source_transfer_fkey FOREIGN KEY (source_pay_bank_transfer_id) REFERENCES pay_bank_transfers(id);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_forwards_source_work_item_fkey
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_forwards_source_work_item_fkey FOREIGN KEY (source_correction_work_item_id) REFERENCES pay_payment_correction_work_items(id);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_forwards_target_batch_fkey
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_forwards_target_batch_fkey FOREIGN KEY (target_pay_batch_id) REFERENCES pay_batches(id);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_forwards_target_item_fkey
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_forwards_target_item_fkey FOREIGN KEY (target_pay_batch_item_id) REFERENCES pay_batch_items(id);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_forwards_target_pay_batch_id_fkey
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_forwards_target_pay_batch_id_fkey FOREIGN KEY (target_pay_batch_id) REFERENCES pay_batches(id);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_forwards_timesheet_fkey
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_forwards_timesheet_fkey FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_forwards_timesheet_id_fkey
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_forwards_timesheet_id_fkey FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_forwards_umbrella_fkey
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_forwards_umbrella_fkey FOREIGN KEY (umbrella_id) REFERENCES umbrellas(id);

-- public.pay_manual_adjustment_carry_forwards.pay_manual_adjustment_carry_forwards_umbrella_id_fkey
alter table public.pay_manual_adjustment_carry_forwards add constraint pay_manual_adjustment_carry_forwards_umbrella_id_fkey FOREIGN KEY (umbrella_id) REFERENCES umbrellas(id);

-- public.pay_payment_correction_actions.pay_payment_correction_actions_actor_user_id_fkey
alter table public.pay_payment_correction_actions add constraint pay_payment_correction_actions_actor_user_id_fkey FOREIGN KEY (actor_user_id) REFERENCES tms_users(id);

-- public.pay_payment_correction_actions.pay_payment_correction_actions_correction_request_id_fkey
alter table public.pay_payment_correction_actions add constraint pay_payment_correction_actions_correction_request_id_fkey FOREIGN KEY (correction_request_id) REFERENCES pay_payment_correction_requests(id);

-- public.pay_payment_correction_actions.pay_payment_correction_actions_pay_batch_id_fkey
alter table public.pay_payment_correction_actions add constraint pay_payment_correction_actions_pay_batch_id_fkey FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id);

-- public.pay_payment_correction_items.pay_payment_correction_items_candidate_id_fkey
alter table public.pay_payment_correction_items add constraint pay_payment_correction_items_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id);

-- public.pay_payment_correction_items.pay_payment_correction_items_correction_request_id_fkey
alter table public.pay_payment_correction_items add constraint pay_payment_correction_items_correction_request_id_fkey FOREIGN KEY (correction_request_id) REFERENCES pay_payment_correction_requests(id);

-- public.pay_payment_correction_items.pay_payment_correction_items_finance_case_id_fkey
alter table public.pay_payment_correction_items add constraint pay_payment_correction_items_finance_case_id_fkey FOREIGN KEY (finance_case_id) REFERENCES pay_advances(id);

-- public.pay_payment_correction_items.pay_payment_correction_items_finance_component_id_fkey
alter table public.pay_payment_correction_items add constraint pay_payment_correction_items_finance_component_id_fkey FOREIGN KEY (finance_component_id) REFERENCES pay_finance_case_components(id);

-- public.pay_payment_correction_items.pay_payment_correction_items_pay_bank_transfer_id_fkey
alter table public.pay_payment_correction_items add constraint pay_payment_correction_items_pay_bank_transfer_id_fkey FOREIGN KEY (pay_bank_transfer_id) REFERENCES pay_bank_transfers(id);

-- public.pay_payment_correction_items.pay_payment_correction_items_pay_batch_candidate_id_fkey
alter table public.pay_payment_correction_items add constraint pay_payment_correction_items_pay_batch_candidate_id_fkey FOREIGN KEY (pay_batch_candidate_id) REFERENCES pay_batch_candidates(id);

-- public.pay_payment_correction_items.pay_payment_correction_items_pay_batch_id_fkey
alter table public.pay_payment_correction_items add constraint pay_payment_correction_items_pay_batch_id_fkey FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id);

-- public.pay_payment_correction_items.pay_payment_correction_items_pay_batch_item_id_fkey
alter table public.pay_payment_correction_items add constraint pay_payment_correction_items_pay_batch_item_id_fkey FOREIGN KEY (pay_batch_item_id) REFERENCES pay_batch_items(id);

-- public.pay_payment_correction_items.pay_payment_correction_items_reservation_id_fkey
alter table public.pay_payment_correction_items add constraint pay_payment_correction_items_reservation_id_fkey FOREIGN KEY (reservation_id) REFERENCES pay_advance_reservations(id);

-- public.pay_payment_correction_request_candidates.pay_payment_correction_request_candidates_candidate_fkey
alter table public.pay_payment_correction_request_candidates add constraint pay_payment_correction_request_candidates_candidate_fkey FOREIGN KEY (pay_batch_candidate_id) REFERENCES pay_batch_candidates(id) ON DELETE RESTRICT;

-- public.pay_payment_correction_request_candidates.pay_payment_correction_request_candidates_request_fkey
alter table public.pay_payment_correction_request_candidates add constraint pay_payment_correction_request_candidates_request_fkey FOREIGN KEY (correction_request_id) REFERENCES pay_payment_correction_requests(id) ON DELETE RESTRICT;

-- public.pay_payment_correction_requests.pay_payment_correction_requests_golden_key_user_id_fkey
alter table public.pay_payment_correction_requests add constraint pay_payment_correction_requests_golden_key_user_id_fkey FOREIGN KEY (golden_key_user_id) REFERENCES tms_users(id);

-- public.pay_payment_correction_requests.pay_payment_correction_requests_pay_batch_id_fkey
alter table public.pay_payment_correction_requests add constraint pay_payment_correction_requests_pay_batch_id_fkey FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id);

-- public.pay_payment_correction_requests.pay_payment_correction_requests_requested_by_user_id_fkey
alter table public.pay_payment_correction_requests add constraint pay_payment_correction_requests_requested_by_user_id_fkey FOREIGN KEY (requested_by_user_id) REFERENCES tms_users(id);

-- public.pay_payment_correction_requests.pay_payment_correction_requests_source_bank_event_id_fkey
alter table public.pay_payment_correction_requests add constraint pay_payment_correction_requests_source_bank_event_id_fkey FOREIGN KEY (source_bank_event_id) REFERENCES pay_bank_transfer_events(id);

-- public.pay_payment_correction_work_items.pay_payment_correction_work_items_candidate_id_fkey
alter table public.pay_payment_correction_work_items add constraint pay_payment_correction_work_items_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id);

-- public.pay_payment_correction_work_items.pay_payment_correction_work_items_correction_request_id_fkey
alter table public.pay_payment_correction_work_items add constraint pay_payment_correction_work_items_correction_request_id_fkey FOREIGN KEY (correction_request_id) REFERENCES pay_payment_correction_requests(id);

-- public.pay_payment_correction_work_items.pay_payment_correction_work_items_pay_bank_transfer_id_fkey
alter table public.pay_payment_correction_work_items add constraint pay_payment_correction_work_items_pay_bank_transfer_id_fkey FOREIGN KEY (pay_bank_transfer_id) REFERENCES pay_bank_transfers(id);

-- public.pay_payment_correction_work_items.pay_payment_correction_work_items_pay_batch_candidate_id_fkey
alter table public.pay_payment_correction_work_items add constraint pay_payment_correction_work_items_pay_batch_candidate_id_fkey FOREIGN KEY (pay_batch_candidate_id) REFERENCES pay_batch_candidates(id);

-- public.pay_payment_correction_work_items.pay_payment_correction_work_items_pay_batch_id_fkey
alter table public.pay_payment_correction_work_items add constraint pay_payment_correction_work_items_pay_batch_id_fkey FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id);

-- public.pay_payment_correction_work_items.pay_payment_correction_work_items_processed_by_user_id_fkey
alter table public.pay_payment_correction_work_items add constraint pay_payment_correction_work_items_processed_by_user_id_fkey FOREIGN KEY (processed_by_user_id) REFERENCES tms_users(id) ON DELETE SET NULL;

-- public.pay_payment_correction_work_items.pay_payment_correction_work_items_umbrella_id_fkey
alter table public.pay_payment_correction_work_items add constraint pay_payment_correction_work_items_umbrella_id_fkey FOREIGN KEY (umbrella_id) REFERENCES umbrellas(id);

-- public.pay_payment_return_notice_groups.pay_payment_return_notice_groups_pay_batch_id_fkey
alter table public.pay_payment_return_notice_groups add constraint pay_payment_return_notice_groups_pay_batch_id_fkey FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id);

-- public.pay_snooze_warning_acknowledgements.pay_snooze_warning_acknowledgements_actor_user_id_fkey
alter table public.pay_snooze_warning_acknowledgements add constraint pay_snooze_warning_acknowledgements_actor_user_id_fkey FOREIGN KEY (actor_user_id) REFERENCES tms_users(id) ON DELETE CASCADE;

-- public.pay_snooze_warning_acknowledgements.pay_snooze_warning_acknowledgements_candidate_id_fkey
alter table public.pay_snooze_warning_acknowledgements add constraint pay_snooze_warning_acknowledgements_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE CASCADE;

-- public.pay_snooze_warning_acknowledgements.pay_snooze_warning_acknowledgements_timesheet_id_fkey
alter table public.pay_snooze_warning_acknowledgements add constraint pay_snooze_warning_acknowledgements_timesheet_id_fkey FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id) ON DELETE CASCADE;

-- public.rates_candidate_overrides.rates_candidate_overrides_candidate_id_fkey
alter table public.rates_candidate_overrides add constraint rates_candidate_overrides_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE CASCADE;

-- public.rates_candidate_overrides.rates_candidate_overrides_client_id_fkey
alter table public.rates_candidate_overrides add constraint rates_candidate_overrides_client_id_fkey FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE;

-- public.rates_client_defaults.rates_client_defaults_client_id_fkey
alter table public.rates_client_defaults add constraint rates_client_defaults_client_id_fkey FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE;

-- public.rates_client_defaults.rates_client_defaults_disabled_by_fkey
alter table public.rates_client_defaults add constraint rates_client_defaults_disabled_by_fkey FOREIGN KEY (disabled_by) REFERENCES tms_users(id) ON DELETE SET NULL;

-- public.rates_presets.rates_presets_client_id_fkey
alter table public.rates_presets add constraint rates_presets_client_id_fkey FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE;

-- public.report_presets.report_presets_user_id_fkey
alter table public.report_presets add constraint report_presets_user_id_fkey FOREIGN KEY (user_id) REFERENCES tms_users(id) ON DELETE CASCADE;

-- public.settings_defaults.settings_defaults_candidate_app_system_actor_fk
alter table public.settings_defaults add constraint settings_defaults_candidate_app_system_actor_fk FOREIGN KEY (candidate_app_system_actor_user_id) REFERENCES tms_users(id) ON DELETE RESTRICT;

-- public.timesheet_evidence.timesheet_evidence_candidate_component_fk
alter table public.timesheet_evidence add constraint timesheet_evidence_candidate_component_fk FOREIGN KEY (candidate_component_id) REFERENCES candidate_submission_components(id) ON DELETE RESTRICT;

-- public.timesheet_evidence.timesheet_evidence_created_by_fkey
alter table public.timesheet_evidence add constraint timesheet_evidence_created_by_fkey FOREIGN KEY (created_by) REFERENCES tms_users(id);

-- public.timesheet_evidence.timesheet_evidence_document_asset_fk
alter table public.timesheet_evidence add constraint timesheet_evidence_document_asset_fk FOREIGN KEY (document_asset_id) REFERENCES invoice_document_assets(id) ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED;

-- public.timesheet_evidence.timesheet_evidence_timesheet_id_fkey
alter table public.timesheet_evidence add constraint timesheet_evidence_timesheet_id_fkey FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id) ON DELETE CASCADE;

-- public.timesheet_financial_retention.timesheet_financial_retention_timesheet_fk
alter table public.timesheet_financial_retention add constraint timesheet_financial_retention_timesheet_fk FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id) ON UPDATE RESTRICT ON DELETE RESTRICT;

-- public.timesheet_lifecycle_bulk_operation_items.timesheet_lifecycle_bulk_operation_items_operation_id_fkey
alter table public.timesheet_lifecycle_bulk_operation_items add constraint timesheet_lifecycle_bulk_operation_items_operation_id_fkey FOREIGN KEY (operation_id) REFERENCES timesheet_lifecycle_bulk_operations(id) ON DELETE CASCADE;

-- public.timesheet_pay_state_history.timesheet_pay_state_history_batch_fkey
alter table public.timesheet_pay_state_history add constraint timesheet_pay_state_history_batch_fkey FOREIGN KEY (pay_batch_id) REFERENCES pay_batches(id) ON DELETE CASCADE;

-- public.timesheet_pay_state_history.timesheet_pay_state_history_timesheet_fkey
alter table public.timesheet_pay_state_history add constraint timesheet_pay_state_history_timesheet_fkey FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id) ON DELETE CASCADE;

-- public.timesheet_pay_state.timesheet_pay_state_last_batch_fkey
alter table public.timesheet_pay_state add constraint timesheet_pay_state_last_batch_fkey FOREIGN KEY (last_settled_pay_batch_id) REFERENCES pay_batches(id) ON DELETE SET NULL;

-- public.timesheet_pay_state.timesheet_pay_state_timesheet_id_fkey
alter table public.timesheet_pay_state add constraint timesheet_pay_state_timesheet_id_fkey FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id) ON DELETE CASCADE;

-- public.timesheet_payment_overrides.timesheet_payment_overrides_candidate_id_fkey
alter table public.timesheet_payment_overrides add constraint timesheet_payment_overrides_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id);

-- public.timesheet_payment_overrides.timesheet_payment_overrides_cleared_by_user_id_fkey
alter table public.timesheet_payment_overrides add constraint timesheet_payment_overrides_cleared_by_user_id_fkey FOREIGN KEY (cleared_by_user_id) REFERENCES tms_users(id);

-- public.timesheet_payment_overrides.timesheet_payment_overrides_consumed_by_pay_batch_id_fkey
alter table public.timesheet_payment_overrides add constraint timesheet_payment_overrides_consumed_by_pay_batch_id_fkey FOREIGN KEY (consumed_by_pay_batch_id) REFERENCES pay_batches(id);

-- public.timesheet_payment_overrides.timesheet_payment_overrides_created_by_user_id_fkey
alter table public.timesheet_payment_overrides add constraint timesheet_payment_overrides_created_by_user_id_fkey FOREIGN KEY (created_by_user_id) REFERENCES tms_users(id);

-- public.timesheet_payment_overrides.timesheet_payment_overrides_timesheet_id_fkey
alter table public.timesheet_payment_overrides add constraint timesheet_payment_overrides_timesheet_id_fkey FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id);

-- public.timesheet_summary_pay_state_cache.timesheet_summary_pay_state_cache_timesheet_id_fkey
alter table public.timesheet_summary_pay_state_cache add constraint timesheet_summary_pay_state_cache_timesheet_id_fkey FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id) ON DELETE CASCADE;

-- public.timesheet_validations.timesheet_validations_hr_request_set_by_fkey
alter table public.timesheet_validations add constraint timesheet_validations_hr_request_set_by_fkey FOREIGN KEY (hr_request_set_by) REFERENCES tms_users(id) ON DELETE SET NULL;

-- public.timesheet_validations.timesheet_validations_last_source_fkey
alter table public.timesheet_validations add constraint timesheet_validations_last_source_fkey FOREIGN KEY (last_source) REFERENCES hr_imports(id) ON DELETE SET NULL;

-- public.timesheet_validations.timesheet_validations_override_confirmed_by_fkey
alter table public.timesheet_validations add constraint timesheet_validations_override_confirmed_by_fkey FOREIGN KEY (override_confirmed_by) REFERENCES tms_users(id) ON DELETE SET NULL;

-- public.timesheet_validations.timesheet_validations_override_requested_by_fkey
alter table public.timesheet_validations add constraint timesheet_validations_override_requested_by_fkey FOREIGN KEY (override_requested_by) REFERENCES tms_users(id) ON DELETE SET NULL;

-- public.timesheets_financials.timesheets_financials_authorised_by_user_id_fkey
alter table public.timesheets_financials add constraint timesheets_financials_authorised_by_user_id_fkey FOREIGN KEY (authorised_by_user_id) REFERENCES tms_users(id) ON DELETE SET NULL;

-- public.timesheets_financials.timesheets_financials_candidate_id_fkey
alter table public.timesheets_financials add constraint timesheets_financials_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id);

-- public.timesheets_financials.timesheets_financials_client_id_fkey
alter table public.timesheets_financials add constraint timesheets_financials_client_id_fkey FOREIGN KEY (client_id) REFERENCES clients(id);

-- public.timesheets_financials.timesheets_financials_locked_by_invoice_id_fkey
alter table public.timesheets_financials add constraint timesheets_financials_locked_by_invoice_id_fkey FOREIGN KEY (locked_by_invoice_id) REFERENCES invoices(id);

-- public.timesheets_financials.timesheets_financials_nhsp_import_fkey
alter table public.timesheets_financials add constraint timesheets_financials_nhsp_import_fkey FOREIGN KEY (nhsp_import_id) REFERENCES hr_imports(id) ON DELETE SET NULL;

-- public.timesheets_financials.timesheets_financials_paid_by_user_id_fkey
alter table public.timesheets_financials add constraint timesheets_financials_paid_by_user_id_fkey FOREIGN KEY (paid_by_user_id) REFERENCES tms_users(id) ON DELETE SET NULL;

-- public.timesheets_financials.timesheets_financials_processed_by_user_id_fkey
alter table public.timesheets_financials add constraint timesheets_financials_processed_by_user_id_fkey FOREIGN KEY (processed_by_user_id) REFERENCES tms_users(id) ON DELETE SET NULL;

-- public.timesheets_financials.timesheets_financials_timesheet_id_fkey
alter table public.timesheets_financials add constraint timesheets_financials_timesheet_id_fkey FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id) ON DELETE CASCADE;

-- public.timesheets_financials.timesheets_financials_unlocked_by_credit_note_id_fkey
alter table public.timesheets_financials add constraint timesheets_financials_unlocked_by_credit_note_id_fkey FOREIGN KEY (unlocked_by_credit_note_id) REFERENCES invoices(id);

-- public.timesheets.timesheets_active_document_operation_fk
alter table public.timesheets add constraint timesheets_active_document_operation_fk FOREIGN KEY (active_document_operation_id) REFERENCES invoice_operations(id) ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED;

-- public.timesheets.timesheets_archived_by_user_id_fkey
alter table public.timesheets add constraint timesheets_archived_by_user_id_fkey FOREIGN KEY (archived_by_user_id) REFERENCES tms_users(id) ON DELETE RESTRICT;

-- public.timesheets.timesheets_candidate_workflow_fk
alter table public.timesheets add constraint timesheets_candidate_workflow_fk FOREIGN KEY (candidate_workflow_id) REFERENCES candidate_submission_workflows(id) ON DELETE RESTRICT;

-- public.timesheets.timesheets_contract_id_fkey
alter table public.timesheets add constraint timesheets_contract_id_fkey FOREIGN KEY (contract_id) REFERENCES contracts(id) ON DELETE SET NULL;

-- public.timesheets.timesheets_current_document_version_fk
alter table public.timesheets add constraint timesheets_current_document_version_fk FOREIGN KEY (current_document_version_id) REFERENCES invoice_document_versions(id) ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED;

-- public.timesheets.timesheets_manual_document_asset_fk
alter table public.timesheets add constraint timesheets_manual_document_asset_fk FOREIGN KEY (manual_document_asset_id) REFERENCES invoice_document_assets(id) ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED;

-- public.timesheets.timesheets_parent_timesheet_id_fkey
alter table public.timesheets add constraint timesheets_parent_timesheet_id_fkey FOREIGN KEY (parent_timesheet_id) REFERENCES timesheets(timesheet_id) ON DELETE SET NULL;

-- public.tms_login_2fa_challenges.tms_login_2fa_challenges_user_id_fkey
alter table public.tms_login_2fa_challenges add constraint tms_login_2fa_challenges_user_id_fkey FOREIGN KEY (user_id) REFERENCES tms_users(id) ON DELETE CASCADE;

-- public.tms_password_resets.tms_password_resets_user_id_fkey
alter table public.tms_password_resets add constraint tms_password_resets_user_id_fkey FOREIGN KEY (user_id) REFERENCES tms_users(id) ON DELETE CASCADE;

-- public.tms_user_2fa_trust.tms_user_2fa_trust_user_id_fkey
alter table public.tms_user_2fa_trust add constraint tms_user_2fa_trust_user_id_fkey FOREIGN KEY (user_id) REFERENCES tms_users(id) ON DELETE CASCADE;

-- public.ts_financials_outbox.ts_financials_outbox_timesheet_id_fkey
alter table public.ts_financials_outbox add constraint ts_financials_outbox_timesheet_id_fkey FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id) ON DELETE CASCADE;

-- public.ts_pay_adjustments.ts_pay_adjustments_candidate_id_fkey
alter table public.ts_pay_adjustments add constraint ts_pay_adjustments_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES candidates(id);

-- public.ts_pay_adjustments.ts_pay_adjustments_client_id_fkey
alter table public.ts_pay_adjustments add constraint ts_pay_adjustments_client_id_fkey FOREIGN KEY (client_id) REFERENCES clients(id);

-- public.ts_pay_adjustments.ts_pay_adjustments_timesheet_id_fkey
alter table public.ts_pay_adjustments add constraint ts_pay_adjustments_timesheet_id_fkey FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id);

-- public.ts_pdfs_outbox.ts_pdfs_outbox_timesheet_id_fkey
alter table public.ts_pdfs_outbox add constraint ts_pdfs_outbox_timesheet_id_fkey FOREIGN KEY (timesheet_id) REFERENCES timesheets(timesheet_id) ON DELETE CASCADE;

-- private.bpay_wb_canonical_stage_session_candidate_idx
CREATE INDEX bpay_wb_canonical_stage_session_candidate_idx ON private.banking_pay_workbench_canonical_stage_lines USING btree (session_id, candidate_id, source_ordinal, line_key);

-- private.bpay_wb_canonical_stage_timesheet_idx
CREATE INDEX bpay_wb_canonical_stage_timesheet_idx ON private.banking_pay_workbench_canonical_stage_lines USING btree (build_id, timesheet_id, source_ordinal) WHERE (timesheet_id IS NOT NULL);

-- private.bpay_wb_dependency_edges_stream_idx
CREATE INDEX bpay_wb_dependency_edges_stream_idx ON private.banking_pay_workbench_economic_build_facts USING btree (build_id, edge_from_timesheet_id, edge_kind, natural_key) INCLUDE (edge_to_timesheet_id, dependency_unit_key, financial_digest) WHERE (fact_family = 'DEPENDENCY_EDGE'::text);

-- private.bpay_wb_economic_build_fact_pages_attempt_idx
CREATE INDEX bpay_wb_economic_build_fact_pages_attempt_idx ON private.banking_pay_workbench_economic_build_fact_pages USING btree (attempt_id, id);

-- private.bpay_wb_economic_build_fact_pages_completion_idx
CREATE INDEX bpay_wb_economic_build_fact_pages_completion_idx ON private.banking_pay_workbench_economic_build_fact_pages USING btree (build_id, fact_family, dependency_unit_key, is_family_final, page_number);

-- private.bpay_wb_economic_build_facts_source_idx
CREATE INDEX bpay_wb_economic_build_facts_source_idx ON private.banking_pay_workbench_economic_build_facts USING btree (source_relation, source_id, build_id, fact_family) WHERE (source_id IS NOT NULL);

-- private.bpay_wb_economic_build_facts_timesheet_idx
CREATE INDEX bpay_wb_economic_build_facts_timesheet_idx ON private.banking_pay_workbench_economic_build_facts USING btree (build_id, timesheet_id, fact_family, natural_key) WHERE (timesheet_id IS NOT NULL);

-- private.bpay_wb_economic_build_facts_unit_idx
CREATE INDEX bpay_wb_economic_build_facts_unit_idx ON private.banking_pay_workbench_economic_build_facts USING btree (build_id, dependency_unit_key, fact_family, natural_key);

-- private.bpay_wb_economic_build_scope_candidate_idx
CREATE INDEX bpay_wb_economic_build_scope_candidate_idx ON private.banking_pay_workbench_economic_build_scope USING btree (candidate_id, timesheet_id, build_id);

-- private.bpay_wb_economic_build_scope_incomplete_idx
CREATE INDEX bpay_wb_economic_build_scope_incomplete_idx ON private.banking_pay_workbench_economic_build_scope USING btree (build_id, closure_status, dependency_unit_anchor_timesheet_id, timesheet_id) WHERE (closure_status <> 'SEALED'::text);

-- private.bpay_wb_economic_build_scope_ordinal_uq
CREATE UNIQUE INDEX bpay_wb_economic_build_scope_ordinal_uq ON private.banking_pay_workbench_economic_build_scope USING btree (build_id, stable_ordinal) WHERE (stable_ordinal IS NOT NULL);

-- private.bpay_wb_economic_build_scope_unit_idx
CREATE INDEX bpay_wb_economic_build_scope_unit_idx ON private.banking_pay_workbench_economic_build_scope USING btree (build_id, dependency_unit_key, stable_ordinal, timesheet_id);

-- private.bpay_wb_economic_builds_candidate_active_uq
CREATE UNIQUE INDEX bpay_wb_economic_builds_candidate_active_uq ON private.banking_pay_workbench_economic_builds USING btree (candidate_id) WHERE (status = ANY (ARRAY['COLLECTING'::text, 'READY_FOR_RECONCILIATION'::text, 'RECONCILING'::text, 'RECONCILED'::text, 'PUBLISHING'::text, 'BLOCKED_UNVALIDATED_RECONCILIATION_SCALE'::text]));

-- private.bpay_wb_economic_builds_candidate_fk_idx
CREATE INDEX bpay_wb_economic_builds_candidate_fk_idx ON private.banking_pay_workbench_economic_builds USING btree (candidate_id, id);

-- private.bpay_wb_economic_builds_cleanup_idx
CREATE INDEX bpay_wb_economic_builds_cleanup_idx ON private.banking_pay_workbench_economic_builds USING btree (cleanup_not_before_utc, id) WHERE (status = ANY (ARRAY['COMPLETE'::text, 'OBSOLETE'::text, 'FAILED'::text, 'CLEANING'::text]));

-- private.bpay_wb_economic_builds_recovery_idx
CREATE INDEX bpay_wb_economic_builds_recovery_idx ON private.banking_pay_workbench_economic_builds USING btree (status, private_stage, updated_at_utc, id) WHERE (status = ANY (ARRAY['COLLECTING'::text, 'READY_FOR_RECONCILIATION'::text, 'RECONCILING'::text, 'RECONCILED'::text, 'PUBLISHING'::text, 'BLOCKED_UNVALIDATED_RECONCILIATION_SCALE'::text, 'CLEANING'::text]));

-- private.bpay_wb_economic_builds_session_authority_idx
CREATE INDEX bpay_wb_economic_builds_session_authority_idx ON private.banking_pay_workbench_economic_builds USING btree (session_id, candidate_id, session_version, source_change_seq, source_build_run_id, id);

-- private.bpay_wb_economic_builds_source_job_uq
CREATE UNIQUE INDEX bpay_wb_economic_builds_source_job_uq ON private.banking_pay_workbench_economic_builds USING btree (source_job_id) WHERE (source_job_id IS NOT NULL);

-- private.bpay_wb_scope_registry_current_build_idx
CREATE INDEX bpay_wb_scope_registry_current_build_idx ON private.banking_pay_workbench_candidate_scope_registry USING btree (current_build_id) WHERE (current_build_id IS NOT NULL);

-- private.bpay_wb_scope_registry_initialisation_idx
CREATE INDEX bpay_wb_scope_registry_initialisation_idx ON private.banking_pay_workbench_candidate_scope_registry USING btree (initialisation_status, updated_at_utc, candidate_id);

-- private.bpay_wb_stage_attempts_build_stage_idx
CREATE INDEX bpay_wb_stage_attempts_build_stage_idx ON private.banking_pay_workbench_stage_attempts USING btree (build_id, private_stage, attempt_number DESC, id);

-- private.bpay_wb_stage_attempts_expiry_idx
CREATE INDEX bpay_wb_stage_attempts_expiry_idx ON private.banking_pay_workbench_stage_attempts USING btree (lease_expires_at_utc, id) WHERE (attempt_status = 'STARTED'::text);

-- private.bpay_wb_stage_attempts_started_job_uq
CREATE UNIQUE INDEX bpay_wb_stage_attempts_started_job_uq ON private.banking_pay_workbench_stage_attempts USING btree (job_id) WHERE (attempt_status = 'STARTED'::text);

-- private.bpay_wb_timesheet_scope_active_idx
CREATE INDEX bpay_wb_timesheet_scope_active_idx ON private.banking_pay_workbench_timesheet_scope_state USING btree (candidate_id, dirty_generation, timesheet_id) INCLUDE (economic_state, evaluated_generation, current_input_fingerprint, current_build_id) WHERE (economic_state = ANY (ARRAY['DIRTY'::text, 'LIVE'::text]));

-- private.bpay_wb_timesheet_scope_build_idx
CREATE INDEX bpay_wb_timesheet_scope_build_idx ON private.banking_pay_workbench_timesheet_scope_state USING btree (current_build_id, candidate_id, timesheet_id) WHERE (current_build_id IS NOT NULL);

-- private.bpay_wb_timesheet_scope_candidate_fk_idx
CREATE INDEX bpay_wb_timesheet_scope_candidate_fk_idx ON private.banking_pay_workbench_timesheet_scope_state USING btree (candidate_id, timesheet_id);

-- private.bpay_wb_timesheet_scope_dirty_idx
CREATE INDEX bpay_wb_timesheet_scope_dirty_idx ON private.banking_pay_workbench_timesheet_scope_state USING btree (candidate_id, last_dirtied_at_utc, timesheet_id) WHERE (economic_state = 'DIRTY'::text);

-- private.candidate_daily_batch_receipts_state_idx
CREATE INDEX candidate_daily_batch_receipts_state_idx ON private.candidate_daily_batch_receipts USING btree (environment, state, updated_at_utc);

-- private.candidate_daily_external_effects_candidate_idx
CREATE INDEX candidate_daily_external_effects_candidate_idx ON private.candidate_daily_external_effect_receipts USING btree (environment, candidate_id, operation, created_at_utc);

-- private.candidate_daily_external_effects_lease_idx
CREATE INDEX candidate_daily_external_effects_lease_idx ON private.candidate_daily_external_effect_receipts USING btree (environment, state, lease_expires_at_utc);

-- private.candidate_daily_source_links_active_hmac_uq
CREATE UNIQUE INDEX candidate_daily_source_links_active_hmac_uq ON private.candidate_daily_source_links USING btree (environment, source_system, hmac_key_version, identifier_hmac) WHERE (state = ANY (ARRAY['PRIMARY'::text, 'OVERLAP'::text]));

-- private.candidate_daily_source_links_candidate_group_idx
CREATE INDEX candidate_daily_source_links_candidate_group_idx ON private.candidate_daily_source_links USING btree (environment, candidate_id, source_system, link_group_id);

-- private.candidate_daily_source_links_history_hmac_uq
CREATE UNIQUE INDEX candidate_daily_source_links_history_hmac_uq ON private.candidate_daily_source_links USING btree (environment, source_system, hmac_key_version, identifier_hmac);

-- private.candidate_daily_source_links_primary_group_uq
CREATE UNIQUE INDEX candidate_daily_source_links_primary_group_uq ON private.candidate_daily_source_links USING btree (environment, candidate_id, source_system) WHERE (state = 'PRIMARY'::text);

-- private.idx_bpay_wb_economic_builds_candidate_complete_recent_v1
CREATE INDEX idx_bpay_wb_economic_builds_candidate_complete_recent_v1 ON private.banking_pay_workbench_economic_builds USING btree (candidate_id, completed_at_utc DESC, id DESC) WHERE ((status = 'COMPLETE'::text) AND (private_stage = 'COMPLETE'::text) AND (completed_at_utc IS NOT NULL));

-- private.idx_invoice_async_snapshot_hmac_keys_one_current
CREATE UNIQUE INDEX idx_invoice_async_snapshot_hmac_keys_one_current ON private.invoice_async_snapshot_hmac_keys USING btree (is_current) WHERE is_current;

-- private.uq_bpay_wb_economic_build_authority_v1
CREATE UNIQUE INDEX uq_bpay_wb_economic_build_authority_v1 ON private.banking_pay_workbench_economic_builds USING btree (candidate_id, authority_fingerprint_version, authority_fingerprint) WHERE ((authority_fingerprint IS NOT NULL) AND (status <> ALL (ARRAY['FAILED'::text, 'OBSOLETE'::text, 'CLEANING'::text])));

-- public.abm_cand_client_contract_target_uq
CREATE UNIQUE INDEX abm_cand_client_contract_target_uq ON public.assignment_band_mappings USING btree (system_type, incoming_code, candidate_id, client_id) WHERE ((active = true) AND (target_contract_id IS NOT NULL));

-- public.abm_cand_client_idx
CREATE INDEX abm_cand_client_idx ON public.assignment_band_mappings USING btree (system_type, incoming_code, candidate_id, client_id) WHERE ((active = true) AND (candidate_id IS NOT NULL) AND (client_id IS NOT NULL));

-- public.abm_candidate_idx
CREATE INDEX abm_candidate_idx ON public.assignment_band_mappings USING btree (system_type, incoming_code, candidate_id) WHERE ((active = true) AND (candidate_id IS NOT NULL));

-- public.abm_client_idx
CREATE INDEX abm_client_idx ON public.assignment_band_mappings USING btree (system_type, incoming_code, client_id) WHERE ((active = true) AND (candidate_id IS NULL) AND (client_id IS NOT NULL));

-- public.abm_global_idx
CREATE INDEX abm_global_idx ON public.assignment_band_mappings USING btree (system_type, incoming_code) WHERE ((active = true) AND (candidate_id IS NULL) AND (client_id IS NULL));

-- public.bank_provider_webhook_configs_last_event_idx
CREATE INDEX bank_provider_webhook_configs_last_event_idx ON public.bank_provider_webhook_configs USING btree (last_event_at_utc DESC) WHERE (last_event_at_utc IS NOT NULL);

-- public.bank_provider_webhook_configs_provider_webhook_uidx
CREATE UNIQUE INDEX bank_provider_webhook_configs_provider_webhook_uidx ON public.bank_provider_webhook_configs USING btree (provider_key, rail_env, provider_webhook_id) WHERE (provider_webhook_id IS NOT NULL);

-- public.bank_provider_webhook_configs_public_uidx
CREATE UNIQUE INDEX bank_provider_webhook_configs_public_uidx ON public.bank_provider_webhook_configs USING btree (provider_key, rail_env, webhook_public_id);

-- public.bank_provider_webhook_configs_status_idx
CREATE INDEX bank_provider_webhook_configs_status_idx ON public.bank_provider_webhook_configs USING btree (provider_key, rail_env, status);

-- public.bank_provider_webhook_receipts_config_received_idx
CREATE INDEX bank_provider_webhook_receipts_config_received_idx ON public.bank_provider_webhook_receipts USING btree (webhook_config_id, request_received_at_utc DESC) WHERE (webhook_config_id IS NOT NULL);

-- public.bank_provider_webhook_receipts_event_key_uidx
CREATE UNIQUE INDEX bank_provider_webhook_receipts_event_key_uidx ON public.bank_provider_webhook_receipts USING btree (provider_key, rail_env, provider_event_key);

-- public.bank_provider_webhook_receipts_request_idx
CREATE INDEX bank_provider_webhook_receipts_request_idx ON public.bank_provider_webhook_receipts USING btree (provider_request_id) WHERE (provider_request_id IS NOT NULL);

-- public.bank_provider_webhook_receipts_status_idx
CREATE INDEX bank_provider_webhook_receipts_status_idx ON public.bank_provider_webhook_receipts USING btree (provider_key, rail_env, status);

-- public.bank_provider_webhook_receipts_transaction_idx
CREATE INDEX bank_provider_webhook_receipts_transaction_idx ON public.bank_provider_webhook_receipts USING btree (provider_transaction_id) WHERE (provider_transaction_id IS NOT NULL);

-- public.bank_provider_webhook_receipts_unmatched_idx
CREATE INDEX bank_provider_webhook_receipts_unmatched_idx ON public.bank_provider_webhook_receipts USING btree (provider_key, rail_env, request_received_at_utc DESC) WHERE (status = 'UNMATCHED_REVIEW_REQUIRED'::text);

-- public.banking_alert_user_preferences_enabled_idx
CREATE INDEX banking_alert_user_preferences_enabled_idx ON public.banking_alert_user_preferences USING btree (enabled);

-- public.banking_alert_user_preferences_snoozed_idx
CREATE INDEX banking_alert_user_preferences_snoozed_idx ON public.banking_alert_user_preferences USING btree (snoozed_until_utc) WHERE (snoozed_until_utc IS NOT NULL);

-- public.banking_alert_user_preferences_user_uidx
CREATE UNIQUE INDEX banking_alert_user_preferences_user_uidx ON public.banking_alert_user_preferences USING btree (user_id);

-- public.banking_pay_batch_change_signals_changed_idx
CREATE INDEX banking_pay_batch_change_signals_changed_idx ON public.banking_pay_batch_change_signals USING btree (last_changed_at_utc);

-- public.banking_pay_batch_change_signals_updated_idx
CREATE INDEX banking_pay_batch_change_signals_updated_idx ON public.banking_pay_batch_change_signals USING btree (updated_at_utc);

-- public.bpay_advances_candidate_open_timesheet_idx
CREATE INDEX bpay_advances_candidate_open_timesheet_idx ON public.pay_advances USING btree (candidate_id, linked_timesheet_id, id) WHERE ((status = 'ACTIVE'::pay_advance_status_enum) AND (linked_timesheet_id IS NOT NULL) AND (cleared_at_utc IS NULL) AND (written_off_at_utc IS NULL));

-- public.bpay_finance_components_candidate_open_timesheet_idx
CREATE INDEX bpay_finance_components_candidate_open_timesheet_idx ON public.pay_finance_case_components USING btree (candidate_id, linked_timesheet_id, finance_case_id, id) WHERE ((linked_timesheet_id IS NOT NULL) AND (closed_at_utc IS NULL));

-- public.bpay_timesheet_overrides_candidate_active_idx
CREATE INDEX bpay_timesheet_overrides_candidate_active_idx ON public.timesheet_payment_overrides USING btree (candidate_id, timesheet_id, id) WHERE ((timesheet_id IS NOT NULL) AND (cleared_at_utc IS NULL) AND (consumed_at_utc IS NULL) AND (consumed_by_pay_batch_id IS NULL));

-- public.bpay_ts_adjustments_candidate_unpaid_idx
CREATE INDEX bpay_ts_adjustments_candidate_unpaid_idx ON public.ts_pay_adjustments USING btree (candidate_id, timesheet_id, id) WHERE ((timesheet_id IS NOT NULL) AND (paid_at_utc IS NULL));

-- public.bpay_wb_jobs_economic_build_idx
CREATE INDEX bpay_wb_jobs_economic_build_idx ON public.banking_pay_workbench_jobs USING btree (economic_build_id, status, id) WHERE (economic_build_id IS NOT NULL);

-- public.bpay_wb_jobs_source_claim_idx
CREATE INDEX bpay_wb_jobs_source_claim_idx ON public.banking_pay_workbench_jobs USING btree (run_at_utc, priority, created_at_utc, id) INCLUDE (candidate_id, session_id, economic_build_id, private_stage, attempt_count, max_attempts) WHERE ((status = 'QUEUED'::text) AND (job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'::text));

-- public.candidate_app_accounts_status_idx
CREATE INDEX candidate_app_accounts_status_idx ON public.candidate_app_accounts USING btree (environment, status, email_normalized);

-- public.candidate_app_global_membership_links_account_idx
CREATE INDEX candidate_app_global_membership_links_account_idx ON public.candidate_app_global_membership_links USING btree (account_id);

-- public.candidate_app_global_membership_links_candidate_state_idx
CREATE INDEX candidate_app_global_membership_links_candidate_state_idx ON public.candidate_app_global_membership_links USING btree (candidate_id, state, membership_generation);

-- public.candidate_app_sessions_account_status_idx
CREATE INDEX candidate_app_sessions_account_status_idx ON public.candidate_app_sessions USING btree (account_id, status, expires_at_utc);

-- public.candidate_app_sessions_control_plane_context_idx
CREATE INDEX candidate_app_sessions_control_plane_context_idx ON public.candidate_app_sessions USING btree (membership_id, membership_generation, route_version, session_epoch, expires_at_utc) WHERE (auth_source = 'CONTROL_PLANE'::text);

-- public.candidate_app_sessions_control_plane_epoch_uq
CREATE UNIQUE INDEX candidate_app_sessions_control_plane_epoch_uq ON public.candidate_app_sessions USING btree (membership_id, session_epoch) WHERE ((auth_source = 'CONTROL_PLANE'::text) AND (status = 'ACTIVE'::text));

-- public.candidate_app_sessions_family_rotation_idx
CREATE INDEX candidate_app_sessions_family_rotation_idx ON public.candidate_app_sessions USING btree (token_family_id, rotation DESC);

-- public.candidate_approval_requests_one_live_uq
CREATE UNIQUE INDEX candidate_approval_requests_one_live_uq ON public.candidate_approval_requests USING btree (workflow_id, workflow_generation) WHERE (state = 'PENDING'::text);

-- public.candidate_approval_requests_reminder_idx
CREATE INDEX candidate_approval_requests_reminder_idx ON public.candidate_approval_requests USING btree (next_reminder_at_utc, id) WHERE ((state = 'PENDING'::text) AND (method = 'EMAIL'::text));

-- public.candidate_approval_requests_review_timesheet_idx
CREATE INDEX candidate_approval_requests_review_timesheet_idx ON public.candidate_approval_requests USING btree (manager_review_timesheet_component_id) WHERE (manager_review_timesheet_component_id IS NOT NULL);

-- public.candidate_approval_requests_token_hash_uq
CREATE UNIQUE INDEX candidate_approval_requests_token_hash_uq ON public.candidate_approval_requests USING btree (token_hash) WHERE (token_hash IS NOT NULL);

-- public.candidate_auth_challenges_email_state_idx
CREATE INDEX candidate_auth_challenges_email_state_idx ON public.candidate_auth_challenges USING btree (environment, email_normalized, purpose, state, created_at_utc DESC);

-- public.candidate_auth_challenges_one_pending_uq
CREATE UNIQUE INDEX candidate_auth_challenges_one_pending_uq ON public.candidate_auth_challenges USING btree (environment, email_normalized, purpose) WHERE (state = ANY (ARRAY['PENDING'::text, 'VERIFIED'::text]));

-- public.candidate_daily_availability_days_candidate_version_idx
CREATE INDEX candidate_daily_availability_days_candidate_version_idx ON public.candidate_daily_availability_days USING btree (environment, candidate_id, availability_version, availability_date);

-- public.candidate_daily_command_receipts_candidate_state_idx
CREATE INDEX candidate_daily_command_receipts_candidate_state_idx ON public.candidate_daily_command_receipts USING btree (environment, candidate_id, state, created_at_utc);

-- public.candidate_daily_command_receipts_source_event_uq
CREATE UNIQUE INDEX candidate_daily_command_receipts_source_event_uq ON public.candidate_daily_command_receipts USING btree (environment, source_system, source_event_id, item_key) WHERE (source_system IS NOT NULL);

-- public.candidate_daily_projection_candidate_cursor_idx
CREATE INDEX candidate_daily_projection_candidate_cursor_idx ON public.candidate_daily_sheet_projection_outbox USING btree (environment, candidate_id, availability_version, state);

-- public.candidate_daily_projection_claim_ready_idx
CREATE INDEX candidate_daily_projection_claim_ready_idx ON public.candidate_daily_sheet_projection_outbox USING btree (target, state, next_available_at_utc, created_at_utc) WHERE (state = ANY (ARRAY['PENDING'::text, 'RETRY'::text]));

-- public.candidate_daily_rota_days_candidate_date_idx
CREATE INDEX candidate_daily_rota_days_candidate_date_idx ON public.candidate_daily_rota_days USING btree (environment, candidate_id, rota_date);

-- public.candidate_daily_rota_generations_active_uq
CREATE UNIQUE INDEX candidate_daily_rota_generations_active_uq ON public.candidate_daily_rota_generations USING btree (environment, candidate_id) WHERE (state = 'ACTIVE'::text);

-- public.candidate_daily_rota_generations_candidate_window_idx
CREATE INDEX candidate_daily_rota_generations_candidate_window_idx ON public.candidate_daily_rota_generations USING btree (environment, candidate_id, window_start, window_end, state);

-- public.candidate_job_titles_primary_idx
CREATE UNIQUE INDEX candidate_job_titles_primary_idx ON public.candidate_job_titles USING btree (candidate_id) WHERE (is_primary = true);

-- public.candidate_notifications_account_page_idx
CREATE INDEX candidate_notifications_account_page_idx ON public.candidate_notifications USING btree (account_id, state, created_at_utc DESC, id DESC);

-- public.candidate_notifications_push_claim_idx
CREATE INDEX candidate_notifications_push_claim_idx ON public.candidate_notifications USING btree (push_state, created_at_utc, id) WHERE (push_state = ANY (ARRAY['PENDING'::text, 'FAILED'::text]));

-- public.candidate_submission_components_approval_request_idx
CREATE INDEX candidate_submission_components_approval_request_idx ON public.candidate_submission_components USING btree (approval_request_id) WHERE (approval_request_id IS NOT NULL);

-- public.candidate_submission_components_final_storage_uq
CREATE UNIQUE INDEX candidate_submission_components_final_storage_uq ON public.candidate_submission_components USING btree (final_signed_storage_key) WHERE (final_signed_storage_key IS NOT NULL);

-- public.candidate_submission_components_hours_review_uq
CREATE UNIQUE INDEX candidate_submission_components_hours_review_uq ON public.candidate_submission_components USING btree (workflow_id, workflow_generation) WHERE ((component_kind = 'HOURS_TIMESHEET'::text) AND (state <> 'SUPERSEDED'::text));

-- public.candidate_submission_components_live_manager_signature_uq
CREATE UNIQUE INDEX candidate_submission_components_live_manager_signature_uq ON public.candidate_submission_components USING btree (approval_request_id) WHERE ((component_kind = 'MANAGER_SIGNATURE'::text) AND (state <> 'SUPERSEDED'::text) AND (approval_request_id IS NOT NULL));

-- public.candidate_submission_components_paper_return_page_uq
CREATE UNIQUE INDEX candidate_submission_components_paper_return_page_uq ON public.candidate_submission_components USING btree (workflow_id, workflow_generation, paper_return_page_key) WHERE ((component_kind = 'SIGNED_RETURN'::text) AND (state <> 'SUPERSEDED'::text));

-- public.candidate_submission_components_required_ordinal_uq
CREATE UNIQUE INDEX candidate_submission_components_required_ordinal_uq ON public.candidate_submission_components USING btree (workflow_id, workflow_generation, review_ordinal) WHERE ((required = true) AND (review_ordinal IS NOT NULL) AND (state <> 'SUPERSEDED'::text));

-- public.candidate_submission_components_review_storage_uq
CREATE UNIQUE INDEX candidate_submission_components_review_storage_uq ON public.candidate_submission_components USING btree (review_storage_key) WHERE (review_storage_key IS NOT NULL);

-- public.candidate_submission_components_source_sha256_uq
CREATE UNIQUE INDEX candidate_submission_components_source_sha256_uq ON public.candidate_submission_components USING btree (source_content_sha256) WHERE ((source_content_sha256 IS NOT NULL) AND (source_component_id IS NULL) AND (component_kind = ANY (ARRAY['MILEAGE_FORM'::text, 'EXPENSE_EVIDENCE'::text, 'SIGNED_RETURN'::text])));

-- public.candidate_submission_components_storage_key_uq
CREATE UNIQUE INDEX candidate_submission_components_storage_key_uq ON public.candidate_submission_components USING btree (storage_key) WHERE ((storage_key IS NOT NULL) AND (source_component_id IS NULL));

-- public.candidate_submission_components_upload_idempotency_uq
CREATE UNIQUE INDEX candidate_submission_components_upload_idempotency_uq ON public.candidate_submission_components USING btree (workflow_id, upload_idempotency_key) WHERE (upload_idempotency_key IS NOT NULL);

-- public.candidate_submission_workflows_candidate_signature_idx
CREATE INDEX candidate_submission_workflows_candidate_signature_idx ON public.candidate_submission_workflows USING btree (candidate_signature_component_id) WHERE (candidate_signature_component_id IS NOT NULL);

-- public.candidate_submission_workflows_candidate_state_idx
CREATE INDEX candidate_submission_workflows_candidate_state_idx ON public.candidate_submission_workflows USING btree (candidate_id, state, week_ending_date DESC, created_at_utc DESC);

-- public.candidate_submission_workflows_manager_signature_idx
CREATE INDEX candidate_submission_workflows_manager_signature_idx ON public.candidate_submission_workflows USING btree (manager_signature_component_id) WHERE (manager_signature_component_id IS NOT NULL);

-- public.candidate_submission_workflows_one_active_expense_uq
CREATE UNIQUE INDEX candidate_submission_workflows_one_active_expense_uq ON public.candidate_submission_workflows USING btree (candidate_id, contract_id, week_ending_date) WHERE ((workflow_kind = ANY (ARRAY['CONTRACT_EXPENSE'::text, 'CONTRACT_COMBINED'::text])) AND (state = ANY (ARRAY['CREATED'::text, 'WORKER_DRAFT'::text, 'WORKER_SUBMITTED'::text, 'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT'::text, 'READY_FOR_MANAGER_APPROVAL'::text, 'AWAITING_MANAGER_APPROVAL'::text, 'MANAGER_APPROVED'::text, 'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT'::text, 'READY_TO_FINALISE'::text, 'AWAITING_PAPER_RETURN'::text, 'RECEIVED'::text])));

-- public.candidate_submission_workflows_replacement_source_uq
CREATE UNIQUE INDEX candidate_submission_workflows_replacement_source_uq ON public.candidate_submission_workflows USING btree (replacement_of_workflow_id) WHERE (replacement_of_workflow_id IS NOT NULL);

-- public.candidate_submission_workflows_timesheet_idx
CREATE INDEX candidate_submission_workflows_timesheet_idx ON public.candidate_submission_workflows USING btree (target_timesheet_id, state);

-- public.candidates_active_normalized_cid1_uq
CREATE UNIQUE INDEX candidates_active_normalized_cid1_uq ON public.candidates USING btree (upper(btrim(key_norm))) WHERE ((active IS TRUE) AND (key_norm IS NOT NULL) AND (upper(btrim(key_norm)) ~ '^CID1-[0-9A-HJKMNP-TV-Z]{5,160}$'::text));

-- public.default_job_titles_parent_label_idx
CREATE UNIQUE INDEX default_job_titles_parent_label_idx ON public.default_job_titles USING btree (COALESCE(parent_id, '00000000-0000-0000-0000-000000000000'::uuid), lower(label));

-- public.hr_daily_grade_role_mappings_client_id_idx
CREATE INDEX hr_daily_grade_role_mappings_client_id_idx ON public.hr_daily_grade_role_mappings USING btree (client_id);

-- public.hr_daily_grade_role_mappings_uq_active
CREATE UNIQUE INDEX hr_daily_grade_role_mappings_uq_active ON public.hr_daily_grade_role_mappings USING btree (client_id, incoming_grade_norm) WHERE (active = true);

-- public.hr_imports_coverage_operation_key_uidx
CREATE UNIQUE INDEX hr_imports_coverage_operation_key_uidx ON public.hr_imports USING btree (coverage_operation_key) WHERE (coverage_operation_key IS NOT NULL);

-- public.hr_imports_revision_group_revision_uidx
CREATE UNIQUE INDEX hr_imports_revision_group_revision_uidx ON public.hr_imports USING btree (revision_group_id, revision_no) WHERE ((revision_group_id IS NOT NULL) AND (revision_no IS NOT NULL));

-- public.hr_imports_supersedes_import_id_idx
CREATE INDEX hr_imports_supersedes_import_id_idx ON public.hr_imports USING btree (supersedes_import_id);

-- public.hr_issue_email_deliveries_import_idx
CREATE INDEX hr_issue_email_deliveries_import_idx ON public.hr_issue_email_deliveries USING btree (import_id, status, id);

-- public.hr_issue_email_deliveries_operation_idx
CREATE INDEX hr_issue_email_deliveries_operation_idx ON public.hr_issue_email_deliveries USING btree (operation_id);

-- public.hr_issue_email_deliveries_outbox_idx
CREATE INDEX hr_issue_email_deliveries_outbox_idx ON public.hr_issue_email_deliveries USING btree (mail_outbox_id) WHERE (mail_outbox_id IS NOT NULL);

-- public.hr_issue_email_delivery_items_action_idx
CREATE INDEX hr_issue_email_delivery_items_action_idx ON public.hr_issue_email_delivery_items USING btree (action_id);

-- public.hr_issue_email_delivery_items_issue_idx
CREATE INDEX hr_issue_email_delivery_items_issue_idx ON public.hr_issue_email_delivery_items USING btree (issue_id);

-- public.hr_issue_emails_contract_idx
CREATE INDEX hr_issue_emails_contract_idx ON public.hr_issue_emails USING btree (contract_id) WHERE (contract_id IS NOT NULL);

-- public.hr_issue_emails_hr_row_id_idx
CREATE INDEX hr_issue_emails_hr_row_id_idx ON public.hr_issue_emails USING btree (hr_row_id);

-- public.hr_issue_emails_issue_fingerprint_key
CREATE UNIQUE INDEX hr_issue_emails_issue_fingerprint_key ON public.hr_issue_emails USING btree (issue_fingerprint);

-- public.hr_issue_emails_recipient_scope_idx
CREATE INDEX hr_issue_emails_recipient_scope_idx ON public.hr_issue_emails USING btree (recipient_scope_key, issue_fingerprint) WHERE (recipient_scope_key IS NOT NULL);

-- public.hr_issue_emails_timesheet_id_idx
CREATE INDEX hr_issue_emails_timesheet_id_idx ON public.hr_issue_emails USING btree (timesheet_id);

-- public.hr_rows_import_resolved_timesheet_idx
CREATE INDEX hr_rows_import_resolved_timesheet_idx ON public.hr_rows USING btree (import_id, ((payload_json ->> 'resolved_timesheet_id'::text)));

-- public.ica_lookup_idx
CREATE INDEX ica_lookup_idx ON public.import_column_aliases USING btree (system_type, field_key, alias_name) WHERE (active = true);

-- public.idx_app_change_counters_pay_candidate_scope_generation
CREATE INDEX idx_app_change_counters_pay_candidate_scope_generation ON public.app_change_counters USING btree (scope_change_generation, entity_key) WHERE ((scope_change_generation IS NOT NULL) AND (entity_key ~~ 'pay_candidate:%'::text));

-- public.idx_audit_actor
CREATE INDEX idx_audit_actor ON public.audit_events USING btree (actor_user_id, ts_utc DESC);

-- public.idx_audit_candidate_auth_mutation_receipt_v1
CREATE INDEX idx_audit_candidate_auth_mutation_receipt_v1 ON public.audit_events USING btree (object_type, object_id_text, correlation_id, ts_utc DESC, id DESC) WHERE (object_type = 'candidate_auth_mutation_receipt'::text);

-- public.idx_audit_candidate_workflow_mutation_receipt_v1
CREATE INDEX idx_audit_candidate_workflow_mutation_receipt_v1 ON public.audit_events USING btree (object_type, object_id_text, correlation_id, ts_utc DESC, id DESC) WHERE (object_type = 'candidate_workflow_mutation_receipt'::text);

-- public.idx_audit_import_correction_source_v1
CREATE INDEX idx_audit_import_correction_source_v1 ON public.audit_events USING btree (((after_json ->> 'shift_id'::text)), ((after_json ->> 'external_row_key'::text)), ((after_json ->> 'correction_id'::text)), action, ts_utc DESC, id) WHERE ((object_type = ANY (ARRAY['timesheets'::text, 'invoices'::text, 'contract_weeks'::text])) AND (action = ANY (ARRAY['NHSP_IMPORT_CORRECTION_APPLIED'::text, 'HR_IMPORT_CORRECTION_APPLIED'::text])));

-- public.idx_audit_lookup
CREATE INDEX idx_audit_lookup ON public.audit_events USING btree (object_type, object_id_text, ts_utc DESC);

-- public.idx_banking_alert_ack_entity
CREATE INDEX idx_banking_alert_ack_entity ON public.banking_alert_acknowledgements USING btree (entity_kind, entity_id, acknowledged_at_utc DESC);

-- public.idx_banking_alert_ack_kind
CREATE INDEX idx_banking_alert_ack_kind ON public.banking_alert_acknowledgements USING btree (alert_kind, acknowledged_at_utc DESC);

-- public.idx_banking_alert_ack_user_scope
CREATE INDEX idx_banking_alert_ack_user_scope ON public.banking_alert_acknowledgements USING btree (acknowledged_by_user_id, acknowledge_scope, acknowledged_at_utc DESC);

-- public.idx_banking_alert_display_summary_updated
CREATE INDEX idx_banking_alert_display_summary_updated ON public.banking_alert_display_summary USING btree (updated_at_utc);

-- public.idx_banking_alert_success_events_active
CREATE INDEX idx_banking_alert_success_events_active ON public.banking_alert_success_events USING btree (expires_at_utc, occurred_at_utc DESC);

-- public.idx_banking_alert_success_events_batch
CREATE INDEX idx_banking_alert_success_events_batch ON public.banking_alert_success_events USING btree (pay_batch_id, alert_kind, occurred_at_utc DESC);

-- public.idx_banking_pay_operation_candidate_allocation_batch_candidate
CREATE INDEX idx_banking_pay_operation_candidate_allocation_batch_candidate ON public.banking_pay_operation_candidate_allocation_rows USING btree (pay_batch_id, candidate_id);

-- public.idx_banking_pay_operation_candidate_allocation_component
CREATE INDEX idx_banking_pay_operation_candidate_allocation_component ON public.banking_pay_operation_candidate_allocation_rows USING btree (finance_component_id) WHERE (finance_component_id IS NOT NULL);

-- public.idx_banking_pay_operation_candidate_allocation_finance_case
CREATE INDEX idx_banking_pay_operation_candidate_allocation_finance_case ON public.banking_pay_operation_candidate_allocation_rows USING btree (finance_case_id) WHERE (finance_case_id IS NOT NULL);

-- public.idx_banking_pay_operation_candidate_allocation_operation_candid
CREATE INDEX idx_banking_pay_operation_candidate_allocation_operation_candid ON public.banking_pay_operation_candidate_allocation_rows USING btree (operation_id, candidate_id);

-- public.idx_banking_pay_operation_candidate_allocation_scope
CREATE INDEX idx_banking_pay_operation_candidate_allocation_scope ON public.banking_pay_operation_candidate_allocation_rows USING btree (candidate_scope_id);

-- public.idx_banking_pay_operation_candidate_scope_batch_candidate
CREATE INDEX idx_banking_pay_operation_candidate_scope_batch_candidate ON public.banking_pay_operation_candidate_scope USING btree (pay_batch_id, candidate_id);

-- public.idx_banking_pay_operation_candidate_scope_operation_candidate
CREATE INDEX idx_banking_pay_operation_candidate_scope_operation_candidate ON public.banking_pay_operation_candidate_scope USING btree (operation_id, candidate_id);

-- public.idx_banking_pay_operation_candidate_scope_operation_channel_sta
CREATE INDEX idx_banking_pay_operation_candidate_scope_operation_channel_sta ON public.banking_pay_operation_candidate_scope USING btree (operation_id, pay_channel, status);

-- public.idx_banking_pay_operation_candidate_scope_operation_status
CREATE INDEX idx_banking_pay_operation_candidate_scope_operation_status ON public.banking_pay_operation_candidate_scope USING btree (operation_id, status);

-- public.idx_banking_pay_operation_candidate_scope_workbench_candidate
CREATE INDEX idx_banking_pay_operation_candidate_scope_workbench_candidate ON public.banking_pay_operation_candidate_scope USING btree (workbench_session_id, candidate_id);

-- public.idx_banking_pay_operation_chunks_freshness_claim
CREATE INDEX idx_banking_pay_operation_chunks_freshness_claim ON public.banking_pay_operation_chunks USING btree (operation_id, phase, chunk_type, status, sequence_no);

-- public.idx_banking_pay_operation_chunks_operation_phase_status
CREATE INDEX idx_banking_pay_operation_chunks_operation_phase_status ON public.banking_pay_operation_chunks USING btree (operation_id, phase, status);

-- public.idx_banking_pay_operation_chunks_operation_phase_type_status_lo
CREATE INDEX idx_banking_pay_operation_chunks_operation_phase_type_status_lo ON public.banking_pay_operation_chunks USING btree (operation_id, phase, chunk_type, status, lock_expires_at_utc);

-- public.idx_banking_pay_operation_chunks_operation_sequence
CREATE INDEX idx_banking_pay_operation_chunks_operation_sequence ON public.banking_pay_operation_chunks USING btree (operation_id, sequence_no);

-- public.idx_banking_pay_operation_chunks_operation_status
CREATE INDEX idx_banking_pay_operation_chunks_operation_status ON public.banking_pay_operation_chunks USING btree (operation_id, status);

-- public.idx_banking_pay_operation_chunks_status_lock_expiry
CREATE INDEX idx_banking_pay_operation_chunks_status_lock_expiry ON public.banking_pay_operation_chunks USING btree (status, lock_expires_at_utc);

-- public.idx_banking_pay_operation_remittance_scope_batch_status
CREATE INDEX idx_banking_pay_operation_remittance_scope_batch_status ON public.banking_pay_operation_remittance_scope USING btree (pay_batch_id, status);

-- public.idx_banking_pay_operation_remittance_scope_operation_status
CREATE INDEX idx_banking_pay_operation_remittance_scope_operation_status ON public.banking_pay_operation_remittance_scope USING btree (operation_id, status);

-- public.idx_banking_pay_operation_settlement_scope_batch_status
CREATE INDEX idx_banking_pay_operation_settlement_scope_batch_status ON public.banking_pay_operation_settlement_scope USING btree (pay_batch_id, status);

-- public.idx_banking_pay_operation_settlement_scope_operation_status
CREATE INDEX idx_banking_pay_operation_settlement_scope_operation_status ON public.banking_pay_operation_settlement_scope USING btree (operation_id, status);

-- public.idx_banking_pay_operation_transfer_scope_batch_status
CREATE INDEX idx_banking_pay_operation_transfer_scope_batch_status ON public.banking_pay_operation_transfer_scope USING btree (pay_batch_id, status);

-- public.idx_banking_pay_operation_transfer_scope_operation_batch_status
CREATE INDEX idx_banking_pay_operation_transfer_scope_operation_batch_status ON public.banking_pay_operation_transfer_scope USING btree (operation_id, pay_batch_id, status, pay_channel, pay_bank_transfer_id);

-- public.idx_banking_pay_operation_transfer_scope_operation_batch_transf
CREATE INDEX idx_banking_pay_operation_transfer_scope_operation_batch_transf ON public.banking_pay_operation_transfer_scope USING btree (operation_id, pay_batch_id, pay_bank_transfer_id) WHERE (pay_bank_transfer_id IS NOT NULL);

-- public.idx_banking_pay_operation_transfer_scope_operation_status
CREATE INDEX idx_banking_pay_operation_transfer_scope_operation_status ON public.banking_pay_operation_transfer_scope USING btree (operation_id, status);

-- public.idx_banking_pay_operations_actor_status
CREATE INDEX idx_banking_pay_operations_actor_status ON public.banking_pay_operations USING btree (actor_user_id, status);

-- public.idx_banking_pay_operations_idempotency_key
CREATE INDEX idx_banking_pay_operations_idempotency_key ON public.banking_pay_operations USING btree (idempotency_key);

-- public.idx_banking_pay_operations_pay_batch
CREATE INDEX idx_banking_pay_operations_pay_batch ON public.banking_pay_operations USING btree (pay_batch_id);

-- public.idx_banking_pay_operations_root_operation
CREATE INDEX idx_banking_pay_operations_root_operation ON public.banking_pay_operations USING btree (root_operation_id);

-- public.idx_banking_pay_operations_status_lock_expiry
CREATE INDEX idx_banking_pay_operations_status_lock_expiry ON public.banking_pay_operations USING btree (status, lock_expires_at_utc);

-- public.idx_banking_pay_operations_type_status
CREATE INDEX idx_banking_pay_operations_type_status ON public.banking_pay_operations USING btree (operation_type, status);

-- public.idx_banking_pay_operations_workbench_session
CREATE INDEX idx_banking_pay_operations_workbench_session ON public.banking_pay_operations USING btree (workbench_session_id);

-- public.idx_banking_pay_workbench_candidate_state_session_candidate
CREATE INDEX idx_banking_pay_workbench_candidate_state_session_candidate ON public.banking_pay_workbench_session_candidate_state USING btree (session_id, candidate_id);

-- public.idx_banking_pay_workbench_candidate_state_session_status
CREATE INDEX idx_banking_pay_workbench_candidate_state_session_status ON public.banking_pay_workbench_session_candidate_state USING btree (session_id, status);

-- public.idx_bpay_case_resolution_carry_candidate
CREATE INDEX idx_bpay_case_resolution_carry_candidate ON public.banking_pay_workbench_case_resolution_carry_registrations USING btree (candidate_id, status, target_session_id);

-- public.idx_bpay_case_resolution_carry_processing
CREATE INDEX idx_bpay_case_resolution_carry_processing ON public.banking_pay_workbench_case_resolution_carry_registrations USING btree (target_session_id, candidate_id, status, source_priority, created_at_utc);

-- public.idx_bpay_case_resolution_carry_source_resolution
CREATE INDEX idx_bpay_case_resolution_carry_source_resolution ON public.banking_pay_workbench_case_resolution_carry_registrations USING btree (source_resolution_id);

-- public.idx_bpay_case_resolution_carry_source_session
CREATE INDEX idx_bpay_case_resolution_carry_source_session ON public.banking_pay_workbench_case_resolution_carry_registrations USING btree (source_session_id, status, created_at_utc);

-- public.idx_bpay_case_resolution_carry_target_resolution
CREATE INDEX idx_bpay_case_resolution_carry_target_resolution ON public.banking_pay_workbench_case_resolution_carry_registrations USING btree (target_resolution_id) WHERE (target_resolution_id IS NOT NULL);

-- public.idx_bpay_delta_runs_session_candidate_status
CREATE INDEX idx_bpay_delta_runs_session_candidate_status ON public.banking_pay_workbench_candidate_delta_projection_runs USING btree (session_id, candidate_id, status, phase);

-- public.idx_bpay_delta_runs_session_seq
CREATE INDEX idx_bpay_delta_runs_session_seq ON public.banking_pay_workbench_candidate_delta_projection_runs USING btree (session_id, candidate_id, session_version, source_change_seq);

-- public.idx_bpay_delta_runs_status_updated
CREATE INDEX idx_bpay_delta_runs_status_updated ON public.banking_pay_workbench_candidate_delta_projection_runs USING btree (status, updated_at_utc);

-- public.idx_bpay_op_scope_units_chunk
CREATE INDEX idx_bpay_op_scope_units_chunk ON public.banking_pay_operation_scope_units USING btree (operation_id, phase, chunk_id);

-- public.idx_bpay_op_scope_units_claim
CREATE INDEX idx_bpay_op_scope_units_claim ON public.banking_pay_operation_scope_units USING btree (operation_id, phase, unit_type, status, unit_ordinal);

-- public.idx_bpay_op_transfer_items_batch_rollup
CREATE INDEX idx_bpay_op_transfer_items_batch_rollup ON public.banking_pay_operation_transfer_scope_items USING btree (operation_id, pay_batch_id, rollup_status);

-- public.idx_bpay_op_transfer_items_rollup
CREATE INDEX idx_bpay_op_transfer_items_rollup ON public.banking_pay_operation_transfer_scope_items USING btree (operation_id, transfer_scope_id, rollup_status, item_ordinal);

-- public.idx_bpay_operations_batch_type_status
CREATE INDEX idx_bpay_operations_batch_type_status ON public.banking_pay_operations USING btree (pay_batch_id, operation_type, status);

-- public.idx_bpay_operations_lease_expires
CREATE INDEX idx_bpay_operations_lease_expires ON public.banking_pay_operations USING btree (lease_expires_at_utc);

-- public.idx_bpay_operations_status_run_after
CREATE INDEX idx_bpay_operations_status_run_after ON public.banking_pay_operations USING btree (status, run_after_utc);

-- public.idx_bpay_operations_type_status_run_after
CREATE INDEX idx_bpay_operations_type_status_run_after ON public.banking_pay_operations USING btree (operation_type, status, run_after_utc);

-- public.idx_bpay_provider_attempts_idempotency
CREATE INDEX idx_bpay_provider_attempts_idempotency ON public.banking_pay_operation_provider_attempts USING btree (provider_idempotency_key) WHERE (provider_idempotency_key IS NOT NULL);

-- public.idx_bpay_provider_attempts_operation_created
CREATE INDEX idx_bpay_provider_attempts_operation_created ON public.banking_pay_operation_provider_attempts USING btree (operation_id, created_at_utc DESC);

-- public.idx_bpay_provider_attempts_request
CREATE INDEX idx_bpay_provider_attempts_request ON public.banking_pay_operation_provider_attempts USING btree (provider_request_id) WHERE (provider_request_id IS NOT NULL);

-- public.idx_bpay_provider_attempts_transaction
CREATE INDEX idx_bpay_provider_attempts_transaction ON public.banking_pay_operation_provider_attempts USING btree (provider_transaction_id) WHERE (provider_transaction_id IS NOT NULL);

-- public.idx_bpay_provider_attempts_transfer_created
CREATE INDEX idx_bpay_provider_attempts_transfer_created ON public.banking_pay_operation_provider_attempts USING btree (transfer_scope_id, created_at_utc DESC);

-- public.idx_bpay_scope_change_transactions_cleanup
CREATE INDEX idx_bpay_scope_change_transactions_cleanup ON public.banking_pay_scope_change_transactions USING btree (state, finalized_at_utc, tx_token) WHERE (state = ANY (ARRAY['FINALIZED'::text, 'NOOP'::text]));

-- public.idx_bpay_selection_carry_pending_target_candidate_key
CREATE INDEX idx_bpay_selection_carry_pending_target_candidate_key ON public.banking_pay_workbench_selection_carry_registrations USING btree (target_session_id, candidate_id, stable_selection_key, status, source_priority DESC);

-- public.idx_bpay_selection_carry_source_session
CREATE INDEX idx_bpay_selection_carry_source_session ON public.banking_pay_workbench_selection_carry_registrations USING btree (source_session_id, status, created_at_utc);

-- public.idx_bpay_session_candidate_state_session_status
CREATE INDEX idx_bpay_session_candidate_state_session_status ON public.banking_pay_workbench_session_candidate_state USING btree (session_id, status);

-- public.idx_bpay_session_case_resolutions_session_candidate
CREATE INDEX idx_bpay_session_case_resolutions_session_candidate ON public.banking_pay_workbench_session_case_resolutions USING btree (session_id, candidate_id);

-- public.idx_bpay_session_overrides_session_candidate_type
CREATE INDEX idx_bpay_session_overrides_session_candidate_type ON public.banking_pay_workbench_session_overrides USING btree (session_id, candidate_id, override_type);

-- public.idx_bpay_snapshot_candidate_state_run_status
CREATE INDEX idx_bpay_snapshot_candidate_state_run_status ON public.banking_pay_snapshot_candidate_state USING btree (snapshot_run_id, status);

-- public.idx_bpay_snapshot_case_component_match
CREATE INDEX idx_bpay_snapshot_case_component_match ON public.banking_pay_snapshot_case_component_state USING btree (snapshot_run_id, candidate_id, case_key, source_basis_fingerprint, source_family_key, bucket_code);

-- public.idx_bpay_snapshot_case_component_run_candidate_case
CREATE INDEX idx_bpay_snapshot_case_component_run_candidate_case ON public.banking_pay_snapshot_case_component_state USING btree (snapshot_run_id, candidate_id, case_key);

-- public.idx_bpay_snapshot_case_component_run_candidate_timesheet
CREATE INDEX idx_bpay_snapshot_case_component_run_candidate_timesheet ON public.banking_pay_snapshot_case_component_state USING btree (snapshot_run_id, candidate_id, timesheet_id);

-- public.idx_bpay_snapshot_line_state_run_candidate
CREATE INDEX idx_bpay_snapshot_line_state_run_candidate ON public.banking_pay_snapshot_line_state USING btree (snapshot_run_id, candidate_id);

-- public.idx_bpay_snapshot_runs_status_cycle
CREATE INDEX idx_bpay_snapshot_runs_status_cycle ON public.banking_pay_snapshot_runs USING btree (status, pay_date, week_ending_cutoff);

-- public.idx_bpay_transfer_scope_provider_chunk
CREATE INDEX idx_bpay_transfer_scope_provider_chunk ON public.banking_pay_operation_transfer_scope USING btree (operation_id, pay_batch_id, provider_submit_state, provider_submit_chunk_id);

-- public.idx_bpay_transfer_scope_provider_claim
CREATE INDEX idx_bpay_transfer_scope_provider_claim ON public.banking_pay_operation_transfer_scope USING btree (operation_id, pay_batch_id, provider_submit_ready, provider_submit_state, id);

-- public.idx_bpay_transfer_scope_provider_request
CREATE INDEX idx_bpay_transfer_scope_provider_request ON public.banking_pay_operation_transfer_scope USING btree (provider_request_id) WHERE (provider_request_id IS NOT NULL);

-- public.idx_bpay_transfer_scope_provider_tx
CREATE INDEX idx_bpay_transfer_scope_provider_tx ON public.banking_pay_operation_transfer_scope USING btree (provider_transaction_id) WHERE (provider_transaction_id IS NOT NULL);

-- public.idx_bpay_wb_batch_candidates_candidate_id
CREATE INDEX idx_bpay_wb_batch_candidates_candidate_id ON public.pay_batch_candidates USING btree (candidate_id, id);

-- public.idx_bpay_wb_case_resolution_origin_session
CREATE INDEX idx_bpay_wb_case_resolution_origin_session ON public.banking_pay_workbench_session_case_resolutions USING btree (resolution_origin_session_id, resolution_origin_pay_date) WHERE (resolution_origin_session_id IS NOT NULL);

-- public.idx_bpay_wb_finance_item_authority_page
CREATE INDEX idx_bpay_wb_finance_item_authority_page ON public.pay_batch_items USING btree (pay_batch_candidate_id, id) WHERE ((item_type = ANY (ARRAY['OVERPAYMENT_RECOVERY'::text, 'UNDERPAYMENT_PAYMENT'::text])) AND (COALESCE(is_voided, false) IS FALSE));

-- public.idx_bpay_wb_finance_item_case_page
CREATE INDEX idx_bpay_wb_finance_item_case_page ON public.pay_batch_items USING btree (finance_case_id, id) WHERE ((item_type = ANY (ARRAY['OVERPAYMENT_RECOVERY'::text, 'UNDERPAYMENT_PAYMENT'::text])) AND (COALESCE(is_voided, false) IS FALSE));

-- public.idx_bpay_wb_finance_item_component_page
CREATE INDEX idx_bpay_wb_finance_item_component_page ON public.pay_batch_items USING btree (finance_component_id, id) WHERE ((item_type = ANY (ARRAY['OVERPAYMENT_RECOVERY'::text, 'UNDERPAYMENT_PAYMENT'::text])) AND (COALESCE(is_voided, false) IS FALSE));

-- public.idx_bpay_wb_finance_item_frozen_basis
CREATE INDEX idx_bpay_wb_finance_item_frozen_basis ON public.pay_batch_items USING gin (frozen_source_basis_json jsonb_path_ops) WHERE ((item_type = ANY (ARRAY['OVERPAYMENT_RECOVERY'::text, 'UNDERPAYMENT_PAYMENT'::text])) AND (COALESCE(is_voided, false) IS FALSE));

-- public.idx_bpay_wb_finance_item_frozen_snapshot
CREATE INDEX idx_bpay_wb_finance_item_frozen_snapshot ON public.pay_batch_items USING gin (frozen_component_snapshot_json jsonb_path_ops) WHERE ((item_type = ANY (ARRAY['OVERPAYMENT_RECOVERY'::text, 'UNDERPAYMENT_PAYMENT'::text])) AND (COALESCE(is_voided, false) IS FALSE));

-- public.idx_bpay_wb_finance_item_timesheet_page
CREATE INDEX idx_bpay_wb_finance_item_timesheet_page ON public.pay_batch_items USING btree (timesheet_id, id) WHERE ((item_type = ANY (ARRAY['OVERPAYMENT_RECOVERY'::text, 'UNDERPAYMENT_PAYMENT'::text])) AND (COALESCE(is_voided, false) IS FALSE));

-- public.idx_bpay_wb_line_work_session_candidate_status_ordinal
CREATE INDEX idx_bpay_wb_line_work_session_candidate_status_ordinal ON public.banking_pay_workbench_candidate_line_work USING btree (session_id, candidate_id, status, line_ordinal);

-- public.idx_bpay_wb_line_work_session_status
CREATE INDEX idx_bpay_wb_line_work_session_status ON public.banking_pay_workbench_candidate_line_work USING btree (session_id, status);

-- public.idx_bpay_wb_preview_economic_key
CREATE INDEX idx_bpay_wb_preview_economic_key ON public.banking_pay_workbench_preview_rows USING btree (timesheet_id, key_type, key_value);

-- public.idx_bpay_wb_preview_ready_candidate_page_v2
CREATE INDEX idx_bpay_wb_preview_ready_candidate_page_v2 ON public.banking_pay_workbench_preview_rows USING btree (session_id, candidate_id, session_version, section, row_ordinal, id) WHERE (status = 'READY'::text);

-- public.idx_bpay_wb_preview_ready_page_v2
CREATE INDEX idx_bpay_wb_preview_ready_page_v2 ON public.banking_pay_workbench_preview_rows USING btree (session_id, session_version, section, row_ordinal, id) WHERE (status = 'READY'::text);

-- public.idx_bpay_wb_preview_session_candidate
CREATE INDEX idx_bpay_wb_preview_session_candidate ON public.banking_pay_workbench_preview_rows USING btree (session_id, candidate_id);

-- public.idx_bpay_wb_preview_session_section_ordinal_id
CREATE INDEX idx_bpay_wb_preview_session_section_ordinal_id ON public.banking_pay_workbench_preview_rows USING btree (session_id, section, row_ordinal, id);

-- public.idx_bpay_wb_scope_certified_preview_incomplete_v1
CREATE INDEX idx_bpay_wb_scope_certified_preview_incomplete_v1 ON public.banking_pay_workbench_session_scope USING btree (session_id, scope_ordinal, candidate_id) WHERE ((certified_preview_publication_required IS TRUE) AND (certified_preview_publication_parity_ok IS NOT TRUE));

-- public.idx_bpay_wb_scope_session_ordinal
CREATE INDEX idx_bpay_wb_scope_session_ordinal ON public.banking_pay_workbench_session_scope USING btree (session_id, scope_ordinal);

-- public.idx_bpay_wb_scope_session_status_ordinal
CREATE INDEX idx_bpay_wb_scope_session_status_ordinal ON public.banking_pay_workbench_session_scope USING btree (session_id, status, scope_ordinal);

-- public.idx_bpay_wb_source_current_page
CREATE INDEX idx_bpay_wb_source_current_page ON public.banking_pay_workbench_candidate_source_lines USING btree (session_id, candidate_id, session_version, source_change_seq, source_ordinal, id) WHERE (status = 'CURRENT'::text);

-- public.idx_bpay_wb_source_current_run_page
CREATE INDEX idx_bpay_wb_source_current_run_page ON public.banking_pay_workbench_candidate_source_lines USING btree (session_id, candidate_id, session_version, source_change_seq, source_build_run_id, source_ordinal, id) WHERE (status = 'CURRENT'::text);

-- public.idx_bpay_wb_source_session_status
CREATE INDEX idx_bpay_wb_source_session_status ON public.banking_pay_workbench_candidate_source_lines USING btree (session_id, status);

-- public.idx_bpay_wb_source_status_cleanup
CREATE INDEX idx_bpay_wb_source_status_cleanup ON public.banking_pay_workbench_candidate_source_lines USING btree (session_id, candidate_id, status, updated_at_utc);

-- public.idx_bpay_wb_source_targeted_timesheet
CREATE INDEX idx_bpay_wb_source_targeted_timesheet ON public.banking_pay_workbench_candidate_source_lines USING btree (session_id, candidate_id, timesheet_id) WHERE ((timesheet_id IS NOT NULL) AND (status = ANY (ARRAY['CURRENT'::text, 'DIRTY'::text])));

-- public.idx_bpay_workbench_jobs_generated_work_authority
CREATE INDEX idx_bpay_workbench_jobs_generated_work_authority ON public.banking_pay_workbench_jobs USING btree (dedupe_key, scope_change_generation DESC, created_at_utc DESC, id DESC) INCLUDE (job_type, status, candidate_id, session_id) WHERE ((scope_change_generation IS NOT NULL) AND (job_type = ANY (ARRAY['WORKBENCH_CANDIDATE_DIRTY_APPLY'::text, 'WORKBENCH_FINANCE_CASE_DIRTY_APPLY'::text, 'CONTRACT_CLIENT_DIRTY_FANOUT'::text])));

-- public.idx_bpay_workbench_jobs_scope_generation_barrier
CREATE INDEX idx_bpay_workbench_jobs_scope_generation_barrier ON public.banking_pay_workbench_jobs USING btree (job_type, status, scope_change_generation) WHERE ((scope_change_generation IS NOT NULL) AND (status = ANY (ARRAY['QUEUED'::text, 'RUNNING'::text, 'FAILED'::text])));

-- public.idx_bpay_workbench_jobs_session_candidate_status
CREATE INDEX idx_bpay_workbench_jobs_session_candidate_status ON public.banking_pay_workbench_jobs USING btree (session_id, candidate_id, status);

-- public.idx_bpay_workbench_jobs_snapshot_candidate_status
CREATE INDEX idx_bpay_workbench_jobs_snapshot_candidate_status ON public.banking_pay_workbench_jobs USING btree (snapshot_run_id, candidate_id, status);

-- public.idx_bpay_workbench_jobs_status_due_priority
CREATE INDEX idx_bpay_workbench_jobs_status_due_priority ON public.banking_pay_workbench_jobs USING btree (status, run_at_utc, priority);

-- public.idx_bpay_workbench_sessions_actor_status_updated
CREATE INDEX idx_bpay_workbench_sessions_actor_status_updated ON public.banking_pay_workbench_sessions USING btree (actor_user_id, status, updated_at_utc DESC);

-- public.idx_bpay_workbench_sessions_progress_state
CREATE INDEX idx_bpay_workbench_sessions_progress_state ON public.banking_pay_workbench_sessions USING btree (status, progress_state, updated_at_utc DESC);

-- public.idx_bpay_workbench_sessions_replacement_session_id
CREATE INDEX idx_bpay_workbench_sessions_replacement_session_id ON public.banking_pay_workbench_sessions USING btree (replacement_session_id) WHERE (replacement_session_id IS NOT NULL);

-- public.idx_bpay_workbench_sessions_scope_candidates_gin
CREATE INDEX idx_bpay_workbench_sessions_scope_candidates_gin ON public.banking_pay_workbench_sessions USING gin (scope_candidate_ids);

-- public.idx_bpay_workbench_sessions_scope_seed
CREATE INDEX idx_bpay_workbench_sessions_scope_seed ON public.banking_pay_workbench_sessions USING btree (status, scope_seed_complete, updated_at_utc DESC);

-- public.idx_bpay_workbench_sessions_snapshot_status
CREATE INDEX idx_bpay_workbench_sessions_snapshot_status ON public.banking_pay_workbench_sessions USING btree (source_snapshot_run_id, status);

-- public.idx_candidate_job_titles_job_title_candidate
CREATE INDEX idx_candidate_job_titles_job_title_candidate ON public.candidate_job_titles USING btree (job_title_id, candidate_id);

-- public.idx_candidates_active
CREATE INDEX idx_candidates_active ON public.candidates USING btree (active);

-- public.idx_candidates_display_btree
CREATE INDEX idx_candidates_display_btree ON public.candidates USING btree (display_name);

-- public.idx_candidates_display_trgm
CREATE INDEX idx_candidates_display_trgm ON public.candidates USING gin (display_name gin_trgm_ops);

-- public.idx_candidates_email_trgm
CREATE INDEX idx_candidates_email_trgm ON public.candidates USING gin (email gin_trgm_ops);

-- public.idx_candidates_first_trgm
CREATE INDEX idx_candidates_first_trgm ON public.candidates USING gin (first_name gin_trgm_ops);

-- public.idx_candidates_last_trgm
CREATE INDEX idx_candidates_last_trgm ON public.candidates USING gin (last_name gin_trgm_ops);

-- public.idx_candidates_nhsp_hr_name_aliases_gin
CREATE INDEX idx_candidates_nhsp_hr_name_aliases_gin ON public.candidates USING gin (nhsp_hr_name_aliases jsonb_path_ops);

-- public.idx_candidates_pay_method
CREATE INDEX idx_candidates_pay_method ON public.candidates USING btree (pay_method);

-- public.idx_candidates_rev
CREATE INDEX idx_candidates_rev ON public.candidates USING btree (rev);

-- public.idx_candidates_roles_gin
CREATE INDEX idx_candidates_roles_gin ON public.candidates USING gin (roles jsonb_path_ops);

-- public.idx_candidates_tms_ref_num
CREATE INDEX idx_candidates_tms_ref_num ON public.candidates USING btree (tms_ref_num, tms_ref);

-- public.idx_candidates_umbrella_id
CREATE INDEX idx_candidates_umbrella_id ON public.candidates USING btree (umbrella_id);

-- public.idx_client_hospitals_hospname_norm_gin
CREATE INDEX idx_client_hospitals_hospname_norm_gin ON public.client_hospitals USING gin (hospital_name_norm jsonb_path_ops);

-- public.idx_clients_email_trgm
CREATE INDEX idx_clients_email_trgm ON public.clients USING gin (primary_invoice_email gin_trgm_ops);

-- public.idx_clients_name_btree
CREATE INDEX idx_clients_name_btree ON public.clients USING btree (name);

-- public.idx_clients_name_trgm
CREATE INDEX idx_clients_name_trgm ON public.clients USING gin (name gin_trgm_ops);

-- public.idx_clients_rev
CREATE INDEX idx_clients_rev ON public.clients USING btree (rev);

-- public.idx_comms_outbox_context
CREATE INDEX idx_comms_outbox_context ON public.comms_outbox USING btree (context_kind, context_id, created_at_utc DESC);

-- public.idx_comms_outbox_provider_lookup
CREATE INDEX idx_comms_outbox_provider_lookup ON public.comms_outbox USING btree (provider_key, provider_message_id);

-- public.idx_comms_outbox_ready_queue
CREATE INDEX idx_comms_outbox_ready_queue ON public.comms_outbox USING btree (channel, status, COALESCE(next_attempt_at_utc, scheduled_for_utc, created_at_utc), created_at_utc) WHERE (status = 'QUEUED'::text);

-- public.idx_comms_outbox_recipient
CREATE INDEX idx_comms_outbox_recipient ON public.comms_outbox USING btree (recipient_kind, recipient_id, created_at_utc DESC);

-- public.idx_comms_outbox_status_created
CREATE INDEX idx_comms_outbox_status_created ON public.comms_outbox USING btree (status, created_at_utc);

-- public.idx_contract_weeks_adjustment
CREATE INDEX idx_contract_weeks_adjustment ON public.contract_weeks USING btree (contract_id, week_ending_date, is_adjustment);

-- public.idx_contract_weeks_contract
CREATE INDEX idx_contract_weeks_contract ON public.contract_weeks USING btree (contract_id);

-- public.idx_contracts_cand_client
CREATE INDEX idx_contracts_cand_client ON public.contracts USING btree (candidate_id, client_id);

-- public.idx_contracts_candidate_dates
CREATE INDEX idx_contracts_candidate_dates ON public.contracts USING btree (candidate_id, start_date, end_date);

-- public.idx_contracts_dates
CREATE INDEX idx_contracts_dates ON public.contracts USING btree (start_date, end_date);

-- public.idx_cw_contract_we
CREATE INDEX idx_cw_contract_we ON public.contract_weeks USING btree (contract_id, week_ending_date);

-- public.idx_cw_status
CREATE INDEX idx_cw_status ON public.contract_weeks USING btree (status);

-- public.idx_cw_timesheet
CREATE INDEX idx_cw_timesheet ON public.contract_weeks USING btree (timesheet_id);

-- public.idx_document_templates_entity_output
CREATE INDEX idx_document_templates_entity_output ON public.document_templates USING btree (entity_type, output_type);

-- public.idx_document_templates_entity_output_filename
CREATE INDEX idx_document_templates_entity_output_filename ON public.document_templates USING btree (entity_type, output_type, filename);

-- public.idx_hr_imports_client_id
CREATE INDEX idx_hr_imports_client_id ON public.hr_imports USING btree (client_id);

-- public.idx_hr_imports_source_system
CREATE INDEX idx_hr_imports_source_system ON public.hr_imports USING btree (source_system);

-- public.idx_hr_results_row
CREATE INDEX idx_hr_results_row ON public.hr_results USING btree (row_id);

-- public.idx_hr_results_status
CREATE INDEX idx_hr_results_status ON public.hr_results USING btree (status);

-- public.idx_hr_rows_external_key
CREATE INDEX idx_hr_rows_external_key ON public.hr_rows USING btree (import_id, external_row_key);

-- public.idx_hr_rows_find
CREATE INDEX idx_hr_rows_find ON public.hr_rows USING btree (import_id, staff_norm, date_local);

-- public.idx_id_consolidation_run_lines_invoice
CREATE INDEX idx_id_consolidation_run_lines_invoice ON public.id_consolidation_run_lines USING btree (invoice_id);

-- public.idx_id_invoice_ledger_changed
CREATE INDEX idx_id_invoice_ledger_changed ON public.id_invoice_ledger USING btree (updated_at_utc DESC);

-- public.idx_import_apply_operations_import_created
CREATE INDEX idx_import_apply_operations_import_created ON public.import_apply_operations USING btree (import_id, created_at_utc DESC, id);

-- public.idx_import_apply_operations_pending
CREATE INDEX idx_import_apply_operations_pending ON public.import_apply_operations USING btree (state, updated_at_utc, id) WHERE (state = ANY (ARRAY['PREPARED'::text, 'SOURCE_COMMITTED_TSFIN_PENDING'::text, 'FINANCIALISED_PENDING_FINALISATION'::text]));

-- public.idx_invoice_batch_result_all_v8
CREATE INDEX idx_invoice_batch_result_all_v8 ON public.invoice_operation_chunks USING btree (operation_id, manifest_generation, selection_key, id) WHERE (result_visible AND (replaced_by_chunk_id IS NULL) AND (chunk_type = ANY (ARRAY['GENERATION_GROUP'::text, 'ISSUE_INVOICE'::text])));

-- public.idx_invoice_batch_result_category_v8
CREATE INDEX idx_invoice_batch_result_category_v8 ON public.invoice_operation_chunks USING btree (operation_id, manifest_generation, result_category, selection_key, id) WHERE (result_visible AND (replaced_by_chunk_id IS NULL) AND (chunk_type = ANY (ARRAY['GENERATION_GROUP'::text, 'ISSUE_INVOICE'::text])));

-- public.idx_invoice_chunks_batch_result_selection_key
CREATE INDEX idx_invoice_chunks_batch_result_selection_key ON public.invoice_operation_chunks USING btree (operation_id, chunk_type, ((payload_json ->> 'selection_key'::text)), id) WHERE (chunk_type = ANY (ARRAY['GENERATION_GROUP'::text, 'ISSUE_INVOICE'::text]));

-- public.idx_invoice_document_assets_operation
CREATE INDEX idx_invoice_document_assets_operation ON public.invoice_document_assets USING btree (operation_id);

-- public.idx_invoice_document_assets_original_hash
CREATE INDEX idx_invoice_document_assets_original_hash ON public.invoice_document_assets USING btree (original_sha256) WHERE (original_sha256 IS NOT NULL);

-- public.idx_invoice_document_assets_source
CREATE INDEX idx_invoice_document_assets_source ON public.invoice_document_assets USING btree (source_kind, source_id);

-- public.idx_invoice_document_assets_status_updated
CREATE INDEX idx_invoice_document_assets_status_updated ON public.invoice_document_assets USING btree (status, updated_at_utc);

-- public.idx_invoice_document_versions_candidate_readiness
CREATE INDEX idx_invoice_document_versions_candidate_readiness ON public.invoice_document_versions USING btree (entity_type, entity_id, purpose, source_revision, status, created_at_utc DESC, id DESC);

-- public.idx_invoice_document_versions_entity
CREATE INDEX idx_invoice_document_versions_entity ON public.invoice_document_versions USING btree (entity_type, entity_id, purpose, status);

-- public.idx_invoice_document_versions_operation_status
CREATE INDEX idx_invoice_document_versions_operation_status ON public.invoice_document_versions USING btree (operation_id, status);

-- public.idx_invoice_document_versions_status
CREATE INDEX idx_invoice_document_versions_status ON public.invoice_document_versions USING btree (status, created_at_utc);

-- public.idx_invoice_jobs_outbox_due
CREATE INDEX idx_invoice_jobs_outbox_due ON public.invoice_jobs_outbox USING btree (next_attempt_at NULLS FIRST, created_at);

-- public.idx_invoice_jobs_outbox_kind
CREATE INDEX idx_invoice_jobs_outbox_kind ON public.invoice_jobs_outbox USING btree (kind);

-- public.idx_invoice_lines_invoice
CREATE INDEX idx_invoice_lines_invoice ON public.invoice_lines USING btree (invoice_id);

-- public.idx_invoice_lines_ts
CREATE INDEX idx_invoice_lines_ts ON public.invoice_lines USING btree (timesheet_id);

-- public.idx_invoice_manifest_carrier_identity_v8
CREATE UNIQUE INDEX idx_invoice_manifest_carrier_identity_v8 ON public.invoice_operation_chunks USING btree (operation_id, manifest_generation, selection_key) WHERE (is_manifest_member AND (selection_key IS NOT NULL) AND (replaced_by_chunk_id IS NULL));

-- public.idx_invoice_manifest_release_v8
CREATE INDEX idx_invoice_manifest_release_v8 ON public.invoice_operation_chunks USING btree (operation_id, manifest_generation, manifest_committed, status, phase, selection_key, id) WHERE is_manifest_member;

-- public.idx_invoice_operation_chunks_active
CREATE INDEX idx_invoice_operation_chunks_active ON public.invoice_operation_chunks USING btree (operation_id, chunk_type, phase, status) WHERE (status = ANY (ARRAY['QUEUED'::text, 'RUNNING'::text, 'WAITING'::text, 'RETRY_WAIT'::text, 'BLOCKED'::text]));

-- public.idx_invoice_operation_chunks_active_entity
CREATE INDEX idx_invoice_operation_chunks_active_entity ON public.invoice_operation_chunks USING btree (chunk_type, entity_type, entity_id, status, operation_id) WHERE (status = ANY (ARRAY['QUEUED'::text, 'RUNNING'::text, 'WAITING'::text, 'RETRY_WAIT'::text, 'BLOCKED'::text]));

-- public.idx_invoice_operation_chunks_asset_status
CREATE INDEX idx_invoice_operation_chunks_asset_status ON public.invoice_operation_chunks USING btree (document_asset_id, status);

-- public.idx_invoice_operation_chunks_claim
CREATE INDEX idx_invoice_operation_chunks_claim ON public.invoice_operation_chunks USING btree (status, priority DESC, run_after_utc, created_at_utc, id) WHERE ((chunk_type <> 'DOCUMENT_INPUT'::text) AND (status = ANY (ARRAY['QUEUED'::text, 'RETRY_WAIT'::text, 'RUNNING'::text])));

-- public.idx_invoice_operation_chunks_claim_v8
CREATE INDEX idx_invoice_operation_chunks_claim_v8 ON public.invoice_operation_chunks USING btree (status, is_manifest_member, manifest_committed, priority DESC, run_after_utc, created_at_utc, id) WHERE ((chunk_type <> 'DOCUMENT_INPUT'::text) AND (status = ANY (ARRAY['QUEUED'::text, 'RETRY_WAIT'::text, 'RUNNING'::text])));

-- public.idx_invoice_operation_chunks_dead_letter
CREATE INDEX idx_invoice_operation_chunks_dead_letter ON public.invoice_operation_chunks USING btree (updated_at_utc) WHERE (status = 'DEAD_LETTER'::text);

-- public.idx_invoice_operation_chunks_document_status
CREATE INDEX idx_invoice_operation_chunks_document_status ON public.invoice_operation_chunks USING btree (document_version_id, status);

-- public.idx_invoice_operation_chunks_input_document_status
CREATE INDEX idx_invoice_operation_chunks_input_document_status ON public.invoice_operation_chunks USING btree (input_document_version_id, status);

-- public.idx_invoice_operation_chunks_lease_expiry
CREATE INDEX idx_invoice_operation_chunks_lease_expiry ON public.invoice_operation_chunks USING btree (lease_expires_at_utc) WHERE (status = 'RUNNING'::text);

-- public.idx_invoice_operation_chunks_operation_status
CREATE INDEX idx_invoice_operation_chunks_operation_status ON public.invoice_operation_chunks USING btree (operation_id, status);

-- public.idx_invoice_operation_chunks_replaced_by
CREATE INDEX idx_invoice_operation_chunks_replaced_by ON public.invoice_operation_chunks USING btree (replaced_by_chunk_id) WHERE (replaced_by_chunk_id IS NOT NULL);

-- public.idx_invoice_operation_chunks_slot_generation
CREATE INDEX idx_invoice_operation_chunks_slot_generation ON public.invoice_operation_chunks USING btree (operation_id, chunk_type, level_no, sequence_no, plan_generation);

-- public.idx_invoice_operation_control_receipt_actor_token_v8
CREATE UNIQUE INDEX idx_invoice_operation_control_receipt_actor_token_v8 ON public.invoice_operations USING btree (actor_user_id, idempotency_key) WHERE (operation_type = 'OPERATION_CONTROL_REQUEST'::text);

-- public.idx_invoice_operations_change_seq
CREATE INDEX idx_invoice_operations_change_seq ON public.invoice_operations USING btree (change_seq);

-- public.idx_invoice_operations_claim
CREATE INDEX idx_invoice_operations_claim ON public.invoice_operations USING btree (status, priority DESC, created_at_utc, id);

-- public.idx_invoice_operations_entity_active
CREATE INDEX idx_invoice_operations_entity_active ON public.invoice_operations USING btree (entity_type, entity_id, operation_type, status) WHERE (status = ANY (ARRAY['QUEUED'::text, 'RUNNING'::text, 'WAITING'::text, 'RETRY_WAIT'::text, 'BLOCKED'::text]));

-- public.idx_invoice_operations_entity_status
CREATE INDEX idx_invoice_operations_entity_status ON public.invoice_operations USING btree (entity_type, entity_id, status);

-- public.idx_invoice_operations_parent
CREATE INDEX idx_invoice_operations_parent ON public.invoice_operations USING btree (parent_operation_id);

-- public.idx_invoice_operations_type_status
CREATE INDEX idx_invoice_operations_type_status ON public.invoice_operations USING btree (operation_type, status);

-- public.idx_invoice_operations_updated
CREATE INDEX idx_invoice_operations_updated ON public.invoice_operations USING btree (updated_at_utc DESC);

-- public.idx_invoice_pdf_outbox_due
CREATE INDEX idx_invoice_pdf_outbox_due ON public.invoice_pdf_outbox USING btree (next_attempt_at, created_at);

-- public.idx_invoices_active_document_operation
CREATE INDEX idx_invoices_active_document_operation ON public.invoices USING btree (active_document_operation_id) WHERE (active_document_operation_id IS NOT NULL);

-- public.idx_invoices_active_issue_operation
CREATE INDEX idx_invoices_active_issue_operation ON public.invoices USING btree (active_issue_operation_id) WHERE (active_issue_operation_id IS NOT NULL);

-- public.idx_invoices_batch_issue_scope
CREATE INDEX idx_invoices_batch_issue_scope ON public.invoices USING btree (client_id, document_state, created_at DESC, id) WHERE ((type = 'INVOICE'::invoice_type_enum) AND (status = ANY (ARRAY['DRAFT'::invoice_status_enum, 'ON_HOLD'::invoice_status_enum])));

-- public.idx_invoices_client
CREATE INDEX idx_invoices_client ON public.invoices USING btree (client_id);

-- public.idx_invoices_credit_note_created_at_utc
CREATE INDEX idx_invoices_credit_note_created_at_utc ON public.invoices USING btree (credit_note_created_at_utc) WHERE (credit_note_created_at_utc IS NOT NULL);

-- public.idx_invoices_do_not_send_true
CREATE INDEX idx_invoices_do_not_send_true ON public.invoices USING btree (id) WHERE (do_not_send = true);

-- public.idx_invoices_issued_document
CREATE INDEX idx_invoices_issued_document ON public.invoices USING btree (issued_document_version_id) WHERE (issued_document_version_id IS NOT NULL);

-- public.idx_invoices_on_hold
CREATE INDEX idx_invoices_on_hold ON public.invoices USING btree (issued_at_utc) WHERE (status = 'ON_HOLD'::invoice_status_enum);

-- public.idx_invoices_preview_document
CREATE INDEX idx_invoices_preview_document ON public.invoices USING btree (preview_document_version_id) WHERE (preview_document_version_id IS NOT NULL);

-- public.idx_invoices_status
CREATE INDEX idx_invoices_status ON public.invoices USING btree (status, issued_at_utc);

-- public.idx_mail_outbox_context
CREATE INDEX idx_mail_outbox_context ON public.mail_outbox USING btree (context_kind, context_id, created_at_utc DESC);

-- public.idx_mail_outbox_invoice_attachment_ready
CREATE INDEX idx_mail_outbox_invoice_attachment_ready ON public.mail_outbox USING btree (status, attachments_ready, next_attempt_at_utc, created_at_utc) WHERE ((status = 'QUEUED'::mail_status_enum) AND (waiting_invoice_operation_id IS NULL));

-- public.idx_mail_outbox_ready_queue
CREATE INDEX idx_mail_outbox_ready_queue ON public.mail_outbox USING btree (status, COALESCE(next_attempt_at_utc, scheduled_for_utc, created_at_utc), created_at_utc) WHERE (status = 'QUEUED'::mail_status_enum);

-- public.idx_mail_outbox_recipient
CREATE INDEX idx_mail_outbox_recipient ON public.mail_outbox USING btree (recipient_kind, recipient_id, created_at_utc DESC);

-- public.idx_mail_outbox_status
CREATE INDEX idx_mail_outbox_status ON public.mail_outbox USING btree (status, created_at_utc);

-- public.idx_mailshot_field_overrides_entity_type
CREATE INDEX idx_mailshot_field_overrides_entity_type ON public.mailshot_field_overrides USING btree (entity_type);

-- public.idx_mailshot_fields_enabled_global
CREATE INDEX idx_mailshot_fields_enabled_global ON public.mailshot_fields USING btree (enabled_global);

-- public.idx_mailshot_runs_created_at
CREATE INDEX idx_mailshot_runs_created_at ON public.mailshot_runs USING btree (created_at_utc DESC);

-- public.idx_manual_timesheet_queue_staged_contract_week
CREATE INDEX idx_manual_timesheet_queue_staged_contract_week ON public.manual_timesheet_queue USING btree (((meta_json ->> 'contract_week_id'::text)), uploaded_at_utc, id) WHERE ((status = 'STAGED'::text) AND (NULLIF(btrim(COALESCE((meta_json ->> 'contract_week_id'::text), ''::text)), ''::text) IS NOT NULL));

-- public.idx_nhsp_shifts_candidate_client_date
CREATE INDEX idx_nhsp_shifts_candidate_client_date ON public.nhsp_shifts USING btree (candidate_id, client_id, work_date);

-- public.idx_nhsp_shifts_invoice_id
CREATE INDEX idx_nhsp_shifts_invoice_id ON public.nhsp_shifts USING btree (invoice_id) WHERE (invoice_id IS NOT NULL);

-- public.idx_nhsp_shifts_invoice_status
CREATE INDEX idx_nhsp_shifts_invoice_status ON public.nhsp_shifts USING btree (invoice_status);

-- public.idx_nhsp_shifts_missing_shift_lookup
CREATE INDEX idx_nhsp_shifts_missing_shift_lookup ON public.nhsp_shifts USING btree (source_system, client_id, candidate_id, work_date, cancelled_at_utc);

-- public.idx_nhsp_shifts_source_system
CREATE INDEX idx_nhsp_shifts_source_system ON public.nhsp_shifts USING btree (source_system);

-- public.idx_nhsp_shifts_timesheet
CREATE INDEX idx_nhsp_shifts_timesheet ON public.nhsp_shifts USING btree (timesheet_id);

-- public.idx_nhsp_shifts_timesheet_date_start
CREATE INDEX idx_nhsp_shifts_timesheet_date_start ON public.nhsp_shifts USING btree (timesheet_id, work_date, start_utc, id) WHERE (timesheet_id IS NOT NULL);

-- public.idx_outbox_booking
CREATE INDEX idx_outbox_booking ON public.sheets_outbox USING btree (booking_id);

-- public.idx_outbox_status_next
CREATE INDEX idx_outbox_status_next ON public.sheets_outbox USING btree (status, next_attempt_at NULLS FIRST);

-- public.idx_pay_advance_patches_advance
CREATE INDEX idx_pay_advance_patches_advance ON public.pay_advance_patches USING btree (advance_id, patched_at_utc DESC);

-- public.idx_pay_advance_patches_batch
CREATE INDEX idx_pay_advance_patches_batch ON public.pay_advance_patches USING btree (pay_batch_id);

-- public.idx_pay_advance_reservations_batch_status
CREATE INDEX idx_pay_advance_reservations_batch_status ON public.pay_advance_reservations USING btree (pay_batch_id, status);

-- public.idx_pay_advance_reservations_case_status
CREATE INDEX idx_pay_advance_reservations_case_status ON public.pay_advance_reservations USING btree (finance_case_id, status);

-- public.idx_pay_advance_reservations_finance_component_id
CREATE INDEX idx_pay_advance_reservations_finance_component_id ON public.pay_advance_reservations USING btree (finance_component_id);

-- public.idx_pay_advance_reservations_finance_component_status
CREATE INDEX idx_pay_advance_reservations_finance_component_status ON public.pay_advance_reservations USING btree (finance_component_id, status) WHERE (finance_component_id IS NOT NULL);

-- public.idx_pay_advances_candidate_status
CREATE INDEX idx_pay_advances_candidate_status ON public.pay_advances USING btree (candidate_id, status);

-- public.idx_pay_advances_case_type_client
CREATE INDEX idx_pay_advances_case_type_client ON public.pay_advances USING btree (case_type, client_id);

-- public.idx_pay_advances_case_type_status_candidate
CREATE INDEX idx_pay_advances_case_type_status_candidate ON public.pay_advances USING btree (case_type, status, candidate_id);

-- public.idx_pay_advances_next_due
CREATE INDEX idx_pay_advances_next_due ON public.pay_advances USING btree (next_due_week_start, status);

-- public.idx_pay_advances_reason_shift
CREATE INDEX idx_pay_advances_reason_shift ON public.pay_advances USING btree (reason, candidate_id, client_id, linked_shift_date);

-- public.idx_pay_bank_transfer_events_provider_evidence_lookup
CREATE INDEX idx_pay_bank_transfer_events_provider_evidence_lookup ON public.pay_bank_transfer_events USING btree (pay_batch_id, event_source, normalised_state, received_at_utc DESC, id);

-- public.idx_pay_bank_transfer_events_transfer_source_state_received
CREATE INDEX idx_pay_bank_transfer_events_transfer_source_state_received ON public.pay_bank_transfer_events USING btree (pay_bank_transfer_id, event_source, normalised_state, received_at_utc DESC) WHERE (pay_bank_transfer_id IS NOT NULL);

-- public.idx_pay_bank_transfers_batch
CREATE INDEX idx_pay_bank_transfers_batch ON public.pay_bank_transfers USING btree (pay_batch_id);

-- public.idx_pay_bank_transfers_batch_channel
CREATE INDEX idx_pay_bank_transfers_batch_channel ON public.pay_bank_transfers USING btree (pay_batch_id, pay_channel);

-- public.idx_pay_bank_transfers_batch_channel_group_key
CREATE INDEX idx_pay_bank_transfers_batch_channel_group_key ON public.pay_bank_transfers USING btree (pay_batch_id, pay_channel, transfer_group_key);

-- public.idx_pay_bank_transfers_batch_id_id
CREATE INDEX idx_pay_bank_transfers_batch_id_id ON public.pay_bank_transfers USING btree (pay_batch_id, id);

-- public.idx_pay_bank_transfers_batch_rail_state
CREATE INDEX idx_pay_bank_transfers_batch_rail_state ON public.pay_bank_transfers USING btree (pay_batch_id, rail_state);

-- public.idx_pay_bank_transfers_batch_request_id
CREATE INDEX idx_pay_bank_transfers_batch_request_id ON public.pay_bank_transfers USING btree (pay_batch_id, request_id);

-- public.idx_pay_bank_transfers_batch_status
CREATE INDEX idx_pay_bank_transfers_batch_status ON public.pay_bank_transfers USING btree (pay_batch_id, status);

-- public.idx_pay_bank_transfers_batch_status_id
CREATE INDEX idx_pay_bank_transfers_batch_status_id ON public.pay_bank_transfers USING btree (pay_batch_id, status, id);

-- public.idx_pay_bank_transfers_payment_reference
CREATE INDEX idx_pay_bank_transfers_payment_reference ON public.pay_bank_transfers USING btree (payment_reference) WHERE (payment_reference IS NOT NULL);

-- public.idx_pay_bank_transfers_rail_tx_id_not_null
CREATE INDEX idx_pay_bank_transfers_rail_tx_id_not_null ON public.pay_bank_transfers USING btree (rail_tx_id) WHERE (rail_tx_id IS NOT NULL);

-- public.idx_pay_bank_transfers_status
CREATE INDEX idx_pay_bank_transfers_status ON public.pay_bank_transfers USING btree (status);

-- public.idx_pay_batch_auth_actions_actor_user_id
CREATE INDEX idx_pay_batch_auth_actions_actor_user_id ON public.pay_batch_auth_actions USING btree (actor_user_id);

-- public.idx_pay_batch_auth_actions_auth_request_id
CREATE INDEX idx_pay_batch_auth_actions_auth_request_id ON public.pay_batch_auth_actions USING btree (auth_request_id);

-- public.idx_pay_batch_auth_actions_pay_batch_id
CREATE INDEX idx_pay_batch_auth_actions_pay_batch_id ON public.pay_batch_auth_actions USING btree (pay_batch_id);

-- public.idx_pay_batch_auth_requests_active_batch_state_created
CREATE INDEX idx_pay_batch_auth_requests_active_batch_state_created ON public.pay_batch_auth_requests USING btree (pay_batch_id, state, created_at_utc DESC, id DESC) WHERE (state = ANY (ARRAY['AWAITING'::text, 'PENDING_AUTHORISATION'::text, 'AUTHORISED'::text]));

-- public.idx_pay_batch_auth_requests_pay_batch_id
CREATE INDEX idx_pay_batch_auth_requests_pay_batch_id ON public.pay_batch_auth_requests USING btree (pay_batch_id);

-- public.idx_pay_batch_auth_requests_requested_by
CREATE INDEX idx_pay_batch_auth_requests_requested_by ON public.pay_batch_auth_requests USING btree (requested_by_user_id);

-- public.idx_pay_batch_auth_tokens_auth_request_id
CREATE INDEX idx_pay_batch_auth_tokens_auth_request_id ON public.pay_batch_auth_tokens USING btree (auth_request_id);

-- public.idx_pay_batch_auth_tokens_target_user_id
CREATE INDEX idx_pay_batch_auth_tokens_target_user_id ON public.pay_batch_auth_tokens USING btree (target_user_id);

-- public.idx_pay_batch_candidates_batch
CREATE INDEX idx_pay_batch_candidates_batch ON public.pay_batch_candidates USING btree (pay_batch_id);

-- public.idx_pay_batch_candidates_batch_id_id
CREATE INDEX idx_pay_batch_candidates_batch_id_id ON public.pay_batch_candidates USING btree (pay_batch_id, id);

-- public.idx_pay_batch_candidates_candidate
CREATE INDEX idx_pay_batch_candidates_candidate ON public.pay_batch_candidates USING btree (candidate_id);

-- public.idx_pay_batch_candidates_candidate_batch
CREATE INDEX idx_pay_batch_candidates_candidate_batch ON public.pay_batch_candidates USING btree (candidate_id, pay_batch_id);

-- public.idx_pay_batch_candidates_remittance_sent_at_utc
CREATE INDEX idx_pay_batch_candidates_remittance_sent_at_utc ON public.pay_batch_candidates USING btree (remittance_sent_at_utc);

-- public.idx_pay_batch_display_summary_updated
CREATE INDEX idx_pay_batch_display_summary_updated ON public.pay_batch_display_summary USING btree (updated_at_utc);

-- public.idx_pay_batch_display_summary_version
CREATE INDEX idx_pay_batch_display_summary_version ON public.pay_batch_display_summary USING btree (summary_version);

-- public.idx_pay_batch_item_breakdowns_item_id_id
CREATE INDEX idx_pay_batch_item_breakdowns_item_id_id ON public.pay_batch_item_breakdowns USING btree (pay_batch_item_id, id);

-- public.idx_pay_batch_items_candidate
CREATE INDEX idx_pay_batch_items_candidate ON public.pay_batch_items USING btree (pay_batch_candidate_id);

-- public.idx_pay_batch_items_candidate_id_id
CREATE INDEX idx_pay_batch_items_candidate_id_id ON public.pay_batch_items USING btree (pay_batch_candidate_id, id);

-- public.idx_pay_batch_items_candidate_itemtype
CREATE INDEX idx_pay_batch_items_candidate_itemtype ON public.pay_batch_items USING btree (pay_batch_candidate_id, item_type);

-- public.idx_pay_batch_items_finance_case_id
CREATE INDEX idx_pay_batch_items_finance_case_id ON public.pay_batch_items USING btree (finance_case_id);

-- public.idx_pay_batch_items_finance_component_id
CREATE INDEX idx_pay_batch_items_finance_component_id ON public.pay_batch_items USING btree (finance_component_id);

-- public.idx_pay_batch_items_finance_source_ref
CREATE INDEX idx_pay_batch_items_finance_source_ref ON public.pay_batch_items USING btree (source_ref) WHERE (source_ref IS NOT NULL);

-- public.idx_pay_batch_items_operation_source_key
CREATE INDEX idx_pay_batch_items_operation_source_key ON public.pay_batch_items USING btree (operation_source_key) WHERE (operation_source_key IS NOT NULL);

-- public.idx_pay_batch_items_reservation_id
CREATE INDEX idx_pay_batch_items_reservation_id ON public.pay_batch_items USING btree (reservation_id);

-- public.idx_pay_batch_items_source_ref
CREATE INDEX idx_pay_batch_items_source_ref ON public.pay_batch_items USING btree (source_ref) WHERE (source_ref IS NOT NULL);

-- public.idx_pay_batch_items_timesheet
CREATE INDEX idx_pay_batch_items_timesheet ON public.pay_batch_items USING btree (timesheet_id);

-- public.idx_pay_batch_paye_net_inputs_candidate
CREATE INDEX idx_pay_batch_paye_net_inputs_candidate ON public.pay_batch_paye_net_inputs USING btree (pay_batch_candidate_id);

-- public.idx_pay_batches_authoritative_payment_date
CREATE INDEX idx_pay_batches_authoritative_payment_date ON public.pay_batches USING btree (authoritative_payment_date);

-- public.idx_pay_batches_bulk_ref_date
CREATE INDEX idx_pay_batches_bulk_ref_date ON public.pay_batches USING btree (bulk_ref_date);

-- public.idx_pay_batches_completion_notice_due
CREATE INDEX idx_pay_batches_completion_notice_due ON public.pay_batches USING btree (completion_notice_queued_at_utc, completion_notice_next_attempt_at_utc, status, rail_provider_snapshot, completed_at_utc);

-- public.idx_pay_batches_execution_commit_state
CREATE INDEX idx_pay_batches_execution_commit_state ON public.pay_batches USING btree (execution_commit_state);

-- public.idx_pay_batches_force_include_ts_gin
CREATE INDEX idx_pay_batches_force_include_ts_gin ON public.pay_batches USING gin (force_include_timesheet_ids);

-- public.idx_pay_batches_override_mode
CREATE INDEX idx_pay_batches_override_mode ON public.pay_batches USING btree (override_mode);

-- public.idx_pay_batches_pay_date
CREATE INDEX idx_pay_batches_pay_date ON public.pay_batches USING btree (pay_date DESC);

-- public.idx_pay_batches_paydate_status_kindfixed
CREATE INDEX idx_pay_batches_paydate_status_kindfixed ON public.pay_batches USING btree (pay_date, status, batch_kind_fixed);

-- public.idx_pay_batches_same_week_paye_override_used
CREATE INDEX idx_pay_batches_same_week_paye_override_used ON public.pay_batches USING btree (same_week_paye_override_used);

-- public.idx_pay_batches_source_snapshot_run_id
CREATE INDEX idx_pay_batches_source_snapshot_run_id ON public.pay_batches USING btree (source_snapshot_run_id);

-- public.idx_pay_batches_source_workbench_session_id
CREATE INDEX idx_pay_batches_source_workbench_session_id ON public.pay_batches USING btree (source_workbench_session_id);

-- public.idx_pay_batches_status
CREATE INDEX idx_pay_batches_status ON public.pay_batches USING btree (status);

-- public.idx_pay_fin_case_oneoff_payout_bank_details_candidate
CREATE INDEX idx_pay_fin_case_oneoff_payout_bank_details_candidate ON public.pay_finance_case_oneoff_payout_bank_details USING btree (candidate_id);

-- public.idx_pay_fin_case_oneoff_payout_bank_details_hash
CREATE INDEX idx_pay_fin_case_oneoff_payout_bank_details_hash ON public.pay_finance_case_oneoff_payout_bank_details USING btree (bank_details_hash);

-- public.idx_pay_finance_case_components_candidate_id
CREATE INDEX idx_pay_finance_case_components_candidate_id ON public.pay_finance_case_components USING btree (candidate_id);

-- public.idx_pay_finance_case_components_case_id
CREATE INDEX idx_pay_finance_case_components_case_id ON public.pay_finance_case_components USING btree (finance_case_id);

-- public.idx_pay_finance_case_components_classification
CREATE INDEX idx_pay_finance_case_components_classification ON public.pay_finance_case_components USING btree (classification);

-- public.idx_pay_finance_case_components_linked_timesheet_id
CREATE INDEX idx_pay_finance_case_components_linked_timesheet_id ON public.pay_finance_case_components USING btree (linked_timesheet_id);

-- public.idx_pay_finance_case_components_open
CREATE INDEX idx_pay_finance_case_components_open ON public.pay_finance_case_components USING btree (finance_case_id, allocation_priority_group, allocation_priority_order, created_at_utc) WHERE ((closed_at_utc IS NULL) AND (remaining_source_amount > (0)::numeric));

-- public.idx_pay_finance_case_components_stale_open
CREATE INDEX idx_pay_finance_case_components_stale_open ON public.pay_finance_case_components USING btree (finance_case_id, is_resolution_stale) WHERE ((closed_at_utc IS NULL) AND (remaining_source_amount > (0)::numeric));

-- public.idx_pay_finance_case_events_batch_at
CREATE INDEX idx_pay_finance_case_events_batch_at ON public.pay_finance_case_events USING btree (pay_batch_id, event_at_utc DESC);

-- public.idx_pay_finance_case_events_case_at
CREATE INDEX idx_pay_finance_case_events_case_at ON public.pay_finance_case_events USING btree (finance_case_id, event_at_utc DESC);

-- public.idx_pay_finance_case_events_component_at
CREATE INDEX idx_pay_finance_case_events_component_at ON public.pay_finance_case_events USING btree (finance_component_id, event_at_utc DESC) WHERE (finance_component_id IS NOT NULL);

-- public.idx_pay_item_snoozes_active_expiry
CREATE INDEX idx_pay_item_snoozes_active_expiry ON public.pay_item_snoozes USING btree (snooze_until_date, candidate_id, id) WHERE ((snooze_until_date IS NOT NULL) AND (cleared_at_utc IS NULL) AND (cancelled_at_utc IS NULL));

-- public.idx_pay_item_snoozes_active_timesheet_scope
CREATE INDEX idx_pay_item_snoozes_active_timesheet_scope ON public.pay_item_snoozes USING btree (candidate_id, timesheet_id, snooze_kind, snooze_until_date) WHERE ((cleared_at_utc IS NULL) AND (cancelled_at_utc IS NULL) AND (timesheet_id IS NOT NULL));

-- public.idx_pay_item_snoozes_active_timesheet_segment_stable
CREATE INDEX idx_pay_item_snoozes_active_timesheet_segment_stable ON public.pay_item_snoozes USING btree (candidate_id, timesheet_id, segment_stable_key, snooze_kind) WHERE ((cleared_at_utc IS NULL) AND (cancelled_at_utc IS NULL) AND (timesheet_id IS NOT NULL));

-- public.idx_pay_item_snoozes_lookup
CREATE INDEX idx_pay_item_snoozes_lookup ON public.pay_item_snoozes USING btree (candidate_id, timesheet_id, segment_id, snooze_kind) WHERE (cleared_at_utc IS NULL);

-- public.idx_pay_item_snoozes_lookup_booking
CREATE INDEX idx_pay_item_snoozes_lookup_booking ON public.pay_item_snoozes USING btree (candidate_id, booking_id, segment_stable_key, snooze_kind) WHERE ((cleared_at_utc IS NULL) AND (source_ref IS NULL) AND (booking_id IS NOT NULL));

-- public.idx_pay_item_snoozes_lookup_source_ref
CREATE INDEX idx_pay_item_snoozes_lookup_source_ref ON public.pay_item_snoozes USING btree (candidate_id, timesheet_id, source_ref, snooze_kind) WHERE ((cleared_at_utc IS NULL) AND (source_ref IS NOT NULL));

-- public.idx_pay_item_snoozes_unchecked_natural_expiry
CREATE INDEX idx_pay_item_snoozes_unchecked_natural_expiry ON public.pay_item_snoozes USING btree (snooze_until_date, candidate_id, id) WHERE ((snooze_until_date IS NOT NULL) AND (cleared_at_utc IS NULL) AND (cancelled_at_utc IS NULL) AND (natural_expiry_source_fingerprint IS NOT NULL) AND (natural_expiry_checked_fingerprint IS DISTINCT FROM natural_expiry_source_fingerprint));

-- public.idx_pay_manual_adjustment_carry_forwards_candidate_status_chann
CREATE INDEX idx_pay_manual_adjustment_carry_forwards_candidate_status_chann ON public.pay_manual_adjustment_carry_forwards USING btree (candidate_id, status, pay_channel);

-- public.idx_pay_manual_adjustment_carry_forwards_source_batch_status
CREATE INDEX idx_pay_manual_adjustment_carry_forwards_source_batch_status ON public.pay_manual_adjustment_carry_forwards USING btree (source_pay_batch_id, status);

-- public.idx_pay_manual_adjustment_carry_forwards_source_request
CREATE INDEX idx_pay_manual_adjustment_carry_forwards_source_request ON public.pay_manual_adjustment_carry_forwards USING btree (source_correction_request_id) WHERE (source_correction_request_id IS NOT NULL);

-- public.idx_pay_manual_adjustment_carry_forwards_source_transfer
CREATE INDEX idx_pay_manual_adjustment_carry_forwards_source_transfer ON public.pay_manual_adjustment_carry_forwards USING btree (source_pay_bank_transfer_id) WHERE (source_pay_bank_transfer_id IS NOT NULL);

-- public.idx_pay_manual_adjustment_carry_forwards_status
CREATE INDEX idx_pay_manual_adjustment_carry_forwards_status ON public.pay_manual_adjustment_carry_forwards USING btree (status);

-- public.idx_pay_manual_adjustment_carry_forwards_target_batch_status
CREATE INDEX idx_pay_manual_adjustment_carry_forwards_target_batch_status ON public.pay_manual_adjustment_carry_forwards USING btree (target_pay_batch_id, status);

-- public.idx_pay_manual_adjustment_carry_forwards_target_operation_key
CREATE INDEX idx_pay_manual_adjustment_carry_forwards_target_operation_key ON public.pay_manual_adjustment_carry_forwards USING btree (target_operation_source_key) WHERE (target_operation_source_key IS NOT NULL);

-- public.idx_pay_payment_correction_request_candidates_instruction
CREATE INDEX idx_pay_payment_correction_request_candidates_instruction ON public.pay_payment_correction_request_candidates USING btree (correction_request_id, shared_instruction_scope_hash, selection_ordinal) WHERE (shared_instruction_scope_hash IS NOT NULL);

-- public.idx_pay_payment_correction_requests_due_v2
CREATE INDEX idx_pay_payment_correction_requests_due_v2 ON public.pay_payment_correction_requests USING btree (status, requested_at_utc, id) WHERE (status = ANY (ARRAY['PLANNING'::text, 'PLANNED'::text, 'REQUESTED'::text, 'AWAITING_AUTHORISATION'::text]));

-- public.idx_pay_payment_return_notice_groups_alert_fingerprint
CREATE INDEX idx_pay_payment_return_notice_groups_alert_fingerprint ON public.pay_payment_return_notice_groups USING btree (alert_fingerprint) WHERE ((alert_fingerprint IS NOT NULL) AND (btrim(alert_fingerprint) <> ''::text));

-- public.idx_pay_snooze_warning_ack_actor_expiry
CREATE INDEX idx_pay_snooze_warning_ack_actor_expiry ON public.pay_snooze_warning_acknowledgements USING btree (actor_user_id, expires_at_utc DESC);

-- public.idx_pay_snooze_warning_ack_expiry
CREATE INDEX idx_pay_snooze_warning_ack_expiry ON public.pay_snooze_warning_acknowledgements USING btree (expires_at_utc, id);

-- public.idx_pay_snooze_warning_ack_scope
CREATE INDEX idx_pay_snooze_warning_ack_scope ON public.pay_snooze_warning_acknowledgements USING btree (actor_user_id, candidate_id, timesheet_id, source_ref, snooze_kind, validation_phase, expires_at_utc DESC);

-- public.idx_pbib_item
CREATE INDEX idx_pbib_item ON public.pay_batch_item_breakdowns USING btree (pay_batch_item_id);

-- public.idx_timesheet_evidence_document_asset
CREATE INDEX idx_timesheet_evidence_document_asset ON public.timesheet_evidence USING btree (document_asset_id) WHERE (document_asset_id IS NOT NULL);

-- public.idx_timesheet_evidence_source_revision
CREATE INDEX idx_timesheet_evidence_source_revision ON public.timesheet_evidence USING btree (timesheet_id, source_revision) WHERE (source_revision IS NOT NULL);

-- public.idx_timesheet_evidence_timesheet_kind
CREATE INDEX idx_timesheet_evidence_timesheet_kind ON public.timesheet_evidence USING btree (timesheet_id, kind);

-- public.idx_timesheet_evidence_ts
CREATE INDEX idx_timesheet_evidence_ts ON public.timesheet_evidence USING btree (timesheet_id);

-- public.idx_timesheet_evidence_ts_kind_created
CREATE INDEX idx_timesheet_evidence_ts_kind_created ON public.timesheet_evidence USING btree (timesheet_id, kind, created_at, id);

-- public.idx_timesheet_pay_state_history_ts
CREATE INDEX idx_timesheet_pay_state_history_ts ON public.timesheet_pay_state_history USING btree (timesheet_id, settled_at_utc DESC);

-- public.idx_timesheet_pay_state_last_batch
CREATE INDEX idx_timesheet_pay_state_last_batch ON public.timesheet_pay_state USING btree (last_settled_pay_batch_id);

-- public.idx_timesheet_payment_overrides_candidate_active
CREATE INDEX idx_timesheet_payment_overrides_candidate_active ON public.timesheet_payment_overrides USING btree (candidate_id, created_at_utc DESC);

-- public.idx_timesheet_validations_latest
CREATE INDEX idx_timesheet_validations_latest ON public.timesheet_validations USING btree (timesheet_id, updated_at DESC);

-- public.idx_timesheet_validations_status
CREATE INDEX idx_timesheet_validations_status ON public.timesheet_validations USING btree (status);

-- public.idx_timesheets_active_document_operation
CREATE INDEX idx_timesheets_active_document_operation ON public.timesheets USING btree (active_document_operation_id) WHERE (active_document_operation_id IS NOT NULL);

-- public.idx_timesheets_active_tools
CREATE INDEX idx_timesheets_active_tools ON public.timesheets USING btree (is_current, week_ending_date DESC, timesheet_id) WHERE (archived_at_utc IS NULL);

-- public.idx_timesheets_archive_actor
CREATE INDEX idx_timesheets_archive_actor ON public.timesheets USING btree (archived_by_user_id, archived_at_utc DESC) WHERE (archived_at_utc IS NOT NULL);

-- public.idx_timesheets_archived_tools
CREATE INDEX idx_timesheets_archived_tools ON public.timesheets USING btree (archived_at_utc DESC, week_ending_date DESC, timesheet_id) WHERE (archived_at_utc IS NOT NULL);

-- public.idx_timesheets_band
CREATE INDEX idx_timesheets_band ON public.timesheets USING btree (band);

-- public.idx_timesheets_booking_id
CREATE INDEX idx_timesheets_booking_id ON public.timesheets USING btree (booking_id);

-- public.idx_timesheets_contract
CREATE INDEX idx_timesheets_contract ON public.timesheets USING btree (contract_id);

-- public.idx_timesheets_contract_scope
CREATE INDEX idx_timesheets_contract_scope ON public.timesheets USING btree (contract_id, sheet_scope);

-- public.idx_timesheets_contract_weekending
CREATE INDEX idx_timesheets_contract_weekending ON public.timesheets USING btree (contract_id, week_ending_date);

-- public.idx_timesheets_current_document
CREATE INDEX idx_timesheets_current_document ON public.timesheets USING btree (current_document_version_id) WHERE (current_document_version_id IS NOT NULL);

-- public.idx_timesheets_hosp_current
CREATE INDEX idx_timesheets_hosp_current ON public.timesheets USING btree (hospital_norm) WHERE (is_current = true);

-- public.idx_timesheets_hosp_current_auth
CREATE INDEX idx_timesheets_hosp_current_auth ON public.timesheets USING btree (hospital_norm) WHERE ((is_current = true) AND (authorised_at_server IS NOT NULL));

-- public.idx_timesheets_invoice_batch_week_scope
CREATE INDEX idx_timesheets_invoice_batch_week_scope ON public.timesheets USING btree (week_ending_date, timesheet_id) WHERE (is_current AND (revoked_at IS NULL));

-- public.idx_timesheets_is_adjustment_true
CREATE INDEX idx_timesheets_is_adjustment_true ON public.timesheets USING btree (is_adjustment) WHERE (is_adjustment = true);

-- public.idx_timesheets_is_current
CREATE INDEX idx_timesheets_is_current ON public.timesheets USING btree (is_current);

-- public.idx_timesheets_line_type
CREATE INDEX idx_timesheets_line_type ON public.timesheets USING btree (line_type);

-- public.idx_timesheets_manual_document_asset
CREATE INDEX idx_timesheets_manual_document_asset ON public.timesheets USING btree (manual_document_asset_id) WHERE (manual_document_asset_id IS NOT NULL);

-- public.idx_timesheets_occ_current
CREATE INDEX idx_timesheets_occ_current ON public.timesheets USING btree (occupant_key_norm) WHERE (is_current = true);

-- public.idx_timesheets_occ_current_auth
CREATE INDEX idx_timesheets_occ_current_auth ON public.timesheets USING btree (occupant_key_norm) WHERE ((is_current = true) AND (authorised_at_server IS NOT NULL));

-- public.idx_timesheets_parent_timesheet_id
CREATE INDEX idx_timesheets_parent_timesheet_id ON public.timesheets USING btree (parent_timesheet_id);

-- public.idx_timesheets_qr_token
CREATE UNIQUE INDEX idx_timesheets_qr_token ON public.timesheets USING btree (qr_token) WHERE (qr_token IS NOT NULL);

-- public.idx_timesheets_reference_number
CREATE INDEX idx_timesheets_reference_number ON public.timesheets USING btree (reference_number);

-- public.idx_timesheets_status
CREATE INDEX idx_timesheets_status ON public.timesheets USING btree (status);

-- public.idx_timesheets_sub_mode
CREATE INDEX idx_timesheets_sub_mode ON public.timesheets USING btree (submission_mode);

-- public.idx_timesheets_weekending
CREATE INDEX idx_timesheets_weekending ON public.timesheets USING btree (week_ending_date);

-- public.idx_tlbo_created_by
CREATE INDEX idx_tlbo_created_by ON public.timesheet_lifecycle_bulk_operations USING btree (created_by_user_id, created_at_utc DESC);

-- public.idx_tlbo_status_run_after
CREATE INDEX idx_tlbo_status_run_after ON public.timesheet_lifecycle_bulk_operations USING btree (status, run_after_utc, created_at_utc);

-- public.idx_tlboi_operation_status_ordinal
CREATE INDEX idx_tlboi_operation_status_ordinal ON public.timesheet_lifecycle_bulk_operation_items USING btree (operation_id, status, ordinal);

-- public.idx_tlboi_timesheet
CREATE INDEX idx_tlboi_timesheet ON public.timesheet_lifecycle_bulk_operation_items USING btree (timesheet_id, created_at_utc DESC);

-- public.idx_tms_login_2fa_challenges_expires_at_utc
CREATE INDEX idx_tms_login_2fa_challenges_expires_at_utc ON public.tms_login_2fa_challenges USING btree (expires_at_utc);

-- public.idx_tms_login_2fa_challenges_used_at_utc
CREATE INDEX idx_tms_login_2fa_challenges_used_at_utc ON public.tms_login_2fa_challenges USING btree (used_at_utc);

-- public.idx_tms_login_2fa_challenges_user_ip
CREATE INDEX idx_tms_login_2fa_challenges_user_ip ON public.tms_login_2fa_challenges USING btree (user_id, ip_address);

-- public.idx_tms_login_2fa_challenges_user_purpose_expires
CREATE INDEX idx_tms_login_2fa_challenges_user_purpose_expires ON public.tms_login_2fa_challenges USING btree (user_id, purpose, expires_at_utc);

-- public.idx_tms_password_resets_token
CREATE INDEX idx_tms_password_resets_token ON public.tms_password_resets USING btree (token);

-- public.idx_tms_user_2fa_trust_user_ip
CREATE INDEX idx_tms_user_2fa_trust_user_ip ON public.tms_user_2fa_trust USING btree (user_id, ip_address);

-- public.idx_tms_user_2fa_trust_verified_at_utc
CREATE INDEX idx_tms_user_2fa_trust_verified_at_utc ON public.tms_user_2fa_trust USING btree (verified_at_utc DESC);

-- public.idx_ts_pdfs_outbox_due
CREATE INDEX idx_ts_pdfs_outbox_due ON public.ts_pdfs_outbox USING btree (next_attempt_at, created_at);

-- public.idx_tsfin_assignment
CREATE INDEX idx_tsfin_assignment ON public.timesheets_financials USING btree (candidate_assignment);

-- public.idx_tsfin_candidate_current
CREATE INDEX idx_tsfin_candidate_current ON public.timesheets_financials USING btree (candidate_id) WHERE (is_current = true);

-- public.idx_tsfin_candidate_current_unlocked
CREATE INDEX idx_tsfin_candidate_current_unlocked ON public.timesheets_financials USING btree (candidate_id) WHERE (is_current AND (locked_by_invoice_id IS NULL));

-- public.idx_tsfin_client
CREATE INDEX idx_tsfin_client ON public.timesheets_financials USING btree (client_id);

-- public.idx_tsfin_client_current_unlocked
CREATE INDEX idx_tsfin_client_current_unlocked ON public.timesheets_financials USING btree (client_id) WHERE (is_current AND (locked_by_invoice_id IS NULL));

-- public.idx_tsfin_current_timesheet
CREATE INDEX idx_tsfin_current_timesheet ON public.timesheets_financials USING btree (timesheet_id) WHERE (is_current = true);

-- public.idx_tsfin_invoice_batch_candidate_scope
CREATE INDEX idx_tsfin_invoice_batch_candidate_scope ON public.timesheets_financials USING btree (client_id, candidate_id, timesheet_id) WHERE is_current;

-- public.idx_tsfin_invoice_batch_client_scope
CREATE INDEX idx_tsfin_invoice_batch_client_scope ON public.timesheets_financials USING btree (client_id, timesheet_id) WHERE is_current;

-- public.idx_tsfin_lock
CREATE INDEX idx_tsfin_lock ON public.timesheets_financials USING btree (locked_by_invoice_id);

-- public.idx_tsfin_nhsp_import
CREATE INDEX idx_tsfin_nhsp_import ON public.timesheets_financials USING btree (nhsp_import_id);

-- public.idx_tsfin_outbox_next
CREATE INDEX idx_tsfin_outbox_next ON public.ts_financials_outbox USING btree (next_attempt_at NULLS FIRST, created_at);

-- public.idx_tsfin_outbox_reason
CREATE INDEX idx_tsfin_outbox_reason ON public.ts_financials_outbox USING btree (reason);

-- public.idx_tsfin_outbox_ts
CREATE INDEX idx_tsfin_outbox_ts ON public.ts_financials_outbox USING btree (timesheet_id);

-- public.idx_tsfin_paid_at
CREATE INDEX idx_tsfin_paid_at ON public.timesheets_financials USING btree (paid_at_utc);

-- public.idx_tsfin_paid_by_user
CREATE INDEX idx_tsfin_paid_by_user ON public.timesheets_financials USING btree (paid_by_user_id);

-- public.idx_tsfin_pay_on_hold
CREATE INDEX idx_tsfin_pay_on_hold ON public.timesheets_financials USING btree (pay_on_hold);

-- public.idx_tsfin_remit_last_sent
CREATE INDEX idx_tsfin_remit_last_sent ON public.timesheets_financials USING btree (remittance_last_sent_at_utc);

-- public.idx_tsfin_stale
CREATE INDEX idx_tsfin_stale ON public.timesheets_financials USING btree (is_stale);

-- public.idx_tsfin_status
CREATE INDEX idx_tsfin_status ON public.timesheets_financials USING btree (processing_status);

-- public.idx_tsfin_ts
CREATE INDEX idx_tsfin_ts ON public.timesheets_financials USING btree (timesheet_id, timesheet_version DESC);

-- public.idx_tsfin_unpaid_current_not_on_hold
CREATE INDEX idx_tsfin_unpaid_current_not_on_hold ON public.timesheets_financials USING btree (client_id, candidate_id) WHERE (is_current AND (paid_at_utc IS NULL) AND (pay_on_hold = false));

-- public.idx_tsv_hr_request_id
CREATE INDEX idx_tsv_hr_request_id ON public.timesheet_validations USING btree (hr_request_id);

-- public.import_review_action_outcomes_candidate_client_idx
CREATE INDEX import_review_action_outcomes_candidate_client_idx ON public.import_review_action_outcomes USING btree (import_id, candidate_id, client_id, applied_at_utc);

-- public.import_review_action_outcomes_import_idx
CREATE INDEX import_review_action_outcomes_import_idx ON public.import_review_action_outcomes USING btree (import_id, applied_at_utc, action_id);

-- public.import_review_action_outcomes_operation_idx
CREATE INDEX import_review_action_outcomes_operation_idx ON public.import_review_action_outcomes USING btree (operation_id, action_id);

-- public.import_review_daily_resolution_import_idx
CREATE INDEX import_review_daily_resolution_import_idx ON public.import_review_daily_timesheet_resolutions USING btree (import_id, status, hr_row_id);

-- public.import_review_daily_resolution_operation_idx
CREATE INDEX import_review_daily_resolution_operation_idx ON public.import_review_daily_timesheet_resolutions USING btree (applied_operation_id) WHERE (applied_operation_id IS NOT NULL);

-- public.import_review_daily_resolution_row_idx
CREATE INDEX import_review_daily_resolution_row_idx ON public.import_review_daily_timesheet_resolutions USING btree (hr_row_id);

-- public.import_review_daily_resolution_timesheet_idx
CREATE INDEX import_review_daily_resolution_timesheet_idx ON public.import_review_daily_timesheet_resolutions USING btree (resolved_timesheet_id) WHERE (resolved_timesheet_id IS NOT NULL);

-- public.import_review_decisions_candidate_idx
CREATE INDEX import_review_decisions_candidate_idx ON public.import_review_decisions USING btree (candidate_id) WHERE (candidate_id IS NOT NULL);

-- public.import_review_decisions_client_idx
CREATE INDEX import_review_decisions_client_idx ON public.import_review_decisions USING btree (client_id) WHERE (client_id IS NOT NULL);

-- public.import_review_decisions_contract_idx
CREATE INDEX import_review_decisions_contract_idx ON public.import_review_decisions USING btree (contract_id) WHERE (contract_id IS NOT NULL);

-- public.import_review_decisions_hr_row_idx
CREATE INDEX import_review_decisions_hr_row_idx ON public.import_review_decisions USING btree (hr_row_id) WHERE (hr_row_id IS NOT NULL);

-- public.import_review_decisions_import_page_idx
CREATE INDEX import_review_decisions_import_page_idx ON public.import_review_decisions USING btree (import_id, is_current, action_id);

-- public.import_review_decisions_selected_idx
CREATE INDEX import_review_decisions_selected_idx ON public.import_review_decisions USING btree (import_id, action_kind, action_id) WHERE (is_current AND selected);

-- public.import_review_decisions_shift_idx
CREATE INDEX import_review_decisions_shift_idx ON public.import_review_decisions USING btree (shift_id) WHERE (shift_id IS NOT NULL);

-- public.import_review_decisions_timesheet_idx
CREATE INDEX import_review_decisions_timesheet_idx ON public.import_review_decisions USING btree (timesheet_id) WHERE (timesheet_id IS NOT NULL);

-- public.import_review_events_import_page_idx
CREATE INDEX import_review_events_import_page_idx ON public.import_review_events USING btree (import_id, id);

-- public.import_review_events_operation_idx
CREATE INDEX import_review_events_operation_idx ON public.import_review_events USING btree (operation_id) WHERE (operation_id IS NOT NULL);

-- public.import_review_scope_candidates_candidate_idx
CREATE INDEX import_review_scope_candidates_candidate_idx ON public.import_review_scope_candidates USING btree (candidate_id) WHERE (candidate_id IS NOT NULL);

-- public.import_review_scope_candidates_import_idx
CREATE INDEX import_review_scope_candidates_import_idx ON public.import_review_scope_candidates USING btree (import_id);

-- public.import_review_scope_clients_client_idx
CREATE INDEX import_review_scope_clients_client_idx ON public.import_review_scope_clients USING btree (client_id) WHERE (client_id IS NOT NULL);

-- public.import_review_scope_clients_import_idx
CREATE INDEX import_review_scope_clients_import_idx ON public.import_review_scope_clients USING btree (import_id);

-- public.import_review_states_follow_up_idx
CREATE INDEX import_review_states_follow_up_idx ON public.import_review_states USING btree (follow_up_status, updated_at_utc, import_id) WHERE ((status = 'APPLIED'::text) AND (follow_up_status = ANY (ARRAY['PENDING'::text, 'FAILED_RETRYABLE'::text])));

-- public.import_review_states_last_operation_idx
CREATE INDEX import_review_states_last_operation_idx ON public.import_review_states USING btree (last_operation_id) WHERE (last_operation_id IS NOT NULL);

-- public.import_review_states_list_idx
CREATE INDEX import_review_states_list_idx ON public.import_review_states USING btree (status, updated_at_utc DESC, import_id);

-- public.import_review_weekly_validation_resolution_hr_row_idx
CREATE INDEX import_review_weekly_validation_resolution_hr_row_idx ON public.import_review_weekly_validation_resolutions USING btree (hr_row_id);

-- public.import_review_weekly_validation_resolution_import_idx
CREATE INDEX import_review_weekly_validation_resolution_import_idx ON public.import_review_weekly_validation_resolutions USING btree (import_id, status, hr_row_id);

-- public.import_review_weekly_validation_resolution_operation_idx
CREATE INDEX import_review_weekly_validation_resolution_operation_idx ON public.import_review_weekly_validation_resolutions USING btree (applied_operation_id) WHERE (applied_operation_id IS NOT NULL);

-- public.import_review_weekly_validation_resolution_timesheet_idx
CREATE INDEX import_review_weekly_validation_resolution_timesheet_idx ON public.import_review_weekly_validation_resolutions USING btree (timesheet_id);

-- public.invoice_hr_source_rows_invoice_idx
CREATE INDEX invoice_hr_source_rows_invoice_idx ON public.invoice_hr_source_rows USING btree (invoice_id);

-- public.ix_bank_name_checks_entity
CREATE INDEX ix_bank_name_checks_entity ON public.bank_name_checks USING btree (entity_kind, entity_id);

-- public.ix_bank_payee_map_entity
CREATE INDEX ix_bank_payee_map_entity ON public.bank_payee_map USING btree (entity_kind, entity_id);

-- public.ix_bank_payee_map_provider_env
CREATE INDEX ix_bank_payee_map_provider_env ON public.bank_payee_map USING btree (rail_provider, rail_env);

-- public.ix_pay_batch_items_transfer_id
CREATE INDEX ix_pay_batch_items_transfer_id ON public.pay_batch_items USING btree (pay_bank_transfer_id);

-- public.ix_pay_batch_timesheet_snapshots_batch
CREATE INDEX ix_pay_batch_timesheet_snapshots_batch ON public.pay_batch_timesheet_snapshots USING btree (pay_batch_id);

-- public.ix_pay_batch_timesheet_snapshots_timesheet
CREATE INDEX ix_pay_batch_timesheet_snapshots_timesheet ON public.pay_batch_timesheet_snapshots USING btree (timesheet_id);

-- public.legacy_contract_rate_lines_contract_idx
CREATE INDEX legacy_contract_rate_lines_contract_idx ON public.legacy_contract_rate_lines USING btree (legacy_contract_id, line_no);

-- public.legacy_contracts_candidate_idx
CREATE INDEX legacy_contracts_candidate_idx ON public.legacy_contracts USING btree (candidate_id, start_date DESC);

-- public.legacy_contracts_client_idx
CREATE INDEX legacy_contracts_client_idx ON public.legacy_contracts USING btree (client_id, start_date DESC);

-- public.legacy_contracts_source_uq
CREATE UNIQUE INDEX legacy_contracts_source_uq ON public.legacy_contracts USING btree (source_system, source_row_hash);

-- public.legacy_eclipse_candidate_map_candidate_id_uq
CREATE UNIQUE INDEX legacy_eclipse_candidate_map_candidate_id_uq ON public.legacy_eclipse_candidate_map USING btree (candidate_id);

-- public.legacy_eclipse_client_map_client_id_uq
CREATE UNIQUE INDEX legacy_eclipse_client_map_client_id_uq ON public.legacy_eclipse_client_map USING btree (client_id);

-- public.mail_outbox_payment_scope_json_gin_idx
CREATE INDEX mail_outbox_payment_scope_json_gin_idx ON public.mail_outbox USING gin (payment_scope_json);

-- public.mail_outbox_uq_invoice_batch_type_ref_to
CREATE UNIQUE INDEX mail_outbox_uq_invoice_batch_type_ref_to ON public.mail_outbox USING btree (type, reference, "to") WHERE ((type = 'INVOICE'::text) AND (reference ~~ 'invoice_batch:%'::text));

-- public.manual_timesheet_queue_content_hash_active_idx
CREATE UNIQUE INDEX manual_timesheet_queue_content_hash_active_idx ON public.manual_timesheet_queue USING btree (content_hash) WHERE (status = ANY (ARRAY['QUEUED'::text, 'IN_PROGRESS'::text]));

-- public.manual_timesheet_queue_status_uploaded_idx
CREATE INDEX manual_timesheet_queue_status_uploaded_idx ON public.manual_timesheet_queue USING btree (status, uploaded_at_utc DESC);

-- public.manual_timesheet_queue_timesheet_idx
CREATE INDEX manual_timesheet_queue_timesheet_idx ON public.manual_timesheet_queue USING btree (timesheet_id);

-- public.manual_timesheet_queue_user_idx
CREATE INDEX manual_timesheet_queue_user_idx ON public.manual_timesheet_queue USING btree (uploaded_by_user_id);

-- public.pay_bank_transfer_events_batch_received_idx
CREATE INDEX pay_bank_transfer_events_batch_received_idx ON public.pay_bank_transfer_events USING btree (pay_batch_id, received_at_utc DESC);

-- public.pay_bank_transfer_events_correction_disp_idx
CREATE INDEX pay_bank_transfer_events_correction_disp_idx ON public.pay_bank_transfer_events USING btree (correction_disposition);

-- public.pay_bank_transfer_events_failure_reason_idx
CREATE INDEX pay_bank_transfer_events_failure_reason_idx ON public.pay_bank_transfer_events USING btree (provider_failure_reason_group, received_at_utc DESC) WHERE (provider_failure_reason_group IS NOT NULL);

-- public.pay_bank_transfer_events_idempotency_key_uidx
CREATE UNIQUE INDEX pay_bank_transfer_events_idempotency_key_uidx ON public.pay_bank_transfer_events USING btree (idempotency_key);

-- public.pay_bank_transfer_events_mapping_method_idx
CREATE INDEX pay_bank_transfer_events_mapping_method_idx ON public.pay_bank_transfer_events USING btree (mapping_method);

-- public.pay_bank_transfer_events_movement_class_idx
CREATE INDEX pay_bank_transfer_events_movement_class_idx ON public.pay_bank_transfer_events USING btree (movement_classification);

-- public.pay_bank_transfer_events_normalised_state_idx
CREATE INDEX pay_bank_transfer_events_normalised_state_idx ON public.pay_bank_transfer_events USING btree (normalised_state);

-- public.pay_bank_transfer_events_provider_event_idx
CREATE INDEX pay_bank_transfer_events_provider_event_idx ON public.pay_bank_transfer_events USING btree (provider_key, provider_event_id);

-- public.pay_bank_transfer_events_provider_reference_idx
CREATE INDEX pay_bank_transfer_events_provider_reference_idx ON public.pay_bank_transfer_events USING btree (provider_reference);

-- public.pay_bank_transfer_events_provider_request_idx
CREATE INDEX pay_bank_transfer_events_provider_request_idx ON public.pay_bank_transfer_events USING btree (provider_key, provider_request_id) WHERE (provider_request_id IS NOT NULL);

-- public.pay_bank_transfer_events_provider_transaction_idx
CREATE INDEX pay_bank_transfer_events_provider_transaction_idx ON public.pay_bank_transfer_events USING btree (provider_key, provider_transaction_id) WHERE (provider_transaction_id IS NOT NULL);

-- public.pay_bank_transfer_events_provider_transport_event_uidx
CREATE UNIQUE INDEX pay_bank_transfer_events_provider_transport_event_uidx ON public.pay_bank_transfer_events USING btree (provider_key, rail_env, provider_event_transport, provider_event_key) WHERE (provider_event_key IS NOT NULL);

-- public.pay_bank_transfer_events_provider_transport_state_idx
CREATE INDEX pay_bank_transfer_events_provider_transport_state_idx ON public.pay_bank_transfer_events USING btree (pay_batch_id, provider_event_transport, normalised_state, received_at_utc DESC) WHERE (provider_event_transport IS NOT NULL);

-- public.pay_bank_transfer_events_transfer_received_idx
CREATE INDEX pay_bank_transfer_events_transfer_received_idx ON public.pay_bank_transfer_events USING btree (pay_bank_transfer_id, received_at_utc DESC);

-- public.pay_bank_transfer_events_webhook_receipt_idx
CREATE INDEX pay_bank_transfer_events_webhook_receipt_idx ON public.pay_bank_transfer_events USING btree (provider_webhook_receipt_id) WHERE (provider_webhook_receipt_id IS NOT NULL);

-- public.pay_payment_correction_actions_actor_action_idx
CREATE INDEX pay_payment_correction_actions_actor_action_idx ON public.pay_payment_correction_actions USING btree (actor_user_id, action_at_utc);

-- public.pay_payment_correction_actions_batch_action_idx
CREATE INDEX pay_payment_correction_actions_batch_action_idx ON public.pay_payment_correction_actions USING btree (pay_batch_id, action_at_utc);

-- public.pay_payment_correction_actions_request_action_idx
CREATE INDEX pay_payment_correction_actions_request_action_idx ON public.pay_payment_correction_actions USING btree (correction_request_id, action_at_utc);

-- public.pay_payment_correction_items_applied_item_kind_uidx
CREATE UNIQUE INDEX pay_payment_correction_items_applied_item_kind_uidx ON public.pay_payment_correction_items USING btree (pay_batch_item_id, correction_item_kind) WHERE ((status = 'APPLIED'::text) AND (pay_batch_item_id IS NOT NULL));

-- public.pay_payment_correction_items_batch_candidate_idx
CREATE INDEX pay_payment_correction_items_batch_candidate_idx ON public.pay_payment_correction_items USING btree (pay_batch_id, pay_batch_candidate_id);

-- public.pay_payment_correction_items_candidate_idx
CREATE INDEX pay_payment_correction_items_candidate_idx ON public.pay_payment_correction_items USING btree (candidate_id);

-- public.pay_payment_correction_items_finance_case_idx
CREATE INDEX pay_payment_correction_items_finance_case_idx ON public.pay_payment_correction_items USING btree (finance_case_id);

-- public.pay_payment_correction_items_finance_component_idx
CREATE INDEX pay_payment_correction_items_finance_component_idx ON public.pay_payment_correction_items USING btree (finance_component_id);

-- public.pay_payment_correction_items_item_idx
CREATE INDEX pay_payment_correction_items_item_idx ON public.pay_payment_correction_items USING btree (pay_batch_item_id);

-- public.pay_payment_correction_items_request_idx
CREATE INDEX pay_payment_correction_items_request_idx ON public.pay_payment_correction_items USING btree (correction_request_id);

-- public.pay_payment_correction_items_reservation_idx
CREATE INDEX pay_payment_correction_items_reservation_idx ON public.pay_payment_correction_items USING btree (reservation_id);

-- public.pay_payment_correction_items_timesheet_key_idx
CREATE INDEX pay_payment_correction_items_timesheet_key_idx ON public.pay_payment_correction_items USING btree (timesheet_id, economic_key_type, economic_key_value);

-- public.pay_payment_correction_items_transfer_idx
CREATE INDEX pay_payment_correction_items_transfer_idx ON public.pay_payment_correction_items USING btree (pay_bank_transfer_id);

-- public.pay_payment_correction_requests_batch_status_idx
CREATE INDEX pay_payment_correction_requests_batch_status_idx ON public.pay_payment_correction_requests USING btree (pay_batch_id, status);

-- public.pay_payment_correction_requests_open_scope_uidx
CREATE UNIQUE INDEX pay_payment_correction_requests_open_scope_uidx ON public.pay_payment_correction_requests USING btree (pay_batch_id, selection_hash, correction_kind) WHERE (status = ANY (ARRAY['REQUESTED'::text, 'AWAITING_AUTHORISATION'::text, 'AUTHORISED'::text, 'EXPANDED'::text, 'PROCESSING'::text]));

-- public.pay_payment_correction_requests_source_event_idx
CREATE INDEX pay_payment_correction_requests_source_event_idx ON public.pay_payment_correction_requests USING btree (source_bank_event_id);

-- public.pay_payment_correction_requests_status_created_idx
CREATE INDEX pay_payment_correction_requests_status_created_idx ON public.pay_payment_correction_requests USING btree (status, created_at_utc);

-- public.pay_payment_correction_work_items_batch_idx
CREATE INDEX pay_payment_correction_work_items_batch_idx ON public.pay_payment_correction_work_items USING btree (pay_batch_id);

-- public.pay_payment_correction_work_items_candidate_idx
CREATE INDEX pay_payment_correction_work_items_candidate_idx ON public.pay_payment_correction_work_items USING btree (candidate_id);

-- public.pay_payment_correction_work_items_processed_by_user_id_idx
CREATE INDEX pay_payment_correction_work_items_processed_by_user_id_idx ON public.pay_payment_correction_work_items USING btree (processed_by_user_id);

-- public.pay_payment_correction_work_items_request_status_idx
CREATE INDEX pay_payment_correction_work_items_request_status_idx ON public.pay_payment_correction_work_items USING btree (correction_request_id, status);

-- public.pay_payment_correction_work_items_scope_uidx
CREATE UNIQUE INDEX pay_payment_correction_work_items_scope_uidx ON public.pay_payment_correction_work_items USING btree (correction_request_id, work_kind, selection_hash);

-- public.pay_payment_correction_work_items_status_created_idx
CREATE INDEX pay_payment_correction_work_items_status_created_idx ON public.pay_payment_correction_work_items USING btree (status, created_at_utc);

-- public.pay_payment_correction_work_items_transfer_idx
CREATE INDEX pay_payment_correction_work_items_transfer_idx ON public.pay_payment_correction_work_items USING btree (pay_bank_transfer_id);

-- public.pay_payment_correction_work_items_umbrella_idx
CREATE INDEX pay_payment_correction_work_items_umbrella_idx ON public.pay_payment_correction_work_items USING btree (umbrella_id);

-- public.pay_payment_return_notice_groups_batch_idx
CREATE INDEX pay_payment_return_notice_groups_batch_idx ON public.pay_payment_return_notice_groups USING btree (pay_batch_id);

-- public.pay_payment_return_notice_groups_provider_commit_idx
CREATE INDEX pay_payment_return_notice_groups_provider_commit_idx ON public.pay_payment_return_notice_groups USING btree (provider_key, execution_commit_ref);

-- public.pay_payment_return_notice_groups_status_max_idx
CREATE INDEX pay_payment_return_notice_groups_status_max_idx ON public.pay_payment_return_notice_groups USING btree (status, max_send_at_utc);

-- public.pay_payment_return_notice_groups_status_quiet_idx
CREATE INDEX pay_payment_return_notice_groups_status_quiet_idx ON public.pay_payment_return_notice_groups USING btree (status, quiet_until_utc);

-- public.rates_candidate_overrides_uniq
CREATE UNIQUE INDEX rates_candidate_overrides_uniq ON public.rates_candidate_overrides USING btree (candidate_id, client_id, role, COALESCE(band, ''::text), date_from, rate_type);

-- public.rates_client_defaults_uniq_window_enabled
CREATE UNIQUE INDEX rates_client_defaults_uniq_window_enabled ON public.rates_client_defaults USING btree (client_id, role, COALESCE(band, ''::text), date_from) WHERE (disabled_at_utc IS NULL);

-- public.rcd_active_bandnull_enabled_idx
CREATE INDEX rcd_active_bandnull_enabled_idx ON public.rates_client_defaults USING btree (client_id, role, date_from DESC) INCLUDE (date_to) WHERE ((band IS NULL) AND (disabled_at_utc IS NULL));

-- public.rcd_active_bandnull_idx
CREATE INDEX rcd_active_bandnull_idx ON public.rates_client_defaults USING btree (client_id, role, date_from DESC) WHERE (band IS NULL);

-- public.rcd_active_exact_enabled_idx
CREATE INDEX rcd_active_exact_enabled_idx ON public.rates_client_defaults USING btree (client_id, role, band, date_from DESC) INCLUDE (date_to) WHERE (disabled_at_utc IS NULL);

-- public.rcd_active_exact_idx
CREATE INDEX rcd_active_exact_idx ON public.rates_client_defaults USING btree (client_id, role, band, date_from DESC);

-- public.rcd_enabled_date_span_idx
CREATE INDEX rcd_enabled_date_span_idx ON public.rates_client_defaults USING btree (client_id, role, COALESCE(band, ''::text), date_from, date_to) WHERE (disabled_at_utc IS NULL);

-- public.rco_active_exact_idx
CREATE INDEX rco_active_exact_idx ON public.rates_candidate_overrides USING btree (candidate_id, client_id, rate_type, role, band, date_from DESC) INCLUDE (date_to, pay_day, pay_night, pay_sat, pay_sun, pay_bh);

-- public.rco_candidate_type_client_role_band_from_idx
CREATE INDEX rco_candidate_type_client_role_band_from_idx ON public.rates_candidate_overrides USING btree (candidate_id, rate_type, client_id, role, band, date_from DESC);

-- public.rpt_presets_section_kind_shared_idx
CREATE INDEX rpt_presets_section_kind_shared_idx ON public.report_presets USING btree (section, kind, is_shared, created_at DESC);

-- public.rpt_presets_user_section_kind_created_idx
CREATE INDEX rpt_presets_user_section_kind_created_idx ON public.report_presets USING btree (user_id, section, kind, created_at DESC);

-- public.settings_finance_windows_date_from_desc_idx
CREATE INDEX settings_finance_windows_date_from_desc_idx ON public.settings_finance_windows USING btree (date_from DESC);

-- public.timesheet_evidence_candidate_component_uq
CREATE UNIQUE INDEX timesheet_evidence_candidate_component_uq ON public.timesheet_evidence USING btree (candidate_component_id) WHERE (candidate_component_id IS NOT NULL);

-- public.timesheet_evidence_one_active_timesheet_uq
CREATE UNIQUE INDEX timesheet_evidence_one_active_timesheet_uq ON public.timesheet_evidence USING btree (timesheet_id) WHERE ((upper(btrim(kind)) = 'TIMESHEET'::text) AND (processing_state <> 'SUPERSEDED'::text));

-- public.timesheet_r2_cleanup_queue_pending_idx
CREATE INDEX timesheet_r2_cleanup_queue_pending_idx ON public.timesheet_r2_cleanup_queue USING btree (next_attempt_at_utc, last_attempt_at_utc, delete_operation_id, r2_key) WHERE (status = ANY (ARRAY['PENDING'::text, 'IN_PROGRESS'::text]));

-- public.timesheets_booking_id_current_uidx
CREATE UNIQUE INDEX timesheets_booking_id_current_uidx ON public.timesheets USING btree (booking_id) WHERE is_current;

-- public.timesheets_booking_id_version_uidx
CREATE UNIQUE INDEX timesheets_booking_id_version_uidx ON public.timesheets USING btree (booking_id, version);

-- public.timesheets_candidate_workflow_idx
CREATE INDEX timesheets_candidate_workflow_idx ON public.timesheets USING btree (candidate_workflow_id, candidate_workflow_generation) WHERE (candidate_workflow_id IS NOT NULL);

-- public.timesheets_generated_pdf_refs_sig_idx
CREATE INDEX timesheets_generated_pdf_refs_sig_idx ON public.timesheets USING btree (generated_pdf_refs_sig);

-- public.timesheets_manual_adjustment_idempotency_uq
CREATE UNIQUE INDEX timesheets_manual_adjustment_idempotency_uq ON public.timesheets USING btree (parent_timesheet_id, idempotency_key) WHERE ((adjustment_origin = 'MANUAL_ADJUSTMENT'::text) AND (parent_timesheet_id IS NOT NULL) AND (NULLIF(btrim(idempotency_key), ''::text) IS NOT NULL));

-- public.timesheets_qr_sent_refs_sig_idx
CREATE INDEX timesheets_qr_sent_refs_sig_idx ON public.timesheets USING btree (qr_sent_refs_sig);

-- public.tms_users_last_login_at_utc_idx
CREATE INDEX tms_users_last_login_at_utc_idx ON public.tms_users USING btree (last_login_at_utc DESC);

-- public.ts_pay_adjustments_candidate_week_idx
CREATE INDEX ts_pay_adjustments_candidate_week_idx ON public.ts_pay_adjustments USING btree (candidate_id, week_ending_date);

-- public.ts_pay_adjustments_meta_import_id_idx
CREATE INDEX ts_pay_adjustments_meta_import_id_idx ON public.ts_pay_adjustments USING btree (((meta_json ->> 'import_id'::text)));

-- public.ts_pay_adjustments_meta_json_gin_idx
CREATE INDEX ts_pay_adjustments_meta_json_gin_idx ON public.ts_pay_adjustments USING gin (meta_json);

-- public.ts_pay_adjustments_meta_shift_id_idx
CREATE INDEX ts_pay_adjustments_meta_shift_id_idx ON public.ts_pay_adjustments USING btree (((meta_json ->> 'shift_id'::text)));

-- public.ts_pay_adjustments_timesheet_idx
CREATE INDEX ts_pay_adjustments_timesheet_idx ON public.ts_pay_adjustments USING btree (timesheet_id);

-- public.uq_banking_alert_ack_user_fingerprint_scope
CREATE UNIQUE INDEX uq_banking_alert_ack_user_fingerprint_scope ON public.banking_alert_acknowledgements USING btree (alert_fingerprint, acknowledged_by_user_id, acknowledge_scope);

-- public.uq_banking_alert_display_summary_actor
CREATE UNIQUE INDEX uq_banking_alert_display_summary_actor ON public.banking_alert_display_summary USING btree (actor_user_id);

-- public.uq_bpay_op_scope_units_operation_phase_type_key
CREATE UNIQUE INDEX uq_bpay_op_scope_units_operation_phase_type_key ON public.banking_pay_operation_scope_units USING btree (operation_id, phase, unit_type, unit_key);

-- public.uq_bpay_op_transfer_items_scope_item
CREATE UNIQUE INDEX uq_bpay_op_transfer_items_scope_item ON public.banking_pay_operation_transfer_scope_items USING btree (operation_id, transfer_scope_id, pay_batch_item_id);

-- public.uq_bpay_wb_line_work_session_candidate_timesheet_line
CREATE UNIQUE INDEX uq_bpay_wb_line_work_session_candidate_timesheet_line ON public.banking_pay_workbench_candidate_line_work USING btree (session_id, candidate_id, COALESCE(timesheet_id, '00000000-0000-0000-0000-000000000000'::uuid), line_key);

-- public.uq_bpay_wb_preview_session_section_candidate_row
CREATE UNIQUE INDEX uq_bpay_wb_preview_session_section_candidate_row ON public.banking_pay_workbench_preview_rows USING btree (session_id, section, candidate_id, row_key);

-- public.uq_bpay_wb_scope_session_candidate
CREATE UNIQUE INDEX uq_bpay_wb_scope_session_candidate ON public.banking_pay_workbench_session_scope USING btree (session_id, candidate_id);

-- public.uq_bpay_wb_source_current_identity
CREATE UNIQUE INDEX uq_bpay_wb_source_current_identity ON public.banking_pay_workbench_candidate_source_lines USING btree (session_id, candidate_id, session_version, source_change_seq, source_build_run_id, COALESCE(timesheet_id, '00000000-0000-0000-0000-000000000000'::uuid), line_key) WHERE (status = 'CURRENT'::text);

-- public.uq_bpay_wb_source_publication_line_v1
CREATE UNIQUE INDEX uq_bpay_wb_source_publication_line_v1 ON public.banking_pay_workbench_candidate_source_lines USING btree (source_publication_id, COALESCE(timesheet_id, '00000000-0000-0000-0000-000000000000'::uuid), line_key) WHERE (source_publication_id IS NOT NULL);

-- public.uq_bpay_wb_source_publication_ordinal_v1
CREATE UNIQUE INDEX uq_bpay_wb_source_publication_ordinal_v1 ON public.banking_pay_workbench_candidate_source_lines USING btree (source_publication_id, source_ordinal) WHERE (source_publication_id IS NOT NULL);

-- public.uq_candidates_key_norm
CREATE UNIQUE INDEX uq_candidates_key_norm ON public.candidates USING btree (key_norm) WHERE (key_norm IS NOT NULL);

-- public.uq_hr_name_mappings_active
CREATE UNIQUE INDEX uq_hr_name_mappings_active ON public.hr_name_mappings USING btree (hr_name_norm, COALESCE(hospital_or_trust, ''::text)) WHERE (active = true);

-- public.uq_hr_rows_import_external_row_key_nonblank
CREATE UNIQUE INDEX uq_hr_rows_import_external_row_key_nonblank ON public.hr_rows USING btree (import_id, external_row_key) WHERE ((external_row_key IS NOT NULL) AND (btrim(external_row_key) <> ''::text));

-- public.uq_invoice_document_assets_source
CREATE UNIQUE INDEX uq_invoice_document_assets_source ON public.invoice_document_assets USING btree (source_kind, source_id, source_revision, original_r2_key);

-- public.uq_invoice_document_versions_active_final_snapshot
CREATE UNIQUE INDEX uq_invoice_document_versions_active_final_snapshot ON public.invoice_document_versions USING btree (entity_type, entity_id, purpose, snapshot_hash, template_version) WHERE ((purpose = 'FINAL_ISSUE'::text) AND (status = ANY (ARRAY['PLANNING'::text, 'WAITING_FOR_INPUTS'::text, 'RENDERING'::text, 'ASSEMBLING'::text, 'VERIFYING'::text, 'READY'::text])));

-- public.uq_invoice_document_versions_active_revision
CREATE UNIQUE INDEX uq_invoice_document_versions_active_revision ON public.invoice_document_versions USING btree (entity_type, entity_id, purpose, source_revision, template_version) WHERE ((purpose = ANY (ARRAY['DRAFT_PREVIEW'::text, 'TIMESHEET'::text])) AND (status = ANY (ARRAY['PLANNING'::text, 'WAITING_FOR_INPUTS'::text, 'RENDERING'::text, 'ASSEMBLING'::text, 'VERIFYING'::text, 'READY'::text])));

-- public.uq_invoice_jobs_outbox_week
CREATE UNIQUE INDEX uq_invoice_jobs_outbox_week ON public.invoice_jobs_outbox USING btree (kind, ((payload ->> 'client_id'::text)), ((payload ->> 'invoice_week_start'::text))) WHERE ((payload ? 'client_id'::text) AND (payload ? 'invoice_week_start'::text));

-- public.uq_invoice_lines_invoice_source_key
CREATE UNIQUE INDEX uq_invoice_lines_invoice_source_key ON public.invoice_lines USING btree (invoice_id, source_key);

-- public.uq_invoice_operation_chunks_active_issue_invoice
CREATE UNIQUE INDEX uq_invoice_operation_chunks_active_issue_invoice ON public.invoice_operation_chunks USING btree (entity_id) WHERE ((chunk_type = 'ISSUE_INVOICE'::text) AND (entity_type = 'INVOICE'::text) AND (status = ANY (ARRAY['QUEUED'::text, 'RUNNING'::text, 'WAITING'::text, 'RETRY_WAIT'::text])));

-- public.uq_invoice_operations_active_idempotency
CREATE UNIQUE INDEX uq_invoice_operations_active_idempotency ON public.invoice_operations USING btree (idempotency_key) WHERE (status = ANY (ARRAY['QUEUED'::text, 'RUNNING'::text, 'WAITING'::text, 'RETRY_WAIT'::text, 'BLOCKED'::text]));

-- public.uq_invoice_operations_delivery_identity
CREATE UNIQUE INDEX uq_invoice_operations_delivery_identity ON public.invoice_operations USING btree (idempotency_key) WHERE ((operation_type = 'DELIVER_INVOICES'::text) AND (status = ANY (ARRAY['QUEUED'::text, 'RUNNING'::text, 'WAITING'::text, 'RETRY_WAIT'::text, 'BLOCKED'::text, 'COMPLETE'::text])));

-- public.uq_invoice_pdf_outbox_invoice_reason
CREATE UNIQUE INDEX uq_invoice_pdf_outbox_invoice_reason ON public.invoice_pdf_outbox USING btree (invoice_id, reason);

-- public.uq_mail_outbox_invoice_delivery_identity
CREATE UNIQUE INDEX uq_mail_outbox_invoice_delivery_identity ON public.mail_outbox USING btree (reference) WHERE (reference ~~ 'INVOICE_DELIVERY_V1:%'::text);

-- public.uq_manual_timesheet_queue_one_active_staged_timesheet_per_contr
CREATE UNIQUE INDEX uq_manual_timesheet_queue_one_active_staged_timesheet_per_contr ON public.manual_timesheet_queue USING btree (NULLIF(btrim((meta_json ->> 'contract_week_id'::text)), ''::text)) WHERE ((status = 'STAGED'::text) AND (NULLIF(btrim((meta_json ->> 'contract_week_id'::text)), ''::text) IS NOT NULL) AND (upper(COALESCE(NULLIF(btrim((meta_json ->> 'staged_kind'::text)), ''::text), NULLIF(btrim((meta_json ->> 'kind'::text)), ''::text), NULLIF(btrim((meta_json ->> 'attached_kind'::text)), ''::text), 'TIMESHEET'::text)) = 'TIMESHEET'::text));

-- public.uq_pay_advance_reservations_pay_batch_item_id
CREATE UNIQUE INDEX uq_pay_advance_reservations_pay_batch_item_id ON public.pay_advance_reservations USING btree (pay_batch_item_id) WHERE (pay_batch_item_id IS NOT NULL);

-- public.uq_pay_advances_overpayment_case
CREATE UNIQUE INDEX uq_pay_advances_overpayment_case ON public.pay_advances USING btree (candidate_id, linked_timesheet_id, baseline_signature) WHERE ((advance_kind = 'OVERPAYMENT'::pay_advance_kind_enum) AND (status = ANY (ARRAY['ACTIVE'::pay_advance_status_enum, 'PAID_OFF'::pay_advance_status_enum])));

-- public.uq_pay_advances_underpayment_case
CREATE UNIQUE INDEX uq_pay_advances_underpayment_case ON public.pay_advances USING btree (candidate_id, linked_timesheet_id, baseline_signature) WHERE ((advance_kind = 'UNDERPAYMENT'::pay_advance_kind_enum) AND (status = ANY (ARRAY['ACTIVE'::pay_advance_status_enum, 'PAID_OFF'::pay_advance_status_enum])));

-- public.uq_pay_batch_display_summary_batch
CREATE UNIQUE INDEX uq_pay_batch_display_summary_batch ON public.pay_batch_display_summary USING btree (pay_batch_id);

-- public.uq_pay_finance_case_components_active_identity
CREATE UNIQUE INDEX uq_pay_finance_case_components_active_identity ON public.pay_finance_case_components USING btree (finance_case_id, source_family_key, component_key_type, component_key_value) WHERE (closed_at_utc IS NULL);

-- public.uq_pay_item_snoozes_active
CREATE UNIQUE INDEX uq_pay_item_snoozes_active ON public.pay_item_snoozes USING btree (candidate_id, timesheet_id, segment_id, source_ref, snooze_kind) WHERE (cleared_at_utc IS NULL);

-- public.uq_pay_item_snoozes_active_booking_identity
CREATE UNIQUE INDEX uq_pay_item_snoozes_active_booking_identity ON public.pay_item_snoozes USING btree (candidate_id, booking_id, COALESCE(segment_stable_key, ''::text), snooze_kind) WHERE ((cleared_at_utc IS NULL) AND (source_ref IS NULL) AND (booking_id IS NOT NULL));

-- public.uq_pay_item_snoozes_active_source_identity
CREATE UNIQUE INDEX uq_pay_item_snoozes_active_source_identity ON public.pay_item_snoozes USING btree (source_ref) WHERE ((cleared_at_utc IS NULL) AND (source_ref IS NOT NULL));

-- public.uq_pay_item_snoozes_active_timesheet_identity
CREATE UNIQUE INDEX uq_pay_item_snoozes_active_timesheet_identity ON public.pay_item_snoozes USING btree (timesheet_id, COALESCE(segment_id, ''::text)) WHERE ((cleared_at_utc IS NULL) AND (source_ref IS NULL) AND (timesheet_id IS NOT NULL));

-- public.uq_pay_payment_return_notice_groups_notice_kind_alert_fingerpri
CREATE UNIQUE INDEX uq_pay_payment_return_notice_groups_notice_kind_alert_fingerpri ON public.pay_payment_return_notice_groups USING btree (notice_kind, alert_fingerprint) WHERE ((alert_fingerprint IS NOT NULL) AND (btrim(alert_fingerprint) <> ''::text));

-- public.uq_rates_preset_name
CREATE UNIQUE INDEX uq_rates_preset_name ON public.rates_presets USING btree (scope, COALESCE(client_id, '00000000-0000-0000-0000-000000000000'::uuid), role, COALESCE(band, ''::text), name);

-- public.uq_timesheet_payment_overrides_active_one
CREATE UNIQUE INDEX uq_timesheet_payment_overrides_active_one ON public.timesheet_payment_overrides USING btree (timesheet_id) WHERE ((cleared_at_utc IS NULL) AND (consumed_at_utc IS NULL) AND (consumed_by_pay_batch_id IS NULL));

-- public.uq_timesheet_validations_booking
CREATE UNIQUE INDEX uq_timesheet_validations_booking ON public.timesheet_validations USING btree (booking_id) WHERE (booking_id IS NOT NULL);

-- public.uq_timesheet_validations_timesheet
CREATE UNIQUE INDEX uq_timesheet_validations_timesheet ON public.timesheet_validations USING btree (timesheet_id) WHERE (timesheet_id IS NOT NULL);

-- public.uq_timesheet_validations_timesheet_id
CREATE UNIQUE INDEX uq_timesheet_validations_timesheet_id ON public.timesheet_validations USING btree (timesheet_id);

-- public.uq_timesheets_correction_id_kind
CREATE UNIQUE INDEX uq_timesheets_correction_id_kind ON public.timesheets USING btree (correction_id, correction_kind) WHERE (correction_id IS NOT NULL);

-- public.uq_timesheets_idempotency_key
CREATE UNIQUE INDEX uq_timesheets_idempotency_key ON public.timesheets USING btree (idempotency_key) WHERE (idempotency_key IS NOT NULL);

-- public.uq_ts_pdfs_outbox_timesheet_reason
CREATE UNIQUE INDEX uq_ts_pdfs_outbox_timesheet_reason ON public.ts_pdfs_outbox USING btree (timesheet_id, reason);

-- public.uq_tsfin_current
CREATE UNIQUE INDEX uq_tsfin_current ON public.timesheets_financials USING btree (timesheet_id) WHERE is_current;

-- public.ux_bank_name_checks_key
CREATE UNIQUE INDEX ux_bank_name_checks_key ON public.bank_name_checks USING btree (rail_provider, rail_env, entity_kind, entity_id, bank_details_hash);

-- public.ux_bank_payee_map_key
CREATE UNIQUE INDEX ux_bank_payee_map_key ON public.bank_payee_map USING btree (rail_provider, rail_env, entity_kind, entity_id, bank_details_hash);

-- public.ux_banking_pay_operation_candidate_allocation_source
CREATE UNIQUE INDEX ux_banking_pay_operation_candidate_allocation_source ON public.banking_pay_operation_candidate_allocation_rows USING btree (operation_id, operation_source_key);

-- public.ux_banking_pay_operation_candidate_scope_identity
CREATE UNIQUE INDEX ux_banking_pay_operation_candidate_scope_identity ON public.banking_pay_operation_candidate_scope USING btree (operation_id, candidate_id, pay_channel);

-- public.ux_banking_pay_operation_chunks_sequence
CREATE UNIQUE INDEX ux_banking_pay_operation_chunks_sequence ON public.banking_pay_operation_chunks USING btree (operation_id, phase, chunk_type, sequence_no);

-- public.ux_banking_pay_operation_config_scope
CREATE UNIQUE INDEX ux_banking_pay_operation_config_scope ON public.banking_pay_operation_config USING btree (operation_type, phase, chunk_type);

-- public.ux_banking_pay_operation_remittance_scope_outbox_key
CREATE UNIQUE INDEX ux_banking_pay_operation_remittance_scope_outbox_key ON public.banking_pay_operation_remittance_scope USING btree (operation_id, deterministic_outbox_key);

-- public.ux_banking_pay_operation_settlement_scope_key
CREATE UNIQUE INDEX ux_banking_pay_operation_settlement_scope_key ON public.banking_pay_operation_settlement_scope USING btree (operation_id, settlement_key);

-- public.ux_banking_pay_operation_transfer_scope_batch_group
CREATE UNIQUE INDEX ux_banking_pay_operation_transfer_scope_batch_group ON public.banking_pay_operation_transfer_scope USING btree (pay_batch_id, pay_channel, transfer_group_key);

-- public.ux_banking_pay_operation_transfer_scope_operation_group
CREATE UNIQUE INDEX ux_banking_pay_operation_transfer_scope_operation_group ON public.banking_pay_operation_transfer_scope USING btree (operation_id, pay_channel, transfer_group_key);

-- public.ux_banking_pay_operations_active_idempotency
CREATE UNIQUE INDEX ux_banking_pay_operations_active_idempotency ON public.banking_pay_operations USING btree (idempotency_key) WHERE (status <> ALL (ARRAY['COMPLETE'::text, 'FAILED'::text, 'CANCELLED'::text, 'REVIEW_REQUIRED'::text]));

-- public.ux_banking_pay_operations_correction_request
CREATE UNIQUE INDEX ux_banking_pay_operations_correction_request ON public.banking_pay_operations USING btree (((input_json ->> 'correction_request_id'::text))) WHERE ((operation_type = 'PAYMENT_CORRECTION'::text) AND (input_json ? 'correction_request_id'::text));

-- public.ux_bpay_case_resolution_carry_target_candidate_canonical_carrie
CREATE UNIQUE INDEX ux_bpay_case_resolution_carry_target_candidate_canonical_carrie ON public.banking_pay_workbench_case_resolution_carry_registrations USING btree (target_session_id, candidate_id, canonical_resolution_key) WHERE (status = 'CARRIED'::text);

-- public.ux_bpay_case_resolution_carry_target_source_resolution
CREATE UNIQUE INDEX ux_bpay_case_resolution_carry_target_source_resolution ON public.banking_pay_workbench_case_resolution_carry_registrations USING btree (target_session_id, source_resolution_id);

-- public.ux_bpay_selection_carry_target_key_applied
CREATE UNIQUE INDEX ux_bpay_selection_carry_target_key_applied ON public.banking_pay_workbench_selection_carry_registrations USING btree (target_session_id, candidate_id, stable_selection_key) WHERE (status = 'APPLIED'::text);

-- public.ux_bpay_selection_carry_target_source_preview
CREATE UNIQUE INDEX ux_bpay_selection_carry_target_source_preview ON public.banking_pay_workbench_selection_carry_registrations USING btree (target_session_id, source_preview_row_id);

-- public.ux_bpay_session_candidate_state_session_candidate
CREATE UNIQUE INDEX ux_bpay_session_candidate_state_session_candidate ON public.banking_pay_workbench_session_candidate_state USING btree (session_id, candidate_id);

-- public.ux_bpay_session_case_resolutions_identity
CREATE UNIQUE INDEX ux_bpay_session_case_resolutions_identity ON public.banking_pay_workbench_session_case_resolutions USING btree (session_id, resolution_identity_key);

-- public.ux_bpay_session_overrides_identity
CREATE UNIQUE INDEX ux_bpay_session_overrides_identity ON public.banking_pay_workbench_session_overrides USING btree (session_id, override_identity_key);

-- public.ux_bpay_snapshot_candidate_state_run_candidate
CREATE UNIQUE INDEX ux_bpay_snapshot_candidate_state_run_candidate ON public.banking_pay_snapshot_candidate_state USING btree (snapshot_run_id, candidate_id);

-- public.ux_bpay_snapshot_case_state_run_candidate_case
CREATE UNIQUE INDEX ux_bpay_snapshot_case_state_run_candidate_case ON public.banking_pay_snapshot_case_state USING btree (snapshot_run_id, candidate_id, case_key);

-- public.ux_bpay_snapshot_line_state_run_candidate_preview_row
CREATE UNIQUE INDEX ux_bpay_snapshot_line_state_run_candidate_preview_row ON public.banking_pay_snapshot_line_state USING btree (snapshot_run_id, candidate_id, preview_row_id);

-- public.ux_bpay_snapshot_runs_active_cycle
CREATE UNIQUE INDEX ux_bpay_snapshot_runs_active_cycle ON public.banking_pay_snapshot_runs USING btree (pay_date, week_ending_cutoff) WHERE (is_active = true);

-- public.ux_bpay_workbench_jobs_active_dedupe
CREATE UNIQUE INDEX ux_bpay_workbench_jobs_active_dedupe ON public.banking_pay_workbench_jobs USING btree (dedupe_key) WHERE (status = ANY (ARRAY['QUEUED'::text, 'RUNNING'::text]));

-- public.ux_bpay_workbench_sessions_actor_signature_open
CREATE UNIQUE INDEX ux_bpay_workbench_sessions_actor_signature_open ON public.banking_pay_workbench_sessions USING btree (actor_user_id, session_signature) WHERE (status = 'OPEN'::text);

-- public.ux_bpay_workbench_sessions_replacement_idempotency_key
CREATE UNIQUE INDEX ux_bpay_workbench_sessions_replacement_idempotency_key ON public.banking_pay_workbench_sessions USING btree (replacement_idempotency_key) WHERE (replacement_idempotency_key IS NOT NULL);

-- public.ux_bpay_workbench_sessions_shared_context_open
CREATE UNIQUE INDEX ux_bpay_workbench_sessions_shared_context_open ON public.banking_pay_workbench_sessions USING btree (session_signature, pay_date, week_ending_cutoff) WHERE ((status = 'OPEN'::text) AND (discarded_at_utc IS NULL));

-- public.ux_comms_outbox_deterministic_outbox_key
CREATE UNIQUE INDEX ux_comms_outbox_deterministic_outbox_key ON public.comms_outbox USING btree (deterministic_outbox_key) WHERE (deterministic_outbox_key IS NOT NULL);

-- public.ux_mail_outbox_deterministic_outbox_key
CREATE UNIQUE INDEX ux_mail_outbox_deterministic_outbox_key ON public.mail_outbox USING btree (deterministic_outbox_key) WHERE (deterministic_outbox_key IS NOT NULL);

-- public.ux_mail_outbox_deterministic_outbox_key_full
CREATE UNIQUE INDEX ux_mail_outbox_deterministic_outbox_key_full ON public.mail_outbox USING btree (deterministic_outbox_key);

-- public.ux_pay_bank_transfers_group_key
CREATE UNIQUE INDEX ux_pay_bank_transfers_group_key ON public.pay_bank_transfers USING btree (pay_batch_id, pay_channel, transfer_group_key);

-- public.ux_pay_batch_auth_requests_one_active
CREATE UNIQUE INDEX ux_pay_batch_auth_requests_one_active ON public.pay_batch_auth_requests USING btree (pay_batch_id) WHERE (state = 'AWAITING'::text);

-- public.ux_pay_batch_candidates_batch_candidate
CREATE UNIQUE INDEX ux_pay_batch_candidates_batch_candidate ON public.pay_batch_candidates USING btree (pay_batch_id, candidate_id);

-- public.ux_pay_batch_item_breakdowns_item_operation_source_key
CREATE UNIQUE INDEX ux_pay_batch_item_breakdowns_item_operation_source_key ON public.pay_batch_item_breakdowns USING btree (pay_batch_item_id, operation_source_key) WHERE (operation_source_key IS NOT NULL);

-- public.ux_pay_batch_items_candidate_operation_source_key
CREATE UNIQUE INDEX ux_pay_batch_items_candidate_operation_source_key ON public.pay_batch_items USING btree (pay_batch_candidate_id, operation_source_key) WHERE (operation_source_key IS NOT NULL);

-- public.ux_pay_batch_timesheet_snapshots_key
CREATE UNIQUE INDEX ux_pay_batch_timesheet_snapshots_key ON public.pay_batch_timesheet_snapshots USING btree (pay_batch_id, timesheet_id, pay_channel);

-- public.ux_pay_batches_bulk_ref_num
CREATE UNIQUE INDEX ux_pay_batches_bulk_ref_num ON public.pay_batches USING btree (bulk_ref_num) WHERE (bulk_ref_num IS NOT NULL);

-- public.ux_pay_batches_bulk_reference
CREATE UNIQUE INDEX ux_pay_batches_bulk_reference ON public.pay_batches USING btree (bulk_reference) WHERE (bulk_reference IS NOT NULL);

-- public.ux_pay_manual_adjustment_carry_forwards_source_item
CREATE UNIQUE INDEX ux_pay_manual_adjustment_carry_forwards_source_item ON public.pay_manual_adjustment_carry_forwards USING btree (source_pay_batch_item_id);

-- public.ux_pay_manual_adjustment_carry_forwards_target_item
CREATE UNIQUE INDEX ux_pay_manual_adjustment_carry_forwards_target_item ON public.pay_manual_adjustment_carry_forwards USING btree (target_pay_batch_item_id) WHERE (target_pay_batch_item_id IS NOT NULL);

-- public.ux_pay_payment_correction_requests_active_batch_v2
CREATE UNIQUE INDEX ux_pay_payment_correction_requests_active_batch_v2 ON public.pay_payment_correction_requests USING btree (pay_batch_id) WHERE (status = ANY (ARRAY['REQUESTED'::text, 'AWAITING_AUTHORISATION'::text, 'AUTHORISED'::text, 'EXPANDED'::text, 'PROCESSING'::text]));

-- public.ux_pay_snooze_warning_ack_token_hash
CREATE UNIQUE INDEX ux_pay_snooze_warning_ack_token_hash ON public.pay_snooze_warning_acknowledgements USING btree (token_hash);

-- public.migration_smoke_once_only_id_seq
alter sequence public.migration_smoke_once_only_id_seq owned by public.migration_smoke_once_only.id;

-- private.candidate_daily_authority_scopes
alter table private.candidate_daily_authority_scopes enable row level security;

-- private.candidate_daily_authority_transitions
alter table private.candidate_daily_authority_transitions enable row level security;

-- private.candidate_daily_batch_receipts
alter table private.candidate_daily_batch_receipts enable row level security;

-- private.candidate_daily_entitlements
alter table private.candidate_daily_entitlements enable row level security;

-- private.candidate_daily_external_effect_receipts
alter table private.candidate_daily_external_effect_receipts enable row level security;

-- private.candidate_daily_source_links
alter table private.candidate_daily_source_links enable row level security;

-- private.candidate_daily_sync_state
alter table private.candidate_daily_sync_state enable row level security;

-- public.app_change_counters
alter table public.app_change_counters enable row level security;

-- public.assignment_band_mappings
alter table public.assignment_band_mappings enable row level security;

-- public.audit_events
alter table public.audit_events enable row level security;

-- public.bank_name_checks
alter table public.bank_name_checks enable row level security;

-- public.bank_payee_map
alter table public.bank_payee_map enable row level security;

-- public.bank_provider_webhook_configs
alter table public.bank_provider_webhook_configs enable row level security;

-- public.bank_provider_webhook_receipts
alter table public.bank_provider_webhook_receipts enable row level security;

-- public.banking_alert_acknowledgements
alter table public.banking_alert_acknowledgements enable row level security;

-- public.banking_alert_display_summary
alter table public.banking_alert_display_summary enable row level security;

-- public.banking_alert_success_events
alter table public.banking_alert_success_events enable row level security;

-- public.banking_alert_user_preferences
alter table public.banking_alert_user_preferences enable row level security;

-- public.banking_pay_batch_change_signals
alter table public.banking_pay_batch_change_signals enable row level security;

-- public.banking_pay_operation_candidate_allocation_rows
alter table public.banking_pay_operation_candidate_allocation_rows enable row level security;

-- public.banking_pay_operation_candidate_scope
alter table public.banking_pay_operation_candidate_scope enable row level security;

-- public.banking_pay_operation_chunks
alter table public.banking_pay_operation_chunks enable row level security;

-- public.banking_pay_operation_config
alter table public.banking_pay_operation_config enable row level security;

-- public.banking_pay_operation_provider_attempts
alter table public.banking_pay_operation_provider_attempts enable row level security;

-- public.banking_pay_operation_remittance_scope
alter table public.banking_pay_operation_remittance_scope enable row level security;

-- public.banking_pay_operation_scope_units
alter table public.banking_pay_operation_scope_units enable row level security;

-- public.banking_pay_operation_settlement_scope
alter table public.banking_pay_operation_settlement_scope enable row level security;

-- public.banking_pay_operation_transfer_scope
alter table public.banking_pay_operation_transfer_scope enable row level security;

-- public.banking_pay_operation_transfer_scope_items
alter table public.banking_pay_operation_transfer_scope_items enable row level security;

-- public.banking_pay_operations
alter table public.banking_pay_operations enable row level security;

-- public.banking_pay_scope_change_transactions
alter table public.banking_pay_scope_change_transactions enable row level security;

-- public.banking_pay_snapshot_candidate_state
alter table public.banking_pay_snapshot_candidate_state enable row level security;

-- public.banking_pay_snapshot_case_component_state
alter table public.banking_pay_snapshot_case_component_state enable row level security;

-- public.banking_pay_snapshot_case_state
alter table public.banking_pay_snapshot_case_state enable row level security;

-- public.banking_pay_snapshot_line_state
alter table public.banking_pay_snapshot_line_state enable row level security;

-- public.banking_pay_snapshot_runs
alter table public.banking_pay_snapshot_runs enable row level security;

-- public.banking_pay_workbench_candidate_delta_projection_runs
alter table public.banking_pay_workbench_candidate_delta_projection_runs enable row level security;

-- public.banking_pay_workbench_candidate_line_work
alter table public.banking_pay_workbench_candidate_line_work enable row level security;

-- public.banking_pay_workbench_candidate_source_lines
alter table public.banking_pay_workbench_candidate_source_lines enable row level security;

-- public.banking_pay_workbench_case_resolution_carry_registrations
alter table public.banking_pay_workbench_case_resolution_carry_registrations enable row level security;

-- public.banking_pay_workbench_jobs
alter table public.banking_pay_workbench_jobs enable row level security;

-- public.banking_pay_workbench_preview_rows
alter table public.banking_pay_workbench_preview_rows enable row level security;

-- public.banking_pay_workbench_selection_carry_registrations
alter table public.banking_pay_workbench_selection_carry_registrations enable row level security;

-- public.banking_pay_workbench_session_candidate_state
alter table public.banking_pay_workbench_session_candidate_state enable row level security;

-- public.banking_pay_workbench_session_case_resolutions
alter table public.banking_pay_workbench_session_case_resolutions enable row level security;

-- public.banking_pay_workbench_session_overrides
alter table public.banking_pay_workbench_session_overrides enable row level security;

-- public.banking_pay_workbench_session_scope
alter table public.banking_pay_workbench_session_scope enable row level security;

-- public.banking_pay_workbench_sessions
alter table public.banking_pay_workbench_sessions enable row level security;

-- public.candidate_app_accounts
alter table public.candidate_app_accounts enable row level security;
alter table public.candidate_app_accounts force row level security;

-- public.candidate_app_global_membership_links
alter table public.candidate_app_global_membership_links enable row level security;
alter table public.candidate_app_global_membership_links force row level security;

-- public.candidate_app_sessions
alter table public.candidate_app_sessions enable row level security;
alter table public.candidate_app_sessions force row level security;

-- public.candidate_approval_requests
alter table public.candidate_approval_requests enable row level security;
alter table public.candidate_approval_requests force row level security;

-- public.candidate_auth_challenges
alter table public.candidate_auth_challenges enable row level security;
alter table public.candidate_auth_challenges force row level security;

-- public.candidate_daily_availability_days
alter table public.candidate_daily_availability_days enable row level security;

-- public.candidate_daily_command_receipts
alter table public.candidate_daily_command_receipts enable row level security;

-- public.candidate_daily_rota_days
alter table public.candidate_daily_rota_days enable row level security;

-- public.candidate_daily_rota_generations
alter table public.candidate_daily_rota_generations enable row level security;

-- public.candidate_daily_sheet_projection_outbox
alter table public.candidate_daily_sheet_projection_outbox enable row level security;

-- public.candidate_job_titles
alter table public.candidate_job_titles enable row level security;

-- public.candidate_notifications
alter table public.candidate_notifications enable row level security;
alter table public.candidate_notifications force row level security;

-- public.candidate_submission_components
alter table public.candidate_submission_components enable row level security;
alter table public.candidate_submission_components force row level security;

-- public.candidate_submission_workflows
alter table public.candidate_submission_workflows enable row level security;
alter table public.candidate_submission_workflows force row level security;

-- public.candidates
alter table public.candidates enable row level security;

-- public.candidates_tombstones
alter table public.candidates_tombstones enable row level security;

-- public.client_hospitals
alter table public.client_hospitals enable row level security;

-- public.client_settings
alter table public.client_settings enable row level security;

-- public.clients
alter table public.clients enable row level security;

-- public.clients_tombstones
alter table public.clients_tombstones enable row level security;

-- public.comms_outbox
alter table public.comms_outbox enable row level security;

-- public.contract_weeks
alter table public.contract_weeks enable row level security;

-- public.contracts
alter table public.contracts enable row level security;

-- public.default_job_titles
alter table public.default_job_titles enable row level security;

-- public.document_templates
alter table public.document_templates enable row level security;

-- public.hr_daily_grade_role_mappings
alter table public.hr_daily_grade_role_mappings enable row level security;

-- public.hr_imports
alter table public.hr_imports enable row level security;

-- public.hr_issue_email_deliveries
alter table public.hr_issue_email_deliveries enable row level security;

-- public.hr_issue_email_delivery_items
alter table public.hr_issue_email_delivery_items enable row level security;

-- public.hr_issue_emails
alter table public.hr_issue_emails enable row level security;

-- public.hr_name_mappings
alter table public.hr_name_mappings enable row level security;

-- public.hr_results
alter table public.hr_results enable row level security;

-- public.hr_rows
alter table public.hr_rows enable row level security;

-- public.id_consolidation_run_lines
alter table public.id_consolidation_run_lines enable row level security;

-- public.id_consolidation_runs
alter table public.id_consolidation_runs enable row level security;

-- public.id_invoice_ledger
alter table public.id_invoice_ledger enable row level security;

-- public.import_apply_operations
alter table public.import_apply_operations enable row level security;

-- public.import_column_aliases
alter table public.import_column_aliases enable row level security;

-- public.import_review_action_outcomes
alter table public.import_review_action_outcomes enable row level security;

-- public.import_review_daily_timesheet_resolutions
alter table public.import_review_daily_timesheet_resolutions enable row level security;

-- public.import_review_decisions
alter table public.import_review_decisions enable row level security;

-- public.import_review_events
alter table public.import_review_events enable row level security;

-- public.import_review_scope_candidates
alter table public.import_review_scope_candidates enable row level security;

-- public.import_review_scope_clients
alter table public.import_review_scope_clients enable row level security;

-- public.import_review_states
alter table public.import_review_states enable row level security;

-- public.import_review_weekly_validation_resolutions
alter table public.import_review_weekly_validation_resolutions enable row level security;

-- public.invoice_document_assets
alter table public.invoice_document_assets enable row level security;

-- public.invoice_document_versions
alter table public.invoice_document_versions enable row level security;

-- public.invoice_hr_source_rows
alter table public.invoice_hr_source_rows enable row level security;

-- public.invoice_jobs_outbox
alter table public.invoice_jobs_outbox enable row level security;

-- public.invoice_lines
alter table public.invoice_lines enable row level security;

-- public.invoice_operation_chunks
alter table public.invoice_operation_chunks enable row level security;

-- public.invoice_operations
alter table public.invoice_operations enable row level security;

-- public.invoice_pdf_outbox
alter table public.invoice_pdf_outbox enable row level security;

-- public.invoices
alter table public.invoices enable row level security;

-- public.legacy_contract_rate_lines
alter table public.legacy_contract_rate_lines enable row level security;

-- public.legacy_contracts
alter table public.legacy_contracts enable row level security;

-- public.legacy_eclipse_candidate_map
alter table public.legacy_eclipse_candidate_map enable row level security;

-- public.legacy_eclipse_client_map
alter table public.legacy_eclipse_client_map enable row level security;

-- public.mail_outbox
alter table public.mail_outbox enable row level security;

-- public.mailshot_field_overrides
alter table public.mailshot_field_overrides enable row level security;

-- public.mailshot_fields
alter table public.mailshot_fields enable row level security;

-- public.mailshot_runs
alter table public.mailshot_runs enable row level security;

-- public.manual_timesheet_queue
alter table public.manual_timesheet_queue enable row level security;

-- public.migration_smoke_once_only
alter table public.migration_smoke_once_only enable row level security;

-- public.nhsp_shifts
alter table public.nhsp_shifts enable row level security;

-- public.pay_advance_patches
alter table public.pay_advance_patches enable row level security;

-- public.pay_advance_reservations
alter table public.pay_advance_reservations enable row level security;

-- public.pay_advances
alter table public.pay_advances enable row level security;

-- public.pay_bank_transfer_events
alter table public.pay_bank_transfer_events enable row level security;

-- public.pay_bank_transfers
alter table public.pay_bank_transfers enable row level security;

-- public.pay_batch_auth_actions
alter table public.pay_batch_auth_actions enable row level security;

-- public.pay_batch_auth_requests
alter table public.pay_batch_auth_requests enable row level security;

-- public.pay_batch_auth_tokens
alter table public.pay_batch_auth_tokens enable row level security;

-- public.pay_batch_candidates
alter table public.pay_batch_candidates enable row level security;

-- public.pay_batch_display_summary
alter table public.pay_batch_display_summary enable row level security;

-- public.pay_batch_item_breakdowns
alter table public.pay_batch_item_breakdowns enable row level security;

-- public.pay_batch_items
alter table public.pay_batch_items enable row level security;

-- public.pay_batch_paye_net_inputs
alter table public.pay_batch_paye_net_inputs enable row level security;

-- public.pay_batch_timesheet_snapshots
alter table public.pay_batch_timesheet_snapshots enable row level security;

-- public.pay_batches
alter table public.pay_batches enable row level security;

-- public.pay_finance_case_components
alter table public.pay_finance_case_components enable row level security;

-- public.pay_finance_case_events
alter table public.pay_finance_case_events enable row level security;

-- public.pay_finance_case_oneoff_payout_bank_details
alter table public.pay_finance_case_oneoff_payout_bank_details enable row level security;

-- public.pay_item_snoozes
alter table public.pay_item_snoozes enable row level security;

-- public.pay_manual_adjustment_carry_forwards
alter table public.pay_manual_adjustment_carry_forwards enable row level security;

-- public.pay_payment_correction_actions
alter table public.pay_payment_correction_actions enable row level security;

-- public.pay_payment_correction_items
alter table public.pay_payment_correction_items enable row level security;

-- public.pay_payment_correction_request_candidates
alter table public.pay_payment_correction_request_candidates enable row level security;
alter table public.pay_payment_correction_request_candidates force row level security;

-- public.pay_payment_correction_requests
alter table public.pay_payment_correction_requests enable row level security;

-- public.pay_payment_correction_work_items
alter table public.pay_payment_correction_work_items enable row level security;

-- public.pay_payment_return_notice_groups
alter table public.pay_payment_return_notice_groups enable row level security;

-- public.pay_snooze_warning_acknowledgements
alter table public.pay_snooze_warning_acknowledgements enable row level security;

-- public.rates_candidate_overrides
alter table public.rates_candidate_overrides enable row level security;

-- public.rates_client_defaults
alter table public.rates_client_defaults enable row level security;

-- public.rates_presets
alter table public.rates_presets enable row level security;

-- public.report_presets
alter table public.report_presets enable row level security;

-- public.schema_migrations
alter table public.schema_migrations enable row level security;

-- public.schema_repeatables
alter table public.schema_repeatables enable row level security;

-- public.settings_defaults
alter table public.settings_defaults enable row level security;

-- public.settings_finance_windows
alter table public.settings_finance_windows enable row level security;

-- public.sheets_outbox
alter table public.sheets_outbox enable row level security;

-- public.timesheet_archive_transition_capability
alter table public.timesheet_archive_transition_capability enable row level security;

-- public.timesheet_evidence
alter table public.timesheet_evidence enable row level security;

-- public.timesheet_financial_retention
alter table public.timesheet_financial_retention enable row level security;

-- public.timesheet_lifecycle_bulk_operation_items
alter table public.timesheet_lifecycle_bulk_operation_items enable row level security;

-- public.timesheet_lifecycle_bulk_operations
alter table public.timesheet_lifecycle_bulk_operations enable row level security;

-- public.timesheet_pay_state
alter table public.timesheet_pay_state enable row level security;

-- public.timesheet_pay_state_history
alter table public.timesheet_pay_state_history enable row level security;

-- public.timesheet_payment_overrides
alter table public.timesheet_payment_overrides enable row level security;

-- public.timesheet_r2_cleanup_queue
alter table public.timesheet_r2_cleanup_queue enable row level security;

-- public.timesheet_summary_pay_state_cache
alter table public.timesheet_summary_pay_state_cache enable row level security;

-- public.timesheet_validations
alter table public.timesheet_validations enable row level security;

-- public.timesheets
alter table public.timesheets enable row level security;

-- public.timesheets_financials
alter table public.timesheets_financials enable row level security;

-- public.tms_login_2fa_challenges
alter table public.tms_login_2fa_challenges enable row level security;

-- public.tms_password_resets
alter table public.tms_password_resets enable row level security;

-- public.tms_user_2fa_trust
alter table public.tms_user_2fa_trust enable row level security;

-- public.tms_users
alter table public.tms_users enable row level security;

-- public.ts_financials_outbox
alter table public.ts_financials_outbox enable row level security;

-- public.ts_pay_adjustments
alter table public.ts_pay_adjustments enable row level security;

-- public.ts_pdfs_outbox
alter table public.ts_pdfs_outbox enable row level security;

-- public.umbrellas
alter table public.umbrellas enable row level security;

-- private.banking_pay_workbench_candidate_scope_registry
alter table private.banking_pay_workbench_candidate_scope_registry owner to postgres;

-- private.banking_pay_workbench_canonical_stage_lines
alter table private.banking_pay_workbench_canonical_stage_lines owner to postgres;

-- private.banking_pay_workbench_economic_build_fact_pages
alter table private.banking_pay_workbench_economic_build_fact_pages owner to postgres;

-- private.banking_pay_workbench_economic_build_facts
alter table private.banking_pay_workbench_economic_build_facts owner to postgres;

-- private.banking_pay_workbench_economic_build_scope
alter table private.banking_pay_workbench_economic_build_scope owner to postgres;

-- private.banking_pay_workbench_economic_builds
alter table private.banking_pay_workbench_economic_builds owner to postgres;

-- private.banking_pay_workbench_queue_scan_state
alter table private.banking_pay_workbench_queue_scan_state owner to postgres;

-- private.banking_pay_workbench_stage_attempts
alter table private.banking_pay_workbench_stage_attempts owner to postgres;

-- private.banking_pay_workbench_timesheet_scope_state
alter table private.banking_pay_workbench_timesheet_scope_state owner to postgres;

-- private.candidate_daily_authority_scopes
alter table private.candidate_daily_authority_scopes owner to postgres;

-- private.candidate_daily_authority_transitions
alter table private.candidate_daily_authority_transitions owner to postgres;

-- private.candidate_daily_batch_receipts
alter table private.candidate_daily_batch_receipts owner to postgres;

-- private.candidate_daily_entitlements
alter table private.candidate_daily_entitlements owner to postgres;

-- private.candidate_daily_external_effect_receipts
alter table private.candidate_daily_external_effect_receipts owner to postgres;

-- private.candidate_daily_source_links
alter table private.candidate_daily_source_links owner to postgres;

-- private.candidate_daily_sync_state
alter table private.candidate_daily_sync_state owner to postgres;

-- private.invoice_async_snapshot_hmac_keys
alter table private.invoice_async_snapshot_hmac_keys owner to postgres;

-- public.app_change_counters
alter table public.app_change_counters owner to postgres;

-- public.assignment_band_mappings
alter table public.assignment_band_mappings owner to postgres;

-- public.audit_events
alter table public.audit_events owner to postgres;

-- public.bank_name_checks
alter table public.bank_name_checks owner to postgres;

-- public.bank_payee_map
alter table public.bank_payee_map owner to postgres;

-- public.bank_provider_webhook_configs
alter table public.bank_provider_webhook_configs owner to postgres;

-- public.bank_provider_webhook_receipts
alter table public.bank_provider_webhook_receipts owner to postgres;

-- public.banking_alert_acknowledgements
alter table public.banking_alert_acknowledgements owner to postgres;

-- public.banking_alert_display_summary
alter table public.banking_alert_display_summary owner to postgres;

-- public.banking_alert_success_events
alter table public.banking_alert_success_events owner to postgres;

-- public.banking_alert_user_preferences
alter table public.banking_alert_user_preferences owner to postgres;

-- public.banking_pay_batch_change_signals
alter table public.banking_pay_batch_change_signals owner to postgres;

-- public.banking_pay_operation_candidate_allocation_rows
alter table public.banking_pay_operation_candidate_allocation_rows owner to postgres;

-- public.banking_pay_operation_candidate_scope
alter table public.banking_pay_operation_candidate_scope owner to postgres;

-- public.banking_pay_operation_chunks
alter table public.banking_pay_operation_chunks owner to postgres;

-- public.banking_pay_operation_config
alter table public.banking_pay_operation_config owner to postgres;

-- public.banking_pay_operation_provider_attempts
alter table public.banking_pay_operation_provider_attempts owner to postgres;

-- public.banking_pay_operation_remittance_scope
alter table public.banking_pay_operation_remittance_scope owner to postgres;

-- public.banking_pay_operation_scope_units
alter table public.banking_pay_operation_scope_units owner to postgres;

-- public.banking_pay_operation_settlement_scope
alter table public.banking_pay_operation_settlement_scope owner to postgres;

-- public.banking_pay_operation_transfer_scope
alter table public.banking_pay_operation_transfer_scope owner to postgres;

-- public.banking_pay_operation_transfer_scope_items
alter table public.banking_pay_operation_transfer_scope_items owner to postgres;

-- public.banking_pay_operations
alter table public.banking_pay_operations owner to postgres;

-- public.banking_pay_scope_change_transactions
alter table public.banking_pay_scope_change_transactions owner to postgres;

-- public.banking_pay_snapshot_candidate_state
alter table public.banking_pay_snapshot_candidate_state owner to postgres;

-- public.banking_pay_snapshot_case_component_state
alter table public.banking_pay_snapshot_case_component_state owner to postgres;

-- public.banking_pay_snapshot_case_state
alter table public.banking_pay_snapshot_case_state owner to postgres;

-- public.banking_pay_snapshot_line_state
alter table public.banking_pay_snapshot_line_state owner to postgres;

-- public.banking_pay_snapshot_runs
alter table public.banking_pay_snapshot_runs owner to postgres;

-- public.banking_pay_workbench_candidate_delta_projection_runs
alter table public.banking_pay_workbench_candidate_delta_projection_runs owner to postgres;

-- public.banking_pay_workbench_candidate_line_work
alter table public.banking_pay_workbench_candidate_line_work owner to postgres;

-- public.banking_pay_workbench_candidate_source_lines
alter table public.banking_pay_workbench_candidate_source_lines owner to postgres;

-- public.banking_pay_workbench_case_resolution_carry_registrations
alter table public.banking_pay_workbench_case_resolution_carry_registrations owner to postgres;

-- public.banking_pay_workbench_jobs
alter table public.banking_pay_workbench_jobs owner to postgres;

-- public.banking_pay_workbench_preview_rows
alter table public.banking_pay_workbench_preview_rows owner to postgres;

-- public.banking_pay_workbench_selection_carry_registrations
alter table public.banking_pay_workbench_selection_carry_registrations owner to postgres;

-- public.banking_pay_workbench_session_candidate_state
alter table public.banking_pay_workbench_session_candidate_state owner to postgres;

-- public.banking_pay_workbench_session_case_resolutions
alter table public.banking_pay_workbench_session_case_resolutions owner to postgres;

-- public.banking_pay_workbench_session_overrides
alter table public.banking_pay_workbench_session_overrides owner to postgres;

-- public.banking_pay_workbench_session_scope
alter table public.banking_pay_workbench_session_scope owner to postgres;

-- public.banking_pay_workbench_sessions
alter table public.banking_pay_workbench_sessions owner to postgres;

-- public.candidate_app_accounts
alter table public.candidate_app_accounts owner to postgres;

-- public.candidate_app_global_membership_links
alter table public.candidate_app_global_membership_links owner to postgres;

-- public.candidate_app_sessions
alter table public.candidate_app_sessions owner to postgres;

-- public.candidate_approval_requests
alter table public.candidate_approval_requests owner to postgres;

-- public.candidate_auth_challenges
alter table public.candidate_auth_challenges owner to postgres;

-- public.candidate_daily_availability_days
alter table public.candidate_daily_availability_days owner to postgres;

-- public.candidate_daily_command_receipts
alter table public.candidate_daily_command_receipts owner to postgres;

-- public.candidate_daily_rota_days
alter table public.candidate_daily_rota_days owner to postgres;

-- public.candidate_daily_rota_generations
alter table public.candidate_daily_rota_generations owner to postgres;

-- public.candidate_daily_sheet_projection_outbox
alter table public.candidate_daily_sheet_projection_outbox owner to postgres;

-- public.candidate_job_titles
alter table public.candidate_job_titles owner to postgres;

-- public.candidate_notifications
alter table public.candidate_notifications owner to postgres;

-- public.candidate_submission_components
alter table public.candidate_submission_components owner to postgres;

-- public.candidate_submission_workflows
alter table public.candidate_submission_workflows owner to postgres;

-- public.candidates
alter table public.candidates owner to postgres;

-- public.candidates_tombstones
alter table public.candidates_tombstones owner to postgres;

-- public.client_hospitals
alter table public.client_hospitals owner to postgres;

-- public.client_settings
alter table public.client_settings owner to postgres;

-- public.clients
alter table public.clients owner to postgres;

-- public.clients_tombstones
alter table public.clients_tombstones owner to postgres;

-- public.comms_outbox
alter table public.comms_outbox owner to postgres;

-- public.contract_weeks
alter table public.contract_weeks owner to postgres;

-- public.contracts
alter table public.contracts owner to postgres;

-- public.default_job_titles
alter table public.default_job_titles owner to postgres;

-- public.document_templates
alter table public.document_templates owner to postgres;

-- public.hr_daily_grade_role_mappings
alter table public.hr_daily_grade_role_mappings owner to postgres;

-- public.hr_imports
alter table public.hr_imports owner to postgres;

-- public.hr_issue_email_deliveries
alter table public.hr_issue_email_deliveries owner to postgres;

-- public.hr_issue_email_delivery_items
alter table public.hr_issue_email_delivery_items owner to postgres;

-- public.hr_issue_emails
alter table public.hr_issue_emails owner to postgres;

-- public.hr_name_mappings
alter table public.hr_name_mappings owner to postgres;

-- public.hr_results
alter table public.hr_results owner to postgres;

-- public.hr_rows
alter table public.hr_rows owner to postgres;

-- public.id_consolidation_run_lines
alter table public.id_consolidation_run_lines owner to postgres;

-- public.id_consolidation_runs
alter table public.id_consolidation_runs owner to postgres;

-- public.id_invoice_ledger
alter table public.id_invoice_ledger owner to postgres;

-- public.import_apply_operations
alter table public.import_apply_operations owner to postgres;

-- public.import_column_aliases
alter table public.import_column_aliases owner to postgres;

-- public.import_review_action_outcomes
alter table public.import_review_action_outcomes owner to postgres;

-- public.import_review_daily_timesheet_resolutions
alter table public.import_review_daily_timesheet_resolutions owner to postgres;

-- public.import_review_decisions
alter table public.import_review_decisions owner to postgres;

-- public.import_review_events
alter table public.import_review_events owner to postgres;

-- public.import_review_scope_candidates
alter table public.import_review_scope_candidates owner to postgres;

-- public.import_review_scope_clients
alter table public.import_review_scope_clients owner to postgres;

-- public.import_review_states
alter table public.import_review_states owner to postgres;

-- public.import_review_weekly_validation_resolutions
alter table public.import_review_weekly_validation_resolutions owner to postgres;

-- public.invoice_document_assets
alter table public.invoice_document_assets owner to postgres;

-- public.invoice_document_versions
alter table public.invoice_document_versions owner to postgres;

-- public.invoice_hr_source_rows
alter table public.invoice_hr_source_rows owner to postgres;

-- public.invoice_jobs_outbox
alter table public.invoice_jobs_outbox owner to postgres;

-- public.invoice_lines
alter table public.invoice_lines owner to postgres;

-- public.invoice_operation_chunks
alter table public.invoice_operation_chunks owner to postgres;

-- public.invoice_operations
alter table public.invoice_operations owner to postgres;

-- public.invoice_pdf_outbox
alter table public.invoice_pdf_outbox owner to postgres;

-- public.invoices
alter table public.invoices owner to postgres;

-- public.legacy_contract_rate_lines
alter table public.legacy_contract_rate_lines owner to postgres;

-- public.legacy_contracts
alter table public.legacy_contracts owner to postgres;

-- public.legacy_eclipse_candidate_map
alter table public.legacy_eclipse_candidate_map owner to postgres;

-- public.legacy_eclipse_client_map
alter table public.legacy_eclipse_client_map owner to postgres;

-- public.mail_outbox
alter table public.mail_outbox owner to postgres;

-- public.mailshot_field_overrides
alter table public.mailshot_field_overrides owner to postgres;

-- public.mailshot_fields
alter table public.mailshot_fields owner to postgres;

-- public.mailshot_runs
alter table public.mailshot_runs owner to postgres;

-- public.manual_timesheet_queue
alter table public.manual_timesheet_queue owner to postgres;

-- public.migration_smoke_once_only
alter table public.migration_smoke_once_only owner to postgres;

-- public.nhsp_shifts
alter table public.nhsp_shifts owner to postgres;

-- public.pay_advance_patches
alter table public.pay_advance_patches owner to postgres;

-- public.pay_advance_reservations
alter table public.pay_advance_reservations owner to postgres;

-- public.pay_advances
alter table public.pay_advances owner to postgres;

-- public.pay_bank_transfer_events
alter table public.pay_bank_transfer_events owner to postgres;

-- public.pay_bank_transfers
alter table public.pay_bank_transfers owner to postgres;

-- public.pay_batch_auth_actions
alter table public.pay_batch_auth_actions owner to postgres;

-- public.pay_batch_auth_requests
alter table public.pay_batch_auth_requests owner to postgres;

-- public.pay_batch_auth_tokens
alter table public.pay_batch_auth_tokens owner to postgres;

-- public.pay_batch_candidates
alter table public.pay_batch_candidates owner to postgres;

-- public.pay_batch_display_summary
alter table public.pay_batch_display_summary owner to postgres;

-- public.pay_batch_item_breakdowns
alter table public.pay_batch_item_breakdowns owner to postgres;

-- public.pay_batch_items
alter table public.pay_batch_items owner to postgres;

-- public.pay_batch_paye_net_inputs
alter table public.pay_batch_paye_net_inputs owner to postgres;

-- public.pay_batch_timesheet_snapshots
alter table public.pay_batch_timesheet_snapshots owner to postgres;

-- public.pay_batches
alter table public.pay_batches owner to postgres;

-- public.pay_finance_case_components
alter table public.pay_finance_case_components owner to postgres;

-- public.pay_finance_case_events
alter table public.pay_finance_case_events owner to postgres;

-- public.pay_finance_case_oneoff_payout_bank_details
alter table public.pay_finance_case_oneoff_payout_bank_details owner to postgres;

-- public.pay_item_snoozes
alter table public.pay_item_snoozes owner to postgres;

-- public.pay_manual_adjustment_carry_forwards
alter table public.pay_manual_adjustment_carry_forwards owner to postgres;

-- public.pay_payment_correction_actions
alter table public.pay_payment_correction_actions owner to postgres;

-- public.pay_payment_correction_items
alter table public.pay_payment_correction_items owner to postgres;

-- public.pay_payment_correction_request_candidates
alter table public.pay_payment_correction_request_candidates owner to postgres;

-- public.pay_payment_correction_requests
alter table public.pay_payment_correction_requests owner to postgres;

-- public.pay_payment_correction_work_items
alter table public.pay_payment_correction_work_items owner to postgres;

-- public.pay_payment_return_notice_groups
alter table public.pay_payment_return_notice_groups owner to postgres;

-- public.pay_snooze_warning_acknowledgements
alter table public.pay_snooze_warning_acknowledgements owner to postgres;

-- public.rates_candidate_overrides
alter table public.rates_candidate_overrides owner to postgres;

-- public.rates_client_defaults
alter table public.rates_client_defaults owner to postgres;

-- public.rates_presets
alter table public.rates_presets owner to postgres;

-- public.report_presets
alter table public.report_presets owner to postgres;

-- public.schema_migrations
alter table public.schema_migrations owner to postgres;

-- public.schema_repeatables
alter table public.schema_repeatables owner to postgres;

-- public.settings_defaults
alter table public.settings_defaults owner to postgres;

-- public.settings_finance_windows
alter table public.settings_finance_windows owner to postgres;

-- public.sheets_outbox
alter table public.sheets_outbox owner to postgres;

-- public.timesheet_archive_transition_capability
alter table public.timesheet_archive_transition_capability owner to postgres;

-- public.timesheet_evidence
alter table public.timesheet_evidence owner to postgres;

-- public.timesheet_financial_retention
alter table public.timesheet_financial_retention owner to postgres;

-- public.timesheet_lifecycle_bulk_operation_items
alter table public.timesheet_lifecycle_bulk_operation_items owner to postgres;

-- public.timesheet_lifecycle_bulk_operations
alter table public.timesheet_lifecycle_bulk_operations owner to postgres;

-- public.timesheet_pay_state
alter table public.timesheet_pay_state owner to postgres;

-- public.timesheet_pay_state_history
alter table public.timesheet_pay_state_history owner to postgres;

-- public.timesheet_payment_overrides
alter table public.timesheet_payment_overrides owner to postgres;

-- public.timesheet_r2_cleanup_queue
alter table public.timesheet_r2_cleanup_queue owner to postgres;

-- public.timesheet_summary_pay_state_cache
alter table public.timesheet_summary_pay_state_cache owner to postgres;

-- public.timesheet_validations
alter table public.timesheet_validations owner to postgres;

-- public.timesheets
alter table public.timesheets owner to postgres;

-- public.timesheets_financials
alter table public.timesheets_financials owner to postgres;

-- public.tms_login_2fa_challenges
alter table public.tms_login_2fa_challenges owner to postgres;

-- public.tms_password_resets
alter table public.tms_password_resets owner to postgres;

-- public.tms_user_2fa_trust
alter table public.tms_user_2fa_trust owner to postgres;

-- public.tms_users
alter table public.tms_users owner to postgres;

-- public.ts_financials_outbox
alter table public.ts_financials_outbox owner to postgres;

-- public.ts_pay_adjustments
alter table public.ts_pay_adjustments owner to postgres;

-- public.ts_pdfs_outbox
alter table public.ts_pdfs_outbox owner to postgres;

-- public.umbrellas
alter table public.umbrellas owner to postgres;
