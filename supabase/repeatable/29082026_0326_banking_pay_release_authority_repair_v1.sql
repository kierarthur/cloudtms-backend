-- Narrow current-authority closure for Banking Pay modal v2.
--
-- The historical 08082026 reassertion is an immutable compatibility artefact.
-- It must not be edited to pull modern authorities through provider-specific
-- historical setup files.  Reapply only the focused, current, provider-safe
-- owners needed after that boundary, then restore the one current four-argument
-- compatibility wrapper that exists only in the NEW-database baseline.
--
-- Every included file is already a current authoritative repeatable.  The
-- protected contract verifier must prove that this closure changes no economic
-- owner and leaves every routine at the canonical current-contract hash.

\ir 13062026_1544_process_authorise_unprocess_unauthorise.sql
\ir 19072026_0233_finance_audit_and_refresh_guard.sql
\ir 19072026_2344_banking_pay_shared_selection_guard.sql
\ir 20072026_1105_resolve_paye_deduction_bank_projection.sql
\ir 31072026_1720_pay_batch_nested_resolution_breakdowns.sql
\ir 31072026_2350_banking_pay_continuous_scope_runtime.sql
\ir 04082026_1219_pay_timesheet_summary_pay_state_refresh_trigger.sql
\ir 04082026_1219_pay_workbench_contract_client_dirty_fanout_chunk.sql
\ir 04082026_1219_pay_workbench_dirty_event_enqueue.sql
\ir 04082026_1219_pay_workbench_enqueue_candidate_refresh.sql
\ir 04082026_1219_pay_workbench_enqueue_stage_continuation.sql
\ir 04082026_1219_pay_workbench_fail_job.sql
\ir 04082026_1219_pay_workbench_mark_candidate_dirty.sql
\ir 04082026_1219_pay_workbench_mark_finance_case_dirty.sql
\ir 04082026_1219_pay_workbench_scope_change_finalize_trg_v1.sql
\ir 04082026_1219_pay_workbench_worker_drain_chunk.sql
\ir 04082026_1231_retire_pay_payment_cancel_not_sent_and_recalculate_complete_v1.sql
\ir 04082026_1231_retire_pay_payment_cancel_not_sent_and_recalculate.sql
\ir 04082026_1231_retire_pay_payment_confirm_no_money_and_unwind.sql
\ir 04082026_1302_pay_workbench_repair_invalid_source_build_poison.sql
\ir 04082026_1302_pay_workbench_session_clone_eligible_rows_v1.sql
\ir 04082026_1302_pay_workbench_session_replay_replaced_queue_v1.sql
\ir 04082026_2035_banking_pay_correction_helper_acl.sql
\ir 05082026_1545_pay_preview_candidate_build_canonical_lines.sql
\ir 07082026_1015_pay_sync_overpayments_from_workbench_workspace_v1.sql
\ir 07082026_1016_banking_pay_targeted_delta_runtime.sql
\ir 07082026_1017_pay_workbench_enqueue_candidate_refresh.sql
\ir 07082026_2224_candidate_app_weekly_office_replacements_v1.sql
\ir 07082026_2358_banking_pay_targeted_fast_route_acl.sql
\ir 08082026_0054_banking_pay_dirty_lane_clone_eligibility.sql
\ir 08082026_0717_pay_workbench_prepare_draft_allocation_rows_seed_sort_order.sql
\ir 08082026_0820_pay_batch_freshness_scope_seed_active_items.sql
\ir 09082026_0825_pay_workbench_patch_preview_after_batch_mutation.sql
\ir 09082026_0826_pay_preview_candidate_build_summary_fragment.sql
\ir 09082026_1128_banking_pay_operation_finish_post_draft_authority.sql
\ir 09082026_1403_pay_payment_correction_selected_items_draft_scope.sql
\ir 09082026_1727_pay_workbench_session_set_selected_rows_semantic_overlay.sql
\ir 11082026_1552_pay_workbench_session_clear_all_decisions.sql
\ir 11082026_1746_pay_active_settled_components_finance_lineage.sql
\ir 12082026_1446_pay_execute_bank_transfer_chunk_prepare_voided_overlay.sql
\ir 14082026_1310_timesheet_processing_status_and_authorise_authority_v1.sql
\ir 16082026_2035_pay_workbench_candidate_preview_effective_section_v1.sql
\ir 17082026_2052_pay_finance_resolution_cancel_authority.sql
\ir 27082026_2205_candidate_weekly_manager_finalisation_authority_v1.sql
\ir 28082026_1424_banking_pay_modal_selection_owner_bridge.sql
\ir 19072026_1816_cancel_refresh_supersede_finance_dirty.sql

CREATE OR REPLACE FUNCTION public.timesheet_daily_manual_unprocess_atomic(
  p_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_now_utc timestamp with time zone DEFAULT now()
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
BEGIN
  RETURN public.timesheet_daily_manual_unprocess_atomic(
    p_timesheet_id => p_timesheet_id,
    p_expected_timesheet_id => p_expected_timesheet_id,
    p_actor_user_id => p_actor_user_id,
    p_now_utc => p_now_utc,
    p_expected_row_signature => NULL::text
  );
END;
$function$;

ALTER FUNCTION public.timesheet_daily_manual_unprocess_atomic(uuid,uuid,uuid,timestamptz)
  OWNER TO postgres;
REVOKE ALL ON FUNCTION public.timesheet_daily_manual_unprocess_atomic(uuid,uuid,uuid,timestamptz)
  FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.timesheet_daily_manual_unprocess_atomic(uuid,uuid,uuid,timestamptz)
  TO service_role;

-- Focused authorities pre-date the provider-neutral browser isolation baseline
-- and some of them still grant browser roles.  Restore only the exact routines
-- touched by this closure to the canonical owner-plus-service ACL.  Do not
-- broaden browser access while repairing installed source.
REVOKE ALL ON FUNCTION
  public._pay_active_settled_components(uuid[]),
  public.bulk_authorise_dataset_v1(jsonb),
  public.bulk_authorise_row_context_v1(jsonb),
  public.bulk_process_dataset_v1(jsonb),
  public.bulk_process_row_context_v1(jsonb),
  public.bulk_timesheet_row_patch_v1(jsonb),
  public.pay_preview_candidate_build_canonical_lines(jsonb,uuid),
  public.pay_preview_candidate_build_finance_case_baseline(jsonb,uuid),
  public.pay_timesheet_summary_pay_state_refresh_trigger(),
  public.pay_workbench_contract_client_dirty_fanout_chunk(uuid,jsonb,integer),
  public.pay_workbench_dirty_apply_jobs_chunk(integer,timestamptz,uuid,uuid,text,integer),
  public.pay_workbench_enqueue_candidate_refresh(uuid,uuid,text,uuid,jsonb),
  public.pay_workbench_enqueue_stage_continuation(uuid,uuid,text,jsonb,uuid,jsonb,uuid,text,integer,integer),
  public.pay_workbench_repair_invalid_source_build_poison(uuid,uuid,integer,timestamptz,text),
  public.pay_workbench_session_clear_case_resolution(uuid,uuid,jsonb),
  public.pay_workbench_session_clone_eligibility_v1(uuid,uuid,uuid,jsonb),
  public.pay_workbench_worker_drain_chunk(integer,timestamptz,uuid,uuid,text[],text,integer),
  public.timesheet_authorise_bulk_atomic(jsonb,uuid,timestamptz),
  public.timesheet_authorise_generic_atomic(uuid,uuid,uuid,timestamptz,text),
  public.timesheet_daily_manual_process_atomic(uuid,uuid,uuid,jsonb,jsonb,timestamptz,text),
  public.timesheet_lifecycle_guard_signature_v1(uuid,uuid,boolean),
  public.timesheet_qr_send_enqueue_v1(uuid,uuid,uuid,text,timestamptz)
FROM PUBLIC, anon, authenticated, service_role;

GRANT EXECUTE ON FUNCTION
  public._pay_active_settled_components(uuid[]),
  public.bulk_authorise_dataset_v1(jsonb),
  public.bulk_authorise_row_context_v1(jsonb),
  public.bulk_process_dataset_v1(jsonb),
  public.bulk_process_row_context_v1(jsonb),
  public.bulk_timesheet_row_patch_v1(jsonb),
  public.pay_preview_candidate_build_canonical_lines(jsonb,uuid),
  public.pay_preview_candidate_build_finance_case_baseline(jsonb,uuid),
  public.pay_timesheet_summary_pay_state_refresh_trigger(),
  public.pay_workbench_contract_client_dirty_fanout_chunk(uuid,jsonb,integer),
  public.pay_workbench_dirty_apply_jobs_chunk(integer,timestamptz,uuid,uuid,text,integer),
  public.pay_workbench_enqueue_candidate_refresh(uuid,uuid,text,uuid,jsonb),
  public.pay_workbench_enqueue_stage_continuation(uuid,uuid,text,jsonb,uuid,jsonb,uuid,text,integer,integer),
  public.pay_workbench_repair_invalid_source_build_poison(uuid,uuid,integer,timestamptz,text),
  public.pay_workbench_session_clear_case_resolution(uuid,uuid,jsonb),
  public.pay_workbench_session_clone_eligibility_v1(uuid,uuid,uuid,jsonb),
  public.pay_workbench_worker_drain_chunk(integer,timestamptz,uuid,uuid,text[],text,integer),
  public.timesheet_authorise_bulk_atomic(jsonb,uuid,timestamptz),
  public.timesheet_authorise_generic_atomic(uuid,uuid,uuid,timestamptz,text),
  public.timesheet_daily_manual_process_atomic(uuid,uuid,uuid,jsonb,jsonb,timestamptz,text),
  public.timesheet_lifecycle_guard_signature_v1(uuid,uuid,boolean),
  public.timesheet_qr_send_enqueue_v1(uuid,uuid,uuid,text,timestamptz)
TO service_role;

NOTIFY pgrst, 'reload schema';
