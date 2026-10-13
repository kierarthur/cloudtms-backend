-- Final closure ACL: preserve audited existing grants; new private helpers
-- are owner-only and new public RPCs are service-only. Never rely on defaults.
alter function private._candidate_timesheet_delete_retire_workflows_v1(text,uuid[],uuid[],uuid[],uuid,uuid,timestamptz) owner to postgres;
alter function private._timesheet_candidate_submission_delete_guard_v1(text,uuid[],boolean) owner to postgres;
alter function private._timesheet_cross_record_overlap_assert_v1(uuid,jsonb,jsonb,timestamptz,timestamptz,uuid,uuid,text) owner to postgres;
alter function private.timesheet_correction_actor_allowed_v1(uuid) owner to postgres;
alter function private.timesheet_correction_actor_assert_v1(uuid) owner to postgres;
alter function private.timesheet_correction_applied_assert_v1(uuid) owner to postgres;
alter function private.timesheet_correction_cancel_capability_v1(uuid,uuid) owner to postgres;
alter function private.timesheet_correction_draft_holds_v1(uuid) owner to postgres;
alter function private.timesheet_correction_draft_round_trip_v1(uuid,text,uuid) owner to postgres;
alter function private.timesheet_correction_financial_authority_hash_v1(jsonb) owner to postgres;
alter function private.timesheet_correction_financial_guard_v1() owner to postgres;
alter function private.timesheet_correction_invoice_route_v1(uuid) owner to postgres;
alter function private.timesheet_correction_invoice_scope_v1(uuid,uuid,uuid,boolean,integer) owner to postgres;
alter function private.timesheet_correction_leg_create_v1(private.timesheet_correction_requests,text,jsonb,timestamptz) owner to postgres;
alter function private.timesheet_correction_origin_v1(uuid) owner to postgres;
alter function private.timesheet_correction_projection_by_request_v1(uuid,uuid) owner to postgres;
alter function private.timesheet_correction_replacement_empty_v1(uuid) owner to postgres;
alter function private.timesheet_correction_request_guard_v1() owner to postgres;
alter function private.timesheet_correction_reversal_assert_v1(jsonb,jsonb) owner to postgres;
alter function private.timesheet_correction_reversal_component_assert_v1(jsonb,jsonb,text[]) owner to postgres;
alter function private.timesheet_correction_signed_witness_v1(uuid,boolean) owner to postgres;
alter function private.timesheet_correction_snapshot_identity_assert_v1(jsonb,uuid,integer,uuid,uuid) owner to postgres;
alter function private.timesheet_correction_terminal_guard_v1() owner to postgres;
alter function private.timesheet_correction_timesheet_authority_hash_v1(jsonb) owner to postgres;
alter function private.timesheet_correction_week_allocate_v1(uuid,date,public.submission_mode_enum,uuid,timestamptz) owner to postgres;
alter function private.timesheet_correction_whole_state_guard_v1() owner to postgres;
alter function private.timesheet_correction_whole_transition_v1(uuid,text,uuid,text,boolean,integer) owner to postgres;
alter function public._ctms_expand_lifecycle_items_v1(jsonb,text,uuid,integer) owner to postgres;
alter function public.invoice_correction_pair_scope_v1(uuid,uuid,uuid,boolean,integer) owner to postgres;
alter function public.timesheet_correction_authorise_atomic_v1(uuid,bigint,jsonb,uuid,timestamptz) owner to postgres;
alter function public.timesheet_correction_cancel_atomic_v1(text,uuid,bigint,uuid,timestamptz) owner to postgres;
alter function public.timesheet_correction_chain_scope_v1(uuid,boolean,integer,integer) owner to postgres;
alter function public.timesheet_correction_create_projection_v1(uuid,uuid,uuid,uuid) owner to postgres;
alter function public.timesheet_correction_manager_summary_v1(uuid,integer,uuid,text) owner to postgres;
alter function public.timesheet_correction_pair_lifecycle_preview_v1(jsonb,text,uuid,integer) owner to postgres;
alter function public.timesheet_correction_pair_transition_v1(uuid,text,uuid,uuid,text,boolean,integer) owner to postgres;
alter function public.timesheet_correction_prepare_v1(uuid,uuid) owner to postgres;
alter function public.timesheet_correction_request_projection_v1(uuid,uuid) owner to postgres;
alter function public.timesheet_correction_return_to_draft_v1(uuid,uuid) owner to postgres;
alter function public.timesheet_correction_send_v1(jsonb,uuid,timestamptz) owner to postgres;
alter function public.timesheet_correction_workflow_projection_v1(uuid,uuid) owner to postgres;
alter function public.timesheet_delete_with_pending_expense_apply_v1(text,text,uuid,uuid,uuid,text,uuid[],uuid[],uuid[],uuid[],uuid[],text,uuid,timestamptz) owner to postgres;
alter function public.timesheet_standard_delete_preview_v1(uuid,uuid,uuid,text) owner to postgres;
alter function public.timesheet_weekly_manual_adjustment_delete_preview(uuid,uuid) owner to postgres;
revoke all on function
  private._candidate_timesheet_delete_retire_workflows_v1(text,uuid[],uuid[],uuid[],uuid,uuid,timestamptz),
  private._timesheet_candidate_submission_delete_guard_v1(text,uuid[],boolean),
  private._timesheet_cross_record_overlap_assert_v1(uuid,jsonb,jsonb,timestamptz,timestamptz,uuid,uuid,text),
  private.timesheet_correction_actor_allowed_v1(uuid),
  private.timesheet_correction_actor_assert_v1(uuid),
  private.timesheet_correction_applied_assert_v1(uuid),
  private.timesheet_correction_cancel_capability_v1(uuid,uuid),
  private.timesheet_correction_draft_holds_v1(uuid),
  private.timesheet_correction_draft_round_trip_v1(uuid,text,uuid),
  private.timesheet_correction_financial_authority_hash_v1(jsonb),
  private.timesheet_correction_financial_guard_v1(),
  private.timesheet_correction_invoice_route_v1(uuid),
  private.timesheet_correction_invoice_scope_v1(uuid,uuid,uuid,boolean,integer),
  private.timesheet_correction_leg_create_v1(private.timesheet_correction_requests,text,jsonb,timestamptz),
  private.timesheet_correction_origin_v1(uuid),
  private.timesheet_correction_projection_by_request_v1(uuid,uuid),
  private.timesheet_correction_replacement_empty_v1(uuid),
  private.timesheet_correction_request_guard_v1(),
  private.timesheet_correction_reversal_assert_v1(jsonb,jsonb),
  private.timesheet_correction_reversal_component_assert_v1(jsonb,jsonb,text[]),
  private.timesheet_correction_signed_witness_v1(uuid,boolean),
  private.timesheet_correction_snapshot_identity_assert_v1(jsonb,uuid,integer,uuid,uuid),
  private.timesheet_correction_terminal_guard_v1(),
  private.timesheet_correction_timesheet_authority_hash_v1(jsonb),
  private.timesheet_correction_week_allocate_v1(uuid,date,public.submission_mode_enum,uuid,timestamptz),
  private.timesheet_correction_whole_state_guard_v1(),
  private.timesheet_correction_whole_transition_v1(uuid,text,uuid,text,boolean,integer),
  public._ctms_expand_lifecycle_items_v1(jsonb,text,uuid,integer),
  public.invoice_correction_pair_scope_v1(uuid,uuid,uuid,boolean,integer),
  public.timesheet_correction_authorise_atomic_v1(uuid,bigint,jsonb,uuid,timestamptz),
  public.timesheet_correction_cancel_atomic_v1(text,uuid,bigint,uuid,timestamptz),
  public.timesheet_correction_chain_scope_v1(uuid,boolean,integer,integer),
  public.timesheet_correction_create_projection_v1(uuid,uuid,uuid,uuid),
  public.timesheet_correction_manager_summary_v1(uuid,integer,uuid,text),
  public.timesheet_correction_pair_lifecycle_preview_v1(jsonb,text,uuid,integer),
  public.timesheet_correction_pair_transition_v1(uuid,text,uuid,uuid,text,boolean,integer),
  public.timesheet_correction_prepare_v1(uuid,uuid),
  public.timesheet_correction_request_projection_v1(uuid,uuid),
  public.timesheet_correction_return_to_draft_v1(uuid,uuid),
  public.timesheet_correction_send_v1(jsonb,uuid,timestamptz),
  public.timesheet_correction_workflow_projection_v1(uuid,uuid),
  public.timesheet_delete_with_pending_expense_apply_v1(text,text,uuid,uuid,uuid,text,uuid[],uuid[],uuid[],uuid[],uuid[],text,uuid,timestamptz),
  public.timesheet_standard_delete_preview_v1(uuid,uuid,uuid,text),
  public.timesheet_weekly_manual_adjustment_delete_preview(uuid,uuid)
  from public,anon,authenticated,service_role,authenticator,supabase_admin;
grant execute on function
  private._candidate_timesheet_delete_retire_workflows_v1(text,uuid[],uuid[],uuid[],uuid,uuid,timestamptz),
  private._timesheet_candidate_submission_delete_guard_v1(text,uuid[],boolean),
  private._timesheet_cross_record_overlap_assert_v1(uuid,jsonb,jsonb,timestamptz,timestamptz,uuid,uuid,text),
  private.timesheet_correction_actor_allowed_v1(uuid),
  private.timesheet_correction_actor_assert_v1(uuid),
  private.timesheet_correction_applied_assert_v1(uuid),
  private.timesheet_correction_cancel_capability_v1(uuid,uuid),
  private.timesheet_correction_draft_holds_v1(uuid),
  private.timesheet_correction_draft_round_trip_v1(uuid,text,uuid),
  private.timesheet_correction_financial_authority_hash_v1(jsonb),
  private.timesheet_correction_financial_guard_v1(),
  private.timesheet_correction_invoice_route_v1(uuid),
  private.timesheet_correction_invoice_scope_v1(uuid,uuid,uuid,boolean,integer),
  private.timesheet_correction_leg_create_v1(private.timesheet_correction_requests,text,jsonb,timestamptz),
  private.timesheet_correction_origin_v1(uuid),
  private.timesheet_correction_projection_by_request_v1(uuid,uuid),
  private.timesheet_correction_replacement_empty_v1(uuid),
  private.timesheet_correction_request_guard_v1(),
  private.timesheet_correction_reversal_assert_v1(jsonb,jsonb),
  private.timesheet_correction_reversal_component_assert_v1(jsonb,jsonb,text[]),
  private.timesheet_correction_signed_witness_v1(uuid,boolean),
  private.timesheet_correction_snapshot_identity_assert_v1(jsonb,uuid,integer,uuid,uuid),
  private.timesheet_correction_terminal_guard_v1(),
  private.timesheet_correction_timesheet_authority_hash_v1(jsonb),
  private.timesheet_correction_week_allocate_v1(uuid,date,public.submission_mode_enum,uuid,timestamptz),
  private.timesheet_correction_whole_state_guard_v1(),
  private.timesheet_correction_whole_transition_v1(uuid,text,uuid,text,boolean,integer),
  public._ctms_expand_lifecycle_items_v1(jsonb,text,uuid,integer),
  public.invoice_correction_pair_scope_v1(uuid,uuid,uuid,boolean,integer),
  public.timesheet_correction_authorise_atomic_v1(uuid,bigint,jsonb,uuid,timestamptz),
  public.timesheet_correction_cancel_atomic_v1(text,uuid,bigint,uuid,timestamptz),
  public.timesheet_correction_chain_scope_v1(uuid,boolean,integer,integer),
  public.timesheet_correction_create_projection_v1(uuid,uuid,uuid,uuid),
  public.timesheet_correction_manager_summary_v1(uuid,integer,uuid,text),
  public.timesheet_correction_pair_lifecycle_preview_v1(jsonb,text,uuid,integer),
  public.timesheet_correction_pair_transition_v1(uuid,text,uuid,uuid,text,boolean,integer),
  public.timesheet_correction_prepare_v1(uuid,uuid),
  public.timesheet_correction_request_projection_v1(uuid,uuid),
  public.timesheet_correction_return_to_draft_v1(uuid,uuid),
  public.timesheet_correction_send_v1(jsonb,uuid,timestamptz),
  public.timesheet_correction_workflow_projection_v1(uuid,uuid),
  public.timesheet_delete_with_pending_expense_apply_v1(text,text,uuid,uuid,uuid,text,uuid[],uuid[],uuid[],uuid[],uuid[],text,uuid,timestamptz),
  public.timesheet_standard_delete_preview_v1(uuid,uuid,uuid,text),
  public.timesheet_weekly_manual_adjustment_delete_preview(uuid,uuid)
  to postgres;
grant execute on function
  public.invoice_correction_pair_scope_v1(uuid,uuid,uuid,boolean,integer),
  public.timesheet_correction_authorise_atomic_v1(uuid,bigint,jsonb,uuid,timestamptz),
  public.timesheet_correction_cancel_atomic_v1(text,uuid,bigint,uuid,timestamptz),
  public.timesheet_correction_chain_scope_v1(uuid,boolean,integer,integer),
  public.timesheet_correction_create_projection_v1(uuid,uuid,uuid,uuid),
  public.timesheet_correction_manager_summary_v1(uuid,integer,uuid,text),
  public.timesheet_correction_pair_lifecycle_preview_v1(jsonb,text,uuid,integer),
  public.timesheet_correction_pair_transition_v1(uuid,text,uuid,uuid,text,boolean,integer),
  public.timesheet_correction_prepare_v1(uuid,uuid),
  public.timesheet_correction_request_projection_v1(uuid,uuid),
  public.timesheet_correction_return_to_draft_v1(uuid,uuid),
  public.timesheet_correction_send_v1(jsonb,uuid,timestamptz),
  public.timesheet_correction_workflow_projection_v1(uuid,uuid),
  public.timesheet_delete_with_pending_expense_apply_v1(text,text,uuid,uuid,uuid,text,uuid[],uuid[],uuid[],uuid[],uuid[],text,uuid,timestamptz),
  public.timesheet_standard_delete_preview_v1(uuid,uuid,uuid,text),
  public.timesheet_weekly_manual_adjustment_delete_preview(uuid,uuid)
  to service_role;
