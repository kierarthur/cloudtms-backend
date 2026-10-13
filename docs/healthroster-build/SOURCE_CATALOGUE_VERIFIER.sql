-- Read-only source expectation verifier for root's generated build/serial TEST rollback.
-- Returns violations; zero rows is necessary, not runtime/feature acceptance.
-- observed_owner must additionally match root's audited logical-owner mapping.
with expected(signature,service_execute) as (values
  ('private.timesheet_correction_component_negate_v1(jsonb,text[])',false),
  ('private.timesheet_correction_native_original_reversal_v1(jsonb,uuid)',false),
  ('public.timesheet_correction_request_authority_v1(uuid,uuid)',true),
  ('private._candidate_timesheet_delete_retire_workflows_v1(text,uuid[],uuid[],uuid[],uuid,uuid,timestamptz)',false),
  ('private._invoice_correction_validate_batch(jsonb,date)',true),
  ('private._invoice_credit_resume_v1(uuid,uuid,uuid,uuid)',false),
  ('private._invoice_dispatch_advance_batch(jsonb,timestamptz)',true),
  ('private._invoice_document_advance_batch(jsonb,timestamptz)',true),
  ('private._invoice_generation_advance_core_v8(jsonb,timestamptz)',true),
  ('private._invoice_generation_resolve_command_groups(jsonb,uuid,timestamptz)',true),
  ('private._invoice_operation_start_core_v8(jsonb,uuid,timestamptz)',true),
  ('private._invoice_presentation_snapshot_batch(jsonb,timestamptz)',true),
  ('private._invoice_support_member_guard_v1()',false),
  ('private._timesheet_candidate_submission_delete_guard_v1(text,uuid[],boolean)',false),
  ('private._timesheet_cross_record_overlap_assert_v1(uuid,jsonb,jsonb,timestamptz,timestamptz,uuid,uuid,text)',false),
  ('private.invoice_credit_original_input_receipt_v1(jsonb)',false),
  ('private.invoice_credit_scope_context_v1(uuid,uuid,uuid,uuid,boolean)',false),
  ('private.invoice_credit_scoped_advance_v1(jsonb,timestamptz)',false),
  ('private.timesheet_correction_actor_allowed_v1(uuid)',false),
  ('private.timesheet_correction_actor_assert_v1(uuid)',false),
  ('private.timesheet_correction_applied_assert_v1(uuid)',false),
  ('private.timesheet_correction_cancel_capability_v1(uuid,uuid)',false),
  ('private.timesheet_correction_draft_holds_v1(uuid)',false),
  ('private.timesheet_correction_draft_round_trip_v1(uuid,text,uuid)',false),
  ('private.timesheet_correction_financial_authority_hash_v1(jsonb)',false),
  ('private.timesheet_correction_financial_guard_v1()',false),
  ('private.timesheet_correction_invoice_route_v1(uuid)',false),
  ('private.timesheet_correction_invoice_scope_v1(uuid,uuid,uuid,boolean,integer)',false),
  ('private.timesheet_correction_leg_create_v1(private.timesheet_correction_requests,text,jsonb,timestamptz)',false),
  ('private.timesheet_correction_nullable_daily_contract_v1(jsonb,uuid)',false),
  ('private.timesheet_correction_origin_v1(uuid)',false),
  ('private.timesheet_correction_projection_by_request_v1(uuid,uuid)',false),
  ('private.timesheet_correction_replacement_empty_v1(uuid)',false),
  ('private.timesheet_correction_request_guard_v1()',false),
  ('private.timesheet_correction_reversal_assert_v1(jsonb,jsonb)',false),
  ('private.timesheet_correction_reversal_component_assert_v1(jsonb,jsonb,text[])',false),
  ('private.timesheet_correction_scoped_source_identity_v1(jsonb)',false),
  ('private.timesheet_correction_signed_witness_v1(uuid,boolean)',false),
  ('private.timesheet_correction_snapshot_identity_assert_v1(jsonb,uuid,integer,uuid,uuid)',false),
  ('private.timesheet_correction_terminal_guard_v1()',false),
  ('private.timesheet_correction_timesheet_authority_hash_v1(jsonb)',false),
  ('private.timesheet_correction_week_allocate_v1(uuid,date,public.submission_mode_enum,uuid,timestamptz)',false),
  ('private.timesheet_correction_whole_state_guard_v1()',false),
  ('private.timesheet_correction_whole_transition_v1(uuid,text,uuid,text,boolean,integer)',false),
  ('public._ctms_assert_invoice_can_unissue_v1(uuid,boolean,text)',false),
  ('public._ctms_expand_lifecycle_items_v1(jsonb,text,uuid,integer)',false),
  ('public.invoice_correction_pair_scope_v1(uuid,uuid,uuid,boolean,integer)',true),
  ('public.invoice_create_credit_note_and_unlock(uuid,uuid)',true),
  ('public.timesheet_correction_authorise_atomic_v1(uuid,bigint,jsonb,uuid,timestamptz)',true),
  ('public.timesheet_correction_cancel_atomic_v1(text,uuid,bigint,uuid,timestamptz)',true),
  ('public.timesheet_correction_chain_scope_v1(uuid,boolean,integer,integer)',true),
  ('public.timesheet_correction_create_projection_v1(uuid,uuid,uuid,uuid)',true),
  ('public.timesheet_correction_manager_summary_v1(uuid,integer,uuid,text)',true),
  ('public.timesheet_correction_pair_lifecycle_preview_v1(jsonb,text,uuid,integer)',true),
  ('public.timesheet_correction_pair_transition_v1(uuid,text,uuid,uuid,text,boolean,integer)',true),
  ('public.timesheet_correction_prepare_v1(uuid,uuid)',true),
  ('public.timesheet_correction_request_projection_v1(uuid,uuid)',true),
  ('public.timesheet_correction_return_to_draft_v1(uuid,uuid)',true),
  ('public.timesheet_correction_send_v1(jsonb,uuid,timestamptz)',true),
  ('public.timesheet_correction_workflow_projection_v1(uuid,uuid)',true),
  ('public.timesheet_delete_with_pending_expense_apply_v1(text,text,uuid,uuid,uuid,text,uuid[],uuid[],uuid[],uuid[],uuid[],text,uuid,timestamptz)',true),
  ('public.timesheet_standard_delete_preview_v1(uuid,uuid,uuid,text)',true),
  ('public.timesheet_weekly_manual_adjustment_delete_preview(uuid,uuid)',true)
), observed as (
  select e.*,p.oid,p.proowner,p.prosecdef,p.proconfig,pg_get_userbyid(p.proowner) observed_owner,
    exists(select 1 from aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
      where a.grantee=(select oid from pg_roles where rolname='service_role') and a.privilege_type='EXECUTE') service_granted,
    exists(select 1 from aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
      where a.grantee<>p.proowner and (a.grantee=0 or a.grantee is distinct from
        case when e.service_execute then (select oid from pg_roles where rolname='service_role') end)
        or a.is_grantable) unexpected_acl
  from expected e left join pg_proc p on p.oid=to_regprocedure(e.signature)
) select signature,observed_owner,proconfig,
  array_remove(array[case when oid is null then 'MISSING_ROUTINE' end,
    case when not prosecdef then 'SECURITY_DEFINER_EXPECTED' end,
    case when service_granted is distinct from service_execute then 'SERVICE_ACL_MISMATCH' end,
    case when unexpected_acl then 'UNEXPECTED_ACL_OR_GRANT_OPTION' end],null) violations
from observed where oid is null or not prosecdef or service_granted is distinct from service_execute or unexpected_acl;

select 'REQUEST_RELATION_GUARD' violation from pg_class c where c.oid='private.timesheet_correction_requests'::regclass
  and (not c.relrowsecurity or not c.relforcerowsecurity
    or exists(select 1 from unnest(array['anon','authenticated','service_role','authenticator','supabase_admin']) role_name
      join pg_roles role_row on role_row.rolname=role_name
      where has_table_privilege(role_row.oid,c.oid,'SELECT,INSERT,UPDATE,DELETE')));

with expected(table_name,trigger_name) as (values
  ('public.invoice_hr_source_rows','trg_invoice_support_member_guard_v1'),
  ('private.timesheet_correction_requests','timesheet_correction_request_guard_v1'),
  ('public.timesheets','zz_timesheet_correction_whole_state_guard_v1'),
  ('public.timesheets','zz_timesheet_correction_applied_timesheet_guard_v1'),
  ('public.timesheets_financials','zz_timesheet_correction_financial_guard_v1'),
  ('private.timesheet_correction_requests','zz_timesheet_correction_terminal_guard_v1')
) select e.*,'MISSING_OR_DISABLED_TRIGGER' violation from expected e left join pg_trigger t
  on t.tgrelid=to_regclass(e.table_name) and t.tgname=e.trigger_name
  where t.oid is null or t.tgenabled<>'O';
