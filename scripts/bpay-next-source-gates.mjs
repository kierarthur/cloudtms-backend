import {createHash} from 'node:crypto';
// F-23 source-only release checks. No database, fixture or runtime execution.
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { repoRoot, inventory } from './cloudtms-db-release-lib.mjs';
import { scanManagedSql, readManagedClosure } from './cloudtms-managed-release-sql.mjs';
import { changedSourcePaths, routineDefinitions, definerInventory, verifyFinalDefiners } from './bpay-next-final-definers.mjs';

export const FROZEN_BASE='a6f7423eb71e6e7cd173e705c2290e5408a1d077';
const FOUNDATION_RECONCILED='3c91e5e6ce35b5833a5c218b77b741261eeebafa';
const DAILY_IDENTITY_GUARD='supabase/repeatable/11102026_0928_banking_pay_next_daily_work_identity_guard_v1.sql';
const DAILY_PUBLICATION_GUARDS="supabase/repeatable/11102026_0939_banking_pay_next_contractless_daily_publication_v1.sql";
// DY04 finite card-required additions; complete frozen parent is compared below.
const DAILY_GUARD_ADDITIONS=Object.freeze({
  "private.bpay_next_revision_guard_v1()": {
    "declaration": "  v_daily_work private.bpay_next_work%rowtype;\n",
    "declAnchor": "  v_total_source_ex numeric;\n",
    "block": "  select w.* into strict v_daily_work from private.bpay_next_work w where w.id=new.work_id;\n  if v_daily_work.origin_kind='DAILY' then\n    if new.source_kind is distinct from 'ORDINARY' or new.financial_snapshot_id is null\n       or jsonb_typeof(new.daily_approval_evidence_json) is distinct from 'object'\n       or new.daily_approval_evidence_json->>'contract_version' is distinct from 'BPAY_NEXT_DAILY_APPROVAL_EVIDENCE_V1'\n       or new.daily_approval_evidence_json->>'timesheet_id' is distinct from new.physical_timesheet_id::text\n       or new.daily_approval_evidence_json->>'timesheet_version' is distinct from new.physical_timesheet_version::text\n       or new.daily_approval_evidence_json->>'financial_id' is distinct from new.financial_snapshot_id::text\n       or new.daily_approval_evidence_json->>'candidate_id' is distinct from v_daily_work.candidate_id::text\n       or new.daily_approval_evidence_json->>'booking_id' is distinct from v_daily_work.booking_id\n       or new.daily_approval_evidence_json->>'work_date' is distinct from v_daily_work.origin_work_date::text then\n      raise exception using errcode='23514',message='BPAY_NEXT_DAILY_REVISION_EVIDENCE_REQUIRED';\n    end if;\n  elsif new.daily_approval_evidence_json is not null then\n    raise exception using errcode='23514',message='BPAY_NEXT_CONTRACT_REVISION_DAILY_EVIDENCE_FORBIDDEN';\n  end if;\n",
    "blockEnd": "  if new.detail_kind='SHIFT' then"
  },
  "private.bpay_next_work_insert_guard_v1()": {
    "declaration": "  v_original public.timesheets%rowtype;\n",
    "declAnchor": "  v_booking_id text;\n",
    "block": "  if new.origin_kind='DAILY' then\n    select t.* into strict v_original from public.timesheets t where t.timesheet_id=new.original_timesheet_id;\n    if v_original.contract_id is not null or v_original.sheet_scope is distinct from 'DAILY'::public.timesheet_scope_enum\n       or v_original.line_type::text is distinct from 'HOURS'\n       or (v_original.worked_start_iso at time zone 'Europe/London')::date is distinct from new.origin_work_date\n       or not exists (select 1 from public.timesheets_financials tf\n         where tf.timesheet_id=v_original.timesheet_id and tf.timesheet_version=v_original.version\n           and tf.candidate_id=new.candidate_id and tf.is_current is true and tf.is_stale is false\n           and tf.authorised_at_utc is not null and tf.basis::text='SELF_REPORTED') then\n      raise exception using errcode='23514',message='BPAY_NEXT_DAILY_ORIGINAL_IDENTITY_REQUIRED';\n    end if;\n  end if;\n",
    "blockEnd": "  return new;"
  }
});
// DY05 exact complete frozen parent plus genuine Daily captured-context guard only.
const DAILY_FROZEN_GUARD='supabase/repeatable/11102026_1001_banking_pay_next_daily_frozen_approval_context_v1.sql';
const DAILY_FROZEN_ADDITION=Object.freeze({
  "declaration": "  v_daily_work private.bpay_next_work%rowtype;\n  v_daily_timesheet public.timesheets%rowtype;\n  v_daily_financial public.timesheets_financials%rowtype;\n",
  "declAnchor": "  v_total_source_ex numeric;\n",
  "block": "  select w.* into strict v_daily_work from private.bpay_next_work w where w.id=new.work_id;\n  if v_daily_work.origin_kind='DAILY' or new.daily_approval_evidence_json is not null then\n    select t.* into strict v_daily_timesheet from public.timesheets t where t.timesheet_id=new.physical_timesheet_id;\n    if v_daily_timesheet.sheet_scope is distinct from 'DAILY'::public.timesheet_scope_enum then\n      raise exception using errcode='23514',message='BPAY_NEXT_CONTRACT_REVISION_DAILY_EVIDENCE_FORBIDDEN';\n    end if;\n    if new.source_kind is distinct from 'ORDINARY' or new.financial_snapshot_id is null\n       or v_daily_timesheet.sheet_scope is distinct from 'DAILY'::public.timesheet_scope_enum\n       or v_daily_timesheet.line_type::text is distinct from 'HOURS'\n       or v_daily_timesheet.booking_id is distinct from v_daily_work.booking_id\n       or v_daily_timesheet.contract_id is distinct from v_daily_work.contract_id\n       or v_daily_timesheet.version is distinct from new.physical_timesheet_version\n       or jsonb_typeof(new.daily_approval_evidence_json) is distinct from 'object'\n       or new.daily_approval_evidence_json->>'contract_version' is distinct from 'BPAY_NEXT_DAILY_APPROVAL_EVIDENCE_V1'\n       or new.daily_approval_evidence_json->>'sheet_scope' is distinct from 'DAILY'\n       or new.daily_approval_evidence_json->>'timesheet_id' is distinct from new.physical_timesheet_id::text\n       or new.daily_approval_evidence_json->>'timesheet_version' is distinct from new.physical_timesheet_version::text\n       or new.daily_approval_evidence_json->>'financial_id' is distinct from new.financial_snapshot_id::text\n       or new.daily_approval_evidence_json->>'candidate_id' is distinct from v_daily_work.candidate_id::text\n       or new.daily_approval_evidence_json->>'booking_id' is distinct from v_daily_work.booking_id\n       or new.daily_approval_evidence_json->'contract_id' is distinct from coalesce(to_jsonb(v_daily_work.contract_id),'null'::jsonb)\n       or new.daily_approval_evidence_json->>'work_date' is distinct from ((v_daily_timesheet.worked_start_iso at time zone 'Europe/London')::date)::text\n       or (v_daily_work.origin_kind='DAILY' and new.daily_approval_evidence_json->>'work_date' is distinct from v_daily_work.origin_work_date::text)\n       or new.daily_approval_evidence_json->'settings_authority_json' is distinct from v_daily_timesheet.settings_authority_json\n       or new.daily_approval_evidence_json->'timesheet_authorised_at_utc' is distinct from to_jsonb(v_daily_timesheet.authorised_at_server) then\n      raise exception using errcode='23514',message='BPAY_NEXT_DAILY_REVISION_EVIDENCE_REQUIRED';\n    end if;\n    select tf.* into strict v_daily_financial from public.timesheets_financials tf where tf.id=new.financial_snapshot_id;\n    if v_daily_financial.is_current is distinct from true or v_daily_financial.is_stale is distinct from false\n       or v_daily_financial.basis::text is distinct from 'SELF_REPORTED'\n       or v_daily_financial.authorised_at_utc is null or v_daily_timesheet.authorised_at_server is null\n       or v_daily_financial.candidate_id is distinct from v_daily_work.candidate_id\n       or new.daily_approval_evidence_json->>'client_id' is distinct from v_daily_financial.client_id::text\n       or new.daily_approval_evidence_json->'financial_authorised_at_utc' is distinct from to_jsonb(v_daily_financial.authorised_at_utc)\n       or new.daily_approval_evidence_json->>'role' is distinct from v_daily_financial.role\n       or new.daily_approval_evidence_json->>'band' is distinct from v_daily_financial.band\n       or new.job_title is distinct from v_daily_financial.role or new.band_label is distinct from v_daily_financial.band then\n      raise exception using errcode='23514',message='BPAY_NEXT_DAILY_REVISION_EVIDENCE_REQUIRED';\n    end if;\n  end if;\n",
  "blockEnd": "  if new.detail_kind='SHIFT' then"
});
export const FROZEN_FILES=Object.freeze([
  'supabase/migrations/03102026_1324_banking_pay_next_core.sql',
  'supabase/repeatable/03102026_1342_banking_pay_next_guards_v1.sql',
  'supabase/repeatable/03102026_1404_banking_pay_next_command_intake_v1.sql',
  'supabase/repeatable/04102026_0555_banking_pay_next_factual_legacy_projection_boundary_v1.sql',
  'supabase/repeatable/04102026_0556_banking_pay_next_source_factual_legacy_boundary_v1.sql',
  'supabase/migrations/04102026_0504_banking_pay_next_factual_email_claim_index.sql',
  'supabase/repeatable/04102026_0558_banking_pay_next_legacy_email_claim_boundary_v1.sql',
]);
// PF04: current operational roots and their finite reviewed helper closure.
// Resolve actual final definitions below; no unchanged-file/history waiver.
export const ROUTINE_WORK_SIGNATURES=Object.freeze([
  "private.bpay_next_source_query_refresh_v1(uuid,uuid)",
  "private.bpay_next_prepare_attention_page_v1(uuid,uuid,integer)",
  "private.bpay_next_retry_prepare_job_v1(uuid,jsonb)",
  "private.bpay_next_prepare_failure_v1(uuid,jsonb)",
  "private.bpay_next_prepare_retry_admit_v1(uuid,text,jsonb)",
  "private.bpay_next_prepare_retry_handoff_v1(uuid)",
  "private.bpay_next_prepare_attention_v1(uuid)",
  "private.bpay_next_retry_execution_v1(uuid,uuid,bigint)",
  "private.bpay_next_assert_candidate_financial_fence_v1(uuid,uuid,uuid,bigint)",
  // DR09 finite discard and native release continuation owners.
  "private.bpay_next_preparation_discard_fenced_v1(uuid,uuid)",
  "private.bpay_next_finish_discard_member_v1(uuid)",
  "private.bpay_next_accept_preparation_discard_v1(uuid,uuid,uuid,bigint,text)",
  "private.bpay_next_enroll_preparation_discard_one_v1(uuid)",
  "private.bpay_next_claim_preparation_discard_job_v1(uuid,integer)",
  "private.bpay_next_discard_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer)",
  "private.bpay_next_discarded_work_collection_scope_v1(uuid,uuid,uuid)",
  "private.bpay_next_reconcile_discarded_work_collection_v1(uuid,uuid)",
  "private.bpay_next_discarded_prepare_receipt_v1(uuid)",
  "private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid)",
  "private.bpay_next_work_collection_guard_v1()",
  "private.bpay_next_preparation_expiry_guard_v1()",
  "private.bpay_next_preparation_discard_guard_v1()",
  "private.bpay_next_enroll_one_v1()",
  "public.bpay_next_preparation_intake_v1(uuid,text,jsonb)",
  "public.bpay_next_financial_job_v1(text,uuid,text,jsonb)",
  "private.bpay_next_confirm_simple_run_v1(uuid,bigint)",
  "public.bpay_next_preparation_page_v1(uuid,text,jsonb)",
  "public.bpay_next_preparation_expiry_due_v1(timestamp with time zone,uuid,integer)",
  "private.bpay_next_accept_preparation_expiry_v1(uuid,uuid,uuid,timestamp with time zone,timestamp with time zone)",
  "public.bpay_next_expiry_dispatch_page_v1(integer)",
  "private.bpay_next_prepare_projection_page_v1(uuid,uuid,bigint,text,bigint,integer)",
  "private.bpay_next_project_worker_release_page_v1(uuid,uuid,bigint,text,bigint,integer)",
  "private.bpay_next_preparation_progress_v1(uuid)",
  "private.bpay_next_preparation_wake_v1(uuid)",
  // DR05 finite native retained-input execution/release support.
  "private.bpay_next_prepare_job_run_v1(uuid)",
  "private.bpay_next_prepare_job_active_v1(uuid)",
  "private.bpay_next_worker_action_receipt_v1(uuid,uuid,bigint,boolean)",
  "private.bpay_next_accept_worker_action_v1(uuid,uuid,text,uuid,bigint,uuid)",
  "private.bpay_next_enroll_worker_action_v1(uuid)",
  "private.bpay_next_worker_action_guard_v1()",
  "private.bpay_next_prepare_release_scope_v1(uuid,uuid,text)",
  "private.bpay_next_release_prepare_work_hold_v1(uuid,uuid,timestamp with time zone)",
  "private.bpay_next_worker_released_collection_scope_v1(uuid,uuid,uuid)",
  "private.bpay_next_reconcile_worker_released_collection_v1(uuid,uuid)",
  "private.bpay_next_release_prepare_case_hold_v1(uuid,uuid,timestamp with time zone)",
  "private.bpay_next_worker_release_page_v1(uuid,uuid,bigint,text,bigint,integer)",
  "private.bpay_next_exclude_preparation_week_v1(uuid,uuid)",
  "private.bpay_next_complete_retained_segment_parent_v1(uuid,uuid,text)",
  "private.bpay_next_worker_release_complete_v1(uuid,timestamp with time zone)",
  "private.bpay_next_worker_execution_guard_v1()",
  // WB38 finite native current-scope control and retained expense result pages.
  'private.bpay_next_snooze_many_v1(uuid,text,jsonb,boolean)',
  'private.bpay_next_snooze_scope_at_cut_v1(uuid,text,text,bigint)',
  'private.bpay_next_snooze_many_guard_v1()',
  // WB37 native indexed recovery only; existing refresh/money owners retained.
  'public.bpay_next_snooze_due_page_v1(jsonb)',
  'public.bpay_next_workbench_week_rollover_page_v1(jsonb)',
  'private.bpay_next_snooze_due_receipt_valid_v1(uuid)',
  "private.bpay_next_snooze_management_row_v1(uuid)",
  "private.bpay_next_snooze_management_page_v1(uuid,jsonb)",
  // WB36/WB44 A2: exact existing guard and native cancellation points.
  "private.bpay_next_snooze_revision_basis_v1(uuid,text,text)",
  "private.bpay_next_snooze_publication_cancel_valid_v1(jsonb,jsonb,boolean,timestamp with time zone)",
  "private.bpay_next_snooze_metadata_guard_v1()",
  "private.bpay_next_snooze_receipt_guard_v1()",
  // WB22 finite read-only memory helper closure; write owners remain separate.
  'private.bpay_next_selection_memory_intent_valid_v1(jsonb)',
  'private.bpay_next_selection_memory_resolve_v1(jsonb,jsonb)',
  'private.bpay_next_selection_memory_leaves_v1(jsonb)',
  'private.bpay_next_selection_memory_choices_v1(jsonb,uuid,jsonb)',
  'private.bpay_next_selection_memory_row_v1(text,jsonb,jsonb,uuid)',
  // WB19 administrative receipt/resume and native status only.

  // WB19 administrative receipt/resume and native status only.
  "private.bpay_next_retry_work_job_v1(uuid,jsonb,boolean)",
  "public.bpay_next_workbench_command_v1(jsonb)",
  "public.bpay_next_work_job_failure_service_v1(uuid,uuid,bigint,text,text,text)",
  "private.bpay_next_workbench_job_progress_v1(jsonb,jsonb,integer)",
  'private.bpay_next_apply_position_page_v1(uuid,uuid,bigint,text,text,integer)',
  'private.bpay_next_begin_prepare_projection_v1(uuid,uuid,bigint,integer)',
  'private.bpay_next_prepare_projection_page_v1(uuid,uuid,bigint,text,bigint,integer)',
  'private.bpay_next_claim_prepare_job_v1(uuid,integer)',
  'private.bpay_next_pin_selected_work_v1(uuid,uuid,bigint,bigint)',
  'private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer)',
  'private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer)',
  'private.bpay_next_complete_no_case_worker_v1(uuid,uuid,bigint)',
  'private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer)',
  'private.bpay_next_case_allocation_plan_v1(jsonb)',
  'private.bpay_next_confirm_simple_run_v1(uuid,bigint)',
  'private.bpay_next_begin_simple_transfer_v1(uuid,uuid)',
  'private.bpay_next_claim_simple_transfer_job_v1(uuid,integer)',
  'private.bpay_next_build_simple_transfer_page_v1(uuid,uuid,bigint,bigint,integer)',
  'private.bpay_next_begin_case_transfer_v1(uuid,uuid)',
  'private.bpay_next_build_case_transfer_page_v1(uuid,uuid,bigint,bigint,integer)',
  'private.bpay_next_build_destination_group_page_v1(uuid,uuid,bigint,bigint,integer)',
  'private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid)',
  'private.bpay_next_expired_work_collection_scope_v1(uuid,uuid,uuid)',
  'private.bpay_next_work_collection_origin_v1(uuid,uuid,uuid)',
  'private.bpay_next_bound_source_snapshot_v1(uuid,bigint)',
  'private.bpay_next_source_disposition_seed_v1(uuid,text,uuid,uuid,uuid)',
  'private.bpay_next_source_book_seed_proof_v1(uuid,text,uuid,uuid,uuid)',
  'private.bpay_next_source_binding_members_v1(uuid)',
  'private.bpay_next_initial_tsfin_bound_assert_v1(uuid)',
  'private.bpay_next_initial_tsfin_certificate_v1(uuid)',
  'private.bpay_next_source_command_id_v1(uuid)',
  'private.weekly_source_pay_query_admit_v2()',
  'private.bpay_next_publish_no_case_week_fact_v1(uuid,uuid)',
  'private.bpay_next_capture_instruction_destination_v1(uuid)',
  'private.bpay_next_case_allocation_row_v1(text,numeric,numeric,numeric,numeric,numeric,numeric,numeric)',
  'private.bpay_next_value_source_for_target_v1(numeric,text,text,boolean,uuid,date)',
  'private.pay_finance_selected_quote_v1(public.pay_finance_component_resolution_mode_enum,numeric,text,text,numeric,numeric,numeric,numeric,numeric,boolean)',
  'private.bpay_next_channel_drafts_page_v1(uuid)',
  'private.bpay_next_receive_command_v1(uuid,text)',
  'private._candidate_expense_source_family_v1(uuid)',
  // WB36 exact normalized helper closure: no hidden financial-history edge.
  'private.bpay_next_selected_position_keys_v1(uuid,uuid,text,integer)',
  'private.bpay_next_run_segment_members_v1(uuid,uuid,text,text,integer)',
  'private.bpay_next_capture_segment_page_v1(uuid,integer)',
  'private.bpay_next_work_segment_gross_balance_v1(uuid,text,text)',
  'private.bpay_next_record_work_segment_origin_shares_v1(uuid,uuid)',
  'private.bpay_next_segment_collection_basis_v1(uuid,text,uuid)',
  'private.bpay_next_work_segment_debt_basis_v1(uuid,text,uuid)',
  'private.bpay_next_snooze_at_cut_v1(uuid,uuid,text,text,bigint,date)',
  'private.bpay_next_segment_pay_basis_v1(uuid)',
  'private.bpay_next_line_pay_basis_v1(uuid)',
  // WB08 exact typed current projection closure. No history waiver.
  'private.bpay_next_workbench_require_next_v1()',
  'private.bpay_next_workbench_ensure_metadata_v1(uuid)',
  'private.bpay_next_workbench_group_contribution_v1(text)',
  'private.bpay_next_workbench_apply_delta_v1(private.bpay_next_workbench_delta[],private.bpay_next_workbench_delta[])',
  'private.bpay_next_workbench_accepted_work_v1(uuid,text,text)',
  'private.bpay_next_workbench_work_members_v1(uuid,uuid)',
  'private.bpay_next_workbench_mark_processing_v1(uuid)',
  'private.bpay_next_work_wait_state_v1(uuid)',
  'private.bpay_next_workbench_refresh_work_v1(uuid)',
  'private.bpay_next_workbench_refresh_case_v1(uuid)',
  'private.bpay_next_workbench_refresh_group_display_v1(text)',
  'private.bpay_next_workbench_refresh_many_v1(uuid[],uuid[])',
  'private.bpay_next_workbench_set_bank_warning_v1(uuid,text)',
  'private.bpay_next_workbench_candidate_name_trigger_v1()',
  "private.bpay_next_workbench_refresh_work_core_v1(uuid)",
  "private.bpay_next_workbench_schedule_task_v1(text,uuid,uuid,uuid,boolean,boolean,boolean)",
  "private.bpay_next_workbench_metadata_changed_v1(uuid,boolean,boolean)",
  "private.bpay_next_workbench_umbrella_metadata_trigger_v1()",
  "private.bpay_next_workbench_task_result_v1(private.bpay_next_workbench_task,boolean,integer,boolean,boolean)",
  "private.bpay_next_workbench_finish_task_v1(uuid)",
  "private.bpay_next_workbench_queue_origin_v1(uuid)",
  "private.bpay_next_workbench_claim_task_v1(uuid,integer)",
  "private.bpay_next_workbench_step_task_v1(uuid,uuid,bigint,bigint,bigint,text,text,integer)",
  "public.bpay_next_workbench_runtime_service_v1(text,uuid,jsonb)",
  'private.bpay_next_workbench_snooze_due_trigger_v1()',
  "public.bpay_next_workbench_page_v1(jsonb)",
  "private.bpay_next_workbench_header_page_v1(jsonb,jsonb,integer)",
  "private.bpay_next_workbench_candidates_page_v1(jsonb,jsonb,integer)",
  "private.bpay_next_workbench_action_required_page_v1(jsonb,jsonb,integer)",
  "private.bpay_next_workbench_blocked_page_v1(jsonb,jsonb,integer)",
  "private.bpay_next_workbench_issue_detail_page_v1(jsonb,jsonb,integer)",
  "private.bpay_next_workbench_candidate_groups_page_v1(jsonb,jsonb,integer)",
  "private.bpay_next_workbench_group_lines_page_v1(jsonb,jsonb,integer)",
  "private.bpay_next_workbench_pick_candidates_page_v1(jsonb,jsonb,integer)",
  "private.bpay_next_workbench_pick_clients_page_v1(jsonb,jsonb,integer)",
  "private.bpay_next_workbench_export_rows_page_v1(jsonb,jsonb,integer)",
]);
const ROUTINE_WORK_CALLEES=Object.freeze({
  "private.bpay_next_snooze_management_page_v1(uuid,jsonb)": ["all","to_char","jsonb_object_keys","octet_length","jsonb_typeof","jsonb_build_object","jsonb_build_array","jsonb_array_length","unnest","isfinite","private.bpay_next_snooze_management_row_v1"],
  "private.bpay_next_snooze_management_row_v1(uuid)": ["to_char","private.bpay_next_snooze_revision_basis_v1"],
  "private.bpay_next_snooze_receipt_guard_v1()": ["upper","to_jsonb","jsonb_set","private.bpay_next_snooze_due_receipt_valid_v1","private.bpay_next_snooze_publication_cancel_valid_v1"],
  "private.bpay_next_snooze_metadata_guard_v1()": ["jsonb_strip_nulls","to_jsonb","jsonb_set","clock_timestamp","current_setting","octet_length","private.bpay_next_snooze_publication_cancel_valid_v1","private.bpay_next_snooze_context_v1"],
  "private.bpay_next_snooze_publication_cancel_valid_v1(jsonb,jsonb,boolean,timestamp with time zone)": ["current_setting","private.bpay_next_snooze_revision_basis_v1"],
  "private.bpay_next_snooze_revision_basis_v1(uuid,text,text)": ["octet_length","private.bpay_next_segment_pay_basis_v1","private.bpay_next_line_pay_basis_v1"],
  "private.bpay_next_source_query_refresh_v1(uuid,uuid)": ["array_agg","cardinality","private.bpay_next_workbench_refresh_work_v1"],
  "private.bpay_next_accept_worker_action_v1(uuid,uuid,text,uuid,bigint,uuid)": ["private.bpay_next_retry_execution_v1","private.bpay_next_prepare_attention_v1"],
  "private.bpay_next_discard_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer)": ["private.bpay_next_retry_execution_v1"],
  "private.bpay_next_expire_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer)": ["private.bpay_next_retry_execution_v1"],
  "private.bpay_next_discarded_prepare_receipt_v1(uuid)": ["private.bpay_next_retry_execution_v1"],
  "private.bpay_next_expired_prepare_receipt_v1(uuid)": ["private.bpay_next_retry_execution_v1"],
  "private.bpay_next_enroll_preparation_discard_one_v1(uuid)": ["private.bpay_next_retry_execution_v1"],
  "private.bpay_next_enroll_preparation_expiry_one_v1(uuid)": ["private.bpay_next_retry_execution_v1"],
  "private.bpay_next_review_simple_run_v1(uuid)": ["private.bpay_next_prepare_attention_v1"],
  "public.bpay_next_preparation_page_v1(uuid,text,jsonb)": ["private.bpay_next_prepare_attention_page_v1"],
  "public.bpay_next_financial_job_v1(text,uuid,text,jsonb)": ["private.bpay_next_prepare_retry_admit_v1","private.bpay_next_prepare_failure_v1"],
  "private.bpay_next_worker_execution_guard_v1()": ["private.bpay_next_retry_execution_v1"],
  "private.bpay_next_prepare_job_active_v1(uuid)": ["private.bpay_next_retry_execution_v1"],
  "private.bpay_next_record_case_hold_transition_v1(uuid,uuid,text)": ["private.bpay_next_retry_execution_v1"],
  "private.bpay_next_prepare_attention_page_v1(uuid,uuid,integer)": ["private.bpay_next_prepare_job_active_v1","jsonb_build_object","btrim","concat_ws","left","statement_timestamp","octet_length","jsonb_build_array"],
  "private.bpay_next_retry_prepare_job_v1(uuid,jsonb)": ["private.bpay_next_prepare_job_run_v1","jsonb_build_object","clock_timestamp","private.bpay_next_prepare_job_active_v1"],
  "private.bpay_next_prepare_failure_v1(uuid,jsonb)": ["jsonb_typeof","octet_length","jsonb_object_keys","private.bpay_next_prepare_job_run_v1","clock_timestamp","private.bpay_next_prepare_job_active_v1","jsonb_build_object","least","make_interval","private.bpay_next_prepare_retry_handoff_v1"],
  "private.bpay_next_prepare_retry_admit_v1(uuid,text,jsonb)": ["clock_timestamp","private.bpay_next_prepare_job_run_v1","private.bpay_next_prepare_retry_handoff_v1","jsonb_build_object"],
  "private.bpay_next_prepare_retry_handoff_v1(uuid)": ["clock_timestamp","private.bpay_next_review_simple_run_v1"],
  "private.bpay_next_prepare_attention_v1(uuid)": ["private.bpay_next_prepare_job_active_v1"],
  "private.bpay_next_retry_execution_v1(uuid,uuid,bigint)": ["private.bpay_next_prepare_job_run_v1"],
  'private.bpay_next_snooze_many_v1(uuid,text,jsonb,boolean)':['current_setting','octet_length','length','clock_timestamp','jsonb_object_keys','jsonb_array_elements_text','private.bpay_next_workbench_require_next_v1','private.bpay_next_snooze_context_v1','private.bpay_next_receive_command_v1','private.bpay_next_snooze_scope_at_cut_v1','private.bpay_next_line_pay_basis_v1','private.bpay_next_snooze_action_v1','array_agg','to_jsonb','initcap','distinct'],
  'private.bpay_next_snooze_scope_at_cut_v1(uuid,text,text,bigint)':[],
  'private.bpay_next_snooze_many_guard_v1()':['current_setting','to_jsonb','private.bpay_next_line_pay_basis_v1'],
  'public.bpay_next_snooze_due_page_v1(jsonb)':['by','private.bpay_next_workbench_require_next_v1','private.bpay_next_receive_command_v1','private.bpay_next_snooze_due_receipt_valid_v1','distinct','clock_timestamp','octet_length','jsonb_insert','array_append','array_remove','gen_random_uuid','to_jsonb'],
  'public.bpay_next_workbench_week_rollover_page_v1(jsonb)':['lateral','by','private.bpay_next_workbench_require_next_v1','private.bpay_next_workbench_refresh_work_v1','private.bpay_next_workbench_refresh_case_v1','distinct','clock_timestamp','octet_length','jsonb_insert','array_append','array_remove','substring'],
  'private.bpay_next_snooze_due_receipt_valid_v1(uuid)':['jsonb_set','to_jsonb'],
  'private.bpay_next_case_allocation_plan_v1(jsonb)':['private.bpay_next_case_edit_origin_v1','isfinite','trunc','max','jsonb_populate_recordset','private.bpay_next_case_allocation_row_v1','private.bpay_next_value_source_for_target_v1','private.bpay_next_segment_collection_basis_v1','private.bpay_next_work_wait_state_v1','to_jsonb','jsonb_populate_record','cardinality','octet_length','jsonb_object_keys','date','sum','filter','distinct'],
  "private.bpay_next_retry_work_job_v1(uuid,jsonb,boolean)": ["private.bpay_next_retry_prepare_job_v1","private.bpay_next_workbench_require_next_v1","pg_catalog.octet_length","jsonb_object_keys","unnest","to_char","clock_timestamp","pg_catalog.clock_timestamp"],
  "public.bpay_next_workbench_command_v1(jsonb)": ["private.bpay_next_retry_work_job_v1","private.bpay_next_snooze_many_v1","private.bpay_next_snooze_action_v1","private.bpay_next_workbench_require_next_v1","public.bpay_next_financial_intake_v1","current_setting","octet_length","jsonb_object_keys","to_jsonb","jsonb_set"],
  "public.bpay_next_work_job_failure_service_v1(uuid,uuid,bigint,text,text,text)": ["current_setting","octet_length","clock_timestamp"],
  "private.bpay_next_workbench_job_progress_v1(jsonb,jsonb,integer)": ["octet_length","statement_timestamp","floor","jsonb_object_keys","by","loop"],

  'private.bpay_next_selection_memory_intent_valid_v1(jsonb)': ['octet_length'],
  'private.bpay_next_selection_memory_resolve_v1(jsonb,jsonb)': [],
  'private.bpay_next_selection_memory_leaves_v1(jsonb)': [],
  'private.bpay_next_selection_memory_choices_v1(jsonb,uuid,jsonb)': ['private.bpay_next_selection_memory_leaves_v1','bool_or','lateral'],
  'private.bpay_next_selection_memory_row_v1(text,jsonb,jsonb,uuid)': ['private.bpay_next_selection_memory_choices_v1','jsonb_set','split_part','max','sum','filter','octet_length'],
  "public.bpay_next_workbench_page_v1(jsonb)": ["private.bpay_next_snooze_management_page_v1","private.bpay_next_snooze_context_v1","lateral","generate_subscripts","private.bpay_next_case_allocation_plan_v1","private.bpay_next_selection_memory_choices_v1","private.bpay_next_selection_memory_leaves_v1","private.bpay_next_value_source_for_target_v1","private.bpay_next_work_wait_state_v1","private.bpay_next_source_command_id_v1","private.weekly_source_sha256_jsonb_v1","to_jsonb","jsonb_agg","array_agg","cardinality","max","sum","filter","distinct","date","private.bpay_next_selection_memory_intent_valid_v1","private.bpay_next_selection_memory_resolve_v1","unnest","private.bpay_next_workbench_job_progress_v1","public.pay_banking_official_date_context_v1","private.bpay_next_workbench_header_page_v1","private.bpay_next_workbench_candidates_page_v1","private.bpay_next_workbench_action_required_page_v1","private.bpay_next_workbench_blocked_page_v1","private.bpay_next_workbench_issue_detail_page_v1","private.bpay_next_workbench_candidate_groups_page_v1","private.bpay_next_workbench_group_lines_page_v1","private.bpay_next_workbench_pick_candidates_page_v1","private.bpay_next_workbench_pick_clients_page_v1","private.bpay_next_workbench_export_rows_page_v1","pg_catalog.current_setting","octet_length","jsonb_object_keys","jsonb_set","length"],
  "private.bpay_next_workbench_header_page_v1(jsonb,jsonb,integer)": ["private.bpay_next_workbench_kind_page_v1"],
  "private.bpay_next_workbench_candidates_page_v1(jsonb,jsonb,integer)": ["private.bpay_next_workbench_kind_page_v1"],
  "private.bpay_next_workbench_action_required_page_v1(jsonb,jsonb,integer)": ["private.bpay_next_workbench_kind_page_v1"],
  "private.bpay_next_workbench_blocked_page_v1(jsonb,jsonb,integer)": ["private.bpay_next_workbench_kind_page_v1"],
  "private.bpay_next_workbench_issue_detail_page_v1(jsonb,jsonb,integer)": ["private.bpay_next_workbench_kind_page_v1"],
  "private.bpay_next_workbench_candidate_groups_page_v1(jsonb,jsonb,integer)": ["private.bpay_next_workbench_kind_page_v1"],
  "private.bpay_next_workbench_group_lines_page_v1(jsonb,jsonb,integer)": ["private.bpay_next_workbench_kind_page_v1"],
  "private.bpay_next_workbench_pick_candidates_page_v1(jsonb,jsonb,integer)": ["private.bpay_next_workbench_kind_page_v1"],
  "private.bpay_next_workbench_pick_clients_page_v1(jsonb,jsonb,integer)": ["private.bpay_next_workbench_kind_page_v1"],
  "private.bpay_next_workbench_export_rows_page_v1(jsonb,jsonb,integer)": ["private.bpay_next_workbench_kind_page_v1"],
  'private.bpay_next_workbench_snooze_due_trigger_v1()': ['private.bpay_next_workbench_queue_origin_v1','private.bpay_next_workbench_refresh_case_v1'],
  "private.bpay_next_workbench_refresh_group_display_v1(text)": ["lateral","private.bpay_next_workbench_require_next_v1","pg_catalog.jsonb_build_array","bool_or","min","array_agg","filter","sum","cardinality"],
  "private.bpay_next_workbench_refresh_work_core_v1(uuid)": ["private.bpay_next_source_query_segment_exclusions_v1","private.bpay_next_workbench_refresh_group_display_v1","private.weekly_source_pay_query_gate_v1","to_jsonb","char_length","private.bpay_next_work_wait_state_v1","private.bpay_next_workbench_require_next_v1","private.bpay_next_workbench_ensure_metadata_v1","private.bpay_next_workbench_group_contribution_v1","private.bpay_next_workbench_work_members_v1","private.bpay_next_workbench_accepted_work_v1","private.bpay_next_snooze_at_cut_v1","private.bpay_next_workbench_apply_delta_v1","array_agg","transaction_timestamp","gen_random_uuid","filter","sum"],
  "private.bpay_next_workbench_schedule_task_v1(text,uuid,uuid,uuid,boolean,boolean,boolean)": ["private.bpay_next_workbench_require_next_v1","private.bpay_next_work_wait_state_v1","private.bpay_next_workbench_ensure_metadata_v1","clock_timestamp","transaction_timestamp","by"],
  "private.bpay_next_workbench_metadata_changed_v1(uuid,boolean,boolean)": ["private.bpay_next_workbench_require_next_v1","private.bpay_next_workbench_ensure_metadata_v1","private.bpay_next_workbench_schedule_task_v1","concat_ws","transaction_timestamp"],
  "private.bpay_next_workbench_umbrella_metadata_trigger_v1()": ["private.bpay_next_workbench_schedule_task_v1"],
  "private.bpay_next_workbench_task_result_v1(private.bpay_next_workbench_task,boolean,integer,boolean,boolean)": [],
  "private.bpay_next_workbench_finish_task_v1(uuid)": ["private.bpay_next_workbench_require_next_v1","private.bpay_next_workbench_ensure_metadata_v1","transaction_timestamp"],
  "private.bpay_next_workbench_queue_origin_v1(uuid)": ["private.bpay_next_workbench_require_next_v1","private.bpay_next_workbench_schedule_task_v1","private.bpay_next_workbench_mark_processing_v1"],
  "private.bpay_next_workbench_claim_task_v1(uuid,integer)": ["private.bpay_next_workbench_require_next_v1","private.bpay_next_workbench_task_result_v1","clock_timestamp","transaction_timestamp","gen_random_uuid","make_interval"],
  "private.bpay_next_workbench_step_task_v1(uuid,uuid,bigint,bigint,bigint,text,text,integer)": ["private.bpay_next_workbench_require_next_v1","private.bpay_next_work_wait_state_v1","private.bpay_next_workbench_ensure_metadata_v1","private.bpay_next_workbench_schedule_task_v1","private.bpay_next_workbench_task_result_v1","private.bpay_next_workbench_refresh_many_v1","private.bpay_next_workbench_group_contribution_v1","private.bpay_next_workbench_apply_delta_v1","private.bpay_next_workbench_finish_task_v1","private.bpay_next_workbench_refresh_case_v1","private.bpay_next_workbench_refresh_work_core_v1","private.bpay_next_workbench_metadata_changed_v1","clock_timestamp","transaction_timestamp","octet_length","array_agg","unnest","filter","cardinality","by","concat_ws"],
  "public.bpay_next_workbench_runtime_service_v1(text,uuid,jsonb)": ["private.bpay_next_workbench_refresh_case_v1","substring","private.bpay_next_workbench_queue_origin_v1","private.bpay_next_workbench_schedule_task_v1","private.bpay_next_workbench_claim_task_v1","private.bpay_next_workbench_step_task_v1","current_setting","pg_column_size","clock_timestamp","transaction_timestamp","array_agg","unnest","filter","cardinality"],
  'private.bpay_next_workbench_require_next_v1()': [],
  'private.bpay_next_workbench_ensure_metadata_v1(uuid)': [
    'private.bpay_next_workbench_require_next_v1','concat_ws'],
  'private.bpay_next_workbench_group_contribution_v1(text)': [
    'private.bpay_next_workbench_require_next_v1','lateral','sum','filter','distinct'],
  'private.bpay_next_workbench_apply_delta_v1(private.bpay_next_workbench_delta[],private.bpay_next_workbench_delta[])': [
    'private.bpay_next_workbench_require_next_v1','cardinality','pg_column_size','unnest','sum'],
  'private.bpay_next_workbench_accepted_work_v1(uuid,text,text)': [
    'private.bpay_next_workbench_require_next_v1','private.bpay_next_snooze_at_cut_v1'],
  'private.bpay_next_workbench_work_members_v1(uuid,uuid)': [
    'private.bpay_next_workbench_require_next_v1','private.bpay_next_bound_source_snapshot_v1',
    'private.bpay_next_work_segment_gross_balance_v1','sum'],
  'private.bpay_next_workbench_mark_processing_v1(uuid)': [
    'private.bpay_next_workbench_require_next_v1','private.bpay_next_workbench_ensure_metadata_v1',
    'private.bpay_next_work_wait_state_v1','private.bpay_next_workbench_group_contribution_v1',
    'private.bpay_next_workbench_apply_delta_v1','array_agg','transaction_timestamp','gen_random_uuid'],
  'private.bpay_next_work_wait_state_v1(uuid)': ['private.bpay_next_workbench_require_next_v1','sum'],
  "private.bpay_next_workbench_refresh_work_v1(uuid)": ["private.bpay_next_workbench_require_next_v1","private.bpay_next_work_wait_state_v1","private.bpay_next_workbench_queue_origin_v1","private.bpay_next_workbench_refresh_work_core_v1","private.bpay_next_workbench_refresh_case_v1","array_agg","cardinality","unnest"],
  'private.bpay_next_workbench_refresh_case_v1(uuid)': [
    'private.bpay_next_workbench_refresh_group_display_v1',
    'private.bpay_next_workbench_require_next_v1','private.bpay_next_workbench_ensure_metadata_v1',
    'private.bpay_next_workbench_group_contribution_v1','private.bpay_next_workbench_apply_delta_v1',
    'array_agg','transaction_timestamp','gen_random_uuid','max','filter','on','sum'],
  "private.bpay_next_workbench_refresh_many_v1(uuid[],uuid[])": ["private.bpay_next_workbench_require_next_v1","private.bpay_next_workbench_ensure_metadata_v1","private.bpay_next_workbench_refresh_work_v1","private.bpay_next_workbench_refresh_case_v1","private.bpay_next_workbench_queue_origin_v1","private.bpay_next_workbench_schedule_task_v1","private.bpay_next_workbench_mark_processing_v1","cardinality","pg_column_size","array_position","array_agg","unnest"],
  "private.bpay_next_workbench_set_bank_warning_v1(uuid,text)": ["private.bpay_next_workbench_require_next_v1","private.bpay_next_workbench_ensure_metadata_v1","private.bpay_next_workbench_schedule_task_v1","transaction_timestamp"],
  "private.bpay_next_workbench_candidate_name_trigger_v1()": ["private.bpay_next_workbench_metadata_changed_v1","concat_ws"],
  "private.bpay_next_apply_position_page_v1(uuid,uuid,bigint,text,text,integer)": [
    "private.bpay_next_snooze_revision_basis_v1","to_jsonb",
    "private.bpay_next_workbench_mark_processing_v1",
    "private.bpay_next_workbench_refresh_many_v1",
    "private.bpay_next_reconcile_work_collection_v1",
    "pg_catalog.clock_timestamp",
    "pg_catalog.transaction_timestamp"
  ],
  "private.bpay_next_begin_prepare_projection_v1(uuid,uuid,bigint,integer)": ["pg_catalog.jsonb_build_object"],
  "private.bpay_next_prepare_projection_page_v1(uuid,uuid,bigint,text,bigint,integer)": ["private.bpay_next_retry_execution_v1","max","private.bpay_next_preparation_expiry_fenced_v1","private.bpay_next_workbench_refresh_many_v1","private.bpay_next_review_simple_run_v1","pg_catalog.clock_timestamp","pg_catalog.array_agg","pg_catalog.jsonb_build_object"],
  "private.bpay_next_claim_prepare_job_v1(uuid,integer)": [
    "pg_catalog.clock_timestamp",
    "pg_catalog.gen_random_uuid",
    "pg_catalog.make_interval",
    "pg_catalog.transaction_timestamp"
  ],
  "private.bpay_next_pin_selected_work_v1(uuid,uuid,bigint,bigint)": ["private.bpay_next_begin_prepare_projection_v1",
    "private._candidate_expense_source_family_v1",
    "private.bpay_next_source_command_id_v1",
    "private.weekly_source_pay_query_admit_v2",
    "pg_catalog.btrim",
    "pg_catalog.clock_timestamp",
    "pg_catalog.decode",
    "pg_catalog.isfinite",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.jsonb_typeof",
    "pg_catalog.to_jsonb"
  ],
  "private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer)": ["jsonb_typeof","jsonb_array_length","private.bpay_next_begin_prepare_projection_v1",
    "private.bpay_next_selected_position_keys_v1",
    "private.bpay_next_capture_segment_page_v1",
    "private.bpay_next_snooze_at_cut_v1",
    "private.bpay_next_bound_source_snapshot_v1",
    "private.bpay_next_source_disposition_seed_v1",
    "private.bpay_next_work_collection_origin_v1",
    "pg_catalog.array_agg",
    "pg_catalog.cardinality",
    "pg_catalog.clock_timestamp",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.octet_length"
  ],
  "private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer)": ["pg_catalog.round","public._pay_umbrella_vat_calc","private.bpay_next_begin_prepare_projection_v1","private.bpay_next_record_work_segment_origin_shares_v1","private.bpay_next_bound_source_snapshot_v1","private.bpay_next_value_source_for_target_v1","private.bpay_next_workbench_refresh_many_v1","array_append","pg_catalog.clock_timestamp","pg_catalog.jsonb_build_object","pg_catalog.transaction_timestamp"],
  "private.bpay_next_complete_no_case_worker_v1(uuid,uuid,bigint)": ["private.bpay_next_begin_prepare_projection_v1",
    "private.bpay_next_publish_no_case_week_fact_v1",
    "pg_catalog.clock_timestamp",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.transaction_timestamp"
  ],
  "private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer)": [
    "private.bpay_next_record_case_hold_transition_v1","private.bpay_next_begin_prepare_projection_v1",
    "private.bpay_next_case_allocation_plan_v1","to_jsonb","jsonb_populate_record","jsonb_agg","jsonb_set","substring",
    "private.bpay_next_capture_instruction_destination_v1",
    "private.bpay_next_case_allocation_row_v1",
    "private.bpay_next_complete_no_case_worker_v1",
    "private.bpay_next_value_source_for_target_v1",
    "private.bpay_next_work_collection_origin_v1",
    "pg_catalog.clock_timestamp",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.transaction_timestamp",
    "pg_catalog.trunc"
  ],
  "private.bpay_next_confirm_simple_run_v1(uuid,bigint)": [
    "private.bpay_next_channel_drafts_page_v1",
    "pg_catalog.clock_timestamp",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.transaction_timestamp"
  ],
  "private.bpay_next_begin_simple_transfer_v1(uuid,uuid)": [
    "private.bpay_next_begin_case_transfer_v1",
    "private.bpay_next_receive_command_v1",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.transaction_timestamp"
  ],
  "private.bpay_next_claim_simple_transfer_job_v1(uuid,integer)": [
    "pg_catalog.clock_timestamp",
    "pg_catalog.gen_random_uuid",
    "pg_catalog.make_interval",
    "pg_catalog.transaction_timestamp"
  ],
  "private.bpay_next_build_simple_transfer_page_v1(uuid,uuid,bigint,bigint,integer)": [
    "private.bpay_next_build_case_transfer_page_v1",
    "private.bpay_next_build_destination_group_page_v1",
    "pg_catalog.clock_timestamp",
    "pg_catalog.jsonb_build_object"
  ],
  "private.bpay_next_begin_case_transfer_v1(uuid,uuid)": [
    "private.bpay_next_receive_command_v1",
    "pg_catalog.hashtextextended",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.pg_advisory_xact_lock",
    "pg_catalog.transaction_timestamp"
  ],
  "private.bpay_next_build_case_transfer_page_v1(uuid,uuid,bigint,bigint,integer)": [
    "pg_catalog.clock_timestamp",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.transaction_timestamp"
  ],
  "private.bpay_next_build_destination_group_page_v1(uuid,uuid,bigint,bigint,integer)": [
    "pg_catalog.clock_timestamp",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.transaction_timestamp"
  ],
  "private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid)": [
    "private.bpay_next_segment_collection_basis_v1",
    "private.bpay_next_expired_work_collection_scope_v1",
    "pg_catalog.cardinality",
    "pg_catalog.char_length",
    "pg_catalog.clock_timestamp",
    "pg_catalog.transaction_timestamp"
  ],
  "private.bpay_next_expired_work_collection_scope_v1(uuid,uuid,uuid)": [
    "pg_catalog.clock_timestamp"
  ],
  "private.bpay_next_work_collection_origin_v1(uuid,uuid,uuid)": ["private.bpay_next_segment_collection_basis_v1"],
  "private.bpay_next_selected_position_keys_v1(uuid,uuid,text,integer)": [],
  "private.bpay_next_run_segment_members_v1(uuid,uuid,text,text,integer)": [],
  "private.bpay_next_capture_segment_page_v1(uuid,integer)": [
    "private.bpay_next_run_segment_members_v1","private.bpay_next_work_segment_gross_balance_v1",
    "private.bpay_next_work_collection_origin_v1","clock_timestamp","row","sum"
  ],
  "private.bpay_next_work_segment_gross_balance_v1(uuid,text,text)": ["sum"],
  "private.bpay_next_record_work_segment_origin_shares_v1(uuid,uuid)": [
    "private.bpay_next_value_source_for_target_v1","lag","sum"
  ],
  "private.bpay_next_segment_collection_basis_v1(uuid,text,uuid)": ["private.bpay_next_work_segment_debt_basis_v1","sum"],
  "private.bpay_next_work_segment_debt_basis_v1(uuid,text,uuid)": ["private.bpay_next_work_segment_gross_balance_v1","sum"],
  "private.bpay_next_snooze_at_cut_v1(uuid,uuid,text,text,bigint,date)": [
    "private.bpay_next_segment_pay_basis_v1","private.bpay_next_line_pay_basis_v1","lateral","by"
  ],
  "private.bpay_next_segment_pay_basis_v1(uuid)": ["pg_catalog.sha256","pg_catalog.convert_to"],
  "private.bpay_next_line_pay_basis_v1(uuid)": ["pg_catalog.sha256","pg_catalog.convert_to","jsonb_agg"],
  "private.bpay_next_bound_source_snapshot_v1(uuid,bigint)": [
    "private.bpay_next_initial_tsfin_bound_assert_v1",
    "private.bpay_next_source_binding_members_v1",
    "private.bpay_next_source_command_id_v1",
    "pg_catalog.count"
  ],
  "private.bpay_next_source_disposition_seed_v1(uuid,text,uuid,uuid,uuid)": [
    "private.bpay_next_workbench_refresh_work_v1",
    "private.bpay_next_source_book_seed_proof_v1"
  ],
  "private.bpay_next_source_book_seed_proof_v1(uuid,text,uuid,uuid,uuid)": [
    "pg_catalog.convert_to",
    "pg_catalog.jsonb_build_array",
    "pg_catalog.sha256"
  ],
  "private.bpay_next_source_binding_members_v1(uuid)": [
    "pg_catalog.cardinality"
  ],
  "private.bpay_next_initial_tsfin_bound_assert_v1(uuid)": [
    "private.bpay_next_initial_tsfin_certificate_v1"
  ],
  "private.bpay_next_initial_tsfin_certificate_v1(uuid)": [
    "private.bpay_next_source_command_id_v1",
    "private.weekly_source_effective_inventory_v1",
    "private.weekly_source_inventory_approval_basis_v2",
    "private.weekly_source_sha256_jsonb_v1"
  ],
  "private.bpay_next_source_command_id_v1(uuid)": [
    "pg_catalog.md5",
    "pg_catalog.substr"
  ],
  "private.weekly_source_pay_query_admit_v2()": [
    "pg_catalog.hashtextextended",
    "pg_catalog.pg_try_advisory_xact_lock"
  ],
  "private.bpay_next_publish_no_case_week_fact_v1(uuid,uuid)": [
    "pg_catalog.trunc"
  ],
  "private.bpay_next_capture_instruction_destination_v1(uuid)": [],
  "private.bpay_next_case_allocation_row_v1(text,numeric,numeric,numeric,numeric,numeric,numeric,numeric)": [
    "pg_catalog.trunc"
  ],
  "private.bpay_next_value_source_for_target_v1(numeric,text,text,boolean,uuid,date)": [
    "private.pay_finance_selected_quote_v1"
  ],
  "private.pay_finance_selected_quote_v1(public.pay_finance_component_resolution_mode_enum,numeric,text,text,numeric,numeric,numeric,numeric,numeric,boolean)": [
    "public._pay_convert_paye_to_umbrella",
    "public._pay_convert_umbrella_to_paye_ex",
    "public._pay_umbrella_vat_calc"
  ],
  "private.bpay_next_channel_drafts_page_v1(uuid)": [
    "pg_catalog.jsonb_agg",
    "pg_catalog.jsonb_array_length",
    "pg_catalog.jsonb_build_object"
  ],
  "private.bpay_next_receive_command_v1(uuid,text)": [
    "pg_catalog.char_length"
  ],
  "private._candidate_expense_source_family_v1(uuid)": []
});
export const HISTORY_RELATIONS=Object.freeze(['bpay_next_financial_effect','bpay_next_case_event',
  'bpay_next_transfer_outcome','bpay_next_internal_receipt','bpay_next_return_cash']);

// PF09: actual NEXT table column types, not prose or private row-lock syntax.
export function assertNextExactNumericTypes(source,file){
  const floating=t=>['REAL','FLOAT','FLOAT4','FLOAT8'].includes(t[0])||(t[0]==='DOUBLE'&&t[1]==='PRECISION');
  const parts=[sqlWithoutComments(source),...executableParts(source,file).map(part=>part.text)];
  for(const part of parts)for(const row of scanManagedSql(part).statements){
    const start=row.tokens.findIndex(token=>token==='CREATE'||token==='ALTER');
    const t=start<0?[]:row.tokens.slice(start),table=t.indexOf('TABLE');
    if(!['CREATE','ALTER'].includes(t[0])||table<0)continue;
    let at=table+1;while(['IF','NOT','EXISTS','ONLY'].includes(t[at]))at++;
    const name=t[at+1]==='.'?t[at+2]:t[at];
    if(!name?.startsWith('BPAY_NEXT_'))continue;
    const type=(tokens)=>floating(tokens[0]==='PG_CATALOG'&&tokens[1]==='.'?tokens.slice(2):tokens);
    if(t[0]==='CREATE'){
      let depth=0,column=[];
      const check=()=>{if(type(column.slice(1)))throw Error(`BPAY_NEXT_FLOAT_COLUMN_FORBIDDEN: ${file}`);};
      for(const token of t.slice(at+(t[at+1]==='.'?3:1))){
        if(token==='('){depth++;if(depth===1)continue;}
        if(token===')'){if(depth===1){check();break;}depth--;}
        if(token===','&&depth===1){check();column=[];continue;}
        if(depth>=1)column.push(token);
      }
    }else for(let i=at+1;i<t.length;i++){
      if(t[i]==='ADD'){
        let col=i+1;while(['COLUMN','IF','NOT','EXISTS'].includes(t[col]))col++;
        if(type(t.slice(col+1)))throw Error(`BPAY_NEXT_FLOAT_COLUMN_FORBIDDEN: ${file}`);
      }else if(t[i]==='TYPE'&&type(t.slice(i+1)))throw Error(`BPAY_NEXT_FLOAT_COLUMN_FORBIDDEN: ${file}`);
    }
  }
}

export const TYPED_TRANSPORT_SIGNATURES=Object.freeze([
  "private.bpay_next_accept_preparation_discard_v1(uuid,uuid,uuid,bigint,text)",
  "private.bpay_next_discard_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer)",
  "private.bpay_next_discarded_prepare_receipt_v1(uuid)",
  "private.bpay_next_project_worker_release_page_v1(uuid,uuid,bigint,text,bigint,integer)",
  "private.bpay_next_preparation_progress_v1(uuid)",
  "private.bpay_next_preparation_wake_v1(uuid)",
  "private.bpay_next_workbench_kind_page_v1(text,jsonb,jsonb,integer)",
  "private.bpay_next_workbench_header_page_v1(jsonb,jsonb,integer)",
  "private.bpay_next_workbench_candidates_page_v1(jsonb,jsonb,integer)",
  "private.bpay_next_workbench_action_required_page_v1(jsonb,jsonb,integer)",
  "private.bpay_next_workbench_blocked_page_v1(jsonb,jsonb,integer)",
  "private.bpay_next_workbench_issue_detail_page_v1(jsonb,jsonb,integer)",
  "private.bpay_next_workbench_candidate_groups_page_v1(jsonb,jsonb,integer)",
  "private.bpay_next_workbench_group_lines_page_v1(jsonb,jsonb,integer)",
  "private.bpay_next_workbench_pick_candidates_page_v1(jsonb,jsonb,integer)",
  "private.bpay_next_workbench_pick_clients_page_v1(jsonb,jsonb,integer)",
  "private.bpay_next_workbench_export_rows_page_v1(jsonb,jsonb,integer)",
  "public.bpay_next_workbench_page_v1(jsonb)",
  "private.bpay_next_advice_work_v1(uuid)",
  "private.bpay_next_current_case_component_page_v1(uuid,uuid,bigint,integer)",
  "private.bpay_next_current_case_page_v1(uuid,uuid,integer)",
  "private.bpay_next_current_stored_credit_page_v1(uuid,uuid,integer)",
  "private.bpay_next_current_work_page_v1(uuid,date,uuid,integer)",
  "private.bpay_next_return_cash_page_v1(uuid,uuid,integer,integer)",
  "private.bpay_next_simple_break_page_v1(uuid,uuid,integer,integer)",
  "private.bpay_next_worker_status_page_v1(uuid,uuid,integer,integer)",
  "public.bpay_next_financial_intake_v1(uuid,text,jsonb)",
  "public.bpay_next_financial_job_v1(text,uuid,text,jsonb)",
  "public.bpay_next_frozen_detail_page_v1(uuid,text,uuid,uuid,uuid,integer,integer,text,text,text,integer)",
  "public.bpay_next_original_csv_advice_page_v1(uuid,uuid,text,uuid,uuid,uuid,uuid,bigint,integer,integer,text,text,text,integer)",
  "public.bpay_next_preparation_intake_v1(uuid,text,jsonb)",
  "public.bpay_next_preparation_page_v1(uuid,text,jsonb)",
  "private.bpay_next_channel_drafts_page_v1(uuid)",
  "private.bpay_next_channel_worker_page_v1(uuid,text,uuid,integer)",
  "private.bpay_next_channel_status_page_v1(uuid,text,uuid,integer,integer)",
  "public.bpay_next_payment_history_page_v1(jsonb)",
  "public.bpay_next_bind_stored_credit_v1(uuid,uuid,uuid)"
]);
const TYPED_PAGE_PROJECTIONS=Object.freeze({
  "private.bpay_next_current_case_component_page_v1(uuid,uuid,bigint,integer)": [
    "pg_catalog.to_jsonb(page) as row_value from ( select c.id as case_component_id,c.case_id,c.candidate_id,c.component_ordinal::text, c.component_revision::text,c.case_kind,c.case_subtype,c.tax_treatment, c.instruction_kind,c.direction,c.payroll_stage,c.source_pay_channel,c.resolution_state, c.approved_source_ex_vat::text,c.funded_source_ex_vat::text,c.recovered_source_ex_vat::text, c.written_off_source_ex_vat::text,c.active_payout_source_ex_vat::text,c.active_recovery_source_ex_vat::text, c.rule_id,fc.current_rule_id,r.rule_revision::text, fc.principal_approved::text as case_principal_approved, r.weekly_due_source_ex_vat::text,r.minimum_earnings_threshold::text,r.take_home_floor_override::text, r.schedule_start_monday,r.next_due_monday,r.schedule_week_count::text, (case when c.direction='DEDUCTION' then case when c.case_kind in ('LOAN','ADVANCE') then c.funded_source_ex_vat else c.approved_source_ex_vat end -c.recovered_source_ex_vat-c.written_off_source_ex_vat else c.approved_source_ex_vat-c.funded_source_ex_vat-c.paid_credit_source_ex_vat-c.written_off_source_ex_vat end)::text as outstanding_source_ex_vat, (p.opening_due_source_ex_vat-p.realised_recovery_source_ex_vat-p.active_unrealised_recovery_source_ex_vat)::text as amount_due, null::text as recoverable_this_run, private.bpay_next_case_edit_state_v1(c.case_id,c.candidate_id) as edit_state,null::jsonb as edit_receipt,v_pause_state as pause_state,null::jsonb as pause_receipt, (select coalesce(amended.input->>'original_liability_reference',cr.original_liability_reference) from private.bpay_next_case_create_request cr left join private.bpay_next_case_edit_request amended on amended.new_rule_id=fc.current_rule_id where cr.case_id=c.case_id and cr.candidate_id=c.candidate_id and cr.case_kind='MANUAL_DEBT' and cr.case_subtype='MANUAL_DEBT' and c.case_kind='MANUAL_DEBT' and c.case_subtype='MANUAL_DEBT') as original_liability_reference from",
    "pg_catalog.to_jsonb(page) as row_value from ( select c.id as case_component_id,c.case_id,c.candidate_id,c.component_ordinal::text, c.component_revision::text,c.case_kind,c.case_subtype,c.tax_treatment, c.instruction_kind,c.direction,c.payroll_stage,c.source_pay_channel,c.resolution_state, c.approved_source_ex_vat::text,c.funded_source_ex_vat::text,c.recovered_source_ex_vat::text, c.written_off_source_ex_vat::text,c.active_payout_source_ex_vat::text,c.active_recovery_source_ex_vat::text, c.rule_id,fc.current_rule_id,r.rule_revision::text, fc.principal_approved::text as case_principal_approved, r.weekly_due_source_ex_vat::text,r.minimum_earnings_threshold::text,r.take_home_floor_override::text, r.schedule_start_monday,r.next_due_monday,r.schedule_week_count::text, (case when c.direction='DEDUCTION' then case when c.case_kind in ('LOAN','ADVANCE') then c.funded_source_ex_vat else c.approved_source_ex_vat end -c.recovered_source_ex_vat-c.written_off_source_ex_vat else c.approved_source_ex_vat-c.funded_source_ex_vat-c.paid_credit_source_ex_vat-c.written_off_source_ex_vat end)::text as outstanding_source_ex_vat, (p.opening_due_source_ex_vat-p.realised_recovery_source_ex_vat-p.active_unrealised_recovery_source_ex_vat)::text as amount_due, null::text as recoverable_this_run, private.bpay_next_case_edit_state_v1(c.case_id,c.candidate_id) as edit_state,null::jsonb as edit_receipt, (select coalesce(amended.input->>'original_liability_reference',cr.original_liability_reference) from private.bpay_next_case_create_request cr left join private.bpay_next_case_edit_request amended on amended.new_rule_id=fc.current_rule_id where cr.case_id=c.case_id and cr.candidate_id=c.candidate_id and cr.case_kind='MANUAL_DEBT' and cr.case_subtype='MANUAL_DEBT' and c.case_kind='MANUAL_DEBT' and c.case_subtype='MANUAL_DEBT') as original_liability_reference from",
    "pg_catalog.to_jsonb(page) as row_value from ( select c.id as case_component_id,c.case_id,c.candidate_id,c.component_ordinal::text, c.component_revision::text,c.case_kind,c.case_subtype,c.tax_treatment, c.instruction_kind,c.direction,c.payroll_stage,c.source_pay_channel,c.resolution_state, c.approved_source_ex_vat::text,c.funded_source_ex_vat::text,c.recovered_source_ex_vat::text, c.written_off_source_ex_vat::text,c.active_payout_source_ex_vat::text,c.active_recovery_source_ex_vat::text from",
    "pg_catalog.to_jsonb(page) as row_value from ( select c.id as case_component_id,c.case_id,c.candidate_id,c.component_ordinal::text, c.component_revision::text,c.case_kind,c.case_subtype,c.tax_treatment, c.instruction_kind,c.direction,c.payroll_stage,c.source_pay_channel,c.resolution_state, c.approved_source_ex_vat::text,c.funded_source_ex_vat::text,c.recovered_source_ex_vat::text, c.written_off_source_ex_vat::text,c.active_payout_source_ex_vat::text,c.active_recovery_source_ex_vat::text, c.rule_id,fc.current_rule_id,r.rule_revision::text, fc.principal_approved::text as case_principal_approved, r.weekly_due_source_ex_vat::text,r.minimum_earnings_threshold::text,r.take_home_floor_override::text, r.schedule_start_monday,r.next_due_monday,r.schedule_week_count::text, (case when c.direction='DEDUCTION' then case when c.case_kind in ('LOAN','ADVANCE') then c.funded_source_ex_vat else c.approved_source_ex_vat end -c.recovered_source_ex_vat-c.written_off_source_ex_vat else c.approved_source_ex_vat-c.funded_source_ex_vat-c.paid_credit_source_ex_vat-c.written_off_source_ex_vat end)::text as outstanding_source_ex_vat, (p.opening_due_source_ex_vat-p.realised_recovery_source_ex_vat-p.active_unrealised_recovery_source_ex_vat)::text as amount_due, null::text as recoverable_this_run from",
    "pg_catalog.to_jsonb(page) as row_value from ( select c.id as case_component_id,c.case_id,c.candidate_id,c.component_ordinal::text, c.component_revision::text,c.case_kind,c.case_subtype,c.tax_treatment, c.instruction_kind,c.direction,c.payroll_stage,c.source_pay_channel,c.resolution_state, c.approved_source_ex_vat::text,c.funded_source_ex_vat::text,c.recovered_source_ex_vat::text, c.written_off_source_ex_vat::text,c.active_payout_source_ex_vat::text,c.active_recovery_source_ex_vat::text, c.rule_id,fc.current_rule_id,r.rule_revision::text, fc.principal_approved::text as case_principal_approved, r.weekly_due_source_ex_vat::text,r.minimum_earnings_threshold::text,r.take_home_floor_override::text, r.schedule_start_monday,r.next_due_monday,r.schedule_week_count::text, (case when c.direction='DEDUCTION' then case when c.case_kind in ('LOAN','ADVANCE') then c.funded_source_ex_vat else c.approved_source_ex_vat end -c.recovered_source_ex_vat-c.written_off_source_ex_vat else c.approved_source_ex_vat-c.funded_source_ex_vat-c.paid_credit_source_ex_vat-c.written_off_source_ex_vat end)::text as outstanding_source_ex_vat, (p.opening_due_source_ex_vat-p.realised_recovery_source_ex_vat-p.active_unrealised_recovery_source_ex_vat)::text as amount_due, null::text as recoverable_this_run, (select cr.original_liability_reference from private.bpay_next_case_create_request cr where cr.case_id=c.case_id and cr.candidate_id=c.candidate_id and cr.case_kind='MANUAL_DEBT' and cr.case_subtype='MANUAL_DEBT' and c.case_kind='MANUAL_DEBT' and c.case_subtype='MANUAL_DEBT') as original_liability_reference from"
  ],
  "private.bpay_next_current_case_page_v1(uuid,uuid,integer)": [
    "pg_catalog.to_jsonb(page) as row_value from ( select c.id as case_id,c.candidate_id,c.case_kind,c.tax_treatment,c.status, c.principal_approved::text,c.principal_funded::text,c.principal_recovered::text, c.principal_written_off::text,c.active_hold_amount::text from"
  ],
  "private.bpay_next_current_stored_credit_page_v1(uuid,uuid,integer)": [
    "pg_catalog.to_jsonb(page) into v_value from ( select a.id as legacy_case_id,a.candidate_id,c.source_amount::text as principal_source_ex_vat, c.source_pay_method as original_source_pay_channel, a.taxability::text as original_tax_treatment,a.routing_kind::text as original_routing_kind, a.created_at as original_created_at_utc,b.updated_at_utc as bank_version_at_utc, b.bank_details_hash,b.beneficiary_name,pg_catalog.right(b.account_number,4) as account_last4 from"
  ],
  "private.bpay_next_current_work_page_v1(uuid,date,uuid,integer)": [
    "pg_catalog.to_jsonb(page) as row_value from ( select w.id as work_id,w.candidate_id,w.original_timesheet_id,w.work_kind, w.week_ending_date,w.approval_state,w.current_revision_id,w.applied_revision_id, coalesce(w.approval_state='APPROVED' and w.current_revision_id=w.applied_revision_id and r.sealed_at_utc is not null,false) as position_ready, r.source_pay_channel,r.timesheet_reference,r.client_display_name, r.approved_source_ex_vat::text from"
  ],
  "private.bpay_next_worker_status_page_v1(uuid,uuid,integer,integer)": [
    "pg_catalog.to_jsonb(page) as row_value from ( select t.id as transfer_id,t.transfer_no::text,t.projection_id,t.execution_kind, t.status,t.cash_amount::text,t.original_transfer_id, rv.reservation_state,rv.remaining_reservation_cash,rv.reservation_holds_state from"
  ],
  "public.bpay_next_frozen_detail_page_v1(uuid,text,uuid,uuid,uuid,integer,integer,text,text,text,integer)": [
    "to_jsonb(page) row_value from (select rate_schedule_id,rate_family,rate_code,unit_label, paye_rate::text,umbrella_rate::text,charge_rate::text from",
    "to_jsonb(page) row_value from (select shift_detail_id,detail_no,work_date,shift_start_at,shift_end_at, shift_start_local,shift_end_local,shift_overnight,submitted_minutes, approved_minutes,approved_hours::text,detail_label, segment_pay_ex_vat::text,pay_excluded, hours_day::text,hours_night::text,hours_sat::text,hours_sun::text,hours_bh::text from",
    "to_jsonb(page) row_value from private.bpay_next_simple_break_page_v1($1,$2,$3,$7) page",
    "to_jsonb(page) row_value from (select rate_detail_id,bucket,approved_hours::text,source_pay_rate::text from"
  ],
  "public.bpay_next_original_csv_advice_page_v1(uuid,uuid,text,uuid,uuid,uuid,uuid,bigint,integer,integer,text,text,text,integer)": [
    "to_jsonb(page) row_value from ( select s.id shift_detail_id,s.detail_no,s.work_date,s.shift_start_at,s.shift_end_at,s.shift_start_local, s.shift_end_local,s.shift_overnight,s.submitted_minutes,s.approved_minutes,s.approved_hours::text,s.detail_label, s.segment_pay_ex_vat::text,s.pay_excluded,s.hours_day::text,s.hours_night::text,s.hours_sat::text,s.hours_sun::text,s.hours_bh::text from",
    "to_jsonb(page) row_value from ( select b.id break_detail_id,b.break_no,b.break_start_at,b.break_end_at,b.break_start_local,b.break_end_local,b.break_minutes from",
    "to_jsonb(page) row_value from ( select d.id rate_detail_id,d.bucket,d.approved_hours::text,d.source_pay_rate::text from",
    "to_jsonb(page) row_value from ( select s.id rate_schedule_id,s.rate_family,s.rate_code,s.unit_label,s.paye_rate::text,s.umbrella_rate::text,s.charge_rate::text from"
  ],
  "private.bpay_next_channel_worker_page_v1(uuid,text,uuid,integer)": [
    "pg_catalog.to_jsonb(page) as row_value from ( select w.id as run_worker_id,w.candidate_id,w.target_pay_channel as pay_channel,w.status as worker_status, w.gross_ex_vat::text as frozen_ex_vat,w.gross_vat::text as frozen_vat, w.gross_inc_vat::text as frozen_inc_vat,w.review_issue_code from"
  ]
});
const TYPED_BREAK_RETURN="returns table ( break_detail_id uuid,break_no integer,break_start_at timestamptz, break_end_at timestamptz,break_start_local text,break_end_local text, break_minutes integer )";
const TYPED_BREAK_QUERY="select b.id,b.break_no,b.break_start_at,b.break_end_at, b.break_start_local,b.break_end_local,b.break_minutes from";
// Exact named JSON constructors: per-routine clauses, never a whole-file allowance.
const TYPED_OBJECT_CONSTRUCTORS=Object.freeze({
  "private.bpay_next_preparation_progress_v1(uuid)": ["pg_catalog.jsonb_build_object('needs_attention',exists(select 1 from private.bpay_next_job j join private.bpay_next_run_worker w on w.run_id=v_run.id and w.candidate_id=j.candidate_id and w.active_prepare_job_id=j.id where j.retry_run_id=v_run.id and j.job_kind='PREPARE' and j.needs_attention), 'run_id',v_run.id,'status',v_run.status, 'actor_display_name',v_actor,'started_at_utc',v_run.created_at_utc,'expires_at_utc',v_run.preparation_expires_at_utc, 'selected_candidates',v_run.selected_candidate_count::text,'ready',v_ready::text,'review',v_review::text, 'pending',v_pending::text,'preparing',v_preparing::text,'removed',v_removed::text, 'enrolled_candidates',v_enrolled::text,'enrolment_complete',v_enrolment_complete, 'enrolment_failed',v_enrolment_failed,'safe_to_close',true, 'review_revision',v_run.review_revision::text,'discard_phase',v_discard.status, 'discard_actor_display_name',v_discard_actor,'discarded_at_utc',v_discard.finished_at_utc, 'discard_removed_candidates',coalesce(v_discard.removed_candidate_count,0)::text, 'discard_already_removed_candidates',coalesce(v_discard.already_removed_candidate_count,0)::text, 'discard_blocked_candidates',coalesce(v_discard.blocked_candidate_count,0)::text, 'expired_at_utc',case when v_run.status='CANCELLED' and v_run.confirmed_at_utc is null and v_expiry.status='EXPIRED' and v_expiry.completed_candidate_count=v_expiry.expected_candidate_count and v_expiry.enrolled_candidate_count=v_expiry.expected_candidate_count then v_expiry.finished_at_utc else null end)"],
  "private.bpay_next_preparation_wake_v1(uuid)": ["pg_catalog.jsonb_build_object('run_id',p_run_id,'accepted',false,'throttled',true)", "pg_catalog.jsonb_build_object('run_id',p_run_id,'accepted',true,'throttled',false)"],

  "private.bpay_next_advice_work_v1(uuid)": [
    "pg_catalog.jsonb_build_object( 'run_work_id',rw.id,'work_id',rw.work_id,'captured_revision_id',rw.captured_revision_id, 'physical_timesheet_id',r.physical_timesheet_id,'physical_timesheet_version',r.physical_timesheet_version, 'source_kind',r.source_kind,'detail_kind',r.detail_kind,'week_ending_date',r.week_ending_date, 'candidate_display_name',r.candidate_display_name,'candidate_reference',r.candidate_reference, 'client_display_name',r.client_display_name,'job_title',r.job_title,'band_label',r.band_label, 'timesheet_reference',r.timesheet_reference,'expected_rate_schedule_count',r.expected_rate_schedule_count)"
  ],
  "private.bpay_next_current_case_component_page_v1(uuid,uuid,bigint,integer)": [
    "pg_catalog.jsonb_build_object('rows',v_rows,'complete',not(v_more or v_count=p_limit))"
  ],
  "private.bpay_next_current_case_page_v1(uuid,uuid,integer)": [
    "pg_catalog.jsonb_build_object('rows',v_rows,'complete',not(v_more or v_count=p_limit))"
  ],
  "private.bpay_next_current_stored_credit_page_v1(uuid,uuid,integer)": [
    "pg_catalog.jsonb_build_object('rows',v_rows,'complete',true,'scan_cursor',null)",
    "pg_catalog.jsonb_build_object('rows',v_rows,'complete',not v_more,'scan_cursor',v_scan_cursor)"
  ],
  "private.bpay_next_current_work_page_v1(uuid,date,uuid,integer)": [
    "pg_catalog.jsonb_build_object('rows',v_rows,'complete',not(v_more or v_count=p_limit))"
  ],
  "private.bpay_next_return_cash_page_v1(uuid,uuid,integer,integer)": [
    "pg_catalog.jsonb_build_object('run_id',r.id,'run_worker_id',w.id, 'candidate_id',w.candidate_id,'financial_view_revision',c.financial_view_revision::text)",
    "pg_catalog.jsonb_build_object( 'original_transfer_id',v_row.id,'original_transfer_no',v_row.transfer_no::text, 'original_transfer_status',v_row.status,'original_cash_amount',v_row.cash_amount::text, 'beneficiary_kind',v_row.beneficiary_kind,'beneficiary_id',v_row.beneficiary_id, 'return_command_id',v_return.command_id,'return_posting_complete',v_return.posting_complete, 'return_cash_id',v_cash.id,'amount_owed',v_cash.amount_owed::text,'amount_held',v_cash.amount_held::text, 'amount_reissued_paid',v_cash.amount_reissued_paid::text,'amount_available',v_available::text, 'pending_reissue_command_id',v_pending.command_id,'action_state',v_action)",
    "pg_catalog.jsonb_build_object('header',v_header,'rows',v_rows, 'complete',not(v_more or v_count=p_limit),'scan_cursor',v_scan::text)"
  ],
  "private.bpay_next_simple_break_page_v1(uuid,uuid,integer,integer)": [],
  "private.bpay_next_worker_status_page_v1(uuid,uuid,integer,integer)": [
    "pg_catalog.jsonb_build_object( 'run_id',r.id,'run_status',r.status,'selection_state',r.selection_state, 'review_revision',r.review_revision::text,'run_worker_id',w.id, 'candidate_id',w.candidate_id,'worker_status',w.status, 'target_pay_channel',w.target_pay_channel,'umbrella_id',w.target_umbrella_id, 'preparation_revision',w.preparation_revision::text, 'case_selection_revision',w.case_selection_revision::text, 'net_request_revision',w.net_request_revision::text, 'net_projection_revision',w.net_projection_revision::text, 'net_pending',w.net_request_revision<>w.net_projection_revision, 'review_issue_code',w.review_issue_code,'review_issue_work_id',w.review_issue_work_id, 'frozen_gross_ex_vat',w.gross_ex_vat::text,'frozen_gross_vat',w.gross_vat::text, 'frozen_gross_inc_vat',w.gross_inc_vat::text, 'projection_id',p.id,'projection_input_kind',p.input_kind, 'entered_paye_net',p.entered_paye_net::text, 'accepted_recoveries',p.accepted_recoveries::text, 'accepted_net_additions',p.accepted_net_additions::text, 'cash_amount',p.cash_amount::text, 'reservation_mode',rv.reservation_mode,'reservation_projection_id',rv.reservation_projection_id, 'remaining_reservation_cash',rv.remaining_reservation_cash::text,'reservation_internal_zero',rv.reservation_internal_zero, 'reservation_holds_state',rv.reservation_holds_state )",
    "pg_catalog.jsonb_build_object('header',v_header,'rows',v_rows,'complete',not(v_more or v_count=p_limit))"
  ],
  "public.bpay_next_financial_intake_v1(uuid,text,jsonb)": [
    "pg_catalog.jsonb_build_object('case_id',v_result->'case_id','command_id',v_result->'command_id', 'sequence',v_result->'sequence','phase',v_result->'phase','replay',v_result->'replay')",
    "jsonb_build_object('action',p_action,'result', private.bpay_next_retry_work_job_v1(p_actor_user_id,p_args,false))",
    "pg_catalog.jsonb_build_object('command_id',v_result->>'command_id', 'case_id',v_result->>'case_id','case_component_id',v_result->>'case_component_id', 'sequence',v_result->>'sequence','phase',v_result->>'phase','requested_scope',v_result->>'requested_scope', 'applied_amount',v_result->>'applied_amount','remaining_amount',v_result->>'remaining_amount', 'protected_amount',v_result->>'protected_amount','event_id',v_result->>'event_id', 'issue_code',v_result->>'issue_code','replay',v_result->'replay')",
    "pg_catalog.jsonb_build_object( 'command_id',v_result->>'command_id','run_id',v_result->>'run_id', 'sequence',v_result->>'sequence','phase',v_result->>'phase', 'expected_candidates',v_result->>'expected_candidates', 'completed_candidates',v_result->>'completed_candidates', 'replay',v_result->'replay')",
    "pg_catalog.jsonb_build_object( 'command_id',v_command_id,'run_worker_id',v_worker_id, 'sequence',v_result->>'sequence','request_no',v_result->>'request_no', 'phase',v_result->>'phase','replay',v_result->'replay')",
    "pg_catalog.jsonb_build_object( 'command_id',v_command_id,'run_worker_id',v_worker_id, 'sequence',v_result->>'sequence','request_no',v_result->>'request_no', 'phase',v_result->>'phase','replay',v_result->'replay')",
    "pg_catalog.jsonb_build_object( 'command_id',v_command_id,'run_worker_id',v_worker_id, 'sequence',v_result->>'sequence','transfer_id',v_result->>'transfer_id', 'phase',v_result->>'phase','replay',v_result->'replay')",
    "pg_catalog.jsonb_build_object( 'command_id',v_command_id,'run_worker_id',v_worker_id, 'sequence',v_result->>'sequence','phase',v_result->>'phase', 'cursor',v_result->>'cursor','released_line_count',v_result->>'released_line_count', 'blocked_code',v_result->>'blocked_code','replay',v_result->'replay')",
    "pg_catalog.jsonb_build_object( 'command_id',v_command_id,'return_cash_id',v_return_cash_id, 'sequence',v_sequence,'transfer_id',v_result->>'transfer_id', 'phase',case when v_result->>'transfer_id' is null then 'ACCEPTED_PENDING_BUILD' else 'TRANSFER_CREATED' end, 'replay',v_result->'replay')",
    "pg_catalog.jsonb_build_object('command_id',v_command_id,'transfer_id',v_transfer_id, 'sequence',v_result->>'sequence','phase',v_result->>'phase','blocked_code',v_key,'replay',v_result->'replay')",
    "pg_catalog.jsonb_build_object( 'instruction_id',v_result->>'instruction_id','transfer_id',v_result->>'transfer_id', 'file_name',v_result->>'file_name','csv_sha256',v_result->>'csv_sha256', 'csv_text',v_result->>'csv_text','phase','ISSUED_CSV', 'payment_recorded',v_result->'payment_recorded','replay',v_result->'replay')",
    "pg_catalog.jsonb_build_object( 'command_id',v_result->>'command_id','transfer_id',v_transfer_id, 'outcome_id',v_result->>'outcome_id','sequence',v_sequence, 'posting_complete',v_result->'posting_complete','replay',v_result->'replay', 'phase',case when (v_result->>'posting_complete')::boolean then 'POSTED' else 'ACCEPTED_PENDING_POSTING' end)",
    "pg_catalog.jsonb_build_object( 'command_id',v_result->>'command_id','transfer_id',v_result->>'transfer_id', 'internal_receipt_id',v_result->>'internal_receipt_id','sequence',v_result->>'sequence', 'posting_complete',v_result->'posting_complete','phase',v_result->>'phase','replay',v_result->'replay')",
    "pg_catalog.jsonb_build_object( 'command_id',v_result->>'command_id','transfer_id',v_transfer_id, 'outcome_id',v_result->>'outcome_id','sequence',v_sequence, 'posting_complete',v_result->'posting_complete','replay',v_result->'replay', 'phase',case when (v_result->>'posting_complete')::boolean then 'POSTED' else 'ACCEPTED_PENDING_POSTING' end)"
  ],
  "public.bpay_next_financial_job_v1(text,uuid,text,jsonb)": [
    "pg_catalog.jsonb_build_object( 'job_id',p_job_id,'job_kind',p_job_kind,'phase',v_job.phase, 'lease_nonce',v_claim.lease_nonce,'owner_epoch',v_claim.owner_epoch::text, 'lease_until_utc',v_lease_until,'cursor',v_job.cursor_key)",
    "pg_catalog.jsonb_build_object( 'cursor',case when v_job.phase in ('NEW','PINNING','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL') then v_job.cursor_key else null end, 'work_cursor',case when v_job.phase in ('RESERVE','ALLOCATE') then v_job.position_work_cursor else null end, 'component_cursor',case when v_job.phase in ('RESERVE','ALLOCATE') then v_job.position_component_cursor else null end)",
    "pg_catalog.jsonb_build_object('phase',v_page.next_phase, 'cursor',v_page.next_cursor,'rows_visited',v_page.rows_visited::text,'done',v_page.done)",
    "pg_catalog.jsonb_build_object('phase','CANCELLED','cursor',v_job.cursor_key,'rows_visited','0','replay',true)",
    "pg_catalog.jsonb_build_object('rows_visited','0')",
    "pg_catalog.jsonb_build_object('rows_visited',case when v_result->>'rows_visited' is not null then v_result->>'rows_visited' when v_result->>'replay'='true' then '0' when v_result->>'work_id' is not null then '1' else '0' end)",
    "pg_catalog.jsonb_build_object( 'job_id',p_job_id,'job_kind',p_job_kind,'owner_epoch',v_job.owner_epoch::text, 'lease_until_utc',v_lease_until,'done',v_job.status='DONE')",
    "pg_catalog.jsonb_build_object('replay',v_result->'replay')",
    "pg_catalog.jsonb_build_object('phase',v_result->>'phase', 'cursor',v_result->>'cursor','rows_visited',v_result->>'rows_visited')",
    "pg_catalog.jsonb_build_object('phase',case when v_discarded_prepare then 'DISCARDED' else 'EXPIRED' end, 'cursor',v_result->>'cursor','work_cursor',v_result->>'work_cursor', 'component_cursor',v_result->>'component_cursor', 'run_worker_id',v_result->>'run_worker_id','worker_status',v_result->>'worker_status', 'issue_code',v_result->>'issue_code','issue_work_id',v_result->>'issue_work_id', 'rows_visited',v_result->>'rows_visited')",
    "pg_catalog.jsonb_build_object('phase',v_result->>'phase', 'cursor',case when v_job.phase in ('NEW','PINNING','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL') then v_job.cursor_key else null end, 'work_cursor',case when v_job.phase in ('RESERVE','ALLOCATE') then v_job.position_work_cursor else null end, 'component_cursor',case when v_job.phase in ('RESERVE','ALLOCATE') then v_job.position_component_cursor else null end, 'run_worker_id',v_worker.id,'worker_status',v_worker.status, 'issue_code',v_worker.review_issue_code,'issue_work_id',v_worker.review_issue_work_id, 'rows_visited',v_result->>'rows_visited')",
    "pg_catalog.jsonb_build_object( 'phase',v_result->>'phase','projection_id',v_result->>'projection_id')",
    "pg_catalog.jsonb_build_object( 'cursor',v_result->>'cursor','rows_visited',v_result->>'rows_visited', 'run_worker_id',v_result->>'run_worker_id','issue_code',v_result->>'issue_code')",
    "pg_catalog.jsonb_build_object( 'phase',v_result->>'phase','transfer_id',v_result->>'transfer_id', 'transfer_status',coalesce(v_result->>'transfer_status',v_result->>'phase'), 'cursor',v_job.cursor_key,'member_count',v_result->>'member_count', 'rows_visited',coalesce(v_result->>'rows_visited','0'))",
    "pg_catalog.jsonb_build_object( 'phase',case when v_job.status='DONE' then 'COMPLETE' else 'MEMBERS' end, 'cursor',v_job.cursor_key,'posted_members',v_result->>'posted_members', 'rows_visited',coalesce(v_result->>'rows_visited','0'))",
    "pg_catalog.jsonb_build_object( 'phase',case when v_job.status='DONE' then 'COMPLETE' else 'MEMBERS' end, 'cursor',v_job.cursor_key,'posted_members',v_result->>'posted_members', 'rows_visited',coalesce(v_result->>'rows_visited','0'))",
    "pg_catalog.jsonb_build_object( 'phase','COMPLETE','return_cash_id',v_result->>'return_cash_id')",
    "pg_catalog.jsonb_build_object( 'phase','COMPLETE','transfer_id',v_result->>'transfer_id')",
    "pg_catalog.jsonb_build_object('phase',v_result->>'phase', 'transfer_id',v_result->>'transfer_id','blocked_code',v_result->>'blocked_code')",
    "pg_catalog.jsonb_build_object( 'phase',v_result->>'phase','run_worker_id',v_result->>'run_worker_id', 'cursor',v_job.cursor_key,'released_line_count',v_result->>'released_line_count', 'rows_visited',coalesce(v_result->>'rows_visited','0'), 'holds_released',coalesce(v_result->>'holds_released','0'), 'blocked_code',v_result->>'blocked_code')",
    "pg_catalog.jsonb_build_object('phase',v_result->>'phase', 'cursor',v_result->>'cursor','rows_visited',v_result->>'rows_visited', 'work_released',v_result->>'work_released','case_holds_released',v_result->>'case_holds_released', 'run_worker_id',v_result->>'run_worker_id')",
    "pg_catalog.jsonb_build_object('phase',v_result->>'phase', 'case_id',v_result->>'case_id')",
    "jsonb_build_object('phase',v_result->>'phase','case_id',v_result->>'case_id', 'case_revision',v_result->>'case_revision','rule_id',v_result->>'rule_id','issue_code',v_result->>'issue_code')",
    "pg_catalog.jsonb_build_object('phase',v_result->>'phase', 'case_id',v_result->>'case_id','case_component_id',v_result->>'case_component_id', 'applied_amount',v_result->>'applied_amount','remaining_amount',v_result->>'remaining_amount', 'protected_amount',v_result->>'protected_amount','event_id',v_result->>'event_id','issue_code',v_result->>'issue_code')"
  ],
  "public.bpay_next_frozen_detail_page_v1(uuid,text,uuid,uuid,uuid,integer,integer,text,text,text,integer)": [
    "jsonb_build_object('rows',v_rows,'complete',not(v_more or v_count=p_limit),'origin',v_origin)"
  ],
  "public.bpay_next_original_csv_advice_page_v1(uuid,uuid,text,uuid,uuid,uuid,uuid,bigint,integer,integer,text,text,text,integer)": [
    "jsonb_build_object( 'instruction_id',v_instruction.id,'transfer_id',v_transfer.id,'projection_id',v_projection.id, 'projection_no',v_projection.projection_no::text,'input_kind',v_projection.input_kind, 'run_id',v_run.id,'run_worker_id',v_worker.id,'candidate_id',v_worker.candidate_id, 'pay_date',v_run.pay_date,'target_pay_channel',v_worker.target_pay_channel, 'beneficiary_name_snapshot',v_transfer.beneficiary_name_snapshot, 'payment_reference',v_instruction.payment_reference,'issued_at_utc',v_instruction.issued_at_utc, 'member_count',v_transfer.member_count::text, 'gross_ex_vat',v_projection.gross_ex_vat::text,'gross_vat',v_projection.gross_vat::text, 'gross_inc_vat',v_projection.gross_inc_vat::text,'entered_paye_net',v_projection.entered_paye_net::text, 'accepted_gross_additions',v_projection.accepted_gross_additions::text, 'accepted_gross_deductions',v_projection.accepted_gross_deductions::text, 'accepted_net_additions',v_projection.accepted_net_additions::text, 'accepted_recoveries',v_projection.accepted_recoveries::text,'cash_amount',v_instruction.cash_amount::text, 'projection_cash_amount',v_projection.cash_amount::text,'destination_kind',v_destination_kind)",
    "jsonb_build_object('rows',v_rows,'complete',true)",
    "jsonb_build_object( 'member_no',m.member_no::text,'subject_kind',m.subject_kind,'signed_cash_contribution',m.signed_cash_contribution::text, 'work',case when m.subject_kind='WORK' then private.bpay_next_advice_work_v1(l.run_work_id) end, 'line',case when m.subject_kind='WORK' then jsonb_build_object( 'run_line_id',l.id,'run_work_id',l.run_work_id,'captured_revision_id',l.captured_revision_id, 'line_no',l.line_no::text,'component_key',l.component_key,'component_kind',a.component_kind, 'work_date',a.work_date,'unit_label',a.unit_label,'approved_quantity',a.approved_quantity::text, 'approved_unit_rate',a.approved_unit_rate::text,'expected_rate_detail_count',a.expected_rate_detail_count, 'evidence_ref',a.evidence_ref,'approved_source_ex_vat',a.source_pay_ex_vat::text, 'source_consumed_ex_vat',l.source_consumed_ex_vat::text,'frozen_ex_vat',l.frozen_ex_vat::text, 'frozen_vat',l.frozen_vat::text,'frozen_inc_vat',l.frozen_inc_vat::text) end, 'case',case when m.subject_kind='CASE' then jsonb_build_object( 'case_instruction_id',ci.id,'allocation_result_id',cr.id,'case_id',ci.case_id, 'case_kind',ci.case_kind,'case_subtype',ci.case_subtype,'tax_treatment',ci.tax_treatment, 'instruction_kind',ci.instruction_kind,'direction',ci.direction,'payroll_stage',ci.payroll_stage, 'resolution_ref',ci.resolution_ref,'explanation',ci.explanation, 'allocated_target_ex_vat',cr.allocated_target_ex_vat::text,'allocated_target_vat',cr.allocated_target_vat::text, 'allocated_target_inc_vat',cr.allocated_target_inc_vat::text) end)",
    "jsonb_build_object( 'run_line_id',l.id,'run_work_id',l.run_work_id,'captured_revision_id',l.captured_revision_id, 'line_no',l.line_no::text,'component_key',l.component_key,'component_kind',a.component_kind, 'work_date',a.work_date,'unit_label',a.unit_label,'approved_quantity',a.approved_quantity::text, 'approved_unit_rate',a.approved_unit_rate::text,'expected_rate_detail_count',a.expected_rate_detail_count, 'evidence_ref',a.evidence_ref,'approved_source_ex_vat',a.source_pay_ex_vat::text, 'source_consumed_ex_vat',l.source_consumed_ex_vat::text,'frozen_ex_vat',l.frozen_ex_vat::text, 'frozen_vat',l.frozen_vat::text,'frozen_inc_vat',l.frozen_inc_vat::text)",
    "jsonb_build_object( 'case_instruction_id',ci.id,'allocation_result_id',cr.id,'case_id',ci.case_id, 'case_kind',ci.case_kind,'case_subtype',ci.case_subtype,'tax_treatment',ci.tax_treatment, 'instruction_kind',ci.instruction_kind,'direction',ci.direction,'payroll_stage',ci.payroll_stage, 'resolution_ref',ci.resolution_ref,'explanation',ci.explanation, 'allocated_target_ex_vat',cr.allocated_target_ex_vat::text,'allocated_target_vat',cr.allocated_target_vat::text, 'allocated_target_inc_vat',cr.allocated_target_inc_vat::text)",
    "jsonb_build_object('rows',v_rows,'complete',not(v_more or v_count=p_limit))"
  ],
  "public.bpay_next_preparation_intake_v1(uuid,text,jsonb)": [
    "pg_catalog.jsonb_build_object('run_id',v_run,'status',v_result->>'status', 'selection_state',v_result->>'selection_state','selection_count',v_header.selection_count::text, 'selected_candidate_count',v_header.selected_candidate_count::text,'official_pay_date',v_header.pay_date::text, 'command_id',v_result->>'command_id','agency_sequence',v_result->'agency_sequence', 'choice_work_count',v_result->>'choice_work_count','choice_case_count',v_result->>'choice_case_count', 'empty',v_result->'empty','replay',v_result->'replay')",
    "pg_catalog.jsonb_build_object('decision',v_header.paye_guard_decision,'channel_scope',v_header.channel_scope)",
    "pg_catalog.jsonb_build_object('run_id',v_run,'candidate_id',v_candidate, 'selection_revision',p_args->>'selection_revision','page_no',v_result->>'page_no', 'item_count',v_result->>'item_count','replay',v_result->'replay')",
    "pg_catalog.jsonb_build_object('run_id',v_run,'candidate_id',v_candidate, 'selection_revision',v_result->>'selection_revision','sealed',v_result->'sealed', 'selected_count',v_result->>'selected_count','excluded_count',v_result->>'excluded_count','replay',v_result->'replay')",
    "pg_catalog.jsonb_build_object('run_id',v_run,'command_id',v_result->>'command_id', 'agency_sequence',v_result->>'agency_sequence','selection_count',v_seal->>'selection_count','replay',v_result->'replay')",
    "pg_catalog.jsonb_build_object('run_id',v_run,'phase',v_result->>'phase','reason',v_result->>'reason', 'review_revision',v_header.review_revision::text,'all_ready',v_result->'all_ready', 'replay',coalesce(v_result->'replay','false'::jsonb))",
    "pg_catalog.jsonb_build_object('run_id',v_run,'phase',v_result->>'phase', 'review_revision',v_result->>'review_revision','replay',v_result->'replay','drafts',v_result->'drafts')"
  ],
  "public.bpay_next_preparation_page_v1(uuid,text,jsonb)": [
    "jsonb_build_object('rows',jsonb_build_array(private.bpay_next_case_edit_preview_v1(p_actor_user_id, (p_args->>'case_id')::uuid,(p_args->>'expected_case_revision')::bigint,p_args->'input')),'complete',true)",
    "pg_catalog.jsonb_build_object('rows',pg_catalog.jsonb_build_array(private.bpay_next_case_create_preview_v1(p_actor_user_id,p_args->'input')),'complete',true)",
    "pg_catalog.jsonb_build_object('rows',pg_catalog.jsonb_build_array(private.bpay_next_paye_guard_page_v1(p_actor_user_id,(p_args->>'run_id')::uuid,p_args->'descriptor')),'complete',true)",
    "pg_catalog.jsonb_build_object('rows',pg_catalog.jsonb_build_array(v_progress),'complete',true)",
    "jsonb_build_object('edit_receipt',v_row)",
    "jsonb_build_object('pause_receipt',v_row)",
    "pg_catalog.jsonb_build_object( 'approved_line_id',l.id,'line_no',l.line_no::text,'component_key',l.component_key,'component_kind',l.component_kind, 'work_date',l.work_date,'source_pay_channel',r.source_pay_channel,'approved_source_ex_vat',l.source_pay_ex_vat::text, 'unit_label',l.unit_label,'approved_quantity',l.approved_quantity::text, 'approved_unit_rate',l.approved_unit_rate::text, 'job_title',r.job_title,'band_label',r.band_label, 'original_work_ref',parent.reference_number,'original_timesheet_id',parent.timesheet_id, 'is_adjustment',ts.is_adjustment,'correction_kind',ts.correction_kind, 'associated_rate_schedule_id',rs.id,'associated_rate_family',rs.rate_family,'associated_rate_code',rs.rate_code, 'available_paye_rate',rs.paye_rate::text,'available_umbrella_rate',rs.umbrella_rate::text, 'available_charge_rate',rs.charge_rate::text,'approved_charge_rate',rs.approved_charge_rate::text, 'standard_rate_schedule',case when l.component_kind in ('WORK','PROTECTED_WORK') then (select coalesce(jsonb_agg(snapshot.value order by snapshot.bucket_order),'[]'::jsonb) from ( select jsonb_build_object('rate_schedule_id',sr.id,'bucket',sr.rate_code, 'available_paye_rate',sr.paye_rate::text,'available_umbrella_rate',sr.umbrella_rate::text, 'available_charge_rate',sr.charge_rate::text,'approved_charge_rate',sr.approved_charge_rate::text) value, case sr.rate_code when 'DAY' then 1 when 'NIGHT' then 2 when 'SAT' then 3 when 'SUN' then 4 when 'BH' then 5 end bucket_order from private.bpay_next_rate_schedule sr where sr.revision_id=v_revision and sr.rate_family='STANDARD' and sr.rate_code in ('DAY','NIGHT','SAT','SUN','BH') order by bucket_order limit 5 ) snapshot) else '[]'::jsonb end, 'realised_source_ex_vat',p.realised_source_ex_vat::text,'held_source_ex_vat',p.held_source_ex_vat::text, 'residual_source_ex_vat',(l.source_pay_ex_vat-p.realised_source_ex_vat-p.held_source_ex_vat)::text, 'position_ready',coalesce(p.applied_revision_id=v_revision and p.approved_source_ex_vat=l.source_pay_ex_vat,false))",
    "jsonb_build_object('rate_schedule_id',sr.id,'bucket',sr.rate_code, 'available_paye_rate',sr.paye_rate::text,'available_umbrella_rate',sr.umbrella_rate::text, 'available_charge_rate',sr.charge_rate::text,'approved_charge_rate',sr.approved_charge_rate::text)",
    "pg_catalog.jsonb_build_object('work_id',c.work_id,'expected_revision_id',c.expected_revision_id, 'selection_mode',c.selection_mode,'selection_state',c.selection_state,'page_count',c.page_count::text,'component_count',c.component_count::text)",
    "pg_catalog.jsonb_build_object('selection_no',c.selection_no::text, 'approved_line_id',c.approved_line_id,'expected_revision_id',c.expected_revision_id,'component_key',c.component_key)",
    "pg_catalog.jsonb_build_object('run_worker_id',w.run_worker_id,'candidate_id',w.candidate_id, 'worker_status',w.worker_status,'frozen_ex_vat',w.frozen_ex_vat::text,'frozen_vat',w.frozen_vat::text, 'frozen_inc_vat',w.frozen_inc_vat::text,'review_issue_code',w.review_issue_code, 'discard_outcome',x.outcome,'discard_blocked_code',x.blocked_code)",
    "pg_catalog.jsonb_build_object('run_work_id',w.run_work_id,'work_id',w.work_id,'original_timesheet_id',w.original_timesheet_id, 'captured_revision_id',w.captured_revision_id,'current_revision_id',w.current_revision_id,'newer_approval_available',w.newer_approval_available)",
    "pg_catalog.jsonb_build_object('run_line_id',l.run_line_id,'line_no',l.line_no::text, 'original_timesheet_id',l.original_timesheet_id,'captured_revision_id',l.captured_revision_id,'component_key',l.component_key, 'component_kind',l.component_kind,'work_date',l.work_date,'source_pay_channel',l.source_pay_channel,'target_pay_channel',l.target_pay_channel, 'source_consumed_ex_vat',l.source_consumed_ex_vat::text,'frozen_ex_vat',l.frozen_ex_vat::text,'frozen_vat',l.frozen_vat::text,'frozen_inc_vat',l.frozen_inc_vat::text)",
    "pg_catalog.jsonb_build_object('shift_detail_id',s.id,'detail_no',s.detail_no::text,'work_date',s.work_date, 'shift_start_at',s.shift_start_at,'shift_end_at',s.shift_end_at,'shift_start_local',s.shift_start_local,'shift_end_local',s.shift_end_local, 'shift_overnight',s.shift_overnight,'submitted_minutes',s.submitted_minutes::text,'approved_minutes',s.approved_minutes::text, 'approved_hours',s.approved_hours::text,'detail_label',s.detail_label,'segment_pay_ex_vat',s.segment_pay_ex_vat::text,'pay_excluded',s.pay_excluded, 'hours_day',s.hours_day::text,'hours_night',s.hours_night::text,'hours_sat',s.hours_sat::text,'hours_sun',s.hours_sun::text,'hours_bh',s.hours_bh::text, 'segment_key',s.segment_key,'approved_segment_id',sg.id,'job_title',wr.job_title,'band_label',wr.band_label)",
    "pg_catalog.jsonb_build_object('break_detail_id',b.id,'break_no',b.break_no::text,'break_start_at',b.break_start_at, 'break_end_at',b.break_end_at,'break_start_local',b.break_start_local,'break_end_local',b.break_end_local,'break_minutes',b.break_minutes::text)",
    "pg_catalog.jsonb_build_object('rate_detail_id',r.id,'bucket',r.bucket,'approved_hours',r.approved_hours::text,'source_pay_rate',r.source_pay_rate::text, 'rate_schedule_id',rs.id,'available_paye_rate',rs.paye_rate::text,'available_umbrella_rate',rs.umbrella_rate::text, 'available_charge_rate',rs.charge_rate::text,'approved_charge_rate',rs.approved_charge_rate::text)",
    "pg_catalog.jsonb_build_object('rows',v_rows,'complete',v_complete)"
  ],
  "private.bpay_next_channel_drafts_page_v1(uuid)": [
    "pg_catalog.jsonb_build_object( 'run_id',p_run_id,'pay_channel',c.pay_channel)",
    "pg_catalog.jsonb_build_object('rows',v_rows,'complete',true)"
  ],
  "private.bpay_next_channel_worker_page_v1(uuid,text,uuid,integer)": [
    "pg_catalog.jsonb_build_object('rows',v_rows,'complete',not(v_more or v_count=p_limit))"
  ],
  "private.bpay_next_channel_status_page_v1(uuid,text,uuid,integer,integer)": [],
  "public.bpay_next_payment_history_page_v1(jsonb)": [
    "jsonb_build_object('section',k,'parent',d->>'parent','keys',d->'keys', 'fields',string_to_array(d->>'fields',','),'amount_scope',d->>'state', 'applicability',case when v_work is not null and k in ('COMMANDS','CASES') then 'CANDIDATE_ONLY_USE_REFERENCED_PARENT' else 'SCOPE_QUALIFIED' end)",
    "jsonb_build_object('version','PAYMENT_HISTORY_V1','candidate_id',v_candidate,'work_id',v_work, 'sections',v_rows,'terminal',true,'next_cursor',null,'consistency','LIVE_KEYSET_REFRESH_FOR_LATE_COMMIT', 'scalar_encoding','STRING_OR_NULL','unavailable',array['MOVED_ROOT_FLAT_PAID_BINDING', 'CANONICAL_MULTI_CHANNEL_VECTOR','MOVE_COMPARISON_RECEIPT','GENERAL_APPROVAL_ACTOR_REASON_NOT_CAPTURED_IN_NEXT_ROWS'] ||case when v_work is not null then array['TIMESHEET_COMMAND_WITHOUT_WORK_OR_CASE_ANCESTRY'] else array[]::text[] end)",
    "jsonb_build_object(''key'',jsonb_build_array(%s),''data'',jsonb_build_object(%s))",
    "jsonb_build_object(%s)",
    "jsonb_build_object('version','PAYMENT_HISTORY_CURSOR_V1','scope_kind',v_scope_kind, 'scope_id',v_scope_id,'section',v_section,'parent_id',v_parent,'context_id',v_context, 'component_key',v_component,'upper',v_upper,'key',v_last)",
    "jsonb_build_object('version','PAYMENT_HISTORY_V1','candidate_id',v_candidate,'work_id',v_work, 'section',v_section,'parent_id',v_parent,'context_id',v_context,'component_key',v_component,'amount_scope',v_def->>'state', 'consistency','LIVE_KEYSET_REFRESH_FOR_LATE_COMMIT','rows',v_rows,'upper',v_upper, 'terminal',not v_more,'next_cursor',v_cursor)"
  ],
  "public.bpay_next_bind_stored_credit_v1(uuid,uuid,uuid)": []
});
function typedObjectConstructors(body){
  const result=[];
  for(const call of body.matchAll(/\b(?:pg_catalog\s*\.\s*)?jsonb?_build_object\s*\(/gi)){
    let end=call.index+call[0].length,depth=1,quote=false;
    for(;end<body.length&&depth;end++){
      const c=body[end];
      if(c==="'"){if(quote&&body[end+1]==="'"){end++;continue;}quote=!quote;}
      else if(!quote){if(c==='(')depth++;if(c===')')depth--;}
    }
    if(depth)throw Error('BPAY_NEXT_TRANSPORT_CONSTRUCTOR_UNCLOSED');
    result.push(body.slice(call.index,end).replace(/\s+/g,' ').trim());
  }
  return result;
}

export function assertTypedFinancialProjection(source,file,signature){
  const parts=executableParts(source,file).filter(part=>part.signature===signature);
  if(parts.length!==1||parts[0].unclassified)throw Error(`BPAY_NEXT_TRANSPORT_BODY_MISSING: ${signature}`);
  const expected=TYPED_OBJECT_CONSTRUCTORS[signature];
  if(expected&&JSON.stringify(typedObjectConstructors(parts[0].text))!==JSON.stringify(expected))throw Error(`BPAY_NEXT_TYPED_REPLY_KEYS_CHANGED: ${signature}`);
  const body=parts[0].text.toLowerCase(),flat=body.replace(/\s+/g,' ').trim();
  if(signature==='private.bpay_next_simple_break_page_v1(uuid,uuid,integer,integer)'){
    const declaration=sqlWithoutComments(source).replace(/\s+/g,' ').toLowerCase();
    const queries=[...flat.matchAll(/\breturn\s+query\s+(select\b[\s\S]*?\bfrom\b)/g)];
    if(!declaration.includes(TYPED_BREAK_RETURN)||queries.length!==1||queries[0][1]!==TYPED_BREAK_QUERY)throw Error(`BPAY_NEXT_TYPED_PROJECTION_CHANGED: ${signature}`);
  }
  if(signature==='public.bpay_next_payment_history_page_v1(jsonb)'&&
    (!flat.includes("v_fields:=v_fields||format('%l,t.%i::text',v_column,v_column)")
      ||!flat.includes("'scalar_encoding','string_or_null'")))throw Error(`BPAY_NEXT_DECIMAL_TEXT_MISSING: ${signature}`);
  const scalar=new Set(['true','false','null']);
  for(const match of body.matchAll(/\b([a-z_][a-z0-9_]*)\s+(?:jsonb?|uuid|text|boolean|date|timestamptz)\b/g))scalar.add(match[1]);
  for(const match of body.matchAll(/\b(?:pg_catalog\s*\.\s*)?(?:to_jsonb?|row_to_json|jsonb?_agg|jsonb?_build_array)\s*\(\s*(?:\(\s*)*([a-z_][a-z0-9_]*)\s*(?:\.\s*\*)?\s*(?:\)\s*)*(?=[,)])/g)){
    const value=match[1],tail=parts[0].text.slice(match.index).replace(/\s+/g,' ');
    if(value==='page'&&(TYPED_PAGE_PROJECTIONS[signature]??[]).some(prefix=>tail.startsWith(prefix)))continue;
    if(!scalar.has(value))throw Error(`BPAY_NEXT_OPAQUE_FINANCIAL_ROW: ${signature} (${value})`);
  }
  for(const call of body.matchAll(/\b(?:pg_catalog\s*\.\s*)?(?:to_jsonb?|row_to_json)\s*\(/g)){
    const argument=body.slice(call.index+call[0].length);
    if(!/^(?:\s*\()*\s*[a-z_][a-z0-9_]*\s*(?:\.\s*\*)?\s*(?:\)\s*)*(?=[,)])/.test(argument))throw Error(`BPAY_NEXT_OPAQUE_FINANCIAL_ROW: ${signature}`);
  }
  if(/\breturn\s+query\s+select\s+(?:(?:distinct|all)\s+)?(?:[a-z_][a-z0-9_]*\s*\.\s*)?\*/.test(body)
    ||(signature==='public.bpay_next_financial_intake_v1(uuid,text,jsonb)'&&/\breturn\s+v_result\s*;/.test(body)))throw Error(`BPAY_NEXT_OPAQUE_FINANCIAL_ROW: ${signature}`);
  if(/::\s*(?:pg_catalog\s*\.\s*)?(?:real|float[48]?|double\s+precision)\b/.test(body))throw Error(`BPAY_NEXT_FLOAT_TRANSPORT_FORBIDDEN: ${signature}`);
  const money=/\b(?:approved|realised|held|residual|funded|recovered|written_off|source_consumed|source_reserved|target_amount|frozen|frozen_gross|gross)_(?:source_)?(?:ex_vat|vat|inc_vat)\b|^(?:entered_paye_net|accepted_recoveries|accepted_net_additions|cash_amount|original_cash_amount|principal_(?:approved|funded|recovered|written_off)|active_hold_amount|amount_(?:owed|held|reissued_paid|available)|applied_amount|remaining_amount|protected_amount)$/;
  if(!signature.startsWith('public.bpay_next_payment_history_page_v1('))for(const match of flat.matchAll(/'([a-z_][a-z0-9_]*)'\s*,\s*/g)){
    if(!money.test(match[1]))continue;
    if(/->>?\s*$/.test(flat.slice(0,match.index)))continue; // operand key, not a reply key
    const start=match.index+match[0].length;let end=start,depth=0,quote=false;
    for(;end<flat.length;end++){
      const token=flat[end];
      if(token==="'"){if(quote&&flat[end+1]==="'"){end++;continue;}quote=!quote;continue;}
      if(quote)continue;
      if(token==='(')depth++;
      if(token===')'){if(depth===0)break;depth--;}
      if((token===','&&depth===0)||token===';')break;
    }
    const value=flat.slice(start,end);
    if(!/::\s*(?:pg_catalog\s*\.\s*)?text\s*\)*$|->>\s*'[^']+'\s*\)*$|^null$/.test(value.trim()))throw Error(`BPAY_NEXT_DECIMAL_TEXT_MISSING: ${signature} (${match[1]})`);
  }
}

// DR05 exact reviewed call edges; no dynamic closure admission.
const DR05_ROUTINE_WORK_CALLEES=Object.freeze({
  "private.bpay_next_preparation_progress_v1(uuid)": ["pg_catalog.btrim","pg_catalog.left","pg_catalog.jsonb_build_object"],
  "private.bpay_next_preparation_wake_v1(uuid)": ["pg_catalog.clock_timestamp","pg_catalog.jsonb_build_object"],
  "public.bpay_next_preparation_page_v1(uuid,text,jsonb)": ["private.bpay_next_case_finance_page_v1","private.bpay_next_preparation_progress_v1","pg_catalog.jsonb_build_array"],
  "private.bpay_next_prepare_job_run_v1(uuid)": [
    "lateral"
  ],
  "private.bpay_next_prepare_job_active_v1(uuid)": ["private.bpay_next_retry_execution_v1",
    "private.bpay_next_prepare_job_run_v1"
  ],
  "private.bpay_next_worker_action_receipt_v1(uuid,uuid,bigint,boolean)": [
    "pg_catalog.jsonb_build_object",
    "private.bpay_next_preparation_expiry_fenced_v1"
  ],
  "private.bpay_next_accept_worker_action_v1(uuid,uuid,text,uuid,bigint,uuid)": ["private.bpay_next_retry_execution_v1","private.bpay_next_prepare_attention_v1",
    "pg_catalog.clock_timestamp",
    "pg_catalog.hashtextextended",
    "pg_catalog.pg_advisory_xact_lock",
    "private.bpay_next_receive_command_v1",
    "private.bpay_next_worker_action_receipt_v1"
  ],
  "private.bpay_next_enroll_worker_action_v1(uuid)": [
    "pg_catalog.clock_timestamp",
    "pg_catalog.transaction_timestamp",
    "private.bpay_next_preparation_expiry_fenced_v1"
  ],
  "private.bpay_next_worker_action_guard_v1()": [
    "array_position",
    "pg_catalog.to_jsonb"
  ],
  "private.bpay_next_prepare_release_scope_v1(uuid,uuid,text)": [
    "pg_catalog.clock_timestamp",
    "private.bpay_next_prepare_job_active_v1"
  ],
  "private.bpay_next_release_prepare_work_hold_v1(uuid,uuid,timestamp with time zone)": [
    "pg_catalog.isfinite",
    "private.bpay_next_prepare_release_scope_v1"
  ],
  "private.bpay_next_worker_released_collection_scope_v1(uuid,uuid,uuid)": [
    "private.bpay_next_prepare_job_run_v1",
    "private.bpay_next_prepare_release_scope_v1"
  ],
  "private.bpay_next_reconcile_worker_released_collection_v1(uuid,uuid)": [
    "private.bpay_next_reconcile_work_collection_v1",
    "private.bpay_next_worker_released_collection_scope_v1"
  ],
  "private.bpay_next_release_prepare_case_hold_v1(uuid,uuid,timestamp with time zone)": [
    "private.bpay_next_record_case_hold_transition_v1",
    "pg_catalog.isfinite",
    "private.bpay_next_prepare_release_scope_v1",
    "private.bpay_next_reconcile_expired_work_collection_v1",
    "private.bpay_next_reconcile_worker_released_collection_v1"
  ],
  "private.bpay_next_worker_release_page_v1(uuid,uuid,bigint,text,bigint,integer)": [
    "pg_catalog.array_append",
    "pg_catalog.clock_timestamp",
    "pg_catalog.jsonb_build_object",
    "private.bpay_next_exclude_preparation_week_v1",
    "private.bpay_next_prepare_job_active_v1",
    "private.bpay_next_release_prepare_case_hold_v1",
    "private.bpay_next_release_prepare_work_hold_v1",
    "private.bpay_next_workbench_refresh_many_v1"
  ],
  "private.bpay_next_exclude_preparation_week_v1(uuid,uuid)": [
    "pg_catalog.clock_timestamp",
    "private.bpay_next_prepare_job_active_v1"
  ],
  "private.bpay_next_claim_prepare_job_v1(uuid,integer)": [
    "pg_catalog.clock_timestamp",
    "pg_catalog.gen_random_uuid",
    "pg_catalog.make_interval",
    "pg_catalog.transaction_timestamp",
    "private.bpay_next_prepare_job_active_v1",
    "private.bpay_next_prepare_job_run_v1"
  ],
  "private.bpay_next_pin_selected_work_v1(uuid,uuid,bigint,bigint)": [
    "pg_catalog.btrim",
    "pg_catalog.clock_timestamp",
    "pg_catalog.decode",
    "pg_catalog.isfinite",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.jsonb_typeof",
    "pg_catalog.to_jsonb",
    "private._candidate_expense_source_family_v1",
    "private.bpay_next_begin_prepare_projection_v1",
    "private.bpay_next_prepare_job_active_v1",
    "private.bpay_next_prepare_job_run_v1",
    "private.bpay_next_source_command_id_v1",
    "private.weekly_source_pay_query_admit_v2"
  ],
  "private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer)": ["jsonb_typeof","jsonb_array_length",
    "pg_catalog.array_agg",
    "pg_catalog.cardinality",
    "pg_catalog.clock_timestamp",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.octet_length",
    "private.bpay_next_begin_prepare_projection_v1",
    "private.bpay_next_bound_source_snapshot_v1",
    "private.bpay_next_capture_segment_page_v1",
    "private.bpay_next_prepare_job_active_v1",
    "private.bpay_next_prepare_job_run_v1",
    "private.bpay_next_selected_position_keys_v1",
    "private.bpay_next_snooze_at_cut_v1",
    "private.bpay_next_source_disposition_seed_v1",
    "private.bpay_next_work_collection_origin_v1"
  ],
  "private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer)": ["pg_catalog.round","public._pay_umbrella_vat_calc",
    "array_append",
    "pg_catalog.clock_timestamp",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.transaction_timestamp",
    "private.bpay_next_begin_prepare_projection_v1",
    "private.bpay_next_bound_source_snapshot_v1",
    "private.bpay_next_prepare_job_active_v1",
    "private.bpay_next_prepare_job_run_v1",
    "private.bpay_next_record_work_segment_origin_shares_v1",
    "private.bpay_next_value_source_for_target_v1",
    "private.bpay_next_workbench_refresh_many_v1"
  ],
  "private.bpay_next_complete_no_case_worker_v1(uuid,uuid,bigint)": [
    "pg_catalog.clock_timestamp",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.transaction_timestamp",
    "private.bpay_next_begin_prepare_projection_v1",
    "private.bpay_next_prepare_job_active_v1",
    "private.bpay_next_prepare_job_run_v1",
    "private.bpay_next_publish_no_case_week_fact_v1"
  ],
  "private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer)": [
    "jsonb_agg",
    "private.bpay_next_apply_case_period_amendment_v1",
    "jsonb_populate_record",
    "jsonb_set",
    "pg_catalog.clock_timestamp",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.transaction_timestamp",
    "pg_catalog.trunc",
    "private.bpay_next_begin_prepare_projection_v1",
    "private.bpay_next_capture_instruction_destination_v1",
    "private.bpay_next_case_allocation_plan_v1",
    "private.bpay_next_complete_no_case_worker_v1",
    "private.bpay_next_prepare_job_active_v1",
    "private.bpay_next_prepare_job_run_v1",
    "substring",
    "to_jsonb"
  ],
  "private.bpay_next_begin_prepare_projection_v1(uuid,uuid,bigint,integer)": [
    "pg_catalog.jsonb_build_object",
    "private.bpay_next_accept_worker_action_v1",
    "private.bpay_next_prepare_job_active_v1",
    "private.bpay_next_prepare_job_run_v1"
  ],
  "private.bpay_next_prepare_projection_page_v1(uuid,uuid,bigint,text,bigint,integer)": [
    "max",
    "pg_catalog.array_agg",
    "pg_catalog.clock_timestamp",
    "pg_catalog.jsonb_build_object",
    "private.bpay_next_preparation_expiry_fenced_v1",
    "private.bpay_next_prepare_job_active_v1",
    "private.bpay_next_prepare_job_run_v1",
    "private.bpay_next_review_simple_run_v1",
    "private.bpay_next_workbench_refresh_many_v1"
  ],
  "private.bpay_next_enroll_one_v1()": [
    "pg_catalog.transaction_timestamp",
    "private.bpay_next_enroll_preparation_expiry_one_v1",
    "private.bpay_next_enroll_worker_action_v1",
    "private.bpay_next_preparation_expiry_fenced_v1",
    "private.bpay_next_snooze_due_receipt_valid_v1"
  ],
  "private.bpay_next_accept_case_net_request_v1(uuid,uuid,uuid,text,numeric,bigint)": [
    "pg_catalog.hashtextextended",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.pg_advisory_xact_lock",
    "pg_catalog.transaction_timestamp",
    "pg_catalog.trunc",
    "private.bpay_next_receive_command_v1"
  ],
  "private.bpay_next_apply_case_paye_net_page_v1(uuid,uuid,bigint,bigint,integer)": [
    "private.bpay_next_record_case_hold_transition_v1",
    "pg_catalog.clock_timestamp",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.transaction_timestamp",
    "private.bpay_next_accumulate_net_destination_v1",
    "private.bpay_next_case_allocation_row_v1",
    "private.bpay_next_seal_net_destination_v1"
  ],
  "private.bpay_next_begin_case_transfer_v1(uuid,uuid)": [
    "pg_catalog.hashtextextended",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.pg_advisory_xact_lock",
    "pg_catalog.transaction_timestamp",
    "private.bpay_next_receive_command_v1"
  ],
  "private.bpay_next_build_case_transfer_page_v1(uuid,uuid,bigint,bigint,integer)": [
    "pg_catalog.clock_timestamp",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.transaction_timestamp"
  ],
  "private.bpay_next_build_destination_group_page_v1(uuid,uuid,bigint,bigint,integer)": [
    "pg_catalog.clock_timestamp",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.transaction_timestamp"
  ],
  "private.bpay_next_case_cancel_block_v1(uuid,boolean)": [],
  "private.bpay_next_accept_case_cancel_v1(uuid,uuid)": [
    "pg_catalog.hashtextextended",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.pg_advisory_xact_lock",
    "pg_catalog.transaction_timestamp",
    "private.bpay_next_case_cancel_block_v1",
    "private.bpay_next_receive_command_v1"
  ],
  "private.bpay_next_cancel_case_worker_page_v1(uuid,uuid,bigint,bigint,integer)": [
    "private.bpay_next_record_case_hold_transition_v1",
    "pg_catalog.clock_timestamp",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.make_interval",
    "pg_catalog.transaction_timestamp",
    "private.bpay_next_apply_cancelled_week_v1",
    "private.bpay_next_case_cancel_block_v1",
    "private.bpay_next_check_destination_cancel_page_v1",
    "private.bpay_next_finish_destination_cancel_page_v1",
    "private.bpay_next_reconcile_cancelled_work_collection_v1"
  ],
  "private.bpay_next_apply_cancelled_week_v1(uuid)": [
    "pg_catalog.clock_timestamp"
  ],
  "private.bpay_next_workbench_refresh_case_v1(uuid)": [
    "array_agg",
    "gen_random_uuid",
    "max",
    "on",
    "private.bpay_next_workbench_apply_delta_v1",
    "private.bpay_next_workbench_ensure_metadata_v1",
    "private.bpay_next_workbench_group_contribution_v1",
    "private.bpay_next_workbench_refresh_group_display_v1",
    "private.bpay_next_workbench_require_next_v1",
    "sum",
    "transaction_timestamp"
  ],
  "private.bpay_next_case_instruction_insert_guard_v1()": [
    "private.bpay_next_prepare_job_active_v1"
  ],
  "private.bpay_next_expire_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer)": ["private.bpay_next_retry_execution_v1",
    "pg_catalog.clock_timestamp",
    "pg_catalog.jsonb_build_object",
    "private.bpay_next_apply_expired_week_v1",
    "private.bpay_next_release_prepare_case_hold_v1",
    "private.bpay_next_release_prepare_work_hold_v1",
    "private.bpay_next_worker_release_complete_v1"
    ,"private.bpay_next_work_wait_state_v1"
  ,"private.bpay_next_workbench_queue_origin_v1"
  ,"private.bpay_next_workbench_refresh_many_v1"
],
  "private.bpay_next_apply_expired_week_v1(uuid)": [
    "pg_catalog.clock_timestamp",
    "private.bpay_next_exclude_preparation_week_v1"
  ],
  "public.bpay_next_financial_job_v1(text,uuid,text,jsonb)": [
    "pg_catalog.array_length",
    "pg_catalog.char_length",
    "pg_catalog.clock_timestamp",
    "pg_catalog.current_setting",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.jsonb_object_keys",
    "pg_catalog.jsonb_typeof",
    "pg_catalog.octet_length",
    "pg_catalog.to_char",
    "private.bpay_next_apply_case_create_v1",
    "private.bpay_next_apply_case_edit_v1",
    "private.bpay_next_apply_case_pause_v1",
    "private.bpay_next_case_pause_receipt_v1",
    "private.bpay_next_apply_case_paye_net_page_v1",
    "private.bpay_next_apply_position_page_v1",
    "private.bpay_next_apply_reissue_cancel_v1",
    "private.bpay_next_apply_simple_paye_net_v1",
    "private.bpay_next_apply_write_off_v1",
    "private.bpay_next_build_simple_reissue_v1",
    "private.bpay_next_build_simple_transfer_page_v1",
    "private.bpay_next_cancel_simple_worker_page_v1",
    "private.bpay_next_capture_position_page_v1",
    "private.bpay_next_claim_case_create_job_v1",
    "private.bpay_next_claim_case_edit_job_v1",
    "private.bpay_next_claim_case_pause_job_v1",
    "private.bpay_next_claim_position_job_v1",
    "private.bpay_next_claim_preparation_expiry_job_v1",
    "private.bpay_next_claim_prepare_job_v1",
    "private.bpay_next_claim_reissue_cancel_job_v1",
    "private.bpay_next_claim_simple_cancel_job_v1",
    "private.bpay_next_claim_simple_outcome_job_v1",
    "private.bpay_next_claim_simple_paye_net_job_v1",
    "private.bpay_next_claim_simple_transfer_job_v1",
    "private.bpay_next_claim_write_off_job_v1",
    "private.bpay_next_complete_no_case_worker_v1",
    "private.bpay_next_expire_preparation_worker_page_v1",
    "private.bpay_next_expired_prepare_receipt_v1",
    "private.bpay_next_hold_fresh_position_page_v1",
    "private.bpay_next_pin_selected_work_v1",
    "private.bpay_next_post_simple_outcome_page_v1",
    "private.bpay_next_post_simple_return_v1",
    "private.bpay_next_preparation_expiry_fenced_v1",
    "private.bpay_next_prepare_case_page_v1",
    "private.bpay_next_prepare_job_active_v1",
    "private.bpay_next_prepare_job_run_v1",
    "private.bpay_next_prepare_projection_page_v1",
    "private.bpay_next_worker_release_page_v1",
    "private.weekly_source_pay_query_admit_v2"
  ],
  "public.bpay_next_preparation_intake_v1(uuid,text,jsonb)": [
    "private.bpay_next_preparation_wake_v1",
    "array",
    "current_setting",
    "pg_catalog.array_append",
    "pg_catalog.cardinality",
    "pg_catalog.jsonb_array_elements",
    "pg_catalog.jsonb_array_length",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.jsonb_typeof",
    "pg_catalog.octet_length",
    "private.bpay_next_accept_worker_action_v1",
    "private.bpay_next_append_case_selection_page_v1",
    "private.bpay_next_append_component_selection_page_v1",
    "private.bpay_next_append_segment_selection_page_v1",
    "private.bpay_next_append_work_choices_v1",
    "private.bpay_next_confirm_simple_run_v1",
    "private.bpay_next_create_preparation_v1",
    "private.bpay_next_enqueue_preparation_v1",
    "private.bpay_next_exclusion_page_v1",
    "private.bpay_next_matching_page_v1",
    "private.bpay_next_preparation_args_v1",
    "private.bpay_next_preparation_authority_v1",
    "private.bpay_next_review_simple_run_v1",
    "private.bpay_next_seal_case_selection_v1",
    "private.bpay_next_seal_component_selection_v1",
    "private.bpay_next_seal_segment_selection_v1",
    "private.bpay_next_seal_selection_v1",
    "private.bpay_next_selection_page_authority_v1",
    "private.weekly_source_pay_query_admit_v2"
  ],
  "private.bpay_next_review_simple_run_v1(uuid)": ["private.bpay_next_prepare_attention_v1",
    "pg_catalog.jsonb_build_object"
  ],
  "private.bpay_next_confirm_simple_run_v1(uuid,bigint)": [
    "pg_catalog.clock_timestamp",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.transaction_timestamp",
    "private.bpay_next_channel_drafts_page_v1"
  ],
  "private.bpay_next_segment_prepare_owner_v1(uuid,text)": [
    "clock_timestamp",
    "private.bpay_next_prepare_job_active_v1",
    "private.bpay_next_prepare_job_run_v1"
  ],
  "private.bpay_next_run_work_query_pin_guard_v2()": [
    "pg_catalog.clock_timestamp",
    "private.bpay_next_prepare_job_active_v1",
    "private.bpay_next_prepare_job_run_v1"
  ],
  "private.bpay_next_work_collection_capture_guard_v1()": [
    "pg_catalog.clock_timestamp",
    "pg_catalog.to_jsonb",
    "private.bpay_next_prepare_job_active_v1",
    "private.bpay_next_prepare_job_run_v1",
    "private.bpay_next_segment_prepare_owner_v1",
    "private.bpay_next_work_collection_origin_v1",
    "sum",
    "to_jsonb"
  ],
  "private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid)": [
    "pg_catalog.cardinality",
    "pg_catalog.char_length",
    "pg_catalog.clock_timestamp",
    "pg_catalog.transaction_timestamp",
    "private.bpay_next_expired_work_collection_scope_v1",
    "private.bpay_next_segment_collection_basis_v1",
    "private.bpay_next_worker_released_collection_scope_v1"
  ],
  "private.bpay_next_work_collection_guard_v1()": [
    "pg_catalog.clock_timestamp",
    "private.bpay_next_expired_work_collection_scope_v1",
    "private.bpay_next_segment_collection_basis_v1",
    "private.bpay_next_worker_released_collection_scope_v1"
  ],
  "private.bpay_next_complete_retained_segment_parent_v1(uuid,uuid,text)": [
    "private.bpay_next_segment_prepare_owner_v1",
    "private.bpay_next_work_collection_origin_v1",
    "row",
    "sum"
  ],
  "private.bpay_next_capture_segment_page_v1(uuid,integer)": [
    "private.bpay_next_complete_retained_segment_parent_v1",
    "private.bpay_next_run_segment_members_v1",
    "private.bpay_next_segment_prepare_owner_v1",
    "private.bpay_next_work_segment_gross_balance_v1",
    "sum"
  ],
  "private.bpay_next_worker_release_complete_v1(uuid,timestamp with time zone)": [],
  "private.bpay_next_accept_preparation_expiry_v1(uuid,uuid,uuid,timestamp with time zone,timestamp with time zone)": [
    "pg_catalog.clock_timestamp",
    "pg_catalog.hashtextextended",
    "pg_catalog.isfinite",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.pg_advisory_xact_lock",
    "private.bpay_next_receive_command_v1"
  ],
  "private.bpay_next_enroll_preparation_expiry_one_v1(uuid)": ["private.bpay_next_retry_execution_v1",
    "pg_catalog.clock_timestamp",
    "private.bpay_next_preparation_expiry_fenced_v1",
    "private.bpay_next_worker_release_complete_v1"
  ],
  "private.bpay_next_preparation_expiry_guard_v1()": [
    "pg_catalog.to_jsonb",
    "private.bpay_next_preparation_expiry_fenced_v1",
    "private.bpay_next_worker_release_complete_v1"
  ],
  "private.bpay_next_expired_work_collection_scope_v1(uuid,uuid,uuid)": [
    "pg_catalog.clock_timestamp",
    "private.bpay_next_prepare_job_run_v1"
  ],
  "private.bpay_next_expired_prepare_receipt_v1(uuid)": ["private.bpay_next_retry_execution_v1",
    "pg_catalog.jsonb_build_object",
    "private.bpay_next_preparation_expiry_fenced_v1",
    "private.bpay_next_prepare_job_run_v1"
  ],
  "private.bpay_next_worker_execution_guard_v1()": ["private.bpay_next_retry_execution_v1",]
});

// DR09 exact reviewed native call edges; no dynamic closure admission.
const DR09_ROUTINE_WORK_CALLEES=Object.freeze({
  "private.bpay_next_accept_preparation_discard_v1(uuid,uuid,uuid,bigint,text)": [
    "pg_catalog.btrim",
    "pg_catalog.clock_timestamp",
    "pg_catalog.hashtextextended",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.octet_length",
    "pg_catalog.pg_advisory_xact_lock",
    "private.bpay_next_receive_command_v1"
  ],
  "private.bpay_next_enroll_preparation_discard_one_v1(uuid)": ["private.bpay_next_retry_execution_v1",
    "pg_catalog.clock_timestamp",
    "private.bpay_next_preparation_discard_fenced_v1",
    "private.bpay_next_worker_release_complete_v1"
  ],
  "private.bpay_next_claim_preparation_discard_job_v1(uuid,integer)": [
    "pg_catalog.clock_timestamp",
    "pg_catalog.gen_random_uuid",
    "pg_catalog.make_interval"
  ],
  "private.bpay_next_discard_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer)": ["private.bpay_next_retry_execution_v1",
    "array_agg",
    "cardinality",
    "max",
    "pg_catalog.clock_timestamp",
    "pg_catalog.jsonb_build_object",
    "private.bpay_next_exclude_preparation_week_v1",
    "private.bpay_next_finish_discard_member_v1",
    "private.bpay_next_release_prepare_case_hold_v1",
    "private.bpay_next_release_prepare_work_hold_v1",
    "private.bpay_next_work_wait_state_v1",
    "private.bpay_next_workbench_queue_origin_v1",
    "private.bpay_next_workbench_refresh_many_v1",
    "private.bpay_next_worker_release_complete_v1",
    "unnest"
  ],
  "private.bpay_next_discarded_work_collection_scope_v1(uuid,uuid,uuid)": [
    "pg_catalog.clock_timestamp",
    "private.bpay_next_prepare_job_run_v1"
  ],
  "private.bpay_next_reconcile_discarded_work_collection_v1(uuid,uuid)": [
    "private.bpay_next_discarded_work_collection_scope_v1",
    "private.bpay_next_reconcile_work_collection_v1"
  ],
  "private.bpay_next_discarded_prepare_receipt_v1(uuid)": ["private.bpay_next_retry_execution_v1",
    "pg_catalog.jsonb_build_object",
    "private.bpay_next_preparation_discard_fenced_v1",
    "private.bpay_next_prepare_job_run_v1"
  ],
  "private.bpay_next_release_prepare_case_hold_v1(uuid,uuid,timestamp with time zone)": [
    "private.bpay_next_reconcile_discarded_work_collection_v1"
  ],
  "private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid)": [
    "private.bpay_next_discarded_work_collection_scope_v1"
  ],
  "private.bpay_next_work_collection_guard_v1()": [
    "private.bpay_next_discarded_work_collection_scope_v1"
  ],
  "private.bpay_next_preparation_expiry_guard_v1()": [
    "unnest"
  ],
  "private.bpay_next_preparation_discard_guard_v1()": [
    "pg_catalog.to_jsonb",
    "private.bpay_next_preparation_discard_fenced_v1",
    "private.bpay_next_worker_release_complete_v1",
    "unnest"
  ],
  "private.bpay_next_enroll_one_v1()": [
    "private.bpay_next_enroll_preparation_discard_one_v1",
    "private.bpay_next_preparation_discard_fenced_v1"
  ],
  "private.bpay_next_enroll_worker_action_v1(uuid)": [
    "private.bpay_next_preparation_discard_fenced_v1"
  ],
  "private.bpay_next_worker_action_receipt_v1(uuid,uuid,bigint,boolean)": [
    "private.bpay_next_preparation_discard_fenced_v1"
  ],
  "public.bpay_next_preparation_intake_v1(uuid,text,jsonb)": [
    "pg_catalog.current_setting",
    "cardinality",
    "pg_catalog.jsonb_object_keys",
    "private.bpay_next_accept_preparation_discard_v1"
  ],
  "public.bpay_next_financial_job_v1(text,uuid,text,jsonb)": [
    "private.bpay_next_claim_preparation_discard_job_v1",
    "private.bpay_next_discard_preparation_worker_page_v1",
    "private.bpay_next_discarded_prepare_receipt_v1",
    "private.bpay_next_preparation_discard_fenced_v1"
  ],
  "private.bpay_next_preparation_progress_v1(uuid)": [
    "left"
  ],
  "public.bpay_next_preparation_page_v1(uuid,text,jsonb)": [
    "pg_catalog.current_setting",
    "private.bpay_next_case_list_page_v1",
    "private.bpay_next_case_create_preview_v1",
    "private.bpay_next_case_edit_preview_v1",
    "private.bpay_next_case_edit_receipt_v1",
    "private.bpay_next_case_pause_receipt_v1",
    "jsonb_set",
    "jsonb_build_array",
    "pg_catalog.jsonb_typeof",
    "private.bpay_next_paye_guard_page_v1",
    "array",
    "array_remove",
    "jsonb_agg",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.octet_length",
    "private.bpay_next_channel_drafts_page_v1",
    "private.bpay_next_channel_status_page_v1",
    "private.bpay_next_channel_worker_page_v1",
    "private.bpay_next_choice_inputs_page_v1",
    "private.bpay_next_current_case_component_page_v1",
    "private.bpay_next_current_case_page_v1",
    "private.bpay_next_current_stored_credit_page_v1",
    "private.bpay_next_current_work_page_v1",
    "private.bpay_next_preparation_args_v1",
    "private.bpay_next_preparation_authority_v1",
    "private.bpay_next_return_cash_page_v1",
    "private.bpay_next_selection_page_authority_v1",
    "private.bpay_next_simple_review_line_page_v1",
    "private.bpay_next_simple_review_worker_page_v1",
    "private.bpay_next_simple_revision_comparison_page_v1",
    "private.bpay_next_worker_status_page_v1",
    "substring"
  ],
  "public.bpay_next_preparation_expiry_due_v1(timestamp with time zone,uuid,integer)": [
    "pg_catalog.clock_timestamp",
    "pg_catalog.current_setting",
    "pg_catalog.isfinite",
    "pg_catalog.jsonb_build_array",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.to_char"
  ],
  "public.bpay_next_expiry_dispatch_page_v1(integer)": [
    "pg_catalog.clock_timestamp",
    "pg_catalog.current_setting",
    "pg_catalog.jsonb_array_elements",
    "pg_catalog.jsonb_build_array",
    "pg_catalog.jsonb_build_object",
    "pg_catalog.to_char",
    "public.bpay_next_preparation_expiry_due_v1"
  ],
  "private.bpay_next_project_worker_release_page_v1(uuid,uuid,bigint,text,bigint,integer)": [
    "array_agg",
    "max",
    "pg_catalog.clock_timestamp",
    "pg_catalog.jsonb_build_object",
    "private.bpay_next_prepare_job_active_v1",
    "private.bpay_next_review_simple_run_v1",
    "private.bpay_next_work_wait_state_v1",
    "private.bpay_next_workbench_queue_origin_v1",
    "private.bpay_next_workbench_refresh_many_v1",
    "unnest"
  ],
  "private.bpay_next_accept_worker_action_v1(uuid,uuid,text,uuid,bigint,uuid)": [
    "private.bpay_next_prepare_job_active_v1"
  ],
  "private.bpay_next_worker_release_page_v1(uuid,uuid,bigint,text,bigint,integer)": [
    "array_agg",
    "cardinality",
    "private.bpay_next_work_wait_state_v1",
    "private.bpay_next_workbench_queue_origin_v1",
    "unnest"
  ],
  "private.bpay_next_worker_action_guard_v1()": [
    "unnest"
  ],
  "private.bpay_next_prepare_projection_page_v1(uuid,uuid,bigint,text,bigint,integer)": [
    "private.bpay_next_project_worker_release_page_v1"
  ]
});

// DR11 exact early fence helper edges; existing allocation/money owners retained.
const DR11_ROUTINE_WORK_CALLEES=Object.freeze({
  "private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer)": [
    "private.bpay_next_assert_candidate_financial_fence_v1"
  ],
  "private.bpay_next_expire_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer)": [
    "private.bpay_next_assert_candidate_financial_fence_v1"
  ],
  "private.bpay_next_assert_candidate_financial_fence_v1(uuid,uuid,uuid,bigint)": ["pg_catalog.clock_timestamp"]
});

export function assertRoutineHistoryPoints(source,file,signature){
  const parts=executableParts(source,file).filter(part=>part.signature===signature);
  if(parts.length!==1||parts[0].unclassified)throw Error(`BPAY_NEXT_ROUTINE_WORK_BODY_MISSING: ${signature}`);
  const body=parts[0].text;
  const tokens=scanManagedSql(body).statements.flatMap(row=>row.tokens).join(' ').toLowerCase();
  if(/\b(?:bpay_next_payment_history_page_v1|audit_events_list|pay_finance_case_audit_actor_index)\s*\(/.test(tokens))throw Error(`BPAY_NEXT_ROUTINE_HISTORY_READER: ${signature}`);
  if(/\bexecute\b/.test(tokens))throw Error(`BPAY_NEXT_ROUTINE_HISTORY_DYNAMIC: ${signature}`);
  const approved=new Set([...(ROUTINE_WORK_CALLEES[signature]??[]),...(DR05_ROUTINE_WORK_CALLEES[signature]??[]),...(DR09_ROUTINE_WORK_CALLEES[signature]??[]),...(DR11_ROUTINE_WORK_CALLEES[signature]??[])]);
  const native=new Set(['and','any','as','bool_and','btrim','coalesce','conflict','convert_to','count','decode',
    'else','elsif','encode','exists','extract','from','greatest','if','in','jsonb_array_elements',
    'jsonb_array_length','jsonb_build_array','jsonb_build_object','jsonb_typeof','least','materialized',
    'not','nullif','numeric','or','over','round','select','sha256','then','values','when','where']);
  for(const match of tokens.matchAll(/\b([a-z_][a-z0-9_$]*)(?:\s*\.\s*([a-z_][a-z0-9_$]*))?\s*\(/g)){
    const prefix=tokens.slice(0,match.index).trimEnd(),schema=match[2]?match[1]:null;
    const name=match[2]?schema+'.'+match[2]:match[1];
    // INSERT column targets and casts are declarations, not helper delegation.
    if(/(?:\binto|:\s*:)\s*$/.test(prefix))continue;
    if(!schema&&prefix.endsWith(')'))continue; // actual SQL table alias(column list)
    if(!schema&&native.has(name))continue;
    if(!approved.has(name))throw Error(`BPAY_NEXT_ROUTINE_HISTORY_DELEGATION: ${signature} (${name})`);
  }
  let code=body.replace(/\s+/g,' ').trim().toLowerCase();
  const exact=[];
  if(signature==='private.bpay_next_exclude_preparation_week_v1(uuid,uuid)')exact.push(
    'exists(select 1 from private.bpay_next_internal_receipt where run_worker_id=v_worker.id)');
  if(signature.startsWith('private.bpay_next_prepare_case_page_v1(')||signature==='private.bpay_next_case_allocation_plan_v1(jsonb)')exact.push(
    "select 1 from private.bpay_next_case_event e where e.id=v_rule.original_funding_event_id and e.case_id=v_case.id and e.event_kind='funded' and e.occurred_at_utc=v_rule.original_funded_at_utc)");
  if(signature.startsWith('private.bpay_next_source_book_seed_proof_v1('))exact.push(
    'select e.* into strict v_effect from private.bpay_next_financial_effect e where e.id=p_seed_effect_id;');
  if(signature==='private.bpay_next_work_collection_guard_v1()')exact.push(
    'select e.* into strict v_effect from private.bpay_next_financial_effect e where e.id=new.original_effect_id;',
    "exists(select 1 from private.bpay_next_financial_effect e join private.bpay_next_outcome_request r on r.command_id=j.command_id where e.id=new.last_effect_id and e.operation_id=j.command_id and e.effect_kind='payroll_settled' and e.work_id=new.work_id and e.component_key=new.component_key and e.candidate_id=new.candidate_id and e.original_transfer_id=r.transfer_id and e.source_disposition_ex_vat>0)",
    "exists(select 1 from private.bpay_next_financial_effect e join private.bpay_next_case_event ce on ce.id=e.operation_item_id and ce.operation_id=j.command_id join private.bpay_next_run_case_instruction i on i.id=ce.instruction_id and i.work_collection_id=new.id join private.bpay_next_case_hold h on h.id=ce.operation_item_id and h.status='realised' join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id and u.status='realised' and u.realisation_event_id=ce.id join private.bpay_next_outcome_request r on r.command_id=j.command_id and r.transfer_id=ce.original_transfer_id where e.id=new.last_effect_id and e.operation_id=j.command_id and e.effect_kind='work_collection_recovered' and ce.event_kind='recovered' and ce.case_id=new.case_id and ce.case_component_id=new.case_component_id and e.work_id=new.work_id and e.component_key=new.component_key and e.candidate_id=new.candidate_id and e.original_transfer_id=ce.original_transfer_id and (e.source_disposition_ex_vat,e.target_amount_ex_vat,e.target_amount_vat,e.target_amount_inc_vat) is not distinct from (-ce.source_amount_ex_vat,-ce.target_amount_ex_vat,-ce.target_amount_vat,-ce.target_amount_inc_vat))");
  if(signature.startsWith('private.bpay_next_reconcile_work_collection_v1('))exact.push(
    'select e.* into strict v_effect from private.bpay_next_financial_effect e where e.id=p_effect_id;',
    `select 1 from private.bpay_next_work_collection b
      join private.bpay_next_case_event ce on ce.id=v_effect.operation_item_id and ce.case_id=b.case_id
      join private.bpay_next_run_case_instruction i on i.id=ce.instruction_id and i.work_collection_id=b.id
      join private.bpay_next_case_hold h on h.id=ce.operation_item_id and h.status='realised'
      join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id and u.status='realised' and u.realisation_event_id=ce.id
      join private.bpay_next_outcome_request r on r.command_id=v_job.command_id and r.transfer_id=ce.original_transfer_id
      where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
        and ce.operation_id=v_job.command_id and ce.event_kind='recovered'
        and ce.case_component_id=b.case_component_id and ce.source_amount_ex_vat>0
        and v_effect.operation_id=v_job.command_id and v_effect.work_id=b.work_id
        and v_effect.component_key=b.component_key and v_effect.candidate_id=b.candidate_id
        and v_effect.case_id=b.case_id and v_effect.original_transfer_id=ce.original_transfer_id
        and (v_effect.source_disposition_ex_vat,v_effect.target_amount_ex_vat,v_effect.target_amount_vat,v_effect.target_amount_inc_vat)
          is not distinct from (-ce.source_amount_ex_vat,-ce.target_amount_ex_vat,-ce.target_amount_vat,-ce.target_amount_inc_vat))`.replace(/\s+/g,' '));
  for(const clause of exact)code=code.replace(clause,' ');
  const remaining=scanManagedSql(code).statements.flatMap(row=>row.tokens).join(' ').toLowerCase();
  for(const match of remaining.matchAll(new RegExp(`\\b(?:private\\s*\\.\\s*)?(?:${HISTORY_RELATIONS.join('|')})\\b`,'g'))){
    const before=remaining.slice(0,match.index).trimEnd(),after=remaining.slice(match.index+match[0].length);
    // Named row types and INSERT targets are not history reads. Every other
    // remaining ledger occurrence fails closed, including ONLY/comma/TABLE.
    if(/^\s*%\s*rowtype\b/.test(after)||/\binsert\s+into\s*$/.test(before)||/:\s*:\s*$/.test(before))continue;
    throw Error(`BPAY_NEXT_ROUTINE_HISTORY_SCAN: ${signature}`);
  }
}
const read=file=>fs.readFileSync(path.join(repoRoot,file),'utf8');
const git=args=>{
  const r=spawnSync('git',args,{cwd:repoRoot,encoding:'utf8',maxBuffer:64*1024*1024,windowsHide:true});
  if(r.status!==0)throw Error('BPAY_NEXT_SOURCE_GIT_FAILED');
  return r.stdout;
};

// Keep executable dollar bodies and literals, mask only genuine SQL comments.
// Quoted comment markers never change scanner state; nested comments are bounded
// by the input. This is a source guard, not SQL rewriting or type inference.
export function sqlWithoutComments(source){
  let result='',i=0;
  while(i<source.length){
    const dollar=source[i]==='$'?source.slice(i).match(/^\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$/):null;
    if(dollar){const end=source.indexOf(dollar[0],i+dollar[0].length);if(end<0)throw Error('BPAY_NEXT_SOURCE_UNCLOSED_BODY');result+=source.slice(i,end+dollar[0].length);i=end+dollar[0].length;continue;}
    if(source[i]==="'"||source[i]==='"'){
      const quote=source[i],start=i++,escape=quote==="'"&&/[eE]/.test(source[start-1]??'')&&!/[\w$]/.test(source[start-2]??'');
      let closed=false;
      while(i<source.length){
        if(escape&&source[i]==='\\'){i+=2;continue;}
        if(source[i]===quote){if(source[i+1]===quote){i+=2;continue;}i++;closed=true;break;}i++;
      }
      if(!closed)throw Error('BPAY_NEXT_SOURCE_UNCLOSED_QUOTE');
      const part=source.slice(start,i);
      if(quote==='"'){
        const identifier=part.slice(1,-1).replaceAll('""','"');
        result+=/^[A-Za-z_][A-Za-z0-9_$]*$/.test(identifier)?identifier:`BPAY_NEXT_QUOTED_IDENTIFIER_UNCLASSIFIED ${part}`;
      }else result+=part;
      continue;
    }
    if(source.startsWith('--',i)){while(i<source.length&&source[i]!=='\n')i++;result+=' ';continue;}
    if(source.startsWith('/*',i)){
      i+=2;let depth=1;
      while(i<source.length&&depth){if(source.startsWith('/*',i)){depth++;i+=2;}else if(source.startsWith('*/',i)){depth--;i+=2;}else i++;}
      if(depth)throw Error('BPAY_NEXT_SOURCE_UNCLOSED_COMMENT');result+=' ';continue;
    }
    result+=source[i++];
  }
  return result;
}

function assertProviderDefault(code,file){
  const target='private\\s*\\.\\s*bpay_next_rail_capability\\b';
  if(!new RegExp(target,'i').test(code))return;
  for(const {text} of executableParts(code,file))for(const row of scanManagedSql(text).statements){
    const statement=text.slice(row.start,row.end);
    if(new RegExp(`alter\\s+table\\s+(?:only\\s+)?${target}[\\s\\S]*\\benabled\\s+set\\s+default\\b`,'i').test(statement)){
      const expression=statement.match(/\benabled\s+set\s+default\s+([\s\S]*?)(?:;|$)/i)?.[1].trim();
      if(!/^(?:false|\(\s*false\s*\))$/i.test(expression??''))throw Error(`BPAY_NEXT_PROVIDER_DEFAULT_UNCLASSIFIED: ${file}`);
    }
    const setClause=statement.match(/\bset\s+([\s\S]*?)(?:\bwhere\b|;|$)/i)?.[1]??'';
    const offScalar=/\benabled\s*=\s*false\s*(?=,|$)/i.test(setClause);
    if(new RegExp(`(?:update|merge\\s+into)\\s+(?:only\\s+)?${target}`,'i').test(statement)&&/\brail\s*=/i.test(setClause)&&!offScalar)throw Error(`BPAY_NEXT_PROVIDER_RAIL_MUTATION_UNCLASSIFIED: ${file}`);
    if(new RegExp(`create\\s+table[^;]*${target}`,'i').test(statement)&&/\benabled\b[^,;]*\bdefault\b/i.test(statement)){
      const expression=statement.match(/\benabled\b[^,;]*?\bdefault\s+([^,;]*)/i)?.[1].trim();
      if(!/^false\s*\)?$/i.test(expression??''))throw Error(`BPAY_NEXT_PROVIDER_ENABLE_FORBIDDEN: ${file}`);
    }
    if(new RegExp(`insert\\s+into\\s+(?:only\\s+)?${target}`,'i').test(statement)){
      const insertion=statement.match(/bpay_next_rail_capability\s*\(([^()]*)\)\s*values\s*([\s\S]*?)(?:on\s+conflict|returning|;|$)/i);
      if(!insertion)throw Error(`BPAY_NEXT_PROVIDER_INSERT_UNCLASSIFIED: ${file}`);
      const columns=insertion[1].toLowerCase().split(',').map(v=>v.trim()),rail=columns.indexOf('rail'),enabled=columns.indexOf('enabled');
      if(rail<0||enabled<0)throw Error(`BPAY_NEXT_PROVIDER_INSERT_UNCLASSIFIED: ${file}`);
      const tuples=scanManagedSql(insertion[2]).statements.flatMap(row=>{
        const values=[];let depth=0,tuple=[],field=[];
        for(const token of row.tokens){
          if(token==='('){if(depth++===0){tuple=[];field=[];continue;}}
          if(token===')'&&--depth===0){tuple.push(field.join(' '));values.push(tuple);continue;}
          if(token===','&&depth===1){tuple.push(field.join(' '));field=[];}else if(depth>0)field.push(token);
        }
        return values;
      });
      const labels=[...insertion[2].matchAll(/'((?:[^']|'')*)'/g)].map(m=>m[1]);
      // Rail is the first explicit string column in the named EX-47 seed. Other
      // layouts fail closed rather than infer row values or omitted defaults.
      if(rail!==0||!tuples.length||tuples.length!==labels.length||tuples.some((tuple,i)=>tuple.length!==columns.length
        ||!['CSV','EXTERNAL','SIMULATED','PROVIDER'].includes(labels[i])
        ||(tuple[enabled]!=='FALSE'&&!(['CSV','EXTERNAL'].includes(labels[i])&&tuple[enabled]==='TRUE'))))throw Error(`BPAY_NEXT_PROVIDER_ENABLE_FORBIDDEN: ${file}`);
      if(/on\s+conflict[^;]*\bdo\s+update\b/i.test(statement))throw Error(`BPAY_NEXT_PROVIDER_UPSERT_UNCLASSIFIED: ${file}`);
    }
    if(new RegExp(`(?:update|merge\\s+into)\\s+(?:only\\s+)?${target}`,'i').test(statement)){
      if(offScalar&&!setClause.match(/\benabled\s*=/gi)?.slice(1).length)continue;
      if(/^\s*update\b/i.test(statement)&&/\benabled\s*=\s*true\b/i.test(statement)
        &&/\bwhere\s+rail\s*=\s*'(?:CSV|EXTERNAL)'\s*;?\s*$/i.test(statement))continue;
      throw Error(`BPAY_NEXT_PROVIDER_ENABLE_FORBIDDEN: ${file}`);
    }
  }
}
export function assertSqlWriteBoundaries(source,file){
  source=source.replaceAll('\r\n','\n');
  const parts=executableParts(source,file);
  for(const part of parts){
    const text=part.unclassified??part.text;
    // PS-08: immutable captured rows have no UPDATE/DELETE producer, even in
    // unchanged physical owners/includes. Literal diagnostic text is not SQL.
    const literals=/(\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$)[\s\S]*?\1|(?:\bE)?'(?:\\.|''|[^'])*'|"(?:""|[^"])*"/g;
    const literalSpans=[...text.matchAll(literals)].map(match=>[match.index,match.index+match[0].length]);
    const captureCode=text.replace(literals,token=>token.startsWith('"')?token.slice(1,-1).replaceAll('""','"'):' ');
    const forbidden=/\b(?:update\s+|delete\s+from\s+)(?:only\s+)?\(?\s*(?:private\s*\.\s*)?bpay_next_run_(?:line|work)\b/i;
    // Direct EXECUTE literals are executable SQL; diagnostic/catalog literals
    // remain inert. Computed/unclassified SQL is denied by the existing gate.
    const executed=[...text.matchAll(/\bexecute\s+(?:(?:pg_catalog\s*\.\s*)?format\s*\(\s*)?((\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$)[\s\S]*?\2|'(?:''|[^'])*')/gi)]
      .filter(match=>!literalSpans.some(([start,end])=>match.index>=start&&match.index<end))
      .map(match=>match[2]?match[1].slice(match[2].length,-match[2].length):match[1].slice(1,-1).replaceAll("''","'"));
    if(forbidden.test(captureCode)||executed.some(sql=>forbidden.test(sql.replace(/"([a-z_]+)"/gi,'$1'))))
      throw Error(`BPAY_NEXT_FROZEN_CAPTURE_MUTATION_FORBIDDEN: ${file}`);
    if(/\b(?:update|insert\s+into|delete\s+from|merge\s+into|truncate(?:\s+table)?)\s+(?:only\s+)?private\s*\.\s*bpay_next_module_control\b/i.test(text)){
      const transition=part.signature==='public.bpay_next_owner_transition_v1(jsonb)';
      const bootstrap=file===FROZEN_FILES[0]&&/^insert\s+into\s+private\.bpay_next_module_control\s*\(id\)\s*values\s*\(1\)\s*;$/i.test(text.trim());
      if(!transition&&!bootstrap)throw Error(`BPAY_NEXT_MODULE_WRITER_FORBIDDEN: ${file}`);
    }
    if(/\b(?:insert\s+into|update|delete\s+from|merge\s+into|truncate(?:\s+table)?)\s+(?:only\s+)?private\s*\.\s*bpay_next_test_database_marker\b/i.test(text))throw Error(`BPAY_NEXT_TEST_MARKER_WRITER_FORBIDDEN: ${file}`);
    if(/'SIMULATED_TEST'/i.test(text))throw Error(`BPAY_NEXT_SIMULATION_VOCABULARY_FORBIDDEN: ${file}`);
    assertProviderDefault(text,file);
    if(/^(?:private|public)\.bpay_next_/.test(part.signature)||/\bbpay_next_/i.test(part.text))assertNoProcessingAdmission(text,file,true);
  }
}

export function executableParts(source,file){
  source=source.replaceAll('\r\n','\n');
  return scanManagedSql(source).statements.flatMap(row=>{
    const text=source.slice(row.start,row.end),signature=routineDefinitions(text,file)[0]??`outer:${file}`;
    const isRoutine=signature!==`outer:${file}`,isDo=row.tokens[0]==='DO';
    if(!isRoutine&&!isDo)return [{signature,text:sqlWithoutComments(text)}];
    let i=0,lastWord='';
    while(i<text.length){
      if(text.startsWith('--',i)){while(i<text.length&&text[i]!=='\n')i++;continue;}
      if(text.startsWith('/*',i)){i+=2;let depth=1;while(i<text.length&&depth){if(text.startsWith('/*',i)){depth++;i+=2;}else if(text.startsWith('*/',i)){depth--;i+=2;}else i++;}continue;}
      const tag=text[i]==='$'?text.slice(i).match(/^\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$/)?.[0]:null;
      if(tag){const start=i+tag.length,end=text.indexOf(tag,start);if(end<0)throw Error('BPAY_NEXT_SOURCE_UNCLOSED_BODY');if(lastWord==='AS'||isDo)return [{signature,text:sqlWithoutComments(text.slice(start,end))}];i=end+tag.length;continue;}
      if(text[i]==="'"||text[i]==='"'){
        const quote=text[i],start=i++,escape=quote==="'"&&lastWord==='E';let closed=false;
        while(i<text.length){if(escape&&text[i]==='\\'){i+=2;continue;}if(text[i]===quote){if(text[i+1]===quote){i+=2;continue;}i++;closed=true;break;}i++;}
        if(!closed)throw Error('BPAY_NEXT_SOURCE_UNCLOSED_QUOTE');
        if(quote==="'"&&(lastWord==='AS'||isDo))return [{signature,text:'',unclassified:text.slice(start,i)}];
        continue;
      }
      const word=text.slice(i).match(/^[A-Za-z_][A-Za-z0-9_$]*/)?.[0];
      if(word){lastWord=word.toUpperCase();i+=word.length;}else i++;
    }
    return [{signature,text:'',unclassified:text}];
  });
}
export function assertExecutableSourceBoundaries(source,file){
  // Only permanent Worker/config/release-script inputs are admitted here.
  // Examples, source tests and this gate's own forbidden tokens are excluded.
  const code=source.replace(/\/\*[\s\S]*?\*\//g,'').replace(/^\s*\/\/.*$/gm,'');
  if(file==='broker/src/index.js'||/\/bpay-next-[^/]+\.[cm]?js$/.test(file))assertNoProcessingAdmission(source,file);
  if(/['"]SIMULATED_TEST['"]/.test(code))throw Error(`BPAY_NEXT_SIMULATION_VOCABULARY_FORBIDDEN: ${file}`);
  for(const name of ['bpay_next_module_control','bpay_next_test_database_marker']){
    if(new RegExp(`(?:update|insert\\s+into|delete\\s+from|merge\\s+into|truncate(?:\\s+table)?)\\s+(?:private\\s*\\.\\s*)?${name}\\b`,'i').test(code)
      ||new RegExp(`\\.from\\(\\s*['"](?:private\\.)?${name}['"]\\s*\\)[\\s\\S]{0,160}?\\.(?:insert|update|upsert|delete)\\s*\\(`,'i').test(code))throw Error(`BPAY_NEXT_EXECUTABLE_CONTROL_WRITER_FORBIDDEN: ${file}`);
  }
  if(/bpay_next_rail_capability/.test(code)&&/(?:enabled\s*[:=]\s*true|\.\s*(?:update|upsert|insert)\s*\()/i.test(code))throw Error(`BPAY_NEXT_EXECUTABLE_PROVIDER_DEFAULT_UNCLASSIFIED: ${file}`);
}
// PF05: mechanisms are identified at executable declarations/calls/fields,
// never by free prose, ordinary money rates, Source C1 or bounded scheduling.
export function assertNoProcessingAdmission(source,file,sql=false){
  const code=sql?sqlWithoutComments(source):source.replace(/\/\*[\s\S]*?\*\//g,'').replace(/^\s*\/\/.*$/gm,'');
  const mechanism=/(?:^|_)(?:admission_(?:tariff|allowance|budget|price|pricing|cost|credits?)|(?:processing|step)_(?:tariff|allowance|budget|price|pricing|cost|fee)|(?:price|quote|charge)_processing|processing_(?:credit_)?allowance|processing_(?:fee_)?(?:ancestor|payer|graph)|fee_payer_(?:ancestor|graph)|(?:g01|c1)_(?:admission|tariff|allowance|budget|price|pricing|fee|ancestor|graph))(?:_|$)/;
  const classify=value=>{
    const name=value.replace(/([a-z0-9])([A-Z])/g,'$1_$2').toLowerCase();
    // Explicit physical bounds do not price or grant processing admission.
    if(/(?:^|_)(?:processing|step)_budget_(?:ms|bytes|rows|pages|seconds)$/.test(name))return;
    if(/^bpay_next_(?:g01|c1)(?:_v\d+)?$/.test(name)||mechanism.test(name))throw Error('BPAY_NEXT_PROCESSING_ADMISSION_FORBIDDEN: '+file+' ('+value+')');
  };
  // Identifier tokens express declared objects, callees and request fields.
  // Quoted literals count only in actual field/access or RPC roles and only
  // when the whole value is an identifier. Diagnostic text stays permitted.
  const masked=code.replace(/'((?:[^']|'')*)'|"((?:[^"\\]|\\.)*)"/g,(_whole,single,double,offset)=>{
    const value=single??double,before=code.slice(Math.max(0,offset-2048),offset),after=code.slice(offset+_whole.length);
    const bracket=before.match(/(?:([A-Za-z_$][\w$]*)|[)\]]|\?\.)\s*\[\s*$/);
    const access=bracket&&!['return','throw','yield','await','case','new','else','in','of'].includes(bracket[1]?.toLowerCase())&&!(sql&&bracket[1]?.toLowerCase()==='array');
    const field=/(?:->>|->|\.)\s*$/.test(before)||(sql&&/\?\s*$/.test(before))||access||(/[{,]\s*$/.test(before)&&/^\s*:/.test(after))
      ||/\b(?:get|has|hasOwn|hasOwnProperty)\s*\([^;\n]*$/.test(before);
    const rpc=/\b(?:rpc|sqlJson|callRpc)\s*\([^;\n]*$/.test(before)
      ||/\b(?:RPCS|ENDPOINTS)\s*=\s*(?:Object\.freeze\s*\(\s*)?\{[^}]*$/i.test(before);
    if((field||rpc)&&/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(value))classify(value);
    return ' ';
  });
  const roles=[
    /\b([A-Za-z_][A-Za-z0-9_]*)\s*\(/g, // declared/called routine or constructor
    /(?:\.|\?\.)\s*([A-Za-z_][A-Za-z0-9_]*)/g, // actual field/RPC access
    /\b([A-Za-z_][A-Za-z0-9_]*)\s*:/g, // request property or local assignment
    /\b([A-Za-z_][A-Za-z0-9_]*)\s+(?:text|jsonb|numeric|integer|bigint|boolean)\b/gi,
    /\b(?:table|view|function|procedure|from|join|into)\s+(?:if\s+not\s+exists\s+)?(?:[A-Za-z_]\w*\s*\.\s*)?([A-Za-z_]\w*)/gi
  ];
  for(const role of roles)for(const match of masked.matchAll(role))classify(match[1]);
  for(const match of code.matchAll(/\/rest\/v1\/rpc\/([A-Za-z_][A-Za-z0-9_]*)/g))classify(match[1]);
  // Exact NEXT abandoned owner invocation is refused; real Source C1 routines have
  // their own qualified names and are not this standalone processing owner.
  if(/\bbpay_next_(?:g01|c1)(?:_v\d+)?\s*\(/i.test(masked))throw Error('BPAY_NEXT_ABANDONED_PROCESSING_OWNER: '+file);
}
function operatorTokens(tokens){
  const out=[];
  for(let i=0;i<tokens.length;i++){
    const c=tokens[i],n=tokens[i+1];
    if((c==='-'||c==='#')&&n==='>'){let op=c+'>';i++;if(tokens[i+1]==='>'){op+='>';i++;}out.push(op);}
    else if((c==='|'&&n==='|')||(c===':'&&n===':')){out.push(c+n);i++;}
    else out.push(c);
  }
  return out;
}
function fixedQueryLiterals(source,file){
  const result=new Map(),assignments=new Map();
  for(const m of source.matchAll(/\b([a-z_][a-z0-9_]*)\s*:=\s*(\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$)([\s\S]*?)\2\s*;/gi)){
    const name=m[1].toLowerCase(); assignments.set(name,assignments.has(name)||!/^\s*select\b/i.test(m[3])?null:[m[3]]);
  }
  for(const [name,literals] of assignments){
    if(!literals)continue;
    const rows=scanManagedSql(source).statements;
    let valid=true;
    for(const row of rows){
      const raw=source.slice(row.start,row.end),at=raw.search(new RegExp(`\\b${name}\\s*:=`,'i'));
      if(at<0)continue;
      const assignment=raw.slice(at);
      if(new RegExp(`^${name}\\s*:=\\s*\\$(?:[A-Za-z_][A-Za-z0-9_]*)?\\$`,'i').test(assignment))continue;
      const suffix=assignment.match(new RegExp(`^${name}\\s*:=\\s*${name}\\s*\\|\\|\\s*'((?:[^']|'')*)'\\s*;\\s*$`,'i'));
      if(!suffix){valid=false;break;}literals.push(suffix[1].replaceAll("''","'"));
    }
    // Reject other assignment mechanisms; query parameters stay bound in USING.
    if(new RegExp(`\\b(?:into|fetch[^;]*into)\\s+(?:strict\\s+)?${name}\\b|\\b${name}\\s*=(?!=)`,'i').test(source))valid=false;
    if(valid){assertSpecialConstructs(literals.join(''),file);result.set(name,literals);}
  }
  return result;
}
function closedHistoryTemplates(part,file){
  if(part.signature!=='public.bpay_next_payment_history_page_v1(jsonb)')return false;
  const literal=part.text.match(/v_sections\s+constant\s+jsonb\s*:=\s*(\$\w*\$)([\s\S]*?)\1\s*(?:::jsonb\s*)?;/i);
  if(!literal)return false;
  let sections;try{sections=JSON.parse(literal[2]);}catch{return false;}
  if(!sections||Array.isArray(sections)||!Object.keys(sections).length)return false;
  for(const definition of Object.values(sections)){
    if(!/^[a-z_][a-z0-9_]*$/.test(definition.table)||!/^[a-z_][a-z0-9_]*$/.test(definition.fk)
      ||!/^([a-z_][a-z0-9_]*)(,[a-z_][a-z0-9_]*)*$/.test(definition.fields)
      ||!Array.isArray(definition.keys)||!definition.keys.length||definition.keys.some(key=>!Array.isArray(key)||key.length!==2
        ||!/^[a-z_][a-z0-9_]*$/.test(key[0])||!['uuid','bigint','integer','date','timestamptz','text'].includes(key[1])))return false;
  }
  if([...part.text.matchAll(/\bv_sections\s*:=/gi)].length||[...part.text.matchAll(/\bv_def\s*:=/gi)].length!==1
    ||!/v_def\s*:=\s*v_sections\s*->\s*v_section\s*;/i.test(part.text))return false;
  const strings=[...part.text.matchAll(/\bformat\(\s*'((?:[^']|'')*)'/gi)].map(m=>m[1].replaceAll("''","'"));
  if(!strings.length)return false;
  for(const template of strings)assertSpecialConstructs(template.replaceAll('%I','safe_identifier').replaceAll('%L',"'safe_literal'").replaceAll('%s','0'),file);
  // The established history owner has finite SQL-fragment producers. Compare
  // every producer (including the order text) with those exact reviewed shapes;
  // a new producer requires classification, never a mere helper-presence waiver.
  const sqlVariables=new Set(['V_QUERY','V_WHERE','V_FIELDS','V_ORDER','V_TUPLE','V_KEY_SQL','V_AFTER_SQL','V_UPPER_SQL']);
  const producerRows=text=>scanManagedSql(text).statements.filter(row=>{
    const tokens=operatorTokens(row.tokens);
    return tokens.some((token,i)=>(sqlVariables.has(token)||token==='V_TEXT')&&((tokens[i+1]===':'&&tokens[i+2]==='=')||tokens[i+1]==='='))
      ||tokens.some((token,i)=>token==='INTO'&&(sqlVariables.has(tokens[i+1+(tokens[i+1]==='STRICT'?1:0)])||tokens[i+1+(tokens[i+1]==='STRICT'?1:0)]==='V_TEXT'));
  }).map(row=>sqlWithoutComments(text.slice(row.start,row.end)).trim());
  const original=executableParts(git(['show',`${FOUNDATION_RECONCILED}:supabase/repeatable/06102026_1206_banking_pay_payment_history_page_v1.sql`]),file).find(row=>row.signature===part.signature);
  if(!original||JSON.stringify(producerRows(part.text))!==JSON.stringify(producerRows(original.text)))return false;
  const allowed=new Set([...sqlVariables,'V_DEF','V_KEY','V_COLUMN','V_I','FORMAT','QUOTE_LITERAL','<QUOTED>']);
  for(const row of scanManagedSql(part.text).statements){
    const tokens=operatorTokens(row.tokens),at=tokens.findIndex((token,i)=>sqlVariables.has(token)&&tokens[i+1]===':'&&tokens[i+2]==='=');
    if(at<0)continue;
    if(tokens.slice(at+3).some(token=>/^[A-Z_][A-Z0-9_]*$/.test(token)&&!allowed.has(token)))return false;
  }
  return /select\s+string_agg\(format\('t\.%I desc',value->>0\),',' order by ord\)\s+into v_text\s+from jsonb_array_elements\(v_def->'keys'\) with ordinality as k\(value,ord\);/i.test(part.text)
    &&/for v_key in select value from jsonb_array_elements\(v_def->'keys'\) loop/i.test(part.text);
}
// WB09 fixed kind/sort fragments only. Caller scalars may select a literal
// branch but never enter generated SQL; they are exclusively USING operands.
function closedWorkbenchTemplates(part,file){
  if(part.signature!=='private.bpay_next_workbench_kind_page_v1(text,jsonb,jsonb,integer)')return false;
  const variables=new Set(['V_SQL','V_WHERE','V_PROJECTION','V_TUPLE','V_SEEK','V_KEY','V_ORDER','V_OP','V_DESC']);
  const allowed=new Set([...variables,'CASE','WHEN','THEN','ELSE','END','IN','AND','LIKE','P_KIND','V_SORT','V_SECTION','V_DESC','V_OP','REPLACE','V_ANCHOR_PAGE','OR']);
  let producers=0;
  const declaration=part.text.match(/\bdeclare\b([\s\S]*?)\bbegin\b/i);
  if(!declaration)return false;
  // WB15 anchor is a closed literal BOOLEAN, never a SQL fragment or caller value.
  if(/\bv_anchor_page\b/i.test(part.text)&&((declaration[1].match(/\bv_anchor_page\b/gi)??[]).length!==1||! /\bv_anchor_page\s+boolean\s*:=\s*false\s*;/i.test(declaration[1])))return false;
  const declared=new Set();
  for(const row of scanManagedSql(declaration[1]).statements){
    const tokens=operatorTokens(row.tokens);
    if(!variables.has(tokens[0]))continue;
    if(tokens.length!==2||tokens[1]!==(tokens[0]==='V_DESC'?'BOOLEAN':'TEXT')||declared.has(tokens[0]))return false;
    declared.add(tokens[0]);
  }
  if(declared.size!==variables.size)return false;
  for(const row of scanManagedSql(part.text).statements){
    const tokens=operatorTokens(row.tokens);
    if(tokens.some((token,n)=>token==='INTO'&&(variables.has(tokens[n+1+(tokens[n+1]==='STRICT'?1:0)])||tokens[n+1+(tokens[n+1]==='STRICT'?1:0)]==='V_ANCHOR_PAGE')))return false;
    for(let n=0;n<tokens.length-2;n++)if(tokens[n]==='V_ANCHOR_PAGE'&&((tokens[n+1]===':'&&tokens[n+2]==='=')||tokens[n+1]==='=')){
      const rest=tokens.slice(n+(tokens[n+1]===':'?3:2));if(rest.length!==1||rest[0]!=='TRUE')return false;
    }
    for(let n=0;n<tokens.length-2;n++){
      if(!variables.has(tokens[n])||!((tokens[n+1]===':'&&tokens[n+2]==='=')||tokens[n+1]==='='))continue;
      producers++;
      const start=n+(tokens[n+1]===':'?3:2);
      if(tokens.slice(start).some(token=>/^[A-Z_][A-Z0-9_]*$/.test(token)&&!allowed.has(token)))return false;
    }
  }
  // Both quoted suffixes and every dollar-tag projection are executable SQL
  // fragments here; no tag or masked string may hide a forbidden construct.
  for(const match of part.text.matchAll(/(\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$)([\s\S]*?)\1|'((?:''|[^'])*)'/g)){
    const fragment=match[2]??match[3].replaceAll("''","'");
    // One actual maintained TEXT column, bound to its literal relation alias.
    // Other aggregate arguments still require their own explicit type evidence.
    const columns=new Map();
    if(/from private\.bpay_next_workbench_item i where i\.group_key=g\.group_key/i.test(fragment)
      &&/\bclient_display_name text check\(octet_length\(client_display_name\)<=8192\)/i.test(fs.readFileSync(path.join(repoRoot,'supabase/migrations/10102026_1252_banking_pay_next_workbench_summary.sql'),'utf8')))
      columns.set('I . CLIENT_DISPLAY_NAME','TEXT');
    assertSpecialConstructs(fragment,file,new Map(),columns);
  }
  return producers>0&&/for v_row in execute v_sql using v_client_scope,v_channel,v_section,v_candidate,v_client,v_search,v_group,v_item,v_reason,p_after,p_limit\+1 loop/i.test(part.text)
    &&/execute 'select exists\(select 1'\|\|v_sql\|\|v_where/i.test(part.text)
    &&!/[|]{2}\s*(?:p_scope|p_after|p_limit|v_search|v_client|v_candidate|v_group|v_item|v_reason|v_anchor_page)\b/i.test(part.text);
}
function closedWorkbenchExecution(statement){
  const flat=sqlWithoutComments(statement).replace(/\s+/g,' ').trim();
  const at=flat.search(/\bexecute\b/i);if(at<0)return false;
  const execution=flat.slice(at);
  const values='v_client_scope,v_channel,v_section,v_candidate,v_client,v_search,v_group,v_item,v_reason,p_after,p_limit\\+1';
  if(new RegExp('^execute v_sql using '+values+' loop\\b','i').test(execution))return /for v_row in\s*$/i.test(flat.slice(0,at));
  // WB15 native first-visible group seeks use only the closed reader template and typed bindings.
  if(new RegExp("^execute 'select '\\|\\|v_key\\|\\|v_sql\\|\\|v_where\\|\\|' and s\\.presentation_key=\\$7' into p_after using "+values+'\\s*;?$','i').test(execution))return true;
  // WB14 whole exact-group Candidate count uses the same closed maintained
  // WHERE/template/bind list; no caller SQL or alternative relation is admitted.
  if(new RegExp("^execute 'select count\\(distinct i\\.candidate_id\\)'\\|\\|v_sql\\|\\|v_where into v_affected using "+values+'\\s*;?$','i').test(execution))return true;
  return new RegExp("^execute 'select exists\\(select 1'\\|\\|v_sql\\|\\|v_where(?:\\|\\|' and '\\|\\|v_tuple\\|\\|'='\\|\\|v_seek)?\\|\\|'\\)' into v_(?:affected|member) using "+values+'\\s*;?$','i').test(execution);
}
// Return exact same-signature/declared-type/statement occurrences. Baseline
// allowances are consumed, never borrowed from another routine or whole file.
// DR05 replaces exactly one catalogue-proved predecessor CHECK. The literal
// body pins the relation, predicate, cardinality assertion and escaped %I;
// no other catalogue-derived SQL or dynamic body receives admission.
function closedExpiryWorkerShapeUpgrade(part,file){
  return file==='supabase/migrations/11102026_0559_banking_pay_next_worker_reprepare.sql'
    && part.signature===`outer:${file}` && part.text.trim()==="declare v_constraint text;v_count integer;\nbegin\n  select min(conname),count(*) into v_constraint,v_count from pg_catalog.pg_constraint\n    where conrelid='private.bpay_next_preparation_expiry_worker'::regclass and contype='c'\n      and pg_catalog.regexp_replace(pg_catalog.pg_get_expr(conbin,conrelid),'\\s','','g')=\n        pg_catalog.regexp_replace($shape$(((run_worker_id IS NULL) AND (original_worker_status IS NULL) AND (preparation_revision IS NULL) AND (selection_revision IS NULL) AND (draft_state_id IS NULL) AND (expected_work_count = 0) AND (expected_case_hold_count = 0) AND (original_review_issue_code IS NULL) AND (original_review_issue_work_id IS NULL)) OR ((run_worker_id IS NOT NULL) AND (original_worker_status = ANY (ARRAY['PREPARING'::text, 'REVIEW'::text, 'READY'::text])) AND (preparation_revision IS NOT NULL) AND (selection_revision IS NOT NULL)))$shape$,'\\s','','g');\n  if v_count<>1 then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_ORIGINAL_WORKER_SHAPE_UNRECOGNISED';end if;\n  execute pg_catalog.format('alter table private.bpay_next_preparation_expiry_worker drop constraint %I',v_constraint);\nend";
}
// EX-08: one immutable migration's exact private receipt ACL/guards only.
// Literal-body admission cannot escape to a caller table, role or financial write.
function closedPayMethodReceiptAcl(part,file){
 return file==="supabase/migrations/11102026_1624_banking_pay_next_pay_method_change_receipt.sql" && part.signature==='outer:'+file
  && part.text.trim()==="declare v_name text;\nbegin\n foreach v_name in array array['bpay_next_pay_method_change_receipt','bpay_next_pay_method_change_member'] loop\n  execute format('alter table private.%I owner to postgres',v_name);\n  execute format('alter table private.%I enable row level security',v_name);\n  execute format('alter table private.%I force row level security',v_name);\n  execute format('revoke all on private.%I from public,anon,authenticated,service_role',v_name);\n  execute format('create policy cloudtms_miget_service_owner_all on private.%I to postgres using(true) with check(true)',v_name);\n  execute format('create trigger %I before update or delete on private.%I for each row execute function private.bpay_next_protected_source_receipt_immutable_v1()',v_name||'_immutable',v_name);\n  execute format('create trigger %I before truncate on private.%I for each statement execute function private.bpay_next_protected_source_receipt_immutable_v1()',v_name||'_no_truncate',v_name);\n end loop;\nend";
}
export function specialConstructViolations(source,file,columnTypes=new Map()){
  const result=[];
  for(const part of executableParts(source,file)){
    if(part.unclassified){result.push({hazard:'BODY_UNCLASSIFIED',key:JSON.stringify([part.signature,'BODY_UNCLASSIFIED',part.unclassified])});continue;}
    // WB22: three direct native scalar aggregates in the fixed local-choice
    // row reader. Verify both actual schema declarations and literal aliases;
    // this classifies types, without allowing an unclassified SQL construct.
    const nativeColumns=new Map(columnTypes);
    const expiryShapeUpgrade=closedExpiryWorkerShapeUpgrade(part,file);
    const payMethodReceiptAcl=closedPayMethodReceiptAcl(part,file);
    if(expiryShapeUpgrade)nativeColumns.set('CONNAME','TEXT');
    if(part.signature==='private.bpay_next_selection_memory_row_v1(text,jsonb,jsonb,uuid)'){
      const position=fs.readFileSync(path.join(repoRoot,'supabase/migrations/03102026_1324_banking_pay_next_core.sql'),'utf8').match(/create table private\.bpay_next_position\s*\(([\s\S]*?)\n\);/i)?.[1]??'';
      const snooze=fs.readFileSync(path.join(repoRoot,'supabase/migrations/10102026_1100_banking_pay_next_snooze.sql'),'utf8').match(/create table private\.bpay_next_snooze\s*\(([\s\S]*?)\n\);/i)?.[1]??'';
      if(/\bupdated_at_utc timestamptz not null\b/i.test(position)&&/\bfrom private\.bpay_next_position pos where pos\.work_id=w\.id\b/i.test(part.text))nativeColumns.set('POS . UPDATED_AT_UTC','TIMESTAMPTZ');
      if(/\bcontrol_sequence bigint not null\b/i.test(snooze)&&/\bfrom private\.bpay_next_snooze sn where sn\.(?:work_id=w\.id|case_component_id=cc\.id)\b/i.test(part.text))nativeColumns.set('SN . CONTROL_SEQUENCE','BIGINT');
    }
    // WB26's immutable Case-only supplement uses the same native BIGINT control field.
    // This is type evidence for the installed predecessor, not an aggregate allowance.
    if(part.signature==='private.bpay_next_selection_memory_leaves_v1(jsonb)'){
      const snooze=fs.readFileSync(path.join(repoRoot,'supabase/migrations/10102026_1100_banking_pay_next_snooze.sql'),'utf8').match(/create table private\.bpay_next_snooze\s*\(([\s\S]*?)\n\);/i)?.[1]??'';
      if(/\bcontrol_sequence bigint not null\b/i.test(snooze)&&/from private\.bpay_next_snooze sn where sn\.case_component_id=cc\.id\b/i.test(part.text))nativeColumns.set('SN . CONTROL_SEQUENCE','BIGINT');
    }
    if(part.signature==='private.bpay_next_choice_inputs_page_v1(uuid,bigint,integer,text)'){
      const members=fs.readFileSync(path.join(repoRoot,'supabase/migrations/10102026_1832_banking_pay_next_accepted_choice_membership_v1.sql'),'utf8').match(/create table private\.bpay_next_run_choice_member\s*\(([\s\S]*?)\n\);/i)?.[1]??'';
      if(/\bmember_no bigint not null\b/i.test(members)&&/from private\.bpay_next_run_choice_member member where member\.run_id=p_run_id/i.test(part.text)
        &&/order by member\.member_no limit p_limit\) page/i.test(part.text))nativeColumns.set('MEMBER_NO','BIGINT');
    }
    const rows=scanManagedSql(part.text).statements;
    const fixedQueries=fixedQueryLiterals(part.text,file);
    const historyTemplates=closedHistoryTemplates(part,file);
    const workbenchTemplates=closedWorkbenchTemplates(part,file);
    const all=rows.flatMap(row=>operatorTokens(row.tokens)),declarations=new Map(),types=new Map();
    const joined=all.join(' ');
    for(const match of joined.matchAll(/\b([A-Z_][A-Z0-9_]*)\s+((?:[A-Z_][A-Z0-9_]*\s*\.\s*)?[A-Z_][A-Z0-9_]*)\s*%\s*ROWTYPE\b/g))declarations.set(match[1],match[2]);
    const declareAt=all.indexOf('DECLARE'),beginAt=all.indexOf('BEGIN',declareAt+1);
    const localDeclarations=declareAt>=0&&beginAt>declareAt?all.slice(declareAt+1,beginAt).join(' '):'';
    for(const match of localDeclarations.matchAll(/\b([A-Z_][A-Z0-9_]*)\s+(UUID|TEXT|NUMERIC|BIGINT|INTEGER|SMALLINT|DATE|TIMESTAMPTZ)\b/g))types.set(match[1],match[2]);
    for(const row of rows){
      const t=operatorTokens(row.tokens),text=t.join(' '),hazards=new Set();
      if(t.includes('BPAY_NEXT_QUOTED_IDENTIFIER_UNCLASSIFIED'))hazards.add('QUOTED_IDENTIFIER_UNCLASSIFIED');
      const statement=part.text.slice(row.start,row.end);
      // The current migrations' owner-only policy uses one escaped identifier
      // from CURRENT_USER, a fixed private relation and fixed true predicates.
      // It cannot contain a SELECT, financial/control write or SQL special form.
      const ownerPolicy=/^\s*(?:begin\s+)?execute\s+(?:pg_catalog\.)?format\(\s*'create policy [a-z_][a-z0-9_]* on private\.[a-z_][a-z0-9_]* for all to %I using \(true\) with check \(true\)'\s*,\s*current_user\s*\)\s*;\s*$/i.test(statement);
      const dynamic=t.some((token,at)=>token==='EXECUTE'&&!['FUNCTION','PROCEDURE','ON'].includes(t[at+1]));
      const queryExecution=t.some((token,at)=>token==='EXECUTE'&&fixedQueries.has(t[at+1]?.toLowerCase())&&t[at+2]==='USING');
      const queryLiteral=t[1]===':'&&t[2]==='='&&fixedQueries.has(t[0]?.toLowerCase())&&t.includes('<BODY>');
      const schemaFormat=/execute\s+pg_catalog\.format\(\s*'(?:alter table private\.%I (?:owner to postgres|enable row level security)|revoke all on private\.%I from public,anon,authenticated,service_role|create trigger %I before (?:update or delete|truncate) on private\.%I for each (?:row|statement) execute function private\.[a-z_][a-z0-9_]*\(\))'\s*,\s*v_table(?:\s*\|\|\s*'_(?:immutable|no_truncate)'\s*,\s*v_table)?\s*\)\s*;\s*$/i.test(statement)
        &&[...part.text.matchAll(/foreach\s+v_table\s+in\s+array\s+array\[([^\]]*)\]\s+loop/gi)].length>0
        &&[...part.text.matchAll(/foreach\s+v_table\s+in\s+array\s+array\[([^\]]*)\]\s+loop/gi)].every(m=>/^\s*'[a-z_][a-z0-9_]*'(?:\s*,\s*'[a-z_][a-z0-9_]*')*\s*$/i.test(m[1]))
        &&[...part.text.matchAll(/foreach\s+v_table\s+in\s+array\b/gi)].length===[...part.text.matchAll(/foreach\s+v_table\s+in\s+array\s+array\[([^\]]*)\]\s+loop/gi)].length
        &&!/\bv_table\s*(?::=|=)|\binto\s+(?:strict\s+)?v_table\b/i.test(part.text);
      const historyExecution=historyTemplates&&(/execute\s+format\(\s*'select jsonb_build_array\(%s\) from private\.%I t where %s order by %s limit 1'\s*,\s*v_key_sql\s*,\s*v_def->>'table'\s*,\s*v_where\s*,\s*v_text\s*\)\s+into\s+v_upper\s+using/i.test(statement)
        ||/for\s+v_item\s+in\s+execute\s+v_query\s+using/i.test(statement));
      const historyConstant=historyTemplates&&/^\s*declare\s+v_sections\s+constant\s+jsonb\s*:=/i.test(statement);
      // A closed catalog predecessor comparison consumes a dollar literal as
      // metadata only; it is not an EXECUTE producer or a SQL body.
      const catalogAssertion=!dynamic&&t.filter(token=>token==='<BODY>').length===1&&/^\s*(?:begin\s+)?if\s+\(select\s+pg_catalog\.pg_get_constraintdef\(c\.oid\)\s+from\s+pg_catalog\.pg_constraint\s+c\s+where\s+c\.conrelid='private\.[a-z_][a-z0-9_]*'::pg_catalog\.regclass\s+and\s+c\.conname='[a-z_][a-z0-9_]*'\)\s+is\s+distinct\s+from\s+\$([a-z_][a-z0-9_]*)\$[\s\S]*?\$\1\$\s+then\s+raise\s+exception\s+using\s+errcode='23514',\s*message='BPAY_NEXT_[A-Z0-9_]+';\s*$/i.test(statement);
      const workbenchExecution=workbenchTemplates&&t.filter(token=>token==='EXECUTE').length===1&&closedWorkbenchExecution(statement);
      const workbenchProjection=workbenchTemplates&&['V_PROJECTION','V_WHERE'].some(token=>t.includes(token))&&t.includes('<BODY>');
      if((dynamic&&!ownerPolicy&&!schemaFormat&&!queryExecution&&!historyExecution&&!workbenchExecution&&!expiryShapeUpgrade&&!payMethodReceiptAcl)||(t.includes('<BODY>')&&!queryLiteral&&!historyConstant&&!catalogAssertion&&!workbenchProjection&&!expiryShapeUpgrade&&!payMethodReceiptAcl))hazards.add('DYNAMIC_SQL_UNCLASSIFIED');
      if(/\bPG_CATALOG \. (?:COALESCE|NULLIF|LEAST|GREATEST) \(/.test(text))hazards.add('SPECIAL_QUALIFIER');
      const intoAt=t.indexOf('INTO'),selectAt=intoAt>=0?t.lastIndexOf('SELECT',intoAt):t.indexOf('SELECT');
      if(selectAt>=0&&intoAt>selectAt){
        const targetAt=intoAt+(t[intoAt+1]==='STRICT'?2:1),target=t[targetAt];
        if(declarations.has(target)&&t[targetAt+1]!=='.'){
          const projection=t.slice(selectAt+1,intoAt);
          const expected=declarations.get(target).split(/\s+/);
          const binding=t.findIndex((token,at)=>at>intoAt&&['FROM','JOIN'].includes(token)
            &&t.slice(at+1,at+1+expected.length).join(' ')===expected.join(' ')
            &&t[at+1+expected.length+(t[at+1+expected.length]==='AS'?1:0)]===projection[0]);
          if(projection.length!==3||!/^\w+$/.test(projection[0])||projection[1]!=='.'||projection[2]!=='*'
             ||binding<0)hazards.add(`ROWTYPE_SELECT:${target}:${declarations.get(target)}`);
        }
      }
      const frames=[{concat:false,extract:false}];
      for(let i=0;i<t.length;i++){
        const token=t[i];
        if(token==='('){frames.push({concat:false,extract:false});continue;}
        if(token===')'){if(frames.length>1)frames.pop();continue;}
        const frame=frames.at(-1);
        if([',','=','AND','OR','THEN','ELSE','WHEN','FROM','WHERE','SET','RETURN','IF','LOOP','IS','IN'].includes(token)){frame.concat=false;frame.extract=false;continue;}
        if(token==='||')frame.concat=true;
        if(['->','->>','#>','#>>'].includes(token))frame.extract=true;
        if(frame.concat&&frame.extract)hazards.add('JSON_CONCAT');
        if(['MIN','MAX'].includes(token)&&t[i+1]==='('){
          let end=i+2,depth=1;
          for(;end<t.length;end++){if(t[end]==='(')depth++;if(t[end]===')'&&--depth===0)break;}
          const argument=t.slice(i+2,end);
          // Explicit scalar casts are finite typed evidence. An unresolved new
          // aggregate is refused for source review instead of guessing *_id.
          const cast=argument.indexOf('::'),type=cast>=0?argument.slice(cast+1).join(' ')
            :argument.length===1?(types.get(argument[0])??(/^\d+$/.test(argument[0])?'NUMERIC':null)):null;
          if(type==='UUID')hazards.add('UUID_AGGREGATE');
          else if(!['TEXT','NUMERIC','BIGINT','INTEGER','SMALLINT','DATE','TIMESTAMPTZ'].includes(type??nativeColumns.get(argument.join(' '))))hazards.add('AGGREGATE_TYPE_UNCLASSIFIED');
          else if(t[end+1]==='::'&&t[end+2]==='UUID')hazards.add('UUID_CARDINALITY_UNCLASSIFIED');
        }
      }
      const raw=part.text.slice(row.start,row.end),at=selectAt>=0?raw.search(/\bselect\b/i):-1;
      for(const hazard of hazards)result.push({hazard,key:JSON.stringify([part.signature,hazard,(hazard.startsWith('ROWTYPE_SELECT:')&&at>=0?raw.slice(at):raw).replaceAll('\r\n','\n').trim()])});
    }
  }
  return result;
}
export function assertSpecialConstructs(source,file,baseline=new Map(),columnTypes=new Map()){
  const remaining=new Map(baseline),failures=[];
  for(const row of specialConstructViolations(source,file,columnTypes)){
    const count=remaining.get(row.key)??0;
    if(!count){failures.push(`BPAY_NEXT_SQL_${row.hazard}: ${file} (${JSON.parse(row.key)[0]})`);continue;}
    remaining.set(row.key,count-1);
  }
  if(failures.length)throw Error(failures.join('\n'));
}
export function assertRpcReload(source,file){
  source=source.replaceAll('\r\n','\n');
  const rows=scanManagedSql(source).statements;
  let lastAuthority=-1,lastReload=-1;
  for(let i=0;i<rows.length;i++){
    const row=rows[i],text=source.slice(row.start,row.end);
    if(routineDefinitions(text,file).some(identity=>identity.startsWith('public.'))||['GRANT','REVOKE'].includes(row.tokens[0]))lastAuthority=i;
    if(row.tokens[0]==='NOTIFY'&&/^notify\s+pgrst\s*,\s*'reload schema'\s*;$/i.test(sqlWithoutComments(text).trim()))lastReload=i;
  }
  if(lastAuthority<0||lastReload<=lastAuthority||rows.slice(lastReload+1).some(row=>row.tokens.join(' ')!=='COMMIT'))throw Error(`BPAY_NEXT_RPC_RELOAD_MISSING: ${file}`);
}
function frozenAuthorityPolicy(sources){
  const triggers=new Map(),tables=new Set(),callbacks=new Set(),routines=new Set();
  for(const [file,source] of sources){
    for(const identity of routineDefinitions(source,file))routines.add(identity.split('(')[0]);
    const code=sqlWithoutComments(source);
    for(const m of code.matchAll(/create\s+(?:constraint\s+)?trigger\s+(\w+)\s+[\s\S]*?\bon\s+((?:private|public)\s*\.\s*\w+)\b[\s\S]*?execute\s+(?:function|procedure)\s+((?:private|public)\s*\.\s*\w+)\s*\(/gi)){
      const relation=m[2].replace(/\s+/g,'').toLowerCase(),name=m[1].toLowerCase(),callback=m[3].replace(/\s+/g,'').toLowerCase();
      triggers.set(`${relation}:${name}`,callback);callbacks.add(callback);
    }
    for(const m of code.matchAll(/create\s+table\s+(?:if\s+not\s+exists\s+)?((?:private|public)\s*\.\s*\w+)\b/gi))tables.add(m[1].replace(/\s+/g,'').toLowerCase());
  }
  if(triggers.size!==18||tables.size!==45)throw Error('BPAY_NEXT_FROZEN_POLICY_INVENTORY_CHANGED');
  return {triggers,tables,callbacks,routines};
}
export function assertFrozenAuthority(source,file,policy,baseline=''){
  source=source.replaceAll('\r\n','\n');baseline=baseline.replaceAll('\r\n','\n');
  for(const part of executableParts(source,file))for(const row of scanManagedSql(part.text).statements){
    const statement=part.text.slice(row.start,row.end);
    const authorityAt=row.tokens.findIndex(token=>['CREATE','DROP','ALTER','GRANT','REVOKE'].includes(token));
    const original=sqlWithoutComments(statement).toLowerCase();
    const code=authorityAt>=0?row.tokens.slice(authorityAt).join(' ').toLowerCase():original,flat=code.replace(/\s*\.\s*/g,'.');
    let competing=false;
    if(/\bexecute\s+(?:pg_catalog\.)?format\(/i.test(original)){
      const arrays=[...part.text.matchAll(/foreach\s+v_table\s+in\s+array\s+array\[([^\]]*)\]\s+loop/gi)].map(m=>m[1]);
      const names=arrays.flatMap(array=>[...array.matchAll(/'([a-z_][a-z0-9_]*)'/gi)].map(m=>`private.${m[1].toLowerCase()}`));
      if(names.some(name=>policy.tables.has(name))||[...policy.callbacks].some(callback=>flat.includes(callback)))competing=true;
      const fixedPolicy=flat.match(/create policy \w+ on ((?:private|public)\.\w+)/)?.[1];
      if(policy.tables.has(fixedPolicy))competing=true;
    }
    if(/^(?:create|drop|alter)\s+(?:(?:or\s+replace|constraint)\s+)?trigger\b/.test(code)){
      const relation=flat.match(/\bon\s+((?:private|public)\.\w+)/)?.[1],name=code.match(/\btrigger\s+(?:if\s+exists\s+)?(\w+)/)?.[1];
      competing=policy.triggers.has(`${relation}:${name}`)||[...policy.callbacks].some(callback=>flat.includes(callback));
      // F05/B1.4 explicitly require these four immutable capture attachments,
      // reusing the unchanged generic immutable callback. They replace none of
      // the original eighteen guard bindings; exact shape is required here.
      const capture=flat.match(/^create trigger (bpay_next_run_(?:work|line)_frozen_(?:immutable|no_truncate)_v1) before (update or delete|truncate) on (private\.bpay_next_run_(?:work|line)) for each (row|statement) execute function private\.bpay_next_effect_immutable_v1\s*\(\s*\)\s*;?\s*$/);
      if(capture){const [,,events,relation,level]=capture,kind=relation.split('_').at(-1),tail=events==='truncate'?'no_truncate':'immutable';
        if(capture[1]===`bpay_next_run_${kind}_frozen_${tail}_v1`&&level===(events==='truncate'?'statement':'row'))competing=false;
      }
    }
    // Rule-B snapshot's exact two additive guards reuse the unchanged callback.
    if(/^create trigger bpay_next_run_work_query_snapshot_immutable before update or delete on private\.bpay_next_run_work_query_snapshot for each row execute function private\.bpay_next_effect_immutable_v1\s*\(\s*\)\s*;?\s*$/.test(flat)
      ||/^create trigger bpay_next_run_work_query_snapshot_no_truncate before truncate on private\.bpay_next_run_work_query_snapshot for each statement execute function private\.bpay_next_effect_immutable_v1\s*\(\s*\)\s*;?\s*$/.test(flat))competing=false;
    // FC14 exact new immutable hold-transition attachments, unchanged generic callback.
    if(/^create trigger bpay_next_case_hold_transition_immutable_v1 before update or delete on private\.bpay_next_case_hold_transition for each row execute function private\.bpay_next_effect_immutable_v1\s*\(\s*\)\s*;?\s*$/.test(flat)
      ||/^create trigger bpay_next_case_hold_transition_truncate_v1 before truncate on private\.bpay_next_case_hold_transition for each statement execute function private\.bpay_next_effect_immutable_v1\s*\(\s*\)\s*;?\s*$/.test(flat))competing=false;
    // FC10 only its two new private audit attachments; no existing guard edit.
    if(/^create trigger bpay_next_case_pause_(request|outcome)_immutable_v1 before update or delete on private\.bpay_next_case_pause_\1 for each row execute function private\.bpay_next_effect_immutable_v1\s*\(\s*\)\s*;?\s*$/.test(flat)
      ||/^create trigger bpay_next_case_pause_(request|outcome)_truncate_v1 before truncate on private\.bpay_next_case_pause_\1 for each statement execute function private\.bpay_next_effect_immutable_v1\s*\(\s*\)\s*;?\s*$/.test(flat))competing=false;
    // FC09 adds two immutable Case receipts using the unchanged callback.
    // Exact new table/name pairs only; no original guard is replaced.
    if(/^create trigger bpay_next_case_edit_immutable_v1 before update or delete on private\.bpay_next_case_edit_request for each row execute function private\.bpay_next_effect_immutable_v1\s*\(\s*\)\s*;?\s*$/.test(flat)
      ||/^create trigger bpay_next_case_period_amendment_immutable_v1 before update or delete on private\.bpay_next_case_period_amendment for each row execute function private\.bpay_next_effect_immutable_v1\s*\(\s*\)\s*;?\s*$/.test(flat))competing=false;
    if(/^alter\s+table\b/.test(code)){
      const relation=flat.match(/^alter\s+table\s+(?:if\s+exists\s+)?(?:only\s+)?((?:private|public)\.\w+)/)?.[1];
      const hasTriggers=[...policy.triggers.keys()].some(key=>key.startsWith(`${relation}:`));
      const triggerChanges=[...code.matchAll(/\b(?:disable|enable(?:\s+always|\s+replica)?)\s+trigger\s+(\w+)/g)];
      competing=hasTriggers&&triggerChanges.some(m=>['all','user'].includes(m[1])||policy.triggers.has(`${relation}:${m[1]}`));
      if((policy.tables.has(relation)||hasTriggers)&&/\b(?:rename\s+to|set\s+schema)\b/.test(code))competing=true;
      if(policy.tables.has(relation)&&/\b(?:owner\s+to|disable\s+row\s+level\s+security|no\s+force\s+row\s+level\s+security)\b/.test(code))competing=true;
    }
    const protectedTables=new Set([...policy.tables,...[...policy.triggers.keys()].map(key=>key.split(':')[0])]);
    if(/^create\s+table\b/.test(code)){
      const relation=flat.match(/^create\s+table\s+(?:if\s+not\s+exists\s+)?((?:private|public)\.\w+)\b/)?.[1];
      if(protectedTables.has(relation))competing=true;
    }
    if(/^drop\s+table\b/.test(code)&&[...flat.matchAll(/\b(?:private|public)\.\w+\b/g)].some(m=>protectedTables.has(m[0])))competing=true;
    if(/^(?:alter|drop|grant|revoke)\b/.test(code)&&/\b(?:function|procedure|routine)\b/.test(code)&&[...policy.routines].some(name=>flat.includes(name+'(')||flat.includes(name+' (')))competing=true;
    if(/^drop\s+schema\s+(?:if\s+exists\s+)?(?:private|public)\b/.test(code)||/^alter\s+schema\s+(?:private|public)\s+rename\b/.test(code))competing=true;
    // DY03 retains the exact owner-only ACL on its one existing guard OID.
    if(file===DAILY_IDENTITY_GUARD&&(
      /^revoke\s+all\s+on\s+function\s+private\.bpay_next_work_pointer_guard_v1\s*\(\s*\)\s+from\s+public\s*,\s*anon\s*,\s*authenticated\s*,\s*service_role\s*;\s*$/.test(original)
      ||/^alter\s+function\s+private\.bpay_next_work_pointer_guard_v1\s*\(\s*\)\s+owner\s+to\s+postgres\s*;\s*$/.test(original)))competing=false;
    // Existing guard owners and owner-only ACLs, no attachment or family waiver.
    if(file===DAILY_PUBLICATION_GUARDS&&(
      /^revoke\s+all\s+on\s+function\s+private\.bpay_next_(?:revision|work_insert)_guard_v1\s*\(\s*\)\s+from\s+public\s*,\s*anon\s*,\s*authenticated\s*,\s*service_role\s*;\s*$/.test(original)
      ||/^alter\s+function\s+private\.bpay_next_(?:revision|work_insert)_guard_v1\s*\(\s*\)\s+owner\s+to\s+postgres\s*;\s*$/.test(original)))competing=false;
    if(file===DAILY_FROZEN_GUARD&&(
      /^revoke\s+all\s+on\s+function\s+private\.bpay_next_revision_guard_v1\s*\(\s*\)\s+from\s+public\s*,\s*anon\s*,\s*authenticated\s*,\s*service_role\s*;\s*$/.test(original)
      ||/^alter\s+function\s+private\.bpay_next_revision_guard_v1\s*\(\s*\)\s+owner\s+to\s+postgres\s*;\s*$/.test(original)))competing=false;
    if(competing&&!baseline.includes(statement))throw Error(`BPAY_NEXT_FROZEN_COMPETING_AUTHORITY: ${file}`);
  }
}

export function assertNextQueueShapes(background,financial,index){
  const clean=source=>source.replace(/\/\*[\s\S]*?\*\//g,'').replace(/^\s*\/\/.*$/gm,'');
  const b=clean(background),f=clean(financial),i=clean(index);
  const sends=[...i.matchAll(/BANKING_PAY_CONTINUATION_QUEUE\.send\(\s*({[^}]*})\s*\)/g)];
  if(sends.length!==4 || sends.some(([,body])=>!/^\{\s*id\s*:\s*(?:'1'|result\.receipt\.job_id)\s*,\s*kind\s*:\s*'(?:ENROLL|POSITION_APPLY)'\s*\}$/.test(body))
    || !b.includes('messages.some(body=>!bpayNextQueueIdentity(body))')
    || !b.includes("const ENROLL=Object.freeze({id:'1',kind:'ENROLL'})")
    || !b.includes('sendBatch(messages.map(body=>({body,contentType:')
    || /Object\.freeze\(\{(?:version|runtime_action):/.test(b)
    || !f.includes('keys.length !== 2') || !f.includes("['id','kind'].includes(key)")
    || !f.includes('await dependencies.sendContinuation(wake)')
    || /sendContinuation\((?!wake\))/.test(f)) throw Error('BPAY_NEXT_QUEUE_IDENTITY_SHAPE');
}

export function verifyBpayNextSourceGates(){
  assertNextQueueShapes(read('broker/src/bpay-next-background.js'),read('broker/src/bpay-next-financial-queue.js'),read('broker/src/index.js'));
  verifyFinalDefiners();
  git(['merge-base','--is-ancestor',FROZEN_BASE,'HEAD']);
  const current=inventory(),changed=changedSourcePaths();
  const {routines,migrationRoots}=definerInventory(current),protectedSignatures=new Map(),frozenSources=new Map();
  // Owner A1: every current Create/append skip literal must fit the member CHECK.
  {const memberChecks=read('supabase/migrations/11102026_1358_banking_pay_next_paye_same_week_guard.sql'),checkSets=[...memberChecks.matchAll(/check\s*\(skip_code\s+in\s*\(([^)]*)\)\)/gi)].map(m=>new Set([...m[1].matchAll(/'([A-Z0-9_]+)'/g)].map(v=>v[1])));for(const [signature,definitions] of routines){if(!/^private\.bpay_next_(?:create|append)_/.test(signature))continue;const final=definitions.at(-1),source=read(final.physical).replaceAll('\r\n','\n'),body=scanManagedSql(source).statements.filter(row=>routineDefinitions(source.slice(row.start,row.end),final.physical).includes(signature)).map(row=>source.slice(row.start,row.end)).join('\n');for(const match of body.matchAll(/(?:v_(?:skip_)?code\s*:=|return)\s*'((?:BPAY_NEXT_(?:WORK_WEEK|SELECTION_)|SOURCE_PAY_QUERY_|PAYE_EXCLUDED_)[A-Z0-9_]+)'/gi))if(checkSets.length!==2||checkSets.some(allowed=>!allowed.has(match[1]))||match[1]==='BPAY_NEXT_SELECTION_PAY_ON_HOLD')throw Error('BPAY_NEXT_MEMBER_SKIP_CHECK_MISSING: '+signature+' '+match[1]);}}
  for(const signature of new Set([...ROUTINE_WORK_SIGNATURES,...TYPED_TRANSPORT_SIGNATURES])){
    const final=routines.get(signature)?.at(-1);
    if(!final)throw Error(`BPAY_NEXT_ROUTINE_WORK_BODY_MISSING: ${signature}`);
    const source=read(final.physical).replaceAll('\r\n','\n');
    const rows=scanManagedSql(source).statements.filter(row=>routineDefinitions(source.slice(row.start,row.end),final.physical).includes(signature));
    if(rows.length!==1)throw Error(`BPAY_NEXT_ROUTINE_WORK_BODY_MISSING: ${signature}`);
    const statement=source.slice(rows[0].start,rows[0].end);
    if(ROUTINE_WORK_SIGNATURES.includes(signature))assertRoutineHistoryPoints(statement,final.physical,signature);
    if(TYPED_TRANSPORT_SIGNATURES.includes(signature))assertTypedFinancialProjection(statement,final.physical,signature);
  }
  for(const file of FROZEN_FILES){
    const base=spawnSync('git',['show',`${FROZEN_BASE}:${file}`],{cwd:repoRoot,maxBuffer:64*1024*1024,windowsHide:true});
    if(base.status!==0||!fs.readFileSync(path.join(repoRoot,file)).equals(base.stdout))throw Error(`BPAY_NEXT_FROZEN_BYTES_CHANGED: ${file}`);
    frozenSources.set(file,base.stdout.toString('utf8'));
    for(const signature of routineDefinitions(base.stdout.toString('utf8'),file))protectedSignatures.set(signature,file);
  }
  for(const [signature,file] of protectedSignatures){
    const definitions=[...(routines.get(signature)??[]),...migrationRoots.flatMap(root=>root.definitions.filter(row=>row.signature===signature))];
    if(!definitions.length)throw Error(`BPAY_NEXT_FROZEN_AUTHORITY_MISSING: ${signature}`);
    for(const definition of definitions.filter(row=>row.physical!==file)){
      const previous=spawnSync('git',['show',`${FROZEN_BASE}:${definition.physical}`],{cwd:repoRoot,encoding:'utf8',maxBuffer:64*1024*1024,windowsHide:true});
      const currentSource=read(definition.physical).replaceAll('\r\n','\n');
      const exact=scanManagedSql(currentSource).statements.filter(row=>routineDefinitions(currentSource.slice(row.start,row.end),definition.physical).includes(signature)).map(row=>currentSource.slice(row.start,row.end).replaceAll('\r\n','\n'));
      // DY03's card extends only the NULL-safe immutable Work identity tuple.
      // Compare the entire statement to the frozen parent with exactly two
      // field-list extensions; every other guard branch remains byte-exact.
      if(signature==='private.bpay_next_work_pointer_guard_v1()'&&definition.physical===DAILY_IDENTITY_GUARD){
        const parent=frozenSources.get(file).replaceAll('\r\n','\n');
        const rows=scanManagedSql(parent).statements.filter(row=>routineDefinitions(parent.slice(row.start,row.end),file).includes(signature));
        if(rows.length!==1||exact.length!==1)throw Error('BPAY_NEXT_DAILY_IDENTITY_GUARD_SHAPE');
        let expected=parent.slice(rows[0].start,rows[0].end);
        for(const prefix of ['new','old']){
          const anchor=`${prefix}.work_kind,${prefix}.week_ending_date)`;
          if(expected.split(anchor).length!==2)throw Error('BPAY_NEXT_DAILY_IDENTITY_PARENT_SHAPE');
          expected=expected.replace(anchor,`${prefix}.work_kind,${prefix}.week_ending_date,${prefix}.origin_kind,${prefix}.origin_work_date)`);
        }
        if(exact[0]!==expected)throw Error('BPAY_NEXT_DAILY_IDENTITY_GUARD_CHANGED');
        continue;
      }
      if(definition.physical===DAILY_PUBLICATION_GUARDS&&Object.hasOwn(DAILY_GUARD_ADDITIONS,signature)){
        const parent=frozenSources.get(file).replaceAll('\r\n','\n'),addition=DAILY_GUARD_ADDITIONS[signature];
        const rows=scanManagedSql(parent).statements.filter(row=>routineDefinitions(parent.slice(row.start,row.end),file).includes(signature));
        if(rows.length!==1||exact.length!==1)throw Error('BPAY_NEXT_DAILY_PUBLICATION_GUARD_SHAPE');
        let expected=parent.slice(rows[0].start,rows[0].end);
        if(expected.split(addition.declAnchor).length!==2||expected.split(addition.blockEnd).length!==2)throw Error('BPAY_NEXT_DAILY_PUBLICATION_PARENT_SHAPE');
        expected=expected.replace(addition.declAnchor,()=>addition.declAnchor+addition.declaration).replace(addition.blockEnd,()=>addition.block+addition.blockEnd);
        if(exact[0]!==expected)throw Error('BPAY_NEXT_DAILY_PUBLICATION_GUARD_CHANGED');
        continue;
      }
      if(definition.physical===DAILY_FROZEN_GUARD&&signature==='private.bpay_next_revision_guard_v1()'){
        const parent=frozenSources.get(file).replaceAll('\r\n','\n'),addition=DAILY_FROZEN_ADDITION;
        const rows=scanManagedSql(parent).statements.filter(row=>routineDefinitions(parent.slice(row.start,row.end),file).includes(signature));
        if(rows.length!==1||exact.length!==1)throw Error('BPAY_NEXT_DAILY_FROZEN_GUARD_SHAPE');
        let expected=parent.slice(rows[0].start,rows[0].end);
        if(expected.split(addition.declAnchor).length!==2||expected.split(addition.blockEnd).length!==2)throw Error('BPAY_NEXT_DAILY_FROZEN_PARENT_SHAPE');
        expected=expected.replace(addition.declAnchor,()=>addition.declAnchor+addition.declaration).replace(addition.blockEnd,()=>addition.block+addition.blockEnd);
        if(exact[0]!==expected)throw Error('BPAY_NEXT_DAILY_FROZEN_GUARD_CHANGED');
        continue;
      }
      if(previous.status!==0||exact.some(text=>!previous.stdout.replaceAll('\r\n','\n').includes(text)))throw Error(`BPAY_NEXT_FROZEN_COMPETING_DEFINER: ${signature}`);
    }
  }
  const sources=new Set([...current.migrations,...current.repeatables].flatMap(row=>row.paths??[row.path]));
  const productFiles=git(['ls-files','--cached','--others','--exclude-standard']).split(/\r?\n/).filter(Boolean);
  const baselineHazards=new Map();
  const authorityPolicy=frozenAuthorityPolicy(frozenSources),baseSources=new Map();
  const baselineFiles=git(['ls-tree','-r','--name-only',FROZEN_BASE,'--','supabase/migrations','supabase/repeatable','supabase/verification']).split(/\r?\n/).filter(file=>/\.sql$/i.test(file));
  const batch=spawnSync('git',['cat-file','--batch'],{cwd:repoRoot,input:baselineFiles.map(file=>`${FROZEN_BASE}:${file}\n`).join(''),maxBuffer:128*1024*1024,windowsHide:true});
  if(batch.status!==0)throw Error('BPAY_NEXT_SOURCE_BASE_BATCH_FAILED');
  let offset=0;
  for(const file of baselineFiles){
    const end=batch.stdout.indexOf(10,offset),header=batch.stdout.subarray(offset,end).toString('ascii'),match=header.match(/^[0-9a-f]{40} blob (\d+)$/);
    if(end<0||!match)throw Error('BPAY_NEXT_SOURCE_BASE_BLOB_INVALID');
    const size=Number(match[1]),start=end+1;
    if(!Number.isSafeInteger(size)||start+size>=batch.stdout.length||batch.stdout[start+size]!==10)throw Error('BPAY_NEXT_SOURCE_BASE_BLOB_TRUNCATED');
    const base=batch.stdout.subarray(start,start+size).toString('utf8');offset=start+size+1;baseSources.set(file,base);
    const counts=new Map();
    for(const row of specialConstructViolations(base,file))counts.set(row.key,(counts.get(row.key)??0)+1);
    for(const [key,count]of counts)baselineHazards.set(key,Math.max(baselineHazards.get(key)??0,count));
  }
  if(offset!==batch.stdout.length)throw Error('BPAY_NEXT_SOURCE_BASE_BATCH_TRAILING');
  // DR04's five existing root-header locks are deliberately strengthened
  // SHARE -> UPDATE before Candidate locks for maintained outcome counters.
  // Retain the exact baseline same-signature/predicate, not other row reads.
  const headerLocks=new Set(['private.bpay_next_claim_prepare_job_v1(uuid,integer)',
    'private.bpay_next_pin_selected_work_v1(uuid,uuid,bigint,bigint)',
    'private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer)',
    'private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer)',
    'private.bpay_next_complete_no_case_worker_v1(uuid,uuid,bigint)']);
  for(const [key,count] of [...baselineHazards]) {
    const [signature,hazard,text]=JSON.parse(key);
    if(headerLocks.has(signature)&&hazard==='ROWTYPE_SELECT:V_RUN:PRIVATE . BPAY_NEXT_PAY_RUN'
      && /where id=v_run_id for share;/i.test(text)) {
      baselineHazards.set(JSON.stringify([signature,hazard,text.replace(/for share;/i,'for update;')]),count);
    }
  }

  for(const file of productFiles)if(/\.sql$/i.test(file)&&/^supabase\/(?:seed|seeds|verification)\//.test(file))sources.add(file);
  // DR09 installed predecessor row projections: exact bytes and finite statements only.
  const discardRowFile="supabase/repeatable/11102026_0814_banking_pay_next_preparation_discard_v1.sql",discardRowSource=read(discardRowFile);
  if(createHash('sha256').update(discardRowSource).digest('hex')!=="29f3099ebfb8a7e11d8c23a8996c5351eb9330316d8bdb6fd6d3a5136d3f379c")throw Error('BPAY_NEXT_DISCARD_RETAINED_PROJECTION_CHANGED');
  const discardRowHazards=new Map(baselineHazards);
  for(const key of ["[\"private.bpay_next_enroll_preparation_discard_one_v1(uuid)\",\"ROWTYPE_SELECT:V_RUN:PRIVATE . BPAY_NEXT_PAY_RUN\",\"select * into strict v_run from private.bpay_next_pay_run where id=v_run_id for share nowait;\"]","[\"private.bpay_next_enroll_preparation_discard_one_v1(uuid)\",\"ROWTYPE_SELECT:V_REQUEST:PRIVATE . BPAY_NEXT_PREPARATION_DISCARD_REQUEST\",\"select * into strict v_request from private.bpay_next_preparation_discard_request where command_id=p_command_id for update;\"]","[\"private.bpay_next_enroll_preparation_discard_one_v1(uuid)\",\"ROWTYPE_SELECT:V_COMMAND:PRIVATE . BPAY_NEXT_COMMAND\",\"select * into strict v_command from private.bpay_next_command where id=p_command_id for update;\"]","[\"private.bpay_next_enroll_preparation_discard_one_v1(uuid)\",\"ROWTYPE_SELECT:V_WORKER:PRIVATE . BPAY_NEXT_RUN_WORKER\",\"select * into v_worker from private.bpay_next_run_worker where run_id=v_run_id and candidate_id=v_candidate;\"]","[\"private.bpay_next_claim_preparation_discard_job_v1(uuid,integer)\",\"ROWTYPE_SELECT:V_RUN:PRIVATE . BPAY_NEXT_PAY_RUN\",\"select * into strict v_run from private.bpay_next_pay_run where id=v_run_id for update;\"]","[\"private.bpay_next_claim_preparation_discard_job_v1(uuid,integer)\",\"ROWTYPE_SELECT:V_JOB:PRIVATE . BPAY_NEXT_JOB\",\"select * into strict v_job from private.bpay_next_job where id=p_job_id for update;\"]","[\"private.bpay_next_discard_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer)\",\"ROWTYPE_SELECT:V_RUN:PRIVATE . BPAY_NEXT_PAY_RUN\",\"select * into strict v_run from private.bpay_next_pay_run where id=v_run_id for update;\"]","[\"private.bpay_next_discard_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer)\",\"ROWTYPE_SELECT:V_CONTROL:PRIVATE . BPAY_NEXT_WORKER_CONTROL\",\"select * into strict v_control from private.bpay_next_worker_control where candidate_id=v_candidate for update;\"]","[\"private.bpay_next_discard_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer)\",\"ROWTYPE_SELECT:V_JOB:PRIVATE . BPAY_NEXT_JOB\",\"select * into strict v_job from private.bpay_next_job where id=p_job_id for update;\"]","[\"private.bpay_next_discard_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer)\",\"ROWTYPE_SELECT:V_BINDING:PRIVATE . BPAY_NEXT_PREPARATION_DISCARD_WORKER\",\"select * into strict v_binding from private.bpay_next_preparation_discard_worker where job_id=p_job_id for update;\"]","[\"private.bpay_next_discard_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer)\",\"ROWTYPE_SELECT:V_WORKER:PRIVATE . BPAY_NEXT_RUN_WORKER\",\"select * into strict v_worker from private.bpay_next_run_worker where id=v_binding.run_worker_id for update;\"]","[\"private.bpay_next_discarded_prepare_receipt_v1(uuid)\",\"ROWTYPE_SELECT:V_JOB:PRIVATE . BPAY_NEXT_JOB\",\"select * into strict v_job from private.bpay_next_job where id=p_job_id and job_kind='PREPARE';\"]","[\"private.bpay_next_discarded_prepare_receipt_v1(uuid)\",\"ROWTYPE_SELECT:V_WORKER:PRIVATE . BPAY_NEXT_RUN_WORKER\",\"select 1 from private.bpay_next_worker_reprepare_request x\\n      join private.bpay_next_command c on c.id=x.command_id and c.command_kind='PREPARE' and c.module_epoch=x.module_epoch\\n      join private.bpay_next_run_worker w on w.id=x.run_worker_id and w.run_id=x.run_id and w.candidate_id=x.candidate_id\\n        and w.preparation_revision>=x.target_preparation_revision and w.captured_preparation_revision=x.captured_preparation_revision\\n      join private.bpay_next_job input_job on input_job.id=x.input_prepare_job_id and input_job.command_id=x.original_prepare_command_id\\n        and input_job.candidate_id=x.candidate_id and input_job.module_epoch=x.module_epoch\\n      join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=x.module_epoch\\n      where x.previous_job_id=p_job_id and x.run_id=v_run.id and x.candidate_id=v_job.candidate_id\\n        and x.module_epoch=v_job.module_epoch and x.initiator_kind='OFFICE' and x.action='RELEASE'\\n        and x.original_prepare_command_id=(select command_id from private.bpay_next_run_command where run_id=v_run.id)) then\\n    select * into strict v_worker from private.bpay_next_run_worker where run_id=v_run.id and candidate_id=v_job.candidate_id;\"]","[\"private.bpay_next_discarded_prepare_receipt_v1(uuid)\",\"ROWTYPE_SELECT:V_WORKER:PRIVATE . BPAY_NEXT_RUN_WORKER\",\"select * into v_worker from private.bpay_next_run_worker where run_id=v_run.id and candidate_id=v_job.candidate_id;\"]","[\"private.bpay_next_discarded_prepare_receipt_v1(uuid)\",\"ROWTYPE_SELECT:V_BINDING:PRIVATE . BPAY_NEXT_PREPARATION_DISCARD_WORKER\",\"select * into v_binding from private.bpay_next_preparation_discard_worker where command_id=v_request.command_id and candidate_id=v_job.candidate_id;\"]","[\"private.bpay_next_preparation_expiry_guard_v1()\",\"ROWTYPE_SELECT:V_RUN:PRIVATE . BPAY_NEXT_PAY_RUN\",\"select * into strict v_run from private.bpay_next_pay_run where id=new.run_id;\"]","[\"private.bpay_next_preparation_expiry_guard_v1()\",\"ROWTYPE_SELECT:V_REQUEST:PRIVATE . BPAY_NEXT_PREPARATION_EXPIRY_REQUEST\",\"select * into strict v_request from private.bpay_next_preparation_expiry_request where command_id=new.command_id;\"]","[\"private.bpay_next_preparation_expiry_guard_v1()\",\"ROWTYPE_SELECT:V_RUN:PRIVATE . BPAY_NEXT_PAY_RUN\",\"select * into strict v_run from private.bpay_next_pay_run where id=new.run_id;\"]","[\"private.bpay_next_preparation_discard_guard_v1()\",\"ROWTYPE_SELECT:V_RUN:PRIVATE . BPAY_NEXT_PAY_RUN\",\"select * into strict v_run from private.bpay_next_pay_run where id=new.run_id;\"]","[\"private.bpay_next_preparation_discard_guard_v1()\",\"ROWTYPE_SELECT:V_REQUEST:PRIVATE . BPAY_NEXT_PREPARATION_DISCARD_REQUEST\",\"select * into strict v_request from private.bpay_next_preparation_discard_request where command_id=new.command_id;\"]","[\"private.bpay_next_preparation_discard_guard_v1()\",\"ROWTYPE_SELECT:V_RUN:PRIVATE . BPAY_NEXT_PAY_RUN\",\"select * into strict v_run from private.bpay_next_pay_run where id=new.run_id;\"]","[\"private.bpay_next_enroll_one_v1()\",\"ROWTYPE_SELECT:V_COMMAND:PRIVATE . BPAY_NEXT_COMMAND\",\"select * into v_command from private.bpay_next_command\\n    where agency_sequence=v_last+1 for update;\"]","[\"private.bpay_next_enroll_one_v1()\",\"ROWTYPE_SELECT:V_RUN:PRIVATE . BPAY_NEXT_PAY_RUN\",\"select * into strict v_run from private.bpay_next_pay_run\\n      where id=v_run_id for share nowait;\"]","[\"private.bpay_next_enroll_one_v1()\",\"ROWTYPE_SELECT:V_MEMBER:PRIVATE . BPAY_NEXT_COMMAND_MEMBER\",\"select * into strict v_member from private.bpay_next_command_member\\n      where command_id=v_command.id and member_no=1;\"]","[\"private.bpay_next_enroll_one_v1()\",\"ROWTYPE_SELECT:V_MEMBER:PRIVATE . BPAY_NEXT_COMMAND_MEMBER\",\"select * into strict v_member from private.bpay_next_command_member\\n      where command_id=v_command.id and member_no=1;\"]","[\"private.bpay_next_enroll_one_v1()\",\"ROWTYPE_SELECT:V_MEMBER:PRIVATE . BPAY_NEXT_COMMAND_MEMBER\",\"select * into strict v_member from private.bpay_next_command_member\\n      where command_id=v_command.id and member_no=1;\"]","[\"private.bpay_next_enroll_one_v1()\",\"ROWTYPE_SELECT:V_MEMBER:PRIVATE . BPAY_NEXT_COMMAND_MEMBER\",\"select * into strict v_member from private.bpay_next_command_member\\n      where command_id=v_command.id and member_no=1;\"]","[\"private.bpay_next_enroll_one_v1()\",\"ROWTYPE_SELECT:V_MEMBER:PRIVATE . BPAY_NEXT_COMMAND_MEMBER\",\"select * into strict v_member from private.bpay_next_command_member\\n      where command_id=v_command.id and member_no=1;\"]","[\"private.bpay_next_enroll_one_v1()\",\"ROWTYPE_SELECT:V_MEMBER:PRIVATE . BPAY_NEXT_COMMAND_MEMBER\",\"select * into strict v_member from private.bpay_next_command_member\\n      where command_id=v_command.id and member_no=1;\"]","[\"private.bpay_next_enroll_one_v1()\",\"ROWTYPE_SELECT:V_MEMBER:PRIVATE . BPAY_NEXT_COMMAND_MEMBER\",\"select * into strict v_member from private.bpay_next_command_member\\n      where command_id=v_command.id and member_no=1;\"]","[\"private.bpay_next_enroll_one_v1()\",\"ROWTYPE_SELECT:V_MEMBER:PRIVATE . BPAY_NEXT_COMMAND_MEMBER\",\"select * into strict v_member from private.bpay_next_command_member\\n    where command_id=v_command.id and member_no=1;\"]","[\"private.bpay_next_confirm_simple_run_v1(uuid,bigint)\",\"ROWTYPE_SELECT:V_RUN:PRIVATE . BPAY_NEXT_PAY_RUN\",\"select * into strict v_run from private.bpay_next_pay_run\\n    where id=p_run_id for update;\"]","[\"private.bpay_next_accept_preparation_expiry_v1(uuid,uuid,uuid,timestamp with time zone,timestamp with time zone)\",\"ROWTYPE_SELECT:V_RUN:PRIVATE . BPAY_NEXT_PAY_RUN\",\"select * into strict v_run from private.bpay_next_pay_run where id=p_run_id for update;\"]","[\"private.bpay_next_accept_preparation_expiry_v1(uuid,uuid,uuid,timestamp with time zone,timestamp with time zone)\",\"ROWTYPE_SELECT:V_REQUEST:PRIVATE . BPAY_NEXT_PREPARATION_EXPIRY_REQUEST\",\"select * into v_request from private.bpay_next_preparation_expiry_request where command_id=p_command_id;\"]","[\"private.bpay_next_project_worker_release_page_v1(uuid,uuid,bigint,text,bigint,integer)\",\"ROWTYPE_SELECT:V_RUN:PRIVATE . BPAY_NEXT_PAY_RUN\",\"select * into strict v_run from private.bpay_next_pay_run where id=v_run_id for update;\"]","[\"private.bpay_next_project_worker_release_page_v1(uuid,uuid,bigint,text,bigint,integer)\",\"ROWTYPE_SELECT:V_JOB:PRIVATE . BPAY_NEXT_JOB\",\"select * into strict v_job from private.bpay_next_job where id=p_job_id for update;\"]","[\"private.bpay_next_project_worker_release_page_v1(uuid,uuid,bigint,text,bigint,integer)\",\"ROWTYPE_SELECT:V_BINDING:PRIVATE . BPAY_NEXT_WORKER_REPREPARE_REQUEST\",\"select * into strict v_binding from private.bpay_next_worker_reprepare_request where job_id=p_job_id and action='RELEASE' and status='RUNNING' and stage='PROJECT' for update;\"]","[\"private.bpay_next_project_worker_release_page_v1(uuid,uuid,bigint,text,bigint,integer)\",\"ROWTYPE_SELECT:V_WORKER:PRIVATE . BPAY_NEXT_RUN_WORKER\",\"select * into strict v_worker from private.bpay_next_run_worker where id=v_binding.run_worker_id for update;\"]"])discardRowHazards.set(key,(discardRowHazards.get(key)??0)+1);
  // Installed DR11 initial expiry admission: exactly five inherited row reads. Current0910 is checked normally.
  const earlyFenceFile="supabase/repeatable/11102026_0904_banking_pay_next_early_candidate_financial_fence_v1.sql",earlyFenceSource=read(earlyFenceFile);
  if(createHash('sha256').update(earlyFenceSource).digest('hex')!=="fa3c73a83075f67246e62adea7ba1467c019bada601d3db880ec51b48cdd1a9e")throw Error('BPAY_NEXT_EARLY_FENCE_RETAINED_CHANGED');
  const earlyFenceHazards=new Map(baselineHazards);
  for(const key of ["[\"private.bpay_next_expire_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer)\",\"ROWTYPE_SELECT:V_RUN:PRIVATE . BPAY_NEXT_PAY_RUN\",\"select * into strict v_run from private.bpay_next_pay_run where id=v_run_id for update nowait;\"]","[\"private.bpay_next_expire_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer)\",\"ROWTYPE_SELECT:V_CONTROL:PRIVATE . BPAY_NEXT_WORKER_CONTROL\",\"select * into strict v_control from private.bpay_next_worker_control where candidate_id=v_candidate for update nowait;\"]","[\"private.bpay_next_expire_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer)\",\"ROWTYPE_SELECT:V_JOB:PRIVATE . BPAY_NEXT_JOB\",\"select * into strict v_job from private.bpay_next_job where id=p_job_id for update nowait;\"]","[\"private.bpay_next_expire_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer)\",\"ROWTYPE_SELECT:V_BINDING:PRIVATE . BPAY_NEXT_PREPARATION_EXPIRY_WORKER\",\"select * into strict v_binding from private.bpay_next_preparation_expiry_worker where job_id=p_job_id for update nowait;\"]","[\"private.bpay_next_expire_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer)\",\"ROWTYPE_SELECT:V_WORKER:PRIVATE . BPAY_NEXT_RUN_WORKER\",\"select * into strict v_worker from private.bpay_next_run_worker where id=v_binding.run_worker_id for update nowait;\"]"])earlyFenceHazards.set(key,(earlyFenceHazards.get(key)??0)+1);
  // Installed historical F05 definitions stay immutable. These four exact
  // committed row reads are retained only after their final successor passes.
  const projectionFile='supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql';
  const correctionSignatures=new Set(["private.bpay_next_bind_simple_csv_destination_v1(uuid)","private.bpay_next_issue_simple_csv_v1(uuid,uuid,uuid)","private.bpay_next_receive_simple_reissue_v1(uuid,uuid,uuid)","private.bpay_next_apply_work_collection_recovery_v1(uuid,uuid)"]);
  git(['merge-base','--is-ancestor',FOUNDATION_RECONCILED,'HEAD']);
  const retainedProjection=git(['show',FOUNDATION_RECONCILED+':'+projectionFile]);
  if(read(projectionFile).replaceAll('\r\n','\n')!==retainedProjection.replaceAll('\r\n','\n'))throw Error('BPAY_NEXT_RETAINED_PROJECTION_CHANGED');
  const projectionHazards=new Map(baselineHazards);
  for(const signature of correctionSignatures){
    const final=routines.get(signature)?.at(-1);
    if(!final||final.physical===projectionFile)throw Error('BPAY_NEXT_ROW_PROJECTION_SUCCESSOR_MISSING');
    const currentSource=read(final.physical).replaceAll('\r\n','\n');
    const finalStatement=scanManagedSql(currentSource).statements.find(row=>routineDefinitions(currentSource.slice(row.start,row.end),final.physical).includes(signature));
    if(!finalStatement)throw Error('BPAY_NEXT_ROW_PROJECTION_SUCCESSOR_MISSING');
    assertSpecialConstructs(currentSource.slice(finalStatement.start,finalStatement.end),final.physical,baselineHazards);
  }
  for(const hazard of specialConstructViolations(retainedProjection,projectionFile))if(correctionSignatures.has(JSON.parse(hazard.key)[0])&&hazard.hazard.startsWith('ROWTYPE_SELECT:'))projectionHazards.set(hazard.key,1);
  // Only the two explicit bounded native subqueries prove this alias type.
  // A third aggregate or a different selected expression gets no allowance.
  const projectionPageFile=routines.get('private.bpay_next_prepare_projection_page_v1(uuid,uuid,bigint,text,bigint,integer)')?.at(-1)?.physical;
  if(!projectionPageFile)throw Error('BPAY_NEXT_PROJECTION_FINAL_DEFINER_MISSING');
  const projectionPageFiles=new Set(['supabase/repeatable/11102026_0533_banking_pay_next_prepare_rule_corrections_v1.sql','supabase/repeatable/11102026_0559_banking_pay_next_worker_reprepare_v1.sql',discardRowFile,projectionPageFile]);
  for(const file of projectionPageFiles){
    const projectionPageSource=read(file);
    if((sqlWithoutComments(projectionPageSource).match(/\b(?:pg_catalog\s*\.\s*)?(?:min|max)\s*\(\s*p\s*\.\s*selection_no\s*\)/gi)??[]).length!==2
      || !projectionPageSource.includes('select s.work_id,s.selection_no from private.bpay_next_run_selection s')
      || !projectionPageSource.includes('select s.case_id,s.selection_no from private.bpay_next_case_selection_item s')
      || (projectionPageSource.match(/order by s\.selection_no limit p_limit\) p;/g)??[]).length!==2)
      throw Error('BPAY_NEXT_PROJECTION_MEMBERSHIP_TYPE_UNCLASSIFIED');
  }
  const projectionMembershipColumns=new Map([['P . SELECTION_NO','BIGINT']]);
  const sourceFailures=[];
  for(const file of sources){
    const source=read(file);assertSqlWriteBoundaries(source,file);assertNextExactNumericTypes(source,file);
    if(changed.has(file)){
      try{if(projectionPageFiles.has(file)) {
        const normalized=source.replaceAll('\r\n','\n');
        for(const statement of scanManagedSql(normalized).statements) {
          const text=normalized.slice(statement.start,statement.end);
          const isProjection=routineDefinitions(text,file).includes('private.bpay_next_prepare_projection_page_v1(uuid,uuid,bigint,text,bigint,integer)');
          assertSpecialConstructs(text,file,file===discardRowFile?discardRowHazards:baselineHazards,isProjection?projectionMembershipColumns:new Map());
        }
      } else assertSpecialConstructs(source,file,file===projectionFile?projectionHazards:file===earlyFenceFile?earlyFenceHazards:baselineHazards);assertFrozenAuthority(source,file,authorityPolicy,baseSources.get(file)??'');}
      catch(error){sourceFailures.push(error.message);}
    }
  }
  if(sourceFailures.length)throw Error(sourceFailures.join('\n'));
  for(const file of productFiles.filter(file=>/^(?:broker\/|candidate-(?:broker|private-api|synthetic-private-api)\/|weekly-source-delivery-worker\/|invoice-(?:queue-dispatcher|document-processor)\/|scripts\/|wrangler\.)/.test(file)
    &&/\.(?:[cm]?js|ts|jsonc?|toml)$/.test(file)&&file!=='scripts/bpay-next-source-gates.mjs'))assertExecutableSourceBoundaries(read(file),file);
  const rpcRoots=new Set();
  for(const [signature,definitions] of routines){
    if(!signature.startsWith('public.')||!definitions.some(row=>changed.has(row.physical)))continue;
    rpcRoots.add(definitions.at(-1).root);
  }
  for(const root of migrationRoots.filter(root=>root.includes.some(file=>changed.has(file))))for(const definition of root.definitions){
    if(!definition.signature.startsWith('public.'))continue;
    const final=routines.get(definition.signature)?.at(-1);
    if(!final)throw Error(`BPAY_NEXT_RPC_FINAL_REPEATABLE_MISSING: ${definition.signature}`);
    rpcRoots.add(final.root);
  }
  for(const file of rpcRoots)assertRpcReload(readManagedClosure(file,repoRoot).expanded,file);
  return {frozenFiles:FROZEN_FILES.length,frozenTriggers:authorityPolicy.triggers.size,frozenTables:authorityPolicy.tables.size,protectedSignatures:protectedSignatures.size,sqlSources:sources.size,rpcRoots:rpcRoots.size,routineWorkSignatures:ROUTINE_WORK_SIGNATURES.length,typedTransportSignatures:TYPED_TRANSPORT_SIGNATURES.length};
}
