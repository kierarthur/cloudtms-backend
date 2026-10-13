-- Office NEXT payment-history reader. Informational only: no financial authority,
-- mutation, operational history SUM, inverse conversion, live pricing or secrets.
--
-- CALLER CONTRACT: trusted authenticated Office broker supplies actor_user_id;
-- never forward an actor from an untrusted browser body. Service JWT + active
-- admin are both required (0575's financial scope, without its mutating locks or
-- NEXT activation requirement). History remains readable after owner rollback.
-- Exact request keys: version,actor_user_id,candidate_id,timesheet_id,section,
-- parent_id,context_id,component_key,after,upper,limit. Exactly one scope ID is non-null.
-- version=PAYMENT_HISTORY_V1; IDs lower-case UUID strings; limit integer 1..100.
-- MANIFEST has null parent/context/component/cursors. Other parent types are declared by
-- its sections. EFFECTS alone requires component_key; all others require null.
-- First page after/upper are both null. Continuation MUST use the returned
-- next_cursor object as after, and returned upper array unchanged. Cursor binds
-- scope kind/ID, section, parent/context/component, upper and last string key.
-- Each page explicitly returns terminal; callers visit EVERY declared child
-- section for EACH emitted parent, not just the first page or current revision.
-- context_id is null except Timesheet-scoped whole WORKER/TRANSFER/COMMAND
-- history for a CASE-only worker: use its exact captured instruction ID, which
-- must link that worker through CASE -> WORK_COLLECTION -> this exact WORK.
--
-- GRAPH: Candidate -> WORKS / CASES / COMMANDS. Durable Timesheet -> one exact
-- booking-family WORK, independent of its current approval. WORK -> REVISIONS,
-- POSITIONS, COLLECTIONS, RUN_WORKS, PUBLICATIONS; REVISION -> LINES/SCHEDULE;
-- LINE -> SHIFTS/RATES/SOURCE_DETAIL; SHIFT -> BREAKS. RUN_WORK -> frozen
-- RUN_POSITIONS/RUN_LINES and WORKER; RUN_LINE -> HOLD/POLICY. WORK/component ->
-- EFFECTS. RUN_WORK -> CHOICE/SELECTION; WORKER -> PROJECTIONS/TRANSFERS/EXPIRY/
-- WORKER_CASE_HOLDS (includes old untyped/released holds); TRANSFER -> MEMBERS,
-- OUTCOMES/RETURN_CASH/CSV/INTERNAL_RECEIPT/GROUP_LEGS. CASE -> COMPONENTS,
-- RULES/CASE_EVENTS; COMPONENT -> INSTRUCTIONS/CASE_HOLDS; INSTRUCTION -> RESULTS.
-- COMMAND -> COMMAND_WORKER/JOBS and each typed request/receipt section
-- (including failed/DONE). COMMAND_WORKER discovers zero-WORK CASE-only runs.
-- Timesheet scope omits unrelated Candidate-only COMMANDS/CASES: their declared
-- applicability is explicit, not a silent page filter. Related CASEs are linked
-- through the existing unique WORK_COLLECTION.case_id; commands through actual
-- publication or captured-worker/request FK points, never a guessed allocation.
-- Follow COLLECTIONS.case_id -> CASE, and RUN_WORKS.run_worker_id or
-- INSTRUCTIONS.run_worker_id -> WORKER (the latter with context_id).
--
-- Scalar data values are strings or null: preserves decimal/64-bit precision;
-- clocks are UTC, null classification/NET is NOT converted to zero/non-taxable.
-- SOURCE amounts, frozen target ex/VAT/inc and WHOLE_WORKER_NET/WHOLE_TRANSFER
-- cash have separate labels. A transfer MEMBER is provenance/contribution, NOT
-- a Timesheet share of total cash. Current POSITION/CASE/collection/control
-- caches are explicitly CURRENT, not a reconstructed history or payable total.
-- Original and successor revisions/children are retained; no current-only filter.
--
-- CONSISTENCY: live keyset pagination, NOT an across-request MVCC snapshot. The
-- server fixes an upper key for each section/parent; append-only frozen parent
-- details are immutable. A late commit/backdated event can appear below an old
-- cursor: refresh that section from null for a new read session. Current caches
-- may change between pages; their updated/revision fields remain visible. No
-- stored read session, compulsory full-history count/hash or misleading as-of.
-- Missing not-yet-implemented moved-root flat binding/vector/comparison receipts
-- are explicitly unavailable; no borrowed target units or fabricated actor.

\set ON_ERROR_STOP on

begin;

create or replace function public.bpay_next_payment_history_page_v1(p_request jsonb)
returns jsonb language plpgsql stable security definer
set search_path=pg_catalog,private,public
set timezone='UTC'
as $function$
declare
  -- Closed SQL registry. Neither table/column/type nor SQL comes from a caller.
  v_sections constant jsonb := $history$
{
 "WORKS":{"table":"bpay_next_work","parent":"SCOPE","fk":"candidate_id","keys":[["week_ending_date","date"],["id","uuid"]],"fields":"id,candidate_id,contract_id,original_timesheet_id,booking_id,work_kind,week_ending_date,approval_state,current_revision_id,applied_revision_id,current_revision_no,created_at_utc,updated_at_utc","state":"CURRENT"},
 "REVISIONS":{"table":"bpay_next_work_revision","parent":"WORK","fk":"work_id","keys":[["revision_no","bigint"]],"fields":"id,work_id,revision_no,source_kind,source_head_id,source_event_id,physical_timesheet_id,physical_timesheet_version,financial_snapshot_id,source_pay_channel,valuation_policy_id,currency,week_ending_date,detail_kind,expected_line_count,expected_rate_schedule_count,certified_zero,approved_source_ex_vat,source_inventory_digest,approved_at_utc,sealed_at_utc,created_at_utc","state":"IMMUTABLE_SOURCE"},
 "LINES":{"table":"bpay_next_approved_line","parent":"REVISION","fk":"revision_id","keys":[["line_no","bigint"]],"fields":"id,revision_id,line_no,component_key,component_kind,tax_treatment,source_component_id,work_date,approved_quantity,approved_unit_rate,unit_label,source_pay_ex_vat,evidence_ref,expected_rate_detail_count,created_at_utc","state":"IMMUTABLE_SOURCE"},
 "SHIFTS":{"table":"bpay_next_shift_detail","parent":"LINE","fk":"approved_line_id","keys":[["detail_no","integer"]],"fields":"id,approved_line_id,detail_no,work_date,shift_start_at,shift_end_at,shift_start_local,shift_end_local,shift_overnight,submitted_minutes,approved_minutes,approved_hours,hours_day,hours_night,hours_sat,hours_sun,hours_bh,pay_excluded,segment_pay_ex_vat,detail_label,immutable_evidence_ref","state":"IMMUTABLE_SOURCE"},
 "BREAKS":{"table":"bpay_next_break_detail","parent":"SHIFT","fk":"shift_detail_id","keys":[["break_no","integer"]],"fields":"id,shift_detail_id,break_no,break_minutes,break_start_at,break_end_at,break_start_local,break_end_local","state":"IMMUTABLE_SOURCE"},
 "RATES":{"table":"bpay_next_rate_detail","parent":"LINE","fk":"approved_line_id","keys":[["bucket","text"]],"fields":"id,approved_line_id,bucket,approved_hours,source_pay_rate","state":"IMMUTABLE_SOURCE"},
 "SCHEDULE":{"table":"bpay_next_rate_schedule","parent":"REVISION","fk":"revision_id","keys":[["rate_family","text"],["rate_code","text"]],"fields":"id,revision_id,rate_family,rate_code,paye_rate,umbrella_rate,charge_rate,unit_label","state":"IMMUTABLE_SOURCE"},
 "POSITIONS":{"table":"bpay_next_position","parent":"WORK","fk":"work_id","keys":[["component_key","text"]],"fields":"work_id,component_key,applied_revision_id,source_basis_channel,approved_source_ex_vat,realised_source_ex_vat,held_source_ex_vat,realised_target_ex_vat,realised_target_vat,realised_target_inc_vat,held_target_ex_vat,held_target_vat,held_target_inc_vat,updated_at_utc","state":"CURRENT_SOURCE_AND_TARGET_CACHE"},
 "COLLECTIONS":{"table":"bpay_next_work_collection","parent":"WORK","fk":"work_id","keys":[["component_key","text"]],"fields":"id,work_id,component_key,candidate_id,original_effect_id,original_run_line_id,original_revision_id,original_approved_line_id,original_transfer_id,original_source_pay_channel,original_valuation_policy_id,original_tax_treatment,paid_basis_qualified,qualification_state,observed_revision_id,observed_approved_source_ex_vat,observed_realised_source_ex_vat,case_id,case_component_id,reconcile_revision,last_job_id,last_effect_id,created_at_utc,updated_at_utc","state":"CURRENT_COLLECTION_CACHE"},
 "EFFECTS":{"table":"bpay_next_financial_effect","parent":"WORK","fk":"work_id","keys":[["occurred_at_utc","timestamptz"],["id","uuid"]],"fields":"id,operation_id,operation_item_id,effect_kind,candidate_id,work_id,component_key,case_id,original_timesheet_id,original_transfer_id,source_disposition_ex_vat,target_amount_ex_vat,target_amount_vat,target_amount_inc_vat,occurred_at_utc","state":"IMMUTABLE_SOURCE_DISPOSITION_AND_TARGET"},
 "PUBLICATIONS":{"table":"bpay_next_publication","parent":"WORK","fk":"work_id","keys":[["revision_no","bigint"]],"fields":"command_id,work_id,candidate_id,revision_id,predecessor_revision_id,revision_no,status,phase,applied_at_utc","state":"RECEIPT_CURRENT_STATUS"},
 "RUN_WORKS":{"table":"bpay_next_run_work","parent":"WORK","fk":"work_id","keys":[["id","uuid"]],"fields":"id,run_worker_id,candidate_id,work_id,captured_revision_id,created_at_utc","state":"IMMUTABLE_CAPTURE"},
 "RUN_POSITIONS":{"table":"bpay_next_run_position","parent":"RUN_WORK","fk":"run_work_id","keys":[["component_key","text"]],"fields":"id,run_work_id,run_worker_id,work_id,captured_revision_id,component_key,approved_line_id,source_pay_channel,source_basis_channel,approved_source_ex_vat,realised_source_ex_vat,held_source_ex_vat,residual_source_ex_vat,realised_target_ex_vat,realised_target_vat,realised_target_inc_vat,held_target_ex_vat,held_target_vat,held_target_inc_vat,needs_financial_resolution,work_collection_id,work_collection_revision,managed_capture_completed,captured_at_utc","state":"FROZEN_SOURCE_AND_TARGET"},
 "RUN_LINES":{"table":"bpay_next_run_line","parent":"RUN_WORK","fk":"run_work_id","keys":[["component_key","text"]],"fields":"id,run_worker_id,run_work_id,line_no,approved_line_id,captured_revision_id,work_id,component_key,source_consumed_ex_vat,source_pay_channel,target_pay_channel,valuation_policy_id,policy_window_id,frozen_ex_vat,frozen_vat,frozen_inc_vat","state":"FROZEN_SOURCE_CONSUMPTION_AND_TARGET"},
 "HOLD":{"table":"bpay_next_hold","parent":"RUN_LINE","fk":"run_line_id","keys":[["id","uuid"]],"fields":"id,run_line_id,work_id,component_key,source_reserved_ex_vat,target_amount_ex_vat,target_amount_vat,target_amount_inc_vat,status,created_at_utc,finished_at_utc","state":"SOURCE_AND_TARGET_HOLD_CURRENT_STATUS"},
 "WORKER":{"table":"bpay_next_run_worker","parent":"WORKER","fk":"id","keys":[["id","uuid"]],"fields":"id,run_id,candidate_id,target_pay_channel,target_umbrella_id,target_umbrella_vat_chargeable,status,gross_ex_vat,gross_vat,gross_inc_vat,entered_paye_net,net_request_revision,net_projection_revision,preparation_revision,case_selection_revision,captured_line_count,captured_position_count,review_issue_code,review_issue_work_id","state":"WHOLE_WORKER_NET_CURRENT_STATUS"},
 "PROJECTIONS":{"table":"bpay_next_net_projection","parent":"WORKER","fk":"run_worker_id","keys":[["projection_no","bigint"]],"fields":"id,run_worker_id,request_command_id,projection_no,input_kind,gross_ex_vat,gross_vat,gross_inc_vat,entered_paye_net,accepted_recoveries,accepted_net_additions,accepted_gross_additions,accepted_gross_deductions,cash_amount,accepted_at_utc,retired_at_utc","state":"WHOLE_WORKER_FROZEN_NET_CASH"},
 "TRANSFERS":{"table":"bpay_next_transfer","parent":"WORKER","fk":"run_worker_id","keys":[["transfer_no","bigint"]],"fields":"id,run_worker_id,candidate_id,projection_id,build_command_id,transfer_no,beneficiary_kind,beneficiary_id,destination_rail,currency,cash_amount,member_count,member_cash_sum,status,original_transfer_id,return_cash_id,execution_kind,created_at_utc","state":"WHOLE_TRANSFER_CASH_CURRENT_STATUS"},
 "MEMBERS":{"table":"bpay_next_transfer_member","parent":"TRANSFER","fk":"transfer_id","keys":[["member_no","bigint"]],"fields":"transfer_id,member_no,run_worker_id,subject_kind,run_line_id,case_hold_id,case_instruction_id,case_allocation_result_id,signed_cash_contribution","state":"TRANSFER_MEMBER_CONTRIBUTION_NOT_TIMESHEET_ALLOCATION"},
 "OUTCOMES":{"table":"bpay_next_transfer_outcome","parent":"TRANSFER","fk":"transfer_id","keys":[["occurred_at_utc","timestamptz"],["id","uuid"]],"fields":"id,transfer_id,receipt_id,outcome_kind,whole_transfer_amount,occurred_at_utc,received_at_utc","state":"WHOLE_TRANSFER_IMMUTABLE_OUTCOME"},
 "RETURN_CASH":{"table":"bpay_next_return_cash","parent":"TRANSFER","fk":"original_transfer_id","keys":[["id","uuid"]],"fields":"id,original_transfer_id,candidate_id,amount_owed,amount_held,amount_reissued_paid,created_at_utc","state":"CURRENT_RETURN_CASH_NOT_SOURCE_DISPOSITION"},
 "CSV":{"table":"bpay_next_csv_instruction","parent":"TRANSFER","fk":"transfer_id","keys":[["id","uuid"]],"fields":"id,transfer_id,run_worker_id,actor_user_id,cash_amount,csv_sha256,issued_at_utc","state":"WHOLE_TRANSFER_IMMUTABLE_CSV_RECEIPT"},
 "INTERNAL_RECEIPT":{"table":"bpay_next_internal_receipt","parent":"TRANSFER","fk":"transfer_id","keys":[["id","uuid"]],"fields":"id,command_id,transfer_id,run_worker_id,candidate_id,actor_user_id,projection_id,execution_kind,cash_amount,occurred_at_utc,received_at_utc","state":"WHOLE_TRANSFER_IMMUTABLE_INTERNAL_RECEIPT"},
 "GROUP_LEGS":{"table":"bpay_next_destination_group_leg","parent":"TRANSFER","fk":"anchor_transfer_id","keys":[["transfer_id","uuid"]],"fields":"anchor_transfer_id,transfer_id,leg_kind,destination_id","state":"TRANSFER_GROUP_PROVENANCE"},
 "EXPIRY":{"table":"bpay_next_preparation_expiry_worker","parent":"WORKER","fk":"run_worker_id","keys":[["command_id","uuid"]],"fields":"command_id,candidate_id,run_id,run_worker_id,job_id,member_no,draft_state_id,preparation_revision,selection_revision,original_job_id,original_job_status,original_job_phase,original_worker_status,original_review_issue_code,original_review_issue_work_id,expected_work_count,expected_case_hold_count,released_work_count,released_case_hold_count,status,stage,checkpoint,created_at_utc,started_at_utc,finished_at_utc","state":"EXPIRY_RECEIPT_CURRENT_STATUS"},
 "CASES":{"table":"bpay_next_finance_case","parent":"SCOPE","fk":"candidate_id","keys":[["id","uuid"]],"fields":"id,candidate_id,case_kind,tax_treatment,status,principal_approved,principal_funded,principal_recovered,principal_written_off,principal_paid_credit,active_hold_amount,active_payout_hold_amount,active_recovery_hold_amount,current_rule_id,current_rule_revision,case_revision,due_date,updated_at_utc","state":"CURRENT_CASE_SOURCE_CACHE"},
 "COMPONENTS":{"table":"bpay_next_case_component","parent":"CASE","fk":"case_id","keys":[["component_ordinal","bigint"]],"fields":"id,case_id,candidate_id,component_key,component_ordinal,component_revision,rule_id,case_kind,case_subtype,tax_treatment,instruction_kind,direction,payroll_stage,source_pay_channel,currency,approved_source_ex_vat,funded_source_ex_vat,recovered_source_ex_vat,paid_credit_source_ex_vat,written_off_source_ex_vat,active_payout_source_ex_vat,active_recovery_source_ex_vat,resolution_state,target_pay_channel,target_ex_vat,target_vat,target_inc_vat,valuation_policy_id,valuation_window_id,resolution_ref,updated_at_utc","state":"CURRENT_CASE_SOURCE_AND_TARGET_CACHE"},
 "RULES":{"table":"bpay_next_case_rule","parent":"CASE","fk":"case_id","keys":[["rule_revision","bigint"]],"fields":"id,case_id,candidate_id,rule_revision,case_kind,case_subtype,tax_treatment,case_created_at_utc,original_funded_at_utc,original_funding_event_id,schedule_start_monday,schedule_week_count,weekly_due_source_ex_vat,minimum_earnings_threshold,take_home_floor_override,next_due_monday,captured_at_utc","state":"CAPTURED_CASE_RULE"},
 "CASE_EVENTS":{"table":"bpay_next_case_event","parent":"CASE","fk":"case_id","keys":[["occurred_at_utc","timestamptz"],["id","uuid"]],"fields":"id,case_id,operation_id,operation_item_id,event_kind,approved_delta,funded_delta,recovered_delta,written_off_delta,paid_credit_delta,original_transfer_id,original_effect_id,occurred_at_utc,candidate_id,case_component_id,instruction_id,allocation_result_id,run_worker_id,event_case_kind,hold_purpose,pay_week_start,source_amount_ex_vat,target_amount_ex_vat,target_amount_vat,target_amount_inc_vat,shortfall_source_ex_vat,cap_reason","state":"IMMUTABLE_CASE_SOURCE_AND_TARGET_EVENT"},
 "INSTRUCTIONS":{"table":"bpay_next_run_case_instruction","parent":"COMPONENT","fk":"case_component_id","keys":[["pay_week_start","date"],["id","uuid"]],"fields":"id,run_worker_id,run_id,candidate_id,case_id,case_component_id,component_ordinal,rule_id,rule_revision,captured_case_revision,captured_component_revision,captured_period_revision,preparation_revision,selection_revision,is_selected,case_kind,case_subtype,tax_treatment,instruction_kind,direction,payroll_stage,hold_purpose,allocation_channel,source_pay_channel,target_pay_channel,currency,nominal_source_ex_vat,capacity_source_ex_vat,nominal_target_ex_vat,nominal_target_vat,nominal_target_inc_vat,capacity_target_ex_vat,capacity_target_vat,capacity_target_inc_vat,valuation_policy_id,valuation_window_id,pay_week_start,minimum_earnings_threshold,resolved_take_home_floor,resolution_ref,explanation,captured_at_utc,work_collection_id","state":"FROZEN_CASE_SOURCE_AND_TARGET_INSTRUCTION"},
 "CASE_HOLDS":{"table":"bpay_next_case_hold","parent":"COMPONENT","fk":"case_component_id","keys":[["pay_week_start","date"],["id","uuid"]],"fields":"id,run_worker_id,candidate_id,case_id,case_component_id,instruction_id,projection_id,allocation_result_id,purpose,pay_week_start,amount,source_reserved_ex_vat,target_amount_ex_vat,target_amount_vat,target_amount_inc_vat,status,created_at_utc,finished_at_utc","state":"CASE_SOURCE_AND_TARGET_HOLD_CURRENT_STATUS"},
 "RESULTS":{"table":"bpay_next_case_allocation_result","parent":"INSTRUCTION","fk":"instruction_id","keys":[["state_id","uuid"]],"fields":"id,state_id,instruction_id,result_no,run_worker_id,candidate_id,preparation_revision,selection_revision,pass_kind,hold_purpose,allocation_channel,nominal_target_ex_vat,usable_capacity_target_ex_vat,allocated_source_ex_vat,allocated_target_ex_vat,allocated_target_vat,allocated_target_inc_vat,shortfall_target_ex_vat,cap_reason,affordability_reason,capacity_reason,channel_headroom_before,channel_headroom_after,worker_take_home_before,worker_take_home_after,created_at_utc","state":"FROZEN_CASE_ALLOCATION_SOURCE_AND_TARGET"},
 "COMMANDS":{"table":"bpay_next_command_member","parent":"SCOPE","fk":"candidate_id","keys":[["command_id","uuid"]],"fields":"command_id,candidate_id,member_no","state":"COMMAND_MEMBERSHIP"},
 "COMMAND":{"table":"bpay_next_command","parent":"COMMAND","fk":"id","keys":[["id","uuid"]],"fields":"id,agency_sequence,module_epoch,command_kind,status,expected_member_count,enrolled_member_count,created_at_utc,sealed_at_utc","state":"COMMAND_RECEIPT_CURRENT_STATUS"},
 "JOBS":{"table":"bpay_next_job","parent":"COMMAND","fk":"command_id","keys":[["job_kind","text"]],"fields":"id,command_id,candidate_id,command_sequence,job_kind,status,phase,attempt_count,applied_line_count,last_error_code,available_at_utc,queue_wake_after_utc","state":"JOB_RECEIPT_CURRENT_STATUS"},
 "NET_REQUEST":{"table":"bpay_next_paye_net_request","parent":"COMMAND","fk":"command_id","keys":[["command_id","uuid"]],"fields":"command_id,run_worker_id,candidate_id,actor_user_id,request_no,input_kind,entered_paye_net,frozen_gross_inc_vat,expected_projection_revision,preparation_revision,selection_revision,case_draft_state_id,accepted_at_utc","state":"WHOLE_WORKER_IMMUTABLE_NET_REQUEST"},
 "OUTCOME_REQUEST":{"table":"bpay_next_outcome_request","parent":"COMMAND","fk":"command_id","keys":[["command_id","uuid"]],"fields":"command_id,outcome_id,transfer_id,run_worker_id,candidate_id,actor_user_id,posted_member_count,posting_complete,internal_receipt_id","state":"OUTCOME_RECEIPT_CURRENT_STATUS"},
 "RETURN_REQUEST":{"table":"bpay_next_return_request","parent":"COMMAND","fk":"command_id","keys":[["command_id","uuid"]],"fields":"command_id,outcome_id,transfer_id,run_worker_id,candidate_id,actor_user_id,posting_complete","state":"RETURN_RECEIPT_CURRENT_STATUS"},
 "REISSUE_REQUEST":{"table":"bpay_next_reissue_request","parent":"COMMAND","fk":"command_id","keys":[["command_id","uuid"]],"fields":"command_id,return_cash_id,run_worker_id,candidate_id,actor_user_id,transfer_id","state":"REISSUE_RECEIPT_CURRENT_STATUS"},
 "CANCEL_REQUEST":{"table":"bpay_next_cancel_request","parent":"COMMAND","fk":"command_id","keys":[["command_id","uuid"]],"fields":"command_id,run_worker_id,candidate_id,status,expected_line_count,released_line_count,blocked_code,started_at_utc,finished_at_utc","state":"CANCEL_RECEIPT_CURRENT_STATUS"},
 "REISSUE_CANCEL":{"table":"bpay_next_reissue_cancel_request","parent":"COMMAND","fk":"command_id","keys":[["command_id","uuid"]],"fields":"command_id,transfer_id,return_cash_id,run_worker_id,candidate_id,actor_user_id,amount,status,blocked_code,accepted_at_utc,completed_at_utc","state":"WHOLE_TRANSFER_REISSUE_CANCEL_RECEIPT"},
 "WRITE_OFF":{"table":"bpay_next_write_off_request","parent":"COMMAND","fk":"command_id","keys":[["command_id","uuid"]],"fields":"command_id,candidate_id,case_id,case_component_id,work_collection_id,actor_user_id,expected_component_revision,basis_case_revision,basis_principal,basis_recovered,basis_written_off,basis_protected,requested_scope,input_amount,requested_amount,reason,accepted_at_utc,status,applied_amount,remaining_amount,protected_amount,event_id,issue_code,completed_at_utc","state":"SOURCE_FORGIVENESS_NOT_PAID_DISPOSITION"},
 "CASE_CREATE":{"table":"bpay_next_case_create_request","parent":"COMMAND","fk":"command_id","keys":[["command_id","uuid"]],"fields":"command_id,case_id,candidate_id,actor_user_id,case_kind,case_subtype,tax_treatment,source_pay_channel,currency,principal_source_ex_vat,due_date,minimum_earnings_threshold,take_home_floor_override,approval_reason,rule_id,primary_component_id,recovery_component_id,valuation_policy_id,opening_pay_week_start,schedule_week_count,weekly_due_source_ex_vat,accepted_at_utc","state":"IMMUTABLE_CASE_APPROVAL_RECEIPT"},
 "EXPIRY_REQUEST":{"table":"bpay_next_preparation_expiry_request","parent":"COMMAND","fk":"command_id","keys":[["command_id","uuid"]],"fields":"command_id,run_id,original_prepare_command_id,actor_user_id,initiator_kind,captured_deadline,expected_candidate_count,enrolled_candidate_count,completed_candidate_count,status,accepted_at_utc,finished_at_utc","state":"EXPIRY_COMMAND_RECEIPT_CURRENT_STATUS"},
 "CASE":{"table":"bpay_next_finance_case","parent":"CASE","fk":"id","keys":[["id","uuid"]],"fields":"id,candidate_id,case_kind,tax_treatment,status,principal_approved,principal_funded,principal_recovered,principal_written_off,principal_paid_credit,active_hold_amount,active_payout_hold_amount,active_recovery_hold_amount,current_rule_id,current_rule_revision,case_revision,due_date,updated_at_utc","state":"CURRENT_CASE_SOURCE_CACHE"},
 "PROTECTED_RECEIPT":{"table":"bpay_next_protected_source_receipt","parent":"REVISION","fk":"revision_id","keys":[["orchestration_run_id","uuid"]],"fields":"orchestration_run_id,family_id,generation_id,approval_id,accepted_family_bound_version,source_manifest_sha256,prepared_request_sha256,publication_request_sha256,work_id,revision_id,command_id,agency_sequence,actor_user_id,accepted_at_utc","state":"IMMUTABLE_APPROVAL_PROVENANCE"},
 "SOURCE_DETAIL":{"table":"bpay_next_source_chosen_detail","parent":"LINE","fk":"component_id","keys":[["component_id","uuid"]],"fields":"head_id,component_id,decision_bundle_id,bundle_revision,component_sha256,detail_sha256,created_at_utc","state":"IMMUTABLE_SOURCE_CERTIFICATE_REFERENCES_NOT_RAW_PAYLOAD"},
 "POLICY":{"table":"bpay_next_valuation_policy_window","parent":"RUN_LINE","fk":"policy_id","keys":[["source_window_id","uuid"]],"fields":"policy_id,source_window_id,date_from,date_to,erni_pct,vat_rate_pct","state":"IMMUTABLE_CAPTURED_POLICY_NOT_LIVE_SETTINGS"},
 "CHOICE":{"table":"bpay_next_work_choice","parent":"RUN_WORK","fk":"work_id","keys":[["work_id","uuid"]],"fields":"run_id,work_id,expected_revision_id,selection_mode,selection_state,page_count,component_count","state":"CAPTURED_SELECTION_CURRENT_STATUS"},
 "SELECTION":{"table":"bpay_next_selected_component","parent":"RUN_WORK","fk":"work_id","keys":[["selection_no","bigint"]],"fields":"run_id,work_id,approved_line_id,expected_revision_id,component_key,selection_no,page_no,page_item_no","state":"CAPTURED_COMPONENT_SELECTION"},
 "WORKER_CASE_HOLDS":{"table":"bpay_next_case_hold","parent":"WORKER","fk":"run_worker_id","keys":[["id","uuid"]],"fields":"id,run_worker_id,candidate_id,case_id,case_component_id,instruction_id,projection_id,allocation_result_id,purpose,pay_week_start,amount,source_reserved_ex_vat,target_amount_ex_vat,target_amount_vat,target_amount_inc_vat,status,created_at_utc,finished_at_utc","state":"HOLD_CURRENT_STATUS_UNTYPED_SOURCE_TARGET_MAY_BE_NULL"},
 "COMMAND_WORKER":{"table":"bpay_next_run_worker","parent":"COMMAND","fk":"run_id","keys":[["id","uuid"]],"fields":"id,run_id,candidate_id,target_pay_channel,target_umbrella_id,target_umbrella_vat_chargeable,status,gross_ex_vat,gross_vat,gross_inc_vat,entered_paye_net,net_request_revision,net_projection_revision,preparation_revision,case_selection_revision,captured_line_count,captured_position_count,review_issue_code,review_issue_work_id","state":"WHOLE_WORKER_NET_CURRENT_STATUS"}
}
$history$::jsonb;
  v_actor uuid; v_candidate uuid; v_timesheet uuid; v_work uuid; v_parent uuid; v_context uuid;
  v_section text; v_component text; v_limit integer; v_def jsonb; v_kind text;
  v_candidate_check uuid; v_work_check uuid; v_worker uuid; v_case uuid; v_extra uuid;
  v_run uuid; v_source_component uuid; v_source_head uuid; v_policy uuid; v_window uuid;
  v_scope uuid; v_where text; v_order text; v_tuple text; v_key_sql text;
  v_after_sql text; v_upper_sql text; v_fields text; v_column text; v_key jsonb;
  v_after jsonb; v_upper jsonb; v_i integer; v_text text; v_type text;
  v_query text; v_item jsonb; v_rows jsonb := '[]'::jsonb; v_last jsonb; v_cursor jsonb;
  v_scope_kind text; v_scope_id uuid;
  v_more boolean := false; v_result jsonb; v_count integer := 0;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
    or p_request is null or pg_catalog.jsonb_typeof(p_request) is distinct from 'object'
    or pg_catalog.octet_length(p_request::text)>16384 then
    raise exception using errcode='42501',message='BPAY_NEXT_HISTORY_FORBIDDEN';
  end if;
  if not p_request ?& array['version','actor_user_id','candidate_id','timesheet_id','section',
      'parent_id','context_id','component_key','after','upper','limit']
    or p_request-array['version','actor_user_id','candidate_id','timesheet_id','section',
      'parent_id','context_id','component_key','after','upper','limit']<>'{}'::jsonb
    or p_request->>'version' is distinct from 'PAYMENT_HISTORY_V1'
    or pg_catalog.jsonb_typeof(p_request->'limit') is distinct from 'number'
    or p_request->>'limit' !~ '^([1-9][0-9]?|100)$'
    or pg_catalog.jsonb_typeof(p_request->'section') is distinct from 'string'
    or pg_catalog.jsonb_typeof(p_request->'component_key') not in ('string','null')
    or (p_request->'candidate_id'='null'::jsonb)=(p_request->'timesheet_id'='null'::jsonb) then
    raise exception using errcode='22023',message='BPAY_NEXT_HISTORY_REQUEST_INVALID';
  end if;
  foreach v_column in array array['actor_user_id','candidate_id','timesheet_id','parent_id','context_id'] loop
    if p_request->v_column='null'::jsonb and v_column<>'actor_user_id' then continue; end if;
    if pg_catalog.jsonb_typeof(p_request->v_column) is distinct from 'string'
      or p_request->>v_column !~ '^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_HISTORY_REQUEST_INVALID';
    end if;
  end loop;
  v_actor:=(p_request->>'actor_user_id')::uuid;
  if not exists(select 1 from public.tms_users u where u.id=v_actor
      and u.is_active is true and u.role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_HISTORY_FORBIDDEN';
  end if;
  v_candidate:=(p_request->>'candidate_id')::uuid; v_timesheet:=(p_request->>'timesheet_id')::uuid;
  v_scope_kind:=case when v_timesheet is null then 'CANDIDATE' else 'TIMESHEET' end;
  v_scope_id:=coalesce(v_timesheet,v_candidate);
  if v_timesheet is not null then
    -- Two existing unique keys; no historical chooser or current-authorised filter.
    select w.id,w.candidate_id into v_work,v_candidate from private.bpay_next_work w
      where w.original_timesheet_id=v_timesheet;
    if not found then
      select w.id,w.candidate_id into v_work,v_candidate from public.timesheets t
        join private.bpay_next_work w on w.booking_id=t.booking_id
        where t.timesheet_id=v_timesheet;
    end if;
    if v_work is null then
      raise exception using errcode='22023',message='BPAY_NEXT_HISTORY_SCOPE_NOT_FOUND';
    end if;
  elsif not exists(select 1 from public.candidates c where c.id=v_candidate) then
    raise exception using errcode='22023',message='BPAY_NEXT_HISTORY_SCOPE_NOT_FOUND';
  end if;
  v_section:=p_request->>'section'; v_parent:=(p_request->>'parent_id')::uuid;
  v_context:=(p_request->>'context_id')::uuid;
  v_component:=p_request->>'component_key'; v_limit:=(p_request->>'limit')::integer;
  v_after:=p_request->'after'; v_upper:=p_request->'upper';
  if v_section='MANIFEST' then
    if v_parent is not null or v_context is not null or v_component is not null or v_after<>'null'::jsonb or v_upper<>'null'::jsonb then
      raise exception using errcode='22023',message='BPAY_NEXT_HISTORY_REQUEST_INVALID';
    end if;
    select jsonb_agg(jsonb_build_object('section',k,'parent',d->>'parent','keys',d->'keys',
      'fields',string_to_array(d->>'fields',','),'amount_scope',d->>'state',
      'applicability',case when v_work is not null and k in ('COMMANDS','CASES') then 'CANDIDATE_ONLY_USE_REFERENCED_PARENT'
        else 'SCOPE_QUALIFIED' end) order by k) into v_rows from jsonb_each(v_sections) as s(k,d);
    return jsonb_build_object('version','PAYMENT_HISTORY_V1','candidate_id',v_candidate,'work_id',v_work,
      'sections',v_rows,'terminal',true,'next_cursor',null,'consistency','LIVE_KEYSET_REFRESH_FOR_LATE_COMMIT',
      'scalar_encoding','STRING_OR_NULL','unavailable',array['MOVED_ROOT_FLAT_PAID_BINDING',
        'CANONICAL_MULTI_CHANNEL_VECTOR','MOVE_COMPARISON_RECEIPT','GENERAL_APPROVAL_ACTOR_REASON_NOT_CAPTURED_IN_NEXT_ROWS']
        ||case when v_work is not null then array['TIMESHEET_COMMAND_WITHOUT_WORK_OR_CASE_ANCESTRY'] else array[]::text[] end);
  end if;
  v_def:=v_sections->v_section;
  if v_def is null or (v_section='EFFECTS' and (v_component is null or octet_length(v_component) not between 1 and 256))
    or (v_section<>'EFFECTS' and v_component is not null) then
    raise exception using errcode='22023',message='BPAY_NEXT_HISTORY_REQUEST_INVALID';
  end if;
  v_kind:=v_def->>'parent';
  if (v_kind='SCOPE')<>(v_parent is null) or (v_work is not null and v_section in ('COMMANDS','CASES'))
    or (v_context is not null and (v_work is null or v_kind not in ('WORKER','TRANSFER','COMMAND'))) then
    raise exception using errcode='22023',message='BPAY_NEXT_HISTORY_PARENT_INVALID';
  end if;
  if v_after<>'null'::jsonb then
    if jsonb_typeof(v_after) is distinct from 'object'
      or not v_after ?& array['version','scope_kind','scope_id','section','parent_id',
        'context_id','component_key','upper','key']
      or v_after-array['version','scope_kind','scope_id','section','parent_id',
        'context_id','component_key','upper','key']<>'{}'::jsonb
      or v_after->>'version' is distinct from 'PAYMENT_HISTORY_CURSOR_V1'
      or v_after->>'scope_kind' is distinct from v_scope_kind
      or v_after->>'scope_id' is distinct from v_scope_id::text
      or v_after->>'section' is distinct from v_section
      or v_after->'parent_id' is distinct from p_request->'parent_id'
      or v_after->'context_id' is distinct from p_request->'context_id'
      or v_after->'component_key' is distinct from p_request->'component_key'
      or v_after->'upper' is distinct from v_upper then
      raise exception using errcode='22023',message='BPAY_NEXT_HISTORY_CURSOR_INVALID';
    end if;
    v_after:=v_after->'key';
    if v_after='null'::jsonb then
      raise exception using errcode='22023',message='BPAY_NEXT_HISTORY_CURSOR_INVALID';
    end if;
  elsif v_upper<>'null'::jsonb then
    raise exception using errcode='22023',message='BPAY_NEXT_HISTORY_CURSOR_INVALID';
  end if;
  -- Exact indexed parent ancestry. Never authorise a child by its bare UUID.
  if v_kind='WORK' then
    select w.candidate_id,w.id into v_candidate_check,v_work_check from private.bpay_next_work w where w.id=v_parent;
  elsif v_kind='REVISION' then
    select w.candidate_id,w.id into v_candidate_check,v_work_check from private.bpay_next_work_revision r
      join private.bpay_next_work w on w.id=r.work_id where r.id=v_parent and r.sealed_at_utc is not null;
  elsif v_kind='LINE' then
    select w.candidate_id,w.id,l.source_component_id,r.source_head_id
      into v_candidate_check,v_work_check,v_source_component,v_source_head from private.bpay_next_approved_line l
      join private.bpay_next_work_revision r on r.id=l.revision_id join private.bpay_next_work w on w.id=r.work_id
      where l.id=v_parent and r.sealed_at_utc is not null;
  elsif v_kind='SHIFT' then
    select w.candidate_id,w.id into v_candidate_check,v_work_check from private.bpay_next_shift_detail s
      join private.bpay_next_approved_line l on l.id=s.approved_line_id join private.bpay_next_work_revision r on r.id=l.revision_id
      join private.bpay_next_work w on w.id=r.work_id where s.id=v_parent and r.sealed_at_utc is not null;
  elsif v_kind='RUN_WORK' then
    select w.candidate_id,w.id,rw.run_id into v_candidate_check,v_work_check,v_run from private.bpay_next_run_work r
      join private.bpay_next_work w on w.id=r.work_id join private.bpay_next_run_worker rw on rw.id=r.run_worker_id
      where r.id=v_parent;
  elsif v_kind='RUN_LINE' then
    select w.candidate_id,w.id,r.valuation_policy_id,r.policy_window_id
      into v_candidate_check,v_work_check,v_policy,v_window from private.bpay_next_run_line r
      join private.bpay_next_work w on w.id=r.work_id where r.id=v_parent;
  elsif v_kind='WORKER' then
    select r.candidate_id,r.id into v_candidate_check,v_worker from private.bpay_next_run_worker r where r.id=v_parent;
  elsif v_kind='TRANSFER' then
    select t.candidate_id,t.run_worker_id into v_candidate_check,v_worker from private.bpay_next_transfer t where t.id=v_parent;
  elsif v_kind='CASE' then
    select c.candidate_id,c.id into v_candidate_check,v_case from private.bpay_next_finance_case c where c.id=v_parent;
  elsif v_kind='COMPONENT' then
    select c.candidate_id,c.case_id into v_candidate_check,v_case from private.bpay_next_case_component c where c.id=v_parent;
  elsif v_kind='INSTRUCTION' then
    select i.candidate_id,i.case_id into v_candidate_check,v_case from private.bpay_next_run_case_instruction i where i.id=v_parent;
  elsif v_kind='COMMAND' then
    select m.candidate_id into v_candidate_check from private.bpay_next_command_member m
      where m.command_id=v_parent and m.candidate_id=v_candidate;
    if v_work is not null then
      select p.work_id into v_work_check from private.bpay_next_publication p where p.command_id=v_parent and p.work_id=v_work;
      if v_work_check is null then
        -- Requests are exact primary-key points; their worker membership uses
        -- unique(run_worker_id,work_id). Does not allocate worker/transfer cash.
        select x.run_worker_id into v_worker from (
          select r.id as run_worker_id from private.bpay_next_run_command c
            join private.bpay_next_run_worker r on r.run_id=c.run_id and r.candidate_id=v_candidate where c.command_id=v_parent
          union all select n.run_worker_id from private.bpay_next_paye_net_request n where n.command_id=v_parent
          union all select o.run_worker_id from private.bpay_next_outcome_request o where o.command_id=v_parent
          union all select r.run_worker_id from private.bpay_next_return_request r where r.command_id=v_parent
          union all select r.run_worker_id from private.bpay_next_reissue_request r where r.command_id=v_parent
          union all select c.run_worker_id from private.bpay_next_cancel_request c where c.command_id=v_parent
          union all select c.run_worker_id from private.bpay_next_reissue_cancel_request c where c.command_id=v_parent
          union all select e.run_worker_id from private.bpay_next_preparation_expiry_worker e
            where e.command_id=v_parent and e.candidate_id=v_candidate
        ) x limit 1;
        if v_worker is null then
          select c.work_id into v_work_check from private.bpay_next_write_off_request r
            join private.bpay_next_work_collection c on c.id=r.work_collection_id where r.command_id=v_parent and c.work_id=v_work;
          if v_work_check is null then
            select c.work_id into v_work_check from private.bpay_next_case_create_request r
              join private.bpay_next_work_collection c on c.case_id=r.case_id where r.command_id=v_parent and c.work_id=v_work;
          end if;
        end if;
      end if;
    end if;
  end if;
  if v_context is not null then
    -- Exact immutable origin for a CASE-only worker, not an instruction scan.
    if not exists(select 1 from private.bpay_next_run_case_instruction i
      join private.bpay_next_work_collection c on c.case_id=i.case_id
      where i.id=v_context and i.candidate_id=v_candidate and i.run_worker_id=v_worker and c.work_id=v_work) then
      raise exception using errcode='42501',message='BPAY_NEXT_HISTORY_CONTEXT_FORBIDDEN';
    end if;
    v_work_check:=v_work;
  end if;
  if v_kind<>'SCOPE' and (v_candidate_check is distinct from v_candidate
    or (v_work is not null and v_work_check is distinct from v_work
      and not (v_worker is not null and exists(select 1 from private.bpay_next_run_work r
        where r.run_worker_id=v_worker and r.work_id=v_work))
      and not (v_case is not null and exists(select 1 from private.bpay_next_work_collection c
        where c.case_id=v_case and c.work_id=v_work)))) then
    raise exception using errcode='42501',message='BPAY_NEXT_HISTORY_PARENT_FORBIDDEN';
  end if;
  v_scope:=case when v_kind='SCOPE' then v_candidate else v_parent end;
  v_where:=format('t.%I=$1',v_def->>'fk');
  if v_section='WORKS' and v_work is not null then v_where:='t.id=$8'; end if;
  if v_section='SOURCE_DETAIL' then
    v_scope:=v_source_component;v_extra:=v_source_head;v_where:=v_where||' and t.head_id=$9';
  elsif v_section='POLICY' then
    v_scope:=v_policy;v_extra:=v_window;v_where:=v_where||' and t.source_window_id=$9';
  elsif v_section in ('CHOICE','SELECTION') then
    v_scope:=v_work_check;v_extra:=v_run;v_where:=v_where||' and t.run_id=$9';
  elsif v_section='COMMAND_WORKER' then
    select c.run_id into v_scope from private.bpay_next_run_command c where c.command_id=v_parent;
    v_where:=v_where||' and t.candidate_id=$7';
  end if;
  if v_section='REVISIONS' then v_where:=v_where||' and t.sealed_at_utc is not null'; end if;
  if v_section='EFFECTS' then v_where:=v_where||' and t.component_key=$4'; end if;
  if v_section='JOBS' then v_where:=v_where||' and t.candidate_id=$7'; end if;
  -- Validate every key type/shape before its closed SQL cast. No free SQL/order.
  foreach v_key in array array[v_after,v_upper] loop
    if v_key='null'::jsonb then continue; end if;
    if jsonb_typeof(v_key) is distinct from 'array' or jsonb_array_length(v_key)<>jsonb_array_length(v_def->'keys') then
      raise exception using errcode='22023',message='BPAY_NEXT_HISTORY_CURSOR_INVALID';
    end if;
    for v_i in 0..jsonb_array_length(v_key)-1 loop
      v_text:=v_key->>v_i; v_type:=v_def->'keys'->v_i->>1;
      if jsonb_typeof(v_key->v_i) is distinct from 'string' or octet_length(v_text) not between 1 and 256
        or (v_type='uuid' and v_text !~ '^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$')
        or (v_type in ('bigint','integer') and (v_text !~ '^(0|[1-9][0-9]{0,18})$' or v_text::numeric>9223372036854775807))
        or (v_type='integer' and v_text::numeric>2147483647)
        or (v_type='date' and (v_text !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' or not isfinite(v_text::date)
          or v_text::date::text<>v_text))
        or (v_type='timestamptz' and (v_text !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2} ' or not isfinite(v_text::timestamptz)
          or v_text::timestamptz::text<>v_text)) then
        raise exception using errcode='22023',message='BPAY_NEXT_HISTORY_CURSOR_INVALID';
      end if;
    end loop;
  end loop;
  if v_after<>'null'::jsonb and v_upper='null'::jsonb then
    raise exception using errcode='22023',message='BPAY_NEXT_HISTORY_CURSOR_INVALID';
  end if;
  v_order:=''; v_tuple:=''; v_key_sql:=''; v_after_sql:=''; v_upper_sql:=''; v_i:=0;
  for v_key in select value from jsonb_array_elements(v_def->'keys') loop
    if v_i>0 then v_order:=v_order||',';v_tuple:=v_tuple||',';v_key_sql:=v_key_sql||',';
      v_after_sql:=v_after_sql||',';v_upper_sql:=v_upper_sql||','; end if;
    v_order:=v_order||format('t.%I',v_key->>0); v_tuple:=v_tuple||format('t.%I',v_key->>0);
    v_key_sql:=v_key_sql||format('t.%I::text',v_key->>0);
    v_after_sql:=v_after_sql||format('($2->>%s)::%s',v_i,v_key->>1);
    v_upper_sql:=v_upper_sql||format('($3->>%s)::%s',v_i,v_key->>1);v_i:=v_i+1;
  end loop;
  v_fields:='';
  foreach v_column in array string_to_array(v_def->>'fields',',') loop
    if v_fields<>'' then v_fields:=v_fields||','; end if;
    v_fields:=v_fields||format('%L,t.%I::text',v_column,v_column);
  end loop;
  if v_section='POSITIONS' then
    v_fields:=v_fields||','||quote_literal('current_residual_source_ex_vat')||',
      (t.approved_source_ex_vat-t.realised_source_ex_vat-t.held_source_ex_vat)::text';
  end if;
  if v_upper='null'::jsonb then
    -- An indexed last key, not COUNT/history hash. DESC applies to EACH key.
    select string_agg(format('t.%I desc',value->>0),',' order by ord) into v_text
      from jsonb_array_elements(v_def->'keys') with ordinality as k(value,ord);
    execute format('select jsonb_build_array(%s) from private.%I t where %s order by %s limit 1',
      v_key_sql,v_def->>'table',v_where,v_text) into v_upper
      using v_scope,v_after,v_upper,v_component,v_limit,v_parent,v_candidate,v_work,v_extra;
    if v_upper is null then v_upper:='null'::jsonb; end if;
  end if;
  -- Execute even an empty section: the actual full allowlist expression is
  -- resolved/compiled, while a NULL upper tuple safely yields zero data rows.
  v_query:=format('select jsonb_build_object(''key'',jsonb_build_array(%s),''data'',jsonb_build_object(%s))
      from private.%I t where %s and ($2=''null''::jsonb or row(%s)>row(%s))
      and row(%s)<=row(%s) order by %s limit $5',v_key_sql,v_fields,v_def->>'table',v_where,
      v_tuple,v_after_sql,v_tuple,v_upper_sql,v_order);
  for v_item in execute v_query using v_scope,v_after,v_upper,v_component,v_limit+1,v_parent,v_candidate,v_work,v_extra loop
      if v_count=v_limit then v_more:=true; exit; end if;
      if octet_length((v_rows||jsonb_build_array(v_item))::text)>100000 then
        if v_count=0 then raise exception using errcode='54000',message='BPAY_NEXT_HISTORY_ROW_TOO_LARGE'; end if;
        v_more:=true;exit;
      end if;
      v_rows:=v_rows||jsonb_build_array(v_item);v_last:=v_item->'key';v_count:=v_count+1;
  end loop;
  if v_more then
    v_cursor:=jsonb_build_object('version','PAYMENT_HISTORY_CURSOR_V1','scope_kind',v_scope_kind,
      'scope_id',v_scope_id,'section',v_section,'parent_id',v_parent,'context_id',v_context,
      'component_key',v_component,'upper',v_upper,'key',v_last);
  end if;
  v_result:=jsonb_build_object('version','PAYMENT_HISTORY_V1','candidate_id',v_candidate,'work_id',v_work,
    'section',v_section,'parent_id',v_parent,'context_id',v_context,'component_key',v_component,'amount_scope',v_def->>'state',
    'consistency','LIVE_KEYSET_REFRESH_FOR_LATE_COMMIT','rows',v_rows,'upper',v_upper,
    'terminal',not v_more,'next_cursor',v_cursor);
  if octet_length(v_result::text)>120000 then
    raise exception using errcode='54000',message='BPAY_NEXT_HISTORY_RESPONSE_TOO_LARGE';
  end if;
  return v_result;
end
$function$;

alter function public.bpay_next_payment_history_page_v1(jsonb) owner to postgres;
revoke all on function public.bpay_next_payment_history_page_v1(jsonb) from public,anon,authenticated;
grant execute on function public.bpay_next_payment_history_page_v1(jsonb) to service_role;
notify pgrst,'reload schema';

commit;
