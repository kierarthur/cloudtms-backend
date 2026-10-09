/**
 * Explicit Office payment-history read; no activation, queue, financial writer
 * or money calculation. The service-only SQL reader rechecks active admin and
 * exact ancestry on EVERY page. Trusted broker injects user.id, never a browser
 * actor. Scalar history facts remain string/null. LIVE keysets require refresh
 * for late commits; no claimed cross-request snapshot or complete moved-root
 * financial binding. One request fetches one bounded section page only.
 */
export const BPAY_NEXT_HISTORY_LIMITS = Object.freeze({
  requestBytes: 12 * 1024, responseBytes: 128 * 1024, pageRows: 100, timeoutMs: 8000
});
// Mechanical allowlists from frozen 1206 registry; focused test proves parity.
const RAW_SECTIONS = {"WORKS":{"parent":"SCOPE","keys":[["week_ending_date","date"],["id","uuid"]],"fields":["id","candidate_id","contract_id","original_timesheet_id","booking_id","work_kind","week_ending_date","approval_state","current_revision_id","applied_revision_id","current_revision_no","created_at_utc","updated_at_utc"],"amount_scope":"CURRENT"},
"REVISIONS":{"parent":"WORK","keys":[["revision_no","bigint"]],"fields":["id","work_id","revision_no","source_kind","source_head_id","source_event_id","physical_timesheet_id","physical_timesheet_version","financial_snapshot_id","source_pay_channel","valuation_policy_id","currency","week_ending_date","detail_kind","expected_line_count","expected_rate_schedule_count","certified_zero","approved_source_ex_vat","source_inventory_digest","approved_at_utc","sealed_at_utc","created_at_utc"],"amount_scope":"IMMUTABLE_SOURCE"},
"LINES":{"parent":"REVISION","keys":[["line_no","bigint"]],"fields":["id","revision_id","line_no","component_key","component_kind","tax_treatment","source_component_id","work_date","approved_quantity","approved_unit_rate","unit_label","source_pay_ex_vat","evidence_ref","expected_rate_detail_count","created_at_utc"],"amount_scope":"IMMUTABLE_SOURCE"},
"SHIFTS":{"parent":"LINE","keys":[["detail_no","integer"]],"fields":["id","approved_line_id","detail_no","work_date","shift_start_at","shift_end_at","shift_start_local","shift_end_local","shift_overnight","submitted_minutes","approved_minutes","approved_hours","hours_day","hours_night","hours_sat","hours_sun","hours_bh","pay_excluded","segment_pay_ex_vat","detail_label","immutable_evidence_ref"],"amount_scope":"IMMUTABLE_SOURCE"},
"BREAKS":{"parent":"SHIFT","keys":[["break_no","integer"]],"fields":["id","shift_detail_id","break_no","break_minutes","break_start_at","break_end_at","break_start_local","break_end_local"],"amount_scope":"IMMUTABLE_SOURCE"},
"RATES":{"parent":"LINE","keys":[["bucket","text"]],"fields":["id","approved_line_id","bucket","approved_hours","source_pay_rate"],"amount_scope":"IMMUTABLE_SOURCE"},
"SCHEDULE":{"parent":"REVISION","keys":[["rate_family","text"],["rate_code","text"]],"fields":["id","revision_id","rate_family","rate_code","paye_rate","umbrella_rate","charge_rate","unit_label"],"amount_scope":"IMMUTABLE_SOURCE"},
"POSITIONS":{"parent":"WORK","keys":[["component_key","text"]],"fields":["work_id","component_key","applied_revision_id","source_basis_channel","approved_source_ex_vat","realised_source_ex_vat","held_source_ex_vat","realised_target_ex_vat","realised_target_vat","realised_target_inc_vat","held_target_ex_vat","held_target_vat","held_target_inc_vat","updated_at_utc"],"amount_scope":"CURRENT_SOURCE_AND_TARGET_CACHE"},
"COLLECTIONS":{"parent":"WORK","keys":[["component_key","text"]],"fields":["id","work_id","component_key","candidate_id","original_effect_id","original_run_line_id","original_revision_id","original_approved_line_id","original_transfer_id","original_source_pay_channel","original_valuation_policy_id","original_tax_treatment","paid_basis_qualified","qualification_state","observed_revision_id","observed_approved_source_ex_vat","observed_realised_source_ex_vat","case_id","case_component_id","reconcile_revision","last_job_id","last_effect_id","created_at_utc","updated_at_utc"],"amount_scope":"CURRENT_COLLECTION_CACHE"},
"EFFECTS":{"parent":"WORK","keys":[["occurred_at_utc","timestamptz"],["id","uuid"]],"fields":["id","operation_id","operation_item_id","effect_kind","candidate_id","work_id","component_key","case_id","original_timesheet_id","original_transfer_id","source_disposition_ex_vat","target_amount_ex_vat","target_amount_vat","target_amount_inc_vat","occurred_at_utc"],"amount_scope":"IMMUTABLE_SOURCE_DISPOSITION_AND_TARGET"},
"PUBLICATIONS":{"parent":"WORK","keys":[["revision_no","bigint"]],"fields":["command_id","work_id","candidate_id","revision_id","predecessor_revision_id","revision_no","status","phase","applied_at_utc"],"amount_scope":"RECEIPT_CURRENT_STATUS"},
"RUN_WORKS":{"parent":"WORK","keys":[["id","uuid"]],"fields":["id","run_worker_id","candidate_id","work_id","captured_revision_id","created_at_utc"],"amount_scope":"IMMUTABLE_CAPTURE"},
"RUN_POSITIONS":{"parent":"RUN_WORK","keys":[["component_key","text"]],"fields":["id","run_work_id","run_worker_id","work_id","captured_revision_id","component_key","approved_line_id","source_pay_channel","source_basis_channel","approved_source_ex_vat","realised_source_ex_vat","held_source_ex_vat","residual_source_ex_vat","realised_target_ex_vat","realised_target_vat","realised_target_inc_vat","held_target_ex_vat","held_target_vat","held_target_inc_vat","needs_financial_resolution","work_collection_id","work_collection_revision","managed_capture_completed","captured_at_utc"],"amount_scope":"FROZEN_SOURCE_AND_TARGET"},
"RUN_LINES":{"parent":"RUN_WORK","keys":[["component_key","text"]],"fields":["id","run_worker_id","run_work_id","line_no","approved_line_id","captured_revision_id","work_id","component_key","source_consumed_ex_vat","source_pay_channel","target_pay_channel","valuation_policy_id","policy_window_id","frozen_ex_vat","frozen_vat","frozen_inc_vat"],"amount_scope":"FROZEN_SOURCE_CONSUMPTION_AND_TARGET"},
"HOLD":{"parent":"RUN_LINE","keys":[["id","uuid"]],"fields":["id","run_line_id","work_id","component_key","source_reserved_ex_vat","target_amount_ex_vat","target_amount_vat","target_amount_inc_vat","status","created_at_utc","finished_at_utc"],"amount_scope":"SOURCE_AND_TARGET_HOLD_CURRENT_STATUS"},
"WORKER":{"parent":"WORKER","keys":[["id","uuid"]],"fields":["id","run_id","candidate_id","target_pay_channel","target_umbrella_id","target_umbrella_vat_chargeable","status","gross_ex_vat","gross_vat","gross_inc_vat","entered_paye_net","net_request_revision","net_projection_revision","preparation_revision","case_selection_revision","captured_line_count","captured_position_count","review_issue_code","review_issue_work_id"],"amount_scope":"WHOLE_WORKER_NET_CURRENT_STATUS"},
"PROJECTIONS":{"parent":"WORKER","keys":[["projection_no","bigint"]],"fields":["id","run_worker_id","request_command_id","projection_no","input_kind","gross_ex_vat","gross_vat","gross_inc_vat","entered_paye_net","accepted_recoveries","accepted_net_additions","accepted_gross_additions","accepted_gross_deductions","cash_amount","accepted_at_utc","retired_at_utc"],"amount_scope":"WHOLE_WORKER_FROZEN_NET_CASH"},
"TRANSFERS":{"parent":"WORKER","keys":[["transfer_no","bigint"]],"fields":["id","run_worker_id","candidate_id","projection_id","build_command_id","transfer_no","beneficiary_kind","beneficiary_id","destination_rail","currency","cash_amount","member_count","member_cash_sum","status","original_transfer_id","return_cash_id","execution_kind","created_at_utc"],"amount_scope":"WHOLE_TRANSFER_CASH_CURRENT_STATUS"},
"MEMBERS":{"parent":"TRANSFER","keys":[["member_no","bigint"]],"fields":["transfer_id","member_no","run_worker_id","subject_kind","run_line_id","case_hold_id","case_instruction_id","case_allocation_result_id","signed_cash_contribution"],"amount_scope":"TRANSFER_MEMBER_CONTRIBUTION_NOT_TIMESHEET_ALLOCATION"},
"OUTCOMES":{"parent":"TRANSFER","keys":[["occurred_at_utc","timestamptz"],["id","uuid"]],"fields":["id","transfer_id","receipt_id","outcome_kind","whole_transfer_amount","occurred_at_utc","received_at_utc"],"amount_scope":"WHOLE_TRANSFER_IMMUTABLE_OUTCOME"},
"RETURN_CASH":{"parent":"TRANSFER","keys":[["id","uuid"]],"fields":["id","original_transfer_id","candidate_id","amount_owed","amount_held","amount_reissued_paid","created_at_utc"],"amount_scope":"CURRENT_RETURN_CASH_NOT_SOURCE_DISPOSITION"},
"CSV":{"parent":"TRANSFER","keys":[["id","uuid"]],"fields":["id","transfer_id","run_worker_id","actor_user_id","cash_amount","csv_sha256","issued_at_utc"],"amount_scope":"WHOLE_TRANSFER_IMMUTABLE_CSV_RECEIPT"},
"INTERNAL_RECEIPT":{"parent":"TRANSFER","keys":[["id","uuid"]],"fields":["id","command_id","transfer_id","run_worker_id","candidate_id","actor_user_id","projection_id","execution_kind","cash_amount","occurred_at_utc","received_at_utc"],"amount_scope":"WHOLE_TRANSFER_IMMUTABLE_INTERNAL_RECEIPT"},
"GROUP_LEGS":{"parent":"TRANSFER","keys":[["transfer_id","uuid"]],"fields":["anchor_transfer_id","transfer_id","leg_kind","destination_id"],"amount_scope":"TRANSFER_GROUP_PROVENANCE"},
"EXPIRY":{"parent":"WORKER","keys":[["command_id","uuid"]],"fields":["command_id","candidate_id","run_id","run_worker_id","job_id","member_no","draft_state_id","preparation_revision","selection_revision","original_job_id","original_job_status","original_job_phase","original_worker_status","original_review_issue_code","original_review_issue_work_id","expected_work_count","expected_case_hold_count","released_work_count","released_case_hold_count","status","stage","checkpoint","created_at_utc","started_at_utc","finished_at_utc"],"amount_scope":"EXPIRY_RECEIPT_CURRENT_STATUS"},
"CASES":{"parent":"SCOPE","keys":[["id","uuid"]],"fields":["id","candidate_id","case_kind","tax_treatment","status","principal_approved","principal_funded","principal_recovered","principal_written_off","principal_paid_credit","active_hold_amount","active_payout_hold_amount","active_recovery_hold_amount","current_rule_id","current_rule_revision","case_revision","due_date","updated_at_utc"],"amount_scope":"CURRENT_CASE_SOURCE_CACHE"},
"COMPONENTS":{"parent":"CASE","keys":[["component_ordinal","bigint"]],"fields":["id","case_id","candidate_id","component_key","component_ordinal","component_revision","rule_id","case_kind","case_subtype","tax_treatment","instruction_kind","direction","payroll_stage","source_pay_channel","currency","approved_source_ex_vat","funded_source_ex_vat","recovered_source_ex_vat","paid_credit_source_ex_vat","written_off_source_ex_vat","active_payout_source_ex_vat","active_recovery_source_ex_vat","resolution_state","target_pay_channel","target_ex_vat","target_vat","target_inc_vat","valuation_policy_id","valuation_window_id","resolution_ref","updated_at_utc"],"amount_scope":"CURRENT_CASE_SOURCE_AND_TARGET_CACHE"},
"RULES":{"parent":"CASE","keys":[["rule_revision","bigint"]],"fields":["id","case_id","candidate_id","rule_revision","case_kind","case_subtype","tax_treatment","case_created_at_utc","original_funded_at_utc","original_funding_event_id","schedule_start_monday","schedule_week_count","weekly_due_source_ex_vat","minimum_earnings_threshold","take_home_floor_override","next_due_monday","captured_at_utc"],"amount_scope":"CAPTURED_CASE_RULE"},
"CASE_EVENTS":{"parent":"CASE","keys":[["occurred_at_utc","timestamptz"],["id","uuid"]],"fields":["id","case_id","operation_id","operation_item_id","event_kind","approved_delta","funded_delta","recovered_delta","written_off_delta","paid_credit_delta","original_transfer_id","original_effect_id","occurred_at_utc","candidate_id","case_component_id","instruction_id","allocation_result_id","run_worker_id","event_case_kind","hold_purpose","pay_week_start","source_amount_ex_vat","target_amount_ex_vat","target_amount_vat","target_amount_inc_vat","shortfall_source_ex_vat","cap_reason"],"amount_scope":"IMMUTABLE_CASE_SOURCE_AND_TARGET_EVENT"},
"INSTRUCTIONS":{"parent":"COMPONENT","keys":[["pay_week_start","date"],["id","uuid"]],"fields":["id","run_worker_id","run_id","candidate_id","case_id","case_component_id","component_ordinal","rule_id","rule_revision","captured_case_revision","captured_component_revision","captured_period_revision","preparation_revision","selection_revision","is_selected","case_kind","case_subtype","tax_treatment","instruction_kind","direction","payroll_stage","hold_purpose","allocation_channel","source_pay_channel","target_pay_channel","currency","nominal_source_ex_vat","capacity_source_ex_vat","nominal_target_ex_vat","nominal_target_vat","nominal_target_inc_vat","capacity_target_ex_vat","capacity_target_vat","capacity_target_inc_vat","valuation_policy_id","valuation_window_id","pay_week_start","minimum_earnings_threshold","resolved_take_home_floor","resolution_ref","explanation","captured_at_utc","work_collection_id"],"amount_scope":"FROZEN_CASE_SOURCE_AND_TARGET_INSTRUCTION"},
"CASE_HOLDS":{"parent":"COMPONENT","keys":[["pay_week_start","date"],["id","uuid"]],"fields":["id","run_worker_id","candidate_id","case_id","case_component_id","instruction_id","projection_id","allocation_result_id","purpose","pay_week_start","amount","source_reserved_ex_vat","target_amount_ex_vat","target_amount_vat","target_amount_inc_vat","status","created_at_utc","finished_at_utc"],"amount_scope":"CASE_SOURCE_AND_TARGET_HOLD_CURRENT_STATUS"},
"RESULTS":{"parent":"INSTRUCTION","keys":[["state_id","uuid"]],"fields":["id","state_id","instruction_id","result_no","run_worker_id","candidate_id","preparation_revision","selection_revision","pass_kind","hold_purpose","allocation_channel","nominal_target_ex_vat","usable_capacity_target_ex_vat","allocated_source_ex_vat","allocated_target_ex_vat","allocated_target_vat","allocated_target_inc_vat","shortfall_target_ex_vat","cap_reason","affordability_reason","capacity_reason","channel_headroom_before","channel_headroom_after","worker_take_home_before","worker_take_home_after","created_at_utc"],"amount_scope":"FROZEN_CASE_ALLOCATION_SOURCE_AND_TARGET"},
"COMMANDS":{"parent":"SCOPE","keys":[["command_id","uuid"]],"fields":["command_id","candidate_id","member_no"],"amount_scope":"COMMAND_MEMBERSHIP"},
"COMMAND":{"parent":"COMMAND","keys":[["id","uuid"]],"fields":["id","agency_sequence","module_epoch","command_kind","status","expected_member_count","enrolled_member_count","created_at_utc","sealed_at_utc"],"amount_scope":"COMMAND_RECEIPT_CURRENT_STATUS"},
"JOBS":{"parent":"COMMAND","keys":[["job_kind","text"]],"fields":["id","command_id","candidate_id","command_sequence","job_kind","status","phase","attempt_count","applied_line_count","last_error_code","available_at_utc","queue_wake_after_utc"],"amount_scope":"JOB_RECEIPT_CURRENT_STATUS"},
"NET_REQUEST":{"parent":"COMMAND","keys":[["command_id","uuid"]],"fields":["command_id","run_worker_id","candidate_id","actor_user_id","request_no","input_kind","entered_paye_net","frozen_gross_inc_vat","expected_projection_revision","preparation_revision","selection_revision","case_draft_state_id","accepted_at_utc"],"amount_scope":"WHOLE_WORKER_IMMUTABLE_NET_REQUEST"},
"OUTCOME_REQUEST":{"parent":"COMMAND","keys":[["command_id","uuid"]],"fields":["command_id","outcome_id","transfer_id","run_worker_id","candidate_id","actor_user_id","posted_member_count","posting_complete","internal_receipt_id"],"amount_scope":"OUTCOME_RECEIPT_CURRENT_STATUS"},
"RETURN_REQUEST":{"parent":"COMMAND","keys":[["command_id","uuid"]],"fields":["command_id","outcome_id","transfer_id","run_worker_id","candidate_id","actor_user_id","posting_complete"],"amount_scope":"RETURN_RECEIPT_CURRENT_STATUS"},
"REISSUE_REQUEST":{"parent":"COMMAND","keys":[["command_id","uuid"]],"fields":["command_id","return_cash_id","run_worker_id","candidate_id","actor_user_id","transfer_id"],"amount_scope":"REISSUE_RECEIPT_CURRENT_STATUS"},
"CANCEL_REQUEST":{"parent":"COMMAND","keys":[["command_id","uuid"]],"fields":["command_id","run_worker_id","candidate_id","status","expected_line_count","released_line_count","blocked_code","started_at_utc","finished_at_utc"],"amount_scope":"CANCEL_RECEIPT_CURRENT_STATUS"},
"REISSUE_CANCEL":{"parent":"COMMAND","keys":[["command_id","uuid"]],"fields":["command_id","transfer_id","return_cash_id","run_worker_id","candidate_id","actor_user_id","amount","status","blocked_code","accepted_at_utc","completed_at_utc"],"amount_scope":"WHOLE_TRANSFER_REISSUE_CANCEL_RECEIPT"},
"WRITE_OFF":{"parent":"COMMAND","keys":[["command_id","uuid"]],"fields":["command_id","candidate_id","case_id","case_component_id","work_collection_id","actor_user_id","expected_component_revision","basis_case_revision","basis_principal","basis_recovered","basis_written_off","basis_protected","requested_scope","input_amount","requested_amount","reason","accepted_at_utc","status","applied_amount","remaining_amount","protected_amount","event_id","issue_code","completed_at_utc"],"amount_scope":"SOURCE_FORGIVENESS_NOT_PAID_DISPOSITION"},
"CASE_CREATE":{"parent":"COMMAND","keys":[["command_id","uuid"]],"fields":["command_id","case_id","candidate_id","actor_user_id","case_kind","case_subtype","tax_treatment","source_pay_channel","currency","principal_source_ex_vat","due_date","minimum_earnings_threshold","take_home_floor_override","approval_reason","rule_id","primary_component_id","recovery_component_id","valuation_policy_id","opening_pay_week_start","schedule_week_count","weekly_due_source_ex_vat","accepted_at_utc"],"amount_scope":"IMMUTABLE_CASE_APPROVAL_RECEIPT"},
"EXPIRY_REQUEST":{"parent":"COMMAND","keys":[["command_id","uuid"]],"fields":["command_id","run_id","original_prepare_command_id","actor_user_id","initiator_kind","captured_deadline","expected_candidate_count","enrolled_candidate_count","completed_candidate_count","status","accepted_at_utc","finished_at_utc"],"amount_scope":"EXPIRY_COMMAND_RECEIPT_CURRENT_STATUS"},
"CASE":{"parent":"CASE","keys":[["id","uuid"]],"fields":["id","candidate_id","case_kind","tax_treatment","status","principal_approved","principal_funded","principal_recovered","principal_written_off","principal_paid_credit","active_hold_amount","active_payout_hold_amount","active_recovery_hold_amount","current_rule_id","current_rule_revision","case_revision","due_date","updated_at_utc"],"amount_scope":"CURRENT_CASE_SOURCE_CACHE"},
"PROTECTED_RECEIPT":{"parent":"REVISION","keys":[["orchestration_run_id","uuid"]],"fields":["orchestration_run_id","family_id","generation_id","approval_id","accepted_family_bound_version","source_manifest_sha256","prepared_request_sha256","publication_request_sha256","work_id","revision_id","command_id","agency_sequence","actor_user_id","accepted_at_utc"],"amount_scope":"IMMUTABLE_APPROVAL_PROVENANCE"},
"SOURCE_DETAIL":{"parent":"LINE","keys":[["component_id","uuid"]],"fields":["head_id","component_id","decision_bundle_id","bundle_revision","component_sha256","detail_sha256","created_at_utc"],"amount_scope":"IMMUTABLE_SOURCE_CERTIFICATE_REFERENCES_NOT_RAW_PAYLOAD"},
"POLICY":{"parent":"RUN_LINE","keys":[["source_window_id","uuid"]],"fields":["policy_id","source_window_id","date_from","date_to","erni_pct","vat_rate_pct"],"amount_scope":"IMMUTABLE_CAPTURED_POLICY_NOT_LIVE_SETTINGS"},
"CHOICE":{"parent":"RUN_WORK","keys":[["work_id","uuid"]],"fields":["run_id","work_id","expected_revision_id","selection_mode","selection_state","page_count","component_count"],"amount_scope":"CAPTURED_SELECTION_CURRENT_STATUS"},
"SELECTION":{"parent":"RUN_WORK","keys":[["selection_no","bigint"]],"fields":["run_id","work_id","approved_line_id","expected_revision_id","component_key","selection_no","page_no","page_item_no"],"amount_scope":"CAPTURED_COMPONENT_SELECTION"},
"WORKER_CASE_HOLDS":{"parent":"WORKER","keys":[["id","uuid"]],"fields":["id","run_worker_id","candidate_id","case_id","case_component_id","instruction_id","projection_id","allocation_result_id","purpose","pay_week_start","amount","source_reserved_ex_vat","target_amount_ex_vat","target_amount_vat","target_amount_inc_vat","status","created_at_utc","finished_at_utc"],"amount_scope":"HOLD_CURRENT_STATUS_UNTYPED_SOURCE_TARGET_MAY_BE_NULL"},
"COMMAND_WORKER":{"parent":"COMMAND","keys":[["id","uuid"]],"fields":["id","run_id","candidate_id","target_pay_channel","target_umbrella_id","target_umbrella_vat_chargeable","status","gross_ex_vat","gross_vat","gross_inc_vat","entered_paye_net","net_request_revision","net_projection_revision","preparation_revision","case_selection_revision","captured_line_count","captured_position_count","review_issue_code","review_issue_work_id"],"amount_scope":"WHOLE_WORKER_NET_CURRENT_STATUS"}};
export const BPAY_NEXT_HISTORY_SECTIONS = Object.freeze(Object.fromEntries(
  Object.entries(RAW_SECTIONS).map(([name, definition]) => [name, Object.freeze({
    ...definition, fields: Object.freeze(definition.fields),
    keys: Object.freeze(definition.keys.map(key => Object.freeze(key)))
  })])
));
const VERSION = 'PAYMENT_HISTORY_V1';
const CONSISTENCY = 'LIVE_KEYSET_REFRESH_FOR_LATE_COMMIT';
const UUID = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const ENCODER = new TextEncoder();
const REQUEST_KEYS = ['version','candidate_id','timesheet_id','section','parent_id','context_id','component_key','after','upper','limit'];
const CURSOR_KEYS = ['version','scope_kind','scope_id','section','parent_id','context_id','component_key','upper','key'];
const UNAVAILABLE = ['MOVED_ROOT_FLAT_PAID_BINDING','CANONICAL_MULTI_CHANNEL_VECTOR',
  'MOVE_COMPARISON_RECEIPT','GENERAL_APPROVAL_ACTOR_REASON_NOT_CAPTURED_IN_NEXT_ROWS'];
const record = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const exact = (value, keys) => record(value) && Object.keys(value).length === keys.length
  && keys.every(key => Object.hasOwn(value, key));
const uuid = value => typeof value === 'string' && UUID.test(value);
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
const bytes = value => ENCODER.encode(value).byteLength;
class HistoryError extends Error {
  constructor(code, status = 502, retryable = false) {
    super(code); this.code = code; this.status = status; this.retryable = retryable;
  }
}
const fail = (code, status, retryable) => { throw new HistoryError(code,status,retryable); };
const invalidRequest = () => fail('BPAY_NEXT_HISTORY_REQUEST_INVALID',400);
const invalidResponse = () => fail('BPAY_NEXT_HISTORY_RESPONSE_INVALID');
function keyTuple(value, definition) {
  if (!Array.isArray(value) || value.length !== definition.keys.length) return false;
  return value.every((item, index) => {
    if (typeof item !== 'string' || bytes(item) < 1 || bytes(item) > 256) return false;
    const type = definition.keys[index][1];
    if (type === 'uuid') return uuid(item);
    if (type === 'integer' || type === 'bigint') return /^(0|[1-9][0-9]{0,18})$/.test(item)
      && BigInt(item) <= (type === 'integer' ? 2147483647n : 9223372036854775807n);
    if (type === 'date') return /^[0-9]{4}-[0-9]{2}-[0-9]{2}$/.test(item)
      && Number.isFinite(Date.parse(item+'T00:00:00Z'))
      && new Date(item+'T00:00:00Z').toISOString().slice(0,10) === item;
    if (type === 'timestamptz') return /^[0-9]{4}-[0-9]{2}-[0-9]{2} (?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](?:\.[0-9]{1,6})?\+00(?::00)?$/.test(item)
      && Number.isFinite(Date.parse(item.replace(' ','T').replace(/\+00$/,'+00:00')));
    return type === 'text';
  });
}
function cursor(value, request, upper, definition) {
  return exact(value, CURSOR_KEYS) && value.version === 'PAYMENT_HISTORY_CURSOR_V1'
    && value.scope_kind === (request.timesheet_id ? 'TIMESHEET' : 'CANDIDATE')
    && value.scope_id === (request.timesheet_id || request.candidate_id)
    && ['section','parent_id','context_id','component_key'].every(key => value[key] === request[key])
    && same(value.upper,upper) && keyTuple(value.upper,definition) && keyTuple(value.key,definition);
}
export function validateBpayNextHistoryRequest(value) {
  if (!exact(value,REQUEST_KEYS) || value.version !== VERSION
    || typeof value.section !== 'string' || !Number.isInteger(value.limit) || value.limit < 1 || value.limit > 100
    || (value.candidate_id === null) === (value.timesheet_id === null)
    || !['candidate_id','timesheet_id','parent_id','context_id'].every(key => value[key] === null || uuid(value[key]))) invalidRequest();
  const definition = Object.hasOwn(BPAY_NEXT_HISTORY_SECTIONS,value.section) ? BPAY_NEXT_HISTORY_SECTIONS[value.section] : null;
  if (value.section === 'MANIFEST') {
    if (['parent_id','context_id','component_key','after','upper'].some(key => value[key] !== null)) invalidRequest();
  } else {
    if (!definition || (definition.parent === 'SCOPE') !== (value.parent_id === null)
      || (value.timesheet_id !== null && ['CASES','COMMANDS'].includes(value.section))
      || (value.context_id !== null && (value.timesheet_id === null || !['WORKER','TRANSFER','COMMAND'].includes(definition.parent)))
      || (value.section === 'EFFECTS' ? typeof value.component_key !== 'string' || bytes(value.component_key) < 1 || bytes(value.component_key) > 256
        : value.component_key !== null)) invalidRequest();
    if (value.after === null ? value.upper !== null : !cursor(value.after,value,value.upper,definition)) invalidRequest();
  }
  return value;
}
export function validateBpayNextHistoryResponse(value, request) {
  if (!record(value) || value.version !== VERSION || value.consistency !== CONSISTENCY
    || !uuid(value.candidate_id) || (request.timesheet_id ? !uuid(value.work_id)
      : value.candidate_id !== request.candidate_id || value.work_id !== null)) invalidResponse();
  if (request.section === 'MANIFEST') {
    if (!exact(value,['version','candidate_id','work_id','sections','terminal','next_cursor','consistency','scalar_encoding','unavailable'])
      || value.terminal !== true || value.next_cursor !== null || value.scalar_encoding !== 'STRING_OR_NULL'
      || !same(value.unavailable,UNAVAILABLE.concat(request.timesheet_id ? ['TIMESHEET_COMMAND_WITHOUT_WORK_OR_CASE_ANCESTRY'] : []))
      || !Array.isArray(value.sections) || value.sections.length !== 52) invalidResponse();
    const seen = new Set();
    for (const section of value.sections) {
      const definition = typeof section?.section === 'string' && Object.hasOwn(BPAY_NEXT_HISTORY_SECTIONS,section.section) ? BPAY_NEXT_HISTORY_SECTIONS[section.section] : null;
      if (!definition || seen.has(section.section)
        || !exact(section,['section','parent','keys','fields','amount_scope','applicability'])
        || section.parent !== definition.parent || section.amount_scope !== definition.amount_scope
        || !same(section.keys,definition.keys) || !same(section.fields,definition.fields)
        || section.applicability !== (request.timesheet_id && ['CASES','COMMANDS'].includes(section.section)
          ? 'CANDIDATE_ONLY_USE_REFERENCED_PARENT' : 'SCOPE_QUALIFIED')) invalidResponse();
      seen.add(section.section);
    }
  } else {
    const definition = BPAY_NEXT_HISTORY_SECTIONS[request.section];
    if (!exact(value,['version','candidate_id','work_id','section','parent_id','context_id','component_key','amount_scope','consistency','rows','upper','terminal','next_cursor'])
      || !['section','parent_id','context_id','component_key'].every(key => value[key] === request[key])
      || value.amount_scope !== definition.amount_scope || typeof value.terminal !== 'boolean'
      || !Array.isArray(value.rows) || value.rows.length > request.limit
      || !(value.upper === null ? value.rows.length === 0 && value.terminal : keyTuple(value.upper,definition))
      || (request.upper !== null && !same(request.upper,value.upper))) invalidResponse();
    const fields = definition.fields.concat(request.section === 'POSITIONS' ? ['current_residual_source_ex_vat'] : []);
    const seen = new Set();
    for (const row of value.rows) {
      if (!exact(row,['key','data']) || !keyTuple(row.key,definition) || !exact(row.data,fields)
        || !fields.every(field => row.data[field] === null || typeof row.data[field] === 'string')
        || !definition.keys.every(([field], index) => row.data[field] === row.key[index])
        || seen.has(JSON.stringify(row.key)) || (request.after && same(row.key,request.after.key))) invalidResponse();
      seen.add(JSON.stringify(row.key));
    }
    if (value.terminal ? value.next_cursor !== null : value.rows.length === 0
      || !cursor(value.next_cursor,request,value.upper,definition)
      || !same(value.next_cursor.key,value.rows.at(-1)?.key)) invalidResponse();
  }
  return value;
}
async function boundedJson(source, maximum, signal, request = false) {
  const reject = () => fail(request ? 'BPAY_NEXT_HISTORY_REQUEST_INVALID' : 'BPAY_NEXT_HISTORY_RESPONSE_INVALID',request ? 400 : 502);
  const length = source.headers.get('content-length');
  if (length && (!/^[0-9]+$/.test(length) || Number(length) > maximum))
    fail(request ? 'BPAY_NEXT_HISTORY_REQUEST_TOO_LARGE' : 'BPAY_NEXT_HISTORY_PAGE_TOO_LARGE',request ? 413 : 502);
  if (!source.body) reject();
  const reader = source.body.getReader(); const parts = []; let size = 0;
  const cancel = () => { void reader.cancel().catch(() => {}); };
  signal?.addEventListener('abort',cancel,{once:true});
  try {
    while (true) {
      if (signal?.aborted) throw new DOMException('Aborted','AbortError');
      const part = await reader.read(); if (part.done) break;
      size += part.value.byteLength;
      if (size > maximum) {
        cancel(); fail(request ? 'BPAY_NEXT_HISTORY_REQUEST_TOO_LARGE' : 'BPAY_NEXT_HISTORY_PAGE_TOO_LARGE',request ? 413 : 502);
      }
      parts.push(part.value);
    }
    const data = new Uint8Array(size); let offset = 0;
    for (const part of parts) { data.set(part,offset); offset += part.byteLength; }
    try { return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(data)); } catch { reject(); }
  } finally { signal?.removeEventListener('abort',cancel); reader.releaseLock(); }
}
function safeError(error) {
  if (error instanceof HistoryError) return error;
  const code = error?.json?.code;
  if (code === '42501' || [401,403].includes(error?.status)) return new HistoryError('BPAY_NEXT_HISTORY_FORBIDDEN',403);
  if (code === '22023') return new HistoryError(error?.json?.message === 'BPAY_NEXT_HISTORY_SCOPE_NOT_FOUND'
    ? 'BPAY_NEXT_HISTORY_SCOPE_NOT_FOUND' : 'BPAY_NEXT_HISTORY_REQUEST_INVALID',error?.json?.message === 'BPAY_NEXT_HISTORY_SCOPE_NOT_FOUND' ? 404 : 400);
  if (code === '54000') return new HistoryError('BPAY_NEXT_HISTORY_PAGE_TOO_LARGE',503,false);
  if (error?.name === 'AbortError') return new HistoryError('BPAY_NEXT_HISTORY_DEPENDENCY_TIMEOUT',503,true);
  return new HistoryError('BPAY_NEXT_HISTORY_DEPENDENCY_UNAVAILABLE',503,true);
}
const json = (status,value) => new Response(JSON.stringify(value),{
  status, headers: {'content-type':'application/json; charset=utf-8','cache-control':'no-store'}
});
export function createBpayNextHistoryRpc({baseUrl,headers,fetchImpl=fetch}) {
  const endpoint = new URL(String(baseUrl || '').replace(/\/+$/,'')+'/rest/v1/rpc/bpay_next_payment_history_page_v1');
  if (!['http:','https:'].includes(endpoint.protocol) || endpoint.username || endpoint.password || endpoint.search || endpoint.hash)
    throw new TypeError('BPAY_NEXT_HISTORY_TRANSPORT_INVALID');
  return async (request,{actorUserId,signal}) => {
    validateBpayNextHistoryRequest(request);
    if (!uuid(actorUserId) || !(signal instanceof AbortSignal)) throw new TypeError('BPAY_NEXT_HISTORY_TRANSPORT_INVALID');
    const serviceHeaders = new Headers(headers);
    serviceHeaders.set('content-type','application/json'); serviceHeaders.set('accept','application/json');
    serviceHeaders.set('X-CloudTMS-Banking-Pay-Route-Class','PREVIEW_PAGE');
    return fetchImpl(endpoint.href,{method:'POST',headers:serviceHeaders,
      body:JSON.stringify({p_request:{...request,actor_user_id:actorUserId}}),signal,redirect:'error'});
  };
}
export async function handleBpayNextHistoryRead(request, dependencies = {}) {
  if (request.method !== 'POST') return json(405,{ok:false,error_code:'BPAY_NEXT_HISTORY_METHOD_NOT_ALLOWED',retryable:false});
  let timer, controller;
  try {
    if (typeof dependencies.requireOfficeUser !== 'function' || typeof dependencies.historyRpc !== 'function')
      fail('BPAY_NEXT_HISTORY_DEPENDENCY_UNAVAILABLE',503,true);
    const user = await dependencies.requireOfficeUser(request,['admin']);
    if (!uuid(user?.id)) fail('BPAY_NEXT_HISTORY_UNAUTHORIZED',401);
    if (user.role !== 'admin') fail('BPAY_NEXT_HISTORY_FORBIDDEN',403);
    controller = new AbortController();
    const timeout = new Promise((_,reject) => { timer=setTimeout(() => {
      controller.abort(); reject(new HistoryError('BPAY_NEXT_HISTORY_DEPENDENCY_TIMEOUT',503,true));
    },BPAY_NEXT_HISTORY_LIMITS.timeoutMs); });
    const read = async () => {
      const call = validateBpayNextHistoryRequest(await boundedJson(request,BPAY_NEXT_HISTORY_LIMITS.requestBytes,controller.signal,true));
      const response = await dependencies.historyRpc(call,{actorUserId:user.id,signal:controller.signal});
      if (!(response instanceof Response)) invalidResponse();
      const payload = await boundedJson(response,BPAY_NEXT_HISTORY_LIMITS.responseBytes,controller.signal);
      if (!response.ok) throw {status:response.status,json:payload};
      return {ok:true,page:validateBpayNextHistoryResponse(payload,call)};
    };
    return json(200,await Promise.race([read(),timeout]));
  } catch (error) {
    const safe = safeError(error);
    return json(safe.status,{ok:false,error_code:safe.code,retryable:safe.retryable});
  } finally { if (timer !== undefined) clearTimeout(timer); controller?.abort(); }
}
