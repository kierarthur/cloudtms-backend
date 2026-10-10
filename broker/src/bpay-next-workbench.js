/** WB09 pure Office reads. Closed kinds, trusted admin actor, fixed RPC only.
 * No reconstruction, financial calculation, command or automatic page retry. */
export const BPAY_NEXT_WORKBENCH_LIMITS=Object.freeze({requestBytes:32768,responseBytes:131072,timeoutMs:4000});
const ENCODER=new TextEncoder(),UUID=/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const KINDS=['HEADER','CANDIDATES','ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL','CANDIDATE_GROUPS','GROUP_LINES','PICK_CANDIDATES','PICK_CLIENTS','EXPORT_ROWS'];
const EXPORT_SORT_FIELDS={"C_NAME":["text","boolean","text","uuid","uuid"],"C_AMOUNT":["boolean","decimal","uuid","uuid"],"C_DEDUCTIONS":["boolean","text","boolean","text","uuid","uuid"],"C_STATUS":["boolean","boolean","text","text","uuid","uuid"],"G_PAYMENT":["boolean","date","text","uuid","uuid"],"G_CLIENT":["boolean","text","text","uuid","uuid"],"G_CHANNEL":["boolean","text","text","uuid","uuid"],"G_AMOUNT":["boolean","decimal","text","uuid","uuid"],"G_DEDUCTIONS":["boolean","text","uuid","uuid"],"G_REASON":["small","text","text","uuid","uuid"]};
const EXPORT_SORTS=Object.keys(EXPORT_SORT_FIELDS).flatMap(k=>[k+'_ASC',k+'_DESC']);
const COMMON={candidate_id:'uuid?',client_id:'uuid?',channel:'scopeChannel',section:'scopeSection',search:'search',sort:'sort',group_key:'groupKey?',item_id:'uuid?',reason_code:'code?',count_selection:'countSelection?',selection_memory:'selectionMemory?',progress_after:'progressCursor?',after:'cursor?',limit:'limit'};
export const BPAY_NEXT_WORKBENCH_PAGE_INPUTS=Object.freeze(Object.fromEntries(KINDS.map(k=>[k,Object.freeze({...COMMON})])));
// Generated once from the explicit SQL projections; reviewed source is durable.
export const ROWS=Object.freeze({
  "HEADER": {
    "candidate_count": "integer",
    "selectable_member_count": "integer?",
    "selected_candidate_count": "integer?",
    "selected_member_count": "integer?",
    "item_count": "integer",
    "contribution_count": "integer",
    "segment_count": "integer",
    "payable_count": "integer",
    "selected_count": "integer",
    "snoozed_count": "integer",
    "held_count": "integer",
    "queried_count": "integer",
    "blocked_count": "integer",
    "deduction_item_count": "integer",
    "unknown_amount_count": "integer",
    "unknown_selected_amount_count": "integer",
    "amount": "decimal?",
    "waiting_amount": "decimal?",
    "ready_net_add_amount": "decimal?",
    "waiting_net_add_amount": "decimal?",
    "gross_deduction_balance": "decimal?",
    "net_deduction_balance": "decimal?",
    "approved_amount": "decimal?",
    "paid_credit_amount": "decimal?",
    "reserved_amount": "decimal?",
    "excluded_amount": "decimal?",
    "recovery_amount": "decimal?",
    "selected_amount": "decimal?",
    "has_deduction": "boolean"
  },
  "CANDIDATES": {
    "candidate_id": "uuid",
    "candidate_sort_name": "text?",
    "tms_ref": "text?",
    "bank_warning_code": "bank?",
    "metadata_version": "positive",
    "projection_revision": "positive",
    "item_count": "integer",
    "contribution_count": "integer",
    "segment_count": "integer",
    "payable_count": "integer",
    "selected_count": "integer",
    "snoozed_count": "integer",
    "held_count": "integer",
    "queried_count": "integer",
    "blocked_count": "integer",
    "deduction_item_count": "integer",
    "unknown_amount_count": "integer",
    "unknown_selected_amount_count": "integer",
    "amount": "decimal?",
    "waiting_amount": "decimal?",
    "ready_net_add_amount": "decimal?",
    "waiting_net_add_amount": "decimal?",
    "gross_deduction_balance": "decimal?",
    "net_deduction_balance": "decimal?",
    "approved_amount": "decimal?",
    "paid_credit_amount": "decimal?",
    "reserved_amount": "decimal?",
    "excluded_amount": "decimal?",
    "recovery_amount": "decimal?",
    "selected_amount": "decimal?",
    "has_deduction": "boolean"
  },
  "PICK_CANDIDATES": {
    "candidate_id": "uuid",
    "candidate_sort_name": "text?",
    "tms_ref": "text?"
  },
  "PICK_CLIENTS": {
    "client_id": "uuid",
    "client_display_name": "text"
  },
  "ACTION_REQUIRED": {
    "item_id": "uuid",
    "group_key": "text",
    "candidate_id": "uuid",
    "work_id": "uuid?",
    "case_id": "uuid?",
    "case_component_id": "uuid?",
    "client_id": "uuid?",
    "physical_timesheet_id": "uuid?",
    "revision_id": "uuid?",
    "component_key": "text",
    "member_kind": "member",
    "member_key": "text",
    "portion_kind": "portion",
    "candidate_display_name": "text?",
    "candidate_reference": "text?",
    "timesheet_reference": "text?",
    "week_ending_date": "date?",
    "reason_code": "code",
    "reason_priority": "small",
    "nominal_source_ex_vat": "decimal?",
    "amount_known": "boolean",
    "source_pay_channel": "channel?",
    "target_pay_channel": "channel?",
    "section": "section",
    "query_state": "query",
    "query_qualified": "boolean",
    "pay_hold": "boolean?",
    "pay_hold_reason": "text?",
    "eligible_from_date": "date?",
    "snooze_id": "uuid?",
    "snooze_version": "positive?",
    "snooze_until_date": "date?",
    "stale_snooze_code": "text?",
    "progress_state": "progress",
    "error_code": "text?",
    "financial_view_revision": "integer",
    "projection_revision": "positive",
    "metadata_version": "positive",
    "direction": "direction",
    "payroll_stage": "stage?",
    "approved_source_ex_vat": "decimal?",
    "active_reserved_source_ex_vat": "decimal?",
    "deduction_balance_source_ex_vat": "decimal?",
    "recovery_source_ex_vat": "decimal?",
    "included_source_ex_vat": "decimal?",
    "gross_due_source_ex_vat": "decimal?"
  },
  "BLOCKED": {
    "item_id": "uuid",
    "group_key": "text",
    "candidate_id": "uuid",
    "work_id": "uuid?",
    "case_id": "uuid?",
    "case_component_id": "uuid?",
    "client_id": "uuid?",
    "physical_timesheet_id": "uuid?",
    "revision_id": "uuid?",
    "component_key": "text",
    "member_kind": "member",
    "member_key": "text",
    "portion_kind": "portion",
    "candidate_display_name": "text?",
    "candidate_reference": "text?",
    "timesheet_reference": "text?",
    "week_ending_date": "date?",
    "reason_code": "code",
    "reason_priority": "small",
    "nominal_source_ex_vat": "decimal?",
    "amount_known": "boolean",
    "source_pay_channel": "channel?",
    "target_pay_channel": "channel?",
    "section": "section",
    "query_state": "query",
    "query_qualified": "boolean",
    "pay_hold": "boolean?",
    "pay_hold_reason": "text?",
    "eligible_from_date": "date?",
    "snooze_id": "uuid?",
    "snooze_version": "positive?",
    "snooze_until_date": "date?",
    "stale_snooze_code": "text?",
    "progress_state": "progress",
    "error_code": "text?",
    "financial_view_revision": "integer",
    "projection_revision": "positive",
    "metadata_version": "positive",
    "direction": "direction",
    "payroll_stage": "stage?",
    "approved_source_ex_vat": "decimal?",
    "active_reserved_source_ex_vat": "decimal?",
    "deduction_balance_source_ex_vat": "decimal?",
    "recovery_source_ex_vat": "decimal?",
    "included_source_ex_vat": "decimal?",
    "gross_due_source_ex_vat": "decimal?"
  },
  "ISSUE_DETAIL": {
    "item_id": "uuid",
    "group_key": "text",
    "candidate_id": "uuid",
    "work_id": "uuid?",
    "case_id": "uuid?",
    "case_component_id": "uuid?",
    "client_id": "uuid?",
    "physical_timesheet_id": "uuid?",
    "revision_id": "uuid?",
    "component_key": "text",
    "member_kind": "member",
    "member_key": "text",
    "portion_kind": "portion",
    "candidate_display_name": "text?",
    "candidate_reference": "text?",
    "timesheet_reference": "text?",
    "week_ending_date": "date?",
    "reason_code": "code",
    "reason_priority": "small",
    "nominal_source_ex_vat": "decimal?",
    "amount_known": "boolean",
    "source_pay_channel": "channel?",
    "target_pay_channel": "channel?",
    "section": "section",
    "query_state": "query",
    "query_qualified": "boolean",
    "pay_hold": "boolean?",
    "pay_hold_reason": "text?",
    "eligible_from_date": "date?",
    "snooze_id": "uuid?",
    "snooze_version": "positive?",
    "snooze_until_date": "date?",
    "stale_snooze_code": "text?",
    "progress_state": "progress",
    "error_code": "text?",
    "financial_view_revision": "integer",
    "projection_revision": "positive",
    "metadata_version": "positive",
    "direction": "direction",
    "payroll_stage": "stage?",
    "approved_source_ex_vat": "decimal?",
    "active_reserved_source_ex_vat": "decimal?",
    "deduction_balance_source_ex_vat": "decimal?",
    "recovery_source_ex_vat": "decimal?",
    "included_source_ex_vat": "decimal?",
    "gross_due_source_ex_vat": "decimal?"
  ,"physical_revision_id":"uuid?","physical_timesheet_version":"positive?","source_kind":"sourceKind?","source_head_id":"uuid?","source_root_timesheet_id":"uuid?","source_root_timesheet_version":"positive?","source_week_ending_date":"date?"},
  "GROUP_LINES": {
    "item_id": "uuid",
    "group_key": "text",
    "component_key": "text",
    "member_key": "text",
    "candidate_id": "uuid",
    "work_id": "uuid?",
    "revision_id": "uuid?",
    "approved_line_id": "uuid?",
    "approved_segment_id": "uuid?",
    "case_id": "uuid?",
    "case_component_id": "uuid?",
    "rule_id": "uuid?",
    "client_id": "uuid?",
    "contract_id": "uuid?",
    "physical_timesheet_id": "uuid?",
    "snooze_id": "uuid?",
    "accepted_run_id": "uuid?",
    "accepted_command_id": "uuid?",
    "segment_key": "text?",
    "candidate_display_name": "text?",
    "candidate_reference": "text?",
    "client_display_name": "text?",
    "timesheet_reference": "text?",
    "component_kind": "text?",
    "tax_treatment": "text?",
    "unit_label": "text?",
    "pay_hold_reason": "text?",
    "channel_note_code": "text?",
    "snooze_scope": "text?",
    "stale_snooze_code": "text?",
    "error_code": "text?",
    "member_kind": "member",
    "portion_kind": "portion",
    "source_pay_channel": "channel?",
    "target_pay_channel": "channel?",
    "direction": "direction",
    "payroll_stage": "stage?",
    "work_date": "date?",
    "week_ending_date": "date?",
    "eligible_from_date": "date?",
    "snooze_until_date": "date?",
    "approved_quantity": "decimal?",
    "amount_known": "boolean",
    "selected_amount_known": "boolean",
    "selectable": "boolean",
    "is_selected": "boolean",
    "query_qualified": "boolean",
    "pay_hold": "boolean?",
    "section": "section",
    "reason_code": "code",
    "reason_priority": "small",
    "query_state": "query",
    "progress_state": "progress",
    "financial_view_revision": "integer",
    "projection_revision": "positive",
    "metadata_version": "positive",
    "snooze_version": "positive?",
    "accepted_module_epoch": "positive?",
    "nominal_source_ex_vat": "decimal?",
    "approved_source_ex_vat": "decimal?",
    "realised_source_ex_vat": "decimal?",
    "active_reserved_source_ex_vat": "decimal?",
    "residual_source_ex_vat": "decimal?",
    "gross_due_source_ex_vat": "decimal?",
    "included_source_ex_vat": "decimal?",
    "excluded_source_ex_vat": "decimal?",
    "recovery_source_ex_vat": "decimal?",
    "held_overhang_source_ex_vat": "decimal?",
    "deduction_balance_source_ex_vat": "decimal?",
    "selected_source_ex_vat": "decimal?",
    "written_off_source_ex_vat": "decimal?"
  ,"approved_line_no":"positive?","shift_detail_id":"uuid?","shift_detail_no":"positive?","job_title":"text?","band_label":"text?","original_work_ref":"text?","original_timesheet_id":"uuid?","is_adjustment":"boolean?","component_amount_known":"boolean?","component_ready_amount":"decimal?","component_waiting_amount":"decimal?","component_reserved_amount":"decimal?"},
  "EXPORT_ROWS": {
    "item_id": "uuid",
    "group_key": "text",
    "candidate_id": "uuid",
    "work_id": "uuid?",
    "case_id": "uuid?",
    "case_component_id": "uuid?",
    "revision_id": "uuid?",
    "client_id": "uuid?",
    "component_key": "text",
    "member_kind": "member",
    "member_key": "text",
    "portion_kind": "portion",
    "candidate_display_name": "text?",
    "candidate_reference": "text?",
    "client_display_name": "text?",
    "timesheet_reference": "text?",
    "work_date": "date?",
    "week_ending_date": "date?",
    "component_kind": "text?",
    "tax_treatment": "text?",
    "source_pay_channel": "channel?",
    "target_pay_channel": "channel?",
    "direction": "direction",
    "payroll_stage": "stage?",
    "approved_quantity": "decimal?",
    "unit_label": "text?",
    "nominal_source_ex_vat": "decimal?",
    "approved_source_ex_vat": "decimal?",
    "realised_source_ex_vat": "decimal?",
    "active_reserved_source_ex_vat": "decimal?",
    "residual_source_ex_vat": "decimal?",
    "included_source_ex_vat": "decimal?",
    "excluded_source_ex_vat": "decimal?",
    "deduction_balance_source_ex_vat": "decimal?",
    "amount_known": "boolean",
    "section": "section",
    "reason_code": "code",
    "selectable": "boolean",
    "financial_view_revision": "integer",
    "projection_revision": "positive",
    "metadata_version": "positive",
    "export_key":"exportKey","approved_line_id":"uuid?","physical_timesheet_id":"uuid?","job_title":"text?","band_label":"text?","shift_start":"text?","shift_finish":"text?","detail_label":"text?","candidate_has_pay":"boolean","export_amount":"decimal?","export_label":"text","choice_eligible":"boolean"
  },
  "CANDIDATE_GROUPS": {
    "group_key": "text",
    "group_kind": "group",
    "candidate_id": "uuid",
    "work_id": "uuid?",
    "case_id": "uuid?",
    "client_id": "uuid?",
    "contract_id": "uuid?",
    "physical_timesheet_id": "uuid?",
    "current_revision_id": "uuid?",
    "source_pay_channel": "channel?",
    "target_pay_channel": "channel?",
    "week_ending_date": "date?",
    "complete": "boolean",
    "dirty": "boolean",
    "certified_zero": "boolean",
    "ready_count": "integer",
    "action_count": "integer",
    "blocked_count": "integer",
    "waiting_count": "integer",
    "stale_snooze_count": "integer",
    "financial_view_revision": "integer",
    "projection_revision": "positive",
    "metadata_version": "positive",
    "stale_snooze_code": "text?",
    "error_code": "text?",
    "progress_state": "progress",
    "client_display_name": "text?",
    "bank_warning_code": "bank?",
    "section": "scopeSection",
    "reason_code": "code?",
    "has_deduction": "boolean",
    "selectable": "boolean",
    "group_amount": "decimal?",
    "ready_net_add_amount": "decimal?",
    "gross_deduction_balance": "decimal?",
    "net_deduction_balance": "decimal?",
    "presentation_id": "uuid",
    "parent_group_key": "groupKey",
    "case_component_id": "uuid?",
    "payment_label": "text",
    "timesheet_reference": "text?",
    "waiting_amount": "decimal?",
    "waiting_net_add_amount": "decimal?",
    "accepted_run_id": "uuid?",
    "accepted_command_id": "uuid?",
    "accepted_module_epoch": "positive?",
    "snooze_until_date": "date?",
    "eligible_from_date": "date?",
    "pay_hold_reason": "text?",
    "reason_priority": "small"
  ,"physical_revision_id":"uuid?","physical_timesheet_version":"positive?","source_kind":"sourceKind?","source_head_id":"uuid?","source_root_timesheet_id":"uuid?","source_root_timesheet_version":"positive?","source_week_ending_date":"date?"}
});
const MEMORY_ROWS={default_selected:'boolean',selection_eligible:'boolean',new_changed:'boolean',choice_member_count:'integer',choice_selected_count:'integer',selection_basis:'memoryBasis?'};
Object.assign(ROWS.HEADER,{selection_memory:'selectionMemory',selection_mode:'memoryMode',remembered_intent:'choiceIntent?'});
Object.assign(ROWS.EXPORT_ROWS,{candidate_default_pay_selected:'boolean'});
for(const kind of ['CANDIDATES','CANDIDATE_GROUPS','GROUP_LINES','EXPORT_ROWS'])Object.assign(ROWS[kind],MEMORY_ROWS);
const MEMORY_BASIS={
 WORK:{kind:'memoryWork',work_id:'uuid',revision_id:'uuid?',applied_revision_id:'uuid?',position_count:'integer',position_updated_at_utc:'jobTime?',approved_source_ex_vat:'decimal?',realised_source_ex_vat:'decimal?',held_source_ex_vat:'decimal?',source_book_revision:'integer',target_pay_channel:'channel?',target_umbrella_id:'uuid?',target_umbrella_vat_chargeable:'boolean?',min_take_home_wtd:'decimal?',control_sequence:'integer'},
 PARENT:{kind:'memoryParent',financial_view_revision:'integer'},
 CASE:{kind:'memoryCase',case_component_id:'uuid',component_revision:'positive',rule_id:'uuid',approved_source_ex_vat:'decimal',funded_source_ex_vat:'decimal',recovered_source_ex_vat:'decimal',paid_credit_source_ex_vat:'decimal',written_off_source_ex_vat:'decimal',active_payout_source_ex_vat:'decimal',active_recovery_source_ex_vat:'decimal',resolution_state:'memoryResolution',resolved_channel:'channel?',target_ex_vat:'decimal?',target_vat:'decimal?',valuation_policy_id:'uuid?',valuation_window_id:'uuid?',target_pay_channel:'channel?',target_umbrella_id:'uuid?',target_umbrella_vat_chargeable:'boolean?',min_take_home_wtd:'decimal?',control_sequence:'integer'},
 COMPONENT:{kind:'memoryComponent',work_id:'uuid',revision_id:'uuid',approved_line_id:'uuid',component_key:'text',applied_revision_id:'uuid?',approved_source_ex_vat:'decimal?',realised_source_ex_vat:'decimal?',held_source_ex_vat:'decimal?',realised_target_ex_vat:'decimal?',realised_target_vat:'decimal?',held_target_ex_vat:'decimal?',held_target_vat:'decimal?',target_pay_channel:'channel?',target_umbrella_id:'uuid?',target_umbrella_vat_chargeable:'boolean?',min_take_home_wtd:'decimal?',control_sequence:'integer'}
};
function memoryIntent(v){
 if(!exact(v,['mode','candidates','works','components','cases'])||!['ALL_MATCHING','NONE'].includes(v.mode))return false;
 const schemas={candidates:{candidate_id:'uuid',selected:'boolean'},works:{candidate_id:'uuid',work_id:'uuid',expected_revision_id:'uuid',selected:'boolean',mode:'mode'},components:{candidate_id:'uuid',work_id:'uuid',expected_revision_id:'uuid',approved_line_id:'uuid',selected:'boolean'},cases:{candidate_id:'uuid',case_component_id:'uuid',selected:'boolean'}};
 return Object.entries(schemas).every(([kind,schema])=>Array.isArray(v[kind])&&v[kind].length<=1000&&v[kind].every(row=>exact(row,Object.keys(schema))&&Object.entries(schema).every(([k,t])=>t==='mode'?['ALL','SUBSET'].includes(row[k]):field(row[k],t)))&&new Set(v[kind].map(row=>row.case_component_id??row.approved_line_id??row.work_id??row.candidate_id)).size===v[kind].length)&&ENCODER.encode(JSON.stringify(v)).byteLength<=32768;
}
const SCOPE=Object.freeze(Object.fromEntries(Object.entries(COMMON).filter(([k])=>!['after','limit','progress_after'].includes(k))));
const CONTEXT={kind:'kind',agency_id:'uuid',actor_user_id:'uuid',owner_epoch:'positive',official_pay_date:'date',workbench_revision:'integer',adoption_revision:'integer',scope:'scope',selection_memory:'selectionMemory'};
const REFUSAL={ok:'false',error_code:'refusal',retryable:'boolean',reload_required:'boolean',workbench_revision:'integer?',complete:'false',projection_state:'adoption?',dirty_candidate_count:'integer?',dirty_group_count:'integer?',pending_task_count:'integer?'};
const FAILURE_CODES=['BPAY_NEXT_MODULE_NOT_ACTIVE','BPAY_NEXT_WORKBENCH_FORBIDDEN','BPAY_NEXT_WORKBENCH_UNAUTHORIZED','BPAY_NEXT_WORKBENCH_METHOD_NOT_ALLOWED','BPAY_NEXT_WORKBENCH_REQUEST_INVALID','BPAY_NEXT_WORKBENCH_RESPONSE_INVALID','BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE','BPAY_NEXT_WORKBENCH_CURSOR_STALE','BPAY_NEXT_WORKBENCH_PROJECTION_NOT_CURRENT','BPAY_NEXT_WORKBENCH_DATE_UNAVAILABLE','BPAY_NEXT_WORKBENCH_UNAVAILABLE'];
const record=v=>v!==null&&typeof v==='object'&&!Array.isArray(v)&&[Object.prototype,null].includes(Object.getPrototypeOf(v));
function exact(v,keys){return record(v)&&Reflect.ownKeys(v).length===keys.length&&keys.every(k=>Object.hasOwn(v,k)&&Object.getOwnPropertyDescriptor(v,k)?.enumerable===true&&Object.hasOwn(Object.getOwnPropertyDescriptor(v,k),'value'));}
const same=(a,b)=>a===b||(Array.isArray(a)&&Array.isArray(b)&&a.length===b.length&&a.every((v,n)=>same(v,b[n])))||(record(a)&&record(b)&&exact(a,Object.keys(b))&&Object.keys(a).every(k=>same(a[k],b[k])));
const enums={kind:KINDS,scopeChannel:['ALL','PAYE','UMBRELLA'],channel:['PAYE','UMBRELLA'],scopeSection:['ALL','READY','ACTION','BLOCKED','WAITING'],section:['READY','ACTION','BLOCKED','WAITING'],sort:[...EXPORT_SORTS,'NAME_ASC','NAME_DESC','AMOUNT_ASC','AMOUNT_DESC','DEDUCTIONS_ASC','DEDUCTIONS_DESC','PAYMENT_ASC','PAYMENT_DESC','STATUS_ASC','STATUS_DESC','REASON_ASC','REASON_DESC','CLIENT_ASC','CLIENT_DESC','CHANNEL_ASC','CHANNEL_DESC'],bank:['BANK_DETAILS_MISSING','BANK_DETAILS_CHANGED'],group:['WORK','CASE','ROW'],member:['WORK_COMPONENT','WORK_SEGMENT','CASE_COMPONENT','GROUP_EVIDENCE'],portion:['AVAILABLE','RESERVED','RECOVERY','ZERO','EVIDENCE','WITHDRAWN'],direction:['PAYMENT','DEDUCTION','INFORMATIONAL'],stage:['GROSS_ADD','GROSS_DEDUCT','NET_ADD','NET_DEDUCT'],query:['NOT_APPLICABLE','UNAVAILABLE','CLEAR','OPEN'],progress:['CURRENT','PROCESSING','UNAVAILABLE'],adoption:['UNSEEDED','SEEDING','CURRENT','ERROR']};
function field(v,type){
  if(type==='workJobProgressKind')return v==='WORK_JOB_PROGRESS';
  if(type==='workJobStatus')return ['READY','LEASED','BLOCKED','FAILED'].includes(v);
  if(type==='jobPhase')return typeof v==='string'&&ENCODER.encode(v).byteLength>=1&&ENCODER.encode(v).byteLength<=64;
  if(type==='jobCheckpoint')return typeof v==='string'&&ENCODER.encode(v).byteLength<=512;
  if(type==='jobError')return typeof v==='string'&&/^BPAY_NEXT_[A-Z0-9_]{1,240}$/.test(v);
  if(type==='jobTime')return typeof v==='string'&&v.length<=40&&Number.isFinite(Date.parse(v));
  if(type==='progressScope')return shape(v,{candidate_id:'uuid?',client_id:'uuid?',channel:'scopeChannel'});
  if(type==='progressCursor')return progressCursor(v);
  if(type.endsWith('?'))return v===null||field(v,type.slice(0,-1));
  if(Object.hasOwn(enums,type))return enums[type].includes(v);
  if(type==='uuid')return typeof v==='string'&&UUID.test(v);
  if(type==='boolean'||type==='false')return typeof v==='boolean'&&(type!=='false'||v===false);
  if(type==='integer'||type==='positive')return typeof v==='string'&&/^(0|[1-9]\d{0,18})$/.test(v)&&BigInt(v)<=9223372036854775807n&&(type!=='positive'||v!=='0');
  if(type==='small')return Number.isInteger(v)&&v>=1&&v<=200;
  if(type==='limit')return Number.isInteger(v)&&v>=1&&v<=100;
  if(type==='decimal')return typeof v==='string'&&ENCODER.encode(v).byteLength<=BPAY_NEXT_WORKBENCH_LIMITS.responseBytes&&/^-?(?:0|[1-9]\d*)(?:\.\d+)?$/.test(v);
  if(type==='text'||type==='search')return typeof v==='string'&&!v.includes('\0')&&ENCODER.encode(v).byteLength<=(type==='search'?800:8192)&&(type!=='search'||([...v].length<=200&&v.trim()===v));
  if(type==='code')return typeof v==='string'&&/^[A-Z][A-Z0-9_]{0,255}$/.test(v);
  if(type==='groupKey')return typeof v==='string'&&/^(?:WORK|CASE|ROW):[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(v);
  if(type==='date'){if(typeof v!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(v)||v.startsWith('0000'))return false;const d=new Date(`${v}T00:00:00Z`);return Number.isFinite(d.getTime())&&d.toISOString().slice(0,10)===v;}
  if(type==='selectionMemory')return shape(v,{run_id:'uuid?',scope:'progressScope'});
  if(type==='choiceIntent')return memoryIntent(v);
  if(type==='memoryMode')return ['ALL_MATCHING','NONE'].includes(v);
  if(type==='memoryResolution')return ['RESOLVED','REVIEW'].includes(v);
  if(type==='memoryResolution')return ['RESOLVED','REVIEW'].includes(v);
  if(type==='memoryWork')return v==='WORK';if(type==='memoryParent')return v==='PARENT';if(type==='memoryCase')return v==='CASE';if(type==='memoryComponent')return v==='COMPONENT';
  if(type==='memoryBasis')return record(v)&&Object.hasOwn(MEMORY_BASIS,v.kind)&&shape(v,MEMORY_BASIS[v.kind]);
  if(type==='countSelection')return (exact(v,['memory_baseline_run_id','intent'])&&field(v.memory_baseline_run_id,'uuid?')&&memoryIntent(v.intent))||(exact(v,['mode','candidate_ids'])&&['ALL','NONE'].includes(v.mode)&&Array.isArray(v.candidate_ids)&&v.candidate_ids.every(id=>field(id,'uuid'))&&new Set(v.candidate_ids).size===v.candidate_ids.length&&ENCODER.encode(JSON.stringify(v)).byteLength<=32768);
  if(type==='exportKey')return Array.isArray(v)&&v.length>=4&&v.length<=6&&v.every(x=>typeof x==='string'||typeof x==='boolean'||Number.isInteger(x));
  if(type==='scope')return shape(v,SCOPE);
  if(type==='sourceKind')return ['ORDINARY','SOURCE','PROTECTED','EXPENSE','ADJUSTMENT'].includes(v);
  if(type==='cursor')return exact(v,['context','key'])&&shape(v.context,CONTEXT)&&validKey(v.key,v.context.kind,v.context.scope.sort);
  if(type==='refusal')return FAILURE_CODES.includes(v);
  return false;
}
const shape=(v,schema)=>exact(v,Object.keys(schema))&&Object.entries(schema).every(([k,type])=>field(v[k],type));
function validKey(key,kind,sort){
  if(!Array.isArray(key))return false;
  if(kind==='EXPORT_ROWS'){const types=EXPORT_SORT_FIELDS[sort.replace(/_(?:ASC|DESC)$/,'')];return Boolean(types&&key.length===types.length&&types.every((t,n)=>field(key[n],t)));}
  if(['CANDIDATES','PICK_CANDIDATES'].includes(kind))return sort.startsWith('STATUS_')?key.length===5&&field(key[0],'boolean')&&field(key[1],'boolean')&&field(key[2],'text')&&field(key[3],'text')&&field(key[4],'uuid'):sort.startsWith('DEDUCTIONS_')?key.length===5&&field(key[0],'boolean')&&field(key[1],'text')&&field(key[2],'boolean')&&field(key[3],'text')&&field(key[4],'uuid'):sort.startsWith('AMOUNT_')?key.length===3&&field(key[0],'boolean')&&field(key[1],'decimal')&&field(key[2],'uuid'):key.length===4&&field(key[0],'text')&&field(key[1],'boolean')&&field(key[2],'text')&&field(key[3],'uuid');
  if(kind==='CANDIDATE_GROUPS'){
    if(sort.startsWith('DEDUCTIONS_'))return key.length===3&&field(key[0],'boolean')&&field(key[1],'group')&&field(key[2],'uuid');
    if(key.length!==4||!field(key[2],'group')||!field(key[3],'uuid'))return false;
    if(sort.startsWith('REASON_'))return field(key[0],'small')&&field(key[1],'code');
    if(!field(key[0],'boolean'))return false;
    if(sort.startsWith('AMOUNT_'))return field(key[1],'decimal')&&(!key[0]||key[1]==='0');
    if(sort.startsWith('CLIENT_'))return field(key[1],'text')&&(!key[0]||key[1]==='');
    if(sort.startsWith('CHANNEL_'))return key[0]?key[1]==='':field(key[1],'channel');
    return key[0]?key[1]==='':field(key[1],'date');
  }
  if(['ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL'].includes(kind))return sort.startsWith('REASON_')?key.length===3&&field(key[0],'small')&&field(key[1],'text')&&field(key[2],'uuid'):sort.startsWith('PAYMENT_')?key.length===2&&field(key[0],'text')&&field(key[1],'uuid'):sort.startsWith('AMOUNT_')?key.length===3&&field(key[0],'boolean')&&field(key[1],'decimal')&&field(key[2],'uuid'):key.length===3&&field(key[0],'text')&&field(key[1],'uuid')&&field(key[2],'uuid');
  return kind!=='HEADER'&&key.length===1&&field(key[0],kind==='CANDIDATE_GROUPS'?'groupKey':'uuid');
}
function rowKey(kind,scope,row){
  if(['CANDIDATES','PICK_CANDIDATES'].includes(kind)){if(scope.sort.startsWith('STATUS_'))return [row.payable_count==='0',row.bank_warning_code===null,row.bank_warning_code??'',row.candidate_sort_name,row.candidate_id];if(scope.sort.startsWith('DEDUCTIONS_'))return [row.has_deduction,row.candidate_sort_name,row.tms_ref===null,row.tms_ref??'',row.candidate_id];if(scope.sort.startsWith('AMOUNT_'))return [row.unknown_amount_count!=='0',row.amount??'0',row.candidate_id];return [row.candidate_sort_name,row.tms_ref===null,row.tms_ref??'',row.candidate_id];}
  if(kind==='EXPORT_ROWS')return row.export_key;
  if(kind==='HEADER')return null;
  if(kind==='CANDIDATE_GROUPS'){
    const suffix=[row.group_kind,row.presentation_id];
    if(scope.sort.startsWith('CLIENT_'))return [row.client_display_name===null,row.client_display_name??'',...suffix];
    if(scope.sort.startsWith('CHANNEL_'))return [row.target_pay_channel===null,row.target_pay_channel??'',...suffix];
    if(scope.sort.startsWith('DEDUCTIONS_'))return [row.has_deduction,...suffix];
    if(scope.sort.startsWith('AMOUNT_'))return [row.group_amount===null,row.group_amount??'0',...suffix];
    if(scope.sort.startsWith('REASON_'))return [row.reason_priority,row.reason_code,...suffix];
    return [row.week_ending_date===null,row.week_ending_date??'',...suffix];
  }
  if(['ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL'].includes(kind)){if(scope.sort.startsWith('REASON_'))return [row.reason_priority,row.reason_code,row.item_id];if(scope.sort.startsWith('PAYMENT_'))return [row.timesheet_reference??'',row.item_id];if(scope.sort.startsWith('AMOUNT_'))return [row.nominal_source_ex_vat===null,row.nominal_source_ex_vat??'0',row.item_id];return [row.candidate_display_name??'',row.candidate_id,row.item_id];}
  return [kind==='PICK_CLIENTS'?row.client_id:row.item_id];
}
function textCompare(a,b){const x=ENCODER.encode(a),y=ENCODER.encode(b);for(let n=0;n<Math.min(x.length,y.length);n++)if(x[n]!==y[n])return x[n]<y[n]?-1:1;return Math.sign(x.length-y.length);}
// Ordering validation only. Exact decimal strings never become browser money.
function decimalCompare(a,b){const negA=a.startsWith('-'),negB=b.startsWith('-');const parts=v=>{const [whole,fraction='']=v.replace(/^-/,'').split('.');return [whole,fraction.replace(/0+$/,'')];};const [aw,af]=parts(a),[bw,bf]=parts(b);const az=aw==='0'&&af==='',bz=bw==='0'&&bf==='';if(az&&bz)return 0;const an=negA&&!az,bn=negB&&!bz;if(an!==bn)return an?-1:1;const sign=an?-1:1;return sign*(aw.length!==bw.length?Math.sign(aw.length-bw.length):aw!==bw?textCompare(aw,bw):textCompare(af.padEnd(Math.max(af.length,bf.length),'0'),bf.padEnd(Math.max(af.length,bf.length),'0')));}
function keyCompare(a,b,kind,sort){for(let n=0;n<a.length;n++){const value=typeof a[n]==='number'?a[n]-b[n]:typeof a[n]==='boolean'?Number(a[n])-Number(b[n]):n===1&&(sort.startsWith('AMOUNT_')||kind==='EXPORT_ROWS'&&sort.includes('_AMOUNT_'))?decimalCompare(a[n],b[n]):textCompare(a[n],b[n]);if(value)return sort.endsWith('_DESC')?-value:value;}return 0;}
function fail(code,status=503,retryable=false,reload=false){return Object.assign(new Error(code),{code,status,retryable,reload});}
export function normalizeWorkbenchInput(kind,input={}){
  if(!KINDS.includes(kind)||!record(input)||Reflect.ownKeys(input).some(k=>typeof k!=='string'||!Object.hasOwn(COMMON,k)))throw fail('BPAY_NEXT_WORKBENCH_REQUEST_INVALID',400);
  const args={candidate_id:null,client_id:null,channel:'ALL',section:'ALL',search:'',sort:'NAME_ASC',group_key:null,item_id:null,reason_code:null,count_selection:null,selection_memory:null,progress_after:null,after:null,limit:kind==='CANDIDATE_GROUPS'?10:50,...input};
  for(const k of ['candidate_id','client_id','item_id'])if(typeof args[k]==='string')args[k]=args[k].toLowerCase();
  if(args.count_selection!==null&&(kind!=='HEADER'||args.section!=='READY')||kind!=='CANDIDATES'&&String(args.sort).startsWith('STATUS_'))throw fail('BPAY_NEXT_WORKBENCH_REQUEST_INVALID',400);
  if(args.progress_after!==null&&kind!=='HEADER')throw fail('BPAY_NEXT_WORKBENCH_REQUEST_INVALID',400);
  if(!shape(args,COMMON)||(kind==='CANDIDATE_GROUPS'&&args.limit>10)||(kind==='EXPORT_ROWS'?!EXPORT_SORTS.includes(args.sort):EXPORT_SORTS.includes(args.sort)||!['CANDIDATES','ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL','CANDIDATE_GROUPS'].includes(kind)&&args.sort!=='NAME_ASC')||(!['ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL','CANDIDATE_GROUPS'].includes(kind)&&args.sort.startsWith('REASON_'))||(kind==='CANDIDATES'&&args.sort.startsWith('PAYMENT_'))||(['ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL'].includes(kind)&&args.sort.startsWith('DEDUCTIONS_'))
    ||(['CANDIDATE_GROUPS','GROUP_LINES'].includes(kind)&&args.candidate_id===null)||(['GROUP_LINES','ISSUE_DETAIL'].includes(kind)&&args.group_key===null)||(kind==='ISSUE_DETAIL'&&args.reason_code===null)
    ||(!['GROUP_LINES','ISSUE_DETAIL','CANDIDATE_GROUPS'].includes(kind)&&[args.group_key,args.item_id,args.reason_code].some(v=>v!==null)||kind==='CANDIDATE_GROUPS'&&[args.item_id,args.reason_code].some(v=>v!==null))
    ||(!['CANDIDATES','PICK_CANDIDATES','PICK_CLIENTS','ACTION_REQUIRED','BLOCKED','EXPORT_ROWS'].includes(kind)&&args.search!=='')||(kind==='HEADER'&&args.after!==null))throw fail('BPAY_NEXT_WORKBENCH_REQUEST_INVALID',400);
  if((args.sort.startsWith('CLIENT_')||args.sort.startsWith('CHANNEL_'))&&kind!=='CANDIDATE_GROUPS'||kind==='CANDIDATE_GROUPS'&&args.sort==='NAME_DESC'||args.group_key?.startsWith('ROW:')&&!['GROUP_LINES','CANDIDATE_GROUPS'].includes(kind))throw fail('BPAY_NEXT_WORKBENCH_REQUEST_INVALID',400);
  if(kind==='CANDIDATES')args.section='READY';else if(kind==='ACTION_REQUIRED')args.section='ACTION';else if(kind==='BLOCKED')args.section='BLOCKED';
  if(kind==='PICK_CANDIDATES'&&args.sort!=='NAME_ASC')throw fail('BPAY_NEXT_WORKBENCH_REQUEST_INVALID',400);
  const scope=Object.fromEntries(Object.keys(SCOPE).map(k=>[k,args[k]]));
  if(args.after!==null&&(args.after.context.kind!==kind||!same(args.after.context.scope,scope)))throw fail('BPAY_NEXT_WORKBENCH_CURSOR_STALE',409,false,true);
  return args;
}
function parseUniqueJson(text){const stack=[];for(let n=0;n<text.length;n++){const c=text[n];if(c==='{'||c==='['){stack.push(c==='{'?new Set():null);if(stack.length>8)throw new Error();}else if(c==='}'||c===']')stack.pop();else if(c==='"'){const start=n;n++;for(;n<text.length;n++){if(text[n]==='\\')n++;else if(text[n]==='"')break;}const token=JSON.parse(text.slice(start,n+1));let after=n+1;while(after<text.length&&/\s/.test(text[after]))after++;if(text[after]===':'){const keys=stack.at(-1);if(!(keys instanceof Set)||keys.has(token))throw new Error();keys.add(token);}}}return JSON.parse(text);}
export async function readWorkbenchJson(response,signal,maximum=BPAY_NEXT_WORKBENCH_LIMITS.responseBytes){
  const reader=response.body?.getReader();if(!reader)throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
  const cancel=()=>{void reader.cancel().catch(()=>{});};signal?.addEventListener('abort',cancel,{once:true});
  const chunks=[];let length=0;try{while(true){if(signal?.aborted)throw fail('BPAY_NEXT_WORKBENCH_UNAVAILABLE',503,true);const {done,value}=await reader.read();if(signal?.aborted)throw fail('BPAY_NEXT_WORKBENCH_UNAVAILABLE',503,true);if(done)break;length+=value.byteLength;if(length>maximum)throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE');chunks.push(value);}
    const bytes=new Uint8Array(length);let offset=0;for(const chunk of chunks){bytes.set(chunk,offset);offset+=chunk.byteLength;}return parseUniqueJson(new TextDecoder('utf-8',{fatal:true}).decode(bytes));
  }catch(e){void reader.cancel().catch(()=>{});throw e?.code?e:fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');}finally{signal?.removeEventListener('abort',cancel);reader.releaseLock();}
}

const JOB_PROGRESS_CONTEXT={kind:'workJobProgressKind',agency_id:'uuid',actor_user_id:'uuid',owner_epoch:'positive',official_pay_date:'date',workbench_revision:'integer',adoption_revision:'integer',scope:'progressScope'};
const JOB_PROGRESS_ROW={job_id:'uuid',work_id:'uuid',candidate_id:'uuid',revision_id:'uuid',command_id:'uuid',command_sequence:'positive',module_epoch:'positive',owner_epoch:'positive',status:'workJobStatus',phase:'jobPhase',cursor:'jobCheckpoint?',attempt_count:'integer',processing:'boolean',failed_job_code:'jobError?',first_error_code:'jobError?',last_error_code:'jobError?',first_error_at_utc:'jobTime?',last_error_at_utc:'jobTime?',created_at_utc:'jobTime?',started_at_utc:'jobTime?',finished_at_utc:'jobTime?',candidate_display_name:'text?',timesheet_reference:'text?',client_display_name:'text?',physical_timesheet_id:'uuid?',evidence_available:'boolean'};
function progressKey(v){return Array.isArray(v)&&v.length===2&&field(v[0],'positive')&&field(v[1],'uuid');}
function progressCursor(v){return exact(v,['context','key'])&&shape(v.context,JOB_PROGRESS_CONTEXT)&&progressKey(v.key);}
function validateJobProgress(progress,args,actorId,revision){
 const invalid=()=>{throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');};
 if(!exact(progress,['context','rows','complete','next_cursor','oldest_pending_age_seconds'])||!shape(progress.context,JOB_PROGRESS_CONTEXT)
  ||!Array.isArray(progress.rows)||progress.rows.length>args.limit||typeof progress.complete!=='boolean'||!field(progress.oldest_pending_age_seconds,'integer?')
  ||progress.context.workbench_revision!==revision||actorId!==null&&progress.context.actor_user_id!==actorId
  ||!same(progress.context.scope,{candidate_id:args.candidate_id,client_id:args.client_id,channel:args.channel})
  ||args.progress_after!==null&&!same(args.progress_after.context,progress.context))invalid();
 let prior=args.progress_after?.key??null,last=null;
 for(const row of progress.rows){const key=[row.command_sequence,row.job_id];
  if(!shape(row,JOB_PROGRESS_ROW)||row.module_epoch!==progress.context.owner_epoch||args.candidate_id!==null&&row.candidate_id!==args.candidate_id
   ||row.processing!==(row.status!=='FAILED')||row.status==='FAILED'&&row.failed_job_code!==row.last_error_code||row.status!=='FAILED'&&row.failed_job_code!==null
   ||prior!==null&&(BigInt(prior[0])>BigInt(key[0])||prior[0]===key[0]&&prior[1]>=key[1]))invalid();
  prior=key;last=key;
 }
 if(!progress.complete&&last===null||!same(progress.next_cursor,progress.complete?null:{context:progress.context,key:last}))invalid();
 return progress;
}

export function validateWorkbenchResponse(payload,kind,args,actorId=null){
  const original=payload;
  if(Object.hasOwn(payload??{},'job_progress')){
    if(kind!=='HEADER')throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
    validateJobProgress(payload.job_progress,args,actorId,payload.workbench_revision);
    payload={...payload};delete payload.job_progress;
    if(payload.ok===true){if(!Array.isArray(payload.rows)||payload.rows.length!==1||!Object.hasOwn(payload.rows[0],'oldest_pending_age_seconds')
      ||payload.rows[0].oldest_pending_age_seconds!==original.job_progress.oldest_pending_age_seconds)throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
      if(!['agency_id','owner_epoch','official_pay_date','adoption_revision'].every(k=>payload.context?.[k]===original.job_progress.context[k]))throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
      const row={...payload.rows[0]};delete row.oldest_pending_age_seconds;payload.rows=[row];}
  }

  if(payload?.ok===false){if(!shape(payload,REFUSAL)||payload.reload_required!==(payload.error_code==='BPAY_NEXT_WORKBENCH_CURSOR_STALE'))throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');return original;}
  if(!exact(payload,['ok','kind','workbench_revision','complete','context','rows','next_cursor','page_receipt','affected_candidate_count'])||payload.ok!==true||payload.kind!==kind||!field(payload.workbench_revision,'integer')||typeof payload.complete!=='boolean'||!shape(payload.context,CONTEXT)||payload.context.kind!==kind||payload.context.workbench_revision!==payload.workbench_revision||!Array.isArray(payload.rows)||payload.rows.length>args.limit||(kind==='HEADER'&&payload.rows.length!==1)||(!payload.complete&&payload.rows.length===0)||(kind==='ISSUE_DETAIL'?!field(payload.affected_candidate_count,'integer'):payload.affected_candidate_count!==null))throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
  const scope=Object.fromEntries(Object.keys(SCOPE).map(k=>[k,args[k]])),context=payload.context;
  if(!same(context.scope,scope)||(actorId!==null&&context.actor_user_id!==actorId)||args.after!==null&&!same(args.after.context,context))throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
  const memory=args.selection_memory;
  if(memory!==null&&!same(context.selection_memory,memory)||context.selection_memory.scope.client_id!==args.client_id||context.selection_memory.scope.channel!==args.channel||context.selection_memory.scope.candidate_id!==null&&context.selection_memory.scope.candidate_id!==args.candidate_id)throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
  let prior=args.after?.key??null,last=null;const seen=new Set();
  for(const row of payload.rows){if(!shape(row,ROWS[kind]))throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');const key=rowKey(kind,scope,row);
    if(kind==='HEADER'&&(!same(row.selection_memory,context.selection_memory)||row.remembered_intent!==null&&row.remembered_intent.mode!==row.selection_mode))throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
    if(Object.hasOwn(row,'choice_member_count')){
      const total=BigInt(row.choice_member_count),selected=BigInt(row.choice_selected_count),basis=row.selection_basis;
      if(selected>total||row.default_selected!==(total>0n&&selected===total)||total===0n&&(row.selection_eligible||row.new_changed))throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
      if(basis!==null){
        const expected=kind==='CANDIDATES'?'PARENT':kind==='CANDIDATE_GROUPS'?row.group_kind==='WORK'?'WORK':row.group_kind==='ROW'?'CASE':null:row.work_id!==null?'COMPONENT':row.case_component_id!==null?'CASE':null;
        if(basis.kind!==expected||expected==='WORK'&&basis.work_id!==row.work_id||expected==='CASE'&&basis.case_component_id!==row.case_component_id||expected==='COMPONENT'&&(basis.work_id!==row.work_id||basis.revision_id!==row.revision_id||basis.approved_line_id!==row.approved_line_id||basis.component_key!==row.component_key))throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
      }
    }

    if(key!==null){if(!validKey(key,kind,scope.sort)||(prior!==null&&keyCompare(prior,key,kind,scope.sort)>=0)||seen.has(JSON.stringify(key)))throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');seen.add(JSON.stringify(key));prior=key;last=key;}
    if(args.candidate_id!==null&&Object.hasOwn(row,'candidate_id')&&row.candidate_id!==args.candidate_id||args.client_id!==null&&Object.hasOwn(row,'client_id')&&row.client_id!==args.client_id||kind!=='CANDIDATE_GROUPS'&&args.group_key!==null&&row.group_key!==args.group_key||args.item_id!==null&&row.item_id!==args.item_id||args.reason_code!==null&&row.reason_code!==args.reason_code)throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
    if(Object.hasOwn(row,'section')&&scope.section!=='ALL'&&row.section!==scope.section||Object.hasOwn(row,'target_pay_channel')&&scope.channel!=='ALL'&&row.target_pay_channel!==scope.channel)throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
    if(Object.hasOwn(row,'amount_known')&&row.amount_known===false&&row.nominal_source_ex_vat!==null)throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
    if(Object.hasOwn(row,'amount_known')&&row.selectable===true&&(row.section!=='READY'||row.amount_known!==true||!['AVAILABLE','ZERO'].includes(row.portion_kind)))throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
    if(Object.hasOwn(row,'group_key')){if(!field(row.group_key,'groupKey')||(row.work_id!==null)===(row.case_id!==null)||row.group_key!==(row.work_id!==null?'WORK:'+row.work_id:row.group_key.startsWith('ROW:')?'ROW:'+row.case_component_id:'CASE:'+row.case_id))throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');}
    if(kind==='EXPORT_ROWS'&&(row.export_key.at(-1)!==row.item_id||row.work_id===null&&[row.approved_line_id,row.physical_timesheet_id,row.job_title,row.band_label,row.shift_start,row.shift_finish].some(v=>v!==null)||row.choice_eligible&&(!row.selectable||row.section!=='READY')))throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
    if(kind==='GROUP_LINES'){
      const facts=['approved_line_no','job_title','band_label','original_work_ref','original_timesheet_id','is_adjustment','component_amount_known','component_ready_amount','component_waiting_amount','component_reserved_amount'];
      if(row.work_id===null&&facts.some(k=>row[k]!==null)||(row.shift_detail_id===null)!==(row.shift_detail_no===null)||row.approved_segment_id===null&&row.shift_detail_id!==null||row.original_timesheet_id===null&&row.original_work_ref!==null||row.component_amount_known!==true&&['component_ready_amount','component_waiting_amount','component_reserved_amount'].some(k=>row[k]!==null)||row.component_amount_known===true&&['component_ready_amount','component_waiting_amount','component_reserved_amount'].some(k=>row[k]===null))throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
    }
    if(['CANDIDATE_GROUPS','ISSUE_DETAIL'].includes(kind)){
      const ids=['physical_revision_id','physical_timesheet_id','physical_timesheet_version'];
      if(ids.some(k=>row[k]===null)&&ids.some(k=>row[k]!==null)||row.work_id===null&&[...ids,'source_kind','source_head_id','source_root_timesheet_id','source_root_timesheet_version','source_week_ending_date'].some(k=>row[k]!==null)
        ||(row.source_root_timesheet_id===null)!==(row.source_root_timesheet_version===null)
        ||!['SOURCE','PROTECTED'].includes(row.source_kind)&&['source_head_id','source_root_timesheet_id','source_root_timesheet_version','source_week_ending_date'].some(k=>row[k]!==null)
        ||['SOURCE','PROTECTED'].includes(row.source_kind)&&row.source_week_ending_date===null)throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
    }
    if(kind==='CANDIDATE_GROUPS'){
      if(row.group_key!==row.group_kind+':'+row.presentation_id||row.parent_group_key!==(row.work_id!==null?'WORK:'+row.work_id:'CASE:'+row.case_id)||(row.group_kind==='ROW')!==(row.case_component_id!==null)||row.group_kind==='ROW'&&row.presentation_id!==row.case_component_id||(row.accepted_run_id===null)!==(row.accepted_command_id===null)||(row.accepted_run_id===null)!==(row.accepted_module_epoch===null)||['ready_net_add_amount','gross_deduction_balance','net_deduction_balance','waiting_amount','waiting_net_add_amount'].some(k=>(row[k]===null)!==(row.group_amount===null)))throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
    }
    if(kind==='CANDIDATE_GROUPS'&&row.selectable&&(!row.complete||row.dirty||row.progress_state!=='CURRENT'))throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
    if(kind==='HEADER'){
      const counted=args.count_selection!==null;
      if((row.selected_candidate_count!==null)!==counted||(row.selected_member_count!==null)!==counted||(row.selectable_member_count!==null)!==(args.section==='READY')||counted&&(BigInt(row.selected_candidate_count)>BigInt(row.candidate_count)||BigInt(row.selected_member_count)>BigInt(row.selectable_member_count)))throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
    }
    if(['HEADER','CANDIDATES'].includes(kind)){const unknown=row.unknown_amount_count!=='0';for(const k of ['amount','waiting_amount','ready_net_add_amount','waiting_net_add_amount','gross_deduction_balance','net_deduction_balance','approved_amount','paid_credit_amount','reserved_amount','excluded_amount','recovery_amount'])if((row[k]===null)!==unknown)throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');if((row.selected_amount===null)!==(row.unknown_selected_amount_count!=='0')||row.has_deduction!==(row.deduction_item_count!=='0'))throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');}
  }
  const expected=payload.complete?null:{context,key:last};if(!same(payload.next_cursor,expected)||!exact(payload.page_receipt,['context','start_key','end_key','row_count'])||!same(payload.page_receipt.context,context)||!same(payload.page_receipt.start_key,args.after?.key??null)||!same(payload.page_receipt.end_key,last)||payload.page_receipt.row_count!==String(payload.rows.length))throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
  return original;
}
export function createBpayNextWorkbenchRpc({baseUrl,headers,fetchImpl=fetch}){
  const endpoint=new URL(`${String(baseUrl||'').replace(/\/+$/,'')}/rest/v1/rpc/bpay_next_workbench_page_v1`);
  if(!['http:','https:'].includes(endpoint.protocol)||endpoint.username||endpoint.password||endpoint.search||endpoint.hash)throw new TypeError('BPAY_NEXT_WORKBENCH_TRANSPORT_INVALID');
  return async(kind,args,options)=>{if(!KINDS.includes(kind)||!field(options?.actorUserId,'uuid')||!options.signal)throw new TypeError('BPAY_NEXT_WORKBENCH_TRANSPORT_INVALID');
    const body=JSON.stringify({p_request:{actor_user_id:options.actorUserId,kind,...normalizeWorkbenchInput(kind,args)}});if(ENCODER.encode(body).byteLength>32768)throw fail('BPAY_NEXT_WORKBENCH_REQUEST_INVALID',400);
    const h=new Headers(headers);h.set('content-type','application/json');h.set('accept','application/json');h.set('X-CloudTMS-Banking-Pay-Route-Class','READ');
    return fetchImpl(endpoint.href,{method:'POST',headers:h,body,redirect:'error',signal:options.signal});};
}
const json=(status,body,kind=null)=>{
  const bytes=JSON.stringify(body),headers={'content-type':'application/json; charset=utf-8','cache-control':'no-store','x-content-type-options':'nosniff'};
  // WB10 factual read metadata only: no identity, search, cursor or bank fields.
  if(KINDS.includes(kind)){headers['x-bpay-next-step']='kind='+kind+';rows='+(body.ok===true?body.rows.length:0)+';bytes='+ENCODER.encode(bytes).byteLength;headers['access-control-expose-headers']='x-bpay-next-step';}
  return new Response(bytes,{status,headers});
};
function refusalStatus(code){return code.endsWith('_FORBIDDEN')?403:code.endsWith('_REQUEST_INVALID')?400:code.endsWith('_CURSOR_STALE')?409:503;}
export async function handleBpayNextWorkbenchPage(request,dependencies={}){
  let timer,controller,readKind=null;try{
    if(request.method!=='GET')throw fail('BPAY_NEXT_WORKBENCH_METHOD_NOT_ALLOWED',405);
    const user=await dependencies.requireOfficeUser?.(request,['admin']);if(!user||typeof user.id!=='string'||!UUID.test(user.id.toLowerCase()))throw fail('BPAY_NEXT_WORKBENCH_UNAUTHORIZED',401);if(user.role!=='admin')throw fail('BPAY_NEXT_WORKBENCH_FORBIDDEN',403);
    const actor=user.id.toLowerCase(),url=new URL(request.url);if(ENCODER.encode(url.search).byteLength>32768)throw fail('BPAY_NEXT_WORKBENCH_REQUEST_INVALID',400);
    const input={},kind=url.searchParams.get('kind');for(const [key,value] of url.searchParams){if(url.searchParams.getAll(key).length!==1||key!=='kind'&&!Object.hasOwn(COMMON,key))throw fail('BPAY_NEXT_WORKBENCH_REQUEST_INVALID',400);if(key==='kind')continue;if(key==='after'||key==='count_selection'||key==='progress_after'||key==='selection_memory'){try{input[key]=parseUniqueJson(value);}catch{throw fail('BPAY_NEXT_WORKBENCH_REQUEST_INVALID',400);}}else if(key==='limit'){if(!/^[1-9]\d{0,2}$/.test(value))throw fail('BPAY_NEXT_WORKBENCH_REQUEST_INVALID',400);input.limit=Number(value);}else input[key]=value;}
    const args=normalizeWorkbenchInput(kind,input);readKind=kind;if(args.after!==null&&args.after.context.actor_user_id!==actor)throw fail('BPAY_NEXT_WORKBENCH_CURSOR_STALE',409,false,true);
    if(typeof dependencies.workbenchRpc!=='function')throw fail('BPAY_NEXT_WORKBENCH_UNAVAILABLE',503,true);
    controller=new AbortController();const timeout=new Promise((_,reject)=>{timer=setTimeout(()=>{controller.abort();reject(fail('BPAY_NEXT_WORKBENCH_UNAVAILABLE',503,true));},BPAY_NEXT_WORKBENCH_LIMITS.timeoutMs);});
    const send=async()=>{const response=await dependencies.workbenchRpc(kind,args,{actorUserId:actor,signal:controller.signal});if(!(response instanceof Response))throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');if(controller.signal.aborted){void response.body?.cancel().catch(()=>{});throw fail('BPAY_NEXT_WORKBENCH_UNAVAILABLE',503,true);}const payload=await readWorkbenchJson(response,controller.signal);
      if(!response.ok)throw fail(response.status===401||response.status===403?'BPAY_NEXT_WORKBENCH_FORBIDDEN':'BPAY_NEXT_WORKBENCH_UNAVAILABLE',response.status===401||response.status===403?403:503,true);
      const body=validateWorkbenchResponse(payload,kind,args,actor);if(ENCODER.encode(JSON.stringify(body)).byteLength>131072)throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE');return json(body.ok?200:refusalStatus(body.error_code),body,kind);};
    return await Promise.race([send(),timeout]);
  }catch(e){const safe=typeof e?.code==='string'&&/^BPAY_NEXT_[A-Z_]+$/.test(e.code)?e:fail('BPAY_NEXT_WORKBENCH_UNAVAILABLE',503,true);return json(safe.status??503,{ok:false,error_code:safe.code,retryable:safe.retryable===true,reload_required:safe.reload===true,workbench_revision:null,complete:false,projection_state:null,dirty_candidate_count:null,dirty_group_count:null,pending_task_count:null},readKind);}
  finally{if(timer!==undefined)clearTimeout(timer);}
}

// WB18 output-only walk. The complete local intent stays immutable here and is
// never copied into every SQL cursor or admitted as a financial command.
export const BPAY_NEXT_EXPORT_HEADERS=Object.freeze(['Included','Line type','Breakdown type','Candidate ref','Candidate','Client','Week / Date','Channel','Amount','Timesheet ID','Economic key type','Economic key value','Role','Band','Start','Finish','Break','Detail','Preview row ID']);
const EXPORT_LIMITS=Object.freeze({requestBytes:32768,csvBytes:33554432,pages:2000,rows:200000,deadlineMs:120000});
function exportInput(body,actor){
  if(!exact(body,['choice','context','candidate_id','sort'])||!shape(body.context,CONTEXT)||body.context.actor_user_id!==actor||!field(body.candidate_id,'uuid?')||!EXPORT_SORTS.includes(body.sort))throw fail('BPAY_NEXT_WORKBENCH_REQUEST_INVALID',400);
  const v=body.choice;
  if(!exact(v,['namespace','intent','revision','workbench_revision','adoption_revision',...(record(v)&&Object.hasOwn(v,'memory_baseline_run_id')?['memory_baseline_run_id']:[])])||Object.hasOwn(v,'memory_baseline_run_id')&&!field(v.memory_baseline_run_id,'uuid?')||!exact(v.namespace,['agency_id','owner_epoch','actor_user_id','official_pay_date','scope'])||!exact(v.namespace.scope,['candidate_id','client_id','channel'])||!Number.isSafeInteger(v.revision)||v.revision<0||!field(v.workbench_revision,'integer')||!field(v.adoption_revision,'integer'))throw fail('BPAY_NEXT_WORKBENCH_REQUEST_INVALID',400);
  const c=body.context,n=v.namespace;
  if(n.agency_id!==c.agency_id||n.owner_epoch!==c.owner_epoch||n.actor_user_id!==actor||n.official_pay_date!==c.official_pay_date||v.workbench_revision!==c.workbench_revision||v.adoption_revision!==c.adoption_revision||n.scope.client_id!==c.scope.client_id||n.scope.channel!==c.scope.channel||n.scope.candidate_id!==null&&n.scope.candidate_id!==c.scope.candidate_id||body.candidate_id!==null&&n.scope.candidate_id!==null&&body.candidate_id!==n.scope.candidate_id||body.sort.startsWith('G_')&&body.candidate_id===null)throw fail('BPAY_NEXT_WORKBENCH_CURSOR_STALE',409,false,true);
  if(!exact(v.intent,['mode','candidates','works','components','cases'])||!['ALL_MATCHING','NONE'].includes(v.intent.mode))throw fail('BPAY_NEXT_WORKBENCH_REQUEST_INVALID',400);
  const schemas={candidates:{candidate_id:'uuid',selected:'boolean'},works:{candidate_id:'uuid',work_id:'uuid',expected_revision_id:'uuid',selected:'boolean',mode:'selectionMode'},components:{candidate_id:'uuid',work_id:'uuid',expected_revision_id:'uuid',approved_line_id:'uuid',selected:'boolean'},cases:{candidate_id:'uuid',case_component_id:'uuid',selected:'boolean'}},maps={};
  for(const [kind,schema]of Object.entries(schemas)){
    const rows=v.intent[kind];if(!Array.isArray(rows)||rows.length>1000)throw fail('BPAY_NEXT_WORKBENCH_REQUEST_INVALID',400);
    maps[kind]=new Map();const supplied=new Set();
    for(const row of rows){if(!exact(row,Object.keys(schema))||!Object.entries(schema).every(([k,t])=>t==='selectionMode'?['ALL','SUBSET'].includes(row[k]):field(row[k],t))||n.scope.candidate_id!==null&&row.candidate_id!==n.scope.candidate_id)throw fail('BPAY_NEXT_WORKBENCH_REQUEST_INVALID',400);const id=row.case_component_id??row.approved_line_id??row.work_id??row.candidate_id;if(supplied.has(id))throw fail('BPAY_NEXT_WORKBENCH_REQUEST_INVALID',400);supplied.add(id);if(body.candidate_id===null||row.candidate_id===body.candidate_id)maps[kind].set(id,row);}
  }
  // Memory overlays may omit a Work parent; exact supplied parents still bind.
  // Legacy non-memory descriptors retain their native SUBSET parent.
  for(const line of maps.components.values()){
    const work=maps.works.get(line.work_id);
    if(!work&&!Object.hasOwn(v,'memory_baseline_run_id')||work&&(!Object.hasOwn(v,'memory_baseline_run_id')&&work.mode!=='SUBSET'||work.candidate_id!==line.candidate_id||work.expected_revision_id!==line.expected_revision_id))throw fail('BPAY_NEXT_WORKBENCH_REQUEST_INVALID',400);
  }
  const frozen=JSON.parse(JSON.stringify(body));
  return {body:frozen,maps,scope:{candidate_id:body.candidate_id??n.scope.candidate_id,client_id:n.scope.client_id,channel:n.scope.channel,section:'ALL',search:'',sort:body.sort,selection_memory:Object.hasOwn(v,'memory_baseline_run_id')?{run_id:v.memory_baseline_run_id,scope:n.scope}:null},seen:Object.fromEntries(Object.keys(maps).map(k=>[k,new Set()]))};
}
function exportIncluded(row,input,caseCandidateQualified=false){
  const {maps,seen}=input,candidate=maps.candidates.get(row.candidate_id),work=row.work_id===null?null:maps.works.get(row.work_id),line=row.approved_line_id===null?null:maps.components.get(row.approved_line_id),component=row.case_component_id===null?null:maps.cases.get(row.case_component_id);
  if(candidate)seen.candidates.add(row.candidate_id);
  for(const [kind,choice,id]of [['works',work,row.work_id],['components',line,row.approved_line_id],['cases',component,row.case_component_id]])if(choice){
    if(choice.candidate_id!==row.candidate_id||choice.work_id!==undefined&&(choice.work_id!==row.work_id||choice.expected_revision_id!==row.revision_id))throw fail('BPAY_NEXT_WORKBENCH_CURSOR_STALE',409,false,true);
    seen[kind].add(id);
  }
  if(!row.choice_eligible||!row.candidate_has_pay||candidate?.selected===false)return false;
  const memory=input.body.choice.memory_baseline_run_id!=null,base=candidate?.selected??(memory?row.default_selected:input.body.choice.intent.mode==='ALL_MATCHING');
  if(row.work_id!==null){if(work?.selected===false||input.invalidSubsets?.has(row.work_id))return false;if(work?.mode==='SUBSET')return line?.selected===true;return line?.selected??work?.selected??base;}
  if(row.case_component_id!==null)return caseCandidateQualified&&(component?.selected??candidate?.selected??(memory?row.default_selected:true));
  return false;
}
function csvCell(value,money=false){let text=value===null?'':String(value);if(!money&&/^[\s]*[=+\-@]/.test(text))text="'"+text;return /[",\r\n]/.test(text)?'"'+text.replaceAll('"','""')+'"':text;}
function exportCsvRow(row,included){
  const date=row.work_date??row.week_ending_date,kind=row.direction==='DEDUCTION'?row.export_label+' balance':row.export_label;
  const values=[included?'Yes':'No',kind,row.member_kind+' / '+row.portion_kind,row.candidate_reference,row.candidate_display_name,row.client_display_name,date===null?'':date.split('-').reverse().join('/'),row.target_pay_channel,row.export_amount,row.physical_timesheet_id,row.member_kind,row.member_key,row.job_title,row.band_label,row.shift_start,row.shift_finish,'',row.detail_label,row.item_id];
  return values.map((v,n)=>csvCell(v,n===8)).join(',')+'\r\n';
}
export async function handleBpayNextWorkbenchExport(request,dependencies={}){
  let controller,deadline,timer,pageTimer,closed=false;
  try{
    if(request.method!=='POST')throw fail('BPAY_NEXT_WORKBENCH_METHOD_NOT_ALLOWED',405);
    const user=await dependencies.requireOfficeUser?.();if(!user||user.role!=='admin'||!field(user.id?.toLowerCase(),'uuid'))throw fail('BPAY_NEXT_WORKBENCH_FORBIDDEN',403);
    const actor=user.id.toLowerCase();controller=new AbortController();
    const bodyDeadline=new Promise((_,reject)=>{timer=setTimeout(()=>{controller.abort();reject(fail('BPAY_NEXT_WORKBENCH_REQUEST_INVALID',400));},BPAY_NEXT_WORKBENCH_LIMITS.timeoutMs);});
    let raw;try{raw=await Promise.race([readWorkbenchJson(request,controller.signal),bodyDeadline]);}finally{clearTimeout(timer);}
    if(ENCODER.encode(JSON.stringify(raw)).byteLength>EXPORT_LIMITS.requestBytes)throw fail('BPAY_NEXT_WORKBENCH_REQUEST_INVALID',400);
    const input=exportInput(raw,actor);if(typeof dependencies.workbenchRpc!=='function')throw fail('BPAY_NEXT_WORKBENCH_UNAVAILABLE',503,true);
    controller=new AbortController();deadline=Date.now()+EXPORT_LIMITS.deadlineMs;
    const stop=()=>{closed=true;controller.abort();clearTimeout(timer);clearTimeout(pageTimer);request.signal?.removeEventListener('abort',stop);};
    request.signal?.addEventListener('abort',stop,{once:true});if(request.signal?.aborted){stop();throw fail('BPAY_NEXT_WORKBENCH_UNAVAILABLE',503,true);}
    let pages=0,rows=0,bytes=0,context=null,after=null,first=true,streamController;
    timer=setTimeout(()=>{stop();streamController?.error(fail('BPAY_NEXT_WORKBENCH_UNAVAILABLE',503,true));},EXPORT_LIMITS.deadlineMs);
    const load=async()=>{
      if(closed||Date.now()>=deadline||++pages>EXPORT_LIMITS.pages)throw fail('BPAY_NEXT_WORKBENCH_UNAVAILABLE',503,true);
      const args=normalizeWorkbenchInput('EXPORT_ROWS',{...input.scope,after,limit:100});
      const task=(async()=>{const response=await dependencies.workbenchRpc('EXPORT_ROWS',args,{actorUserId:actor,signal:controller.signal});if(!(response instanceof Response)||!response.ok)throw fail('BPAY_NEXT_WORKBENCH_UNAVAILABLE',503,true);const payload=await readWorkbenchJson(response,controller.signal),page=validateWorkbenchResponse(payload,'EXPORT_ROWS',args,actor);if(!page.ok)throw fail(page.error_code,refusalStatus(page.error_code),page.retryable,page.reload_required);if(!context){context=page.context;const c=input.body.context;if(!['agency_id','owner_epoch','actor_user_id','official_pay_date','workbench_revision','adoption_revision'].every(k=>context[k]===c[k]))throw fail('BPAY_NEXT_WORKBENCH_CURSOR_STALE',409,false,true);}else if(!same(context,page.context))throw fail('BPAY_NEXT_WORKBENCH_CURSOR_STALE',409,false,true);return page;})();
      const timeout=new Promise((_,reject)=>{pageTimer=setTimeout(()=>{controller.abort();reject(fail('BPAY_NEXT_WORKBENCH_UNAVAILABLE',503,true));},BPAY_NEXT_WORKBENCH_LIMITS.timeoutMs);});
      try{return await Promise.race([task,timeout]);}finally{clearTimeout(pageTimer);}
    };
    // A finite validation walk records only Candidates addressed by bounded
    // exceptions (or selected NONE intents), never every agency row/identity.
    // It also proves every retained override before any CSV can be offered.
    const touched=new Set();for(const map of Object.values(input.maps))for(const choice of map.values())touched.add(choice.candidate_id);
    input.qualifiedCandidates=new Set();const eligibleLines=new Set(),invalidSubsets=new Set();input.invalidSubsets=invalidSubsets;let scanned=0;
    let page=await load();
    while(true){
      for(const row of page.rows){if(++scanned>EXPORT_LIMITS.rows)throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE');
        exportIncluded(row,input);
        const subset=row.work_id===null?null:input.maps.works.get(row.work_id),line=row.approved_line_id===null?null:input.maps.components.get(row.approved_line_id);
        if(subset?.mode==='SUBSET'&&line?.selected===true&&row.choice_eligible&&row.direction==='PAYMENT')eligibleLines.add(row.approved_line_id);
        if(row.direction==='PAYMENT'&&row.choice_eligible&&row.candidate_has_pay&&input.maps.candidates.get(row.candidate_id)?.selected!==false){
          const candidate=input.maps.candidates.get(row.candidate_id),base=candidate?.selected??(input.body.choice.memory_baseline_run_id!=null?row.default_selected:input.body.choice.intent.mode==='ALL_MATCHING');
          const chosenCase=row.case_component_id===null?null:input.maps.cases.get(row.case_component_id);
          const chosenWork=row.work_id===null?null:input.maps.works.get(row.work_id);
          if(touched.has(row.candidate_id)&&exportIncluded(row,input,true)&&(row.work_id!==null&&chosenWork?.mode!=='SUBSET'&&chosenWork?.selected!==false||row.case_component_id!==null&&(chosenCase?.selected??true)&&(base||chosenCase?.selected===true)))input.qualifiedCandidates.add(row.candidate_id);
        }
      }
      if(page.complete)break;after=page.next_cursor;page=await load();
    }
    for(const kind of Object.keys(input.maps))for(const id of input.maps[kind].keys())if(!input.seen[kind].has(id))throw fail('BPAY_NEXT_WORKBENCH_CURSOR_STALE',409,false,true);
    for(const line of input.maps.components.values())if(line.selected===true&&input.maps.works.get(line.work_id)?.mode==='SUBSET'&&!eligibleLines.has(line.approved_line_id))invalidSubsets.add(line.work_id);
    for(const work of input.maps.works.values())if(work.mode==='SUBSET'&&work.selected===true&&input.maps.candidates.get(work.candidate_id)?.selected!==false&&!invalidSubsets.has(work.work_id)&&[...input.maps.components.values()].some(line=>line.work_id===work.work_id&&line.selected===true&&eligibleLines.has(line.approved_line_id)))input.qualifiedCandidates.add(work.candidate_id);
    // All export pages retain that exact first context; no newer-version retry.
    after=null;page=await load();
    const stream=new ReadableStream({start(c){streamController=c;},async pull(c){
      if(closed)return;
      try{
        if(!first)page=await load();
        let csv=first?'\ufeff'+BPAY_NEXT_EXPORT_HEADERS.map(v=>csvCell(v)).join(',')+'\r\n':'';first=false;
        for(const row of page.rows){if(++rows>EXPORT_LIMITS.rows)throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE');
          const qualified=touched.has(row.candidate_id)?input.qualifiedCandidates.has(row.candidate_id):input.body.choice.memory_baseline_run_id!=null?row.candidate_default_pay_selected:input.body.choice.intent.mode==='ALL_MATCHING'&&row.candidate_has_pay;
          csv+=exportCsvRow(row,exportIncluded(row,input,qualified));}
        if(page.complete){for(const kind of Object.keys(input.maps))for(const id of input.maps[kind].keys())if(!input.seen[kind].has(id))throw fail('BPAY_NEXT_WORKBENCH_CURSOR_STALE',409,false,true);}
        const chunk=ENCODER.encode(csv);bytes+=chunk.byteLength;if(bytes>EXPORT_LIMITS.csvBytes)throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE');
        c.enqueue(chunk);after=page.next_cursor;
        if(page.complete){stop();c.close();}
      }catch(e){stop();c.error(e?.code?e:fail('BPAY_NEXT_WORKBENCH_UNAVAILABLE',503,true));}
    },cancel(){stop();}},{highWaterMark:0});
    return new Response(stream,{headers:{'content-type':'text/csv; charset=utf-8','cache-control':'no-store','x-content-type-options':'nosniff'}});
  }catch(e){closed=true;controller?.abort();clearTimeout(timer);clearTimeout(pageTimer);const safe=typeof e?.code==='string'&&FAILURE_CODES.includes(e.code)?e:fail('BPAY_NEXT_WORKBENCH_UNAVAILABLE',503,true);return json(safe.status??503,{ok:false,error_code:safe.code,retryable:safe.retryable===true,reload_required:safe.reload===true,workbench_revision:null,complete:false,projection_state:null,dirty_candidate_count:null,dirty_group_count:null,pending_task_count:null});}
}


// WB19 W2: administrative resume only. Original job inputs never expand.
const WORK_RETRY_ARGS={request_id:'uuid',candidate_id:'uuid',work_id:'uuid',job_id:'uuid',
 module_epoch:'positive',expected_owner_epoch:'positive',expected_phase:'phase',expected_cursor:'checkpoint?'};
const RETRY_REFUSALS=['BPAY_NEXT_WORKBENCH_COMMAND_INPUT_INVALID','BPAY_NEXT_WORKBENCH_COMMAND_REPLAY_CONFLICT',
 'BPAY_NEXT_WORKBENCH_COMMAND_EPOCH_STALE','BPAY_NEXT_WORK_JOB_REPAIR_REQUIRED',
 'BPAY_NEXT_WORK_JOB_STATE_CHANGED','BPAY_NEXT_WORK_JOB_NOT_DEFINITELY_FAILED',
 'BPAY_NEXT_COMMAND_FORBIDDEN','BPAY_NEXT_MODULE_NOT_ACTIVE'];
function retryField(v,type){return type==='phase'?typeof v==='string'&&ENCODER.encode(v).byteLength>=1&&ENCODER.encode(v).byteLength<=64
 :type==='checkpoint?'?v===null||typeof v==='string'&&ENCODER.encode(v).byteLength<=512
 :type==='jobStatus'?['READY','DONE'].includes(v):type==='error?'?v===null||typeof v==='string'&&/^BPAY_NEXT_[A-Z0-9_]{1,240}$/.test(v)
 :type==='timestamp'?typeof v==='string'&&v.length<=40&&Number.isFinite(Date.parse(v)):field(v,type);}
export function validateWorkbenchCommandReceipt(r,args){
 const absent={request_id:'uuid',candidate_id:'uuid',work_id:'uuid',job_id:'uuid',received:'boolean',replay:'boolean'};
 const schema=r?.received===false?absent:{...absent,command_id:'uuid',command_sequence:'positive',module_epoch:'positive',
  owner_epoch:'positive',status:'jobStatus',phase:'phase',cursor:'checkpoint?',first_recorded_error_code:'error?',
  last_error_code:'error?',workbench_revision:'integer',adoption_revision:'integer',repaired:'boolean',captured_at_utc:'timestamp'};
 if(!exact(r,Object.keys(schema))||Object.entries(schema).some(([k,t])=>!retryField(r[k],t))
  ||['request_id','candidate_id','work_id','job_id'].some(k=>r[k]!==args[k])
  ||r.received&&(r.module_epoch!==args.module_epoch||r.owner_epoch!==args.expected_owner_epoch
    ||r.status==='DONE'&&(r.phase!=='DONE'||r.cursor!==null)
    ||r.status==='READY'&&(!['NEW','REMOVED'].includes(r.phase)||!r.repaired&&(r.phase!==args.expected_phase||r.cursor!==args.expected_cursor)))
  ||!r.received&&r.replay)throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
 return r;
}
export function createBpayNextWorkbenchCommandRpc({baseUrl,headers,fetchImpl=fetch}){
 const endpoint=new URL(`${String(baseUrl||'').replace(/\/+$/,'')}/rest/v1/rpc/bpay_next_workbench_command_v1`);
 if(!['http:','https:'].includes(endpoint.protocol)||endpoint.username||endpoint.password||endpoint.search||endpoint.hash)throw new TypeError('BPAY_NEXT_COMMAND_TRANSPORT_INVALID');
 return (args,readback,options)=>{if(!field(options?.actorUserId,'uuid')||!options.signal||typeof readback!=='boolean')throw new TypeError('BPAY_NEXT_COMMAND_TRANSPORT_INVALID');
  const h=new Headers(headers);h.set('content-type','application/json');h.set('accept','application/json');h.set('X-CloudTMS-Banking-Pay-Route-Class',readback?'READ':'OPERATION_NUDGE');
  return fetchImpl(endpoint.href,{method:'POST',headers:h,signal:options.signal,redirect:'error',
   body:JSON.stringify({p_request:{actor_user_id:options.actorUserId,action:'RETRY_WORK_JOB',args,readback}})});};
}
export async function handleBpayNextWorkbenchCommand(request,dependencies={}){
 let timer,controller,attempted=false;const abort=()=>controller?.abort();
 try{
  if(request.method!=='POST')throw fail('BPAY_NEXT_WORKBENCH_METHOD_NOT_ALLOWED',405);
  const user=await dependencies.requireOfficeUser?.(request,['admin']);
  if(!user||!field(user.id?.toLowerCase(),'uuid'))throw fail('BPAY_NEXT_WORKBENCH_UNAUTHORIZED',401);
  if(user.role!=='admin')throw fail('BPAY_NEXT_WORKBENCH_FORBIDDEN',403);
  controller=new AbortController();request.signal?.addEventListener('abort',abort,{once:true});if(request.signal?.aborted)abort();
  const timeout=new Promise((_,reject)=>{timer=setTimeout(()=>{abort();reject(fail('BPAY_NEXT_WORKBENCH_UNAVAILABLE',503,true));},8000);});
  const send=async()=>{
   const body=await readWorkbenchJson(request,controller.signal,2048);
   if(!exact(body,['action','readback',...Object.keys(WORK_RETRY_ARGS)])||body.action!=='RETRY_WORK_JOB'
    ||typeof body.readback!=='boolean'||Object.entries(WORK_RETRY_ARGS).some(([k,t])=>!retryField(body[k],t)))throw fail('BPAY_NEXT_WORKBENCH_COMMAND_INPUT_INVALID',400);
   const args=Object.freeze(Object.fromEntries(Object.keys(WORK_RETRY_ARGS).map(k=>[k,body[k]])));
   if(typeof dependencies.workbenchCommandRpc!=='function')throw fail('BPAY_NEXT_WORKBENCH_UNAVAILABLE',503,true);
   attempted=!body.readback;
   const response=await dependencies.workbenchCommandRpc(args,body.readback,{actorUserId:user.id.toLowerCase(),signal:controller.signal});
   if(!(response instanceof Response))throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
   const r=await readWorkbenchJson(response,controller.signal,16384);
   if(!response.ok){if(record(r)&&RETRY_REFUSALS.includes(r.message)&&['22023','23514','55000','42501'].includes(r.code)){
     attempted=false;throw fail(r.message,r.code==='42501'?403:r.code==='22023'?400:409);}
    throw fail('BPAY_NEXT_WORKBENCH_UNAVAILABLE',503,true);}
   const receipt=validateWorkbenchCommandReceipt(r,args);if(!body.readback&&!receipt.received)throw fail('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
   return json(200,{ok:true,action:'RETRY_WORK_JOB',receipt});
  };
  return await Promise.race([send(),timeout]);
 }catch(e){return json(e?.status??503,{ok:false,error_code:e?.code??'BPAY_NEXT_WORKBENCH_UNAVAILABLE',retryable:e?.retryable===true,outcome_unknown:attempted});}
 finally{clearTimeout(timer);abort();request.signal?.removeEventListener('abort',abort);}
}
export function createBpayNextWorkJobFailureRpc({baseUrl,headers,fetchImpl=fetch}){
 const endpoint=new URL(`${String(baseUrl||'').replace(/\/+$/,'')}/rest/v1/rpc/bpay_next_work_job_failure_service_v1`);
 if(!['http:','https:'].includes(endpoint.protocol)||endpoint.username||endpoint.password||endpoint.search||endpoint.hash)throw new TypeError('BPAY_NEXT_COMMAND_TRANSPORT_INVALID');
 return async evidence=>{
  const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),4000);
  try{const h=new Headers(headers);h.set('content-type','application/json');h.set('accept','application/json');
   h.set('X-CloudTMS-Banking-Pay-Route-Class','OPERATION_NUDGE');
   const r=await fetchImpl(endpoint.href,{method:'POST',headers:h,signal:controller.signal,redirect:'error',body:JSON.stringify({
    p_job_id:evidence.job_id,p_lease_nonce:evidence.lease_nonce,p_owner_epoch:evidence.owner_epoch,
    p_expected_phase:evidence.expected_phase,p_expected_cursor:evidence.expected_cursor,p_error_code:evidence.error_code})});
   const value=await readWorkbenchJson(r,controller.signal,1024);return r.ok&&value===true;
  }finally{clearTimeout(timer);controller.abort();}
 };
}
