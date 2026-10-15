// Private Source consumer for the single native Checklist owner. No route,
// ledger, discrepancy classifier, calculator, payment inference or mutation.
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const HASH = /^[0-9a-f]{64}$/;
const DATE = /^\d{4}-\d{2}-\d{2}$/;
const ROUTES = new Set(['SELF_BILL', 'AGENCY_INVOICE', 'SIGNED_TIMESHEETS', 'NATIVE']);
const TONES = new Set(['positive', 'warning', 'danger', 'muted']);
const VIEWS = new Set(['ADMIN', 'PAY_READONLY']);
const SORTS = new Set(['week_ending', 'client', 'group', 'checked', 'completed', 'worker', 'work_date', 'reason']);
const LINKS = new Set(['UPLOAD_DETAIL', 'FINALISE', 'QUERIES', 'TIMESHEET', 'DAILY_REVIEW', 'UPLOAD', 'INVOICE_HOLD_DECISION']);
const OWN = (o, k) => Object.prototype.hasOwnProperty.call(o, k);
const MAX_BYTES = 200000;

export class ChecklistSourceError extends Error {
  constructor(code, status = 409) { super(code); this.name = 'ChecklistSourceError'; this.code = code; this.status = status; }
}
const fail = (code, status) => { throw new ChecklistSourceError(code, status); };
function object(v) { if (!v || typeof v !== 'object' || Array.isArray(v)) fail('CHECKLIST_SOURCE_CONTRACT_INVALID'); return v; }
function closed(v, keys) { object(v); if (Object.keys(v).some(k => !keys.includes(k))) fail('CHECKLIST_SOURCE_FIELD_NOT_ALLOWED', 400); }
function uuid(v) { if (typeof v !== 'string' || !UUID.test(v)) fail('CHECKLIST_SOURCE_IDENTITY_INVALID'); return v.toLowerCase(); }
function text(v, max = 2000) { if (typeof v !== 'string' || v.length > max) fail('CHECKLIST_SOURCE_CONTRACT_INVALID'); return v; }
function count(v) { if (!Number.isSafeInteger(v) || v < 0) fail('CHECKLIST_SOURCE_CONTRACT_INVALID'); return v; }
function same(a, b) { return typeof a === 'string' && typeof b === 'string' && a.toLowerCase() === b.toLowerCase(); }
function copy(v) { return JSON.parse(JSON.stringify(v)); }
function bindScope(scope) {
  closed(scope, ['source_group_id', 'client_id', 'source_cycle_id', 'week_ending']);
  uuid(scope.source_group_id); uuid(scope.client_id); uuid(scope.source_cycle_id);
  if (typeof scope.week_ending !== 'string' || !DATE.test(scope.week_ending)) fail('CHECKLIST_SOURCE_IDENTITY_INVALID');
  return scope;
}
function sameScope(a, b) {
  return ['source_group_id', 'client_id', 'source_cycle_id'].every(k => same(a[k], b[k])) && a.week_ending === b.week_ending;
}
function status(s) {
  if (!s || typeof s !== 'object' || Array.isArray(s) || !TONES.has(s.tone) || typeof s.text !== 'string') {
    return { text: 'Decision needed', tone: 'warning', subtext: 'Source information needs review', subtone: 'warning' };
  }
  closed(s, ['text', 'tone', 'subtext', 'subtone']); text(s.text); text(s.subtext ?? '');
  if (s.subtone != null && !TONES.has(s.subtone)) fail('CHECKLIST_SOURCE_CONTRACT_INVALID');
  return { text: s.text, tone: s.tone, subtext: s.subtext ?? '', subtone: s.subtone ?? s.tone };
}

/** Preserve native scoped withholding truth. Unknown is null, never fake zero. */
export function decodeChecklistHold(value) {
  closed(value, ['holds_pay', 'holds_pay_scope', 'withheld_amount_pence', 'holds_invoice', 'pending_decision', 'authority']);
  if (![true, false, null].includes(value.holds_pay) || ![true, false, null].includes(value.holds_invoice) ||
      ![true, false, null].includes(value.pending_decision)) fail('CHECKLIST_HOLD_EVIDENCE_INVALID');
  closed(value.authority, ['owner', 'hash', 'withholding_known']);
  if (!['SOURCE', 'SOURCE_QUERY_EX_VAT', 'SOURCE_INVOICE_BLOCK', 'SOURCE_INVOICE_DECISION', 'ORDINARY_NEXT_AVAILABLE_EX_VAT', 'NEXT', 'ORDINARY', 'UNAVAILABLE'].includes(value.authority.owner) ||
      typeof value.authority.withholding_known !== 'boolean') fail('CHECKLIST_HOLD_EVIDENCE_INVALID');
  if (value.authority.owner === 'UNAVAILABLE') {
    if (value.authority.hash !== null || value.authority.withholding_known !== false || value.holds_invoice !== null ||
        value.pending_decision !== null) fail('CHECKLIST_HOLD_EVIDENCE_INVALID');
  } else if (typeof value.authority.hash !== 'string' || !HASH.test(value.authority.hash)) fail('CHECKLIST_HOLD_EVIDENCE_INVALID');
  if (value.holds_pay === null) {
    if (value.withheld_amount_pence !== null || value.authority.withholding_known !== false) fail('CHECKLIST_HOLD_EVIDENCE_INVALID');
  } else {
    const amount = value.withheld_amount_pence;
    if (!value.authority.withholding_known || typeof amount !== 'string' || !/^(?:0|[1-9]\d{0,17})$/.test(amount)) {
      fail('CHECKLIST_WITHHELD_AMOUNT_UNPROVED');
    }
    if (value.holds_pay ? amount === '0' : amount !== '0') fail('CHECKLIST_WITHHELD_AMOUNT_CONTRADICTION');
  }
  if (value.holds_pay === true && !['SHIFT', 'TIMESHEET'].includes(value.holds_pay_scope)) fail('CHECKLIST_HOLD_EVIDENCE_INVALID');
  if (value.holds_pay_scope !== null && !['SHIFT', 'TIMESHEET'].includes(value.holds_pay_scope)) fail('CHECKLIST_HOLD_EVIDENCE_INVALID');
  // This finite authority means genuine positive query-withheld SOURCE EX-VAT
  // at one native WORK segment. It supplies neither invoice/decision facts nor
  // target VAT, payroll net, cash, a whole Timesheet amount or known zero.
  if (value.authority.owner === 'SOURCE_QUERY_EX_VAT' && (value.holds_pay !== true || value.holds_pay_scope !== 'SHIFT' ||
      value.holds_invoice !== null || value.pending_decision !== null || value.authority.withholding_known !== true)) {
    fail('CHECKLIST_SOURCE_QUERY_MONEY_GRAIN_INVALID');
  }
  // Exact native unreleased VAT_FIT admission block only. It certifies no pay
  // quantity, cleared invoice state, Office decision or permitted action.
  if (value.authority.owner === 'SOURCE_INVOICE_BLOCK' && (value.holds_pay !== null || value.holds_pay_scope !== null ||
      value.withheld_amount_pence !== null || value.holds_invoice !== true || value.pending_decision !== null ||
      value.authority.withholding_known !== false)) fail('CHECKLIST_SOURCE_INVOICE_BLOCK_GRAIN_INVALID');
  if (value.authority.owner === 'SOURCE_INVOICE_DECISION' && (value.holds_pay !== null || value.holds_pay_scope !== null ||
      value.withheld_amount_pence !== null || value.holds_invoice !== true || value.pending_decision !== true ||
      value.authority.withholding_known !== false)) fail('CHECKLIST_SOURCE_INVOICE_DECISION_GRAIN_INVALID');
  if (value.authority.owner === 'ORDINARY_NEXT_AVAILABLE_EX_VAT' && (value.holds_pay !== true || value.holds_pay_scope !== 'TIMESHEET' ||
      value.holds_invoice !== null || value.pending_decision !== null || value.authority.withholding_known !== true)) {
    fail('CHECKLIST_ORDINARY_NEXT_MONEY_GRAIN_INVALID');
  }
  return copy(value);
}

export function unavailableChecklistHold() {
  return { holds_pay: null, holds_pay_scope: null, withheld_amount_pence: null, holds_invoice: null, pending_decision: null,
    authority: { owner: 'UNAVAILABLE', hash: null, withholding_known: false } };
}
function link(l, scope, invoiceDecision = null) {
  if (l === null) return null;
  closed(l, ['kind', 'label', 'target', 'native_allowed']);
  if (!LINKS.has(l.kind) || l.native_allowed !== true) fail('CHECKLIST_SOURCE_ACTION_NOT_PERMITTED');
  text(l.label, 250); object(l.target);
  if(l.kind==='INVOICE_HOLD_DECISION') {
    const t=l.target,d=invoiceDecision;
    closed(t,['contract','actor_user_id','permission_view','scope','billing_movement_id','client_manifest_id','upload_row_id',
      'expected_source_row_hash','expected_authority_scope_version','hold_kind','decision_read_proof_hash','choices']);
    uuid(t.actor_user_id);bindScope(t.scope);
    for(const key of ['billing_movement_id','client_manifest_id','upload_row_id'])uuid(t[key]);
    if(!d||l.label!=='Review invoice hold'||t.contract!=='WEEKLY_SOURCE_CHECKLIST_INVOICE_HOLD_DECISION_ACTION_V1'||t.permission_view!=='ADMIN'||
      !same(t.actor_user_id,d.actor_user_id)||!sameScope(t.scope,scope)||!sameScope(t.scope,d.scope)||
      !['billing_movement_id','client_manifest_id','upload_row_id'].every(k=>same(t[k],d[k]))||
      typeof t.expected_source_row_hash!=='string'||!HASH.test(t.expected_source_row_hash)||t.expected_source_row_hash!==d.source_row_hash||
      !Number.isSafeInteger(t.expected_authority_scope_version)||t.expected_authority_scope_version<0||t.expected_authority_scope_version!==d.authority_scope_version||
      t.hold_kind!=='VAT_FIT'||typeof t.decision_read_proof_hash!=='string'||!HASH.test(t.decision_read_proof_hash)||t.decision_read_proof_hash!==d.read_proof_hash||
      !Array.isArray(t.choices)||t.choices.length!==2||t.choices[0]!=='BILL_AT_NET_PLUS_20'||t.choices[1]!=='BILL_AS_FILE') {
      fail('CHECKLIST_INVOICE_DECISION_ACTION_INVALID');
    }
    return copy(l);
  }
  const allowed = {
    UPLOAD_DETAIL: ['upload_id'], FINALISE: ['source_group_id', 'client_id', 'source_cycle_id', 'report_scope_id'],
    QUERIES: ['source_group_id', 'client_id', 'source_cycle_id', 'row_key'], TIMESHEET: ['timesheet_id'],
    DAILY_REVIEW: ['import_id'], UPLOAD: ['source_group_id', 'client_id', 'source_cycle_id']
  }[l.kind];
  closed(l.target, allowed);
  for (const [k, v] of Object.entries(l.target)) {
    if (k.endsWith('_id') && v !== null) uuid(v);
    if (['source_group_id', 'client_id', 'source_cycle_id'].includes(k) && !same(v, scope[k])) fail('CHECKLIST_SOURCE_ACTION_SCOPE_CHANGED');
  }
  const required = l.kind === 'UPLOAD_DETAIL' ? ['upload_id'] : l.kind === 'TIMESHEET' ? ['timesheet_id']
    : l.kind === 'DAILY_REVIEW' ? ['import_id'] : ['source_group_id', 'client_id', 'source_cycle_id'];
  if (required.some(k => !OWN(l.target, k) || l.target[k] == null)) fail('CHECKLIST_SOURCE_ACTION_SCOPE_CHANGED');
  if (l.kind === 'QUERIES' && (typeof l.target.row_key !== 'string' || !l.target.row_key || l.target.row_key.length > 512)) fail('CHECKLIST_SOURCE_ACTION_SCOPE_CHANGED');
  return copy(l);
}
function cell(c, scope) {
  closed(c, ['value', 'status', 'item_count', 'link']);
  if (![true, false, null].includes(c.value)) fail('CHECKLIST_SOURCE_CONTRACT_INVALID');
  count(c.item_count);
  const s = status(c.status);
  // Unknown future status must not preserve a green readiness claim.
  const unknown = s.text === 'Decision needed' && s.tone === 'warning' && c.status?.text !== 'Decision needed';
  return { value: unknown ? null : c.value, status: s, item_count: c.item_count, link: link(c.link ?? null, scope) };
}
function identity(record) { text(record.item_id ?? record.row_id, 512); bindScope(record.scope); }

/** Service-owned evidence already contains native labels and admissible subjects. */
export function projectChecklistItem(item, actor) {
  closed(item, ['item_id', 'item_kind', 'scope', 'route', 'worker', 'work_date', 'sheet_scope', 'reason_code', 'status', 'hold', 'subject',
    'permitted_pay_subject', 'action', 'counted_timesheet_id', 'correction_binding', 'source_information_witness', 'submission_read', 'native_source_witness', 'queries_parent_witness', 'native_invoice_witness', 'native_invoice_decision_witness', 'native_ordinary_money_witness']);
  identity(item); text(item.worker, 250); text(item.reason_code, 200);
  if (typeof item.work_date !== 'string' || !DATE.test(item.work_date) || !['WEEKLY', 'DAILY'].includes(item.sheet_scope) ||
      !['PAY', 'PAY_AND_INVOICE', 'INVOICE_ONLY', 'INFORMATION'].includes(item.subject) ||
      !ROUTES.has(item.route) || !['ACTUAL_TIMESHEET', 'SOURCE_INFORMATION', 'NATIVE_SOURCE', 'NATIVE_INVOICE', 'NATIVE_ORDINARY_FINANCIAL'].includes(item.item_kind) ||
      typeof item.permitted_pay_subject !== 'boolean') fail('CHECKLIST_SOURCE_CONTRACT_INVALID');
  if (item.item_kind === 'SOURCE_INFORMATION') {
    const witness = item.source_information_witness;
    closed(witness, ['native_action_id', 'evidence_fingerprint', 'import_id', 'hr_row_id', 'existing_row_id', 'counts_for_row_admission']);
    text(witness.native_action_id, 512); uuid(witness.import_id); uuid(witness.hr_row_id); text(witness.existing_row_id, 512);
    if (!witness.native_action_id || !witness.existing_row_id || typeof witness.evidence_fingerprint !== 'string' || !HASH.test(witness.evidence_fingerprint) ||
        witness.counts_for_row_admission !== false || item.item_id !== `native-action:${witness.native_action_id}` ||
        item.counted_timesheet_id !== null || item.correction_binding !== null ||
        item.subject !== 'INFORMATION' || item.permitted_pay_subject !== false) fail('CHECKLIST_SOURCE_INFORMATION_TARGET_FORBIDDEN');
    // R-254 is a native informational row inside an already admitted week.
    // Its one permitted action stays at the exact existing Queries identity.
    if (item.action === undefined || (item.action !== null && (!['WEEKLY_TIMESHEET_NOT_SUBMITTED', 'DAILY_TIMESHEET_NOT_SUBMITTED'].includes(item.reason_code) ||
        item.action.kind !== 'QUERIES' || item.action.target?.row_key !== witness.existing_row_id))) {
      fail('CHECKLIST_SOURCE_INFORMATION_TARGET_FORBIDDEN');
    }
  } else if (item.item_kind === 'NATIVE_SOURCE') {
    const witness = item.native_source_witness;
    closed(witness, ['owner', 'projection_publication_id', 'work_event_id', 'upload_row_id', 'row_resolution_id',
      'incident_id', 'comparison_revision_id', 'manual_review_id', 'native_row_key', 'parent_group_key', 'evidence_fingerprint']);
    if (witness.owner !== 'private.weekly_source_office_query_groups_v1' ||
        typeof witness.evidence_fingerprint !== 'string' || !HASH.test(witness.evidence_fingerprint) || !['SELF_BILL', 'AGENCY_INVOICE', 'NATIVE'].includes(item.route) ||
        item.counted_timesheet_id !== null || item.correction_binding !== null || item.submission_read != null ||
        item.source_information_witness != null || item.action === undefined || item.action?.kind === 'TIMESHEET') fail('CHECKLIST_NATIVE_SOURCE_TARGET_INVALID');
    uuid(witness.projection_publication_id); uuid(witness.work_event_id);
    text(witness.parent_group_key, 512);
    if (!/^qg_[0-9a-f]{64}$/.test(witness.parent_group_key) || witness.parent_group_key === witness.native_row_key) fail('CHECKLIST_NATIVE_SOURCE_TARGET_INVALID');
    for (const key of ['upload_row_id', 'row_resolution_id', 'incident_id', 'comparison_revision_id', 'manual_review_id']) {
      if (witness[key] !== null) uuid(witness[key]);
    }
    const kind = witness.incident_id !== null ? 'incident' : witness.manual_review_id !== null ? 'manual' : 'missing';
    const id = kind === 'incident' ? witness.incident_id : kind === 'manual' ? witness.manual_review_id : witness.upload_row_id;
    if (!id || witness.native_row_key !== `${kind}-${id}` || item.item_id !== `native-source:${witness.native_row_key}` ||
        (kind === 'incident' && (witness.comparison_revision_id === null || witness.manual_review_id !== null)) ||
        (kind === 'manual' && (witness.upload_row_id === null || witness.comparison_revision_id !== null)) ||
        (kind === 'missing' && witness.row_resolution_id === null) ||
        (item.action !== null && (item.action.kind !== 'QUERIES' || item.action.target?.row_key !== witness.parent_group_key))) {
      fail('CHECKLIST_NATIVE_SOURCE_TARGET_INVALID');
    }
  } else if (item.item_kind === 'NATIVE_INVOICE') {
    const witness=item.native_invoice_witness;
    closed(witness,['owner','actor_user_id','scope','final_revision_id','client_manifest_id','billing_movement_id',
      'upload_row_id','work_event_id','manifest_hash','movement_economic_hash','read_proof_hash']);
    uuid(witness.actor_user_id);bindScope(witness.scope);
    for(const key of ['final_revision_id','client_manifest_id','billing_movement_id','upload_row_id'])uuid(witness[key]);
    if(witness.work_event_id!==null)uuid(witness.work_event_id);
    for(const key of ['manifest_hash','movement_economic_hash','read_proof_hash']) {
      if(typeof witness[key]!=='string'||!HASH.test(witness[key]))fail('CHECKLIST_NATIVE_INVOICE_TARGET_INVALID');
    }
    if(witness.owner!=='private.weekly_source_invoice_unreleased_vat_fit_movements_v1'||
      !same(witness.actor_user_id,actor.actor_user_id)||!sameScope(witness.scope,item.scope)||
      item.item_id!==`native-invoice:${witness.billing_movement_id}`||item.route!=='SELF_BILL'||
      item.reason_code!=='VAT_FIT'||item.subject!=='INVOICE_ONLY'||item.permitted_pay_subject!==false||
      item.status?.text!=='Holds invoice'||item.status?.tone!=='danger'||
      item.counted_timesheet_id!==null||item.correction_binding!==null||item.submission_read!==null||
      item.source_information_witness!==null||item.native_source_witness!==null||item.queries_parent_witness!=null||item.action===undefined||
      (item.action!==null&&(item.action.kind!=='INVOICE_HOLD_DECISION'||item.native_invoice_decision_witness==null))) {
      fail('CHECKLIST_NATIVE_INVOICE_TARGET_INVALID');
    }
    if (item.native_invoice_decision_witness != null) {
      const decision=item.native_invoice_decision_witness;
      closed(decision,['owner','actor_user_id','scope','billing_movement_id','client_manifest_id','upload_row_id',
        'invoice_block_read_proof_hash','source_row_hash','authority_scope_version','hold_kind','qualification_choice','read_proof_hash']);
      uuid(decision.actor_user_id);bindScope(decision.scope);
      for(const key of ['billing_movement_id','client_manifest_id','upload_row_id'])uuid(decision[key]);
      for(const key of ['invoice_block_read_proof_hash','source_row_hash','read_proof_hash']) {
        if(typeof decision[key]!=='string'||!HASH.test(decision[key]))fail('CHECKLIST_NATIVE_INVOICE_DECISION_INVALID');
      }
      if(decision.owner!=='private.weekly_source_invoice_hold_release_context_core_v1'||
        !same(decision.actor_user_id,actor.actor_user_id)||!sameScope(decision.scope,item.scope)||
        !['billing_movement_id','client_manifest_id','upload_row_id'].every(k=>same(decision[k],witness[k]))||
        decision.invoice_block_read_proof_hash!==witness.read_proof_hash||
        !Number.isSafeInteger(decision.authority_scope_version)||decision.authority_scope_version<0||
        decision.hold_kind!=='VAT_FIT'||decision.qualification_choice!=='BILL_AT_NET_PLUS_20'||
        item.hold?.authority?.owner!=='SOURCE_INVOICE_DECISION'||item.hold.authority.hash!==decision.read_proof_hash) {
        fail('CHECKLIST_NATIVE_INVOICE_DECISION_INVALID');
      }
    }
  } else if (item.item_kind === 'NATIVE_ORDINARY_FINANCIAL') {
    const witness=item.native_ordinary_money_witness;
    closed(witness,['owner','actor_user_id','scope','timesheet_id','financial_id','work_id','revision_id',
      'amount_basis','grain','daily_work_date','counts_for_row_admission','read_proof_hash']);
    uuid(witness.actor_user_id);bindScope(witness.scope);
    for(const key of ['timesheet_id','financial_id','work_id','revision_id'])uuid(witness[key]);uuid(item.counted_timesheet_id);
    if(witness.owner!=='private.bpay_next_workbench_refresh_work_core_v1'||!same(witness.actor_user_id,actor.actor_user_id)||
      !sameScope(witness.scope,item.scope)||!same(witness.timesheet_id,item.counted_timesheet_id)||
      item.item_id!=='native-financial:'+witness.work_id||witness.amount_basis!=='SOURCE_EX_VAT'||
      witness.grain!=='CURRENT_ORDINARY_WORK_AVAILABLE'||witness.counts_for_row_admission!==false||
      typeof witness.read_proof_hash!=='string'||!HASH.test(witness.read_proof_hash)||
      !['SIGNED_TIMESHEETS','AGENCY_INVOICE'].includes(item.route)||!['WEEKLY','DAILY'].includes(item.sheet_scope)||
      (item.sheet_scope==='DAILY'&&(typeof witness.daily_work_date!=='string'||!DATE.test(witness.daily_work_date)||witness.daily_work_date!==item.work_date))||
      (item.sheet_scope==='WEEKLY'&&witness.daily_work_date!=null)||
      item.subject!=='PAY'||item.permitted_pay_subject!==true||item.reason_code!=='PAY_ON_HOLD'||
      item.status?.text!=='Holds pay'||item.status?.tone!=='danger'||item.correction_binding!==null||item.submission_read!==null||
      item.source_information_witness!==null||item.native_source_witness!=null||item.native_invoice_witness!=null||
      item.native_invoice_decision_witness!=null||item.queries_parent_witness!=null||item.action===undefined||
      (item.action!==null&&(item.action.kind!=='TIMESHEET'||!same(item.action.target?.timesheet_id,witness.timesheet_id)))) {
      fail('CHECKLIST_NATIVE_ORDINARY_MONEY_INVALID');
    }
  } else {
    uuid(item.counted_timesheet_id);
    if (item.source_information_witness != null) fail('CHECKLIST_SOURCE_CONTRACT_INVALID');
  }
  if (item.item_kind !== 'NATIVE_SOURCE' && item.native_source_witness != null) fail('CHECKLIST_SOURCE_CONTRACT_INVALID');
  if (item.item_kind !== 'NATIVE_INVOICE' && (item.native_invoice_witness != null || item.native_invoice_decision_witness != null)) fail('CHECKLIST_SOURCE_CONTRACT_INVALID');
  if (item.item_kind !== 'NATIVE_ORDINARY_FINANCIAL' && item.native_ordinary_money_witness != null) fail('CHECKLIST_SOURCE_CONTRACT_INVALID');
  if(item.action?.kind==='INVOICE_HOLD_DECISION'&&item.item_kind!=='NATIVE_INVOICE')fail('CHECKLIST_INVOICE_DECISION_ACTION_INVALID');
  const action = link(item.action ?? null, item.scope,item.item_kind==='NATIVE_INVOICE'?item.native_invoice_decision_witness:null);
  if (item.queries_parent_witness != null) {
    const parent=item.queries_parent_witness;
    closed(parent,['item_id','parent_group_key','owner','actor_user_id','permission_view','import_id',
      'preview_generation','state_version','preview_fingerprint','proof_hash']);
    uuid(parent.actor_user_id);uuid(parent.import_id);
    if(parent.owner!=='private.weekly_source_check_only_query_groups_v1'||parent.item_id!==item.item_id||
      typeof parent.parent_group_key!=='string'||!/^d10g_[0-9a-f]{64}$/.test(parent.parent_group_key)||!same(parent.actor_user_id,actor.actor_user_id)||
      parent.permission_view!==actor.permission_view||typeof parent.preview_fingerprint!=='string'||!HASH.test(parent.preview_fingerprint)||
      typeof parent.proof_hash!=='string'||!HASH.test(parent.proof_hash)||
      !Number.isSafeInteger(parent.preview_generation)||parent.preview_generation<0||!Number.isSafeInteger(parent.state_version)||parent.state_version<0||
      item.item_kind==='NATIVE_SOURCE'||item.action?.kind!=='QUERIES'||item.action.target?.row_key!==parent.parent_group_key) {
      fail('CHECKLIST_CHECK_ONLY_QUERY_PARENT_INVALID');
    }
    if(item.item_kind==='SOURCE_INFORMATION'&&(item.source_information_witness.existing_row_id!==parent.parent_group_key||
      !same(item.source_information_witness.import_id,parent.import_id)))fail('CHECKLIST_CHECK_ONLY_QUERY_PARENT_INVALID');
  }
  if(item.item_kind!=='NATIVE_SOURCE'&&item.action?.kind==='QUERIES'&&item.queries_parent_witness==null) {
    fail('CHECKLIST_CHECK_ONLY_QUERY_PARENT_REQUIRED');
  }
  if (item.submission_read != null) {
    const submitted=item.submission_read;
    closed(submitted, ['state', 'timesheet_id', 'client_id', 'candidate_id', 'work_date', 'workflow_id',
      'workflow_generation', 'submission_sha256', 'actions_held']);
    uuid(submitted.timesheet_id);uuid(submitted.client_id);uuid(submitted.candidate_id);uuid(submitted.workflow_id);
    if (submitted.state !== 'SUBMITTED_PENDING_NATIVE_MATERIALISATION' || submitted.actions_held !== true ||
        item.item_kind !== 'ACTUAL_TIMESHEET' || item.route !== 'SIGNED_TIMESHEETS' || item.sheet_scope !== 'DAILY' ||
        !same(submitted.timesheet_id,item.counted_timesheet_id) || !same(submitted.client_id,item.scope.client_id) ||
        submitted.work_date !== item.work_date || !Number.isSafeInteger(submitted.workflow_generation) || submitted.workflow_generation < 1 ||
        typeof submitted.submission_sha256 !== 'string' || !HASH.test(submitted.submission_sha256) || item.action !== null ||
        item.status?.tone === 'positive') {
      fail('CHECKLIST_PENDING_SUBMISSION_ACTION_UNAVAILABLE');
    }
  }
  const hold = decodeChecklistHold(item.hold), s = status(item.status), correction = item.correction_binding;
  if (hold.authority.owner === 'SOURCE_QUERY_EX_VAT' && (item.item_kind !== 'NATIVE_SOURCE' ||
      item.subject !== 'PAY' || item.permitted_pay_subject !== true)) fail('CHECKLIST_SOURCE_QUERY_MONEY_GRAIN_INVALID');
  const invoiceOwner=['SOURCE_INVOICE_BLOCK','SOURCE_INVOICE_DECISION'].includes(hold.authority.owner);
  if (invoiceOwner !== (item.item_kind === 'NATIVE_INVOICE') ||
      (hold.authority.owner === 'SOURCE_INVOICE_BLOCK' && hold.authority.hash !== item.native_invoice_witness.read_proof_hash)) {
    fail('CHECKLIST_SOURCE_INVOICE_BLOCK_GRAIN_INVALID');
  }
  if (hold.authority.owner === 'SOURCE_INVOICE_DECISION' && item.native_invoice_decision_witness == null) {
    fail('CHECKLIST_NATIVE_INVOICE_DECISION_INVALID');
  }
  if ((hold.authority.owner === 'ORDINARY_NEXT_AVAILABLE_EX_VAT') !== (item.item_kind === 'NATIVE_ORDINARY_FINANCIAL') ||
      (item.item_kind === 'NATIVE_ORDINARY_FINANCIAL' && hold.authority.hash !== item.native_ordinary_money_witness.read_proof_hash)) {
    fail('CHECKLIST_ORDINARY_NEXT_MONEY_GRAIN_INVALID');
  }
  if (correction != null) {
    closed(correction, ['request_id', 'state', 'state_revision', 'original_timesheet_id', 'replacement_timesheet_id', 'reversal_timesheet_id']);
    uuid(correction.request_id); uuid(correction.original_timesheet_id); uuid(correction.replacement_timesheet_id);
    if (!['WAITING', 'APPLIED', 'CANCELLED'].includes(correction.state) || !Number.isSafeInteger(correction.state_revision) || correction.state_revision < 1) fail('CHECKLIST_SOURCE_CONTRACT_INVALID');
    if (correction.reversal_timesheet_id !== null) uuid(correction.reversal_timesheet_id);
    const expected = correction.state === 'APPLIED' ? correction.replacement_timesheet_id : correction.original_timesheet_id;
    if (!same(item.counted_timesheet_id, expected) || same(item.counted_timesheet_id, correction.reversal_timesheet_id)) fail('CHECKLIST_CORRECTION_MEMBER_SELECTION_INVALID');
  }
  if (item.route === 'SIGNED_TIMESHEETS' && action?.kind === 'FINALISE') fail('CHECKLIST_SIGNED_FINALISE_NOT_ALLOWED');
  if (action?.kind === 'TIMESHEET' && !same(action.target.timesheet_id,
      correction?.state === 'WAITING' ? correction.replacement_timesheet_id : item.counted_timesheet_id)) {
    fail('CHECKLIST_CORRECTION_ACTION_TARGET_INVALID');
  }
  if (correction?.state === 'WAITING' && /reminder/i.test(action?.label ?? '')) fail('CHECKLIST_CORRECTION_REMINDER_NOT_ALLOWED');
  if (actor.permission_view === 'PAY_READONLY' && (!item.permitted_pay_subject || item.subject === 'INVOICE_ONLY')) return null;
  return { ...copy(item), status: hold.holds_pay === null && item.item_kind !== 'NATIVE_INVOICE' ? { ...s, text: 'Decision needed', tone: 'warning', subtone: 'warning' } : s,
    hold, action: actor.permission_view === 'PAY_READONLY' ? null : action };
}

export function projectChecklistRow(row, actor) {
  closed(row, ['row_id', 'scope', 'client', 'group', 'route', 'retention', 'checked', 'completed', 'pay_completed',
    'hold', 'permitted_pay_item_count', 'admin_actions']);
  identity(row); text(row.client, 250); text(row.group, 250);
  if (!ROUTES.has(row.route)) fail('CHECKLIST_SOURCE_ROUTE_UNSUPPORTED');
  closed(row.retention, ['actual_timesheet_count', 'most_recent_ended_week', 'has_open_items', 'earlier_open_weeks']);
  count(row.retention.actual_timesheet_count); count(row.retention.earlier_open_weeks);
  if (typeof row.retention.has_open_items !== 'boolean' || typeof row.retention.most_recent_ended_week !== 'string' || !DATE.test(row.retention.most_recent_ended_week)) fail('CHECKLIST_SOURCE_CONTRACT_INVALID');
  const checked = cell(row.checked, row.scope), completed = cell(row.completed, row.scope), pay = cell(row.pay_completed, row.scope);
  const hold = decodeChecklistHold(row.hold); count(row.permitted_pay_item_count);
  if (['SOURCE_QUERY_EX_VAT','ORDINARY_NEXT_AVAILABLE_EX_VAT'].includes(hold.authority.owner)) fail('CHECKLIST_SOURCE_QUERY_MONEY_GRAIN_INVALID');
  if (['SOURCE_INVOICE_BLOCK','SOURCE_INVOICE_DECISION'].includes(hold.authority.owner)) fail('CHECKLIST_SOURCE_INVOICE_BLOCK_GRAIN_INVALID');
  if (!Array.isArray(row.admin_actions) || row.admin_actions.length > 2) fail('CHECKLIST_SOURCE_CONTRACT_INVALID');
  const admin = row.admin_actions.map(a => {
    closed(a, ['kind', 'label', 'native_allowed', 'payload']);
    if (!['NO_ONE_WORKED', 'UNDO_NO_ONE_WORKED'].includes(a.kind) || a.native_allowed !== true) fail('CHECKLIST_SOURCE_ACTION_NOT_PERMITTED');
    text(a.label, 250);
    closed(a.payload, ['source_cycle_id', 'source_group_id', 'client_id', 'expected_cycle_version', 'attestation_text', 'completion_id']);
    for (const k of ['source_group_id', 'client_id', 'source_cycle_id']) {
      if (!same(a.payload[k], row.scope[k])) fail('CHECKLIST_SOURCE_ACTION_SCOPE_CHANGED');
    }
    if (!Number.isSafeInteger(a.payload.expected_cycle_version) || a.payload.expected_cycle_version < 0) fail('CHECKLIST_SOURCE_ACTION_SCOPE_CHANGED');
    if (a.payload.attestation_text != null) text(a.payload.attestation_text, 500);
    if (a.payload.completion_id != null) uuid(a.payload.completion_id);
    return copy(a);
  });
  if (row.route === 'SIGNED_TIMESHEETS') {
    if (row.retention.actual_timesheet_count === 0) return null;
    if (row.scope.week_ending !== row.retention.most_recent_ended_week && !row.retention.has_open_items) return null;
    if ([checked, completed, pay].some(c => c.link?.kind === 'FINALISE')) fail('CHECKLIST_SIGNED_FINALISE_NOT_ALLOWED');
  }
  if (actor.permission_view === 'PAY_READONLY' && row.permitted_pay_item_count === 0) return null;
  return { ...copy(row), checked: actor.permission_view === 'PAY_READONLY'
    ? { value: null, status: { text: '—', tone: 'muted', subtext: '', subtone: 'muted' }, item_count: 0, link: null } : checked,
    completed: actor.permission_view === 'PAY_READONLY' ? { ...pay, link: null } : completed,
    pay_completed: { ...pay, link: actor.permission_view === 'PAY_READONLY' ? null : pay.link },
    admin_actions: actor.permission_view === 'PAY_READONLY' ? [] : admin, hold };
}

/**
 * Root callbacks compose the existing native Checklist owner and private Source
 * facts before pagination; this module verifies and presents that same snapshot.
 */
export function createHealthRosterChecklistSourceReader(deps) {
  if (typeof deps?.assertActor !== 'function' || typeof deps?.readNativePage !== 'function' ||
      typeof deps?.loadAuthoritativeEvidence !== 'function') fail('CHECKLIST_NATIVE_SOURCE_OWNER_UNAVAILABLE', 503);
  async function actorFor(context) {
    const actor = object(await deps.assertActor(context));
    uuid(actor.actor_user_id);
    if (!VIEWS.has(actor.permission_view) || actor.active !== true || actor.ok === false) fail('CHECKLIST_SOURCE_ACTOR_NOT_ALLOWED', 403);
    return actor;
  }
  return Object.freeze({ async read(payload, context) {
    closed(payload, ['command', 'source_group_id', 'client_id', 'source_cycle_id', 'week_ending', 'cursor', 'sort', 'direction', 'limit', 'anchor']);
    if (!['CHECKLIST', 'CHECKLIST_ISSUES'].includes(payload.command) || !SORTS.has(payload.sort ?? 'week_ending') ||
        !['ASC', 'DESC'].includes(payload.direction ?? 'DESC') || !Number.isInteger(payload.limit ?? 50) ||
        (payload.limit ?? 50) < 1 || (payload.limit ?? 50) > 100) fail('CHECKLIST_SOURCE_REQUEST_INVALID', 400);
    for (const k of ['source_group_id', 'client_id', 'source_cycle_id']) if (payload[k] != null) uuid(payload[k]);
    if (payload.week_ending != null && (typeof payload.week_ending !== 'string' || !DATE.test(payload.week_ending))) fail('CHECKLIST_SOURCE_REQUEST_INVALID', 400);
    for (const k of ['cursor', 'anchor']) if (payload[k] != null) text(payload[k], 4096);
    if (payload.command === 'CHECKLIST_ISSUES') bindScope(Object.fromEntries(
      ['source_group_id', 'client_id', 'source_cycle_id', 'week_ending'].map(k => [k, payload[k]])));
    const actor = await actorFor(context);
    const request = { ...copy(payload), actor_user_id: actor.actor_user_id, permission_view: actor.permission_view };
    const native = await deps.readNativePage(request);
    if (!native || native.ok !== true) fail(native?.code ?? 'CHECKLIST_NATIVE_SOURCE_READ_FAILED');
    const frame = object(await deps.loadAuthoritativeEvidence({ native, request, actor }));
    closed(frame, ['contract', 'actor_user_id', 'permission_view', 'binding', 'read_revision', 'rows', 'items',
      'has_more', 'next_cursor', 'refresh_required', 'anchor', 'native_preservation']);
    if (frame.contract !== 'WEEKLY_SOURCE_CHECKLIST_SOURCE_FRAME_V1' || !same(frame.actor_user_id, actor.actor_user_id) ||
        frame.permission_view !== actor.permission_view || typeof frame.binding !== 'string' || !HASH.test(frame.binding) ||
        typeof frame.read_revision !== 'string' || !HASH.test(frame.read_revision) ||
        typeof frame.has_more !== 'boolean' || typeof frame.refresh_required !== 'boolean' ||
        typeof frame.native_preservation !== 'boolean' ||
        (frame.anchor !== null && (typeof frame.anchor !== 'string' || frame.anchor.length > 4096)) ||
        !Array.isArray(frame.rows) || !Array.isArray(frame.items) || frame.rows.length > (payload.limit ?? 50) ||
        frame.items.length > (payload.limit ?? 50) || new TextEncoder().encode(JSON.stringify(frame)).length > MAX_BYTES ||
        (frame.has_more ? typeof frame.next_cursor !== 'string' || !frame.next_cursor || frame.next_cursor.length > 4096 : frame.next_cursor !== null)) {
      fail('CHECKLIST_SOURCE_CONTRACT_INVALID');
    }
    if (frame.refresh_required) fail('CHECKLIST_ANCHORED_REFRESH_REQUIRED');
    if (frame.native_preservation === true) return native; // Exact NHSP/nonpreset native owner payload, not reclassified here.
    if ((payload.command === 'CHECKLIST' && frame.items.length) || (payload.command === 'CHECKLIST_ISSUES' && frame.rows.length)) {
      fail('CHECKLIST_SOURCE_CONTRACT_INVALID');
    }
    const rows = frame.rows.map(r => projectChecklistRow(r, actor)).filter(Boolean);
    const items = frame.items.map(i => projectChecklistItem(i, actor)).filter(Boolean);
    if (new Set(rows.map(r => r.row_id)).size !== rows.length || new Set(items.map(i => i.item_id)).size !== items.length) fail('CHECKLIST_SOURCE_IDENTITY_DUPLICATED');
    // Native selection applies view/retention filters before cursor slicing.
    // A filtered row here means that contract was violated; never publish a
    // misleading empty/short page or false end after client-side filtering.
    if (rows.length !== frame.rows.length || items.length !== frame.items.length) fail('CHECKLIST_NATIVE_VIEW_FILTER_NOT_APPLIED');
    if ([...rows,...items].some(record => ['source_group_id','client_id','source_cycle_id'].some(k =>
      payload[k] != null && !same(record.scope[k],payload[k])) ||
      (payload.week_ending != null && record.scope.week_ending !== payload.week_ending))) fail('CHECKLIST_SOURCE_SCOPE_CHANGED');
    if (payload.command === 'CHECKLIST_ISSUES' && items.some(i => !sameScope(i.scope, payload))) fail('CHECKLIST_SOURCE_SCOPE_CHANGED');
    return { ...copy(frame), rows, items };
  }, async readTimesheetInformation(payload, context) {
    closed(payload, ['timesheet_id']); uuid(payload.timesheet_id);
    const actor = await actorFor(context);
    if (typeof deps.readNativeTimesheetPresentation !== 'function' || typeof deps.applyNextPaidEvidence !== 'function') {
      fail('CHECKLIST_NATIVE_TIMESHEET_READ_OWNER_UNAVAILABLE', 503);
    }
    const source = object(await deps.readNativeTimesheetPresentation({ timesheetId: payload.timesheet_id, actor }));
    if (source.ok !== true || !same(source.requested_timesheet_id, payload.timesheet_id) ||
        !same(source.actor_user_id, actor.actor_user_id)) fail('CHECKLIST_SOURCE_IDENTITY_INVALID');
    uuid(source.root_timesheet_id);
    object(source.presentation);
    // Exactly the existing V3 consumer decides informational availability.
    // This module never turns authorised/processing/current/WAIT into paid.
    return deps.applyNextPaidEvidence(source.presentation, actor.actor_user_id, source.root_timesheet_id);
  } });
}

/** Local presentation cache only, using the existing modal/session owner. */
export function createChecklistSession({ compareRecords }) {
  if (typeof compareRecords !== 'function') fail('CHECKLIST_NATIVE_ORDER_OWNER_UNAVAILABLE');
  const scopes = new Map(); let disposed = false;
  function state(key) {
    if (disposed) fail('CHECKLIST_SESSION_DISPOSED'); text(key, 4096);
    if (!scopes.has(key)) scopes.set(key, { records: new Map(), order: [], binding: null, revision: null, cursor: null,
      hasMore: true, anchor: null, scrollTop: 0, observer: null, expanded: new Set() });
    return scopes.get(key);
  }
  return Object.freeze({
    apply(key, page, { anchoredRefresh = false, removedIds = [] } = {}) {
      const s = state(key), records = page.rows.length ? page.rows : page.items;
      if (s.binding !== null && s.binding !== page.binding) fail('CHECKLIST_SESSION_SCOPE_CHANGED');
      if (s.revision !== null && s.revision !== page.read_revision && !anchoredRefresh) fail('CHECKLIST_ANCHORED_REFRESH_REQUIRED');
      const before = [...s.order], oldAnchor = s.anchor;
      for (const id of removedIds) { s.records.delete(id); s.expanded.delete(id); }
      for (const record of records) s.records.set(record.row_id ?? record.item_id, copy(record));
      const liveOld = before.filter(id => s.records.has(id));
      const incoming = records.map(r => r.row_id ?? r.item_id);
      // Anchored refresh supplies the actual affected range in native order.
      // Keep other loaded identities; root supplies removals and range identity.
      s.order = [...new Set([...liveOld, ...incoming])].sort((a, b) => compareRecords(s.records.get(a), s.records.get(b)));
      s.binding = page.binding; s.revision = page.read_revision; s.cursor = page.next_cursor; s.hasMore = page.has_more;
      if (oldAnchor && !s.records.has(oldAnchor)) {
        const index = before.indexOf(oldAnchor), neighbours = [...before.slice(index + 1), ...before.slice(0, index).reverse()];
        s.anchor = neighbours.find(id => s.records.has(id)) ?? incoming[0] ?? null;
      }
      return { records: s.order.map(id => copy(s.records.get(id))), anchor: s.anchor, scrollTop: s.scrollTop, hasMore: s.hasMore, cursor: s.cursor };
    },
    remember(key, anchor, scrollTop) { const s = state(key); if (anchor != null) text(anchor, 512); if (!Number.isFinite(scrollTop) || scrollTop < 0) fail('CHECKLIST_SOURCE_CONTRACT_INVALID'); s.anchor = anchor; s.scrollTop = scrollTop; },
    expand(key, rowId) { state(key).expanded.add(rowId); },
    collapse(key, rowId, expansionKey) { state(key).expanded.delete(rowId); if (scopes.has(expansionKey)) { const e = state(expansionKey); e.observer?.disconnect(); e.observer = null; } },
    attachObserver(key, observer) { const s = state(key); if (typeof observer?.disconnect !== 'function') fail('CHECKLIST_SOURCE_CONTRACT_INVALID'); s.observer?.disconnect(); s.observer = observer; },
    isLive(key) { return !disposed && scopes.has(key); },
    dispose() { for (const s of scopes.values()) s.observer?.disconnect(); scopes.clear(); disposed = true; }
  });
}
