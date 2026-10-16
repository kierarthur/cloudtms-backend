import { sourceNativeClosed, sourceNativePositiveBigint } from './source-native-contract.mjs';

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const HASH = /^[0-9a-f]{64}$/;
const isUuid = (value) => typeof value === 'string' && UUID.test(value);
const isHash = (value) => typeof value === 'string' && HASH.test(value);
const token = (value) => typeof value === 'string' && value.length > 0 && value.length <= 512;
const decisions = ['EXCLUDE_FUTURE_WORK_PAY', 'ACCEPT_FINAL_SOURCE_PAY'];
const previewKeys = ['version', 'decision_id', 'request_id', 'decision', 'selection'];
const acceptKeys = ['version', 'action', 'decision_id', 'request_id', 'review_id', 'review_digest', 'decision',
  'supersedes_decision_id', 'expected_prior_dispositions', 'expected_prior_dispositions_sha256',
  'invoice_policy', 'contact_policy', 'reason', 'evidence_refs', 'expected_pay_cut_hash', 'expected_source_invoice_hash', 'expected_issue_set_hash'];
const resumeKeys = ['version', 'action', 'decision_id', 'request_id', 'expected_state_revision', 'review_id',
  'review_digest', 'expected_pay_cut_hash', 'evidence_refs', 'expected_prior_dispositions', 'expected_prior_dispositions_sha256'];
function fail() {
  const error = new Error('The Source pay decision request is invalid.');
  error.code = 'SOURCE_PAY_DECISION_REQUEST_INVALID';error.status = 400;throw error;
}
function date(value) {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const parsed = new Date(`${value}T00:00:00Z`);
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value;
}
function unique(values, predicate) { return Array.isArray(values) && values.every(predicate) && new Set(values).size === values.length; }
const sorted = (values) => values.every((value, i) => !i || values[i - 1] < value);
function evidence(refs) {
  if (!Array.isArray(refs)) fail();
  const seen = new Set();
  for (const ref of refs) {
    if (!sourceNativeClosed(ref, ['kind', 'id', 'version', 'sha256'])
        || !['SOURCE_ROW', 'WORKER_RESPONSE', 'SIGNED_DOCUMENT', 'OFFICE_NOTE'].includes(ref.kind)
        || !isUuid(ref.id) || !(ref.version === null || token(ref.version))
        || !(ref.sha256 === null || isHash(ref.sha256))) fail();
    const key = `${ref.kind}:${ref.id}:${ref.version ?? ''}`;
    if (seen.has(key)) fail();seen.add(key);
  }
}
function history(refs) {
  const simple = new Set(['RUN_LINE', 'FINANCIAL_EFFECT', 'WORK_HOLD', 'RECOVERY_HOLD', 'TRANSFER',
    'TRANSFER_OUTCOME', 'EXPENSE_COMPONENT', 'LEGACY_PAY_ITEM', 'LEGACY_SNAPSHOT', 'LEGACY_BREAKDOWN', 'LEGACY_BANK_TRANSFER']);
  if (!Array.isArray(refs) || refs.length > 250) fail();
  const seen = new Set();
  for (const ref of refs) {
    if (!sourceNativeClosed(ref, ['kind', 'key', 'record_sha256']) || !isHash(ref.record_sha256)) fail();
    const key = ref.key;
    if (simple.has(ref.kind)) {
      if (!sourceNativeClosed(key, ['id']) || !isUuid(key.id)) fail();
    } else if (ref.kind === 'APPROVED_LINE') {
      if (!sourceNativeClosed(key, ['revision_id', 'line_id', 'component_key'])
          || !isUuid(key.revision_id) || !isUuid(key.line_id) || !token(key.component_key)) fail();
    } else if (ref.kind === 'APPROVED_SEGMENT') {
      if (!sourceNativeClosed(key, ['revision_id', 'segment_id', 'component_key', 'segment_key'])
          || !isUuid(key.revision_id) || !isUuid(key.segment_id) || !token(key.component_key) || !token(key.segment_key)) fail();
    } else if (ref.kind === 'LEGACY_V8_COMPONENT') {
      if (!sourceNativeClosed(key, ['certificate_uuid', 'constituent_ordinal', 'evidence_kind', 'evidence_ordinal'])
          || !isUuid(key.certificate_uuid) || !Number.isSafeInteger(key.constituent_ordinal)
          || key.constituent_ordinal < 0 || key.constituent_ordinal > 49999
          || !Number.isSafeInteger(key.evidence_ordinal) || key.evidence_ordinal < 0
          || !['ALL_SAME_ECONOMIC_KEY', 'FULL_SIGNED_PRE_SIGNATURE', 'DECISIVE_SIGNED_EVIDENCE'].includes(key.evidence_kind)) fail();
    } else fail();
    // Property order is a transport detail; the identity excludes record hash
    // so two contradictory hashes cannot duplicate the same native reference.
    const identity = JSON.stringify([ref.kind, Object.keys(key).sort().map((k) => [k, key[k]])]);
    if (seen.has(identity)) fail();seen.add(identity);
  }
}
function prior(refs) {
  if (!Array.isArray(refs)) fail();
  const seen = new Set();
  for (const ref of refs) {
    if (!sourceNativeClosed(ref, ['member_key', 'component_key', 'disposition_id', 'decision_id', 'disposition_version'])
        || !token(ref.member_key) || !(ref.component_key === null || token(ref.component_key))) fail();
    const empty = ref.disposition_id === null && ref.decision_id === null && ref.disposition_version === null;
    if (!empty && (!isUuid(ref.disposition_id) || !isUuid(ref.decision_id) || !sourceNativePositiveBigint(ref.disposition_version))) fail();
    const identity = JSON.stringify([ref.member_key, ref.component_key]);
    if (seen.has(identity)) fail();seen.add(identity);
  }
}

// Browser structure only. SQL rederives native identity, visibility, quantities,
// provenance and prior aggregate CAS; no hash supplied here is financial proof.
export function decodeSourcePayDecisionBrowserRequest(value, mode) {
  if (mode === 'PREVIEW') {
    const allowed = [...previewKeys, 'evidence_refs', 'financial_history_refs'];
    if (value === null || typeof value !== 'object' || Array.isArray(value)
        || Object.keys(value).some((key) => !allowed.includes(key))
        || previewKeys.some((key) => !Object.hasOwn(value, key))) fail();
  } else {
    const keys = mode === 'ACCEPT' ? acceptKeys : mode === 'RESUME_FINANCIAL' ? resumeKeys
      : mode === 'READ_RESULT' ? ['version', 'action', 'decision_id', 'request_id'] : null;
    if (!keys || !sourceNativeClosed(value, keys) || value.action !== mode) fail();
  }
  if (value.version !== 'SOURCE_PAY_DECISION_V1' || !isUuid(value.decision_id) || !isUuid(value.request_id)) fail();
  if (mode === 'READ_RESULT') return value;
  if (mode === 'PREVIEW' || mode === 'ACCEPT') { if (!decisions.includes(value.decision)) fail(); }
  if (mode === 'PREVIEW') {
    if (!Array.isArray(value.selection) || !value.selection.length) fail();
    const seen = new Set();const subjects = new Map();
    for (const member of value.selection) {
      if (!sourceNativeClosed(member, ['scope_kind', 'scope_mode', 'family_root_id', 'source_work_event_id',
        'source_row_id', 'component_keys', 'work_dates', 'expected_work_revision'])
          || !['TIMESHEET_FAMILY', 'SOURCE_ROW'].includes(member.scope_kind)
          || !['COMPONENT_QUANTITIES', 'WHOLE_SHIFT'].includes(member.scope_mode)
          || !(member.family_root_id === null || isUuid(member.family_root_id))
          || (member.scope_kind === 'TIMESHEET_FAMILY' && member.family_root_id === null)
          || !(member.source_work_event_id === null || isUuid(member.source_work_event_id))
          || !isUuid(member.source_row_id) || !unique(member.component_keys, token)
          || (member.scope_mode === 'COMPONENT_QUANTITIES' && !member.component_keys.length)
          || !unique(member.work_dates, date) || !member.work_dates.length || !sorted(member.work_dates)
          || !(member.expected_work_revision === null || sourceNativePositiveBigint(member.expected_work_revision))) fail();
      const identity = JSON.stringify([member.scope_kind, member.scope_mode, member.family_root_id,
        member.source_work_event_id, member.source_row_id, member.component_keys, member.work_dates]);
      if (seen.has(identity)) fail();seen.add(identity);
      const siblings=subjects.get(member.source_row_id)??[];
      if(siblings.some(prior=>prior.scope_mode==='WHOLE_SHIFT'||member.scope_mode==='WHOLE_SHIFT'
        ||prior.component_keys.some(key=>member.component_keys.includes(key))))fail();
      siblings.push(member);subjects.set(member.source_row_id,siblings);
    }
    evidence(value.evidence_refs ?? []);history(value.financial_history_refs ?? []);
    return { ...value, evidence_refs: value.evidence_refs ?? [], financial_history_refs: value.financial_history_refs ?? [] };
  }
  if (!isUuid(value.review_id) || !isHash(value.review_digest) || !isHash(value.expected_pay_cut_hash)
      || !isHash(value.expected_prior_dispositions_sha256)) fail();
  prior(value.expected_prior_dispositions);evidence(value.evidence_refs);
  if (mode === 'RESUME_FINANCIAL') {
    if (!sourceNativePositiveBigint(value.expected_state_revision)) fail();
  } else if (!(value.supersedes_decision_id === null || isUuid(value.supersedes_decision_id))
      || value.invoice_policy !== 'KEEP_SOURCE_INVOICE' || value.contact_policy !== 'DO_NOT_QUERY_SELECTED_ISSUES'
      || typeof value.reason !== 'string' || !value.reason.trim() || [...value.reason].length > 1000
      || !isHash(value.expected_source_invoice_hash) || !isHash(value.expected_issue_set_hash)) fail();
  return value;
}
