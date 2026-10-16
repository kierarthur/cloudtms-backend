import assert from 'node:assert/strict';
import test from 'node:test';
import { decodeSourcePayDecisionBrowserRequest as decode } from '../../broker/src/weekly-source/source-pay-decision-request.mjs';

test('Source-row overlap is rejected across scope labels, while disjoint real component selections stay separate',()=>{
 const base=request('PREVIEW');const first={...base.selection[0],scope_mode:'COMPONENT_QUANTITIES',component_keys:['actual:one']};
 const second={...first,component_keys:['actual:two']};decode({...base,selection:[first,second]},'PREVIEW');
 assert.throws(()=>decode({...base,selection:[first,{...second,component_keys:['actual:one']}]},'PREVIEW'));
 assert.throws(()=>decode({...base,selection:[base.selection[0],{...first,scope_kind:'TIMESHEET_FAMILY',family_root_id:id(7)}]},'PREVIEW'));
});
const id = (n) => `91000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const hash = 'a'.repeat(64);
const common = { version: 'SOURCE_PAY_DECISION_V1', decision_id: id(1), request_id: id(2) };
function request(mode) {
  if (mode === 'READ_RESULT') return { ...common, action: mode };
  if (mode === 'PREVIEW') return { ...common, decision: 'EXCLUDE_FUTURE_WORK_PAY', selection: [{
    scope_kind: 'SOURCE_ROW', scope_mode: 'WHOLE_SHIFT', family_root_id: null, source_work_event_id: null,
    source_row_id: id(3), component_keys: [], work_dates: ['2026-10-16'], expected_work_revision: null }] };
  const base = { ...common, action: mode, review_id: id(4), review_digest: hash, expected_pay_cut_hash: hash,
    evidence_refs: [], expected_prior_dispositions: [{ member_key: 'real-source-row', component_key: null,
      disposition_id: null, decision_id: null, disposition_version: null }], expected_prior_dispositions_sha256: hash };
  if (mode === 'RESUME_FINANCIAL') return { ...base, expected_state_revision: '9007199254740993' };
  return { ...base, decision: 'EXCLUDE_FUTURE_WORK_PAY', supersedes_decision_id: null,
    invoice_policy: 'KEEP_SOURCE_INVOICE', contact_policy: 'DO_NOT_QUERY_SELECTED_ISSUES',
    reason: 'Keep the real Source invoice', expected_source_invoice_hash: hash, expected_issue_set_hash: hash };
}
for (const mode of ['PREVIEW', 'ACCEPT', 'RESUME_FINANCIAL', 'READ_RESULT']) {
  test(`${mode} accepts its exact browser variant`, () => { decode(request(mode), mode); });
  for (const field of ['actor_user_id', 'application_id', 'native_payload', 'unprotect_preparations', 'preview_assembly_ref', 'expectedAfter', 'amount']) {
    test(`${mode} refuses browser ${field}`, () => {
      assert.throws(() => decode({ ...request(mode), [field]: id(9) }, mode), { code: 'SOURCE_PAY_DECISION_REQUEST_INVALID' });
    });
  }
  for (const field of Object.keys(request(mode))) {
    test(`${mode} requires ${field}`, () => {
      const value = request(mode);delete value[field];assert.throws(() => decode(value, mode));
    });
  }
}
test('positive Source decision uses the same variant and cannot add Unprotect authority', () => {
  for (const mode of ['PREVIEW', 'ACCEPT']) {
    const value = request(mode);value.decision = 'ACCEPT_FINAL_SOURCE_PAY';decode(value, mode);
    value.unprotect_preparations = [];assert.throws(() => decode(value, mode));
  }
});
test('prefinal WHOLE_SHIFT preserves absent family/Work/revision and optional facts defaults', () => {
  const value = decode(request('PREVIEW'), 'PREVIEW');
  assert.equal(value.selection[0].family_root_id, null);
  assert.equal(value.selection[0].expected_work_revision, null);
  assert.deepEqual(value.evidence_refs, []);assert.deepEqual(value.financial_history_refs, []);
});
test('selection rejects duplicate subjects, duplicate components, impossible date and Number revision', () => {
  for (const mutate of [v => v.selection.push({ ...v.selection[0] }),
    v => { v.selection[0].component_keys = ['real', 'real']; },
    v => { v.selection[0].work_dates = ['2026-02-30']; },
    v => { v.selection[0].expected_work_revision = 9007199254740992; },
    v => { v.selection[0].scope_kind = 'TIMESHEET_FAMILY'; }]) {
    const value = request('PREVIEW');mutate(value);assert.throws(() => decode(value, 'PREVIEW'));
  }
});
test('prior aggregate repeats across different real components, with full bigint provenance', () => {
  const value = request('ACCEPT');
  value.expected_prior_dispositions = ['day', 'night'].map((component_key) => ({ member_key: 'same-shift', component_key,
    disposition_id: id(5), decision_id: id(6), disposition_version: '9007199254740993' }));
  decode(value, 'ACCEPT');
  value.expected_prior_dispositions.push({ ...value.expected_prior_dispositions[0] });
  assert.throws(() => decode(value, 'ACCEPT'));
});
test('partial prior pointers and overflow never become no-prior history', () => {
  for (const changes of [{ disposition_id: id(5) }, { disposition_version: '0' },
    { disposition_id: id(5), decision_id: id(6), disposition_version: '9223372036854775808' }]) {
    const value = request('ACCEPT');Object.assign(value.expected_prior_dispositions[0], changes);
    assert.throws(() => decode(value, 'ACCEPT'));
  }
});
test('real legacy item history needs neither a fabricated Work nor V8 reference', () => {
  const value = request('PREVIEW');value.financial_history_refs = [{ kind: 'LEGACY_PAY_ITEM', key: { id: id(7) }, record_sha256: hash }];
  decode(value, 'PREVIEW');
  value.financial_history_refs[0].key.work_id = id(8);assert.throws(() => decode(value, 'PREVIEW'));
});
test('history union rejects an arbitrary NET/URL proof and contradictory duplicate native reference', () => {
  for (const refs of [[{ kind: 'NATIVE_PAYMENT', key: { id: id(7) }, record_sha256: hash }],
    [{ kind: 'TRANSFER', key: { id: id(7), amount: '100.00' }, record_sha256: hash }],
    [{ kind: 'TRANSFER', key: { id: id(7) }, record_sha256: hash },
      { kind: 'TRANSFER', key: { id: id(7) }, record_sha256: 'b'.repeat(64) }]]) {
    const value = request('PREVIEW');value.financial_history_refs = refs;assert.throws(() => decode(value, 'PREVIEW'));
  }
});
