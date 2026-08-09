const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const workerSource = fs.readFileSync(path.resolve(__dirname, '../broker/src/index.js'), 'utf8');
const helperStart = workerSource.indexOf('  const numericOrNull =');
const helperEnd = workerSource.indexOf('\n  const fetchCurrentSessionSelectionForCreateDraft =', helperStart);
assert.ok(helperStart >= 0 && helperEnd > helperStart, 'Worker Create Draft selection helpers must be present');

const helperSource = workerSource.slice(helperStart, helperEnd);

function installHarness() {
  const context = {
    booleanFrom: (value) => value === true || ['true', '1', 'yes'].includes(String(value || '').toLowerCase()),
    isPlainObject: (value) => !!value && typeof value === 'object' && !Array.isArray(value),
    trimStr: (value) => String(value ?? '').trim(),
    upperTrim: (value) => String(value ?? '').trim().toUpperCase(),
    uuidRe: /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i
  };
  vm.runInNewContext(
    `${helperSource}\nthis.__readOverlay = readCreateDraftRecoverySelectionOverlay; this.__buildContract = buildCreateDraftSelectedPreviewRowContract;`,
    context,
    { filename: 'worker-create-draft-recovery-overlay.js' }
  );
  return { readOverlay: context.__readOverlay, buildContract: context.__buildContract };
}

const candidateId = '11111111-1111-4111-8111-111111111111';
const timesheetId = '22222222-2222-4222-8222-222222222222';

const validPromotedRecovery = () => ({
  id: '33333333-3333-4333-8333-333333333333',
  candidate_id: candidateId,
  timesheet_id: timesheetId,
  section: 'blocked_for_pay',
  status: 'READY',
  selection_state: 'SELECTED',
  selected: true,
  amount_ex_vat: -1,
  row_json: {
    candidate_id: candidateId,
    timesheet_id: timesheetId,
    line_type: 'OVERPAYMENT_RECOVERY',
    pay_channel: 'PAYE',
    presentation_section: 'READY_TO_PAY',
    selection_state: 'SELECTED',
    selected: true,
    status: 'READY',
    draftable: true,
    is_ready_for_draft: true,
    is_excluded_from_allocation: false,
    selection_allowed: true,
    amount_ex_vat: -1,
    economic_key: { timesheet_id: timesheetId, key_type: 'TS_DAY', key_value: '2026-07-05' },
    preview_contract: { ok: true, selection_allowed: false },
    selection_recovery_headroom_v1: {
      contract_version: 1,
      candidate_id: candidateId,
      pay_channel: 'PAYE',
      physical_section: 'blocked_for_pay',
      effective_section: 'canonical_preview_lines',
      selected_positive_headroom_ex_vat: 1,
      nominal_due_amount_ex_vat: 78.26,
      recoverable_amount_ex_vat: 1,
      static_recovery_eligible: true,
      overlay_digest: '0123456789abcdef0123456789abcdef',
      policy_x_authority_scope: 'PRE_DRAFT_LIVE_TRUTH'
    }
  }
});

test('Worker derives the effective Draft contract from a valid recovery-selection overlay', () => {
  const harness = installHarness();
  const row = validPromotedRecovery();
  const overlay = harness.readOverlay(row);
  const contract = harness.buildContract(row, 0);
  assert.equal(overlay.effective_section, 'canonical_preview_lines');
  assert.equal(contract.section, 'canonical_preview_lines');
  assert.equal(contract.physical_section, 'blocked_for_pay');
  assert.equal(contract.selection_recovery_headroom_contract_version, 1);
  assert.equal(contract.selection_recovery_headroom_overlay_digest, '0123456789abcdef0123456789abcdef');
});

test('Worker fails closed for malformed or inconsistent recovery-selection overlays', () => {
  const harness = installHarness();
  for (const mutate of [
    (row) => { row.row_json.selection_recovery_headroom_v1.overlay_digest = 'bad'; },
    (row) => { row.row_json.selection_recovery_headroom_v1.recoverable_amount_ex_vat = 2; },
    (row) => { row.row_json.selection_recovery_headroom_v1.candidate_id = '44444444-4444-4444-8444-444444444444'; },
    (row) => { row.row_json.selection_recovery_headroom_v1.policy_x_authority_scope = 'POST_DRAFT'; },
    (row) => { row.row_json.line_type = 'TIMESHEET_PAYMENT'; }
  ]) {
    const row = validPromotedRecovery();
    mutate(row);
    assert.equal(harness.readOverlay(row), null);
    assert.equal(harness.buildContract(row, 0).section, 'blocked_for_pay');
  }
});

test('Worker selection read includes selected overlay rows and binds overlay proof into row-contract comparison', () => {
  const selectionStart = workerSource.indexOf('  const fetchCurrentSessionSelectionForCreateDraft =');
  const selectionEnd = workerSource.indexOf('\n  const isActiveDraftCreateStatus =', selectionStart);
  const selectionBody = workerSource.slice(selectionStart, selectionEnd);
  assert.doesNotMatch(selectionBody, /section=eq\.canonical_preview_lines&selected=eq\.true/);
  assert.match(selectionBody, /selected=eq\.true&status=eq\.READY/);
  assert.match(selectionBody, /!recoverySelectionOverlay && !booleanFrom\(previewContract\.selection_allowed\)/);
  assert.match(workerSource, /selection_recovery_headroom_overlay_digest: trimStr\(contract\.selection_recovery_headroom_overlay_digest/);
});
