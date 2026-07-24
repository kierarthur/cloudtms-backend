const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const workerSource = fs.readFileSync(
  path.resolve(__dirname, '../broker/src/index.js'),
  'utf8'
);
const bankingSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql'),
  'utf8'
);

const selectedStart = workerSource.indexOf('const createDraftPreviewRowSelectedOrReady =');
const helperEnd = workerSource.indexOf('\n  const fetchCurrentSessionSelectionForCreateDraft =', selectedStart);
assert.ok(selectedStart >= 0 && helperEnd > selectedStart, 'Worker TS_TOTAL create-draft helper must be present');

const helperSource = workerSource.slice(selectedStart, helperEnd);

function installHarness() {
  const context = {
    Array,
    Object,
    String,
    isPlainObject: (value) => !!value && typeof value === 'object' && !Array.isArray(value),
    booleanFrom: (value) => value === true || ['true', '1', 'yes'].includes(String(value || '').toLowerCase()),
    upperTrim: (value) => String(value || '').trim().toUpperCase(),
    trimStr: (value) => String(value || '').trim(),
    getCreateDraftPreviewRowJson: (row) => row.row_json || {},
    getCreateDraftPreviewSourceBasis: (row) => row.source_basis_json || row.row_json?.source_basis_json || {},
    getCreateDraftPreviewKeyType: (row) => row.key_type || row.row_json?.key_type || row.row_json?.economic_key?.key_type || '',
    getCreateDraftPreviewKeyValue: (row) => row.key_value || row.row_json?.key_value || row.row_json?.economic_key?.key_value || '',
    getCreateDraftPreviewSourceBasisKeyType: (row) => row.source_basis_json?.component_key_type || row.row_json?.source_basis_json?.component_key_type || '',
    getCreateDraftPreviewSourceBasisKeyValue: (row) => row.source_basis_json?.component_key_value || row.row_json?.source_basis_json?.component_key_value || '',
    createDraftPreviewRowIdentityText: (row) => [row.row_key, row.preview_row_id, row.row_json?.row_key, row.row_json?.preview_row_id].filter(Boolean).join('|').toLowerCase(),
    getCreateDraftPreviewTimesheetId: (row) => row.timesheet_id || row.row_json?.timesheet_id || ''
  };

  vm.runInNewContext(
    `${helperSource}\nthis.__isSynthetic = isSyntheticResolvedTimesheetResidualPreviewRow;`,
    context,
    { filename: 'worker-banking-pay-ts-total-draftability.js' }
  );
  return context.__isSynthetic;
}

const totalRow = (overrides = {}) => ({
  row_key: '11111111-1111-4111-8111-111111111111:non_segment:total',
  preview_row_id: '11111111-1111-4111-8111-111111111111:non_segment:total',
  timesheet_id: '11111111-1111-4111-8111-111111111111',
  key_type: 'TS_TOTAL',
  key_value: 'TOTAL',
  selected: true,
  selection_state: 'SELECTED',
  status: 'READY',
  ...overrides
});

test('Worker accepts a genuine non-segment TS_TOTAL row', () => {
  const isSynthetic = installHarness();
  assert.equal(isSynthetic(totalRow(), []), false);
});

test('Worker rejects a TS_TOTAL row explicitly replaced by segments', () => {
  const isSynthetic = installHarness();
  assert.equal(isSynthetic(totalRow({ resolved_segment_rows_replace_source_total: true }), []), true);
});

test('Worker rejects a stale TS_TOTAL row alongside a TS_DAY row', () => {
  const isSynthetic = installHarness();
  const total = totalRow();
  const day = {
    timesheet_id: total.timesheet_id,
    key_type: 'TS_DAY',
    key_value: '2026-06-30',
    selected: true,
    selection_state: 'SELECTED',
    status: 'READY'
  };
  assert.equal(isSynthetic(total, [total, day]), true);
});
test('terminally materialised workbench candidates are not reported as unseeded', () => {
  const progressStart = bankingSql.indexOf(
    'CREATE OR REPLACE FUNCTION public.pay_workbench_session_get_progress_light'
  );
  const progressEnd = bankingSql.indexOf('\n$function$;', progressStart);
  assert.ok(progressStart >= 0 && progressEnd > progressStart, 'progress RPC must be present');

  const progressSql = bankingSql.slice(progressStart, progressEnd);
  assert.match(
    progressSql,
    /COUNT\(\*\) FILTER \(\s*WHERE COALESCE\(scope_row\.seeded, false\)\s*OR UPPER\(BTRIM\(COALESCE\(scope_row\.status, ''\)\)\) IN \(\s*'READY', 'MATERIALISED', 'MATERIALIZED', 'SOURCE_EMPTY'\s*\)\s*\)::integer/
  );
  assert.match(
    progressSql,
    /\(\s*COALESCE\(scope_row\.seeded, false\)\s*OR UPPER\(BTRIM\(COALESCE\(scope_row\.status, ''\)\)\) IN \(\s*'READY', 'MATERIALISED', 'MATERIALIZED', 'SOURCE_EMPTY'\s*\)\s*\) AS seeded/
  );
});
test('Worker defensively caps unseeded candidates after terminal materialisation', () => {
  assert.match(
    workerSource,
    /const candidateUnseededCapacity = total > 0\s*\?\s*Math\.max\(0, total - completed - failed\)\s*:\s*candidateUnseededReported;/
  );
  assert.match(
    workerSource,
    /const candidateUnseeded = Math\.min\(candidateUnseededReported, candidateUnseededCapacity\);/
  );
});