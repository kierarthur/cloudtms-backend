import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const source = fs.readFileSync(path.resolve(__dirname, '../broker/src/index.js'), 'utf8');

function between(startMarker, endMarker) {
  const start = source.indexOf(startMarker);
  const end = source.indexOf(endMarker, start);
  assert.notEqual(start, -1, `missing start marker: ${startMarker}`);
  assert.notEqual(end, -1, `missing end marker: ${endMarker}`);
  return source.slice(start, end);
}

const classifierSource = between('function classifyBulkAuthoriseRow', 'function isBulkAuthoriseSystemAuthorisationEvidence');
const sandbox = {};
vm.runInNewContext(`${classifierSource}\nthis.classify = classifyBulkAuthoriseRow;`, sandbox, {
  filename: 'office-expense-route-classifier.js'
});

test('canonical Office route classification distinguishes Manual, Electronic and QR', () => {
  const classify = sandbox.classify;
  const manual = classify({ submission_mode: 'MANUAL' }, {}, {});
  const electronic = classify({ submission_mode: 'ELECTRONIC' }, {}, {});
  const qr = classify({ submission_mode: 'QR', qr_status: 'USED' }, {}, {});
  const additionalManual = classify({ submission_mode: 'MANUAL', additional_seq: 2, is_adjustment: true }, {}, {});

  assert.equal(manual.route_family, 'MANUAL_NON_QR');
  assert.equal(electronic.route_family, 'ELECTRONIC');
  assert.equal(qr.route_family, 'QR');
  assert.equal(additionalManual.route_family, 'MANUAL_NON_QR');
});

test('real-timesheet Office expense and mileage mutations require the Manual route', () => {
  const handler = between('async function patchTsfinCommon', 'async function handleTsfinPatchPO');
  assert.match(handler, /const officeExpenseRoute = classifyBulkAuthoriseRow/);
  assert.match(handler, /officeExpenseRouteFamily !== 'MANUAL_NON_QR'/);
  assert.match(handler, /isExpenseValueMutation = !!\(patch\.expenses \|\| patch\.mileage\)/);
  assert.match(handler, /OFFICE_EXPENSES_MANUAL_ROUTE_REQUIRED/);
});

test('planned-week Office expense values require a Manual submission-mode snapshot', () => {
  const handler = between('async function handleContractWeekManualDraftUpsert', 'async function handleContractWeekManualAuthorise');
  assert.match(handler, /const mode = String\(cw\.submission_mode_snapshot \|\| ''\)\.toUpperCase\(\)/);
  assert.match(handler, /isExpensesOnlyDraftSave[\s\S]*if \(mode !== 'MANUAL'\)/);
  assert.match(handler, /OFFICE_EXPENSES_MANUAL_ROUTE_REQUIRED/);
});

test('Candidate application routes remain separate from the Office-only guard', () => {
  const candidateRouter = fs.readFileSync(path.resolve(__dirname, '../candidate-broker/src/candidate-broker.js'), 'utf8');
  const candidateBackend = fs.readFileSync(path.resolve(__dirname, '../broker/src/candidate-app-backend.js'), 'utf8');

  assert.doesNotMatch(candidateRouter, /OFFICE_EXPENSES_MANUAL_ROUTE_REQUIRED/);
  assert.doesNotMatch(candidateBackend, /OFFICE_EXPENSES_MANUAL_ROUTE_REQUIRED/);
  assert.match(candidateRouter, /\/candidate-app\/v1/);
});
