import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const worker = readFileSync(new URL('../src/index.js', import.meta.url), 'utf8');

function functionBody(name) {
  const start = worker.indexOf(`async function ${name}(`);
  assert.notEqual(start, -1, `${name} must be defined`);
  const next = worker.indexOf('\nasync function ', start + 1);
  return worker.slice(start, next < 0 ? worker.length : next);
}

test('case-resolution discovery rebinds only its read-only progress fence', () => {
  const body = functionBody(
    'handleBankingPayWorkbenchSessionApplyCaseResolution',
  );
  assert.match(
    body,
    /if \(operation === 'DISCOVER'\)[\s\S]*expected_progress_counter_version:\s*currentProgressCounterVersion/,
  );
  assert.match(
    body,
    /else \{\s*return buildKnownCaseResolutionConflict\(\s*'WORKBENCH_SESSION_PROGRESS_CHANGED'/,
  );
  assert.match(
    body,
    /APPLY remains strictly[\s\S]*fenced to the version the user reviewed/,
  );
});

test('a changed Apply fence returns specific friendly recovery guidance', () => {
  const body = functionBody(
    'handleBankingPayWorkbenchSessionApplyCaseResolution',
  );
  assert.match(
    body,
    /WORKBENCH_SESSION_PROGRESS_CHANGED:\s*\{[\s\S]*title: 'Payment preview updated'/,
  );
  assert.match(
    body,
    /Close this window, refresh Banking Pay, and review the latest values before trying again\./,
  );
});

test('a stale correction clear scope returns specific friendly recovery guidance', () => {
  const body = functionBody(
    'handleBankingPayWorkbenchSessionClearCaseResolution',
  );
  assert.match(
    body,
    /CORRECTION_COMPONENT_REQUIRED_FOR_CASE_RESOLUTION/,
  );
  assert.match(
    body,
    /code: 'WORKBENCH_RESOLUTION_SCOPE_CHANGED'/,
  );
  assert.match(
    body,
    /The resolved-rate details changed after this window opened\./,
  );
});
