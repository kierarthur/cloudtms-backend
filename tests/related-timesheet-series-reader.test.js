import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');
const worker = fs.readFileSync(path.join(root, 'broker', 'src', 'index.js'), 'utf8');

test('Related Timesheet series uses the supported server-owned Summary reader', () => {
  const start = worker.indexOf('const fetchTimesheetSummaryByIds = async');
  const end = worker.indexOf('\n  try {', start);
  assert.notEqual(start, -1);
  assert.notEqual(end, -1);
  const reader = worker.slice(start, end);

  assert.match(reader, /sbRpc\(env, 'timesheet_summary_lightweight_rows_v1'/);
  assert.doesNotMatch(reader, /\/rest\/v1\/v_timesheets_summary/);
  assert.match(reader, /ids: chunk/);
  assert.match(reader, /limit: chunk\.length/);
});

test('Related Timesheet routes expose an expired-session 401 to the browser', () => {
  for (const marker of ['async function handleRelatedList', 'async function handleRelatedCounts']) {
    const start = worker.indexOf(marker);
    const end = worker.indexOf('\n\n', start);
    assert.notEqual(start, -1);
    assert.notEqual(end, -1);
    const opening = worker.slice(start, end);
    assert.match(opening, /if \(!user\) return withCORS\(env, req, unauthorized\(\)\);/);
  }
});
