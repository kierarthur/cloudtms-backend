import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const source = fs.readFileSync(path.join(root, 'broker/src/index.js'), 'utf8');
const start = source.indexOf('async function handleTimesheetEvidenceList');
const end = source.indexOf('function isImportAuthoritativeEvidenceContext', start);

assert.ok(start >= 0 && end > start, 'timesheet evidence list handler must exist');
const handler = source.slice(start, end);

test('withdrawn evidence history is lineage-bound and absent from the legacy array response', () => {
  assert.match(handler, /const bookingId = asUuidStringOrNull\(ts\?\.booking_id\)/);
  assert.match(handler, /booking_id=eq\.\$\{enc\(bookingId\)\}[\s\S]*is_current=eq\.false[\s\S]*status=eq\.REVOKED[\s\S]*revoked_by=eq\.CANDIDATE/);
  assert.match(handler, /if \(wantMeta && bookingId\)/);
  assert.match(handler, /evidence: all,[\s\S]*withdrawn_submissions: withdrawnSubmissions/);
  assert.match(handler, /return withCORS\(env, req, ok\(all\)\)/);
});

test('every historical item is audit-only and cannot enter current evidence mutations', () => {
  assert.match(handler, /withdrawn_history: true,[\s\S]*is_view_only: true,[\s\S]*can_delete: false,[\s\S]*can_reclassify: false,[\s\S]*can_return_to_queue: false/);
  assert.match(handler, /status: 'WITHDRAWN'[\s\S]*read_only: true[\s\S]*evidence: historicalEvidence/);
  assert.doesNotMatch(handler, /withdrawnSubmissions[\s\S]*\b(?:insert|update|delete)\b[\s\S]*timesheet/i);
});

test('the retained official timesheet and signatures are projected as separate viewable history', () => {
  assert.match(handler, /kind: 'TIMESHEET'[\s\S]*display_name: 'Official withdrawn timesheet'[\s\S]*preview_mode: 'PDF'/);
  assert.match(handler, /kind: 'ELECTRONIC_SIGNATURES'[\s\S]*preview_mode: 'SIGNATURES'/);
  assert.match(handler, /r2_nurse_key: nurseKey \|\| null[\s\S]*r2_auth_key: authoriserKey \|\| null/);
});
