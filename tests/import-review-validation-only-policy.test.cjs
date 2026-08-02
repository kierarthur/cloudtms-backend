const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const read = (relative) => fs.readFileSync(path.join(__dirname, '..', relative), 'utf8');
const migration = read('supabase/migrations/02082026_1626_healthroster_weekly_candidate_not_worked_resolution.sql');
const resolver = read('supabase/repeatable/02082026_1626_hr_weekly_candidate_not_worked_resolution.sql');
const preview = read('supabase/repeatable/21072026_1820_13_hr_weekly_validation_preview.sql');
const core = read('supabase/repeatable/21072026_1820_00_import_review_internal_core.sql');
const ui = read('supabase/repeatable/22072026_0052_import_review_ui_contract_v1.sql');
const worker = read('broker/src/import-review.js');

test('Weekly candidate-did-not-work evidence is a locked validation-only record', () => {
  assert.match(migration, /import_review_weekly_validation_resolutions/);
  assert.match(migration, /resolution_code='CANDIDATE_DID_NOT_WORK'/);
  assert.match(migration, /enable row level security/i);
  assert.match(migration, /revoke all on table public\.import_review_weekly_validation_resolutions[\s\S]*service_role/i);
  assert.match(migration, /never authorises an hours, TSFIN, invoice, payment or other financial mutation/i);
});

test('resolution RPC re-proves the current submitted Weekly comparison and is server-only', () => {
  assert.match(resolver, /_import_review_assert_actor_v1/);
  assert.match(resolver, /hr_weekly_validation_preview\(p_import_id\)/);
  assert.match(resolver, /match_status'='HR_ONLY/);
  assert.match(resolver, /jsonb_array_length\(t\.actual_schedule_json\)>0/);
  assert.match(resolver, /jsonb_array_length\(tf\.invoice_breakdown_json->'segments'\)>0/);
  assert.match(resolver, /WEEKLY_CANDIDATE_DID_NOT_WORK/);
  assert.match(resolver, /revoke all on function[\s\S]*public,anon,authenticated/i);
  assert.match(resolver, /grant execute on function[\s\S]*postgres,service_role/i);
});

test('Weekly preview excludes only the confirmed HR row and returns OVERRIDDEN', () => {
  assert.match(preview, /confirmed_hr_exceptions/);
  assert.match(preview, /hr_day_totals_effective/);
  assert.match(preview, /exception_evidence_fingerprint/);
  assert.match(preview, /when coalesce\(g\.confirmed_exception_count,0\)>0 then 'OVERRIDDEN'/);
  assert.match(preview, /confirmed_exceptions_json/);
  assert.match(preview, /t\.revoked_at is null/);
  assert.match(preview, /t\.archived_at_utc is null/);
});

test('catalog exposes the resolution only for a proved omitted Weekly shift', () => {
  assert.match(core, /WEEKLY_SHIFT_ABSENT_FROM_TIMESHEET/);
  assert.match(core, /WEEKLY_CANDIDATE_DID_NOT_WORK/);
  assert.match(core, /nullif\(o\.comparison_json->>'hr_row_id',''\)::uuid/);
  assert.match(core, /CANDIDATE_DID_NOT_WORK_CONFIRMED/);
  assert.match(core, /Passed with confirmed exception/);
  assert.match(core, /import_review_weekly_validation_resolutions[\s\S]*EVIDENCE_CHANGED/);
});

test('UI contract hides mismatch support rows from Passed checks', () => {
  assert.doesNotMatch(ui, /create or replace function public\.import_review_contract_version_get_v1/i);
  assert.match(ui, /when 'NO_ACTION' then a\.action_category='NO_ACTION' and/);
  assert.match(ui, /CANDIDATE_DID_NOT_WORK_CONFIRMED/);
  assert.match(ui, /jsonb_array_length/);
  assert.match(ui, /when 'NO_ACTION' then 'Passed checks'/);
});

test('UI state accepts the Emails section but persists only opaque expansion tokens', () => {
  assert.match(core, /upper\(v->>'active_section'\) not in \('PENDING','READY','EMAILS','NO_ACTION'\)/);
  assert.match(core, /\^\(candidate\|client\|week\|shift\):u-/);
  assert.match(core, /IMPORT_REVIEW_UI_STATE_CONTAINS_AUTHORITY/);
  assert.doesNotMatch(core, /v::text ~\* '\(recipient\|email\|amount/);
});

test('Worker exposes one bounded intent-only resolution route', () => {
  assert.match(worker, /hr_weekly_candidate_not_worked_resolution_save_v1/);
  assert.match(worker, /weekly-candidate-not-worked/);
  assert.match(worker, /typeof body\.confirmed !== 'boolean'/);
  assert.match(worker, /p_action_id: sha256\(body\.action_id/);
  assert.doesNotMatch(worker, /weekly-candidate-not-worked[\s\S]{0,1800}(?:hours|pay|charge|invoice)_value/i);
});
