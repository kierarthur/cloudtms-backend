const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const sqlPath = path.join(
  __dirname,
  '..',
  'supabase',
  'repeatable',
  '13062026_1544_process_authorise_unprocess_unauthorise.sql'
);

const sql = fs.readFileSync(sqlPath, 'utf8');

function functionDefinition(name) {
  const marker = `CREATE OR REPLACE FUNCTION public.${name}`;
  const start = sql.indexOf(marker);
  assert.notEqual(start, -1, `${name} must exist in the repeatable SQL`);

  const next = sql.indexOf('\nCREATE OR REPLACE FUNCTION public.', start + marker.length);
  return sql.slice(start, next === -1 ? sql.length : next);
}

test('Bulk Authorise sources its complete correction contract from timesheets', () => {
  const definition = functionDefinition('bulk_authorise_dataset_v1');

  assert.match(
    definition,
    /JOIN public\.timesheets AS correction_timesheet\s+ON correction_timesheet\.timesheet_id = s\.timesheet_id/
  );
  assert.match(
    definition,
    /LEFT JOIN public\.timesheets AS correction_timesheet\s+ON correction_timesheet\.timesheet_id = summary_row\.timesheet_id/
  );

  for (const projection of [
    'correction_timesheet.is_current AS correction_is_current',
    'correction_timesheet.archived_at_utc AS correction_archived_at_utc',
    'correction_timesheet.is_adjustment AS correction_is_adjustment',
    'correction_timesheet.parent_timesheet_id AS correction_parent_timesheet_id',
    'correction_timesheet.correction_id',
    'correction_timesheet.adjustment_origin',
    'correction_timesheet.correction_kind',
    'correction_timesheet.candidate_hint_text AS correction_candidate_hint_text'
  ]) {
    assert.ok(definition.includes(projection), `missing correction projection: ${projection}`);
  }

  assert.doesNotMatch(definition, /\bs\.correction_/);
});

test('Bulk Process row patches carry the archived state under an explicit timesheet alias', () => {
  const definition = functionDefinition('bulk_timesheet_row_patch_v1');

  assert.match(
    definition,
    /timesheet_row\.archived_at_utc AS timesheet_archived_at_utc/
  );
  assert.doesNotMatch(definition, /classified_rows\.archived_at_utc/);
  assert.doesNotMatch(definition, /payload_rows\.archived_at_utc/);
  assert.equal(
    (definition.match(/timesheet_archived_at_utc/g) || []).length,
    6,
    'the projection and all five correction checks must share the explicit alias'
  );
});
