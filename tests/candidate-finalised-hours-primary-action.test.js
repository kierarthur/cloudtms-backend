import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';

const source = fs.readFileSync(new URL(
  '../supabase/repeatable/02092026_1918_candidate_finalised_hours_primary_action_v1.sql',
  import.meta.url,
), 'utf8');
const verification = fs.readFileSync(new URL(
  '../supabase/verification/02092026_1920_candidate_finalised_hours_primary_action_verification.sql',
  import.meta.url,
), 'utf8');

test('finalised submitted hours cannot fall through to Enter Timesheet', () => {
  assert.match(source, /v_has_finalised_hours[\s\S]*state'='FINALISED'[\s\S]*CONTRACT_HOURS'[\s\S]*CONTRACT_COMBINED'[\s\S]*DAILY'/i);
  assert.match(source, /if v_has_finalised_hours then[\s\S]*can_edit_expenses'[\s\S]*'ADD_EXPENSES'[\s\S]*return null/i);
  assert.match(verification, /AWAITING_MANUAL_SIGNATURE[\s\S]*CONTRACT_COMBINED[\s\S]*FINALISED[\s\S]*ADD_EXPENSES/i);
  assert.match(verification, /CONTRACT_EXPENSE[\s\S]*FINALISED[\s\S]*ENTER_TIMESHEET/i);
  assert.doesNotMatch(source, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});
