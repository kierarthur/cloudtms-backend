import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const replacement = readFileSync(new URL(
  '../supabase/repeatable/30082026_0454_candidate_daily_emergency_window_row_v1.sql',
  import.meta.url
), 'utf8');
const verifier = readFileSync(new URL(
  '../supabase/verification/30082026_0455_candidate_daily_emergency_window_row_verification.sql',
  import.meta.url
), 'utf8');

test('Daily emergency window assigns the selected row into the matching rowtype', () => {
  assert.match(replacement, /select\s+d\.\*\s+into\s+v_day/i);
  assert.doesNotMatch(replacement, /select\s+d\s+into\s+v_day/i);
  assert.match(replacement, /%rowtype/);
  assert.match(replacement, /revoke all[\s\S]*service_role/i);
  assert.doesNotMatch(replacement, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});

test('Daily emergency window verifier executes private and public first use', () => {
  assert.match(verifier, /_candidate_daily_specialist_shift_v1\s*\(/);
  assert.match(verifier, /candidate_daily_specialist_read_v1\s*\(/);
  assert.match(verifier, /'EMERGENCY_WINDOW'/);
  assert.match(verifier, /rollback;/i);
  assert.match(verifier, /jsonb_array_length\(v_result->'shifts'\)<>1/);
});
