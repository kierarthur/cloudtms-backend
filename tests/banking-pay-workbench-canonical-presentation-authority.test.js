import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';

const read = (path) => fs.readFileSync(path, 'utf8');
const canonical = read('supabase/repeatable/05082026_1545_pay_preview_candidate_build_canonical_lines.sql');
const authority = read('supabase/repeatable/08082026_0354_pay_preview_candidate_build_canonical_lines_authority.sql');

test('late authority restores the complete protected canonical presentation graph', () => {
  assert.match(authority, /\\ir 05082026_1545_pay_preview_candidate_build_canonical_lines\.sql/);
  assert.match(canonical, /Protected presentation authority is built for every authoritative/);
  assert.doesNotMatch(
    canonical,
    /where round\(coalesce\(tcr\.payment_amount_ex_vat,0\),2\) <> 0/i,
  );
  assert.doesNotMatch(
    canonical,
    /and not \(ats\.snooze_id is not null and ats\.snooze_until_date is null\)/i,
  );
});

test('public eligibility remains downstream of protected state construction', () => {
  assert.match(canonical, /create temporary table canonical_timesheet_presentation_state on commit drop as/i);
  assert.match(canonical, /create temporary table canonical_timesheet_presentation_rows on commit drop as/i);
  assert.match(canonical, /and ctpp\.has_ready_presentation = true/i);
});
