import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';

const authority = readFileSync(
  new URL('../../supabase/repeatable/15092026_2203_weekly_source_candidate_app_contract_v1.sql', import.meta.url),
  'utf8',
);
const verifier = readFileSync(
  new URL('../../supabase/verification/15092026_2203_weekly_source_candidate_app_contract_v1.sql', import.meta.url),
  'utf8',
);

test('a provisional replacement rechecks older signed weeks without treating final backing rows as competing provisional events', () => {
  const historicalFallback = authority.match(/with prior_events as \([\s\S]*?\), nearest as \(/)?.[0];
  assert.ok(historicalFallback, 'historical missing-shift fallback must exist');
  assert.match(historicalFallback, /historical_upload\.source_format_profile_id=v_upload\.source_format_profile_id/);

  const prefinalRecheck = authority.match(/create or replace function private\.weekly_source_candidate_prefinal_publish_recheck_v1\([\s\S]*?end;\s*\$function\$;/)?.[0];
  assert.ok(prefinalRecheck, 'provisional publication recheck must exist');
  assert.match(prefinalRecheck, /historical_upload\.source_format_profile_id=v_profile_id/);
  assert.match(prefinalRecheck, /source_row\.work_date between sheet\.week_ending_date-6/);
  assert.match(prefinalRecheck, /source_row\.work_date between\s+v_upload\.confirmed_coverage_start_local_date\s+and v_upload\.confirmed_coverage_end_local_date/);

  const compare = authority.match(/create or replace function private\.weekly_source_candidate_submission_compare_sync_v1\([\s\S]*?end;\s*\$function\$;/)?.[0];
  assert.ok(compare, 'candidate/source comparison must exist');
  assert.match(compare, /candidate_row\.value->>'date'\)::date between\s+v_upload\.confirmed_coverage_start_local_date/);
  assert.match(compare, /work_event\.work_date between v_upload\.confirmed_coverage_start_local_date\s+and v_upload\.confirmed_coverage_end_local_date/);

  assert.match(verifier, /same-shift-final-backing\.xlsx/);
  assert.match(verifier, /final-backing profile fixture is unavailable/);
  assert.match(verifier, /omitted Previously Released shifts did not become Office-visible queries/);
  assert.match(verifier, /one-day September export rechecked a signed August week outside its range/);
  assert.match(verifier, /earlier open August query disappeared after a one-day September export/);
});
