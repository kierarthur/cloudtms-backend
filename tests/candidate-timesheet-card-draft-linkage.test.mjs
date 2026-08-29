import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const sql = readFileSync(new URL('../supabase/repeatable/26082026_1516_candidate_timesheet_card_draft_linkage_v1.sql', import.meta.url), 'utf8');
const verification = readFileSync(new URL('../supabase/verification/26082026_1517_candidate_timesheet_card_draft_linkage_verification.sql', import.meta.url), 'utf8');

test('mutable workflows without a submitted anchor remain attached to the immutable contract-week card', () => {
  assert.match(sql, /classified\.state in \('CREATED','WORKER_DRAFT'\)/i);
  assert.match(sql, /classified\.target_timesheet_id is null/i);
  assert.match(sql, /classified\.anchor_timesheet_id is null/i);
  assert.match(sql, /draft_week\.id=classified\.contract_week_id/i);
  assert.match(sql, /workflows\.display_timesheet_id=coalesce\(base\.timesheet_id,base\.id\)/i);
});

test('timesheet-page replacement preserves the service-only browser boundary', () => {
  assert.match(sql, /security definer/i);
  assert.match(sql, /revoke all on function public\.candidate_app_timesheet_page_v1[\s\S]*from public, anon, authenticated/i);
  assert.match(sql, /grant execute on function public\.candidate_app_timesheet_page_v1[\s\S]*to service_role/i);
  assert.match(verification, /CANDIDATE_TIMESHEET_PAGE_BROWSER_EXECUTE_EXPOSED/);
  assert.match(verification, /CANDIDATE_MUTABLE_DRAFT_WEEK_FALLBACK_MISSING/);
});
