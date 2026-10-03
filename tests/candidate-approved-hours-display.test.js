import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const sql = readFileSync(new URL('../supabase/repeatable/03102026_1540_candidate_approved_hours_visible.sql', import.meta.url), 'utf8');
const broker = readFileSync(new URL('../broker/src/candidate-app-backend.js', import.meta.url), 'utf8');

test('Candidate cards certify approved hours from the committed entitlement, not the financial or submitted total', () => {
  assert.match(sql, /create or replace function private\.weekly_source_candidate_list_approval_v1\(/i);
  assert.match(sql, /private\.weekly_source_candidate_approved_entitlement_v1\(v_context\)/i);
  assert.match(sql, /v_entitlement->>'authority' is distinct from 'HEAD'/i);
  assert.match(sql, /v_entitlement->>'total_hours'/i);
  assert.match(sql, /'state','NOT_PROCESSED','total_hours',null/i);
  assert.match(sql, /'state','UNAVAILABLE','total_hours',null/i);
  assert.match(sql, /if v_item->>'route_family'='IMPORT_AUTHORITATIVE' then/i);
  assert.match(broker, /rpcCall\(deps, 'candidate_app_timesheet_page_v2'/);
  assert.doesNotMatch(sql, /v_item->>'total_hours'/i);
});

test('Candidate detail restores matching approved rows and the public page remains service-only', () => {
  assert.match(sql, /create or replace function private\.weekly_source_candidate_view_merge_v1\(/i);
  assert.match(sql, /jsonb_set\(v_view,'\{approved_hours_to_be_paid\}'/i);
  assert.match(sql, /'weekly_source_approved_hours_state',v_approval->'state'/i);
  assert.match(sql, /'weekly_source_approved_total_hours',v_approval->'total_hours'/i);
  assert.match(sql, /revoke all on function public\.candidate_app_timesheet_page_v2\([^;]+from public,anon,authenticated;/is);
  assert.match(sql, /grant execute on function public\.candidate_app_timesheet_page_v2\([^;]+to service_role;/is);
  assert.match(sql, /notify pgrst, 'reload schema';/i);
});
