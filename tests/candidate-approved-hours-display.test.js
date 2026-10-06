import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const sql = readFileSync(new URL('../supabase/repeatable/03102026_1540_candidate_approved_hours_visible.sql', import.meta.url), 'utf8');
const broker = readFileSync(new URL('../broker/src/candidate-app-backend.js', import.meta.url), 'utf8');
const saved = readFileSync(new URL('../supabase/repeatable/04102026_2338_weekly_source_candidate_saved_local_hours_v2.sql', import.meta.url), 'utf8');
const producer = readFileSync(new URL('../supabase/repeatable/17092026_1100_weekly_source_candidate_view_producer_v1.sql', import.meta.url), 'utf8');
const clock = readFileSync(new URL('../supabase/repeatable/05102026_0100_weekly_source_candidate_approved_clock_row_v2.sql', import.meta.url), 'utf8');
const initial = readFileSync(new URL('../supabase/repeatable/04102026_2253_weekly_source_candidate_initial_approval_v2.sql', import.meta.url), 'utf8');
const head = readFileSync(new URL('../supabase/repeatable/04102026_2317_weekly_source_candidate_head_hours_v2.sql', import.meta.url), 'utf8');

test('all certified display bases use exact retained UK instants and native rounded bucket minutes without repricing', () => {
  for (const reader of [initial, head, saved]) {
    assert.match(reader, /private\.weekly_source_candidate_approved_clock_row_v2\(/);
    assert.doesNotMatch(reader, /round\(extract\(epoch from \(v_end-v_start\)\)/);
  }
  assert.match(clock, /v_start_utc at time zone 'Europe\/London'/);
  assert.match(clock, /round\(v_bucket_minutes::numeric\/60,2\)/);
  assert.match(clock, /round\(v_bucket_minutes::numeric\/60,6\)/);
  assert.match(clock, /v_paid_minutes is distinct from \(v_elapsed-v_break\)::integer/);
  assert.match(clock, /from public,anon,authenticated,service_role;/);
  assert.doesNotMatch(clock, /\b(?:insert into|update|delete from|grant execute)\b/i);
});

test('Candidate cards use only positively certified approved bases, not arbitrary financial or submitted totals', () => {
  assert.match(sql, /create or replace function private\.weekly_source_candidate_list_approval_v1\(/i);
  assert.match(sql, /private\.weekly_source_candidate_approved_entitlement_v1\(v_context\)/i);
  assert.match(sql, /coalesce\(v_entitlement->>'authority',''\) not in\s*\('HEAD','INITIAL_AUTHORISED_TSFIN_V1','SAVED_UNAUTHORISED_LOCAL_V1'\)/i);
  assert.match(sql, /v_entitlement->>'total_hours'/i);
  assert.match(sql, /'state','NOT_PROCESSED','total_hours',null/i);
  assert.match(sql, /'state','UNAVAILABLE','total_hours',null/i);
  assert.match(sql, /if v_item->>'route_family'='IMPORT_AUTHORITATIVE' then/i);
  assert.match(broker, /rpcCall\(deps, 'candidate_app_timesheet_page_v2'/);
  assert.doesNotMatch(sql, /v_item->>'total_hours'/i);
});

test('saved local hours require a completed sealed decision, remain owner-only and do not imply first authorisation', () => {
  assert.match(saved, /v_receipt\.state<>'COMPLETE'/);
  assert.match(saved, /v_generation\.lifecycle_state<>'PUBLISHED'/);
  assert.match(saved, /'WEEKLY_SOURCE_SAVED_UNAUTHORISED_FINANCIAL_V1',to_jsonb\(v_fin\)/);
  assert.match(saved, /'WEEKLY_SOURCE_SAVED_UNAUTHORISED_DETAIL_V1'/);
  assert.match(saved, /v_fin\.authorised_at_utc is not null or v_fin\.processing_status<>'PENDING_AUTH'/);
  assert.match(saved, /v_root\.authorised_at_server is not null/);
  assert.match(saved, /from public,anon,authenticated,service_role;/);
  assert.doesNotMatch(saved, /\b(?:insert into|update|delete from|grant execute)\b/i);
  assert.match(producer, /if p_context->>'authority_mode'='SOURCE_AUTHORITY' then\s*return private\.weekly_source_candidate_saved_local_hours_v2\(/);
  assert.match(sql, /'HEAD','INITIAL_AUTHORISED_TSFIN_V1','SAVED_UNAUTHORISED_LOCAL_V1'/);
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
