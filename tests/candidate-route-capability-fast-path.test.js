import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const sql = fs.readFileSync(path.join(
  root,
  'supabase/repeatable/05092026_1455_candidate_route_capability_fast_path_v1.sql'
), 'utf8');

test('Candidate route fast path keeps the exact private function contract', () => {
  assert.match(sql, /create or replace function private\._candidate_route_family_v1\(\s*p_timesheet_id uuid default null,\s*p_contract_week_id uuid default null\s*\)/i);
  assert.match(sql, /returns jsonb\s+language plpgsql\s+stable\s+security definer\s+set search_path = pg_catalog, public, private, pg_temp/i);
  assert.match(sql, /alter function private\._candidate_route_family_v1\(uuid,uuid\) owner to postgres/i);
  assert.match(sql, /revoke all on function private\._candidate_route_family_v1\(uuid,uuid\)\s+from public,anon,authenticated,service_role/i);
  assert.doesNotMatch(sql, /grant\s+execute[\s\S]+to\s+(?:public|anon|authenticated|service_role)/i);
});

test('Weekly route evaluation reuses canonical facts without reading the full Office summary', () => {
  assert.match(sql, /v_authority:=private\._contract_settings_effective_core_v1\([\s\S]+?'WEEKLY',v_timesheet\.timesheet_id\s*\)/i);
  assert.doesNotMatch(sql, /from\s+public\.v_timesheets_summary/i);
  assert.match(sql, /v_config_import or v_route_import or v_snapshot_import/i);
  assert.match(sql, /when v_config_import then 'CONFIG_'/i);
  assert.match(sql, /when v_route_import then 'ROUTE_'\|\|v_route_type/i);
  assert.match(sql, /when v_fin\.nhsp_import_id is not null then 'NHSP_IMPORT_SNAPSHOT'/i);
  assert.match(sql, /when v_has_external_source_rows then 'EXTERNAL_SOURCE_SNAPSHOT'/i);
  assert.match(sql, /when v_snapshot_import then 'IMPORT_BASIS_SNAPSHOT'/i);
});

test('Daily route evaluation retains the established import-authority resolver', () => {
  assert.match(sql, /if v_is_daily then[\s\S]+?private\._candidate_import_authoritative_v1\(/i);
  assert.match(sql, /v_timesheet\.worked_start_iso,v_timesheet\.scheduled_start_iso,\s*v_timesheet\.week_ending_date/i);
});

test('Every established route and capability output field remains present', () => {
  for (const routeType of [
    'WEEKLY_NHSP_ADJUSTMENT',
    'WEEKLY_HEALTHROSTER_ADJUSTMENT',
    'WEEKLY_MANUAL_ADJUSTMENT',
    'WEEKLY_NHSP',
    'WEEKLY_HEALTHROSTER',
    'WEEKLY_ELECTRONIC',
    'WEEKLY_MANUAL',
    'UNKNOWN'
  ]) assert.match(sql, new RegExp(`'${routeType}'`));

  for (const key of [
    'route_family',
    'effective_submission_mode',
    'pending_route_intent',
    'import_authoritative',
    'import_source_family',
    'qr_backed',
    'electronic_paper_fallback_enabled',
    'candidate_hours_submission_allowed',
    'candidate_expenses_allowed',
    'candidate_paper_submission_allowed',
    'candidate_no_work_allowed',
    'policy'
  ]) assert.match(sql, new RegExp(`'${key}'`));
});

test('Fast path is read-only and preserves deterministic adjustment facts', () => {
  assert.doesNotMatch(sql, /\b(?:insert\s+into|update\s+public\.|delete\s+from|truncate\s+)\b/i);
  assert.match(sql, /v_timesheet\.is_adjustment/i);
  assert.match(sql, /v_week\.is_adjustment/i);
  assert.match(sql, /v_week\.additional_seq/i);
  assert.match(sql, /v_timesheet\.parent_timesheet_id/i);
  assert.match(sql, /v_timesheet\.correction_id/i);
  assert.match(sql, /v_timesheet\.correction_kind/i);
});
