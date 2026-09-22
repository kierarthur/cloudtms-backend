import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const bridge = readFileSync(
  new URL('../supabase/repeatable/22092026_2210_settings_service_view_bridge_v1.sql', import.meta.url),
  'utf8',
);

test('service-only settings views keep private authority behind exact wrappers', () => {
  assert.match(bridge, /create or replace function public\.timesheet_settings_authority_frozen_get_v1/);
  assert.match(bridge, /security definer/);
  assert.match(bridge, /revoke all on function public\.timesheet_settings_authority_frozen_get_v1\(uuid\)[\s\S]*from public,anon,authenticated/);
  assert.match(bridge, /grant execute on function public\.timesheet_settings_authority_frozen_get_v1\(uuid\)[\s\S]*to service_role/);
  assert.match(bridge, /\('public','v_timesheets_summary_base',2,0\)/);
  assert.match(bridge, /\('public','v_ts_invoice_precheck',1,1\)/);
  assert.match(bridge, /SETTINGS_SERVICE_VIEW_BRIDGE_SOURCE_DRIFT/);
  assert.match(bridge, /private\._contract_settings_effective_core_v1'[\s\S]*public\.contract_settings_effective_get_v1'/);
  assert.match(bridge, /private\._timesheet_settings_authority_frozen_v1'[\s\S]*public\.timesheet_settings_authority_frozen_get_v1'/);
  assert.match(bridge, /create or replace view %I\.%I with \(security_invoker=true\)/);
  assert.doesNotMatch(bridge, /grant execute on function private\._contract_settings_effective_core_v1/i);
  assert.doesNotMatch(bridge, /grant execute on function private\._timesheet_settings_authority_frozen_v1/i);
});
