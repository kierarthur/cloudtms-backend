import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const read = path => readFileSync(new URL('../../' + path, import.meta.url), 'utf8');
const seed = read('supabase/verification/05102026_0552_weekly_source_invoice_isolation_fixture_v1.sql');
const integration = read('supabase/verification/15092026_1534_weekly_source_invoice_batch_integration_v1.sql');
const release = JSON.parse(read('supabase/release/current-release.json'));

test('invoice first-preparation assertions scope only the exact owned contract and source cycle', () => {
  assert.doesNotMatch(seed, /not exists\(select 1 from public\.weekly_source_root_authorisations\)/);
  assert.doesNotMatch(seed, /not exists\(select 1 from public\.timesheets where authorised_at_server is not null\)/);
  assert.doesNotMatch(seed, /not exists\(select 1 from public\.weekly_exceptional_pay_target_families\)/);
  assert.doesNotMatch(seed, /count\(\*\)=1 from public\.timesheets_financials where is_current/);
  assert.match(seed, /authorisation\.root_timesheet_id[\s\S]*?root\.contract_id='a0000000-0000-4000-8000-000000000004'/);
  assert.match(seed, /financial\.timesheet_id[\s\S]*?root\.contract_id='a0000000-0000-4000-8000-000000000004' and financial\.is_current/);
  assert.equal((seed.match(/receipt\.final_revision_id=\(select id from public\.weekly_source_final_revisions/g) || []).length, 2);
});

test('the fixture captures pre-existing rows and preserves their complete non-target fingerprints', () => {
  assert.ok(seed.indexOf('source_workbench_fixture_isolation.sql') < seed.indexOf('insert into public.settings_defaults'));
  assert.match(seed, /INVOICE_FIXTURE_NAMESPACE_COLLISION/);
  for (const relation of ['timesheets', 'timesheets_financials', 'weekly_source_root_authorisations', 'weekly_exceptional_pay_target_families', 'invoices', 'invoice_lines']) {
    assert.ok(seed.includes("'public." + relation + "'::regclass"), relation);
  }
  assert.match(seed, /ws_verify_keys own/);
  assert.match(seed, /invoice_fixture_nontarget_before/);
  assert.ok(integration.indexOf('invoice_fixture_nontarget_fingerprint()=') > integration.indexOf('$wp27_rotation$;'));
});

test('rotation, ordinary positive control and protected-family negatives remain mandatory', () => {
  assert.ok(release.verificationFiles.includes('supabase/verification/15092026_1534_weekly_source_invoice_batch_integration_v1.sql'));
  for (const code of ['WP-27 F4:', 'WP-27 F5:', 'WP-27 rotation case: the ordinary control Timesheet is not offered', 'WP-27 protected-only exclusion changed the unrelated ordinary positive control']) {
    assert.ok(integration.includes(code), code);
  }
  assert.match(integration, /rollback;/);
});
