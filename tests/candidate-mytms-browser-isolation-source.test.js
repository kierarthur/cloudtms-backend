import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';

const read = (relativePath) => fs.readFileSync(new URL(`../${relativePath}`, import.meta.url), 'utf8');
const migration = read('supabase/migrations/22082026_0951_candidate_mytms_browser_isolation.sql');
const verification = read('supabase/verification/22082026_0952_candidate_mytms_browser_isolation_verification.sql');
const candidateBackend = read('broker/src/candidate-app-backend.js');
const officeControl = read('broker/src/mytms-office-control.js');

const sharedRelations = [
  'candidate_job_titles', 'candidates', 'client_settings', 'clients',
  'contract_weeks', 'contracts', 'mail_outbox', 'settings_defaults',
  'timesheet_evidence', 'timesheets', 'timesheets_financials'
];

test('scoped Candidate and MyTMS transitive relations are RLS-enabled and browser-denied', () => {
  for (const relation of sharedRelations) {
    assert.match(migration, new RegExp(`alter table public\\.${relation} enable row level security`, 'i'));
    assert.match(verification, new RegExp(`'${relation}'`));
  }
  assert.match(migration, /revoke all on table[\s\S]*from public, anon, authenticated/i);
  assert.match(migration, /alter view public\.candidate_activity_rollup set \(security_invoker=true\)/i);
  assert.match(verification, /security_invoker=true/i);
});

test('legacy browser-callable Candidate helpers become Worker-only', () => {
  for (const signature of [
    'candidate_delete_apply', 'candidate_delete_eligibility',
    'candidate_list_ids', 'candidate_picker_search'
  ]) {
    assert.match(migration, new RegExp(`revoke all on function public\\.${signature}`, 'i'));
    assert.match(migration, new RegExp(`grant execute on function public\\.${signature}[\\s\\S]*to service_role`, 'i'));
  }
  assert.match(migration, /candidate_picker_search\(text,integer,integer,boolean\)[\s\S]*set search_path to pg_catalog, public/i);
});

test('Candidate and Office data-plane calls remain service-role mediated', () => {
  assert.match(candidateBackend, /function serviceHeaders\(env, extras = \{\}\)[\s\S]*SUPABASE_SERVICE_ROLE_KEY/);
  assert.match(candidateBackend, /rest\/v1\/\$\{table\}[\s\S]*serviceHeaders\(env/);
  assert.match(officeControl, /function supabaseHeaders\(env, prefer = ''\)[\s\S]*SUPABASE_SERVICE_ROLE_KEY/);
  assert.match(officeControl, /rest\/v1\/candidates[\s\S]*supabaseHeaders\(env\)/);
});

test('the scoped migration does not touch Banking Pay or payment economics', () => {
  assert.doesNotMatch(migration, /banking|pay_workbench|payment|settlement|remittance|invoice_financial/i);
});
