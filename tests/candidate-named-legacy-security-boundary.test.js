import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const read = (relative) => readFileSync(new URL(`../${relative}`, import.meta.url), 'utf8');
const migration = read('supabase/migrations/22082026_1402_candidate_named_legacy_relation_isolation.sql');
const repeatable = read('supabase/repeatable/22082026_1402_candidate_named_security_definer_browser_isolation.sql');
const verification = read('supabase/verification/22082026_1402_candidate_named_legacy_security_verification.sql');
const historicalVerificationV2 = read('supabase/verification/23082026_0400_candidate_named_security_verification_v2.sql');
const currentVerification = read('supabase/verification/27082026_1947_candidate_named_security_verification_v3.sql');
const workflow = read('.github/workflows/supabase-migrate.yml');
const currentRelease = read('supabase/release/current-release.json');

const relations = [
  'banking_pay_snapshot_candidate_state',
  'banking_pay_workbench_session_candidate_state',
  'legacy_eclipse_candidate_map',
  'pay_batch_candidates',
  'rates_candidate_overrides',
  'candidates_tombstones',
  'v_legacy_candidate_contract_summary',
  'v_legacy_client_candidates',
  'v_legacy_contracts_by_candidate',
  'candidates_summary',
  'candidates_summary_activity',
  'v_mailshot_src_candidate',
  'vw_picker_candidates'
];

test('the residual Advisor-error relation set is exact and browser-isolated only', () => {
  for (const relation of relations) {
    assert.match(migration, new RegExp(`'${relation}'`));
    assert.match(verification, new RegExp(`'${relation}'`));
  }
  assert.match(migration, /alter table %I\.%I enable row level security/i);
  assert.match(migration, /alter view %I\.%I set \(security_invoker=true\)/i);
  assert.match(migration, /from PUBLIC, anon, authenticated/i);
  assert.doesNotMatch(migration, /\b(?:insert into|update public\.|delete from|drop table|truncate table)\b/i);
});

test('Candidate-named SECURITY DEFINER functions are reclosed after earlier repeatables', () => {
  assert.match(repeatable, /p\.prosecdef and p\.proname ilike '%candidate%'/i);
  assert.match(repeatable, /v_count=85 and v_service_missing=7 and v_browser_executable=34/i);
  assert.match(repeatable, /v_hash='92b080451840f5ab3940fb540907d466'/i);
  assert.match(repeatable, /v_browser_executable=0/i);
  assert.match(repeatable, /v_hash='1058d64351f6e5cbbe572564d7c89b28'/i);
  assert.match(repeatable, /revoke all privileges on function %s from PUBLIC, anon, authenticated/i);
  assert.doesNotMatch(repeatable, /create\s+(?:or\s+replace\s+)?function/i);
});

test('the exact security verifier is mandatory after every migration workflow', () => {
  const general = workflow.indexOf('22082026_1302_general_browser_isolation_verification.sql');
  const protectedCandidateNamed = workflow.indexOf('22082026_1402_candidate_named_legacy_security_verification.sql');
  const historicalCandidateNamed = workflow.indexOf('23082026_0400_candidate_named_security_verification_v2.sql');
  const candidateNamed = workflow.indexOf('27082026_1947_candidate_named_security_verification_v3.sql');
  const candidateApp = workflow.indexOf('22082026_0952_candidate_mytms_browser_isolation_verification.sql');
  assert.ok(general >= 0);
  assert.ok(protectedCandidateNamed > general);
  assert.ok(historicalCandidateNamed > protectedCandidateNamed);
  assert.ok(candidateNamed > historicalCandidateNamed);
  assert.ok(candidateApp > protectedCandidateNamed);
  assert.ok(candidateApp > candidateNamed);
  assert.match(verification, /cloudtms_data_api_mfa_gate/);
  assert.match(historicalVerificationV2, /v_count<>105/i);
  assert.match(historicalVerificationV2, /4166c08c7abd5e9ed638091c182ce2e5/i);
  assert.match(currentVerification, /v_count<>111/i);
  assert.match(currentVerification, /v_service_missing<>8/i);
  assert.match(currentVerification, /v_browser_executable<>0/i);
  assert.match(currentVerification, /25ff5272e1f5dfc36a0fcf7c0c381a03/i);
  assert.match(currentRelease, /27082026_1947_candidate_named_security_verification_v3\.sql/);
  assert.doesNotMatch(currentRelease, /23082026_0400_candidate_named_security_verification_v2\.sql/);
  assert.doesNotMatch(currentRelease, /22082026_1402_candidate_named_legacy_security_verification\.sql/);
});
