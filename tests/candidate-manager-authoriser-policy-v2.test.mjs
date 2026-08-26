import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const sql = readFileSync(new URL('../supabase/repeatable/26082026_0043_candidate_manager_authoriser_policy_v2.sql', import.meta.url), 'utf8');
const worker = readFileSync(new URL('../broker/src/index.js', import.meta.url), 'utf8');

test('manager authoriser policy v2 retains the 63-operation Candidate boundary and uses narrow Office RPCs', () => {
  assert.match(sql, /client_manager_authoriser_policy_update_v1/);
  assert.match(sql, /contract_manager_authoriser_policy_update_v1/);
  assert.doesNotMatch(sql, /update\s+public\.contracts\s+set[\s\S]{0,250}overrideclientsettings/i);
  assert.match(worker, /\/api\/clients\/:id\/manager-authorisers/);
  assert.match(worker, /\/api\/contracts\/:id\/manager-authorisers/);
  assert.doesNotMatch(worker.slice(worker.indexOf('handleClientManagerAuthoriserPolicyUpdate'), worker.indexOf('async function handleUpdateClient')), /overrideclientsettings/);
  assert.doesNotMatch(sql, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});

test('manager authoriser policy v2 closes additive, contract-only, legacy and exact-domain semantics', () => {
  assert.match(sql, /v_mode='EXTEND'/);
  assert.match(sql, /v_mode='CONTRACT_ONLY'/);
  assert.match(sql, /LEGACY_REPLACEMENT/);
  assert.match(sql, /x\.value=v_domain/);
  assert.doesNotMatch(sql, /ends_with|like\s+['"]%/i);
  assert.match(sql, /CANDIDATE_MANAGER_RESTRICTED_POLICY_EMPTY/);
  assert.match(sql, /CANDIDATE_MANAGER_CONTRACT_ONLY_EMPTY/);
});

test('manager authoriser Office routes are admin-authenticated and version aware', () => {
  for (const name of [
    'handleClientManagerAuthoriserPolicyGet','handleClientManagerAuthoriserPolicyUpdate',
    'handleContractManagerAuthoriserPolicyGet','handleContractManagerAuthoriserPolicyUpdate'
  ]) {
    const start = worker.indexOf(`async function ${name}`);
    assert.ok(start >= 0, `${name} missing`);
    const body = worker.slice(start, worker.indexOf('\nasync function ', start + 20));
    assert.match(body, /requireUser\(env, req, \['admin'\]\)/);
  }
  assert.match(worker, /expected_settings_updated_at/);
  assert.match(worker, /expected_contract_updated_at/);
  assert.match(worker, /request_key/);
  assert.match(worker, /Only the manager authoriser policy can be changed here\./);
  assert.match(worker, /Only the Contract manager authoriser policy can be changed here\./);
  assert.match(worker, /Object\.keys\(body\.policy\)\.some/);
});
