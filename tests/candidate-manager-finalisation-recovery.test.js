import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import { candidateAppBackendInternals } from '../broker/src/candidate-app-backend.js';

const { withManagerFinalisationLease } = candidateAppBackendInternals;

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const weeklySql = fs.readFileSync(path.join(root,
  'supabase/repeatable/27082026_2205_candidate_weekly_manager_finalisation_authority_v1.sql'), 'utf8');
const recoverySql = fs.readFileSync(path.join(root,
  'supabase/repeatable/27082026_2207_candidate_manager_finalisation_recovery_v1.sql'), 'utf8');
const lockBudgetSql = fs.readFileSync(path.join(root,
  'supabase/repeatable/28082026_1925_candidate_daily_receipt_finalisation_v1.sql'), 'utf8');
const backendSource = fs.readFileSync(path.join(root, 'broker/src/candidate-app-backend.js'), 'utf8');
const privateWorkerSource = fs.readFileSync(path.join(root, 'broker/src/candidate-private-worker.js'), 'utf8');

test('trusted weekly finalisation remains route-checked without relying on Candidate edit capability', () => {
  assert.match(weeklySql, /current_setting\('cloudtms\.candidate_electronic_finalise'/);
  assert.match(weeklySql, /_candidate_route_family_v1/);
  assert.match(weeklySql, /IMPORT_AUTHORITATIVE','MANUAL_NON_QR/);
  assert.doesNotMatch(weeklySql, /v_candidate_capability_guard/);
  assert.doesNotMatch(weeklySql, /can_edit_hours/);
  assert.match(weeklySql, /revoke all[\s\S]*from public,anon,authenticated/i);
  assert.match(weeklySql, /grant execute[\s\S]*to service_role/i);
  assert.doesNotMatch(weeklySql, /grant execute[\s\S]*to (?:anon|authenticated)/i);
});

test('manager finalisation recovery contract is service-only and fail-closed', () => {
  assert.match(recoverySql, /security definer/i);
  assert.match(recoverySql, /MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE/);
  assert.match(recoverySql, /route not in \('PHONE','EMAIL'\)/i);
  assert.match(recoverySql, /v_approval_count<>1/);
  assert.match(recoverySql, /_candidate_render_contract_v1\([\s\S]*'FINAL_SIGNED'/);
  assert.match(recoverySql, /revoke all[\s\S]*from public,anon,authenticated/i);
  assert.match(recoverySql, /grant execute[\s\S]*to service_role/i);
  assert.doesNotMatch(recoverySql, /grant execute[\s\S]*to (?:anon|authenticated)/i);
  assert.match(recoverySql, /notify pgrst, 'reload schema'/i);
});

test('manager finalisation has bounded database lock and statement waits', () => {
  assert.match(lockBudgetSql, /candidate_submission_finalize_atomic_v1/);
  assert.match(lockBudgetSql, /set lock_timeout = '5s'/i);
  assert.match(lockBudgetSql, /set statement_timeout = '120s'/i);
  assert.match(lockBudgetSql, /revoke all[\s\S]*from public,anon,authenticated/i);
  assert.match(lockBudgetSql, /grant execute[\s\S]*to service_role/i);
});

test('private scheduler retries only bounded manager finalisations through the service contract', () => {
  assert.match(backendSource, /recoverPendingCandidateManagerFinalisations\(env, deps, limit = 5\)/);
  assert.match(backendSource, /Math\.max\(1, Math\.min\(Number\(limit\) \|\| 5, 10\)\)/);
  assert.match(backendSource, /state=in\.\(MANAGER_APPROVED_PENDING_FINAL_DOCUMENT,READY_TO_FINALISE\)/);
  assert.match(backendSource, /candidate_manager_finalisation_recovery_v1/);
  assert.match(backendSource, /withManagerFinalisationLease\(/);
  assert.match(backendSource, /await renderAndRegister\(env, deps, recovery\?\.final_render_contract, 'FINAL'\)/);
  assert.match(backendSource, /candidate-system-finalise-recovery:/);
  assert.match(privateWorkerSource, /ctx\.waitUntil\(recoverPendingCandidateManagerFinalisations\([\s\S]*,\s*5\s*\)\)/);
});

function finalisationLeaseBucket() {
  let object = null;
  let generation = 0;
  return {
    async head() { return object; },
    async put(key, _bytes, options) {
      const condition = options?.onlyIf || {};
      if (condition.etagDoesNotMatch === '*' && object) return null;
      if (condition.etagMatches && object?.etag !== condition.etagMatches) return null;
      generation += 1;
      object = {
        key,
        etag: `lease-${generation}`,
        customMetadata: { ...options.customMetadata }
      };
      return object;
    },
    async delete(key) {
      if (object?.key === key) object = null;
    }
  };
}

test('manager finalisation is single-flight across approval and scheduled recovery', async () => {
  const env = { CANDIDATE_APP_ENVIRONMENT: 'TEST', R2: finalisationLeaseBucket() };
  const workflowId = '00000000-0000-4000-8000-000000000901';
  let releaseWork;
  let startedWork;
  const started = new Promise(resolve => { startedWork = resolve; });
  const first = withManagerFinalisationLease(env, workflowId, 2, async () => {
    startedWork();
    await new Promise(resolve => { releaseWork = resolve; });
    return { ok: true, state: 'FINALISED' };
  });
  await started;
  const second = await withManagerFinalisationLease(env, workflowId, 2, async () => {
    throw new Error('competing finaliser must not run');
  });
  assert.equal(second.single_flight_deferred, true);
  assert.equal(second.finalisation_pending, true);
  releaseWork();
  assert.deepEqual(await first, { ok: true, state: 'FINALISED' });
});

test('a failed finaliser retains its short lease before recovery retries', async () => {
  const env = { CANDIDATE_APP_ENVIRONMENT: 'TEST', R2: finalisationLeaseBucket() };
  const workflowId = '00000000-0000-4000-8000-000000000902';
  await assert.rejects(
    withManagerFinalisationLease(env, workflowId, 3, async () => {
      throw new Error('simulated disconnected database call');
    }),
    /simulated disconnected database call/
  );
  const recovery = await withManagerFinalisationLease(env, workflowId, 3, async () => {
    throw new Error('recovery must wait for the failed call lease');
  });
  assert.equal(recovery.single_flight_deferred, true);
  assert.equal(recovery.finalisation_pending, true);
});
