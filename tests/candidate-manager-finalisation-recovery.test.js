import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const weeklySql = fs.readFileSync(path.join(root,
  'supabase/repeatable/27082026_2205_candidate_weekly_manager_finalisation_authority_v1.sql'), 'utf8');
const recoverySql = fs.readFileSync(path.join(root,
  'supabase/repeatable/27082026_2207_candidate_manager_finalisation_recovery_v1.sql'), 'utf8');
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

test('private scheduler retries only bounded manager finalisations through the service contract', () => {
  assert.match(backendSource, /recoverPendingCandidateManagerFinalisations\(env, deps, limit = 5\)/);
  assert.match(backendSource, /Math\.max\(1, Math\.min\(Number\(limit\) \|\| 5, 10\)\)/);
  assert.match(backendSource, /state=in\.\(MANAGER_APPROVED_PENDING_FINAL_DOCUMENT,READY_TO_FINALISE\)/);
  assert.match(backendSource, /candidate_manager_finalisation_recovery_v1/);
  assert.match(backendSource, /await renderAndRegister\(env, deps, recovery\?\.final_render_contract, 'FINAL'\)/);
  assert.match(backendSource, /candidate-system-finalise-recovery:/);
  assert.match(privateWorkerSource, /ctx\.waitUntil\(recoverPendingCandidateManagerFinalisations\([\s\S]*,\s*5\s*\)\)/);
});
