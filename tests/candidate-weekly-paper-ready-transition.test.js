import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';

const read = (path) => fs.readFileSync(new URL(`../${path}`, import.meta.url), 'utf8');
const sql = read('supabase/repeatable/30082026_0640_candidate_weekly_paper_prepare_state_adapter_v1.sql');
const backend = read('broker/src/candidate-app-backend.js');
const verification = read('supabase/verification/30082026_0605_candidate_weekly_paper_target_prepare_verification.sql');
const release = read('supabase/release/current-release.json');
const contract = JSON.parse(read('supabase/release/current-contract.json'));

test('weekly PAPER adapter accepts only the exact submitted and ready states', () => {
  assert.match(sql, /v_workflow\.state='READY_FOR_MANAGER_APPROVAL'/i);
  assert.match(sql, /set state='WORKER_SUBMITTED'/i);
  assert.match(sql, /state not in \('WORKER_SUBMITTED','AWAITING_PAPER_RETURN'\)/i);
  assert.match(sql, /scope<>'WEEKLY'/i);
  assert.match(sql, /workflow_kind not in \([\s\S]*'CONTRACT_HOURS'[\s\S]*'CONTRACT_COMBINED'[\s\S]*'CONTRACT_EXPENSE'/i);
  assert.match(sql, /set qr_status='PENDING'/i);
  assert.match(sql, /qr_status::text,''\)\) not in \('','PENDING'\)/i);
  assert.doesNotMatch(sql, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});

test('state adaptation and authoritative PAPER transition share one transaction', () => {
  assert.match(sql, /update public\.candidate_submission_workflows[\s\S]*set state='WORKER_SUBMITTED'/i);
  assert.match(sql, /return public\.candidate_workflow_transition_atomic_v1\([\s\S]*p_action=>'PAPER_PREPARE'/i);
  assert.ok(sql.indexOf('pg_advisory_xact_lock') < sql.indexOf('from public.timesheets timesheet_row'));
  assert.ok(sql.indexOf('from public.timesheets timesheet_row') < sql.lastIndexOf('from public.candidate_submission_workflows workflow_row'));
  assert.doesNotMatch(sql, /exception when others/i);
});

test('PAPER dispatch uses the bounded adapter without changing other actions', () => {
  assert.match(backend, /dbAction === 'PAPER_PREPARE'[\s\S]*'candidate_weekly_paper_prepare_atomic_v1'[\s\S]*'candidate_workflow_transition_atomic_v1'/i);
});

test('weekly PAPER adapter remains service-only', () => {
  assert.match(sql, /revoke all on function public\.candidate_weekly_paper_prepare_atomic_v1[\s\S]*from public,anon,authenticated/i);
  assert.match(sql, /grant execute on function public\.candidate_weekly_paper_prepare_atomic_v1[\s\S]*to service_role/i);
});

test('mandatory rollback proof executes ready-state transition and exact replay', () => {
  assert.match(verification, /'READY_FOR_MANAGER_APPROVAL'/i);
  assert.match(verification, /candidate_weekly_paper_prepare_atomic_v1\(/i);
  assert.match(verification, /AWAITING_PAPER_RETURN/i);
  assert.match(verification, /count\(\*\) from public\.mail_outbox/i);
  assert.match(release, /30082026_0605_candidate_weekly_paper_target_prepare_verification\.sql/i);
});

test('generated contract contains the exact PAPER adapter authority', () => {
  const routine = contract.routines.find((item) => item.identity.startsWith(
    'candidate_weekly_paper_prepare_atomic_v1('
  ));
  assert.ok(routine);
  assert.equal(routine.owner, 'postgres');
  assert.deepEqual(routine.acl.map((item) => item.grantee), ['postgres', 'service_role']);
  assert.equal(
    routine.definition_sha256,
    'c9a3a73845d0ad85a443c332a3cfbd617ab4bbc2926678f824299bf68a1fa291'
  );
});
