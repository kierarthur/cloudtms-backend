import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';

import { candidatePrivateWorkerInternals } from '../broker/src/candidate-private-worker.js';

const sql = fs.readFileSync(path.join(process.cwd(), 'supabase', 'repeatable',
  '26082026_1607_candidate_daily_membership_ready_activation_v1.sql'), 'utf8');
const workerSource = fs.readFileSync(path.join(process.cwd(), 'broker', 'src',
  'candidate-private-worker.js'), 'utf8');

const context = Object.freeze({
  environment: 'TEST',
  agency_candidate_id: '00000000-0000-4000-8000-000000000101',
  membership_id: '00000000-0000-4000-8000-000000000102',
  membership_generation: 3
});

function request() {
  return new Request('https://candidate-private.test.example/private/candidate-app/v1/bootstrap');
}

test('membership-ready SQL accepts lower-case CID1 without rewriting Candidate identity', () => {
  assert.match(sql,
    /upper\(btrim\(coalesce\(v_candidate\.key_norm,''\)\)\)!~'\^CID1-/i);
  assert.doesNotMatch(sql, /update\s+public\.candidates[\s\S]*key_norm/i);
});

test('Google Candidate publication stays immediate while membership may use the current complete window', () => {
  assert.match(sql,
    /not v_membership_activation[\s\S]*published_at_utc<pg_catalog\.clock_timestamp\(\)-interval '120 seconds'/i);
  assert.match(sql,
    /v_membership_activation[\s\S]*window_start<>\(pg_catalog\.clock_timestamp\(\) at time zone 'Europe\/London'\)::date[\s\S]*window_end<>/i);
  assert.match(sql, /expected_day_count<>14[\s\S]*actual_day_count<>14[\s\S]*v_day_count<>14/i);
  assert.doesNotMatch(sql, /availability_(?:value|code)[\s\S]{0,80}(?:is not null|<>)/i);
});

test('membership freshness exception is bound to one exact active membership and the central transition', () => {
  assert.match(sql,
    /candidate_app_global_membership_links[\s\S]*membership_id=v_membership_id[\s\S]*candidate_id=v_candidate_id[\s\S]*membership_generation=v_membership_generation[\s\S]*state='ACTIVE'/i);
  assert.match(sql, /candidate_daily_authority_transition_atomic_v1\s*\(/i);
  assert.match(sql,
    /candidate_daily_authority_transition_atomic_v1\s*\(\s*p_internal_context\|\|/i);
  assert.match(sql,
    /'activation_reason',[\s\S]*'FEDERATED_MEMBERSHIP_ACTIVE'[\s\S]*'membership_id',v_membership_id[\s\S]*'membership_generation',v_membership_generation/i);
  assert.match(sql, /notify pgrst, 'reload schema'/i);
});

test('private membership projection attempts Rota activation after membership link and before session projection', () => {
  const linked = workerSource.indexOf("candidate_app_federated_membership_link_set_v1");
  const activated = workerSource.indexOf('await attemptFederatedDailyActivation');
  const projected = workerSource.indexOf("candidate_app_federated_session_project_v1");
  assert.ok(linked >= 0 && linked < activated && activated < projected);
});

test('a renewed federated projection always mints a genuinely fresh local access token', async () => {
  const historicalIssuedAt = '2026-08-21T16:00:00.000Z';
  const startedAtSeconds = Math.floor(Date.now() / 1000);
  const routedRequest = await candidatePrivateWorkerInternals.projectFederatedRequest(
    request(), {
      CANDIDATE_APP_ENVIRONMENT: 'TEST',
      CANDIDATE_FEDERATED_IDENTITY_SECRET: 'test-federated-identity-secret',
      CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'test-private-session-secret'
    }, {
      context: {
        ...context,
        global_account_id: '00000000-0000-4000-8000-000000000103',
        global_session_id: '00000000-0000-4000-8000-000000000104',
        route_version: 7,
        session_epoch: 12,
        expires_at_utc: new Date(Date.now() + 5 * 60_000).toISOString()
      }
    }, {
      async rpc(name) {
        if (name === 'candidate_app_federated_membership_link_set_v1') {
          return { ok: true, status: 'LINKED' };
        }
        if (name === 'candidate_daily_system_policy_activate_ready_v1') {
          return { ok: true, outcomes: [{
            candidate_id: context.agency_candidate_id,
            status: 'NOT_READY'
          }] };
        }
        if (name === 'candidate_app_federated_session_project_v1') {
          return {
            ok: true,
            session_id: '00000000-0000-4000-8000-000000000105',
            rotation: 0,
            issued_at_utc: historicalIssuedAt
          };
        }
        throw new Error(`unexpected RPC ${name}`);
      }
    }
  );
  const token = routedRequest.headers.get('authorization')?.replace(/^Bearer\s+/i, '');
  assert.ok(token);
  const payload = JSON.parse(Buffer.from(token.split('.')[0], 'base64url').toString('utf8'));
  assert.ok(payload.iat >= startedAtSeconds);
  assert.ok(payload.iat <= Math.floor(Date.now() / 1000));
  assert.equal(payload.exp - payload.iat, 15 * 60);
  assert.notEqual(new Date(payload.iat * 1000).toISOString(), historicalIssuedAt);
});

test('active membership attempts immediate Rota activation with service-only proof', async () => {
  const calls = [];
  const status = await candidatePrivateWorkerInternals.attemptFederatedDailyActivation(
    request(), {
      async rpc(name, args) {
        calls.push({ name, args });
        return {
          ok: true,
          outcomes: [{ candidate_id: context.agency_candidate_id, status: 'ACTIVATED' }]
        };
      }
    }, context
  );
  assert.equal(status, 'ACTIVATED');
  assert.equal(calls.length, 1);
  assert.equal(calls[0].name, 'candidate_daily_system_policy_activate_ready_v1');
  assert.equal(calls[0].args.p_internal_context.activation_reason,
    'FEDERATED_MEMBERSHIP_ACTIVE');
  assert.equal(calls[0].args.p_internal_context.candidate_id, context.agency_candidate_id);
  assert.deepEqual(calls[0].args.p_candidate_source_hmacs, []);
  assert.deepEqual(calls[0].args.p_projection_outbox_ids, []);
  assert.match(calls[0].args.p_correlation_id, /^[0-7][0-9A-HJKMNP-TV-Z]{25}$/);
});

test('a not-ready Rota never traps Timesheets login and is retried on the next request', async () => {
  const status = await candidatePrivateWorkerInternals.attemptFederatedDailyActivation(
    request(), {
      async rpc() {
        return {
          ok: true,
          outcomes: [{ candidate_id: context.agency_candidate_id, status: 'NOT_READY' }]
        };
      }
    }, context
  );
  assert.equal(status, 'NOT_READY');
});

test('an unexpected Rota dependency failure remains safe and does not revoke membership access', async () => {
  const originalWarn = console.warn;
  const warnings = [];
  console.warn = (...args) => warnings.push(args);
  try {
    const status = await candidatePrivateWorkerInternals.attemptFederatedDailyActivation(
      request(), { async rpc() { throw new Error('test dependency failure'); } }, context
    );
    assert.equal(status, 'RETRY_PENDING');
    assert.equal(warnings.length, 1);
    assert.equal(JSON.stringify(warnings).includes('test dependency failure'), false);
    assert.match(JSON.stringify(warnings), /CANDIDATE_DAILY_ACTIVATION_RETRY_PENDING/);
  } finally {
    console.warn = originalWarn;
  }
});
