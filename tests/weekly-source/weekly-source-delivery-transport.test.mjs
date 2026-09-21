import assert from 'node:assert/strict';
import test from 'node:test';
import {
  signWeeklySourceDeliveryRequest,
  verifyWeeklySourceDeliveryRequest,
} from '../../broker/src/weekly-source/delivery-auth.mjs';
import {
  weeklyCandidatePushContent,
  weeklyPushProviderReadiness,
  sendWeeklySourcePush,
} from '../../candidate-broker/src/weekly-source-push-providers.js';
import {
  runWeeklySourceDelivery,
  weeklySourceDeliveryRuntimeInternals,
} from '../../broker/src/weekly-source/delivery-runtime.mjs';
import {
  weeklySourceDeliveryWorkerInternals,
} from '../../weekly-source-delivery-worker/src/index.js';

const AUTH_ENV = Object.freeze({
  WEEKLY_SOURCE_ENVIRONMENT: 'TEST',
  WEEKLY_SOURCE_DELIVERY_SERVICE_SECRET: 'weekly-source-test-secret-that-is-long-enough',
});
const IDS = Object.freeze({
  command: '11111111-1111-4111-8111-111111111111',
  generation: '22222222-2222-4222-8222-222222222222',
  agency: '33333333-3333-4333-8333-333333333333',
  candidate: '44444444-4444-4444-8444-444444444444',
  lease: '55555555-5555-4555-8555-555555555555',
  snapshot: '66666666-6666-4666-8666-666666666666',
  target1: '77777777-7777-4777-8777-777777777777',
  target2: '88888888-8888-4888-8888-888888888888',
  attempt: '99999999-9999-4999-8999-999999999999',
});
const HASH_A = 'a'.repeat(64);
const HASH_B = 'b'.repeat(64);

function pem(label, bytes) {
  const base64 = Buffer.from(bytes).toString('base64').match(/.{1,64}/g).join('\n');
  return `-----BEGIN ${label}-----\n${base64}\n-----END ${label}-----`;
}

async function privateKeyPem(algorithm) {
  const keys = await crypto.subtle.generateKey(algorithm, true, ['sign', 'verify']);
  return pem('PRIVATE KEY', await crypto.subtle.exportKey('pkcs8', keys.privateKey));
}

test('signed internal request verifies and any body change is rejected', async () => {
  const unsigned = new Request('https://internal.example/internal/weekly-source-delivery/v1/run', {
    method: 'POST', body: JSON.stringify({ operation: 'RUN' }),
  });
  const signed = await signWeeklySourceDeliveryRequest(unsigned, AUTH_ENV);
  assert.equal(await verifyWeeklySourceDeliveryRequest(signed, AUTH_ENV), true);
  const changed = new Request(signed.url, {
    method: signed.method, headers: signed.headers, body: JSON.stringify({ operation: 'OTHER' }),
  });
  assert.equal(await verifyWeeklySourceDeliveryRequest(changed, AUTH_ENV), false);
});

test('candidate content is hours-only and uses the exact weekly request deep link', () => {
  const content = weeklyCandidatePushContent({
    tranche_kind: 'CANDIDATE_REMINDER_6H',
    deep_link: { destination: 'WEEKLY_SOURCE_REQUEST', request_id: IDS.generation },
  });
  assert.deepEqual(content.data, {
    destination: 'WEEKLY_SOURCE_REQUEST', request_id: IDS.generation,
  });
  assert.match(`${content.title} ${content.body}`, /hours/i);
  assert.doesNotMatch(`${content.title} ${content.body}`, /pay|money|amount|remittance|invoice|rate/i);
});

test('provider readiness fails closed without non-secret identity and secret credentials', () => {
  assert.equal(weeklyPushProviderReadiness({}, 'APNS').ok, false);
  assert.deepEqual(weeklyPushProviderReadiness({}, 'FCM').missing.sort(), [
    'FCM_CLIENT_EMAIL', 'FCM_PRIVATE_KEY', 'FCM_PROJECT_ID',
  ]);
});

test('APNS invalid token is definite and retires only that target upstream', async () => {
  const key = await privateKeyPem({ name: 'ECDSA', namedCurve: 'P-256' });
  let calls = 0;
  const result = await sendWeeklySourcePush({
    env: {
      APNS_KEY_ID: 'KEY', APNS_TEAM_ID: 'TEAM', APNS_PRIVATE_KEY_P8: key,
      APNS_TOPIC: 'net.cloudtms.mytms.test', APNS_ENVIRONMENT: 'sandbox',
    },
    target: {
      provider: 'APNS', tranche_kind: 'CANDIDATE_INITIAL',
      deep_link: { destination: 'WEEKLY_SOURCE_REQUEST', request_id: IDS.generation },
    },
    token: 'device-token', attemptId: IDS.attempt,
    fetchImpl: async (url, init) => {
      calls += 1;
      assert.match(String(url), /^https:\/\/api\.sandbox\.push\.apple\.com\/3\/device\//);
      assert.equal(init.headers['apns-id'], IDS.attempt);
      return new Response(JSON.stringify({ reason: 'Unregistered' }), {
        status: 410, headers: { 'content-type': 'application/json', 'apns-id': IDS.attempt },
      });
    },
  });
  assert.equal(calls, 1);
  assert.equal(result.outcome, 'DEFINITELY_REJECTED');
  assert.equal(result.invalid_target, true);
  assert.equal(result.bounded_error.error_code, 'INVALID_TARGET');
});

test('FCM accepted, transient, invalid and ambiguous outcomes use mocked providers only', async () => {
  const key = await privateKeyPem({ name: 'RSASSA-PKCS1-v1_5', modulusLength: 2048,
    publicExponent: new Uint8Array([1, 0, 1]), hash: 'SHA-256' });
  const env = { FCM_PROJECT_ID: 'test-project', FCM_CLIENT_EMAIL: 'test@example.invalid',
    FCM_PRIVATE_KEY: key };
  const target = { provider: 'FCM', tranche_kind: 'CANDIDATE_INITIAL',
    deep_link: { destination: 'WEEKLY_SOURCE_REQUEST', request_id: IDS.generation } };
  const run = (providerReply) => sendWeeklySourcePush({
    env, target, token: 'token', attemptId: IDS.attempt,
    fetchImpl: async (url) => String(url).includes('oauth2.googleapis.com')
      ? new Response(JSON.stringify({ access_token: 'mock-access-token' }), { status: 200 })
      : typeof providerReply === 'function' ? providerReply()
        : new Response(JSON.stringify(providerReply.body), { status: providerReply.status }),
  });
  assert.equal((await run({ status: 200, body: { name: 'projects/test/messages/1' } })).outcome, 'ACCEPTED');
  assert.equal((await run({ status: 503, body: { error: { status: 'UNAVAILABLE' } } })).outcome,
    'TRANSIENT_FAILURE');
  const invalid = await run({ status: 404, body: { error: { status: 'UNREGISTERED' } } });
  assert.equal(invalid.outcome, 'DEFINITELY_REJECTED');
  assert.equal(invalid.invalid_target, true);
  assert.equal((await run(() => { throw new Error('network reset after submit'); })).outcome, 'AMBIGUOUS');
});

test('one Candidate command snapshots and registers every active device independently', async () => {
  const calls = [];
  const dependencies = {
    rpc: async (name, args) => {
      calls.push({ name, request: args.p_request });
      if (name === 'weekly_source_message_dispatch_claim_v1') return {
        claimed_count: 1,
        commands: [{
          dispatch_command_id: IDS.command, lease_token: IDS.lease,
          audience_kind: 'CANDIDATE', environment: 'TEST', agency_id: IDS.agency,
          candidate_id: IDS.candidate, candidate_generation_id: IDS.generation,
          rendered_content_hash: HASH_A, provider_idempotency_key: 'command-key',
        }],
      };
      if (name === 'weekly_source_message_targets_register_atomic_v1') {
        assert.equal(args.p_request.targets.length, 2);
        assert.deepEqual(args.p_request.targets.map((item) => item.external_target_id),
          [IDS.target1, IDS.target2]);
        assert.equal(JSON.stringify(args.p_request).includes('device-token'), false);
        return { ok: true, suppressed: false, target_count: 2 };
      }
      throw new Error(`unexpected RPC ${name}`);
    },
    candidatePushFetch: async (request) => {
      assert.equal(await verifyWeeklySourceDeliveryRequest(request, AUTH_ENV), true);
      return new Response(JSON.stringify({
        ok: true, snapshot_id: IDS.snapshot, state: 'ELIGIBLE', targets: [
          { external_target_id: IDS.target1, target_fingerprint: HASH_A,
            target_snapshot_hash: HASH_A, target_version: 1, provider: 'APNS',
            safe_target_snapshot: { snapshot_device_id: IDS.target1 } },
          { external_target_id: IDS.target2, target_fingerprint: HASH_B,
            target_snapshot_hash: HASH_B, target_version: 1, provider: 'FCM',
            safe_target_snapshot: { snapshot_device_id: IDS.target2 } },
        ],
      }), { status: 200 });
    },
  };
  const result = await weeklySourceDeliveryRuntimeInternals.prepareDispatchTargets(
    AUTH_ENV, dependencies, 'test-worker', 10,
  );
  assert.deepEqual(result, { claimed: 1, prepared: 1, suppressed: 0, deferred: 0, failed: 0 });
  assert.equal(calls.filter((item) => item.name.includes('targets_register')).length, 1);
});

test('a Candidate personal push opt-out suppresses providers but preserves the in-app request', async () => {
  const calls = [];
  const dependencies = {
    rpc: async (name, args) => {
      calls.push({ name, request: args.p_request });
      if (name === 'weekly_source_message_dispatch_claim_v1') return {
        claimed_count: 1,
        commands: [{
          dispatch_command_id: IDS.command, lease_token: IDS.lease,
          audience_kind: 'CANDIDATE', environment: 'TEST', agency_id: IDS.agency,
          candidate_id: IDS.candidate, candidate_generation_id: IDS.generation,
          rendered_content_hash: HASH_A, provider_idempotency_key: 'command-key',
        }],
      };
      if (name === 'weekly_source_message_targets_register_atomic_v1') {
        assert.deepEqual(args.p_request.targets, []);
        assert.equal(args.p_request.suppression_reason, 'PERSONAL_PREFERENCE');
        return { ok: true, suppressed: true, target_count: 0 };
      }
      throw new Error(`unexpected RPC ${name}`);
    },
    candidatePushFetch: async () => new Response(JSON.stringify({
      ok: true, snapshot_id: IDS.snapshot, state: 'SUPPRESSED',
      suppression_reason: 'PERSONAL_PREFERENCE', targets: [],
    }), { status: 200 }),
  };
  const result = await weeklySourceDeliveryRuntimeInternals.prepareDispatchTargets(
    AUTH_ENV, dependencies, 'test-worker', 10,
  );
  assert.deepEqual(result, { claimed: 1, prepared: 0, suppressed: 1, deferred: 0, failed: 0 });
  assert.equal(calls.some((item) => item.name.includes('target_start')), false);
});

test('candidate delivery ambiguity is never guessed as accepted', async () => {
  const result = await weeklySourceDeliveryRuntimeInternals.deliverCandidateTarget(
    AUTH_ENV,
    { candidatePushFetch: async () => { throw new Error('connection ended after submit'); } },
    { dispatch_target_id: IDS.target1, dispatch_command_id: IDS.command, provider: 'APNS',
      tranche_kind: 'CANDIDATE_INITIAL', deep_link: {
        destination: 'WEEKLY_SOURCE_REQUEST', request_id: IDS.generation,
      }, safe_target_snapshot: {}, target_snapshot_hash: HASH_A,
      provider_idempotency_key: 'target-key' },
    IDS.attempt,
  );
  assert.equal(result.outcome, 'AMBIGUOUS');
  assert.equal(result.bounded_error.error_code, 'PROVIDER_OUTCOME_UNKNOWN');
});

test('manager addresses are independent and provider classes remain bounded', async () => {
  const accepted = await weeklySourceDeliveryRuntimeInternals.deliverManagerTarget({
    sendManagerEmail: async () => ({ ok: true, status: 202, provider_message_id: 'safe-id' }),
  }, { manager_recipient: 'manager@example.invalid', subject_text: 'Review', html_body: '<p>x</p>',
    plain_body: 'x', dispatch_command_id: IDS.command });
  assert.equal(accepted.outcome, 'ACCEPTED');
  const temporary = await weeklySourceDeliveryRuntimeInternals.deliverManagerTarget({
    sendManagerEmail: async () => ({ ok: false, status: 503 }),
  }, {});
  assert.equal(temporary.outcome, 'TRANSIENT_FAILURE');
  const ambiguous = await weeklySourceDeliveryRuntimeInternals.deliverManagerTarget({
    sendManagerEmail: async () => { throw new Error('unknown'); },
  }, {});
  assert.equal(ambiguous.outcome, 'AMBIGUOUS');
});

test('dedicated queue calls only the signed Weekly Source runtime binding', async () => {
  let called = 0;
  const scheduledAtUtc = '2026-09-16T03:45:00.000Z';
  const env = {
    ...AUTH_ENV,
    CLOUDTMS_WEEKLY_SOURCE_RUNTIME: {
      fetch: async (request) => {
        called += 1;
        assert.equal(new URL(request.url).pathname, '/internal/weekly-source-delivery/v1/run');
        assert.equal(await verifyWeeklySourceDeliveryRequest(request, AUTH_ENV), true);
        assert.equal((await request.clone().json()).scheduled_started_at_utc, scheduledAtUtc);
        return new Response(JSON.stringify({ ok: true }), { status: 200 });
      },
    },
  };
  await weeklySourceDeliveryWorkerInternals.invokeRuntime(env, {
    worker_id: 'test-worker', limit: 3, scheduled_at_utc: scheduledAtUtc,
  });
  assert.equal(called, 1);
});

test('scheduler time is one canonical server-owned UTC instant', () => {
  assert.equal(
    weeklySourceDeliveryRuntimeInternals.canonicalServerUtc('2026-09-16T03:45:00Z'),
    '2026-09-16T03:45:00.000Z',
  );
  assert.throws(
    () => weeklySourceDeliveryRuntimeInternals.canonicalServerUtc('2026-09-16T04:45:00+01:00'),
    /WEEKLY_SOURCE_SCHEDULED_TIME_INVALID/,
  );
  assert.throws(
    () => weeklySourceDeliveryRuntimeInternals.canonicalServerUtc('not-a-time'),
    /WEEKLY_SOURCE_SCHEDULED_TIME_INVALID/,
  );
});

test('delivery runner passes the exact injected server instant to due-work scheduling', async () => {
  const scheduledAtUtc = '2026-09-16T03:45:00.000Z';
  const calls = [];
  const result = await runWeeklySourceDelivery(AUTH_ENV, {
    nowUtc: () => scheduledAtUtc,
    rpc: async (name, args) => {
      calls.push({ name, request: args.p_request });
      if (name === 'weekly_source_query_scheduler_tick_v1') return { ok: true };
      if (name === 'weekly_source_message_render_due_list_v1') return { due_count: 0, rows: [] };
      if (name === 'weekly_source_message_dispatch_claim_v1') return { claimed_count: 0, commands: [] };
      if (name === 'weekly_source_message_dispatch_target_claim_v1') return { claimed_count: 0, targets: [] };
      throw new Error(`unexpected RPC ${name}`);
    },
  }, { limit: 3, workerId: 'test-worker' });
  assert.equal(result.ok, true);
  assert.equal(
    calls.find((call) => call.name === 'weekly_source_query_scheduler_tick_v1').request.now_utc,
    scheduledAtUtc,
  );
});

// ---------------------------------------------------------------------------
// WP-44 F1.  `weekly_source_message_dispatch_target_start_atomic_v1` RETURNS
// `{ok:false, reason}` on each of its four retirement branches: it has retired
// the target, revoked the review batch and the route receipt and written no
// attempt row.  Before this fix the runtime guarded only a THROWN start, so it
// sent the manager the email it was already holding - stale hours and a link
// whose batch and receipt were already revoked - and then aborted the whole
// tick on `P0002`, starving every remaining target and the pending-release
// step.  No test covered a start returning `ok:false`.  These do.
// ---------------------------------------------------------------------------

const RETIRED_START = Object.freeze({
  ok: false, reason: 'REVIEW_CHANGED',
  dispatch_target_id: IDS.target1, dispatch_command_id: IDS.command,
});

function managerTarget(targetId, commandId, htmlBody) {
  return {
    dispatch_target_id: targetId, dispatch_command_id: commandId,
    lease_token: IDS.lease, channel: 'EMAIL', target_kind: 'MANAGER_ADDRESS',
    provider: 'POWER_AUTOMATE',
    subject_text: 'Timesheet queries requiring your review - 1 shift',
    html_body: htmlBody, plain_body: htmlBody,
    manager_recipient: 'manager@example.invalid',
    provider_idempotency_key: 'render/' + targetId,
    safe_target_snapshot: { recipient_route_id: targetId },
  };
}

test('a retired start sends no manager email and never reaches the result owner', async () => {
  const sends = [];
  const resultCalls = [];
  const startedTargets = [];
  const result = await weeklySourceDeliveryRuntimeInternals.deliverTargets(
    AUTH_ENV,
    {
      rpc: async (name, args) => {
        if (name === 'weekly_source_message_dispatch_target_claim_v1') {
          return {
            claimed_count: 1,
            targets: [managerTarget(IDS.target1, IDS.command, '<p>STALE HOURS</p>')],
          };
        }
        if (name === 'weekly_source_message_dispatch_target_start_atomic_v1') {
          startedTargets.push(args.p_request.dispatch_target_id);
          return RETIRED_START;
        }
        if (name === 'weekly_source_message_dispatch_target_result_atomic_v1') {
          resultCalls.push(args.p_request);
          return { ok: true };
        }
        throw new Error('unexpected RPC ' + name);
      },
      sendManagerEmail: async (outgoing) => {
        sends.push(outgoing);
        return { ok: true, status: 202 };
      },
    },
    'test-worker', 10,
  );
  assert.deepEqual(sends, []);
  assert.deepEqual(resultCalls, []);
  assert.deepEqual(startedTargets, [IDS.target1]);
  assert.equal(result.retired, 1);
  assert.equal(result.accepted, 0);
  assert.equal(result.unstarted, 0);
});

test('a retired target never starves the healthy target behind it', async () => {
  const sends = [];
  const startedTargets = [];
  const result = await weeklySourceDeliveryRuntimeInternals.deliverTargets(
    AUTH_ENV,
    {
      rpc: async (name, args) => {
        if (name === 'weekly_source_message_dispatch_target_claim_v1') {
          return {
            claimed_count: 2,
            targets: [
              managerTarget(IDS.target1, IDS.command, '<p>STALE HOURS</p>'),
              managerTarget(IDS.target2, IDS.generation, '<p>HEALTHY SIBLING</p>'),
            ],
          };
        }
        if (name === 'weekly_source_message_dispatch_target_start_atomic_v1') {
          startedTargets.push(args.p_request.dispatch_target_id);
          return args.p_request.dispatch_target_id === IDS.target1
            ? RETIRED_START
            : { ok: true, replay: false, provider_attempt_id: IDS.attempt };
        }
        if (name === 'weekly_source_message_dispatch_target_result_atomic_v1') {
          assert.equal(args.p_request.provider_attempt_id, IDS.attempt);
          return { ok: true };
        }
        throw new Error('unexpected RPC ' + name);
      },
      sendManagerEmail: async (outgoing) => {
        sends.push(outgoing);
        return { ok: true, status: 202 };
      },
    },
    'test-worker', 10,
  );
  assert.deepEqual(startedTargets, [IDS.target1, IDS.target2]);
  assert.equal(sends.length, 1);
  assert.equal(sends[0].htmlBody, '<p>HEALTHY SIBLING</p>');
  assert.equal(result.retired, 1);
  assert.equal(result.accepted, 1);
});

test('a retired start on the push branch reaches no push authority at all', async () => {
  let pushCalls = 0;
  const result = await weeklySourceDeliveryRuntimeInternals.deliverTargets(
    AUTH_ENV,
    {
      rpc: async (name) => {
        if (name === 'weekly_source_message_dispatch_target_claim_v1') {
          return {
            claimed_count: 1,
            targets: [{
              dispatch_target_id: IDS.target1, dispatch_command_id: IDS.command,
              lease_token: IDS.lease, channel: 'PUSH', target_kind: 'CANDIDATE_DEVICE',
              provider: 'APNS', tranche_kind: 'CANDIDATE_INITIAL',
              deep_link: { destination: 'WEEKLY_SOURCE_REQUEST', request_id: IDS.command },
              safe_target_snapshot: { snapshot_device_id: IDS.target1 },
              target_snapshot_hash: HASH_A, provider_idempotency_key: 'render-key',
            }],
          };
        }
        if (name === 'weekly_source_message_dispatch_target_start_atomic_v1') return RETIRED_START;
        throw new Error('unexpected RPC ' + name);
      },
      candidatePushFetch: async () => {
        pushCalls += 1;
        return new Response(JSON.stringify({ ok: true }), { status: 200 });
      },
    },
    'test-worker', 10,
  );
  assert.equal(pushCalls, 0);
  assert.equal(result.retired, 1);
});

test('a start that succeeds without a provider attempt id is treated as not started', async () => {
  const sends = [];
  const result = await weeklySourceDeliveryRuntimeInternals.deliverTargets(
    AUTH_ENV,
    {
      rpc: async (name) => {
        if (name === 'weekly_source_message_dispatch_target_claim_v1') {
          return {
            claimed_count: 1,
            targets: [managerTarget(IDS.target1, IDS.command, '<p>STALE HOURS</p>')],
          };
        }
        if (name === 'weekly_source_message_dispatch_target_start_atomic_v1') {
          return { ok: true, replay: false, provider_attempt_id: null };
        }
        throw new Error('unexpected RPC ' + name);
      },
      sendManagerEmail: async (outgoing) => { sends.push(outgoing); return { ok: true }; },
    },
    'test-worker', 10,
  );
  assert.deepEqual(sends, []);
  assert.equal(result.retired, 1);
});

test('a start that raises sends nothing and the rest of the page still runs', async () => {
  const sends = [];
  const result = await weeklySourceDeliveryRuntimeInternals.deliverTargets(
    AUTH_ENV,
    {
      rpc: async (name, args) => {
        if (name === 'weekly_source_message_dispatch_target_claim_v1') {
          return {
            claimed_count: 2,
            targets: [
              managerTarget(IDS.target1, IDS.command, '<p>STALE HOURS</p>'),
              managerTarget(IDS.target2, IDS.generation, '<p>HEALTHY SIBLING</p>'),
            ],
          };
        }
        if (name === 'weekly_source_message_dispatch_target_start_atomic_v1') {
          if (args.p_request.dispatch_target_id === IDS.target1) {
            throw new Error('WEEKLY_SOURCE_TARGET_LEASE_STALE');
          }
          return { ok: true, replay: false, provider_attempt_id: IDS.attempt };
        }
        if (name === 'weekly_source_message_dispatch_target_result_atomic_v1') return { ok: true };
        throw new Error('unexpected RPC ' + name);
      },
      sendManagerEmail: async (outgoing) => {
        sends.push(outgoing);
        return { ok: true, status: 202 };
      },
    },
    'test-worker', 10,
  );
  assert.equal(sends.length, 1);
  assert.equal(sends[0].htmlBody, '<p>HEALTHY SIBLING</p>');
  assert.equal(result.unstarted, 1);
  assert.equal(result.accepted, 1);
});

test('a result that cannot be recorded does not abort the remaining targets', async () => {
  const sends = [];
  const result = await weeklySourceDeliveryRuntimeInternals.deliverTargets(
    AUTH_ENV,
    {
      rpc: async (name) => {
        if (name === 'weekly_source_message_dispatch_target_claim_v1') {
          return {
            claimed_count: 2,
            targets: [
              managerTarget(IDS.target1, IDS.command, '<p>FIRST</p>'),
              managerTarget(IDS.target2, IDS.generation, '<p>SECOND</p>'),
            ],
          };
        }
        if (name === 'weekly_source_message_dispatch_target_start_atomic_v1') {
          return { ok: true, replay: false, provider_attempt_id: IDS.attempt };
        }
        if (name === 'weekly_source_message_dispatch_target_result_atomic_v1') {
          if (sends.length === 1) throw new Error('WEEKLY_SOURCE_TARGET_RESULT_STALE');
          return { ok: true };
        }
        throw new Error('unexpected RPC ' + name);
      },
      sendManagerEmail: async (outgoing) => {
        sends.push(outgoing);
        return { ok: true, status: 202 };
      },
    },
    'test-worker', 10,
  );
  assert.equal(sends.length, 2);
  assert.equal(result.unrecorded, 1);
  assert.equal(result.accepted, 1);
});

// ---------------------------------------------------------------------------
// WP-44 F2.  A snapshot step that fails is a transient failure of the QUESTION,
// never an answer about this Candidate, and the runtime must not mint a
// control-plane snapshot identity it was not given.
// ---------------------------------------------------------------------------

function candidateCommandDependencies(recorder, snapshotBehaviour) {
  return {
    rpc: async (name, args) => {
      if (name === 'weekly_source_message_dispatch_claim_v1') {
        return {
          claimed_count: 1,
          commands: [{
            dispatch_command_id: IDS.command, lease_token: IDS.lease,
            audience_kind: 'CANDIDATE', environment: 'TEST', agency_id: IDS.agency,
            candidate_id: IDS.candidate, candidate_generation_id: IDS.generation,
            rendered_content_hash: HASH_A, provider_idempotency_key: 'command-key',
          }],
        };
      }
      if (name === 'weekly_source_message_targets_register_atomic_v1') {
        recorder.registrations.push(args.p_request);
        return {
          ok: true,
          suppressed: args.p_request.suppression_reason != null,
          target_count: args.p_request.targets.length,
        };
      }
      if (name === 'weekly_source_message_dispatch_snapshot_failure_atomic_v1') {
        recorder.deferrals.push(args.p_request);
        return { ok: true, deferred: true, re_claimable: true };
      }
      throw new Error('unexpected RPC ' + name);
    },
    candidatePushFetch: snapshotBehaviour,
  };
}

test('a transient push snapshot failure defers and invents no snapshot identity', async () => {
  const cases = [
    ['connection reset', async () => { throw new Error('read ECONNRESET'); }],
    ['binding absent', undefined],
    ['control plane 503', async () => new Response(
      JSON.stringify({ ok: false, error_code: 'WEEKLY_PUSH_DEPENDENCY_UNAVAILABLE' }),
      { status: 503 },
    )],
  ];
  for (const [label, behaviour] of cases) {
    const recorder = { registrations: [], deferrals: [] };
    const result = await weeklySourceDeliveryRuntimeInternals.prepareDispatchTargets(
      AUTH_ENV, candidateCommandDependencies(recorder, behaviour), 'test-worker', 10,
    );
    assert.deepEqual(result,
      { claimed: 1, prepared: 0, suppressed: 0, deferred: 1, failed: 0 }, label);
    assert.deepEqual(recorder.registrations, [], label);
    assert.equal(recorder.deferrals.length, 1, label);
    assert.equal(recorder.deferrals[0].dispatch_command_id, IDS.command, label);
    assert.match(recorder.deferrals[0].failure_code, /^[A-Z][A-Z0-9_]{2,119}$/, label);
  }
});

test('an authority answer the runtime cannot read is deferred, not guessed', async () => {
  const recorder = { registrations: [], deferrals: [] };
  const result = await weeklySourceDeliveryRuntimeInternals.prepareDispatchTargets(
    AUTH_ENV,
    candidateCommandDependencies(recorder, async () => new Response(JSON.stringify({
      ok: true, snapshot_id: IDS.snapshot, state: 'DEFERRED',
      suppression_reason: null, targets: [],
    }), { status: 200 })),
    'test-worker', 10,
  );
  assert.deepEqual(result, { claimed: 1, prepared: 0, suppressed: 0, deferred: 1, failed: 0 });
  assert.deepEqual(recorder.registrations, []);
  assert.equal(recorder.deferrals[0].failure_code, 'PUSH_SNAPSHOT_STATE_UNRECOGNISED');
});

test('a snapshot answer without a real snapshot identity is deferred', async () => {
  const recorder = { registrations: [], deferrals: [] };
  await weeklySourceDeliveryRuntimeInternals.prepareDispatchTargets(
    AUTH_ENV,
    candidateCommandDependencies(recorder, async () => new Response(JSON.stringify({
      ok: true, snapshot_id: 'not-a-uuid', state: 'SUPPRESSED',
      suppression_reason: 'NO_ACTIVE_DEVICE', targets: [],
    }), { status: 200 })),
    'test-worker', 10,
  );
  assert.deepEqual(recorder.registrations, []);
  assert.equal(recorder.deferrals[0].failure_code, 'PUSH_SNAPSHOT_IDENTITY_MISSING');
});

test('a genuine suppression still finalises, with the authority own snapshot id', async () => {
  for (const reason of ['PERSONAL_PREFERENCE', 'NO_ACTIVE_DEVICE', 'PUSH_DELIVERY_UNAVAILABLE']) {
    const recorder = { registrations: [], deferrals: [] };
    const result = await weeklySourceDeliveryRuntimeInternals.prepareDispatchTargets(
      AUTH_ENV,
      candidateCommandDependencies(recorder, async () => new Response(JSON.stringify({
        ok: true, snapshot_id: IDS.snapshot, state: 'SUPPRESSED',
        suppression_reason: reason, targets: [],
      }), { status: 200 })),
      'test-worker', 10,
    );
    assert.deepEqual(result,
      { claimed: 1, prepared: 0, suppressed: 1, deferred: 0, failed: 0 }, reason);
    assert.deepEqual(recorder.deferrals, [], reason);
    assert.equal(recorder.registrations[0].suppression_reason, reason);
    assert.equal(recorder.registrations[0].control_plane_snapshot_id, IDS.snapshot);
    assert.notEqual(recorder.registrations[0].control_plane_snapshot_id, IDS.command);
  }
});

// ---------------------------------------------------------------------------
// WP-44 F3.  The already-agreed hours-only notification must arrive once,
// saying what the policy says, and an identical republication must not be
// announced as a change.  The in-app half is proved here against the real
// exported projection; the database half is proved by
// supabase/verification/17092026_1200_weekly_source_audit_and_export_v1.sql.
// ---------------------------------------------------------------------------

test('the approved-hours notice carries its agreed copy and no forbidden word', async () => {
  const backend = await import('../../broker/src/candidate-app-backend.js');
  const safeCandidateNotification =
    backend.candidateAppBackendInternals.safeCandidateNotification;
  const row = {
    id: '00000000-0000-4000-8000-0000000000e1',
    event_type: 'TIMESHEET_HOURS_UPDATED',
    template_key: 'approved-hours-updated-v1',
    timesheet_id: '00000000-0000-4000-8000-000000000305',
    state: 'UNREAD', created_at_utc: '2026-09-18T20:00:00.000Z', read_at_utc: null,
    deep_link_json: {
      destination: 'TIMESHEET_DETAIL',
      timesheet_id: '00000000-0000-4000-8000-000000000305',
    },
    template_params: {
      week_ending_date: '2026-09-13',
      approved_hours_total: 7,
      approved_hours: [{
        date: '2026-09-08', start: '09:00', end: '17:00', worked: true,
        row_key: 'approved-00000000-0000-4000-8000-0000000000d6',
        break_entry: { kind: 'DURATION_MINUTES', break_minutes: 60 },
        additional_units: [],
      }],
    },
  };
  const withoutClient = safeCandidateNotification(row);
  assert.equal(
    withoutClient.payload_json.message,
    'The approved hours for your Timesheet for the week ending 13/09/2026 have changed. '
    + 'Open your Timesheet to review them.',
  );
  assert.deepEqual(withoutClient.deep_link_json, {
    destination: 'TIMESHEET_DETAIL',
    timesheet_id: '00000000-0000-4000-8000-000000000305',
  });

  const withClient = safeCandidateNotification({
    ...row,
    template_params: { ...row.template_params, client_name: 'St Mary NHS Trust' },
  });
  assert.equal(
    withClient.payload_json.message,
    'The approved hours for your Timesheet at St Mary NHS Trust, week ending 13/09/2026, '
    + 'have changed. Open your Timesheet to review them.',
  );

  // Addendum R8A section 2: nothing Imported, Adjustment, Protected, Exceptional,
  // Source, Reconciliation, Recovery, money, rates, invoice or remittance.
  const forbidden = [
    'imported', 'adjustment', 'protected', 'exceptional', 'source',
    'reconciliation', 'recovery', 'invoice', 'remittance', 'rate', 'pay',
    'vat', 'charge', 'margin', '£',
  ];
  for (const message of [withoutClient.payload_json.message, withClient.payload_json.message]) {
    for (const word of forbidden) {
      assert.equal(new RegExp('\\b' + word + '\\b', 'i').test(message), false,
        word + ' reached the Candidate');
    }
    assert.equal(message.includes('£'), false);
  }

  // The hours themselves never leave the database: the projection carries ids,
  // state and one message, never template_params.
  const serialised = JSON.stringify(withoutClient);
  assert.equal(serialised.includes('approved_hours'), false);
  assert.equal(serialised.includes('approved_hours_total'), false);
  assert.equal(serialised.includes('row_key'), false);
  assert.equal(serialised.includes('break_entry'), false);
  assert.equal(Object.prototype.hasOwnProperty.call(withoutClient, 'template_params'), false);
  // The only digits that reach the Candidate are the week-ending date.
  assert.deepEqual(
    withoutClient.payload_json.message.match(/\d+/g),
    ['13', '09', '2026'],
  );
});
