import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';

import {
  candidateAppBackendInternals,
  handleCandidateAppRequest
} from '../broker/src/candidate-app-backend.js';

const { createAccessToken, deterministicOpaqueToken } = candidateAppBackendInternals;

const CHALLENGE_ID = '13082026-0002-4000-8000-000000000001';

function challengeEnv(version) {
  return {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_APP_PUBLIC_URL: 'https://candidate.test.example',
    CANDIDATE_CHALLENGE_TOKEN_KEY_VERSION: String(version),
    CANDIDATE_CHALLENGE_TOKEN_READ_KEY_VERSIONS: '1,2',
    CANDIDATE_CHALLENGE_TOKEN_SECRET_V1: 'mixed-challenge-version-one-secret',
    CANDIDATE_CHALLENGE_TOKEN_SECRET_V2: 'mixed-challenge-version-two-secret',
    SUPABASE_URL: `https://worker-v${version}.supabase.invalid`,
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
}

function challengeRequest(isResend, key) {
  return new Request(
    `https://private.test/candidate-app/v1/auth/challenge/${isResend ? 'resend' : 'start'}`,
    {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        email: 'mixed-version@example.test',
        purpose: 'ACTIVATE',
        ...(isResend ? { challenge_id: CHALLENGE_ID } : {}),
        idempotency_key: key
      })
    }
  );
}

async function runChallengeRace({ isResend, winnerVersion, firstMailVersion }) {
  const originalFetch = globalThis.fetch;
  const key = `mixed-${isResend ? 'resend' : 'start'}-winner-${winnerVersion}-mail-${firstMailVersion}`;
  const preflightResolvers = [];
  const mainCalls = [];
  const mainResolvers = new Map();
  const mailWrites = [];
  let releasedMain = false;

  const releaseMainResults = () => {
    if (releasedMain || mainCalls.length !== 2) return;
    releasedMain = true;
    const winner = mainCalls.find(call => call.version === winnerVersion);
    assert.ok(winner, `missing version-${winnerVersion} main call`);
    const result = {
      ok: true,
      accepted: true,
      deliver_email: true,
      idempotent_replay: false,
      challenge_id: '13082026-0002-4000-8000-000000000002',
      expires_at_utc: '2099-01-01T00:00:00.000Z',
      deterministic_outbox_key: key,
      token_hash_hex: winner.args.p_token_hash.slice(2),
      token_key_version: winner.args.p_token_key_version
    };
    const first = mainCalls.find(call => call.version === firstMailVersion);
    const second = mainCalls.find(call => call.version !== firstMailVersion);
    mainResolvers.get(first.version)(result);
    setTimeout(() => mainResolvers.get(second.version)(result), 10);
  };

  const depsFor = version => ({
    routeAudience: 'PRIVATE',
    async rpc(name, args) {
      assert.equal(name, 'candidate_auth_challenge_transition_v1');
      if (args.p_token_hash == null) {
        return new Promise(resolve => {
          preflightResolvers.push(resolve);
          if (preflightResolvers.length === 2) {
            for (const release of preflightResolvers) {
              release({ replay_receipt_found: false });
            }
          }
        });
      }
      mainCalls.push({ version, args });
      return new Promise(resolve => {
        mainResolvers.set(version, resolve);
        releaseMainResults();
      });
    }
  });

  globalThis.fetch = async (url, init = {}) => {
    assert.equal(init.method, 'POST');
    const payload = JSON.parse(init.body);
    const tokenMatch = String(payload.body_text).match(/#token=([^\s]+)/);
    assert.ok(tokenMatch, 'challenge mail must contain its delivery token');
    mailWrites.push({
      version: Number(new URL(url).hostname.match(/worker-v(\d+)/)?.[1]),
      token: decodeURIComponent(tokenMatch[1])
    });
    return Response.json(mailWrites.length === 1 ? [{}] : []);
  };

  try {
    let timeoutId;
    const timeout = new Promise((_, reject) => {
      timeoutId = setTimeout(
        () => reject(new Error('mixed-version challenge race timed out')), 5000
      );
    });
    const outcomes = await Promise.race([
      Promise.all([1, 2].map(version => handleCandidateAppRequest(
        challengeRequest(isResend, key), challengeEnv(version), {}, depsFor(version)
      ))),
      timeout
    ]);
    clearTimeout(timeoutId);
    assert.deepEqual(outcomes.map(result => result.status), [202, 202]);
    assert.equal(mainCalls.length, 2);
    assert.equal(mailWrites.length, 2);
    assert.equal(mailWrites[0].version, firstMailVersion);

    const winningMain = mainCalls.find(call => call.version === winnerVersion);
    const winningHash = winningMain.args.p_token_hash.slice(2);
    for (const mail of mailWrites) {
      assert.equal(createHash('sha256').update(mail.token).digest('hex'), winningHash);
    }
  } finally {
    globalThis.fetch = originalFetch;
  }
}

test('mixed challenge writer versions always deliver the database-winning START and RESEND token', async () => {
  for (const isResend of [false, true]) {
    for (const winnerVersion of [1, 2]) {
      for (const firstMailVersion of [winnerVersion, winnerVersion === 1 ? 2 : 1]) {
        await runChallengeRace({ isResend, winnerVersion, firstMailVersion });
      }
    }
  }
});

function authEnv(version) {
  return {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_AUTH_REPLAY_KEY_VERSION: String(version),
    CANDIDATE_AUTH_REPLAY_READ_KEY_VERSIONS: '1,2',
    CANDIDATE_AUTH_REPLAY_SECRET_V1: 'mixed-auth-version-one-secret',
    CANDIDATE_AUTH_REPLAY_SECRET_V2: 'mixed-auth-version-two-secret',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'mixed-auth-session-secret',
    SUPABASE_URL: `https://auth-v${version}.supabase.invalid`,
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
}

function loginRequest(key) {
  return new Request('https://private.test/candidate-app/v1/auth/login', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      email: 'unknown-mixed-version@example.test',
      password: 'same-factual-password',
      idempotency_key: key
    })
  });
}

async function runAuthReservationRace(reservedVersion) {
  const originalFetch = globalThis.fetch;
  const key = `mixed-auth-reserved-version-${reservedVersion}`;
  let frozenVersion = null;
  const mainRequests = [];

  const deps = {
    routeAudience: 'PRIVATE',
    async rpc(name, args) {
      assert.equal(name, 'candidate_auth_account_transition_v1');
      if (args.p_payload.replay_probe_only === true) {
        const proposed = Number(args.p_payload.idempotency_key_version);
        frozenVersion ??= proposed;
        return {
          ok: true,
          replay_receipt_found: false,
          request_version_reserved: true,
          request_key_version: frozenVersion
        };
      }
      mainRequests.push({
        request_sha256: args.p_payload.idempotency_request_sha256,
        request_key_version: args.p_payload.idempotency_key_version
      });
      return {
        ok: false,
        error_code: 'CANDIDATE_LOGIN_INVALID',
        failed_login_recorded: false
      };
    }
  };

  globalThis.fetch = async url => {
    assert.match(new URL(url).pathname, /candidate_app_accounts$/);
    return Response.json([]);
  };

  try {
    const versions = reservedVersion === 1 ? [1, 2] : [2, 1];
    const results = await Promise.all(versions.map(version => handleCandidateAppRequest(
      loginRequest(key), authEnv(version), {}, deps
    )));
    assert.deepEqual(results.map(result => result.status), [401, 401]);
    assert.equal(frozenVersion, reservedVersion);
    assert.equal(mainRequests.length, 2);
    assert.deepEqual(mainRequests.map(item => item.request_key_version), [reservedVersion, reservedVersion]);
    assert.equal(new Set(mainRequests.map(item => item.request_sha256)).size, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

test('mixed authentication writers hash an identical request under one reserved key version', async () => {
  await runAuthReservationRace(1);
  await runAuthReservationRace(2);
});

test('a reserved authentication request version fails closed after deliberate reader retirement', async () => {
  let accountLookupAttempted = false;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    accountLookupAttempted = true;
    return Response.json([]);
  };
  try {
    const response = await handleCandidateAppRequest(
      loginRequest('retired-auth-request-version'),
      {
        ...authEnv(2),
        CANDIDATE_AUTH_REPLAY_READ_KEY_VERSIONS: '2'
      },
      {},
      {
        routeAudience: 'PRIVATE',
        async rpc(name, args) {
          assert.equal(name, 'candidate_auth_account_transition_v1');
          assert.equal(args.p_payload.replay_probe_only, true);
          return {
            ok: true,
            replay_receipt_found: false,
            request_version_reserved: true,
            request_key_version: 1
          };
        }
      }
    );
    assert.equal(response.status, 503);
    assert.equal((await response.json()).error_code,
      'CANDIDATE_REPLAY_SECRET_VERSION_UNAVAILABLE');
    assert.equal(accountLookupAttempted, false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('a private Worker fails closed when the database has not acknowledged version reservation', async () => {
  let accountLookupAttempted = false;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    accountLookupAttempted = true;
    return Response.json([]);
  };
  try {
    const response = await handleCandidateAppRequest(
      loginRequest('missing-version-reservation-ack'), authEnv(1), {}, {
        routeAudience: 'PRIVATE',
        async rpc(name, args) {
          assert.equal(name, 'candidate_auth_account_transition_v1');
          assert.equal(args.p_payload.reserve_request_key_version, true);
          return { replay_receipt_found: false, request_key_version: 1 };
        }
      }
    );
    assert.equal(response.status, 503);
    assert.equal((await response.json()).error_code, 'CANDIDATE_AUTH_RECEIPT_UNAVAILABLE');
    assert.equal(accountLookupAttempted, false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

function sessionResult(sessionId) {
  return {
    ok: true,
    session_id: sessionId,
    rotation: 0,
    selected_candidate_id: null,
    selection_required: false,
    issued_at_utc: '2026-08-13T00:02:00.000Z',
    expires_at_utc: '2026-09-12T00:02:00.000Z',
    absolute_expires_at_utc: '2026-11-11T00:02:00.000Z'
  };
}

function mixedActionDeps(action, resultFor) {
  let frozenVersion = null;
  let mainResult = null;
  const preflightResolvers = [];
  const mainRequests = [];
  return {
    mainRequests,
    deps: {
      routeAudience: 'PRIVATE',
      async rpc(name, args) {
        assert.equal(name, 'candidate_auth_account_transition_v1');
        assert.equal(args.p_action, action);
        if (args.p_payload.replay_probe_only === true) {
          frozenVersion ??= Number(args.p_payload.idempotency_key_version);
          return new Promise(resolve => {
            preflightResolvers.push(resolve);
            if (preflightResolvers.length === 2) {
              for (const release of preflightResolvers) {
                release({
                  ok: true,
                  replay_receipt_found: false,
                  request_version_reserved: true,
                  request_key_version: frozenVersion
                });
              }
            }
          });
        }
        mainRequests.push({
          request_sha256: args.p_payload.idempotency_request_sha256,
          request_key_version: args.p_payload.idempotency_key_version
        });
        mainResult ??= resultFor(args);
        return { ...mainResult, idempotent_replay: mainRequests.length > 1 };
      }
    }
  };
}

test('activation, refresh and authenticated account mutations share the reserved version', async () => {
  const originalFetch = globalThis.fetch;
  try {
    const activation = mixedActionDeps('ACTIVATE_PASSWORD', args =>
      sessionResult(args.p_session_id)
    );
    const activationBody = {
      challenge_id: '13082026-0002-4000-8000-000000000011',
      password: 'same-activation-password',
      idempotency_key: 'mixed-activation-key'
    };
    const activationResponses = await Promise.all([2, 1].map(version =>
      handleCandidateAppRequest(new Request(
        'https://private.test/candidate-app/v1/auth/password/complete', {
          method: 'POST', headers: { 'content-type': 'application/json' },
          body: JSON.stringify(activationBody)
        }
      ), authEnv(version), {}, activation.deps)
    ));
    assert.deepEqual(activationResponses.map(response => response.status), [200, 200]);
    assert.equal(new Set(activation.mainRequests.map(item => item.request_sha256)).size, 1);
    assert.deepEqual(activation.mainRequests.map(item => item.request_key_version), [2, 2]);
    const activationPayloads = await Promise.all(activationResponses.map(response => response.json()));
    assert.equal(new Set(activationPayloads.map(item => item.session_id)).size, 1);
    assert.equal(new Set(activationPayloads.map(item => item.refresh_token)).size, 1);

    const refresh = mixedActionDeps('REFRESH_SESSION', args =>
      sessionResult(args.p_payload.new_session_id)
    );
    const refreshBody = {
      session_id: '13082026-0002-4000-8000-000000000012',
      refresh_token: 'same-private-refresh-token',
      idempotency_key: 'mixed-refresh-key'
    };
    const refreshResponses = await Promise.all([1, 2].map(version =>
      handleCandidateAppRequest(new Request(
        'https://private.test/candidate-app/v1/auth/refresh', {
          method: 'POST', headers: { 'content-type': 'application/json' },
          body: JSON.stringify(refreshBody)
        }
      ), authEnv(version), {}, refresh.deps)
    ));
    assert.deepEqual(refreshResponses.map(response => response.status), [200, 200]);
    assert.equal(new Set(refresh.mainRequests.map(item => item.request_sha256)).size, 1);
    assert.deepEqual(refresh.mainRequests.map(item => item.request_key_version), [1, 1]);
    const refreshPayloads = await Promise.all(refreshResponses.map(response => response.json()));
    assert.equal(new Set(refreshPayloads.map(item => item.session_id)).size, 1);
    assert.equal(new Set(refreshPayloads.map(item => item.refresh_token)).size, 1);

    const sessionId = '13082026-0002-4000-8000-000000000013';
    const logout = mixedActionDeps('LOGOUT', () => ({ ok: true, logged_out: true }));
    const access = await createAccessToken(authEnv(1), {
      session_id: sessionId,
      rotation: 0,
      issued_at_utc: new Date().toISOString()
    });
    globalThis.fetch = async url => {
      assert.match(new URL(url).pathname, /candidate_app_sessions$/);
      return Response.json([{
        id: sessionId,
        account_id: '13082026-0002-4000-8000-000000000014',
        environment: 'TEST',
        selected_candidate_id: null,
        status: 'ACTIVE',
        rotation: 0,
        expires_at_utc: '2099-01-01T00:00:00.000Z',
        absolute_expires_at_utc: '2099-01-01T00:00:00.000Z'
      }]);
    };
    const logoutResponses = await Promise.all([2, 1].map(version =>
      handleCandidateAppRequest(new Request(
        'https://private.test/candidate-app/v1/auth/logout', {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            authorization: `Bearer ${access}`
          },
          body: JSON.stringify({ idempotency_key: 'mixed-logout-key' })
        }
      ), authEnv(version), {}, logout.deps)
    ));
    assert.deepEqual(logoutResponses.map(response => response.status), [200, 200]);
    assert.equal(new Set(logout.mainRequests.map(item => item.request_sha256)).size, 1);
    assert.deepEqual(logout.mainRequests.map(item => item.request_key_version), [2, 2]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

function phoneEnv(version) {
  return {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'mixed-phone-access-secret',
    CANDIDATE_MANAGER_TOKEN_KEY_VERSION: String(version),
    CANDIDATE_MANAGER_TOKEN_SECRET_V1: 'mixed-phone-manager-version-one-secret',
    CANDIDATE_MANAGER_TOKEN_SECRET_V2: 'mixed-phone-manager-version-two-secret',
    SUPABASE_URL: `https://phone-v${version}.supabase.invalid`,
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
}

async function runPhoneWinnerRace(winnerVersion) {
  const originalFetch = globalThis.fetch;
  const workflowId = '13082026-0002-4000-8000-000000000021';
  const sessionId = '13082026-0002-4000-8000-000000000022';
  const accountId = '13082026-0002-4000-8000-000000000023';
  const candidateId = '13082026-0002-4000-8000-000000000024';
  const operationKey = `mixed-phone-winner-${winnerVersion}`;
  const publicBinding = {
    contract_version: 'CANDIDATE_PUBLIC_PHONE_BINDING_V1',
    public_session_binding_sha256: '51'.repeat(32),
    device_binding_sha256: '52'.repeat(32)
  };
  const accessToken = await createAccessToken(phoneEnv(1), {
    session_id: sessionId,
    rotation: 0,
    issued_at_utc: new Date().toISOString()
  });
  const probeResolvers = [];
  const mainCalls = [];
  const mainResolvers = [];

  const releaseMain = () => {
    if (mainCalls.length !== 2 || mainResolvers.length !== 2) return;
    const winner = mainCalls.find(call => call.version === winnerVersion);
    assert.ok(winner, `missing PHONE version-${winnerVersion} proposal`);
    const durable = {
      ok: true,
      workflow_id: workflowId,
      generation: 2,
      state: 'AWAITING_MANAGER_APPROVAL',
      approval_request_id: '13082026-0002-4000-8000-000000000025',
      method: 'PHONE',
      approval_token_hash_hex: winner.args.p_payload.approval_token_hash_hex,
      handoff_token_key_version: winner.args.p_payload.handoff_token_key_version,
      public_broker_binding: winner.args.p_payload.public_broker_binding,
      broker_handoff_key_version: winner.args.p_payload.broker_handoff_key_version
    };
    while (mainResolvers.length) mainResolvers.shift()(durable);
  };

  const depsFor = version => ({
    routeAudience: 'PRIVATE',
    async rpc(name, args) {
      assert.equal(name, 'candidate_workflow_transition_atomic_v1');
      assert.equal(args.p_action, 'SELECT_PHONE_APPROVAL');
      if (args.p_payload.mutation_replay_probe_only === true) {
        return new Promise(resolve => {
          probeResolvers.push(resolve);
          if (probeResolvers.length === 2) {
            while (probeResolvers.length) probeResolvers.shift()({ replay_found: false });
          }
        });
      }
      mainCalls.push({ version, args });
      return new Promise(resolve => {
        mainResolvers.push(resolve);
        releaseMain();
      });
    }
  });

  globalThis.fetch = async url => {
    assert.match(new URL(url).pathname, /candidate_app_sessions$/);
    return Response.json([{
      id: sessionId,
      account_id: accountId,
      environment: 'TEST',
      selected_candidate_id: candidateId,
      status: 'ACTIVE',
      rotation: 0,
      expires_at_utc: '2099-01-01T00:00:00.000Z',
      absolute_expires_at_utc: '2099-01-01T00:00:00.000Z'
    }]);
  };

  const invoke = version => handleCandidateAppRequest(new Request(
    `https://private.test/candidate-app/v1/workflows/${workflowId}/actions/select-phone-approval`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        authorization: `Bearer ${accessToken}`
      },
      body: JSON.stringify({
        generation: 1,
        idempotency_key: operationKey,
        payload: {
          public_broker_binding: publicBinding,
          broker_handoff_key_version: 4
        }
      })
    }
  ), phoneEnv(version), {}, depsFor(version));

  try {
    const responses = await Promise.all([1, 2].map(invoke));
    assert.deepEqual(responses.map(response => response.status), [201, 201]);
    const bodies = await Promise.all(responses.map(response => response.json()));
    const winner = mainCalls.find(call => call.version === winnerVersion);
    const winnerHash = winner.args.p_payload.approval_token_hash_hex;
    assert.equal(new Set(bodies.map(body => body.manager_handoff_token)).size, 1);
    assert.deepEqual(bodies.map(body => body.handoff_token_key_version), [winnerVersion, winnerVersion]);
    for (const body of bodies) {
      assert.equal(createHash('sha256').update(body.manager_handoff_token).digest('hex'), winnerHash);
      assert.equal(body.approval_token_hash_hex, undefined);
    }
  } finally {
    globalThis.fetch = originalFetch;
  }
}

test('mixed PHONE writers return only the database-winning handoff token', async () => {
  await runPhoneWinnerRace(1);
  await runPhoneWinnerRace(2);
});

test('refresh-token-reuse security response is replayed without re-evaluating revoked state', async () => {
  const key = 'refresh-security-event-lost-response';
  const sessionId = '13082026-0002-4000-8000-000000000031';
  let receipt = null;
  let mutationCalls = 0;
  const deps = {
    routeAudience: 'PRIVATE',
    async rpc(name, args) {
      assert.equal(name, 'candidate_auth_account_transition_v1');
      assert.equal(args.p_action, 'REFRESH_SESSION');
      if (args.p_payload.replay_probe_only === true
          && !args.p_payload.idempotency_request_sha256) {
        return {
          replay_receipt_found: receipt != null,
          request_version_reserved: true,
          request_key_version: 1
        };
      }
      if (args.p_payload.replay_probe_only === true) {
        assert.ok(receipt, 'negative refresh receipt must exist before exact replay');
        return { ...receipt, idempotent_replay: true };
      }
      mutationCalls += 1;
      receipt = {
        ok: false,
        error_code: 'CANDIDATE_REFRESH_TOKEN_REUSE',
        family_revoked: true
      };
      return receipt;
    }
  };
  const invoke = () => handleCandidateAppRequest(new Request(
    'https://private.test/candidate-app/v1/auth/refresh', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        session_id: sessionId,
        refresh_token: 'rotated-predecessor-refresh-token',
        idempotency_key: key
      })
    }
  ), authEnv(1), {}, deps);

  const first = await invoke();
  const replay = await invoke();
  const firstBody = await first.json();
  const replayBody = await replay.json();
  assert.equal(first.status, 401);
  assert.equal(replay.status, 401);
  assert.equal(firstBody.error_code, 'CANDIDATE_REFRESH_TOKEN_REUSE');
  assert.equal(replayBody.error_code, 'CANDIDATE_REFRESH_TOKEN_REUSE');
  assert.equal(mutationCalls, 1);
});
