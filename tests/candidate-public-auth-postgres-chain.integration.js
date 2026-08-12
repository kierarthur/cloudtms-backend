import assert from 'node:assert/strict';
import { execFile, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import test from 'node:test';

import {
  handleCandidateBrokerRequest
} from '../candidate-broker/src/candidate-broker.js';
import {
  handleCandidateAppRequest
} from '../broker/src/candidate-app-backend.js';
import {
  verifyCandidatePrivateRequest
} from '../broker/src/candidate-service-auth.js';

const enabled = process.env.CANDIDATE_AUTH_POSTGRES_CHAIN === '1';
const origin = 'https://candidate-postgres-chain.test.example';

function psqlArguments(sql) {
  const args = ['-X', '-h', '127.0.0.1', '-U', 'postgres', '-d', 'postgres', '-tA',
    '-v', 'ON_ERROR_STOP=1'];
  args.push('-c', sql);
  return args;
}

function psql(sql, payload = null) {
  const renderedSql = payload == null ? sql : sql.replaceAll(
    ":'payload_b64'", `'${Buffer.from(JSON.stringify(payload)).toString('base64')}'`
  );
  const container = String(process.env.CANDIDATE_AUTH_PG_CONTAINER || '').trim();
  const command = container ? 'docker' : 'psql';
  const args = container
    ? ['exec', container, 'psql', ...psqlArguments(renderedSql)]
    : psqlArguments(renderedSql);
  const result = spawnSync(command, args, { encoding: 'utf8', env: process.env });
  if (result.status !== 0) {
    throw new Error(`${result.stderr || ''}\n${result.stdout || ''}`.trim());
  }
  return String(result.stdout || '').trim();
}

function psqlAsync(sql, payload = null) {
  const renderedSql = payload == null ? sql : sql.replaceAll(
    ":'payload_b64'", `'${Buffer.from(JSON.stringify(payload)).toString('base64')}'`
  );
  const container = String(process.env.CANDIDATE_AUTH_PG_CONTAINER || '').trim();
  const command = container ? 'docker' : 'psql';
  const args = container
    ? ['exec', container, 'psql', ...psqlArguments(renderedSql)]
    : psqlArguments(renderedSql);
  return new Promise((resolve, reject) => {
    execFile(command, args, {
      encoding: 'utf8', env: process.env, maxBuffer: 4 * 1024 * 1024
    }, (error, stdout, stderr) => {
      if (error) {
        reject(new Error(`${stderr || ''}\n${stdout || ''}`.trim()));
        return;
      }
      resolve(String(stdout || '').trim());
    });
  });
}

function rpcSql(name) {
  if (name === 'candidate_auth_challenge_transition_v1') {
    return `
      with a as (
        select convert_from(decode(:'payload_b64','base64'),'UTF8')::jsonb j
      )
      select public.candidate_auth_challenge_transition_v1(
        j->>'p_action',j->>'p_environment',j->>'p_email_normalized',j->>'p_purpose',
        nullif(j->>'p_challenge_id','')::uuid,
        case when j->>'p_token_hash' is null then null
          else decode(substring(j->>'p_token_hash' from 3),'hex') end,
        j->>'p_idempotency_key',(j->>'p_now_utc')::timestamptz,
        nullif(j->>'p_token_key_version','')::integer
      )::text from a`;
  }
  if (name === 'candidate_auth_account_transition_v1') {
    return `
      with a as (
        select convert_from(decode(:'payload_b64','base64'),'UTF8')::jsonb j
      )
      select public.candidate_auth_account_transition_v1(
        j->>'p_action',j->>'p_environment',nullif(j->>'p_account_id','')::uuid,
        j->>'p_email_normalized',nullif(j->>'p_session_id','')::uuid,
        nullif(j->>'p_selected_candidate_id','')::uuid,coalesce(j->'p_payload','{}'::jsonb),
        j->>'p_idempotency_key',(j->>'p_now_utc')::timestamptz
      )::text from a`;
  }
  throw new Error(`unexpected RPC ${name}`);
}

function limiter() {
  return { async limit() { return { success: true }; } };
}

function publicRequest(path, body) {
  return new Request(`https://candidate-postgres-chain.test.example${path}`, {
    method: 'POST',
    headers: {
      origin, 'cf-connecting-ip': '192.0.2.44', 'content-type': 'application/json'
    },
    body: JSON.stringify(body)
  });
}

test('public broker, signed private backend and PostgreSQL share durable auth outcomes', {
  skip: !enabled
}, async () => {
  const originalFetch = globalThis.fetch;
  const serviceSecret = 'postgres-chain-service-secret-not-live';
  let rpcFailure = null;
  const deps = {
    routeAudience: 'PRIVATE',
    async rpc(name, args) {
      try {
        return JSON.parse(psql(rpcSql(name), args));
      } catch (error) {
        rpcFailure = error.message;
        throw error;
      }
    }
  };
  const privateEnv = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_APP_PUBLIC_URL: origin,
    CANDIDATE_PRIVATE_SERVICE_SECRET: serviceSecret,
    CANDIDATE_PRIVATE_CHALLENGE_TOKEN_SECRET: 'postgres-chain-challenge-secret-not-live',
    CANDIDATE_CHALLENGE_TOKEN_KEY_VERSION: '1',
    CANDIDATE_CHALLENGE_TOKEN_READ_KEY_VERSIONS: '1',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'postgres-chain-session-secret-not-live',
    CANDIDATE_AUTH_REPLAY_SECRET_V1: 'postgres-chain-auth-replay-secret-not-live',
    SUPABASE_URL: 'https://postgres-chain.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  let privateFailure = null;
  let publicEnv;
  publicEnv = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_ALLOWED_ORIGINS: origin,
    CANDIDATE_ALLOW_NATIVE_CLIENTS: 'true',
    CANDIDATE_PRIVATE_SERVICE_SECRET: serviceSecret,
    CANDIDATE_BROKER_ACCESS_TOKEN_SECRET: 'postgres-chain-access-secret-not-live',
    CANDIDATE_BROKER_REFRESH_TOKEN_SECRET: 'postgres-chain-refresh-secret-not-live',
    CANDIDATE_BROKER_DEVICE_TOKEN_SECRET: 'postgres-chain-device-secret-not-live',
    CANDIDATE_BROKER_DEVICE_TOKEN_IDENTITY_SECRET: 'postgres-chain-device-id-secret-not-live',
    CANDIDATE_BROKER_MANAGER_HANDOFF_SECRET: 'postgres-chain-manager-secret-not-live',
    CANDIDATE_BROKER_PUBLIC_SESSION_ID_SECRET: 'postgres-chain-public-session-secret-not-live',
    CANDIDATE_GENERAL_RATE_LIMIT: limiter(), CANDIDATE_AUTH_RATE_LIMIT: limiter(),
    CANDIDATE_MANAGER_RATE_LIMIT: limiter(), CANDIDATE_UPLOAD_RATE_LIMIT: limiter(),
    CLOUDTMS_PRIVATE: {
      async fetch(request) {
        assert.equal(await verifyCandidatePrivateRequest(request.clone(), publicEnv), true);
        const url = new URL(request.url);
        url.pathname = url.pathname.replace('/private/candidate-app/v1', '/candidate-app/v1');
        const response = await handleCandidateAppRequest(new Request(url, {
          method: request.method, headers: request.headers,
          body: ['GET', 'HEAD'].includes(request.method) ? undefined : await request.arrayBuffer()
        }), privateEnv, {}, deps);
        if (response.status >= 400) privateFailure = await response.clone().json();
        return response;
      }
    }
  };

  psql(`
    update public.settings_defaults
    set candidate_app_feature_flags_json=candidate_app_feature_flags_json||
      jsonb_build_object('candidate_account_registration',true)
    where id=1;
    insert into public.candidates(id,email,active,key_norm)
    values('ca120812-2146-4000-8000-000000000099',
      'postgres-chain@example.test',true,'POSTGRES-CHAIN-AUTH')
    on conflict (id) do nothing;
  `);

  globalThis.fetch = async (url, init = {}) => {
    const path = new URL(url).pathname;
    if (path.endsWith('/candidate_app_accounts')) return Response.json([]);
    if (path.endsWith('/mail_outbox') && init.method === 'POST') return Response.json([{}]);
    throw new Error(`unexpected private REST operation ${path}`);
  };

  try {
    const start = await handleCandidateBrokerRequest(publicRequest(
      '/candidate-app/v1/auth/challenge/start', {
        email: 'postgres-chain@example.test', purpose: 'ACTIVATE',
        idempotency_key: 'postgres-chain-start-key'
      }
    ), publicEnv);
    assert.equal(start.status, 202, JSON.stringify({
      public: await start.clone().json(), private: privateFailure, rpc: rpcFailure
    }));
    const challengeId = psql(`
      select id::text from public.candidate_auth_challenges
      where deterministic_outbox_key='postgres-chain-start-key'
    `);
    assert.match(challengeId, /^[0-9a-f-]{36}$/);

    const resendBody = {
      email: 'postgres-chain@example.test', purpose: 'ACTIVATE', challenge_id: challengeId,
      idempotency_key: 'postgres-chain-throttle-key'
    };
    const throttled = await handleCandidateBrokerRequest(publicRequest(
      '/candidate-app/v1/auth/challenge/resend', resendBody
    ), publicEnv);
    assert.equal(throttled.status, 429);
    assert.equal((await throttled.clone().json()).error_code, 'CANDIDATE_CHALLENGE_RESEND_TOO_SOON');
    assert.match(throttled.headers.get('retry-after') || '', /^\d+$/);

    psql(`update public.candidate_auth_challenges
      set last_sent_at_utc=clock_timestamp()-interval '61 seconds'
      where id='${challengeId}'::uuid`);
    const sameKeyAfterTimeMoved = await handleCandidateBrokerRequest(publicRequest(
      '/candidate-app/v1/auth/challenge/resend', resendBody
    ), publicEnv);
    assert.equal(sameKeyAfterTimeMoved.status, 429);
    assert.equal((await sameKeyAfterTimeMoved.json()).error_code,
      'CANDIDATE_CHALLENGE_RESEND_TOO_SOON');

    const newKeyAfterTimeMoved = await handleCandidateBrokerRequest(publicRequest(
      '/candidate-app/v1/auth/challenge/resend', {
        ...resendBody, idempotency_key: 'postgres-chain-resend-after-delay-key'
      }
    ), publicEnv);
    assert.equal(newKeyAfterTimeMoved.status, 202);

    const loginBody = {
      email: 'unknown-postgres-chain@example.test', password: 'unknown-password-value',
      idempotency_key: 'postgres-chain-unknown-login-key'
    };
    const firstUnknown = await handleCandidateBrokerRequest(publicRequest(
      '/candidate-app/v1/auth/login', loginBody
    ), publicEnv);
    const replayUnknown = await handleCandidateBrokerRequest(publicRequest(
      '/candidate-app/v1/auth/login', loginBody
    ), publicEnv);
    const changedUnknown = await handleCandidateBrokerRequest(publicRequest(
      '/candidate-app/v1/auth/login', {
        ...loginBody, email: 'changed-unknown-postgres-chain@example.test'
      }
    ), publicEnv);
    assert.equal(firstUnknown.status, 401);
    assert.equal(replayUnknown.status, 401);
    assert.equal(changedUnknown.status, 409);
    assert.equal((await changedUnknown.json()).error_code, 'CANDIDATE_IDEMPOTENCY_CONFLICT');
    assert.equal(psql(`select count(*) from public.candidate_app_accounts
      where email_normalized like '%postgres-chain@example.test'`), '0');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('real private handlers and PostgreSQL keep one winner across mixed auth writer versions', {
  skip: !enabled
}, async () => {
  const originalFetch = globalThis.fetch;
  const challengeEmail = 'mixed-postgres-chain@example.test';
  const challengeKey = 'mixed-postgres-chain-start-key';
  const loginKey = 'mixed-postgres-chain-login-key';
  const challengeMailTokens = [];

  psql(`
    update public.settings_defaults
    set candidate_app_feature_flags_json=candidate_app_feature_flags_json||
      jsonb_build_object('candidate_account_registration',true)
    where id=1;
    insert into public.candidates(id,email,active,key_norm)
    values('ca130826-0002-4000-8000-000000000001',
      '${challengeEmail}',true,'MIXED-POSTGRES-CHAIN')
    on conflict (id) do nothing;
  `);

  const deps = {
    routeAudience: 'PRIVATE',
    async rpc(name, args) {
      return JSON.parse(await psqlAsync(rpcSql(name), args));
    }
  };
  const envFor = version => ({
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_APP_PUBLIC_URL: origin,
    CANDIDATE_CHALLENGE_TOKEN_KEY_VERSION: String(version),
    CANDIDATE_CHALLENGE_TOKEN_READ_KEY_VERSIONS: '1,2',
    CANDIDATE_CHALLENGE_TOKEN_SECRET_V1: 'postgres-mixed-challenge-v1-not-live',
    CANDIDATE_CHALLENGE_TOKEN_SECRET_V2: 'postgres-mixed-challenge-v2-not-live',
    CANDIDATE_AUTH_REPLAY_KEY_VERSION: String(version),
    CANDIDATE_AUTH_REPLAY_READ_KEY_VERSIONS: '1,2',
    CANDIDATE_AUTH_REPLAY_SECRET_V1: 'postgres-mixed-auth-v1-not-live',
    CANDIDATE_AUTH_REPLAY_SECRET_V2: 'postgres-mixed-auth-v2-not-live',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'postgres-mixed-session-not-live',
    SUPABASE_URL: `https://postgres-mixed-v${version}.invalid`,
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  });
  const challengeRequest = () => new Request(
    'https://private.test/candidate-app/v1/auth/challenge/start', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        email: challengeEmail, purpose: 'ACTIVATE', idempotency_key: challengeKey
      })
    }
  );
  const loginRequest = email => new Request(
    'https://private.test/candidate-app/v1/auth/login', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        email, password: 'same-mixed-postgres-password', idempotency_key: loginKey
      })
    }
  );

  globalThis.fetch = async (url, init = {}) => {
    const path = new URL(url).pathname;
    if (path.endsWith('/candidate_app_accounts')) return Response.json([]);
    if (path.endsWith('/mail_outbox') && init.method === 'POST') {
      const body = JSON.parse(init.body);
      const match = String(body.body_text).match(/#token=([^\s]+)/);
      assert.ok(match, 'real-chain challenge mail omitted its token');
      challengeMailTokens.push(decodeURIComponent(match[1]));
      return Response.json(challengeMailTokens.length === 1 ? [{}] : []);
    }
    throw new Error(`unexpected mixed-version REST operation ${path}`);
  };

  try {
    const challengeResults = await Promise.all([1, 2].map(version =>
      handleCandidateAppRequest(challengeRequest(), envFor(version), {}, deps)
    ));
    assert.deepEqual(challengeResults.map(response => response.status), [202, 202]);
    assert.equal(challengeMailTokens.length, 2);
    const winningChallengeHash = psql(`
      select encode(token_hash,'hex')
      from public.candidate_auth_challenges
      where deterministic_outbox_key='${challengeKey}'
    `);
    assert.match(winningChallengeHash, /^[0-9a-f]{64}$/);
    for (const token of challengeMailTokens) {
      assert.equal(createHash('sha256').update(token).digest('hex'), winningChallengeHash);
    }

    const loginResults = await Promise.all([1, 2].map(version =>
      handleCandidateAppRequest(
        loginRequest('unknown-mixed-postgres@example.test'), envFor(version), {}, deps
      )
    ));
    assert.deepEqual(loginResults.map(response => response.status), [401, 401]);
    const receipt = JSON.parse(psql(`
      select jsonb_build_object(
        'count',count(*),
        'key_versions',jsonb_agg(before_json#>>'{metadata,request_key_version}'),
        'request_hashes',jsonb_agg(before_json->>'request_sha256'),
        'response_recorded',bool_and(after_json is not null)
      )::text
      from public.audit_events
      where object_type='candidate_auth_mutation_receipt'
        and object_id_text='TEST'
        and correlation_id='${loginKey}'
    `));
    assert.equal(receipt.count, 1);
    assert.equal(receipt.key_versions.length, 1);
    assert.match(receipt.request_hashes[0], /^[0-9a-f]{64}$/);
    assert.equal(receipt.response_recorded, true);

    const changed = await handleCandidateAppRequest(
      loginRequest('changed-mixed-postgres@example.test'), envFor(2), {}, deps
    );
    assert.equal(changed.status, 409);
    assert.equal((await changed.json()).error_code, 'CANDIDATE_IDEMPOTENCY_CONFLICT');
  } finally {
    globalThis.fetch = originalFetch;
  }
});
