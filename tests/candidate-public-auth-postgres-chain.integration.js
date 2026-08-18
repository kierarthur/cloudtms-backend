import assert from 'node:assert/strict';
import { execFile, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import test from 'node:test';

import {
  handleCandidateBrokerRequest
} from '../candidate-broker/src/candidate-broker.js';
import {
  handleCandidateAppRequest,
  candidateAppBackendInternals
} from '../broker/src/candidate-app-backend.js';
import {
  verifyCandidatePrivateRequest
} from '../broker/src/candidate-service-auth.js';

const enabled = process.env.CANDIDATE_AUTH_POSTGRES_CHAIN === '1';
const origin = 'https://candidate-postgres-chain.test.example';
const {
  derivePasswordVerifier, passwordVerificationProof, createAccessToken
} = candidateAppBackendInternals;

function psqlArguments(sql, inContainer = false) {
  const port = inContainer ? '5432'
    : String(process.env.CANDIDATE_AUTH_PG_PORT || process.env.PGPORT || '5432');
  const args = ['-X', '-h', '127.0.0.1', '-p', port, '-U', 'postgres', '-d', 'postgres', '-tA',
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
    ? ['exec', container, 'psql', ...psqlArguments(renderedSql, true)]
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
    ? ['exec', container, 'psql', ...psqlArguments(renderedSql, true)]
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

test('real private handlers and PostgreSQL revalidate password authority under the account lock', {
  skip: !enabled
}, async () => {
  const originalFetch = globalThis.fetch;
  const oldPassword = 'password-authority-old-value';
  const resetPassword = 'password-authority-reset-value';
  const changePassword = 'password-authority-change-value';
  const oldVerifier = await derivePasswordVerifier(oldPassword);
  const resetVerifier = await derivePasswordVerifier(resetPassword);
  const changeVerifier = await derivePasswordVerifier(changePassword);
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'password-authority-session-not-live',
    CANDIDATE_AUTH_REPLAY_SECRET_V1: 'password-authority-replay-not-live',
    SUPABASE_URL: 'https://password-authority-postgres.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  psql(`
    delete from public.candidate_auth_challenges
    where email_normalized like 'password-authority-%@example.test';
    delete from public.candidate_app_accounts
    where email_normalized like 'password-authority-%@example.test';
    delete from public.candidates
    where email like 'password-authority-%@example.test';
    delete from public.audit_events
    where object_type='candidate_auth_mutation_receipt'
      and correlation_id like 'password-authority-%';
  `);

  function quote(value) {
    return `'${String(value).replaceAll("'", "''")}'`;
  }

  function rows(sql) {
    return JSON.parse(psql(`select coalesce(jsonb_agg(row_data),'[]'::jsonb)::text from (${sql}) rows(row_data)`));
  }

  function accountRows(email, accountId = null) {
    return rows(`select jsonb_build_object(
      'id',a.id,'environment',a.environment,'status',a.status,
      'password_scheme',a.password_scheme,'password_scheme_version',a.password_scheme_version,
      'password_salt',encode(a.password_salt,'hex'),'password_digest',encode(a.password_digest,'hex'),
      'password_params_json',a.password_params_json,'locked_until_utc',a.locked_until_utc
    ) from public.candidate_app_accounts a
    where a.environment='TEST' and ${accountId
      ? `a.id=${quote(accountId)}::uuid`
      : `a.email_normalized=${quote(email)}`}`);
  }

  function sessionRows(sessionId) {
    return rows(`select jsonb_build_object(
      'id',s.id,'account_id',s.account_id,'environment',s.environment,
      'selected_candidate_id',s.selected_candidate_id,'status',s.status,'rotation',s.rotation,
      'expires_at_utc',s.expires_at_utc,'absolute_expires_at_utc',s.absolute_expires_at_utc
    ) from public.candidate_app_sessions s where s.id=${quote(sessionId)}::uuid`);
  }

  globalThis.fetch = async url => {
    const parsed = new URL(url);
    if (parsed.pathname.endsWith('/candidate_app_accounts')) {
      const rawId = parsed.searchParams.get('id') || '';
      if (rawId) return Response.json(accountRows(null, rawId.replace(/^eq\./, '')));
      const raw = parsed.searchParams.get('email_normalized') || '';
      const email = decodeURIComponent(raw.replace(/^eq\./, ''));
      return Response.json(accountRows(email));
    }
    if (parsed.pathname.endsWith('/candidate_app_sessions')) {
      const raw = parsed.searchParams.get('id') || '';
      const sessionId = raw.replace(/^eq\./, '');
      return Response.json(sessionRows(sessionId));
    }
    throw new Error(`unexpected password-authority REST operation ${parsed.pathname}`);
  };

  let fixtureNo = 0;
  async function seed(label, verifier = oldVerifier) {
    fixtureNo += 1;
    const suffix = String(fixtureNo).padStart(12, '0');
    const accountId = `aa130813-2000-4000-8000-${suffix}`;
    const candidateId = `ca130813-2000-4000-8000-${suffix}`;
    const sessionId = `5a130813-2000-4000-8000-${suffix}`;
    const email = `password-authority-${label}@example.test`;
    psql(`
      insert into public.candidates(id,email,active,key_norm)
      values(${quote(candidateId)}::uuid,${quote(email)},true,upper(replace(${quote(accountId)},'-','')));
      insert into public.candidate_app_accounts(
        id,environment,email_normalized,status,password_scheme,password_scheme_version,
        password_salt,password_digest,password_params_json,password_changed_at_utc
      ) values (
        ${quote(accountId)}::uuid,'TEST',${quote(email)},'ACTIVE',${quote(verifier.scheme)},
        ${Number(verifier.scheme_version)}::smallint,decode(${quote(verifier.salt_hex)},'hex'),
        decode(${quote(verifier.digest_hex)},'hex'),${quote(JSON.stringify(verifier.params))}::jsonb,
        clock_timestamp()-interval '1 day'
      );
      insert into public.candidate_app_sessions(
        id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
        token_family_id,rotation,issued_at_utc,expires_at_utc,absolute_expires_at_utc,
        last_used_at_utc,created_at_utc,updated_at_utc
      ) values (
        ${quote(sessionId)}::uuid,${quote(accountId)}::uuid,'TEST',${quote(candidateId)}::uuid,
        'ACTIVE',extensions.digest(convert_to(${quote(`seed-refresh-${label}`)},'UTF8'),'sha256'),
        gen_random_uuid(),0,clock_timestamp(),clock_timestamp()+interval '30 days',
        clock_timestamp()+interval '90 days',clock_timestamp(),clock_timestamp(),clock_timestamp()
      );
    `);
    return { accountId, candidateId, sessionId, email };
  }

  async function currentProof(fixture, password, accountVerifier = oldVerifier) {
    return passwordVerificationProof(password, {
      id: fixture.accountId,
      password_scheme: accountVerifier.scheme,
      password_scheme_version: accountVerifier.scheme_version,
      password_salt: accountVerifier.salt_hex,
      password_digest: accountVerifier.digest_hex,
      password_params_json: accountVerifier.params
    });
  }

  async function resetAccount(fixture, verifier, key) {
    const challengeId = `ca130813-3000-4000-8000-${String(fixtureNo).padStart(12, '0')}`;
    const newSessionId = `5a130813-3000-4000-8000-${String(fixtureNo).padStart(12, '0')}`;
    psql(`insert into public.candidate_auth_challenges(
      id,account_id,environment,email_normalized,purpose,state,token_hash,expires_at_utc,
      verified_at_utc,deterministic_outbox_key
    ) values (${quote(challengeId)}::uuid,${quote(fixture.accountId)}::uuid,'TEST',${quote(fixture.email)},
      'RESET','VERIFIED',extensions.digest(convert_to(${quote(key)},'UTF8'),'sha256'),
      clock_timestamp()+interval '30 minutes',clock_timestamp(),${quote(key)})`);
    const payload = {
      challenge_id: challengeId,
      password_scheme: verifier.scheme,
      password_scheme_version: verifier.scheme_version,
      password_salt_hex: verifier.salt_hex,
      password_digest_hex: verifier.digest_hex,
      password_params: verifier.params,
      refresh_token_hash_hex: createHash('sha256').update(`${key}-refresh`).digest('hex'),
      expires_at_utc: new Date(Date.now() + 30 * 86400000).toISOString(),
      absolute_expires_at_utc: new Date(Date.now() + 90 * 86400000).toISOString(),
      idempotency_request_sha256: createHash('sha256').update(`${key}-request`).digest('hex'),
      idempotency_key_version: 1
    };
    return JSON.parse(await psqlAsync(rpcSql('candidate_auth_account_transition_v1'), {
      p_action: 'ACTIVATE_PASSWORD', p_environment: 'TEST', p_account_id: fixture.accountId,
      p_email_normalized: fixture.email, p_session_id: newSessionId,
      p_selected_candidate_id: fixture.candidateId, p_payload: payload,
      p_idempotency_key: key, p_now_utc: new Date().toISOString()
    }));
  }

  async function changeAccount(fixture, verifier, key, proof = null) {
    const authority = proof || await currentProof(fixture, oldPassword);
    return JSON.parse(await psqlAsync(rpcSql('candidate_auth_account_transition_v1'), {
      p_action: 'CHANGE_PASSWORD', p_environment: 'TEST', p_account_id: fixture.accountId,
      p_email_normalized: null, p_session_id: fixture.sessionId, p_selected_candidate_id: null,
      p_payload: {
        password_scheme: verifier.scheme, password_scheme_version: verifier.scheme_version,
        password_salt_hex: verifier.salt_hex, password_digest_hex: verifier.digest_hex,
        password_params: verifier.params,
        presented_password_digest_hex: authority.presented_password_digest_hex,
        expected_password_authority_sha256: authority.expected_password_authority_sha256,
        idempotency_request_sha256: createHash('sha256').update(`${key}-request`).digest('hex'),
        idempotency_key_version: 1
      }, p_idempotency_key: key, p_now_utc: new Date().toISOString()
    }));
  }

  function privateLogin(fixture, password, key, deps) {
    return handleCandidateAppRequest(new Request('https://private.test/candidate-app/v1/auth/login', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ email: fixture.email, password, idempotency_key: key })
    }), env, {}, deps);
  }

  async function accessToken(fixture) {
    return createAccessToken(env, { session_id: fixture.sessionId, rotation: 0 });
  }

  function privateChange(fixture, currentPassword, password, key, deps, token) {
    return handleCandidateAppRequest(new Request('https://private.test/candidate-app/v1/account/password', {
      method: 'POST',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
      body: JSON.stringify({ current_password: currentPassword, password, idempotency_key: key })
    }), env, {}, deps);
  }

  function pausingDeps(action, count = 1) {
    let arrivals = 0;
    let release;
    let readyResolve;
    const ready = new Promise(resolve => { readyResolve = resolve; });
    const gate = new Promise(resolve => { release = resolve; });
    const payloads = [];
    return {
      ready, release, payloads,
      deps: {
        routeAudience: 'PRIVATE',
        async rpc(name, args) {
          assert.equal(name, 'candidate_auth_account_transition_v1');
          if (args.p_action === action && args.p_payload.replay_probe_only !== true) {
            payloads.push(args.p_payload);
            arrivals += 1;
            if (arrivals === count) readyResolve();
            await gate;
          }
          return JSON.parse(await psqlAsync(rpcSql(name), args));
        }
      }
    };
  }

  function waitForReady(controller, label) {
    return Promise.race([
      controller.ready,
      new Promise((_, reject) => setTimeout(
        () => reject(new Error(`timed out waiting for ${label} private-handler barrier`)), 10000
      ))
    ]);
  }

  function activeSessions(fixture) {
    return JSON.parse(psql(`select coalesce(jsonb_agg(id order by id),'[]'::jsonb)::text
      from public.candidate_app_sessions where account_id=${quote(fixture.accountId)}::uuid
        and status='ACTIVE'`));
  }

  try {
    const resetFirst = await seed('reset-first');
    const resetPause = pausingDeps('LOGIN_SUCCESS');
    const waitingResetLogin = privateLogin(
      resetFirst, oldPassword, 'password-authority-reset-first-login', resetPause.deps
    );
    await waitForReady(resetPause, 'reset-first login');
    const resetResult = await resetAccount(
      resetFirst, resetVerifier, 'password-authority-reset-first-reset'
    );
    assert.equal(resetResult.ok, true);
    resetPause.release();
    const rejectedOldLogin = await waitingResetLogin;
    assert.equal(rejectedOldLogin.status, 401);
    assert.deepEqual(activeSessions(resetFirst), [resetResult.session_id]);

    const loginFirst = await seed('login-first-reset');
    const directDeps = { routeAudience: 'PRIVATE', async rpc(name, args) {
      return JSON.parse(await psqlAsync(rpcSql(name), args));
    } };
    const acceptedBeforeReset = await privateLogin(
      loginFirst, oldPassword, 'password-authority-login-first', directDeps
    );
    assert.equal(acceptedBeforeReset.status, 200);
    const loginFirstResetResult = await resetAccount(
      loginFirst, resetVerifier, 'password-authority-login-first-reset'
    );
    assert.equal(loginFirstResetResult.ok, true);
    assert.deepEqual(activeSessions(loginFirst), [loginFirstResetResult.session_id]);

    const changeFirst = await seed('change-first');
    const changePause = pausingDeps('LOGIN_SUCCESS');
    const waitingChangeLogin = privateLogin(
      changeFirst, oldPassword, 'password-authority-change-first-login', changePause.deps
    );
    await waitForReady(changePause, 'change-first login');
    assert.equal((await changeAccount(
      changeFirst, changeVerifier, 'password-authority-change-first-change'
    )).ok, true);
    changePause.release();
    assert.equal((await waitingChangeLogin).status, 401);
    assert.deepEqual(activeSessions(changeFirst), [changeFirst.sessionId]);

    const loginFirstChange = await seed('login-first-change');
    const acceptedBeforeChange = await privateLogin(
      loginFirstChange, oldPassword, 'password-authority-login-first-change-login', directDeps
    );
    assert.equal(acceptedBeforeChange.status, 200);
    assert.equal((await changeAccount(
      loginFirstChange, changeVerifier, 'password-authority-login-first-change-change'
    )).ok, true);
    assert.deepEqual(activeSessions(loginFirstChange), [loginFirstChange.sessionId]);

    const staleFailure = await seed('stale-failure');
    const stalePause = pausingDeps('LOGIN_SUCCESS');
    const waitingFailure = privateLogin(
      staleFailure, resetPassword, 'password-authority-stale-failure-login', stalePause.deps
    );
    await waitForReady(stalePause, 'stale failed login');
    assert.equal((await resetAccount(
      staleFailure, resetVerifier, 'password-authority-stale-failure-reset'
    )).ok, true);
    stalePause.release();
    assert.equal((await waitingFailure).status, 401);
    assert.equal(psql(`select failed_login_count from public.candidate_app_accounts
      where id=${quote(staleFailure.accountId)}::uuid`), '0');

    const concurrentChange = await seed('concurrent-change');
    const token = await accessToken(concurrentChange);
    const concurrentPauseOne = pausingDeps('CHANGE_PASSWORD');
    const concurrentPauseTwo = pausingDeps('CHANGE_PASSWORD');
    const firstBody = 'password-authority-first-new-value';
    const secondBody = 'password-authority-second-new-value';
    const changes = [
      privateChange(concurrentChange, oldPassword, firstBody,
        'password-authority-concurrent-change-one', concurrentPauseOne.deps, token),
      privateChange(concurrentChange, oldPassword, secondBody,
        'password-authority-concurrent-change-two', concurrentPauseTwo.deps, token)
    ];
    await Promise.race([
      Promise.all([
        waitForReady(concurrentPauseOne, 'first concurrent password change'),
        waitForReady(concurrentPauseTwo, 'second concurrent password change')
      ]),
      changes[0].then(async response => {
        throw new Error(`first password change completed before barrier: ${response.status} ${JSON.stringify(await response.json())}`);
      }),
      changes[1].then(async response => {
        throw new Error(`second password change completed before barrier: ${response.status} ${JSON.stringify(await response.json())}`);
      })
    ]);
    for (const payload of [...concurrentPauseOne.payloads, ...concurrentPauseTwo.payloads]) {
      assert.equal(JSON.stringify(payload).includes(oldPassword), false);
      assert.equal(JSON.stringify(payload).includes(firstBody), false);
      assert.equal(JSON.stringify(payload).includes(secondBody), false);
      assert.match(payload.presented_password_digest_hex, /^[0-9a-f]{64}$/);
      assert.match(payload.expected_password_authority_sha256, /^[0-9a-f]{64}$/);
    }
    concurrentPauseOne.release();
    concurrentPauseTwo.release();
    const changeResponses = await Promise.all(changes);
    assert.deepEqual(changeResponses.map(response => response.status).sort(), [200, 401]);
    assert.deepEqual(activeSessions(concurrentChange), [concurrentChange.sessionId]);
    const successfulIndex = changeResponses.findIndex(response => response.status === 200);
    const replayPassword = successfulIndex === 0 ? firstBody : secondBody;
    const replayKey = successfulIndex === 0
      ? 'password-authority-concurrent-change-one'
      : 'password-authority-concurrent-change-two';
    const exactReplay = await privateChange(
      concurrentChange, oldPassword, replayPassword, replayKey, directDeps, token
    );
    assert.equal(exactReplay.status, 200);
    assert.equal((await exactReplay.json()).idempotent_replay, true);

    const wrongCurrentKey = 'password-authority-wrong-current-durable';
    const wrongCurrentOne = await privateChange(
      concurrentChange, oldPassword, 'password-authority-rejected-new-value',
      wrongCurrentKey, directDeps, token
    );
    const wrongCurrentTwo = await privateChange(
      concurrentChange, oldPassword, 'password-authority-rejected-new-value',
      wrongCurrentKey, directDeps, token
    );
    assert.equal(wrongCurrentOne.status, 401);
    assert.equal(wrongCurrentTwo.status, 401);
    assert.equal((await wrongCurrentOne.json()).error_code, 'CANDIDATE_LOGIN_INVALID');
    assert.equal((await wrongCurrentTwo.json()).error_code, 'CANDIDATE_LOGIN_INVALID');
  } finally {
    globalThis.fetch = originalFetch;
  }
});
