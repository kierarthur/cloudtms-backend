import assert from 'node:assert/strict';
import test from 'node:test';

import {
  handleMyTmsManagerControlAdapter,
  managerControlPlaneRpc,
  purgeMyTmsManagerControlAdapterNonces,
  MYTMS_MANAGER_CONTROL_ADAPTER_PATH,
  myTmsManagerControlAdapterInternals
} from '../broker/src/mytms-manager-control-adapter.js';
import { signCandidatePrivateRequest } from '../broker/src/candidate-service-auth.js';

function testEnv() {
  const nonces = new Set();
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    MYTMS_MANAGER_CONTROL_ADAPTER_SECRET: 'unit-test-manager-control-adapter-secret-material',
    MYTMS_CONTROL_PLANE_ENABLED: 'TRUE',
    MYTMS_CONTROL_PLANE_URL: 'https://control.test.invalid',
    MYTMS_CONTROL_PLANE_SERVICE_ROLE_KEY: 'unit-test-control-service-key',
    R2: {
      async put(key) {
        if (nonces.has(key)) return null;
        nonces.add(key);
        return { key };
      }
    }
  };
  env.MYTMS_MANAGER_CONTROL_ADAPTER = {
    fetch: (request) => handleMyTmsManagerControlAdapter(request, env)
  };
  return env;
}

test('manager control adapter signs one closed service call and returns only its result', async () => {
  const env = testEnv();
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (input) => {
    assert.match(input instanceof Request ? input.url : String(input),
      /\/rest\/v1\/rpc\/manager_review_origin_resolve_v1$/);
    return Response.json({
      manager_review_public_origin: 'https://manager.test.invalid',
      settings_version: 1
    });
  };
  try {
    const result = await managerControlPlaneRpc(
      env, 'control', 'manager_review_origin_resolve_v1', {
        p_agency_id: '00000000-0000-4000-8000-000000000001',
        p_environment_label: 'TEST'
      }
    );
    assert.deepEqual(result, {
      manager_review_public_origin: 'https://manager.test.invalid',
      settings_version: 1
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('manager control adapter rejects unknown functions before a dependency call', async () => {
  const env = testEnv();
  await assert.rejects(
    managerControlPlaneRpc(env, 'control', 'unbounded_control_rpc', {}),
    /MYTMS_MANAGER_CONTROL_OPERATION_NOT_ALLOWED/
  );
});

test('manager control adapter rejects a replayed signed nonce', async () => {
  const env = testEnv();
  const unsigned = new Request(
    `https://control.internal${MYTMS_MANAGER_CONTROL_ADAPTER_PATH}`,
    {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        schema: 'control',
        function_name: 'manager_email_route_transition_v1',
        args: { p_transition: { target_state: 'RETIRED' } }
      })
    }
  );
  const signed = await signCandidatePrivateRequest(
    unsigned, myTmsManagerControlAdapterInternals.adapterAuthEnv(env)
  );
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => Response.json({ state: 'RETIRED' });
  try {
    const first = await handleMyTmsManagerControlAdapter(signed.clone(), env);
    assert.equal(first.status, 200);
    const second = await handleMyTmsManagerControlAdapter(signed.clone(), env);
    assert.equal(second.status, 401);
    assert.equal((await second.json()).error_code, 'MYTMS_MANAGER_CONTROL_AUTHORITY_INVALID');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('manager control adapter removes only expired adapter nonces', async () => {
  const deleted = [];
  const env = {
    R2: {
      async list({ prefix }) {
        assert.equal(prefix, 'mytms-manager-control-adapter-nonces/');
        return {
          truncated: false,
          objects: [
            { key: `${prefix}test/1000-old` },
            { key: `${prefix}test/1950-current` },
            { key: `${prefix}test/not-a-timestamp` }
          ]
        };
      },
      async delete(keys) {
        deleted.push(...keys);
      }
    }
  };
  await purgeMyTmsManagerControlAdapterNonces(env, 2000);
  assert.deepEqual(deleted, ['mytms-manager-control-adapter-nonces/test/1000-old']);
});
