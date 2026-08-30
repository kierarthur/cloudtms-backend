import assert from 'node:assert/strict';
import test from 'node:test';

import {
  handleMyTmsManagerControlAdapter,
  handleMyTmsPaperDocumentNudgeAdapter,
  handleMyTmsPaperQrVerifyAdapter,
  managerControlPlaneRpc,
  nudgeCandidatePaperDocumentViaAdapter,
  purgeMyTmsManagerControlAdapterNonces,
  verifyCandidatePaperQrViaAdapter,
  MYTMS_MANAGER_CONTROL_ADAPTER_PATH,
  MYTMS_PAPER_DOCUMENT_NUDGE_ADAPTER_PATH,
  myTmsManagerControlAdapterInternals
} from '../broker/src/mytms-manager-control-adapter.js';
import { signCandidatePrivateRequest } from '../broker/src/candidate-service-auth.js';
import { buildTsq1String } from '../broker/src/timesheet-qr-payload.js';

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

test('private Candidate QR verification reuses the agency backend secret owner', async () => {
  const serviceEnv = testEnv();
  serviceEnv.QR_SIGNING_SECRET = 'unit-test-existing-qr-signing-secret';
  const qrText = await buildTsq1String({ v: 1, tok: 'paper-return-token-0001' }, serviceEnv);
  const clientEnv = {
    CANDIDATE_APP_ENVIRONMENT: serviceEnv.CANDIDATE_APP_ENVIRONMENT,
    MYTMS_MANAGER_CONTROL_ADAPTER_SECRET: serviceEnv.MYTMS_MANAGER_CONTROL_ADAPTER_SECRET,
    MYTMS_MANAGER_CONTROL_ADAPTER: {
      fetch: (request) => handleMyTmsPaperQrVerifyAdapter(request, serviceEnv)
    }
  };

  const verified = await verifyCandidatePaperQrViaAdapter(clientEnv, qrText);
  assert.deepEqual(verified, { v: 1, tok: 'paper-return-token-0001' });
  assert.equal(Object.hasOwn(clientEnv, 'QR_SIGNING_SECRET'), false);
});

test('private Candidate QR verification fails closed on a forged QR', async () => {
  const serviceEnv = testEnv();
  serviceEnv.QR_SIGNING_SECRET = 'unit-test-existing-qr-signing-secret';
  const qrText = await buildTsq1String({ v: 1, tok: 'paper-return-token-0001' }, serviceEnv);
  const clientEnv = {
    CANDIDATE_APP_ENVIRONMENT: serviceEnv.CANDIDATE_APP_ENVIRONMENT,
    MYTMS_MANAGER_CONTROL_ADAPTER_SECRET: serviceEnv.MYTMS_MANAGER_CONTROL_ADAPTER_SECRET,
    MYTMS_MANAGER_CONTROL_ADAPTER: {
      fetch: (request) => handleMyTmsPaperQrVerifyAdapter(request, serviceEnv)
    }
  };

  await assert.rejects(
    verifyCandidatePaperQrViaAdapter(clientEnv, `${qrText.slice(0, -1)}A`),
    /TSQ1_SIGNATURE_INVALID/
  );
});

test('private Candidate paper preparation signs one exact immediate document nudge', async () => {
  const serviceEnv = testEnv();
  const calls = [];
  const clientEnv = {
    CANDIDATE_APP_ENVIRONMENT: serviceEnv.CANDIDATE_APP_ENVIRONMENT,
    MYTMS_MANAGER_CONTROL_ADAPTER_SECRET: serviceEnv.MYTMS_MANAGER_CONTROL_ADAPTER_SECRET,
    MYTMS_MANAGER_CONTROL_ADAPTER: {
      fetch: (request) => handleMyTmsPaperDocumentNudgeAdapter(
        request,
        serviceEnv,
        {
          nudgeDocumentOperation: async (call) => {
            calls.push(call);
            return { scheduled: true };
          }
        }
      )
    }
  };
  const operationId = '00000000-0000-4000-8000-000000000101';
  const timesheetId = '00000000-0000-4000-8000-000000000102';
  const result = await nudgeCandidatePaperDocumentViaAdapter(clientEnv, {
    operationId,
    timesheetId
  });
  assert.deepEqual(result, { scheduled: true, coalesced: false });
  assert.deepEqual(calls, [{ operationId, timesheetId }]);
});

test('paper document nudge adapter rejects unbounded or malformed work before dispatch', async () => {
  const env = testEnv();
  let calls = 0;
  const unsigned = new Request(
    `https://control.internal${MYTMS_PAPER_DOCUMENT_NUDGE_ADAPTER_PATH}`,
    {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        operation_id: 'not-an-operation',
        timesheet_id: '00000000-0000-4000-8000-000000000102',
        lanes: ['ALL']
      })
    }
  );
  const response = await handleMyTmsPaperDocumentNudgeAdapter(
    await signCandidatePrivateRequest(
      unsigned, myTmsManagerControlAdapterInternals.adapterAuthEnv(env)
    ),
    env,
    { nudgeDocumentOperation: async () => { calls += 1; } }
  );
  assert.equal(response.status, 400);
  assert.equal((await response.json()).error_code, 'MYTMS_PAPER_DOCUMENT_REQUEST_INVALID');
  assert.equal(calls, 0);
});
