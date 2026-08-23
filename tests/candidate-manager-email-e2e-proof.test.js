import assert from 'node:assert/strict';
import test from 'node:test';

import {
  CANDIDATE_MANAGER_EMAIL_E2E_IDS,
  CANDIDATE_MANAGER_EMAIL_E2E_PROOF_PATH,
  candidateManagerEmailE2EProofInternals,
  handleCandidateManagerEmailE2EProof
} from '../broker/src/candidate-manager-email-e2e-proof.js';

test('manager email E2E proof is a fixed TEST-only admin harness outside the 63 operations', async () => {
  assert.equal(CANDIDATE_MANAGER_EMAIL_E2E_PROOF_PATH,
    '/api/manager-email-e2e-proof/start');
  assert.equal(CANDIDATE_MANAGER_EMAIL_E2E_PROOF_PATH.startsWith('/api/candidate-app/'), false);
  assert.equal(new Set(Object.values(CANDIDATE_MANAGER_EMAIL_E2E_IDS)).size,
    Object.keys(CANDIDATE_MANAGER_EMAIL_E2E_IDS).length);
  assert.equal(candidateManagerEmailE2EProofInternals.proofEnabled({
    CANDIDATE_MANAGER_E2E_PROOF_ENABLED: 'TRUE', CANDIDATE_APP_ENVIRONMENT: 'TEST'
  }), true);
  assert.equal(candidateManagerEmailE2EProofInternals.proofEnabled({
    CANDIDATE_MANAGER_E2E_PROOF_ENABLED: 'TRUE', CANDIDATE_APP_ENVIRONMENT: 'LIVE'
  }), false);
  assert.equal(candidateManagerEmailE2EProofInternals.proofEnabled({
    CANDIDATE_MANAGER_E2E_PROOF_ENABLED: 'FALSE', CANDIDATE_APP_ENVIRONMENT: 'TEST'
  }), false);

  const disabled = await handleCandidateManagerEmailE2EProof(new Request(
    `https://backend.test${CANDIDATE_MANAGER_EMAIL_E2E_PROOF_PATH}`, { method: 'POST' }
  ), { CANDIDATE_APP_ENVIRONMENT: 'TEST' }, {}, {});
  assert.equal(disabled.status, 404);
  assert.equal((await disabled.json()).error_code, 'E2E_PROOF_DISABLED');
});

test('manager email E2E proof date fixture is one bounded weekly submission', () => {
  const dates = candidateManagerEmailE2EProofInternals.proofDates(
    new Date('2026-08-23T12:00:00.000Z')
  );
  assert.deepEqual(dates, {
    weekEnding: '2026-08-23',
    workDate: '2026-08-21',
    contractStart: '2026-07-22',
    contractEnd: '2026-09-22',
    weekEndingWeekday: 0
  });
  const signature = candidateManagerEmailE2EProofInternals.candidateSignatureBytes();
  assert.ok(signature.byteLength > 32);
  assert.deepEqual(Array.from(signature.slice(0, 8)), [137, 80, 78, 71, 13, 10, 26, 10]);
});

test('manager email E2E proof diagnostics redact every credential-shaped value', () => {
  const diagnostic = candidateManagerEmailE2EProofInternals.safeProofDiagnostic(new Error(
    'failed https://example.test/path#token=secret for f1000000-0000-4000-8000-000000000008 '
    + `${'a'.repeat(64)} and private@example.test`
  ));
  assert.equal(diagnostic.includes('secret'), false);
  assert.equal(diagnostic.includes('f1000000'), false);
  assert.equal(diagnostic.includes('private@example.test'), false);
  assert.equal(diagnostic.includes('a'.repeat(64)), false);
  assert.match(diagnostic, /\[url\]/);
  assert.match(diagnostic, /\[uuid\]/);
  assert.match(diagnostic, /\[sha256\]/);
  assert.match(diagnostic, /\[email\]/);
});

test('manager email E2E proof accepts only the current routed manager outbox shape', async () => {
  const originalFetch = globalThis.fetch;
  const outboxId = 'f1000000-0000-4000-8000-000000000009';
  globalThis.fetch = async (input) => {
    const url = input instanceof Request ? input.url : String(input);
    if (url.endsWith('/rest/v1/rpc/manager_review_origin_resolve_v1')) {
      return Response.json({ manager_review_public_origin: 'https://manager.test.invalid' });
    }
    assert.match(url, new RegExp(`/rest/v1/mail_outbox\\?id=eq\\.${outboxId}`));
    return Response.json([{
      body_text: `Review and approve: https://manager.test.invalid/manager/timesheet/${CANDIDATE_MANAGER_EMAIL_E2E_IDS.workflow}#token=opaque-proof-token`,
      status: 'QUEUED',
      email_type: 'CANDIDATE_APP_TRANSACTIONAL',
      context_kind: 'CANDIDATE_WORKFLOW',
      context_id: CANDIDATE_MANAGER_EMAIL_E2E_IDS.workflow,
      payment_scope_json: {
        candidate_manager_workflow_id: CANDIDATE_MANAGER_EMAIL_E2E_IDS.workflow,
        candidate_manager_mail_kind: 'INITIAL'
      }
    }]);
  };
  try {
    const result = await candidateManagerEmailE2EProofInternals.managerLinkFromOutbox({
      MYTMS_CONTROL_PLANE_ENABLED: 'TRUE',
      MYTMS_CONTROL_PLANE_URL: 'https://control.test.invalid',
      MYTMS_CONTROL_PLANE_SERVICE_ROLE_KEY: 'test-only-control-key',
      MYTMS_OFFICE_AGENCY_ID: 'f1000000-0000-4000-8000-000000000010',
      SUPABASE_URL: 'https://agency.test.invalid',
      SUPABASE_SERVICE_ROLE_KEY: 'test-only-agency-key'
    }, outboxId);
    assert.equal(result.providerSendPerformed, false);
    assert.equal(result.managerUrl.includes('#token=opaque-proof-token'), true);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
