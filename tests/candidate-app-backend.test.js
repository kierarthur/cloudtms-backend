import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { PDFDocument } from 'pdf-lib';

import {
  candidateAppBackendInternals,
  handleCandidateAppRequest,
  processPendingCandidatePaperPacks
} from '../broker/src/candidate-app-backend.js';

const {
  deferBackground,
  derivePasswordVerifier,
  deterministicOpaqueToken,
  explicitNoBreak,
  finaliseReceivedPaperReturn,
  forbiddenFinancialKeys,
  officialPresentationFromRows,
  immutablePut,
  preparedUploadContract,
  expenseSummaryDisplayLines,
  mileageJourneyRows,
  paperPackIdentity,
  candidatePaperDeliveryGeneration,
  candidatePaperCompleteReceipt,
  readyPaperPackReceipt,
  readyGeneratedDocumentReceipt,
  releaseCandidatePaperPack,
  bindCandidatePaperOutbox,
  assembleCandidatePaperPack,
  renderAndRegister,
  candidateDocumentBranding,
  createAccessToken,
  mileageClaimFormBytes,
  officeErrorCode,
  renderExpensePage,
  routeMatch,
  safeFinalisationResult,
  safeQrPackResponse,
  segmentBreak,
  uploadTicket,
  validateComponentBytes,
  verifyUploadTicket,
  withoutInternalRenderContracts,
  verifyPassword
} = candidateAppBackendInternals;

function noLogoBranding(agencyName = 'Configured Agency') {
  const base = {
    contract_version: 'CANDIDATE_DOCUMENT_BRANDING_V1',
    agency_name: agencyName,
    logo_key: null,
    logo_sha256: null,
    logo_media_type: null
  };
  return {
    ...base,
    branding_contract_sha256: createHash('sha256').update(JSON.stringify(base)).digest('hex')
  };
}

function readyPaperScope(workflowId, generation, manifestHash, complete = {}) {
  return {
    candidate_workflow_id: workflowId,
    candidate_workflow_generation: generation,
    paper_return_manifest_sha256: manifestHash,
    candidate_paper_pack_ready: true,
    mail_held_until_pdf_rendered: false,
    mail_hold_reason: null,
    candidate_complete_pack_storage_key: complete.key || 'candidate-app/test/pack.pdf',
    candidate_complete_pack_sha256: complete.sha256 || 'e'.repeat(64),
    candidate_complete_pack_size_bytes: complete.byte_size || 500,
    candidate_complete_pack_page_count: complete.page_count || 2,
    candidate_complete_pack_media_type: 'application/pdf'
  };
}

function readyPaperAttachment(workflowId, generation, manifestHash, complete = {}) {
  return {
    r2_key: complete.key || 'candidate-app/test/pack.pdf',
    sha256: complete.sha256 || 'e'.repeat(64),
    size_bytes: complete.byte_size || 500,
    page_count: complete.page_count || 2,
    content_type: 'application/pdf',
    candidate_workflow_id: workflowId,
    candidate_workflow_generation: generation,
    paper_return_manifest_sha256: manifestHash
  };
}

function completePaperFixture(overrides = {}) {
  return {
    key: 'candidate-app/test/pack.pdf',
    sha256: 'e'.repeat(64),
    byte_size: 500,
    page_count: 2,
    manifest_hash: 'd'.repeat(64),
    base_hash: 'c'.repeat(64),
    branding_hash: 'f'.repeat(64),
    renderer_contract_version: 'CANDIDATE_REVIEW_DOCUMENTS_V1',
    ...overrides
  };
}

test('Candidate password verifiers accept the exact password and reject a different password', async () => {
  const verifier = await derivePasswordVerifier('correct-horse-battery-staple');
  const account = {
    password_scheme: verifier.scheme,
    password_scheme_version: verifier.scheme_version,
    password_salt: verifier.salt_hex,
    password_digest: verifier.digest_hex,
    password_params_json: verifier.params
  };
  assert.equal(await verifyPassword('correct-horse-battery-staple', account), true);
  assert.equal(await verifyPassword('different-horse-battery-staple', account), false);
});

test('challenge delivery tokens are stable for an idempotent replay and scoped to its identity', async () => {
  const first = await deterministicOpaqueToken('test-secret', 'challenge', 'TEST', 'ACTIVATE', 'person@example.test', 'key-1');
  const replay = await deterministicOpaqueToken('test-secret', 'challenge', 'TEST', 'ACTIVATE', 'person@example.test', 'key-1');
  const other = await deterministicOpaqueToken('test-secret', 'challenge', 'TEST', 'ACTIVATE', 'person@example.test', 'key-2');
  assert.equal(first, replay);
  assert.notEqual(first, other);
  assert.equal(first.includes('='), false);
});

test('Candidate payload validation rejects canonical financial truth but accepts factual claim amounts', () => {
  assert.deepEqual(forbiddenFinancialKeys({
    canonical_tsfin_snapshot: { total_pay_ex_vat: 100 },
    nested: { margin_ex_vat: 5, invoice_breakdown_json: [] }
  }), [
    'canonical_tsfin_snapshot',
    'canonical_tsfin_snapshot.total_pay_ex_vat',
    'nested.margin_ex_vat',
    'nested.invoice_breakdown_json'
  ]);
  assert.deepEqual(forbiddenFinancialKeys({
    mileage_units: 12,
    travel_amount: 18.5,
    accommodation_amount: 0,
    other_amount: 4,
    description: 'Parking'
  }), []);
});

test('explicit no-break input is represented as zero minutes with no interval', () => {
  assert.deepEqual(segmentBreak({ no_break: true, break_minutes: 0 }), {
    break_start_local: '',
    break_end_local: '',
    break_minutes: 0,
    break_display_mode: 'NONE'
  });
});

test('blank or null break values are not silently converted into a no-break declaration', () => {
  assert.equal(explicitNoBreak({ break_minutes: null }), false);
  assert.equal(explicitNoBreak({ break_minutes: '' }), false);
  assert.equal(explicitNoBreak({}), false);
  assert.equal(explicitNoBreak({ break_minutes: 0 }), true);
  assert.equal(explicitNoBreak({ no_break: true }), true);
});

test('Candidate route matching decodes stable path parameters and rejects partial paths', () => {
  assert.deepEqual(
    routeMatch('/candidate-app/v1/workflows/abc%201/actions/worker-submit', '/candidate-app/v1/workflows/:workflowId/actions/:action'),
    { workflowId: 'abc 1', action: 'worker-submit' }
  );
  assert.equal(routeMatch('/candidate-app/v1/workflows/a', '/candidate-app/v1/workflows/:workflowId/actions/:action'), null);
  assert.deepEqual(
    routeMatch(
      '/candidate-app/v1/timesheets/00000000-0000-4000-8000-000000000001/paper-pack',
      '/candidate-app/v1/timesheets/:timesheetId/paper-pack'
    ),
    { timesheetId: '00000000-0000-4000-8000-000000000001' }
  );
  assert.deepEqual(
    routeMatch(
      '/candidate-app/v1/timesheets/00000000-0000-4000-8000-000000000001/paper-pack/status',
      '/candidate-app/v1/timesheets/:timesheetId/paper-pack/status'
    ),
    { timesheetId: '00000000-0000-4000-8000-000000000001' }
  );
});

test('timesheet page boundary defaults to Current and validates the explicit History view', async () => {
  const sessionId = '00000000-0000-4000-8000-000000000071';
  const candidateId = '00000000-0000-4000-8000-000000000072';
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'test-only-secret-material',
    SUPABASE_URL: 'https://test.example.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  const session = {
    session_id: sessionId,
    id: sessionId,
    account_id: '00000000-0000-4000-8000-000000000073',
    selected_candidate_id: candidateId,
    environment: 'TEST',
    status: 'ACTIVE',
    rotation: 2,
    expires_at_utc: '2099-01-01T00:00:00.000Z',
    absolute_expires_at_utc: '2099-01-02T00:00:00.000Z'
  };
  const token = await createAccessToken(env, session);
  const originalFetch = globalThis.fetch;
  const rpcCalls = [];
  globalThis.fetch = async url => {
    assert.match(String(url), /candidate_app_sessions/);
    return Response.json([session]);
  };
  const deps = {
    routeAudience: 'PRIVATE',
    async rpc(name, args) {
      rpcCalls.push({ name, args });
      return { ok: true, view: args.p_view, items: [] };
    }
  };
  try {
    for (const [query, expected] of [['', 'CURRENT'], ['?view=history&limit=25', 'HISTORY']]) {
      const response = await handleCandidateAppRequest(new Request(
        `https://private.test/candidate-app/v1/timesheets${query}`,
        { headers: { authorization: `Bearer ${token}` } }
      ), env, {}, deps);
      assert.equal(response.status, 200);
      assert.equal((await response.json()).view, expected);
    }
    assert.equal(rpcCalls.length, 2);
    assert.equal(rpcCalls[0].name, 'candidate_app_timesheet_page_v1');
    assert.equal(rpcCalls[0].args.p_view, 'CURRENT');
    assert.equal(rpcCalls[0].args.p_limit, 50);
    assert.equal(rpcCalls[1].args.p_view, 'HISTORY');
    assert.equal(rpcCalls[1].args.p_limit, 25);

    const invalid = await handleCandidateAppRequest(new Request(
      'https://private.test/candidate-app/v1/timesheets?view=other',
      { headers: { authorization: `Bearer ${token}` } }
    ), env, {}, deps);
    assert.equal(invalid.status, 400);
    assert.equal((await invalid.json()).error_code, 'CANDIDATE_VIEW_INVALID');
    assert.equal(rpcCalls.length, 2);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('timesheet detail aliases pass one exact server identity to the shared detail RPC', async () => {
  const sessionId = '00000000-0000-4000-8000-000000000081';
  const contractWeekId = '00000000-0000-4000-8000-000000000082';
  const workflowId = '00000000-0000-4000-8000-000000000083';
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'test-only-secret-material',
    SUPABASE_URL: 'https://test.example.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  const session = {
    session_id: sessionId,
    id: sessionId,
    account_id: '00000000-0000-4000-8000-000000000084',
    selected_candidate_id: '00000000-0000-4000-8000-000000000085',
    environment: 'TEST', status: 'ACTIVE', rotation: 1,
    expires_at_utc: '2099-01-01T00:00:00.000Z',
    absolute_expires_at_utc: '2099-01-02T00:00:00.000Z'
  };
  const token = await createAccessToken(env, session);
  const originalFetch = globalThis.fetch;
  const rpcCalls = [];
  globalThis.fetch = async () => Response.json([session]);
  const deps = {
    routeAudience: 'PRIVATE',
    async rpc(name, args) { rpcCalls.push({ name, args }); return { ok: true }; }
  };
  try {
    for (const path of [
      `/candidate-app/v1/contract-weeks/${contractWeekId}/detail`,
      `/candidate-app/v1/workflows/${workflowId}/timesheet-detail`
    ]) {
      const response = await handleCandidateAppRequest(new Request(`https://private.test${path}`, {
        headers: { authorization: `Bearer ${token}` }
      }), env, {}, deps);
      assert.equal(response.status, 200);
    }
    assert.equal(rpcCalls.length, 2);
    assert.equal(rpcCalls[0].name, 'candidate_app_timesheet_detail_v1');
    assert.equal(rpcCalls[0].args.p_contract_week_id, contractWeekId);
    assert.equal(rpcCalls[0].args.p_workflow_id, null);
    assert.equal(rpcCalls[1].args.p_contract_week_id, null);
    assert.equal(rpcCalls[1].args.p_workflow_id, workflowId);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('rejected resubmission is a thin adapter over the atomic source-bound database action', async () => {
  const sessionId = '00000000-0000-4000-8000-0000000000a1';
  const accountId = '00000000-0000-4000-8000-0000000000a2';
  const candidateId = '00000000-0000-4000-8000-0000000000a3';
  const workflowId = '00000000-0000-4000-8000-0000000000a4';
  const idempotencyKey = '00000000-0000-4000-8000-0000000000a7';
  const replacementId = '00000000-0000-4000-8000-0000000000a8';
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'test-only-secret-material',
    SUPABASE_URL: 'https://test.example.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  const session = {
    session_id: sessionId, id: sessionId, account_id: accountId,
    selected_candidate_id: candidateId, environment: 'TEST', status: 'ACTIVE', rotation: 1,
    expires_at_utc: '2099-01-01T00:00:00.000Z',
    absolute_expires_at_utc: '2099-01-02T00:00:00.000Z'
  };
  const token = await createAccessToken(env, session);
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async url => {
    const value = String(url);
    if (value.includes('candidate_app_sessions')) return Response.json([session]);
    throw new Error(`unexpected fetch ${value}`);
  };
  const calls = [];
  const deps = {
    routeAudience: 'PRIVATE',
    async rpc(name, args) {
      calls.push({ name, args });
      return {
        ok: true, idempotent_replay: calls.length > 1,
        rejected_workflow_id: workflowId, replacement_workflow_id: replacementId,
        replacement_created: calls.length === 1, workflow_id: replacementId,
        state: 'WORKER_DRAFT', generation: 1
      };
    }
  };
  try {
    const response = await handleCandidateAppRequest(new Request(
      `https://private.test/candidate-app/v1/workflows/${workflowId}/resubmit`, {
        method: 'POST', headers: {
          authorization: `Bearer ${token}`, 'content-type': 'application/json'
        },
        body: JSON.stringify({
          generation: 4, idempotency_key: idempotencyKey,
          workflow: { workflow_kind: 'CONTRACT_HOURS', route: 'ELECTRONIC' }
        })
      }
    ), env, {}, deps);
    assert.equal(response.status, 201);
    const payload = await response.json();
    assert.equal(payload.rejected_workflow_id, workflowId);
    assert.equal(payload.replacement_workflow_id, replacementId);
    assert.equal(payload.replacement_created, true);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].name, 'candidate_workflow_transition_atomic_v1');
    assert.equal(calls[0].args.p_action, 'RESUBMIT_REJECTED');
    assert.equal(calls[0].args.p_workflow_id, workflowId);
    assert.equal(calls[0].args.p_expected_generation, 4);
    assert.deepEqual(calls[0].args.p_payload, {});
    assert.equal(calls[0].args.p_idempotency_key, idempotencyKey);
    const replayResponse = await handleCandidateAppRequest(new Request(
      `https://private.test/candidate-app/v1/workflows/${workflowId}/resubmit`, {
        method: 'POST', headers: {
          authorization: `Bearer ${token}`, 'content-type': 'application/json'
        }, body: JSON.stringify({ generation: 4, idempotency_key: idempotencyKey })
      }
    ), env, {}, deps);
    assert.equal(replayResponse.status, 201);
    const replayBody = await replayResponse.json();
    assert.equal(replayBody.idempotent_replay, true);
    assert.equal(replayBody.replacement_workflow_id, replacementId);
    assert.equal(calls.length, 2);
    assert.equal(calls[1].args.p_action, 'RESUBMIT_REJECTED');
    assert.equal(calls[1].args.p_workflow_id, workflowId);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('public reminder stays REMIND and cancellation requires and forwards a reason', async () => {
  const sessionId = '00000000-0000-4000-8000-000000000091';
  const accountId = '00000000-0000-4000-8000-000000000092';
  const candidateId = '00000000-0000-4000-8000-000000000093';
  const workflowId = '00000000-0000-4000-8000-000000000094';
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'test-only-secret-material',
    CANDIDATE_APP_PUBLIC_URL: 'https://candidate.test.invalid',
    SUPABASE_URL: 'https://test.example.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  const session = {
    session_id: sessionId, id: sessionId, account_id: accountId,
    selected_candidate_id: candidateId, environment: 'TEST', status: 'ACTIVE', rotation: 1,
    expires_at_utc: '2099-01-01T00:00:00.000Z',
    absolute_expires_at_utc: '2099-01-02T00:00:00.000Z'
  };
  const token = await createAccessToken(env, session);
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async url => {
    const value = String(url);
    if (value.includes('candidate_app_sessions')) return Response.json([session]);
    if (value.includes('candidate_approval_requests')) {
      return Response.json([{ manager_email_normalized: 'manager@example.test' }]);
    }
    throw new Error(`unexpected fetch ${value}`);
  };
  const calls = [];
  const deps = {
    routeAudience: 'PRIVATE',
    async rpc(name, args) { calls.push({ name, args }); return { ok: true, state: 'AWAITING_MANAGER_APPROVAL' }; }
  };
  const request = (action, body) => new Request(
    `https://private.test/candidate-app/v1/workflows/${workflowId}/actions/${action}`,
    {
      method: 'POST', headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
      body: JSON.stringify(body)
    }
  );
  try {
    const reminder = await handleCandidateAppRequest(request('remind', {
      generation: 3, idempotency_key: 'reminder-1'
    }), env, {}, deps);
    assert.equal(reminder.status, 200);
    assert.equal(calls[0].name, 'candidate_workflow_transition_atomic_v1');
    assert.equal(calls[0].args.p_action, 'REMIND');
    assert.match(calls[0].args.p_payload.mail.subject, /^Reminder:/);
    assert.match(calls[0].args.p_payload.approval_token_hash_hex, /^[0-9a-f]{64}$/);

    const missingReason = await handleCandidateAppRequest(request('cancel', {
      generation: 3, idempotency_key: 'cancel-1'
    }), env, {}, deps);
    assert.equal(missingReason.status, 400);
    assert.equal((await missingReason.json()).error_code, 'CANDIDATE_CANCELLATION_REASON_REQUIRED');
    assert.equal(calls.length, 1);

    const cancelled = await handleCandidateAppRequest(request('cancel', {
      generation: 3, idempotency_key: 'cancel-2', reason_note: 'I entered the wrong week.'
    }), env, {}, deps);
    assert.equal(cancelled.status, 200);
    assert.equal(calls[1].args.p_action, 'CANCEL');
    assert.equal(calls[1].args.p_payload.reason_note, 'I entered the wrong week.');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('Candidate HTTP boundary ignores unrelated routes and fails protected routes closed', async () => {
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'test-only-secret-material'
  };
  const deps = { routeAudience: 'PRIVATE', rpc: async () => { throw new Error('unexpected RPC'); } };
  assert.equal(await handleCandidateAppRequest(
    new Request('https://backend.test/healthz'), env, {}, deps
  ), null);
  const response = await handleCandidateAppRequest(
    new Request('https://backend.test/candidate-app/v1/bootstrap'), env, {}, deps
  );
  assert.equal(response.status, 401);
  const payload = await response.json();
  assert.equal(payload.error_code, 'CANDIDATE_ACCESS_TOKEN_INVALID');
});

test('normal CloudTMS office audience cannot expose Candidate or manager public routes', async () => {
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'test-only-secret-material'
  };
  const request = new Request('https://backend.test/candidate-app/v1/bootstrap');
  assert.equal(await handleCandidateAppRequest(request.clone(), env, {}, { routeAudience: 'OFFICE' }), null);
  assert.equal(await handleCandidateAppRequest(request.clone(), env, {}, {}), null);
});

test('normal office Candidate endpoints enforce exact methods and use one service adapter call', async () => {
  const actorId = '00000000-0000-4000-8000-000000000201';
  const timesheetId = '00000000-0000-4000-8000-000000000202';
  const calls = [];
  const deps = {
    routeAudience: 'OFFICE',
    async requireOfficeUser() { return { id: actorId, role: 'admin' }; },
    async rpc(name, args) {
      calls.push({ name, args });
      return name === 'cloudtms_office_candidate_adapter_v1'
        ? { ok: true, contract_version: 'OFFICE_CANDIDATE_TIMESHEET_V1' } : { ok: true };
    }
  };
  const env = { CANDIDATE_APP_ENVIRONMENT: 'TEST' };
  const detail = await handleCandidateAppRequest(new Request(
    `https://office.test/api/candidate-app/timesheets/${timesheetId}/office-detail?row_key=row-1`,
    { method: 'GET' }
  ), env, {}, deps);
  assert.equal(detail.status, 200);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].name, 'cloudtms_office_candidate_adapter_v1');
  assert.equal(calls[0].args.p_action, 'PROJECT_ONE');
  assert.equal(calls[0].args.p_actor_user_id, actorId);
  assert.equal(calls[0].args.p_payload.timesheet_id, timesheetId);
  assert.equal(calls[0].args.p_payload.row_key, 'row-1');

  const wrongMethod = await handleCandidateAppRequest(new Request(
    `https://office.test/api/candidate-app/timesheets/${timesheetId}/office-detail`,
    { method: 'POST' }
  ), env, {}, deps);
  assert.equal(wrongMethod.status, 405);
  assert.equal((await wrongMethod.json()).error_code, 'METHOD_NOT_ALLOWED');
  assert.equal(calls.length, 1, 'wrong method must not reach an RPC');
});

test('office batch projection is bounded and never fans out into per-row RPC calls', async () => {
  const calls = [];
  const deps = {
    routeAudience: 'OFFICE',
    async requireOfficeUser() { return { id: '00000000-0000-4000-8000-000000000211' }; },
    async rpc(name, args) {
      calls.push({ name, args });
      return { ok: true, result_count: args.p_payload.identities.length, results: [] };
    }
  };
  const rows = [1, 2, 3].map(value => ({
    row_key: `row-${value}`,
    timesheet_id: `00000000-0000-4000-8000-${String(220 + value).padStart(12, '0')}`,
    expected_row_signature: `signature-${value}`
  }));
  const response = await handleCandidateAppRequest(new Request(
    'https://office.test/api/candidate-app/timesheets/office-projections', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ surface: 'TIMESHEET_SUMMARY', selected_rows: rows })
    }
  ), { CANDIDATE_APP_ENVIRONMENT: 'TEST' }, {}, deps);
  assert.equal(response.status, 200);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].args.p_action, 'PROJECT_BATCH');
  assert.equal(calls[0].args.p_payload.identities.length, 3);
});

test('office route confirmation requires a caller-owned UUID before any mutation RPC', async () => {
  const calls = [];
  const deps = {
    routeAudience: 'OFFICE',
    async requireOfficeUser() { return { id: '00000000-0000-4000-8000-000000000226' }; },
    async rpc(name, args) { calls.push({ name, args }); return { ok: true }; }
  };
  const timesheetId = '00000000-0000-4000-8000-000000000227';
  const response = await handleCandidateAppRequest(new Request(
    `https://office.test/api/candidate-app/timesheets/${timesheetId}/route-confirm`, {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        expected_timesheet_id: timesheetId,
        expected_row_signature: 'row-signature',
        expected_context_sha256: 'a'.repeat(64),
        action: 'SWITCH_TO_MANUAL'
      })
    }
  ), { CANDIDATE_APP_ENVIRONMENT: 'TEST' }, {}, deps);
  assert.equal(response.status, 400);
  assert.equal((await response.json()).error_code, 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED');
  assert.equal(calls.length, 0);
});

test('office rejection requires a bounded reason before the canonical confirmation call', async () => {
  const actorId = '00000000-0000-4000-8000-000000000224';
  const timesheetId = '00000000-0000-4000-8000-000000000225';
  const idempotencyKey = '00000000-0000-4000-8000-000000000226';
  const calls = [];
  const deps = {
    routeAudience: 'OFFICE',
    async requireOfficeUser() { return { id: actorId }; },
    async rpc(name, args) {
      calls.push({ name, args });
      return { ok: true, contract_version: 'OFFICE_CANDIDATE_REJECTION_RESULT_V1' };
    }
  };
  const request = body => new Request(
    `https://office.test/api/candidate-app/timesheets/${timesheetId}/reject`, {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body)
    }
  );
  const identity = {
    expected_timesheet_id: timesheetId,
    expected_row_signature: 'row-signature',
    context_sha256: 'a'.repeat(64),
    idempotency_key: idempotencyKey
  };

  const missing = await handleCandidateAppRequest(
    request(identity), { CANDIDATE_APP_ENVIRONMENT: 'TEST' }, {}, deps
  );
  assert.equal(missing.status, 400);
  assert.equal((await missing.json()).error_code, 'CANDIDATE_REASON_REQUIRED');
  assert.equal(calls.length, 0);

  const oversized = await handleCandidateAppRequest(
    request({ ...identity, reason: 'x'.repeat(1001) }),
    { CANDIDATE_APP_ENVIRONMENT: 'TEST' }, {}, deps
  );
  assert.equal(oversized.status, 400);
  assert.equal((await oversized.json()).error_code, 'CANDIDATE_REASON_INVALID');
  assert.equal(calls.length, 0);

  const valid = await handleCandidateAppRequest(
    request({ ...identity, reason: 'The candidate must correct the submitted hours.' }),
    { CANDIDATE_APP_ENVIRONMENT: 'TEST' }, {}, deps
  );
  assert.equal(valid.status, 200);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].name, 'cloudtms_office_candidate_adapter_v1');
  assert.equal(calls[0].args.p_action, 'REJECT_CONFIRM');
  assert.equal(calls[0].args.p_payload.reason, 'The candidate must correct the submitted hours.');
});

test('office errors use stable aliases without changing underlying lifecycle codes', () => {
  assert.equal(officeErrorCode(new Error('IDEMPOTENCY_CONFLICT')), 'CANDIDATE_IDEMPOTENCY_CONFLICT');
  assert.equal(officeErrorCode(new Error('ROW_SIGNATURE_MISMATCH')), 'CANDIDATE_CONTEXT_STALE');
  assert.equal(officeErrorCode(new Error('TIMESHEET_MOVED')), 'CANDIDATE_TIMESHEET_MOVED');
  assert.equal(officeErrorCode(new Error('CANDIDATE_PAPER_MAIL_DELIVERY_IN_PROGRESS')),
    'CANDIDATE_PROVIDER_HANDOFF_IN_PROGRESS');
  assert.equal(officeErrorCode(new Error('CANDIDATE_REJECT_REQUIRES_UNAUTHORISE')),
    'CANDIDATE_REQUIRES_UNAUTHORISE');
  assert.equal(officeErrorCode(new Error('CANDIDATE_REJECT_PROTECTED_HISTORY')),
    'CANDIDATE_PROTECTED_FINANCIAL_HISTORY');
  assert.equal(officeErrorCode(new Error('CANDIDATE_WORKFLOW_NOT_FOUND')), 'CANDIDATE_WORKFLOW_NOT_FOUND');
});

test('office W07 route preview returns the server-owned reject-versus-manual decision only for a Candidate scope', async () => {
  const actorId = '00000000-0000-4000-8000-000000000228';
  const timesheetId = '00000000-0000-4000-8000-000000000229';
  const workflowId = '00000000-0000-4000-8000-00000000022a';
  let includeScope = true;
  const calls = [];
  const deps = {
    routeAudience: 'OFFICE',
    async requireOfficeUser() { return { id: actorId }; },
    async rpc(name, args) {
      calls.push({ name, args });
      if (name === 'timesheet_route_version_preview_v1') {
        return { ok: true, action: 'SWITCH_TO_MANUAL', expected_timesheet_id: timesheetId };
      }
      assert.equal(name, 'cloudtms_office_candidate_adapter_v1');
      assert.equal(args.p_action, 'REJECT_PREVIEW');
      return includeScope ? {
        permitted: true,
        target_workflows: [{ workflow_id: workflowId, generation: 3 }]
      } : { permitted: false, target_workflows: [] };
    }
  };
  const request = () => new Request(
    `https://office.test/api/candidate-app/timesheets/${timesheetId}/route-preview?action=SWITCH_TO_MANUAL`
  );
  const first = await handleCandidateAppRequest(request(), { CANDIDATE_APP_ENVIRONMENT: 'TEST' }, {}, deps);
  assert.equal(first.status, 200);
  const decision = (await first.json()).intervention_choice;
  assert.equal(decision.required, true);
  assert.equal(decision.decision_code, 'REJECT_OR_MANUAL');
  assert.equal(decision.title, 'Does the candidate need to resubmit instead?');
  assert.match(decision.message, /^Use Reject Candidate Submission/);
  assert.match(decision.message, /Convert to Manual only/);
  assert.equal(decision.reject_available, true);
  assert.equal(decision.reject_action.label, 'Use Reject Candidate Submission');
  assert.equal(decision.reject_action.method, 'GET');
  assert.equal(decision.continue_action.label, 'Continue to Manual conversion');
  assert.equal(decision.continue_action.method, 'POST');
  assert.deepEqual(decision.continue_action.fixed_body, { action: 'SWITCH_TO_MANUAL' });

  includeScope = false;
  const second = await handleCandidateAppRequest(request(), { CANDIDATE_APP_ENVIRONMENT: 'TEST' }, {}, deps);
  assert.equal(second.status, 200);
  assert.equal((await second.json()).intervention_choice, null);
  includeScope = true;
  const originalRpc = deps.rpc;
  deps.rpc = async (name, args) => {
    const result = await originalRpc(name, args);
    return name === 'cloudtms_office_candidate_adapter_v1'
      ? { ...result, permitted: false, disabled_reason_code: 'CANDIDATE_REQUIRES_UNAUTHORISE' }
      : result;
  };
  const third = await handleCandidateAppRequest(request(), { CANDIDATE_APP_ENVIRONMENT: 'TEST' }, {}, deps);
  assert.equal(third.status, 200);
  assert.equal((await third.json()).intervention_choice, null);
  assert.equal(calls.length, 6);
});

test('office W07 route preview fails closed when Candidate scope authority is unavailable', async () => {
  const timesheetId = '00000000-0000-4000-8000-00000000022d';
  let calls = 0;
  const response = await handleCandidateAppRequest(new Request(
    `https://office.test/api/candidate-app/timesheets/${timesheetId}/route-preview?action=SWITCH_TO_MANUAL`
  ), { CANDIDATE_APP_ENVIRONMENT: 'TEST' }, {}, {
    routeAudience: 'OFFICE',
    async requireOfficeUser() { return { id: '00000000-0000-4000-8000-00000000022e' }; },
    async rpc(name) {
      calls += 1;
      if (name === 'timesheet_route_version_preview_v1') return { ok: true };
      throw new Error('unexpected adapter outage');
    }
  });
  assert.equal(response.status, 500);
  assert.equal((await response.json()).error_code, 'CANDIDATE_REQUEST_FAILED');
  assert.equal(calls, 2);
});

test('office PAPER history uses the preceding immutable delivery generation and retained receipt', () => {
  const workflowId = '00000000-0000-4000-8000-00000000022b';
  const manifestHash = 'a'.repeat(64);
  const baseHash = 'b'.repeat(64);
  const brandingHash = 'c'.repeat(64);
  const packHash = 'd'.repeat(64);
  const workflow = {
    id: workflowId,
    generation: 2,
    state: 'FINALISED',
    paper_return_manifest_sha256: manifestHash
  };
  assert.equal(candidatePaperDeliveryGeneration(workflow), 1);
  assert.equal(candidatePaperDeliveryGeneration({ ...workflow, state: 'RECEIVED' }), 2);
  const complete = {
    key: `candidate-app/test/${workflowId}/1/paper-pack/`
      + `${manifestHash}-${baseHash}-${brandingHash}-CANDIDATE_REVIEW_DOCUMENTS_V1.pdf`,
    sha256: packHash,
    byte_size: 321,
    page_count: 4
  };
  const activeScope = {
    ...readyPaperScope(workflowId, 1, manifestHash, complete),
    candidate_mail_authority: 'CANDIDATE_PAPER_V1',
    base_document_sha256: baseHash,
    branding_contract_sha256: brandingHash,
    renderer_contract_version: 'CANDIDATE_REVIEW_DOCUMENTS_V1'
  };
  const attachment = readyPaperAttachment(workflowId, 1, manifestHash, complete);
  const retired = {
    context_id: '00000000-0000-4000-8000-00000000022c',
    status: 'QUEUED',
    attachments: [],
    payment_scope_json: {
      ...activeScope,
      candidate_paper_generation_retired: true,
      candidate_paper_pack_ready: false,
      mail_held_until_pdf_rendered: true,
      mail_hold_reason: 'CANDIDATE_PAPER_GENERATION_RETIRED',
      candidate_retired_delivery_receipt: {
        attachments: [attachment],
        candidate_complete_pack_storage_key: complete.key,
        candidate_complete_pack_sha256: complete.sha256,
        candidate_complete_pack_size_bytes: complete.byte_size,
        candidate_complete_pack_page_count: complete.page_count,
        candidate_complete_pack_media_type: 'application/pdf'
      }
    }
  };
  const receipt = candidatePaperCompleteReceipt(
    { CANDIDATE_APP_ENVIRONMENT: 'TEST' }, workflow, 1, retired
  );
  assert.equal(receipt.ready, true);
  assert.equal(receipt.retired, true);
  assert.equal(receipt.delivery_generation, 1);
  assert.equal(receipt.key, complete.key);
  assert.equal(receipt.sha256, packHash);
});

test('office phone review sends only declared typed fields to the service adapter', async () => {
  const actorId = '00000000-0000-4000-8000-000000000241';
  const workflowId = '00000000-0000-4000-8000-000000000242';
  const approvalId = '00000000-0000-4000-8000-000000000243';
  const componentId = '00000000-0000-4000-8000-000000000244';
  const idempotencyKey = '00000000-0000-4000-8000-000000000245';
  const manifestHash = 'b'.repeat(64);
  const componentHash = 'c'.repeat(64);
  const calls = [];
  const deps = {
    routeAudience: 'OFFICE',
    async requireOfficeUser() { return { id: actorId }; },
    async rpc(name, args) { calls.push({ name, args }); return { ok: true }; }
  };
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async url => {
    const value = String(url);
    if (value.includes('candidate_submission_workflows')) return Response.json([{
      id: workflowId, generation: 4, route: 'PHONE', state: 'AWAITING_MANAGER_APPROVAL'
    }]);
    if (value.includes('candidate_approval_requests')) return Response.json([{
      id: approvalId, workflow_id: workflowId, workflow_generation: 4,
      request_generation: 2, method: 'PHONE', state: 'PENDING'
    }]);
    throw new Error(`unexpected URL: ${value}`);
  };
  try {
    const response = await handleCandidateAppRequest(new Request(
      `https://office.test/api/candidate-app/workflows/${workflowId}/actions/phone-progress`, {
        method: 'POST', headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          generation: 4, approval_request_id: approvalId, approval_request_generation: 2,
          idempotency_key: idempotencyKey, manifest_sha256_hex: manifestHash,
          component_id: componentId, component_sha256_hex: componentHash,
          viewed_receipt: { page_count: 1 },
          payload: { injected_authority: true }, injected_authority: true
        })
      }
    ), {
      CANDIDATE_APP_ENVIRONMENT: 'TEST', SUPABASE_URL: 'https://test.supabase.invalid',
      SUPABASE_SERVICE_ROLE_KEY: 'placeholder'
    }, {}, deps);
    assert.equal(response.status, 200);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].name, 'cloudtms_office_candidate_adapter_v1');
    assert.equal(calls[0].args.p_action, 'WORKFLOW_ACTION_EXECUTE');
    assert.deepEqual(calls[0].args.p_payload.payload, {
      manifest_sha256_hex: manifestHash,
      component_id: componentId,
      component_sha256_hex: componentHash,
      viewed_receipt: { page_count: 1 }
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('office signature preparation binds the exact phone request and returns an office upload route', async () => {
  const actorId = '00000000-0000-4000-8000-000000000251';
  const workflowId = '00000000-0000-4000-8000-000000000252';
  const approvalId = '00000000-0000-4000-8000-000000000253';
  const componentId = '00000000-0000-4000-8000-000000000254';
  const idempotencyKey = '00000000-0000-4000-8000-000000000255';
  const calls = [];
  const deps = {
    routeAudience: 'OFFICE',
    async requireOfficeUser() { return { id: actorId }; },
    async rpc(name, args) {
      calls.push({ name, args });
      return {
        ok: true, component_id: componentId, workflow_generation: 3,
        storage_key: 'candidate-app/test/workflow/3/manager-signature.png',
        media_type: 'image/png', byte_size: 128, component_kind: 'MANAGER_SIGNATURE',
        document_role: 'MANAGER_SIGNATURE', expense_category: null,
        paper_return_page_key: null, state: 'PENDING'
      };
    }
  };
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async url => {
    const value = String(url);
    if (value.includes('candidate_submission_workflows')) return Response.json([{
      id: workflowId, generation: 3, route: 'PHONE', state: 'AWAITING_MANAGER_APPROVAL'
    }]);
    if (value.includes('candidate_approval_requests')) return Response.json([{
      id: approvalId, workflow_id: workflowId, workflow_generation: 3,
      request_generation: 2, method: 'PHONE', state: 'PENDING'
    }]);
    throw new Error(`unexpected URL: ${value}`);
  };
  try {
    const response = await handleCandidateAppRequest(new Request(
      `https://office.test/api/candidate-app/workflows/${workflowId}/signature/prepare`, {
        method: 'POST', headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          generation: 3, approval_request_id: approvalId, approval_request_generation: 2,
          idempotency_key: idempotencyKey, media_type: 'image/png', byte_size: 128
        })
      }
    ), {
      CANDIDATE_APP_ENVIRONMENT: 'TEST', SUPABASE_URL: 'https://test.supabase.invalid',
      SUPABASE_SERVICE_ROLE_KEY: 'placeholder',
      CANDIDATE_PRIVATE_UPLOAD_TOKEN_SECRET: 'test-only-office-upload-secret'
    }, {}, deps);
    assert.equal(response.status, 201);
    const result = await response.json();
    assert.match(result.upload.url, /^\/api\/candidate-app\/uploads\//);
    assert.equal(Object.hasOwn(result.upload, 'storage_key'), false);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].args.p_action, 'COMPONENT_PREPARE');
    assert.equal(calls[0].args.p_payload.component_kind, 'MANAGER_SIGNATURE');
    assert.equal(calls[0].args.p_payload.document_role, 'MANAGER_SIGNATURE');
    assert.equal(calls[0].args.p_payload.approval_request_id, approvalId);
    assert.equal(calls[0].args.p_payload.actor_user_id, actorId);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('office reminder batch execute remains one browser operation with server-owned reminder material', async () => {
  const actorId = '00000000-0000-4000-8000-000000000231';
  const workflowId = '00000000-0000-4000-8000-000000000232';
  const requestId = '00000000-0000-4000-8000-000000000233';
  const batchId = '00000000-0000-4000-8000-000000000234';
  const fingerprint = 'a'.repeat(64);
  const identity = {
    row_key: 'row-1',
    timesheet_id: '00000000-0000-4000-8000-000000000235',
    expected_row_signature: 'sig-1'
  };
  const rpcCalls = [];
  let replayFound = false;
  const deps = {
    routeAudience: 'OFFICE',
    async requireOfficeUser() { return { id: actorId }; },
    async rpc(name, args) {
      rpcCalls.push({ name, args });
      if (args.p_action === 'REMINDER_BATCH_REPLAY') return replayFound
        ? { ok: true, found: true, idempotent_replay: true, batch_id: batchId, status: 'COMPLETED', items: [] }
        : { ok: true, found: false, batch_id: batchId };
      if (args.p_action === 'REMINDER_BATCH_PREVIEW') return {
        ok: true, preview_context_hash: fingerprint, selection_fingerprint: fingerprint,
        items: [{
          correlation_key: 'row-1', eligible: true, workflow_id: workflowId,
          workflow_generation: 2, approval_request_id: requestId,
          approval_request_generation: 3, row_signature: 'sig-1'
        }]
      };
      assert.equal(args.p_action, 'REMINDER_BATCH_EXECUTE');
      assert.equal(args.p_payload.reminders.length, 1);
      assert.match(args.p_payload.reminders[0].payload.approval_token_hash_hex, /^[a-f0-9]{64}$/);
      assert.equal(args.p_payload.reminders[0].payload.mail.to, 'manager@example.test');
      return { ok: true, batch_id: batchId, status: 'COMPLETED', items: [] };
    }
  };
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async url => {
    assert.match(String(url), /candidate_approval_requests/);
    return Response.json([{
      id: requestId, workflow_id: workflowId, workflow_generation: 2,
      request_generation: 3, method: 'EMAIL', manager_email_normalized: 'manager@example.test'
    }]);
  };
  try {
    const response = await handleCandidateAppRequest(new Request(
      'https://office.test/api/candidate-app/manager-reminder-batches', {
        method: 'POST', headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          selected_rows: [identity], batch_id: batchId, idempotency_key: batchId,
          preview_context_hash: fingerprint, selection_fingerprint: fingerprint
        })
      }
    ), {
      CANDIDATE_APP_ENVIRONMENT: 'TEST', SUPABASE_URL: 'https://test.supabase.invalid',
      SUPABASE_SERVICE_ROLE_KEY: 'placeholder',
      CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'office-reminder-secret',
      CANDIDATE_APP_PUBLIC_URL: 'https://candidate.example.test'
    }, {}, deps);
    assert.equal(response.status, 202);
    assert.equal(rpcCalls.length, 3, 'one replay probe, preview and batch execute RPC are expected');
    assert.equal(rpcCalls.filter(call => call.args.p_action === 'REMINDER_BATCH_EXECUTE').length, 1);
    assert.equal((await response.json()).status_url,
      `/api/candidate-app/manager-reminder-batches/${batchId}`);

    replayFound = true;
    globalThis.fetch = async () => { throw new Error('an exact replay must not reread manager approval state'); };
    const replayResponse = await handleCandidateAppRequest(new Request(
      'https://office.test/api/candidate-app/manager-reminder-batches', {
        method: 'POST', headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          selected_rows: [identity], batch_id: batchId, idempotency_key: batchId,
          preview_context_hash: fingerprint, selection_fingerprint: fingerprint
        })
      }
    ), {
      CANDIDATE_APP_ENVIRONMENT: 'TEST', SUPABASE_URL: 'https://test.supabase.invalid',
      SUPABASE_SERVICE_ROLE_KEY: 'placeholder',
      CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'office-reminder-secret',
      CANDIDATE_APP_PUBLIC_URL: 'https://candidate.example.test'
    }, {}, deps);
    assert.equal(replayResponse.status, 202);
    assert.equal((await replayResponse.json()).idempotent_replay, true);
    assert.equal(rpcCalls.length, 4, 'the replay uses only the durable database receipt probe');
    assert.equal(rpcCalls.at(-1).args.p_action, 'REMINDER_BATCH_REPLAY');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('paper pack responses never expose an R2 storage identity', () => {
  const safe = safeQrPackResponse({
    queued: true,
    document_operation_id: '00000000-0000-4000-8000-000000000001',
    current_timesheet_id: '00000000-0000-4000-8000-000000000002',
    current_version: 3,
    pdf_storage_key: 'private/secret.pdf',
    storage_keys: ['private/secret.pdf'],
    recipient_email: 'private@example.test'
  });
  assert.equal(safe.queued, true);
  assert.equal(safe.timesheet_version, 3);
  assert.equal(Object.prototype.hasOwnProperty.call(safe, 'pdf_storage_key'), false);
  assert.equal(Object.prototype.hasOwnProperty.call(safe, 'storage_keys'), false);
  assert.equal(Object.prototype.hasOwnProperty.call(safe, 'recipient_email'), false);
});

test('component upload tickets are encrypted and do not disclose the R2 key', async () => {
  const env = { CANDIDATE_PRIVATE_UPLOAD_TOKEN_SECRET: 'test-only-secret-material' };
  const storageKey = 'candidate-app/test/workflow/source/private-object.pdf';
  const ticket = await uploadTicket(env, {
    workflow_id: '00000000-0000-4000-8000-000000000001',
    key: storageKey,
    owner: 'candidate'
  });
  assert.equal(ticket.includes(storageKey), false);
  assert.equal(ticket.split('.').length, 3);
  const opened = await verifyUploadTicket(env, ticket);
  assert.equal(opened.key, storageKey);
  assert.equal(opened.owner, 'candidate');
});

test('component validation accepts one-page PDFs and rejects mixed multi-page evidence', async () => {
  const onePage = await PDFDocument.create();
  onePage.addPage([100, 100]);
  const accepted = await validateComponentBytes(await onePage.save(), 'application/pdf');
  assert.deepEqual(accepted, { media_type: 'application/pdf', page_count: 1, width: null, height: null });

  const twoPages = await PDFDocument.create();
  twoPages.addPage([100, 100]);
  twoPages.addPage([100, 100]);
  await assert.rejects(
    validateComponentBytes(await twoPages.save(), 'application/pdf'),
    error => error?.code === 'CANDIDATE_SOURCE_PDF_ONE_PAGE_REQUIRED'
  );
});

test('component validation rejects malformed images and encrypted-PDF markers', async () => {
  await assert.rejects(
    validateComponentBytes(new Uint8Array([137, 80, 78, 71, 0, 0, 0, 0]), 'image/png'),
    error => error?.code === 'CANDIDATE_SOURCE_IMAGE_INVALID'
  );
  await assert.rejects(
    validateComponentBytes(new TextEncoder().encode('%PDF-1.7\n/Encrypt\n%%EOF'), 'application/pdf'),
    error => error?.code === 'CANDIDATE_SOURCE_PDF_INVALID'
  );
});

test('immutable generated documents use create-only storage and permit only same-digest replay', async () => {
  const objects = new Map();
  const env = { R2: {
    async put(key, bytes, options) {
      if (options.onlyIf?.etagDoesNotMatch === '*' && objects.has(key)) return null;
      const row = { key, customMetadata: options.customMetadata, bytes: new Uint8Array(bytes) };
      objects.set(key, row);
      return row;
    },
    async head(key) { return objects.get(key) || null; }
  } };
  const first = await immutablePut(env, 'immutable/test.pdf', new Uint8Array([1, 2, 3]), 'application/pdf');
  const replay = await immutablePut(env, 'immutable/test.pdf', new Uint8Array([1, 2, 3]), 'application/pdf');
  assert.equal(first.created, true);
  assert.equal(replay.created, false);
  await assert.rejects(
    immutablePut(env, 'immutable/test.pdf', new Uint8Array([1, 2, 4]), 'application/pdf'),
    error => error?.code === 'CANDIDATE_RENDER_IDEMPOTENCY_CONFLICT'
  );
});

test('idempotent component preparation always uses the original database-owned object identity', () => {
  const response = {
    component_id: '00000000-0000-4000-8000-000000000010',
    storage_key: 'candidate-app/test/workflow/source/original.pdf',
    media_type: 'application/pdf', byte_size: 512,
    component_kind: 'EXPENSE_EVIDENCE', document_role: 'SOURCE_EVIDENCE',
    expense_category: 'TRAVEL', paper_return_page_key: null,
    workflow_generation: 4, state: 'PENDING'
  };
  const expected = {
    media_type: 'application/pdf', byte_size: 512,
    component_kind: 'EXPENSE_EVIDENCE', document_role: 'SOURCE_EVIDENCE',
    expense_category: 'TRAVEL', paper_return_page_key: null,
    workflow_generation: 4
  };
  assert.equal(preparedUploadContract(response, expected).storage_key, response.storage_key);
  assert.throws(
    () => preparedUploadContract({ ...response, expense_category: 'OTHER' }, expected),
    error => error?.code === 'CANDIDATE_COMPONENT_PREPARE_CONTRACT_MISMATCH'
  );
  assert.throws(
    () => preparedUploadContract({ ...response, media_type: 'image/png' }, expected),
    error => error?.code === 'CANDIDATE_COMPONENT_PREPARE_CONTRACT_MISMATCH'
  );
  assert.throws(
    () => preparedUploadContract({ ...response, workflow_generation: 3 }, expected),
    error => error?.code === 'CANDIDATE_COMPONENT_PREPARE_CONTRACT_MISMATCH'
  );
  for (const state of ['SUPERSEDED', 'REJECTED', 'ABANDONED']) {
    assert.throws(
      () => preparedUploadContract({ ...response, state }, expected),
      error => error?.code === 'CANDIDATE_COMPONENT_PREPARE_CONTRACT_MISMATCH'
    );
  }
});

test('paper mileage form preserves the approved labels and UK week-ending format', async () => {
  const workflow = {
    id: '00000000-0000-4000-8000-000000000020', generation: 3,
    week_ending_date: '2026-08-09',
    immutable_submission_json: {
      official_presentation: { worker: { first_name: 'Test', surname: 'Worker' }, client: { name: 'Test Client' } },
      expense_submission: {
        canonical_tsfin_snapshot: { mileage_units: 18, travel_pay_ex_vat: 10, accommodation_pay_ex_vat: 100, expenses_pay_ex_vat: 110 },
        mileage_journeys: [{ post_code_from: 'AA1 1AA', cost_code_to: 'WARD-1', miles: 18 }]
      }
    }
  };
  const bytes = await mileageClaimFormBytes({}, workflow, { agency_name: 'Configured Agency', logo: null });
  const pdf = await PDFDocument.load(bytes);
  assert.equal(pdf.getPageCount(), 1);
  assert.equal(mileageJourneyRows(workflow)[0].post_code_from, 'AA1 1AA');
  assert.deepEqual(expenseSummaryDisplayLines(workflow), {
    lines: ['Mileage: 18 miles', 'Accommodation: £100.00', 'Travel: £10.00'],
    total: '£110.00'
  });
  const source = await readFile(new URL('../broker/src/candidate-app-backend.js', import.meta.url), 'utf8');
  for (const label of ['Mileage Claim Form for week ending', 'Post Code from', 'Cost Code To', 'Number of miles', 'Total mileage', 'Manager signature']) {
    assert.equal(source.includes(label), true);
  }
});

test('persisted expense and mileage PDFs are byte-deterministic across wall-clock time', async () => {
  const branding = noLogoBranding();
  const workflow = {
    id: '00000000-0000-4000-8000-000000000021', generation: 2,
    week_ending_date: '2026-08-09',
    immutable_submission_json: {
      official_presentation: {
        branding,
        worker: { first_name: 'Test', surname: 'Worker' },
        client: { name: 'Test Client' }
      },
      expense_submission: { canonical_tsfin_snapshot: { expenses_pay_ex_vat: 25 } }
    }
  };
  const component = {
    id: '00000000-0000-4000-8000-000000000022', component_kind: 'EXPENSE_SUMMARY',
    document_role: 'EXPENSE_APPROVAL_SUMMARY', review_ordinal: 1
  };
  const env = {};
  const firstMileage = await mileageClaimFormBytes(env, workflow, { ...branding, logo: null });
  const firstExpense = (await renderExpensePage(env, { review_ordinal: 1, render_input: {} }, { workflow, component }, 'REVIEW')).pdf_bytes;
  await new Promise(resolve => setTimeout(resolve, 1100));
  const secondMileage = await mileageClaimFormBytes(env, workflow, { ...branding, logo: null });
  const secondExpense = (await renderExpensePage(env, { review_ordinal: 1, render_input: {} }, { workflow, component }, 'REVIEW')).pdf_bytes;
  const digest = bytes => createHash('sha256').update(bytes).digest('hex');
  assert.equal(digest(firstMileage), digest(secondMileage));
  assert.equal(digest(firstExpense), digest(secondExpense));
});

test('expense summary rendering fails closed without the frozen canonical display total', () => {
  assert.throws(
    () => expenseSummaryDisplayLines({
      immutable_submission_json: {
        expense_submission: { canonical_tsfin_snapshot: { travel_pay_ex_vat: 10, accommodation_pay_ex_vat: 20 } }
      }
    }),
    error => error?.code === 'CANDIDATE_EXPENSE_DISPLAY_TOTAL_REQUIRED'
  );
  assert.throws(
    () => expenseSummaryDisplayLines({
      immutable_submission_json: { expense_submission: { canonical_tsfin_snapshot: { expenses_pay_ex_vat: null } } }
    }),
    error => error?.code === 'CANDIDATE_EXPENSE_DISPLAY_TOTAL_REQUIRED'
  );
});

test('frozen branding ignores later live settings and validates its immutable contract', async () => {
  const branding = noLogoBranding('Frozen Agency');
  let settingsReads = 0;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    settingsReads += 1;
    return Response.json([{ agency_name: 'Later Agency', agency_logo: null }]);
  };
  try {
    const resolved = await candidateDocumentBranding({
      SUPABASE_URL: 'https://test.supabase.invalid', SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
    }, { immutable_submission_json: { official_presentation: { branding } } });
    assert.equal(resolved.agency_name, 'Frozen Agency');
    assert.equal(settingsReads, 0);
    await assert.rejects(
      candidateDocumentBranding({}, {
        immutable_submission_json: { official_presentation: { branding: { ...branding, agency_name: 'Tampered' } } }
      }),
      error => error?.code === 'CANDIDATE_DOCUMENT_BRANDING_CONTRACT_INVALID'
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('live branding is copied once to a content-addressed immutable logo key', async () => {
  const originalFetch = globalThis.fetch;
  const logoBytes = new Uint8Array([137, 80, 78, 71, 13, 10, 26, 10, 1, 2, 3, 4]);
  const logoDigest = createHash('sha256').update(logoBytes).digest('hex');
  const objects = new Map();
  let putCount = 0;
  const env = {
    SUPABASE_URL: 'https://test.supabase.invalid', SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder',
    R2: {
      async get(key) {
        if (key !== 'Assets/LOGO.png') return null;
        return {
          httpMetadata: { contentType: 'image/png' },
          async arrayBuffer() { return logoBytes.buffer.slice(0); }
        };
      },
      async put(key, bytes, options) {
        if (objects.has(key)) return null;
        putCount += 1;
        const value = new Uint8Array(bytes);
        const row = { key, size: value.byteLength, customMetadata: options.customMetadata };
        objects.set(key, row);
        return row;
      },
      async head(key) { return objects.get(key) || null; }
    }
  };
  globalThis.fetch = async () => Response.json([{ agency_name: 'Configured Agency', agency_logo: 'Assets/LOGO.png' }]);
  try {
    const first = await candidateDocumentBranding(env);
    const replay = await candidateDocumentBranding(env);
    assert.equal(first.logo_key, `candidate-app/branding/${logoDigest}.png`);
    assert.equal(first.logo_sha256, logoDigest);
    assert.equal(replay.branding_contract_sha256, first.branding_contract_sha256);
    assert.equal(putCount, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('document registration recovers after R2 success without rerendering a different object', async () => {
  const branding = noLogoBranding();
  const workflow = {
    id: '00000000-0000-4000-8000-000000000023', generation: 1,
    candidate_id: '00000000-0000-4000-8000-000000000024', scope: 'WEEKLY',
    renderer_contract_version: 'CANDIDATE_REVIEW_DOCUMENTS_V1',
    immutable_submission_json: {
      official_presentation: {
        renderer_contract_version: 'CANDIDATE_REVIEW_DOCUMENTS_V1',
        branding, worker: { first_name: 'A', surname: 'B' }, client: { name: 'C' }
      },
      expense_submission: { canonical_tsfin_snapshot: { expenses_pay_ex_vat: 12 } }
    }
  };
  const component = {
    id: '00000000-0000-4000-8000-000000000025', workflow_id: workflow.id,
    workflow_generation: 1, component_kind: 'EXPENSE_SUMMARY',
    document_role: 'EXPENSE_APPROVAL_SUMMARY', review_ordinal: 1, expense_category: null
  };
  const objects = new Map();
  let putCount = 0;
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    SUPABASE_URL: 'https://test.supabase.invalid', SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder',
    R2: {
      async put(key, bytes, options) {
        if (objects.has(key)) return null;
        putCount += 1;
        const data = new Uint8Array(bytes);
        const row = { key, size: data.byteLength, customMetadata: options.customMetadata, bytes: data };
        objects.set(key, row);
        return row;
      },
      async head(key) { return objects.get(key) || null; }
    }
  };
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async url => {
    const path = new URL(url).pathname;
    if (path.endsWith('/candidate_submission_workflows')) return Response.json([workflow]);
    if (path.endsWith('/candidate_submission_components')) return Response.json([component]);
    return Response.json([]);
  };
  let rpcCalls = 0;
  const deps = { async rpc() {
    rpcCalls += 1;
    if (rpcCalls === 1) throw new Error('simulated registration failure');
    return { ok: true };
  } };
  const contract = {
    workflow_id: workflow.id, workflow_generation: 1, component_id: component.id,
    component_kind: component.component_kind, document_role: component.document_role,
    review_ordinal: 1, scope: 'WEEKLY', form_variant: 'ELECTRONIC_MANAGER_REVIEW',
    render_input_sha256: 'a'.repeat(64), candidate_signature_embedded: false, render_input: {}
  };
  try {
    await assert.rejects(renderAndRegister(env, deps, [contract], 'REVIEW'), /simulated registration failure/);
    assert.equal(putCount, 1);
    await renderAndRegister(env, deps, [contract], 'REVIEW');
    assert.equal(putCount, 1, 'retry must reuse the existing immutable object');
    assert.equal(rpcCalls, 2);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('complete paper pack retry reuses the same deterministic object and digest', async () => {
  const branding = noLogoBranding();
  const basePdf = await PDFDocument.create({ updateMetadata: false });
  basePdf.addPage([200, 200]);
  const baseBytes = new Uint8Array(await basePdf.save());
  const baseHash = createHash('sha256').update(baseBytes).digest('hex');
  const workflow = {
    id: '00000000-0000-4000-8000-000000000026', generation: 1,
    renderer_contract_version: 'CANDIDATE_REVIEW_DOCUMENTS_V1',
    paper_return_manifest_sha256: 'b'.repeat(64),
    paper_return_manifest_json: { pages: [{ page_key: 'HOURS_TIMESHEET', component_kind: 'HOURS_TIMESHEET' }] },
    immutable_submission_json: { official_presentation: { branding } }
  };
  const timesheet = { timesheet_id: '00000000-0000-4000-8000-000000000027' };
  const version = { r2_key: 'base.pdf', sha256: baseHash };
  const objects = new Map();
  let putCount = 0;
  const baseObject = {
    httpMetadata: { contentType: 'application/pdf' },
    async arrayBuffer() { return baseBytes.buffer.slice(baseBytes.byteOffset, baseBytes.byteOffset + baseBytes.byteLength); }
  };
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    SUPABASE_URL: 'https://test.supabase.invalid', SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder',
    R2: {
      async get(key) { return key === 'base.pdf' ? baseObject : null; },
      async put(key, bytes, options) {
        if (objects.has(key)) return null;
        putCount += 1;
        const data = new Uint8Array(bytes);
        const row = { key, size: data.byteLength, customMetadata: options.customMetadata, bytes: data };
        objects.set(key, row);
        return row;
      },
      async head(key) { return objects.get(key) || null; }
    }
  };
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => Response.json([]);
  try {
    const first = await assembleCandidatePaperPack(env, workflow, timesheet, version);
    await new Promise(resolve => setTimeout(resolve, 1100));
    const replay = await assembleCandidatePaperPack(env, workflow, timesheet, version);
    assert.equal(first.sha256, replay.sha256);
    assert.equal(first.key, replay.key);
    assert.equal(putCount, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('paper pack readiness is a read-only durable-object receipt check', async () => {
  const workflow = {
    id: '00000000-0000-4000-8000-000000000030', generation: 1, environment: 'TEST',
    renderer_contract_version: 'CANDIDATE_REVIEW_DOCUMENTS_V1',
    paper_return_manifest_sha256: 'a'.repeat(64),
    paper_return_manifest_json: { pages: [
      { page_key: 'HOURS_TIMESHEET' }, { page_key: 'EXPENSE_SUMMARY' },
      { page_key: 'MILEAGE_FORM:1' }, { page_key: 'EXPENSE_EVIDENCE:1' }
    ] },
    immutable_submission_json: {
      official_presentation: { branding: { branding_contract_sha256: 'f'.repeat(64) } }
    }
  };
  const timesheet = { timesheet_id: '00000000-0000-4000-8000-000000000031' };
  const version = { sha256: 'b'.repeat(64) };
  const env = { CANDIDATE_APP_ENVIRONMENT: 'TEST', R2: {
    async head(key) {
      return {
        key, size: 123,
        customMetadata: {
          purpose: 'candidate-complete-paper-pack', workflow_id: workflow.id,
          workflow_generation: '1',
          timesheet_id: timesheet.timesheet_id, manifest_sha256: 'a'.repeat(64),
          base_document_sha256: 'b'.repeat(64), branding_contract_sha256: 'f'.repeat(64),
          renderer_contract_version: 'CANDIDATE_REVIEW_DOCUMENTS_V1', media_type: 'application/pdf',
          sha256: 'c'.repeat(64), byte_size: '123', page_count: '4'
        }
      };
    },
    async put() { throw new Error('read path must not write'); },
    async get() { throw new Error('status path must not download'); }
  } };
  const identity = paperPackIdentity(env, workflow, timesheet, version);
  assert.match(identity.key, /paper-pack\/[a-f0-9]{64}-[a-f0-9]{64}-[a-f0-9]{64}-CANDIDATE_REVIEW_DOCUMENTS_V1\.pdf$/);
  const receipt = await readyPaperPackReceipt(env, workflow, timesheet, version);
  assert.equal(receipt.ready, true);
  assert.equal(receipt.page_count, 4);
  const source = await readFile(new URL('../broker/src/candidate-app-backend.js', import.meta.url), 'utf8');
  const readStart = source.indexOf('async function candidatePaperPackContext');
  const readEnd = source.indexOf('async function handlePaperPackStatus', readStart);
  const readPath = source.slice(readStart, readEnd);
  assert.doesNotMatch(readPath, /assembleCandidatePaperPack|restWrite|immutablePut/);
  assert.match(readPath, /readyPaperPackReceipt/);
  assert.match(readPath, /workflows\.length > 1[\s\S]*CANDIDATE_PAPER_WORKFLOW_CONFLICT/);
});

test('paper pack receipt rejects malformed hashes, generation and page-count metadata', async () => {
  const workflow = {
    id: '00000000-0000-4000-8000-000000000032', generation: 3,
    renderer_contract_version: 'CANDIDATE_REVIEW_DOCUMENTS_V1',
    paper_return_manifest_sha256: 'a'.repeat(64),
    paper_return_manifest_json: { pages: [{ page_key: 'HOURS_TIMESHEET' }, { page_key: 'EXPENSE_SUMMARY' }] },
    immutable_submission_json: {
      official_presentation: { branding: { branding_contract_sha256: 'f'.repeat(64) } }
    }
  };
  const timesheet = { timesheet_id: '00000000-0000-4000-8000-000000000033' };
  const version = { sha256: 'b'.repeat(64) };
  const baseMetadata = {
    purpose: 'candidate-complete-paper-pack', workflow_id: workflow.id,
    workflow_generation: '3', timesheet_id: timesheet.timesheet_id,
    manifest_sha256: 'a'.repeat(64), base_document_sha256: 'b'.repeat(64),
    branding_contract_sha256: 'f'.repeat(64),
    renderer_contract_version: 'CANDIDATE_REVIEW_DOCUMENTS_V1',
    media_type: 'application/pdf', sha256: 'c'.repeat(64), byte_size: '123', page_count: '2'
  };
  for (const metadata of [
    { ...baseMetadata, sha256: 'z'.repeat(64) },
    { ...baseMetadata, workflow_generation: '2' },
    { ...baseMetadata, page_count: '0' },
    { ...baseMetadata, page_count: '3' },
    { ...baseMetadata, byte_size: '-1' }
  ]) {
    await assert.rejects(readyPaperPackReceipt({ CANDIDATE_APP_ENVIRONMENT: 'TEST', R2: {
      async head() { return { size: 123, customMetadata: metadata }; }
    } }, workflow, timesheet, version), error => error?.code === 'CANDIDATE_PAPER_PACK_IDENTITY_CONFLICT');
  }
  assert.throws(() => paperPackIdentity({ CANDIDATE_APP_ENVIRONMENT: 'TEST' }, {
    ...workflow, paper_return_manifest_sha256: 'not-a-sha'
  }, timesheet, version), error => error?.code === 'CANDIDATE_PAPER_PACK_IDENTITY_INVALID');
});

test('paper pack release never requeues a failed mail operation', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  globalThis.fetch = async (url, options = {}) => {
    calls.push({ url: String(url), method: options.method || 'GET' });
    return Response.json([{
      id: '00000000-0000-4000-8000-000000000040', status: 'FAILED',
      payment_scope_json: {
        candidate_workflow_id: '00000000-0000-4000-8000-000000000041',
        candidate_workflow_generation: 2,
        paper_return_manifest_sha256: 'd'.repeat(64)
      }, attachments: []
    }]);
  };
  try {
    await assert.rejects(releaseCandidatePaperPack({
      SUPABASE_URL: 'https://test.supabase.invalid', SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
    }, { async rpc() { throw new Error('RPC must not be called'); } }, {
      id: '00000000-0000-4000-8000-000000000041', generation: 2,
      paper_return_manifest_sha256: 'd'.repeat(64)
    }, { timesheet_id: '00000000-0000-4000-8000-000000000042' }, {
      key: 'candidate-app/test/pack.pdf', sha256: 'e'.repeat(64), byte_size: 500,
      page_count: 2, manifest_hash: 'd'.repeat(64)
    }), error => error?.code === 'CANDIDATE_PAPER_OUTBOX_FAILED');
    assert.deepEqual(calls.map(call => call.method), ['GET']);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('paper pack release rejects a missing outbox before creating a readiness notification', async () => {
  const originalFetch = globalThis.fetch;
  let notificationCalls = 0;
  globalThis.fetch = async (url) => {
    const path = new URL(url).pathname;
    if (path.endsWith('/mail_outbox')) return Response.json([]);
    if (path.endsWith('/candidate_notifications')) {
      notificationCalls += 1;
      return Response.json([]);
    }
    throw new Error(`unexpected request ${path}`);
  };
  try {
    await assert.rejects(releaseCandidatePaperPack({
      SUPABASE_URL: 'https://test.supabase.invalid', SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
    }, { async rpc() { throw new Error('RPC must not be called'); } }, {
      id: '00000000-0000-4000-8000-000000000041', generation: 2,
      account_id: '00000000-0000-4000-8000-000000000045',
      candidate_id: '00000000-0000-4000-8000-000000000046'
    }, { timesheet_id: '00000000-0000-4000-8000-000000000042' }, {
      key: 'candidate-app/test/pack.pdf', sha256: 'e'.repeat(64), byte_size: 500,
      page_count: 2, manifest_hash: 'd'.repeat(64)
    }), error => error?.code === 'CANDIDATE_PAPER_OUTBOX_NOT_READY');
    assert.equal(notificationCalls, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('paper pack release is insert-once and preserves an existing notification lifecycle', async () => {
  const originalFetch = globalThis.fetch;
  const existingNotification = { state: 'READ', push_state: 'SENT', created_at_utc: '2026-08-01T00:00:00Z' };
  let rpcArgs = null;
  const deps = { async rpc(name, args) { rpcArgs = { name, args }; return { data: { ok: true } }; } };
  globalThis.fetch = async (url, options = {}) => {
    const path = new URL(url).pathname;
    if (path.endsWith('/mail_outbox')) {
      return Response.json([{
        id: '00000000-0000-4000-8000-000000000043', status: 'SENT',
        payment_scope_json: readyPaperScope(
          '00000000-0000-4000-8000-000000000044', 2, 'd'.repeat(64)
        ),
        attachments: [readyPaperAttachment(
          '00000000-0000-4000-8000-000000000044', 2, 'd'.repeat(64)
        )]
      }]);
    }
    if (path.endsWith('/candidate_notifications')) throw new Error('notification REST mutation is forbidden');
    throw new Error(`unexpected request ${path}`);
  };
  try {
    await releaseCandidatePaperPack({
      SUPABASE_URL: 'https://test.supabase.invalid', SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder',
      CANDIDATE_APP_ENVIRONMENT: 'TEST'
    }, deps, {
      id: '00000000-0000-4000-8000-000000000044', generation: 2,
      paper_return_manifest_sha256: 'd'.repeat(64),
      account_id: '00000000-0000-4000-8000-000000000045',
      candidate_id: '00000000-0000-4000-8000-000000000046'
    }, { timesheet_id: '00000000-0000-4000-8000-000000000047' }, completePaperFixture());
    assert.equal(rpcArgs?.name, 'candidate_workflow_transition_atomic_v1');
    assert.equal(rpcArgs?.args?.p_action, 'PAPER_PACK_RELEASE');
    assert.equal(rpcArgs?.args?.p_payload?.service_paper_pack_release, true);
    assert.deepEqual(existingNotification, {
      state: 'READ', push_state: 'SENT', created_at_utc: '2026-08-01T00:00:00Z'
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('paper pack notification replay never resets terminal read or push states', async () => {
  const originalFetch = globalThis.fetch;
  const lifecyclePairs = [
    ['READ', 'SENT'], ['DISMISSED', 'CLAIMED'], ['READ', 'FAILED']
  ];
  try {
    for (const [state, pushState] of lifecyclePairs) {
      const existing = { state, push_state: pushState, created_at_utc: '2026-08-01T00:00:00Z' };
      let rpcCalls = 0;
      const deps = { async rpc() { rpcCalls += 1; return { data: { ok: true } }; } };
      globalThis.fetch = async (url, options = {}) => {
        const path = new URL(url).pathname;
        if (path.endsWith('/mail_outbox')) {
          return Response.json([{
            id: '00000000-0000-4000-8000-000000000043', status: 'SENT',
            payment_scope_json: readyPaperScope(
              '00000000-0000-4000-8000-000000000044', 2, 'd'.repeat(64)
            ),
            attachments: [readyPaperAttachment(
              '00000000-0000-4000-8000-000000000044', 2, 'd'.repeat(64)
            )]
          }]);
        }
        if (path.endsWith('/candidate_notifications')) {
          throw new Error('notification REST mutation is forbidden');
        }
        throw new Error(`unexpected request ${path}`);
      };
      await releaseCandidatePaperPack({
        SUPABASE_URL: 'https://test.supabase.invalid', SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder',
        CANDIDATE_APP_ENVIRONMENT: 'TEST'
      }, deps, {
        id: '00000000-0000-4000-8000-000000000044', generation: 2,
        paper_return_manifest_sha256: 'd'.repeat(64),
        account_id: '00000000-0000-4000-8000-000000000045',
        candidate_id: '00000000-0000-4000-8000-000000000046'
      }, { timesheet_id: '00000000-0000-4000-8000-000000000047' }, completePaperFixture());
      assert.equal(rpcCalls, 1);
      assert.deepEqual(existing, { state, push_state: pushState, created_at_utc: '2026-08-01T00:00:00Z' });
    }
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('paper pack release does not notify when the guarded attachment update loses its race', async () => {
  const originalFetch = globalThis.fetch;
  let notificationCalls = 0;
  let mailReads = 0;
  const deps = { async rpc() {
    const error = new Error('CANDIDATE_PAPER_OUTBOX_NOT_READY');
    error.code = 'CANDIDATE_PAPER_OUTBOX_NOT_READY';
    throw error;
  } };
  globalThis.fetch = async (url, options = {}) => {
    const path = new URL(url).pathname;
    if (path.endsWith('/candidate_notifications')) {
      notificationCalls += 1;
      return Response.json([]);
    }
    if (!path.endsWith('/mail_outbox')) throw new Error(`unexpected request ${path}`);
    if ((options.method || 'GET') === 'PATCH') return Response.json([]);
    mailReads += 1;
    if (mailReads === 1) {
      return Response.json([{
        id: '00000000-0000-4000-8000-000000000043', status: 'QUEUED',
        payment_scope_json: {
          candidate_workflow_id: '00000000-0000-4000-8000-000000000044',
          candidate_workflow_generation: 2,
          paper_return_manifest_sha256: 'd'.repeat(64),
          candidate_paper_pack_ready: false,
          mail_held_until_pdf_rendered: true,
          mail_hold_reason: 'CANDIDATE_PAPER_PACK_PENDING'
        }, attachments: [], attempt_lease_token: null
      }]);
    }
    return Response.json([{
      id: '00000000-0000-4000-8000-000000000043', status: 'CLAIMED', attachments: []
    }]);
  };
  try {
    await assert.rejects(releaseCandidatePaperPack({
      SUPABASE_URL: 'https://test.supabase.invalid', SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
    }, deps, {
      id: '00000000-0000-4000-8000-000000000044', generation: 2,
      account_id: '00000000-0000-4000-8000-000000000045',
      candidate_id: '00000000-0000-4000-8000-000000000046'
    }, { timesheet_id: '00000000-0000-4000-8000-000000000047' }, completePaperFixture()),
    error => error?.code === 'CANDIDATE_PAPER_OUTBOX_NOT_READY');
    assert.equal(notificationCalls, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('paper pack release installs the exact complete pack before notification', async () => {
  const originalFetch = globalThis.fetch;
  const workflowId = '00000000-0000-4000-8000-000000000044';
  const manifestHash = 'd'.repeat(64);
  const heldScope = {
    candidate_workflow_id: workflowId,
    candidate_workflow_generation: 2,
    paper_return_manifest_sha256: manifestHash,
    candidate_paper_pack_ready: false,
    mail_held_until_pdf_rendered: true,
    mail_hold_reason: 'CANDIDATE_PAPER_PACK_PENDING'
  };
  let rpcArgs = null;
  const deps = { async rpc(name, args) { rpcArgs = { name, args }; return { data: { ok: true } }; } };
  globalThis.fetch = async (url, options = {}) => {
    const path = new URL(url).pathname;
    if (path.endsWith('/mail_outbox') && (options.method || 'GET') === 'GET') {
      return Response.json([{
        id: '00000000-0000-4000-8000-000000000043', status: 'QUEUED',
        payment_scope_json: heldScope, attachments: [], attempt_lease_token: null
      }]);
    }
    if (path.endsWith('/mail_outbox') && options.method === 'PATCH') {
      throw new Error('mail REST mutation is forbidden');
    }
    if (path.endsWith('/candidate_notifications')) {
      throw new Error('notification REST mutation is forbidden');
    }
    throw new Error(`unexpected request ${path}`);
  };
  try {
    await releaseCandidatePaperPack({
      SUPABASE_URL: 'https://test.supabase.invalid', SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder',
      CANDIDATE_APP_ENVIRONMENT: 'TEST'
    }, deps, {
      id: workflowId, generation: 2,
      paper_return_manifest_sha256: manifestHash,
      account_id: '00000000-0000-4000-8000-000000000045',
      candidate_id: '00000000-0000-4000-8000-000000000046'
    }, { timesheet_id: '00000000-0000-4000-8000-000000000047' }, completePaperFixture({ manifest_hash: manifestHash }));
    assert.equal(rpcArgs?.name, 'candidate_workflow_transition_atomic_v1');
    assert.equal(rpcArgs?.args?.p_action, 'PAPER_PACK_RELEASE');
    assert.equal(rpcArgs?.args?.p_payload?.mail_outbox_id, '00000000-0000-4000-8000-000000000043');
    assert.equal(rpcArgs?.args?.p_payload?.complete_pack_sha256, 'e'.repeat(64));
    assert.equal(rpcArgs?.args?.p_payload?.complete_pack_page_count, 2);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('paper outbox binding is already atomic and the backend only adopts the exact held row', async () => {
  const originalFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async (url, options = {}) => {
    calls += 1;
    assert.equal(options.method || 'GET', 'GET');
    return Response.json([{
      id: '00000000-0000-4000-8000-000000000048', type: 'TIMESHEET_QR',
      context_kind: 'timesheets', context_id: '00000000-0000-4000-8000-000000000049',
      status: 'QUEUED', attachments: [], attempt_lease_token: null,
      payment_scope_json: {
        candidate_workflow_id: '00000000-0000-4000-8000-000000000050',
        candidate_workflow_generation: 1,
        paper_return_manifest_sha256: 'a'.repeat(64),
        candidate_paper_pack_ready: false,
        mail_held_until_pdf_rendered: true,
        mail_hold_reason: 'CANDIDATE_PAPER_PACK_PENDING'
      }
    }]);
  };
  try {
    const result = await bindCandidatePaperOutbox({
      SUPABASE_URL: 'https://test.supabase.invalid', SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
    }, {
      id: '00000000-0000-4000-8000-000000000050', generation: 1,
      paper_return_manifest_sha256: 'a'.repeat(64)
    }, '00000000-0000-4000-8000-000000000049', {
      queued: true, recipient_available: true,
      mail_outbox_id: '00000000-0000-4000-8000-000000000048'
    });
    assert.deepEqual(result, {
      bound: true, recipient_available: true,
      mail_outbox_id: '00000000-0000-4000-8000-000000000048'
    });
    assert.equal(calls, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('paper pack release rejects an already claimed held email before PATCH or notification', async () => {
  const originalFetch = globalThis.fetch;
  const methods = [];
  globalThis.fetch = async (url, options = {}) => {
    methods.push(options.method || 'GET');
    const path = new URL(url).pathname;
    if (!path.endsWith('/mail_outbox')) throw new Error(`unexpected request ${path}`);
    return Response.json([{
      id: '00000000-0000-4000-8000-000000000043', status: 'QUEUED',
      payment_scope_json: {
        candidate_workflow_id: '00000000-0000-4000-8000-000000000044',
        candidate_workflow_generation: 2,
        paper_return_manifest_sha256: 'd'.repeat(64),
        candidate_paper_pack_ready: false,
        mail_held_until_pdf_rendered: true,
        mail_hold_reason: 'CANDIDATE_PAPER_PACK_PENDING'
      }, attachments: [], attempt_lease_token: 'already-claimed'
    }]);
  };
  try {
    await assert.rejects(releaseCandidatePaperPack({
      SUPABASE_URL: 'https://test.supabase.invalid', SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
    }, { async rpc() { throw new Error('RPC must not be called'); } }, {
      id: '00000000-0000-4000-8000-000000000044', generation: 2,
      account_id: '00000000-0000-4000-8000-000000000045',
      candidate_id: '00000000-0000-4000-8000-000000000046'
    }, { timesheet_id: '00000000-0000-4000-8000-000000000047' }, {
      key: 'candidate-app/test/pack.pdf', sha256: 'e'.repeat(64), byte_size: 500,
      page_count: 2, manifest_hash: 'd'.repeat(64)
    }), error => error?.code === 'CANDIDATE_PAPER_OUTBOX_NOT_READY');
    assert.deepEqual(methods, ['GET']);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('paper-pack scheduler proves the exact held email before any R2 pack work', async () => {
  const originalFetch = globalThis.fetch;
  let r2Calls = 0;
  const workflow = {
    id: '00000000-0000-4000-8000-000000000060', generation: 1,
    route: 'PAPER', state: 'AWAITING_PAPER_RETURN',
    target_timesheet_id: '00000000-0000-4000-8000-000000000061',
    paper_return_manifest_sha256: 'a'.repeat(64)
  };
  globalThis.fetch = async (url) => {
    const parsed = new URL(url);
    if (parsed.pathname.endsWith('/candidate_submission_workflows')) return Response.json([workflow]);
    if (parsed.pathname.endsWith('/timesheets')) return Response.json([{
      timesheet_id: workflow.target_timesheet_id, version: 1, sheet_scope: 'WEEKLY',
      submission_mode: 'MANUAL', qr_status: 'PENDING', document_state: 'READY',
      current_document_version_id: '00000000-0000-4000-8000-000000000062'
    }]);
    if (parsed.pathname.endsWith('/invoice_document_versions')) return Response.json([{
      id: '00000000-0000-4000-8000-000000000062', r2_key: 'candidate/base.pdf',
      sha256: 'b'.repeat(64), status: 'READY'
    }]);
    if (parsed.pathname.endsWith('/mail_outbox')) return Response.json([]);
    throw new Error(`unexpected request ${parsed.pathname}`);
  };
  try {
    const result = await processPendingCandidatePaperPacks({
      CANDIDATE_APP_ENVIRONMENT: 'TEST',
      SUPABASE_URL: 'https://test.supabase.invalid',
      SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder',
      R2: {
        async head() { r2Calls += 1; return null; },
        async get() { r2Calls += 1; return null; },
        async put() { r2Calls += 1; }
      }
    }, {}, 1);
    assert.equal(result.results.length, 1);
    assert.equal(result.results[0].ok, false);
    assert.equal(result.results[0].error_code, 'CANDIDATE_PAPER_OUTBOX_NOT_READY');
    assert.equal(r2Calls, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('paper outbox binding rejects missing, opted-out or otherwise unavailable email delivery', async () => {
  for (const pack of [
    { queued: false, recipient_available: false },
    { queued: false, recipient_available: true },
    { queued: true, recipient_available: false }
  ]) {
    await assert.rejects(bindCandidatePaperOutbox({}, {
      id: '00000000-0000-4000-8000-000000000050', generation: 1,
      paper_return_manifest_sha256: 'a'.repeat(64)
    }, '00000000-0000-4000-8000-000000000049', pack),
    error => error?.code === 'CANDIDATE_PAPER_EMAIL_NOT_AVAILABLE');
  }
});

test('QR pack public response never defaults a missing queue result to accepted', () => {
  assert.equal(safeQrPackResponse({ recipient_available: true }).queued, false);
  assert.equal(safeQrPackResponse({ queued: true, recipient_available: true }).queued, true);
  assert.equal('mail_outbox_id' in safeQrPackResponse({
    queued: true, recipient_available: true,
    mail_outbox_id: '00000000-0000-4000-8000-000000000048'
  }), false);
});

test('paper outbox adoption rejects an immediately due base-PDF row', async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => Response.json([{
    id: '00000000-0000-4000-8000-000000000048', type: 'TIMESHEET_QR',
    context_kind: 'timesheets', context_id: '00000000-0000-4000-8000-000000000049',
    status: 'QUEUED', attachments: [{ r2_key: 'ordinary-base.pdf' }], attempt_lease_token: null,
    payment_scope_json: {
      candidate_workflow_id: '00000000-0000-4000-8000-000000000050',
      candidate_workflow_generation: 1,
      paper_return_manifest_sha256: 'a'.repeat(64),
      candidate_paper_pack_ready: false,
      mail_held_until_pdf_rendered: false,
      mail_hold_reason: null
    }
  }]);
  try {
    await assert.rejects(bindCandidatePaperOutbox({
      SUPABASE_URL: 'https://test.supabase.invalid', SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
    }, {
      id: '00000000-0000-4000-8000-000000000050', generation: 1,
      paper_return_manifest_sha256: 'a'.repeat(64)
    }, '00000000-0000-4000-8000-000000000049', {
      queued: true, recipient_available: true,
      mail_outbox_id: '00000000-0000-4000-8000-000000000048'
    }), error => error?.code === 'CANDIDATE_PAPER_OUTBOX_NOT_READY');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('paper workflow multiplicity is checked before every document-state early return', async () => {
  const source = await readFile(new URL('../broker/src/candidate-app-backend.js', import.meta.url), 'utf8');
  const start = source.indexOf('async function candidatePaperPackContext');
  const end = source.indexOf('async function handlePaperPackStatus', start);
  const body = source.slice(start, end);
  const multiplicity = body.indexOf('workflows.length > 1');
  const documentReadiness = body.indexOf("upper(timesheet.document_state) === 'READY'");
  assert.ok(multiplicity >= 0 && documentReadiness >= 0 && multiplicity < documentReadiness);
});

test('private manager routes reject wrong HTTP methods before any RPC mutation', async () => {
  let rpcCalls = 0;
  const deps = { routeAudience: 'PRIVATE', async rpc() { rpcCalls += 1; return {}; } };
  const env = { CANDIDATE_APP_ENVIRONMENT: 'TEST' };
  const workflowId = '00000000-0000-4000-8000-000000000051';
  for (const [action, method] of [['start', 'POST'], ['progress', 'GET'], ['approve', 'GET'], ['refuse', 'GET']]) {
    const response = await handleCandidateAppRequest(
      new Request(`https://private.test/candidate-manager/v1/workflows/${workflowId}/${action}`, { method }),
      env, {}, deps
    );
    assert.equal(response.status, 405);
    assert.equal((await response.json()).error_code, 'METHOD_NOT_ALLOWED');
  }
  const componentId = '00000000-0000-4000-8000-000000000052';
  for (const [path, method] of [
    [`/candidate-manager/v1/workflows/${workflowId}/components/${componentId}/document`, 'POST'],
    [`/candidate-manager/v1/workflows/${workflowId}/signature/prepare`, 'GET']
  ]) {
    const response = await handleCandidateAppRequest(
      new Request(`https://private.test${path}`, { method }), env, {}, deps
    );
    assert.equal(response.status, 405);
    assert.equal((await response.json()).error_code, 'METHOD_NOT_ALLOWED');
  }
  assert.equal(rpcCalls, 0);
});

test('public Candidate workflow actions exclude service finalisation', async () => {
  const source = await readFile(new URL('../broker/src/candidate-app-backend.js', import.meta.url), 'utf8');
  const openapi = await readFile(new URL('../docs/candidate-app/CANDIDATE_API_OPENAPI_V1.yaml', import.meta.url), 'utf8');
  const handler = source.match(/async function handleWorkflowAction[\s\S]*?\n}\n\nasync function managerTokenContext/)?.[0] || '';
  assert.doesNotMatch(handler, /dbAction\s*===\s*'FINALISE'/);
  assert.doesNotMatch(openapi, /supersede,\s*finalise,\s*component-supersede/i);
  assert.match(source, /manager-final-render-and-finalise/);
  assert.match(source, /paper-finalise/);
});

test('public workflow responses omit renderer contracts and canonical financial internals', () => {
  assert.deepEqual(withoutInternalRenderContracts({
    ok: true,
    state: 'READY_TO_FINALISE',
    final_render_contract: { manager: { signature_storage_key: 'private/signature.png' } }
  }), { ok: true, state: 'READY_TO_FINALISE' });
  assert.deepEqual(safeFinalisationResult({
    ok: true,
    workflow_id: 'workflow-1',
    state: 'FINALISED',
    auto_authorised: false,
    hours_result: { canonical_tsfin_snapshot: { total_pay_ex_vat: 100 } }
  }), {
    ok: true,
    workflow_id: 'workflow-1',
    state: 'FINALISED',
    auto_authorised: false
  });
});

test('official review presentation is frozen from authoritative CloudTMS display rows', () => {
  const presentation = officialPresentationFromRows({
    timesheet: { ward_norm: 'ward 9', job_title_norm: 'fallback role', band: '5' },
    contractRow: {
      role: 'Registered Nurse', display_site: 'Royal Test Hospital',
      ward_hint: 'Acute Ward', band: '6'
    },
    candidate: { first_name: 'Test', last_name: 'Candidate', display_name: 'Ignored Name' },
    client: { name: 'Test NHS Trust' }
  });
  assert.equal(presentation.worker.first_name, 'Test');
  assert.equal(presentation.worker.surname, 'Candidate');
  assert.equal(presentation.worker.job_profile_title, 'Registered Nurse');
  assert.equal(presentation.client.name, 'Test NHS Trust');
  assert.equal(presentation.client.hospital, 'Royal Test Hospital');
  assert.equal(presentation.client.site_ward, 'Acute Ward');
  assert.equal(presentation.band, '6');
});

test('background approval work is isolated and reports a bounded failure without rejecting waitUntil', async () => {
  let scheduled;
  const context = { waitUntil(value) { scheduled = value; } };
  const prior = console.error;
  console.error = () => {};
  try {
    assert.equal(deferBackground(context, Promise.reject(new Error('MANAGER_REVIEW_DOCUMENT_NOT_READY')), 'test', {
      workflow_id: 'safe-id'
    }), true);
    assert.deepEqual(await scheduled, { ok: false, error_code: 'MANAGER_REVIEW_DOCUMENT_NOT_READY' });
  } finally {
    console.error = prior;
  }
});

test('fifty simultaneous manager follow-on tasks settle independently', async () => {
  const scheduled = [];
  const context = { waitUntil(value) { scheduled.push(value); } };
  const prior = console.error;
  console.error = () => {};
  try {
    for (let index = 0; index < 50; index += 1) {
      const work = index === 17
        ? Promise.reject(new Error('MANAGER_REVIEW_DOCUMENT_NOT_READY'))
        : Promise.resolve({ ok: true, workflow: index });
      assert.equal(deferBackground(context, work, 'manager-finalise', { workflow_index: index }), true);
    }
    const results = await Promise.all(scheduled);
    assert.equal(results.length, 50);
    assert.equal(results.filter(result => result?.ok === true).length, 49);
    assert.deepEqual(results[17], { ok: false, error_code: 'MANAGER_REVIEW_DOCUMENT_NOT_READY' });
  } finally {
    console.error = prior;
  }
});

test('a complete paper return immediately advances through canonical finalisation', async () => {
  const completion = await finaliseReceivedPaperReturn(
    { ok: true, state: 'RECEIVED', workflow_id: 'workflow-1' },
    async () => ({ ok: true, state: 'FINALISED', auto_authorised: false })
  );
  assert.equal(completion.status, 200);
  assert.equal(completion.body.canonical_processing_attempted, true);
  assert.equal(completion.body.finalisation.state, 'FINALISED');
  assert.equal(completion.body.finalisation_pending, false);
});

test('a paper finalisation blocker preserves RECEIVED and returns a controlled retry state', async () => {
  const completion = await finaliseReceivedPaperReturn(
    { ok: true, state: 'RECEIVED', workflow_id: 'workflow-2' },
    async () => { throw new Error('RATE_ISSUE'); }
  );
  assert.equal(completion.status, 202);
  assert.equal(completion.body.state, 'RECEIVED');
  assert.equal(completion.body.retry_required, true);
  assert.equal(completion.body.retry_error_code, 'RATE_ISSUE');
});
