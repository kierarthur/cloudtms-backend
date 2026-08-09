import assert from 'node:assert/strict';
import test from 'node:test';

import { candidateAppBackendInternals, handleCandidateAppRequest } from '../broker/src/candidate-app-backend.js';

const {
  deferBackground,
  derivePasswordVerifier,
  deterministicOpaqueToken,
  explicitNoBreak,
  finaliseReceivedPaperReturn,
  forbiddenFinancialKeys,
  officialPresentationFromRows,
  routeMatch,
  safeFinalisationResult,
  safeQrPackResponse,
  segmentBreak,
  uploadTicket,
  verifyUploadTicket,
  withoutInternalRenderContracts,
  verifyPassword
} = candidateAppBackendInternals;

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
    routeMatch('/candidate-app/v1/workflows/abc%201/actions/finalise', '/candidate-app/v1/workflows/:workflowId/actions/:action'),
    { workflowId: 'abc 1', action: 'finalise' }
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

test('Candidate HTTP boundary ignores unrelated routes and fails protected routes closed', async () => {
  const env = { CANDIDATE_APP_ENVIRONMENT: 'TEST', SESSION_TOKEN_SECRET: 'test-only-secret-material' };
  const deps = { rpc: async () => { throw new Error('unexpected RPC'); } };
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
  const env = { SESSION_TOKEN_SECRET: 'test-only-secret-material' };
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
