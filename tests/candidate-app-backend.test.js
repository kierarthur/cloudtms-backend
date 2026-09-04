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
  preparedCandidateComponentReplay,
  currentCandidateExpenseComponentReplay,
  expenseSummaryDisplayLines,
  officialPeriodWithShiftLines,
  paperPackIdentity,
  passwordVerificationProof,
  candidatePaperDeliveryGeneration,
  candidatePaperExecutionState,
  candidatePaperCompleteReceipt,
  readyPaperPackReceiptFromOutbox,
  readyPaperPackReceipt,
  readyGeneratedDocumentReceipt,
  restartCandidatePaperSourceDocumentFromStatus,
  requireCandidatePaperOutbox,
  releaseCandidatePaperPack,
  bindCandidatePaperOutbox,
  assembleCandidatePaperPack,
  candidatePaperTimesheetPageBytes,
  candidatePaperPackComponentForPage,
  renderAndRegister,
  candidateDocumentBranding,
  candidateAppAgencyBranding,
  candidateAppAgencyDocumentBranding,
  createAccessToken,
  candidateGenericMileageFormArtifact,
  submittedMileageUnits,
  assertCandidateMileageEvidenceMatchesSubmission,
  mileageClaimFormBytes,
  queueCandidatePaperPackEmail,
  candidatePaperEmailDeliveryMatches,
  candidatePaperEmailDeliveryByWorkflow,
  enrichCandidatePaperDeliveryState,
  londonCalendarDate,
  knownErrorCode,
  officeErrorCode,
  assertManagerRouteApprovalContext,
  documentStreamSource,
  renderExpensePage,
  routeMatch,
  safeFinalisationResult,
  safeCandidateWorkflowPolicy,
  safeExpensePlacement,
  normaliseCandidateWorkflowCreatePayload,
  safeCandidateNotificationPreferences,
  safeCandidateNotification,
  requireCandidateNotificationPreferences,
  safePaperReturnPages,
  safeQrPackResponse,
  segmentBreak,
  uploadTicket,
  validateComponentBytes,
  verifyUploadTicket,
  authenticateUploadOwner,
  withoutInternalRenderContracts,
  verifyPassword
} = candidateAppBackendInternals;

test('current paper-pack email delivery restores the photograph stage only for the exact workflow generation and manifest', async () => {
  const workflow = {
    id: '00000000-0000-4000-8000-000000000091',
    candidate_id: '00000000-0000-4000-8000-000000000092',
    generation: 3,
    route: 'PAPER',
    state: 'AWAITING_PAPER_RETURN',
    paper_return_manifest_sha256: 'a'.repeat(64)
  };
  const delivery = {
    status: 'SENT',
    reference: `candidate-paper-pack-email:${workflow.id}:3`,
    created_at_utc: '2026-09-03T08:30:00.000Z',
    payment_scope_json: {
      candidate_mail_authority: 'CANDIDATE_PAPER_PACK_EMAIL_V1',
      candidate_workflow_id: workflow.id,
      candidate_workflow_generation: 3,
      paper_return_manifest_sha256: 'a'.repeat(64)
    }
  };
  assert.equal(candidatePaperEmailDeliveryMatches(delivery, workflow), true);
  assert.equal(candidatePaperEmailDeliveryMatches({
    ...delivery,
    payment_scope_json: { ...delivery.payment_scope_json, candidate_workflow_generation: 2 }
  }, workflow), false);
  assert.equal(candidatePaperEmailDeliveryMatches({
    ...delivery,
    payment_scope_json: { ...delivery.payment_scope_json, paper_return_manifest_sha256: 'b'.repeat(64) }
  }, workflow), false);
  assert.equal(candidatePaperEmailDeliveryMatches({ ...delivery, reference: 'candidate-paper-pack-email:wrong:3' }, workflow), false);
  assert.equal(candidatePaperEmailDeliveryMatches({ ...delivery, status: 'FAILED' }, workflow), false);

  const originalFetch = globalThis.fetch;
  globalThis.fetch = async input => {
    const url = new URL(String(input));
    assert.ok(url.pathname.endsWith('/mail_outbox'));
    assert.equal(url.searchParams.get('type'), 'eq.TIMESHEET_QR');
    assert.equal(url.searchParams.get('recipient_kind'), 'eq.CANDIDATE');
    return Response.json([
      delivery,
      { ...delivery, status: 'QUEUED', created_at_utc: '2026-09-03T08:25:00.000Z' },
      { ...delivery, status: 'FAILED', created_at_utc: '2026-09-03T08:35:00.000Z' }
    ]);
  };
  try {
    const result = await candidatePaperEmailDeliveryByWorkflow({
      CANDIDATE_APP_ENVIRONMENT: 'TEST',
      SUPABASE_URL: 'https://test.invalid',
      SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
    }, [workflow]);
    assert.deepEqual(result.get(workflow.id), {
      paper_pack_obtained: true,
      paper_pack_delivery_methods: ['EMAIL'],
      paper_pack_obtained_at_utc: '2026-09-03T08:30:00.000Z'
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('paper delivery enrichment changes only the matching current workflow presentation', async () => {
  const workflowId = '00000000-0000-4000-8000-000000000093';
  const candidateId = '00000000-0000-4000-8000-000000000094';
  const manifest = 'c'.repeat(64);
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async input => {
    const url = new URL(String(input));
    if (url.pathname.endsWith('/candidate_submission_workflows')) return Response.json([{
      id: workflowId, candidate_id: candidateId, generation: 4, route: 'PAPER',
      state: 'AWAITING_PAPER_RETURN', paper_return_manifest_sha256: manifest
    }]);
    if (url.pathname.endsWith('/mail_outbox')) return Response.json([{
      status: 'QUEUED', reference: `candidate-paper-pack-email:${workflowId}:4`,
      created_at_utc: '2026-09-03T08:45:00.000Z',
      payment_scope_json: {
        candidate_mail_authority: 'CANDIDATE_PAPER_PACK_EMAIL_V1',
        candidate_workflow_id: workflowId,
        candidate_workflow_generation: 4,
        paper_return_manifest_sha256: manifest
      }
    }]);
    throw new Error(`unexpected fetch ${url.pathname}`);
  };
  try {
    const response = await enrichCandidatePaperDeliveryState({
      CANDIDATE_APP_ENVIRONMENT: 'TEST',
      SUPABASE_URL: 'https://test.invalid',
      SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
    }, {
      ok: true,
      workflows: [
        // candidate_app_timesheet_page_v1 uses this deliberately lightweight
        // shape: route and generation are not present on its workflow overlay.
        { workflow_id: workflowId, state: 'AWAITING_PAPER_RETURN' },
        { workflow_id: '00000000-0000-4000-8000-000000000095', generation: 1, route: 'PHONE', state: 'RECEIVED' }
      ]
    });
    assert.equal(response.workflows[0].paper_pack_obtained, true);
    assert.equal(response.workflows[0].paper_pack_obtained_at_utc, '2026-09-03T08:45:00.000Z');
    assert.equal('paper_pack_obtained' in response.workflows[1], false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('paper delivery enrichment performs no extra reads when no signed-paper return is current', async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async input => {
    throw new Error(`unexpected fetch ${String(input)}`);
  };
  const response = {
    ok: true,
    items: [{
      workflows: [
        { workflow_id: '00000000-0000-4000-8000-000000000096', state: 'FINALISED', route: 'PHONE' },
        { workflow_id: '00000000-0000-4000-8000-000000000097', state: 'AWAITING_MANAGER_APPROVAL', route: 'EMAIL' }
      ]
    }]
  };
  try {
    assert.equal(await enrichCandidatePaperDeliveryState({}, response), response);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('verified current paper pack supersedes an older workflow outbox-conflict receipt', () => {
  const state = candidatePaperExecutionState({
    last_mutation_response_json: {
      failure_scope: 'WORKFLOW',
      paper_pack_state: 'FAILED_TERMINAL',
      failure_code: 'CANDIDATE_PAPER_OUTBOX_CONFLICT'
    }
  }, {
    status: 'SENT',
    payment_scope_json: {
      candidate_paper_pack_ready: true,
      candidate_paper_pack_failure_code: 'CANDIDATE_PAPER_OUTBOX_CONFLICT'
    }
  }, {
    document_state: 'READY'
  }, {
    ready: true,
    key: 'paper-pack/current.pdf',
    sha256: 'a'.repeat(64)
  });

  assert.equal(state.state, 'READY');
  assert.equal(state.retryable, false);
  assert.equal(state.failure_code, null);
});

test('workflow terminal failure remains authoritative without a verified current paper pack', () => {
  const state = candidatePaperExecutionState({
    last_mutation_response_json: {
      failure_scope: 'WORKFLOW',
      paper_pack_state: 'FAILED_TERMINAL',
      failure_code: 'CANDIDATE_PAPER_PACK_OPERATIONAL_REVIEW_REQUIRED'
    }
  });

  assert.equal(state.state, 'FAILED_TERMINAL');
  assert.equal(state.retryable, false);
  assert.equal(state.failure_scope, 'WORKFLOW');
});

test('verified pack does not mask an unrelated workflow terminal failure', () => {
  const state = candidatePaperExecutionState({
    last_mutation_response_json: {
      failure_scope: 'WORKFLOW',
      paper_pack_state: 'FAILED_TERMINAL',
      failure_code: 'CANDIDATE_PAPER_PACK_OPERATIONAL_REVIEW_REQUIRED'
    }
  }, null, null, { ready: true });

  assert.equal(state.state, 'FAILED_TERMINAL');
  assert.equal(state.failure_code, 'CANDIDATE_PAPER_PACK_OPERATIONAL_REVIEW_REQUIRED');
});

test('stale Paper source recovery creates one exact replacement document job', async () => {
  const timesheetId = '00000000-0000-4000-8000-000000000901';
  const workflowId = '00000000-0000-4000-8000-000000000902';
  const operationId = '00000000-0000-4000-8000-000000000903';
  let observed = null;
  const result = await restartCandidatePaperSourceDocumentFromStatus({}, {
    async enqueueQrPack(options) {
      observed = options;
      return {
        document_operation_id: operationId,
        current_timesheet_id: timesheetId
      };
    }
  }, {
    id: timesheetId,
    state: 'PREPARING',
    outbox: { id: 'held-paper-outbox' },
    workflow: { id: workflowId, generation: 4 },
    timesheet: {
      document_state: 'STALE',
      active_document_operation_id: null,
      document_revision: 7
    }
  }, null);

  assert.deepEqual(result, { ok: true, document_operation_id: operationId });
  assert.equal(observed.timesheetId, timesheetId);
  assert.equal(observed.expectedTimesheetId, timesheetId);
  assert.equal(observed.idempotencyKey,
    `candidate-paper-status:${workflowId}:g4:r7`);
});

test('Paper source recovery never queues over an existing active document job', async () => {
  let calls = 0;
  const result = await restartCandidatePaperSourceDocumentFromStatus({}, {
    async enqueueQrPack() { calls += 1; }
  }, {
    id: '00000000-0000-4000-8000-000000000911',
    state: 'PREPARING',
    outbox: { id: 'held-paper-outbox' },
    workflow: { id: '00000000-0000-4000-8000-000000000912', generation: 1 },
    timesheet: {
      document_state: 'STALE',
      active_document_operation_id: '00000000-0000-4000-8000-000000000913',
      document_revision: 3
    }
  }, null);

  assert.deepEqual(result, {
    ok: true, skipped: true, reason: 'SOURCE_REQUEUE_NOT_REQUIRED'
  });
  assert.equal(calls, 0);
});

test('successful factual Daily receipt never exposes internal Office resolution to the Candidate', () => {
  assert.deepEqual(safeFinalisationResult({
    ok: true, workflow_id: 'daily-workflow', generation: 2, state: 'RECEIVED', auto_authorised: false,
    office_resolution_pending: true, financial_processing_performed: false,
    reason: 'OFFICE_RESOLUTION_REQUIRED', error_code: 'CLIENT_UNRESOLVED',
    issue_codes: ['RATE_MISSING'], auto_authorisation_blockers: ['CLIENT_UNRESOLVED'],
    canonical_result: { client_id: null, internal: true }
  }), { ok: true, workflow_id: 'daily-workflow', generation: 2, state: 'RECEIVED', auto_authorised: false });
  assert.deepEqual(safeFinalisationResult({ ok: false, state: 'READY_TO_FINALISE',
    error_code: 'FINAL_SIGNED_DOCUMENT_NOT_READY', office_resolution_pending: true }),
  { ok: false, state: 'READY_TO_FINALISE', error_code: 'FINAL_SIGNED_DOCUMENT_NOT_READY' });
});

test('Candidate RPC failures prefer the database closed code over the transport function name', () => {
  const error = new Error(
    'RPC candidate_app_bootstrap_v1 failed 400: {"message":"CANDIDATE_FEATURE_DISABLED"}'
  );
  error.json = {
    code: '42501',
    message: 'CANDIDATE_FEATURE_DISABLED',
    details: null,
    hint: null
  };
  assert.equal(knownErrorCode(error), 'CANDIDATE_FEATURE_DISABLED');
});

test('official signature dates use the Europe/London calendar date across midnight BST', () => {
  assert.equal(londonCalendarDate('2026-08-27T23:30:11.000Z'), '2026-08-28');
  assert.equal(londonCalendarDate('2026-01-27T23:30:11.000Z'), '2026-01-27');
  assert.equal(londonCalendarDate('not-a-date'), null);
});

test('Candidate expense placement closes the controlling RPC response', () => {
  const anchor = '00000000-0000-4000-8000-000000000071';
  const week = '00000000-0000-4000-8000-000000000072';
  assert.deepEqual(safeExpensePlacement({
    ok: true,
    placement: 'CREATE_CARRIER',
    reason_code: 'NO_SAFE_CARRIER',
    anchor_timesheet_id: anchor,
    anchor_contract_week_id: week,
    capabilities: {
      can_edit_expenses: true,
      private_database_flag: true
    }
  }), {
    ok: true,
    placement: 'CREATE_CARRIER',
    reason_code: 'NO_SAFE_CARRIER',
    anchor_timesheet_id: anchor,
    anchor_contract_week_id: week,
    target_timesheet_id: null,
    target_contract_week_id: null,
    target_record_role: null,
    capabilities: {
      can_use_same_record: false,
      can_reuse_carrier: false,
      can_create_carrier: true,
      can_edit_expenses: true,
      requires_carrier: true
    }
  });
  assert.throws(
    () => safeExpensePlacement({ placement: 'CREATE_CARRIER' }),
    /CANDIDATE_EXPENSE_PLACEMENT_INVALID/
  );
});

test('Candidate workflow creation maps the public timesheet identity to server-owned routing', () => {
  const timesheetId = '00000000-0000-4000-8000-000000000073';
  assert.equal(normaliseCandidateWorkflowCreatePayload({
    workflow_kind: 'DAILY', timesheet_id: timesheetId
  }).target_timesheet_id, timesheetId);
  assert.equal(normaliseCandidateWorkflowCreatePayload({
    workflow_kind: 'CONTRACT_EXPENSE', timesheet_id: timesheetId
  }).anchor_timesheet_id, timesheetId);
  assert.equal(normaliseCandidateWorkflowCreatePayload({
    workflow_kind: 'CONTRACT_COMBINED', timesheet_id: timesheetId
  }).anchor_timesheet_id, undefined);
});

test('Daily booked creation requires explicit PHONE submission and cannot borrow weekly or financial identity', () => {
  const payload = { workflow_kind: 'DAILY', scope: 'DAILY', route: 'PHONE', submission_requested: true,
    daily_source: { generation_id: '00000000-0000-4000-8000-000000000073',
      work_date: '2026-08-28', source_row_hash: 'a'.repeat(64), booking_id: 'test-daily-booking' } };
  assert.deepEqual(normaliseCandidateWorkflowCreatePayload(payload), payload);
  for (const extra of [{ submission_requested: false }, { route: 'EMAIL' }, { route: 'PAPER' },
    { workflow_kind: 'CONTRACT_HOURS' }, { scope: 'WEEKLY' }, { contract_id: null },
    { timesheet_id: '00000000-0000-4000-8000-000000000073' },
    { daily_source: { ...payload.daily_source, rate: 20 } }]) {
    assert.throws(() => normaliseCandidateWorkflowCreatePayload({ ...payload, ...extra }),
      error => error.code === 'CANDIDATE_DAILY_IDENTITY_INVALID');
  }
});

test('Candidate official period adds shift lines without mutating frozen week-day authority', () => {
  const period = officialPeriodWithShiftLines('2026-08-23', [{
    date: '2026-08-21',
    display_start_local: '09:00',
    display_end_local: '17:00',
    segment_id: 'synthetic-segment'
  }]);
  assert.equal(period.days.length, 7);
  assert.equal(period.days.find((day) => day.date === '2026-08-21').shift_lines.length, 1);
  assert.equal(period.days.find((day) => day.date === '2026-08-21').shift_lines[0].display_order, 1);
  assert.equal(period.days.filter((day) => day.date !== '2026-08-21')
    .every((day) => day.shift_lines.length === 0), true);
});

test('Candidate notification preferences map installed legacy names to the closed app contract', () => {
  assert.deepEqual(safeCandidateNotificationPreferences({
    push: false,
    manager_approval: true,
    manager_refusal: false,
    office_rejection: false,
    authorisation: true,
    payment: false,
    resubmission_required: true
  }), {
    push: false,
    manager_approval_updates: false,
    timesheet_expense_attention: false,
    authorisation: true,
    payment: false,
    approval_reminders: true,
    resubmission_required: true
  });
});

test('Candidate notification preference writes require the exact closed boolean map', () => {
  const current = safeCandidateNotificationPreferences(undefined);
  assert.deepEqual(requireCandidateNotificationPreferences(current), current);
  assert.throws(
    () => requireCandidateNotificationPreferences({ ...current, email: true }),
    /CANDIDATE_NOTIFICATION_PREFERENCES_INVALID/
  );
  assert.throws(
    () => requireCandidateNotificationPreferences({ ...current, payment: 'yes' }),
    /CANDIDATE_NOTIFICATION_PREFERENCES_INVALID/
  );
});

test('Candidate notification projection closes internal database payloads and links', () => {
  const notification = safeCandidateNotification({
    id: '00000000-0000-4000-8000-000000000061',
    workflow_id: '00000000-0000-4000-8000-000000000062',
    timesheet_id: '00000000-0000-4000-8000-000000000063',
    event_type: 'PAPER_PACK_READY',
    template_key: 'candidate-paper-pack-ready-v1',
    template_params: { page_count: 4, workflow_generation: 2, internal_note: 'never return' },
    deep_link_json: {
      type: 'paper_pack',
      timesheet_id: '00000000-0000-4000-8000-000000000063',
      workflow_id: '00000000-0000-4000-8000-000000000062',
      workflow_generation: 2,
      private_service: 'never return'
    },
    state: 'UNREAD', created_at_utc: '2026-08-23T10:00:00.000Z', read_at_utc: null
  });
  assert.deepEqual(notification, {
    id: '00000000-0000-4000-8000-000000000061',
    event_type: 'PAPER_PACK_READY',
    template_key: 'candidate-paper-pack-ready-v1',
    payload_json: {
      state: 'PAPER_PACK_READY', candidate_status_code: 'PAPER_PACK_READY',
      message: 'Your printed signing documents are ready.',
      occurred_at_utc: '2026-08-23T10:00:00.000Z',
      workflow_id: '00000000-0000-4000-8000-000000000062',
      timesheet_id: '00000000-0000-4000-8000-000000000063'
    },
    deep_link_json: {
      destination: 'TIMESHEET_DETAIL',
      workflow_id: '00000000-0000-4000-8000-000000000062',
      timesheet_id: '00000000-0000-4000-8000-000000000063'
    },
    state: 'UNREAD', created_at_utc: '2026-08-23T10:00:00.000Z', read_at_utc: null
  });
  assert.equal(JSON.stringify(notification).includes('internal_note'), false);
  assert.equal(JSON.stringify(notification).includes('private_service'), false);
  assert.equal(JSON.stringify(notification).includes('workflow_generation'), false);
});

test('Candidate notification feed is scoped to the selected candidate and reads template_params', async () => {
  const sessionId = '00000000-0000-4000-8000-000000000064';
  const accountId = '00000000-0000-4000-8000-000000000065';
  const candidateId = '00000000-0000-4000-8000-000000000066';
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'test-only-secret-material',
    SUPABASE_URL: 'https://test.example.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  const session = {
    id: sessionId, session_id: sessionId, account_id: accountId,
    selected_candidate_id: candidateId, environment: 'TEST', status: 'ACTIVE', rotation: 1,
    expires_at_utc: '2099-01-01T00:00:00.000Z',
    absolute_expires_at_utc: '2099-01-02T00:00:00.000Z'
  };
  const token = await createAccessToken(env, session);
  const originalFetch = globalThis.fetch;
  let notificationRequest = null;
  globalThis.fetch = async url => {
    const value = String(url);
    if (value.includes('/candidate_app_sessions?')) return Response.json([session]);
    if (value.includes('/candidate_notifications?')) {
      notificationRequest = value;
      return Response.json([{
        id: '00000000-0000-4000-8000-000000000067', workflow_id: null,
        timesheet_id: '00000000-0000-4000-8000-000000000068',
        event_type: 'AUTHORISED', template_key: 'candidate-submission-finalised-v1',
        template_params: { auto_authorised: true },
        deep_link_json: { type: 'timesheet', timesheet_id: '00000000-0000-4000-8000-000000000068' },
        state: 'UNREAD', created_at_utc: '2026-08-23T11:00:00.000Z', read_at_utc: null
      }]);
    }
    throw new Error(`unexpected request ${value}`);
  };
  try {
    const response = await handleCandidateAppRequest(new Request(
      'https://private.test/candidate-app/v1/notifications',
      { headers: { authorization: `Bearer ${token}` } }
    ), env, {}, { routeAudience: 'PRIVATE' });
    assert.equal(response.status, 200);
    const body = await response.json();
    assert.equal(body.notifications[0].payload_json.message, 'Your timesheet has been authorised.');
    assert.equal(body.notifications[0].deep_link_json.destination, 'TIMESHEET_DETAIL');
    assert.match(notificationRequest, new RegExp(`candidate_id=eq\\.${candidateId}`));
    assert.match(notificationRequest, /template_params/);
    assert.doesNotMatch(notificationRequest, /payload_json/);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

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
    candidate_mail_authority: 'CANDIDATE_PAPER_V1',
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
  const proof = await passwordVerificationProof('correct-horse-battery-staple', {
    id: 'ca130813-4000-4000-8000-000000000001', ...account
  });
  assert.equal(proof.matches, true);
  assert.equal(proof.presented_password_digest_hex, verifier.digest_hex);
  assert.match(proof.expected_password_authority_sha256, /^[0-9a-f]{64}$/);
});

test('challenge delivery tokens are stable for an idempotent replay and scoped to its identity', async () => {
  const first = await deterministicOpaqueToken('test-secret', 'challenge', 'TEST', 'ACTIVATE', 'person@example.test', 'key-1');
  const replay = await deterministicOpaqueToken('test-secret', 'challenge', 'TEST', 'ACTIVATE', 'person@example.test', 'key-1');
  const other = await deterministicOpaqueToken('test-secret', 'challenge', 'TEST', 'ACTIVATE', 'person@example.test', 'key-2');
  assert.equal(first, replay);
  assert.notEqual(first, other);
  assert.equal(first.includes('='), false);
});

test('challenge replay validates changed factual input before reconstructing its token', async () => {
  const secret = 'challenge-replay-secret';
  const key = 'challenge-changed-email-key';
  const originalToken = await deterministicOpaqueToken(
    secret, 'candidate-auth-challenge-v1', 'TEST', 'ACTIVATE',
    'original@example.test', '', key
  );
  const tokenHash = createHash('sha256').update(originalToken).digest('hex');
  let calls = 0;
  const response = await handleCandidateAppRequest(new Request(
    'https://private.test/candidate-app/v1/auth/challenge/start', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        email: 'changed@example.test', purpose: 'ACTIVATE', idempotency_key: key
      })
    }
  ), {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_CHALLENGE_TOKEN_SECRET: secret
  }, {}, {
    routeAudience: 'PRIVATE',
    async rpc(_name, args) {
      calls += 1;
      if (args.p_token_hash == null) {
        return { replay_receipt_found: true, token_hash_hex: tokenHash, token_key_version: 1 };
      }
      assert.equal(args.p_token_hash, `\\x${tokenHash}`);
      assert.equal(args.p_token_key_version, 1);
      throw new Error('CANDIDATE_IDEMPOTENCY_CONFLICT');
    }
  });
  assert.equal(response.status, 409);
  assert.equal((await response.json()).error_code, 'CANDIDATE_IDEMPOTENCY_CONFLICT');
  assert.equal(calls, 2);
});

test('challenge replay reads its recorded token version across writer rollback and honours reader retirement', async () => {
  const originalFetch = globalThis.fetch;
  let queuedMail = null;
  const key = 'challenge-version-rollback-key';
  const email = 'rollback@example.test';
  const versionTwoSecret = 'challenge-version-two-secret';
  const token = await deterministicOpaqueToken(
    versionTwoSecret, 'candidate-auth-challenge-v1', 'TEST', 'ACTIVATE', email, '', key
  );
  const tokenHash = createHash('sha256').update(token).digest('hex');
  const deps = {
    routeAudience: 'PRIVATE',
    async rpc(_name, args) {
      if (args.p_token_hash == null) {
        return { replay_receipt_found: true, token_hash_hex: tokenHash, token_key_version: 2 };
      }
      assert.equal(args.p_token_key_version, 2);
      return {
        ok: true, accepted: true, deliver_email: false, idempotent_replay: true,
        challenge_id: '00000000-0000-4000-8000-000000000043',
        expires_at_utc: '2099-01-01T00:00:00.000Z',
        token_hash_hex: tokenHash,
        token_key_version: 2
      };
    }
  };
  const request = () => new Request('https://private.test/candidate-app/v1/auth/challenge/start', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ email, purpose: 'ACTIVATE', idempotency_key: key })
  });
  const rolledBackEnv = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_CHALLENGE_TOKEN_KEY_VERSION: '1',
    CANDIDATE_CHALLENGE_TOKEN_READ_KEY_VERSIONS: '1,2',
    CANDIDATE_CHALLENGE_TOKEN_SECRET_V1: 'challenge-version-one-secret',
    CANDIDATE_CHALLENGE_TOKEN_SECRET_V2: versionTwoSecret,
    CANDIDATE_APP_PUBLIC_URL: 'https://candidate.test.example',
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  globalThis.fetch = async (_url, init) => {
    queuedMail = JSON.parse(String(init?.body || '{}'));
    return Response.json([{}]);
  };
  try {
    const readable = await handleCandidateAppRequest(request(), rolledBackEnv, {}, deps);
    assert.equal(readable.status, 202, JSON.stringify(await readable.clone().json()));
    assert.match(queuedMail.body_text, /\/candidate\/activate#token=[^&\s]+&challenge=00000000-0000-4000-8000-000000000043/);
    assert.match(queuedMail.body_html, /\/candidate\/activate#token=[^&"]+&amp;challenge=00000000-0000-4000-8000-000000000043/);

    const retired = await handleCandidateAppRequest(request(), {
      ...rolledBackEnv, CANDIDATE_CHALLENGE_TOKEN_READ_KEY_VERSIONS: '1'
    }, {}, deps);
    assert.equal(retired.status, 503);
    assert.equal((await retired.json()).error_code, 'CANDIDATE_REPLAY_SECRET_VERSION_UNAVAILABLE');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('an enumeration-masked challenge result replays without requiring retired token material', async () => {
  const response = await handleCandidateAppRequest(new Request(
    'https://private.test/candidate-app/v1/auth/challenge/start', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        email: 'ineligible@example.test', purpose: 'RESET',
        idempotency_key: 'ineligible-retired-token-key'
      })
    }
  ), {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_CHALLENGE_TOKEN_KEY_VERSION: '1',
    CANDIDATE_CHALLENGE_TOKEN_READ_KEY_VERSIONS: '1',
    CANDIDATE_CHALLENGE_TOKEN_SECRET_V1: 'current-reader-secret'
  }, {}, {
    routeAudience: 'PRIVATE',
    async rpc(_name, args) {
      if (args.p_token_hash == null) {
        return {
          replay_receipt_found: true, token_hash_hex: 'ab'.repeat(32), token_key_version: 2
        };
      }
      return { ok: true, accepted: true, deliver_email: false, idempotent_replay: true };
    }
  });
  assert.equal(response.status, 202);
});

test('private challenge resend exposes durable throttle receipts as HTTP 429', async () => {
  let writes = 0;
  let receipt = null;
  const deps = {
    routeAudience: 'PRIVATE',
    async rpc(_name, args) {
      if (args.p_token_hash == null) {
        return receipt
          ? { replay_receipt_found: true, token_hash_hex: receipt.token_hash_hex, token_key_version: 1 }
          : { replay_receipt_found: false };
      }
      if (!receipt) {
        writes += 1;
        receipt = {
          token_hash_hex: String(args.p_token_hash).slice(2),
          response: {
            ok: false, error_code: 'CANDIDATE_CHALLENGE_RESEND_TOO_SOON',
            terminal: false, retry_after_seconds: 41
          }
        };
      }
      return receipt.response;
    }
  };
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_CHALLENGE_TOKEN_SECRET: 'private-throttle-secret',
    CANDIDATE_CHALLENGE_TOKEN_KEY_VERSION: '1',
    CANDIDATE_CHALLENGE_TOKEN_READ_KEY_VERSIONS: '1'
  };
  const invoke = () => handleCandidateAppRequest(new Request(
    'https://private.test/candidate-app/v1/auth/challenge/resend', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        email: 'candidate@example.test', purpose: 'ACTIVATE',
        challenge_id: '00000000-0000-4000-8000-000000000042',
        idempotency_key: 'private-durable-throttle-key'
      })
    }
  ), env, {}, deps);
  const first = await invoke();
  const replay = await invoke();
  assert.equal(first.status, 429);
  assert.equal(replay.status, 429);
  assert.equal(first.headers.get('retry-after'), '41');
  assert.equal((await first.json()).details.retry_after_seconds, 41);
  assert.equal((await replay.json()).error_code, 'CANDIDATE_CHALLENGE_RESEND_TOO_SOON');
  assert.equal(writes, 1);
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
    assert.equal(rpcCalls[0].name, 'candidate_app_timesheet_detail_v2');
    assert.equal(rpcCalls[0].args.p_contract_week_id, contractWeekId);
    assert.equal(rpcCalls[0].args.p_workflow_id, null);
    assert.equal(rpcCalls[1].args.p_contract_week_id, null);
    assert.equal(rpcCalls[1].args.p_workflow_id, workflowId);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('timesheet page adds mileage units and current supporting-evidence facts without exposing summary pages', async () => {
  const sessionId = '00000000-0000-4000-8000-000000000091';
  const candidateId = '00000000-0000-4000-8000-000000000092';
  const workflowId = '00000000-0000-4000-8000-000000000093';
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'test-only-secret-material',
    SUPABASE_URL: 'https://test.example.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  const session = {
    session_id: sessionId, id: sessionId,
    account_id: '00000000-0000-4000-8000-000000000094',
    selected_candidate_id: candidateId, environment: 'TEST', status: 'ACTIVE', rotation: 1,
    expires_at_utc: '2099-01-01T00:00:00.000Z', absolute_expires_at_utc: '2099-01-02T00:00:00.000Z'
  };
  const token = await createAccessToken(env, session);
  const originalFetch = globalThis.fetch;
  const reads = [];
  globalThis.fetch = async input => {
    const url = new URL(String(input));
    reads.push(url.href);
    if (url.pathname.endsWith('/candidate_app_sessions')) return Response.json([session]);
    if (url.pathname.endsWith('/candidate_submission_workflows')) return Response.json([{
      id: workflowId, generation: 2, workflow_kind: 'CONTRACT_COMBINED', state: 'RECEIVED',
      immutable_submission_json: { expense_submission: { canonical_tsfin_snapshot: { mileage_units: 18 } } },
      updated_at_utc: '2026-08-31T10:00:00.000Z'
    }]);
    if (url.pathname.endsWith('/candidate_submission_components')) return Response.json([
      { id: '00000000-0000-4000-8000-000000000095', workflow_id: workflowId, workflow_generation: 2, component_kind: 'MILEAGE_FORM', expense_category: null },
      { id: '00000000-0000-4000-8000-000000000096', workflow_id: workflowId, workflow_generation: 2, component_kind: 'EXPENSE_EVIDENCE', expense_category: 'ACCOMMODATION' },
      { id: '00000000-0000-4000-8000-000000000097', workflow_id: workflowId, workflow_generation: 1, component_kind: 'EXPENSE_EVIDENCE', expense_category: 'TRAVEL' },
      { id: '00000000-0000-4000-8000-000000000098', workflow_id: workflowId, workflow_generation: 2, component_kind: 'EXPENSE_SUMMARY', expense_category: 'OTHER' }
    ]);
    throw new Error(`unexpected fetch ${url.href}`);
  };
  const deps = {
    routeAudience: 'PRIVATE',
    async rpc() {
      return { ok: true, items: [{
        expenses: { expenses_pay_ex_vat: 20, mileage_pay_ex_vat: 4.5, travel_pay_ex_vat: 0, accommodation_pay_ex_vat: 20, other_pay_ex_vat: 0 },
        workflows: [{ workflow_id: workflowId, workflow_kind: 'CONTRACT_COMBINED', state: 'RECEIVED' }]
      }] };
    }
  };
  try {
    const response = await handleCandidateAppRequest(new Request('https://private.test/candidate-app/v1/timesheets', {
      headers: { authorization: `Bearer ${token}` }
    }), env, {}, deps);
    assert.equal(response.status, 200);
    const body = await response.json();
    assert.equal(body.items[0].expenses.mileage_units, 18);
    assert.equal(body.items[0].expenses.supporting_evidence_count, 2);
    assert.deepEqual(body.items[0].expenses.supporting_evidence_categories, ['MILEAGE', 'ACCOMMODATION']);
    assert.match(reads.find((url) => url.includes('/candidate_submission_workflows?')), new RegExp(`candidate_id=eq.${candidateId}`));
    const componentRead = reads.find((url) => url.includes('/candidate_submission_components?'));
    assert.match(componentRead, /component_kind=in\.\(MILEAGE_FORM%2CEXPENSE_EVIDENCE\)|component_kind=in\.\(MILEAGE_FORM,EXPENSE_EVIDENCE\)/);
    assert.match(componentRead, /workflow_generation/);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('timesheet detail enriches only authorised component ids with page lineage', async () => {
  const sessionId = '00000000-0000-4000-8000-0000000000b1';
  const componentId = '00000000-0000-4000-8000-0000000000b2';
  const sourceId = '00000000-0000-4000-8000-0000000000b3';
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST', CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'test-only-secret-material',
    SUPABASE_URL: 'https://test.example.invalid', SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  const session = {
    session_id: sessionId, id: sessionId, account_id: '00000000-0000-4000-8000-0000000000b4',
    selected_candidate_id: '00000000-0000-4000-8000-0000000000b5', environment: 'TEST', status: 'ACTIVE', rotation: 1,
    expires_at_utc: '2099-01-01T00:00:00.000Z', absolute_expires_at_utc: '2099-01-02T00:00:00.000Z'
  };
  const token = await createAccessToken(env, session);
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async input => {
    const url = new URL(String(input));
    if (url.pathname.endsWith('/candidate_app_sessions')) return Response.json([session]);
    if (url.pathname.endsWith('/candidate_submission_components')) {
      assert.equal(url.searchParams.get('id'), `in.(${componentId})`);
      return Response.json([{ id: componentId, source_component_id: sourceId, paper_return_page_key: 'EXPENSE_SUMMARY' }]);
    }
    throw new Error(`unexpected fetch ${url.href}`);
  };
  const deps = { routeAudience: 'PRIVATE', async rpc() {
    return { ok: true, components: [{ id: componentId, component_kind: 'SIGNED_RETURN' }], workflows: [] };
  } };
  try {
    const response = await handleCandidateAppRequest(new Request(
      `https://private.test/candidate-app/v1/timesheets/00000000-0000-4000-8000-0000000000b6`,
      { headers: { authorization: `Bearer ${token}` } }
    ), env, {}, deps);
    assert.equal(response.status, 200);
    const body = await response.json();
    assert.equal(body.components[0].source_component_id, sourceId);
    assert.equal(body.components[0].paper_return_page_key, 'EXPENSE_SUMMARY');
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
  const approvalRequestId = '00000000-0000-4000-8000-000000000095';
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'test-only-secret-material',
    CANDIDATE_AGENCY_ID: '00000000-0000-4000-8000-000000000099',
    MYTMS_MANAGER_ROUTE_HMAC_SECRET: 'test-only-manager-route-secret-material',
    MYTMS_CONTROL_PLANE_ENABLED: 'true',
    MYTMS_CONTROL_PLANE_URL: 'https://control.test.invalid',
    MYTMS_CONTROL_PLANE_SERVICE_ROLE_KEY: 'test-control-service-key',
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
  globalThis.fetch = async input => {
    const value = input instanceof Request ? input.url : String(input);
    if (value.includes('/rpc/manager_review_origin_resolve_v1')) return Response.json({
      ok: true, manager_review_public_origin: 'https://manager.test.invalid', settings_version: 1,
      manager_review_origin_semantic_sha256_hex: 'a'.repeat(64)
    });
    if (value.includes('/rpc/manager_email_route_register_v1')) return Response.json({
      ok: true, manager_route_ticket_id: '00000000-0000-4000-8000-000000000096',
      route_revision: 1, registration_receipt_sha256_hex: 'b'.repeat(64)
    });
    if (value.includes('candidate_app_sessions')) return Response.json([session]);
    if (value.includes('candidate_submission_workflows')) return Response.json([{
      id: workflowId, workflow_kind: 'CONTRACT_HOURS', candidate_id: candidateId
    }]);
    if (value.includes('/candidates?')) return Response.json([{
      id: candidateId, display_name: 'Kier Arthur', first_name: 'Kier', last_name: 'Arthur'
    }]);
    if (value.includes('candidate_approval_requests')) {
      return Response.json([{
        id: approvalRequestId, request_generation: 2,
        manager_email_normalized: 'manager@example.test', resend_count: 1,
        expires_at_utc: '2099-01-01T00:00:00.000Z', token_hash: 'c'.repeat(64)
      }]);
    }
    throw new Error(`unexpected fetch ${value}`);
  };
  const calls = [];
  const deps = {
    routeAudience: 'PRIVATE',
    async rpc(name, args) {
      if (name === 'candidate_manager_email_settings_get_v1') return {
        ok: true, version: 1, semantic_sha256_hex: 'd'.repeat(64),
        templates: { TIMESHEET: {
          REMINDER: {
            subject: 'Reminder: timesheet approval required', body_text: 'Please review every page.',
            body_html: '<p>Please review every page.</p>', button_text: 'Review and approve', include_link: true
          },
          CANCELLATION: {
            subject: 'Timesheet approval request cancelled',
            body_text: 'This approval request has been cancelled. No further action is required.',
            body_html: '<p>This approval request has been cancelled. No further action is required.</p>',
            button_text: null, include_link: false
          }
        } }
      };
      if (name === 'candidate_manager_email_route_receipt_commit_v1') return { ok: true };
      calls.push({ name, args });
      return {
        ok: true, state: 'AWAITING_MANAGER_APPROVAL', approval_request_id: approvalRequestId,
        mail_outbox_id: '00000000-0000-4000-8000-000000000097'
      };
    }
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
      generation: 3, idempotency_key: 'reminder-1',
      approval_request_id: approvalRequestId, approval_request_generation: 2
    }), env, {}, deps);
    assert.equal(reminder.status, 200);
    assert.equal(calls[0].name, 'candidate_workflow_transition_atomic_v1');
    assert.equal(calls[0].args.p_action, 'REMIND');
    assert.equal(calls[0].args.p_payload.mutation_replay_probe_only, true);
    assert.equal(calls[0].args.p_payload.mutation_replay_semantic_payload.mail, undefined);
    assert.equal(calls[0].args.p_payload.mutation_replay_semantic_payload.approval_token_hash_hex, undefined);
    assert.equal(calls[1].args.p_action, 'REMIND');
    assert.match(calls[1].args.p_payload.mail.subject, /^Reminder:/);
    assert.match(calls[1].args.p_payload.mail.body_text,
      /Review and approve Kier Arthur timesheet:/);
    assert.match(calls[1].args.p_payload.mail.body_html,
      />Review and approve Kier Arthur timesheet<\/a>/);
    assert.match(calls[1].args.p_payload.approval_token_hash_hex, /^[0-9a-f]{64}$/);

    const missingReason = await handleCandidateAppRequest(request('cancel', {
      generation: 3, idempotency_key: 'cancel-1'
    }), env, {}, deps);
    assert.equal(missingReason.status, 400);
    assert.equal((await missingReason.json()).error_code, 'CANDIDATE_CANCELLATION_REASON_REQUIRED');
    assert.equal(calls.length, 2);

    const cancelled = await handleCandidateAppRequest(request('cancel', {
      generation: 3, idempotency_key: 'cancel-2', reason_note: 'I entered the wrong week.'
    }), env, {}, deps);
    assert.equal(cancelled.status, 200);
    assert.equal(calls[2].name, 'candidate_workflow_cancel_atomic_v2');
    assert.equal(calls[2].args.p_action, undefined);
    assert.equal(calls[2].args.p_payload.reason_note, 'I entered the wrong week.');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('Candidate workflow mutation requires a caller key and an exact WORKER_SUBMIT replay skips mutable enrichment', async () => {
  const sessionId = '00000000-0000-4000-8000-0000000000a1';
  const accountId = '00000000-0000-4000-8000-0000000000a2';
  const candidateId = '00000000-0000-4000-8000-0000000000a3';
  const workflowId = '00000000-0000-4000-8000-0000000000a4';
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
  let sessionReads = 0;
  globalThis.fetch = async url => {
    const value = String(url);
    if (value.includes('candidate_app_sessions')) {
      sessionReads += 1;
      return Response.json([session]);
    }
    throw new Error(`mutable enrichment ran before replay: ${value}`);
  };
  const calls = [];
  const deps = {
    routeAudience: 'PRIVATE',
    async rpc(name, args) {
      calls.push({ name, args });
      return {
        ok: true, idempotent_replay: true, state: 'WORKER_SUBMITTED',
        workflow_id: workflowId, generation: 2
      };
    }
  };
  try {
    const missingKey = await handleCandidateAppRequest(new Request(
      `https://private.test/candidate-app/v1/workflows/${workflowId}/actions/cancel`, {
        method: 'POST',
        headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
        body: JSON.stringify({ generation: 1, reason_note: 'Wrong week.' })
      }
    ), env, {}, deps);
    assert.equal(missingKey.status, 400);
    assert.equal((await missingKey.json()).error_code, 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED');
    assert.equal(calls.length, 0);

    const contradictoryHours = await handleCandidateAppRequest(new Request(
      `https://private.test/candidate-app/v1/workflows/${workflowId}/actions/worker-submit`, {
        method: 'POST',
        headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
        body: JSON.stringify({
          generation: 1,
          idempotency_key: 'worker-submit-contradictory-hours',
          candidate_signed_at_utc: '2026-08-12T10:00:00.000Z',
          immutable_submission: {
            actual_schedule_json: [{
              date: '2026-07-20',
              start: '09:00', end: '17:00',
              start_time: '17:00', end_time: '18:00'
            }]
          }
        })
      }
    ), env, {}, deps);
    assert.equal(contradictoryHours.status, 400);
    assert.equal((await contradictoryHours.json()).error_code,
      'CANDIDATE_WORK_INTERVAL_CONTRADICTORY');
    assert.equal(calls.length, 0);

    const replay = await handleCandidateAppRequest(new Request(
      `https://private.test/candidate-app/v1/workflows/${workflowId}/actions/worker-submit`, {
        method: 'POST',
        headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
        body: JSON.stringify({
          generation: 1,
          idempotency_key: 'worker-submit-exact-replay',
          candidate_signed_at_utc: '2026-08-12T10:00:00.000Z',
          immutable_submission: { worked_minutes: 480 }
        })
      }
    ), env, {}, deps);
    assert.equal(replay.status, 200);
    assert.equal((await replay.json()).idempotent_replay, true);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].args.p_payload.mutation_replay_probe_only, true);
    assert.equal(calls[0].args.p_payload.mutation_replay_semantic_payload
      .submission_request_identity.factual_submission.worked_minutes, 480);
    assert.equal(sessionReads, 3);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('pending WORKER_SUBMIT replay resumes review rendering without exposing the internal contract', async () => {
  const sessionId = '00000000-0000-4000-8000-0000000000b1';
  const accountId = '00000000-0000-4000-8000-0000000000b2';
  const candidateId = '00000000-0000-4000-8000-0000000000b3';
  const workflowId = '00000000-0000-4000-8000-0000000000b4';
  const componentId = '00000000-0000-4000-8000-0000000000b5';
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
  const originalConsoleError = console.error;
  const background = [];
  globalThis.fetch = async url => {
    if (String(url).includes('candidate_app_sessions')) return Response.json([session]);
    throw new Error('synthetic render dependency unavailable');
  };
  console.error = () => {};
  const deps = {
    routeAudience: 'PRIVATE',
    async rpc() {
      return {
        ok: true,
        idempotent_replay: true,
        state: 'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT',
        workflow_id: workflowId,
        generation: 2,
        render_contract: {
          components: [{
            workflow_id: workflowId,
            workflow_generation: 2,
            component_id: componentId,
            render_input_sha256: '0'.repeat(64)
          }]
        }
      };
    }
  };
  try {
    const response = await handleCandidateAppRequest(new Request(
      `https://private.test/candidate-app/v1/workflows/${workflowId}/actions/worker-submit`, {
        method: 'POST',
        headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
        body: JSON.stringify({
          generation: 1,
          idempotency_key: 'worker-submit-render-replay',
          candidate_signed_at_utc: '2026-08-23T06:35:47.104Z',
          immutable_submission: { worked_minutes: 450 }
        })
      }
    ), env, { waitUntil(promise) { background.push(promise); } }, deps);
    assert.equal(response.status, 202);
    const body = await response.json();
    assert.equal(body.idempotent_replay, true);
    assert.equal(body.review_rendering_accepted, true);
    assert.equal(body.render_contract, undefined);
    assert.equal(background.length, 1);
    assert.deepEqual(await background[0], { ok: false, error_code: 'CANDIDATE_REQUEST_FAILED' });
  } finally {
    globalThis.fetch = originalFetch;
    console.error = originalConsoleError;
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

test('office cancel manager request maps to request-only authority and preserves the typed reason', async () => {
  const actorId = '00000000-0000-4000-8000-000000000271';
  const workflowId = '00000000-0000-4000-8000-000000000272';
  const approvalId = '00000000-0000-4000-8000-000000000273';
  const idempotencyKey = '00000000-0000-4000-8000-000000000274';
  const calls = [];
  const officeRoles = [];
  const deps = {
    routeAudience: 'OFFICE',
    async requireOfficeUser(_request, roles) {
      officeRoles.push(roles);
      return { id: actorId, role: 'admin' };
    },
    async rpc(name, args) {
      if (name === 'candidate_manager_email_settings_get_v1') return {
        ok: true, version: 1, semantic_sha256_hex: 'd'.repeat(64),
        templates: { TIMESHEET: { WITHDRAWAL: {
          subject: 'Timesheet approval request withdrawn',
          body_text: 'This approval request has been withdrawn. No further action is required.',
          body_html: '<p>This approval request has been withdrawn. No further action is required.</p>',
          button_text: null, include_link: false
        } } }
      };
      calls.push({ name, args });
      return { ok: true, state: 'READY_FOR_MANAGER_APPROVAL', claim_cancelled: false };
    }
  };
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async url => {
    const value = String(url);
    if (value.includes('candidate_submission_workflows')) return Response.json([{
      id: workflowId, generation: 2, route: 'EMAIL', state: 'AWAITING_MANAGER_APPROVAL'
    }]);
    if (value.includes('candidate_approval_requests')) return Response.json([{
      id: approvalId, workflow_id: workflowId, workflow_generation: 2,
      request_generation: 1, method: 'EMAIL', state: 'PENDING'
    }]);
    throw new Error(`unexpected URL: ${value}`);
  };
  try {
    const response = await handleCandidateAppRequest(new Request(
      `https://office.test/api/candidate-app/workflows/${workflowId}/actions/cancel`, {
        method: 'POST', headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          generation: 2, approval_request_id: approvalId, approval_request_generation: 1,
          idempotency_key: idempotencyKey,
          reason: 'The office is selecting another approval method.'
        })
      }
    ), {
      CANDIDATE_APP_ENVIRONMENT: 'TEST', SUPABASE_URL: 'https://test.supabase.invalid',
      SUPABASE_SERVICE_ROLE_KEY: 'placeholder'
    }, {}, deps);
    assert.equal(response.status, 200);
    assert.deepEqual(officeRoles, [['admin']]);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].name, 'cloudtms_office_candidate_adapter_v1');
    assert.equal(calls[0].args.p_action, 'WORKFLOW_ACTION_EXECUTE');
    assert.equal(calls[0].args.p_payload.workflow_action, 'MANAGER_REQUEST_CANCEL');
    assert.equal(calls[0].args.p_payload.payload.reason_note,
      'The office is selecting another approval method.');
    assert.equal((await response.json()).claim_cancelled, false);
  } finally {
    globalThis.fetch = originalFetch;
  }
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
      if (name === 'candidate_manager_email_settings_get_v1') return {
        ok: true, version: 1, semantic_sha256_hex: 'd'.repeat(64),
        templates: { TIMESHEET: { REMINDER: {
          subject: 'Reminder: timesheet approval required', body_text: 'Please review every page.',
          body_html: '<p>Please review every page.</p>', button_text: 'Review and approve', include_link: true
        } } }
      };
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
  globalThis.fetch = async input => {
    const value = input instanceof Request ? input.url : String(input);
    if (value.includes('/rpc/manager_review_origin_resolve_v1')) return Response.json({
      ok: true, manager_review_public_origin: 'https://manager.test.invalid', settings_version: 1,
      manager_review_origin_semantic_sha256_hex: 'a'.repeat(64)
    });
    if (value.includes('candidate_submission_workflows')) return Response.json([{
      id: workflowId, workflow_kind: 'CONTRACT_HOURS',
      candidate_id: '00000000-0000-4000-8000-000000000236'
    }]);
    if (value.includes('/candidates?')) return Response.json([{
      id: '00000000-0000-4000-8000-000000000236', display_name: 'Kier Arthur',
      first_name: 'Kier', last_name: 'Arthur'
    }]);
    assert.match(value, /candidate_approval_requests/);
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
      CANDIDATE_AGENCY_ID: '00000000-0000-4000-8000-000000000239',
      MYTMS_MANAGER_ROUTE_HMAC_SECRET: 'test-only-manager-route-secret-material',
      MYTMS_CONTROL_PLANE_ENABLED: 'true',
      MYTMS_CONTROL_PLANE_URL: 'https://control.test.invalid',
      MYTMS_CONTROL_PLANE_SERVICE_ROLE_KEY: 'test-control-service-key'
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

test('office reminder eligibility is server-owned, eligible-only and paginated', async () => {
  const actorId = '00000000-0000-4000-8000-000000000261';
  const workflowId = '00000000-0000-4000-8000-000000000262';
  const requestId = '00000000-0000-4000-8000-000000000263';
  const candidateId = '00000000-0000-4000-8000-000000000264';
  const timesheetId = '00000000-0000-4000-8000-000000000265';
  const acceptedAt = '2026-08-13T08:15:00.000Z';
  const rpcCalls = [];
  const deps = {
    routeAudience: 'OFFICE',
    async requireOfficeUser() { return { id: actorId }; },
    async rpc(name, args) {
      rpcCalls.push({ name, args });
      assert.equal(args.p_action, 'PROJECT_BATCH');
      return {
        ok: true,
        results: [{
          ok: true,
          correlation_key: timesheetId,
          projection: {
            current_identity: { row_key: timesheetId, timesheet_id: timesheetId, expected_row_signature: 'row-signature' },
            manager_approval: {
              request_id: requestId,
              request_generation: 3,
              provider_accepted_at_utc: acceptedAt
            },
            available_actions: [{
              code: 'SEND_MANAGER_REMINDER', enabled: true,
              invocation: { path: `/api/candidate-app/workflows/${workflowId}/actions/remind` }
            }]
          }
        }]
      };
    }
  };
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async url => {
    const value = String(url);
    if (value.includes('/candidate_approval_requests?')) return Response.json([{
      id: requestId, workflow_id: workflowId, workflow_generation: 2,
      request_generation: 3, method: 'EMAIL', state: 'PENDING'
    }]);
    if (value.includes('/candidate_submission_workflows?')) return Response.json([{
      id: workflowId, candidate_id: candidateId, generation: 2,
      contract_week_id: null, anchor_timesheet_id: timesheetId,
      target_timesheet_id: timesheetId, updated_at_utc: '2026-08-13T08:00:00.000Z'
    }]);
    if (value.includes('/candidates?')) return Response.json([{
      id: candidateId, display_name: 'Alex Candidate', first_name: 'Alex', last_name: 'Candidate'
    }]);
    throw new Error(`unexpected URL: ${value}`);
  };
  try {
    const response = await handleCandidateAppRequest(new Request(
      'https://office.test/api/candidate-app/manager-reminder-eligibility?page=1&page_size=25'
    ), {
      CANDIDATE_APP_ENVIRONMENT: 'TEST', SUPABASE_URL: 'https://test.supabase.invalid',
      SUPABASE_SERVICE_ROLE_KEY: 'placeholder'
    }, {}, deps);
    assert.equal(response.status, 200);
    const result = await response.json();
    assert.equal(result.contract_version, 'OFFICE_CANDIDATE_REMINDER_ELIGIBILITY_PAGE_V1');
    assert.equal(result.total_items, 1);
    assert.equal(result.catalogue_total_items, 1);
    assert.equal(result.page_count, 1);
    assert.equal(result.sort_by, 'CANDIDATE_SURNAME');
    assert.equal(result.sort_direction, 'ASC');
    assert.deepEqual(result.matching_selection_keys, [requestId]);
    assert.equal(result.items[0].selection_key, requestId);
    assert.equal(result.items[0].candidate_name, 'Alex Candidate');
    assert.equal(result.items[0].candidate_surname, 'Candidate');
    assert.equal(result.items[0].last_manager_email_at_utc, acceptedAt);
    assert.equal(result.items[0].identity.timesheet_id, timesheetId);
    assert.match(result.catalogue_revision, /^[a-f0-9]{64}$/);
    assert.equal(rpcCalls.length, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('office reminder catalogue filters by Candidate surname and sorts every matching page', async () => {
  const actorId = '00000000-0000-4000-8000-000000000361';
  const surnames = ['Baines', 'Smith', 'Barker'];
  const approvals = surnames.map((surname, index) => ({
    id: `00000000-0000-4000-8000-00000000037${index + 1}`,
    workflow_id: `00000000-0000-4000-8000-00000000038${index + 1}`,
    workflow_generation: 1, request_generation: 1, method: 'EMAIL', state: 'PENDING'
  }));
  const workflows = approvals.map((approval, index) => ({
    id: approval.workflow_id,
    candidate_id: `00000000-0000-4000-8000-00000000039${index + 1}`,
    generation: 1, contract_week_id: null,
    anchor_timesheet_id: `00000000-0000-4000-8000-00000000040${index + 1}`,
    target_timesheet_id: `00000000-0000-4000-8000-00000000040${index + 1}`,
    updated_at_utc: `2026-08-13T08:0${index}:00.000Z`
  }));
  const candidates = workflows.map((workflow, index) => ({
    id: workflow.candidate_id, display_name: `Candidate ${surnames[index]}`,
    first_name: 'Candidate', last_name: surnames[index]
  }));
  const deps = {
    routeAudience: 'OFFICE',
    async requireOfficeUser() { return { id: actorId }; },
    async rpc(name, args) {
      assert.equal(args.p_action, 'PROJECT_BATCH');
      return {
        ok: true,
        results: workflows.map((workflow, index) => ({
          ok: true,
          correlation_key: workflow.target_timesheet_id,
          projection: {
            current_identity: { row_key: workflow.target_timesheet_id, timesheet_id: workflow.target_timesheet_id },
            manager_approval: {
              request_id: approvals[index].id, request_generation: 1,
              provider_accepted_at_utc: `2026-08-13T0${7 + index}:00:00.000Z`
            },
            available_actions: [{
              code: 'SEND_MANAGER_REMINDER', enabled: true,
              invocation: { path: `/api/candidate-app/workflows/${workflow.id}/actions/remind` }
            }]
          }
        }))
      };
    }
  };
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async url => {
    const value = String(url);
    if (value.includes('/candidate_approval_requests?')) return Response.json(approvals);
    if (value.includes('/candidate_submission_workflows?')) return Response.json(workflows);
    if (value.includes('/candidates?')) return Response.json(candidates);
    throw new Error(`unexpected URL: ${value}`);
  };
  try {
    const response = await handleCandidateAppRequest(new Request(
      'https://office.test/api/candidate-app/manager-reminder-eligibility?page=1&page_size=1&surname_query=ba&sort_by=CANDIDATE_SURNAME&sort_direction=DESC'
    ), {
      CANDIDATE_APP_ENVIRONMENT: 'TEST', SUPABASE_URL: 'https://test.supabase.invalid',
      SUPABASE_SERVICE_ROLE_KEY: 'placeholder'
    }, {}, deps);
    assert.equal(response.status, 200);
    const result = await response.json();
    assert.equal(result.catalogue_total_items, 3);
    assert.equal(result.total_items, 2);
    assert.equal(result.page_count, 2);
    assert.equal(result.surname_query, 'ba');
    assert.equal(result.sort_direction, 'DESC');
    assert.equal(result.items[0].candidate_surname, 'Barker');
    assert.deepEqual(result.matching_selection_keys, [approvals[2].id, approvals[0].id]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('office reminder catalogue limit counts eligible rows rather than all pending EMAIL requests', async () => {
  const actorId = '00000000-0000-4000-8000-000000000461';
  const uuid = (prefix, number) => `${prefix}-0000-4000-8000-${String(number).padStart(12, '0')}`;
  const approvals = Array.from({ length: 1001 }, (_, index) => ({
    id: uuid('10000000', index + 1),
    workflow_id: uuid('20000000', index + 1),
    workflow_generation: 1,
    request_generation: 1,
    method: 'EMAIL',
    state: 'PENDING'
  }));
  const workflows = approvals.map((approval, index) => ({
    id: approval.workflow_id,
    candidate_id: uuid('30000000', index + 1),
    generation: 1,
    contract_week_id: null,
    anchor_timesheet_id: uuid('40000000', index + 1),
    target_timesheet_id: uuid('40000000', index + 1),
    updated_at_utc: '2026-08-13T08:00:00.000Z'
  }));
  const workflowByTimesheet = new Map(workflows.map((workflow, index) => [
    workflow.target_timesheet_id,
    { workflow, approval: approvals[index], index }
  ]));
  let projectionCalls = 0;
  const deps = {
    routeAudience: 'OFFICE',
    async requireOfficeUser() { return { id: actorId }; },
    async rpc(name, args) {
      assert.equal(args.p_action, 'PROJECT_BATCH');
      projectionCalls += 1;
      return {
        ok: true,
        results: args.p_payload.identities.map(identity => {
          const current = workflowByTimesheet.get(identity.timesheet_id);
          const enabled = current.index === 0;
          return {
            ok: true,
            correlation_key: identity.row_key,
            projection: {
              current_identity: { row_key: identity.row_key, timesheet_id: identity.timesheet_id },
              manager_approval: {
                request_id: current.approval.id,
                request_generation: 1,
                provider_accepted_at_utc: '2026-08-13T07:00:00.000Z'
              },
              available_actions: [{
                code: 'SEND_MANAGER_REMINDER',
                enabled,
                invocation: { path: `/api/candidate-app/workflows/${current.workflow.id}/actions/remind` }
              }]
            }
          };
        })
      };
    }
  };
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async url => {
    const value = String(url);
    if (value.includes('/candidate_approval_requests?')) {
      const offset = Number(new URL(value).searchParams.get('offset') || 0);
      return Response.json(approvals.slice(offset, offset + 1000));
    }
    if (value.includes('/candidate_submission_workflows?')) return Response.json(workflows);
    if (value.includes('/candidates?')) return Response.json(workflows.map((workflow, index) => ({
      id: workflow.candidate_id,
      display_name: `Candidate ${index + 1}`,
      first_name: 'Candidate',
      last_name: String(index + 1)
    })));
    throw new Error(`unexpected URL: ${value}`);
  };
  try {
    const response = await handleCandidateAppRequest(new Request(
      'https://office.test/api/candidate-app/manager-reminder-eligibility?page=1&page_size=25'
    ), {
      CANDIDATE_APP_ENVIRONMENT: 'TEST',
      SUPABASE_URL: 'https://test.supabase.invalid',
      SUPABASE_SERVICE_ROLE_KEY: 'placeholder'
    }, {}, deps);
    assert.equal(response.status, 200);
    const result = await response.json();
    assert.equal(result.catalogue_total_items, 1);
    assert.equal(result.total_items, 1);
    assert.equal(projectionCalls, 11);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('office reminder catalogue fails closed before an unbounded pending-request scan', async () => {
  const actorId = '00000000-0000-4000-8000-000000000462';
  const page = Array.from({ length: 1000 }, (_, index) => ({
    id: `10000000-0000-4000-8000-${String(index + 1).padStart(12, '0')}`,
    workflow_id: `20000000-0000-4000-8000-${String(index + 1).padStart(12, '0')}`,
    workflow_generation: 1,
    request_generation: 1
  }));
  let sourceCalls = 0;
  let joinedRowsRequested = false;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async url => {
    const value = String(url);
    if (value.includes('/candidate_approval_requests?')) {
      sourceCalls += 1;
      return Response.json(page);
    }
    if (value.includes('/candidate_submission_workflows?') || value.includes('/candidates?')) {
      joinedRowsRequested = true;
    }
    throw new Error(`unexpected URL: ${value}`);
  };
  try {
    const response = await handleCandidateAppRequest(new Request(
      'https://office.test/api/candidate-app/manager-reminder-eligibility?page=1&page_size=25'
    ), {
      CANDIDATE_APP_ENVIRONMENT: 'TEST',
      SUPABASE_URL: 'https://test.supabase.invalid',
      SUPABASE_SERVICE_ROLE_KEY: 'placeholder'
    }, {}, {
      routeAudience: 'OFFICE',
      async requireOfficeUser() { return { id: actorId }; },
      async rpc() { throw new Error('projection must not start after the source ceiling is exceeded'); }
    });
    assert.equal(response.status, 409);
    assert.equal((await response.json()).error_code, 'CANDIDATE_REMINDER_CATALOGUE_TOO_LARGE');
    assert.equal(sourceCalls, 11);
    assert.equal(joinedRowsRequested, false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('office reminder all-eligible selection is resolved across the complete server catalogue', async () => {
  const actorId = '00000000-0000-4000-8000-000000000271';
  const fingerprint = 'b'.repeat(64);
  const approvals = [1, 2, 3].map(number => ({
    id: `00000000-0000-4000-8000-00000000027${number}`,
    workflow_id: `00000000-0000-4000-8000-00000000028${number}`,
    workflow_generation: 2, request_generation: 1, method: 'EMAIL', state: 'PENDING'
  }));
  const workflows = approvals.map((approval, index) => ({
    id: approval.workflow_id,
    candidate_id: `00000000-0000-4000-8000-00000000029${index + 1}`,
    generation: 2, contract_week_id: null,
    anchor_timesheet_id: `00000000-0000-4000-8000-00000000030${index + 1}`,
    target_timesheet_id: `00000000-0000-4000-8000-00000000030${index + 1}`,
    updated_at_utc: `2026-08-13T08:0${index}:00.000Z`
  }));
  const candidates = workflows.map((workflow, index) => ({
    id: workflow.candidate_id, display_name: `Candidate ${index + 1}`, first_name: null, last_name: null
  }));
  let previewIdentities = null;
  const deps = {
    routeAudience: 'OFFICE',
    async requireOfficeUser() { return { id: actorId }; },
    async rpc(name, args) {
      if (args.p_action === 'PROJECT_BATCH') {
        return {
          ok: true,
          results: workflows.map((workflow, index) => ({
            ok: true,
            correlation_key: workflow.target_timesheet_id,
            projection: {
              current_identity: { row_key: workflow.target_timesheet_id, timesheet_id: workflow.target_timesheet_id },
              manager_approval: {
                request_id: approvals[index].id, request_generation: 1,
                provider_accepted_at_utc: `2026-08-13T07:0${index}:00.000Z`
              },
              available_actions: [{
                code: 'SEND_MANAGER_REMINDER', enabled: true,
                invocation: { path: `/api/candidate-app/workflows/${workflow.id}/actions/remind` }
              }]
            }
          }))
        };
      }
      assert.equal(args.p_action, 'REMINDER_BATCH_PREVIEW');
      previewIdentities = args.p_payload.identities;
      return {
        ok: true, contract_version: 'OFFICE_CANDIDATE_REMINDER_BATCH_PREVIEW_V1',
        preview_context_hash: fingerprint, selection_fingerprint: fingerprint,
        selected_count: args.p_payload.identities.length, items: []
      };
    }
  };
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async url => {
    const value = String(url);
    if (value.includes('/candidate_approval_requests?')) return Response.json(approvals);
    if (value.includes('/candidate_submission_workflows?')) return Response.json(workflows);
    if (value.includes('/candidates?')) return Response.json(candidates);
    throw new Error(`unexpected URL: ${value}`);
  };
  try {
    const pageResponse = await handleCandidateAppRequest(new Request(
      'https://office.test/api/candidate-app/manager-reminder-eligibility?page=1&page_size=1'
    ), {
      CANDIDATE_APP_ENVIRONMENT: 'TEST', SUPABASE_URL: 'https://test.supabase.invalid',
      SUPABASE_SERVICE_ROLE_KEY: 'placeholder'
    }, {}, deps);
    const page = await pageResponse.json();
    assert.equal(page.page_count, 3);
    const excludedKey = approvals[1].id;
    const previewResponse = await handleCandidateAppRequest(new Request(
      'https://office.test/api/candidate-app/manager-reminder-batches/preview', {
        method: 'POST', headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          catalogue_revision: page.catalogue_revision,
          selection: { mode: 'ALL_ELIGIBLE', included_row_keys: [], excluded_row_keys: [excludedKey] }
        })
      }
    ), {
      CANDIDATE_APP_ENVIRONMENT: 'TEST', SUPABASE_URL: 'https://test.supabase.invalid',
      SUPABASE_SERVICE_ROLE_KEY: 'placeholder'
    }, {}, deps);
    assert.equal(previewResponse.status, 200);
    const preview = await previewResponse.json();
    assert.equal(preview.selected_count, 2);
    assert.equal(preview.selected_rows.length, 2);
    assert.equal(previewIdentities.length, 2);
    assert.equal(previewIdentities.some(item => item.timesheet_id === workflows[1].target_timesheet_id), false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('office reminder exact selection replay is returned before eligibility is recalculated', async () => {
  const actorId = '00000000-0000-4000-8000-000000000311';
  const batchId = '00000000-0000-4000-8000-000000000312';
  const timesheetId = '00000000-0000-4000-8000-000000000313';
  const requestId = '00000000-0000-4000-8000-000000000314';
  const hash = 'c'.repeat(64);
  const deps = {
    routeAudience: 'OFFICE',
    async requireOfficeUser() { return { id: actorId }; },
    async rpc(name, args) {
      assert.equal(args.p_action, 'REMINDER_BATCH_REPLAY');
      return {
        ok: true, found: true, idempotent_replay: true, batch_id: batchId,
        contract_version: 'OFFICE_CANDIDATE_REMINDER_BATCH_RESULT_V1',
        status: 'COMPLETED', success_count: 1, failure_count: 0, skipped_count: 0, items: []
      };
    }
  };
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => { throw new Error('an exact replay must not recalculate current reminder eligibility'); };
  try {
    const response = await handleCandidateAppRequest(new Request(
      'https://office.test/api/candidate-app/manager-reminder-batches', {
        method: 'POST', headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          catalogue_revision: hash,
          selection: { mode: 'EXPLICIT', included_row_keys: [requestId], excluded_row_keys: [] },
          selected_rows: [{ row_key: timesheetId, timesheet_id: timesheetId }],
          batch_id: batchId, idempotency_key: batchId,
          preview_context_hash: hash, selection_fingerprint: hash
        })
      }
    ), {
      CANDIDATE_APP_ENVIRONMENT: 'TEST', SUPABASE_URL: 'https://test.supabase.invalid',
      SUPABASE_SERVICE_ROLE_KEY: 'placeholder'
    }, {}, deps);
    assert.equal(response.status, 202);
    const result = await response.json();
    assert.equal(result.idempotent_replay, true);
    assert.equal(result.success_count, 1);
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

test('workflow creation policy exposes only app-safe approval choices', () => {
  const safe = safeCandidateWorkflowPolicy({
    client_id: '00000000-0000-4000-8000-000000000001',
    paper_submission_enabled: true,
    allow_daily_manager_authorise_on_phone: true,
    allow_daily_manager_authorise_by_email: false,
    manager_approval_policy: {
      mode: 'OVERRIDE',
      approved_emails: ['manager@example.test'],
      approved_domains: ['example.test'],
      allow_free_business_email: false,
      private_rule: 'must-not-leak'
    },
    policy_fingerprint: 'a'.repeat(64)
  });
  assert.deepEqual(safe, {
    paper_submission_enabled: true,
    allow_daily_manager_authorise_on_phone: true,
    allow_daily_manager_authorise_by_email: false,
    manager_approval_policy: {
      approved_emails: ['manager@example.test'],
      approved_domains: ['example.test'],
      allow_free_business_email: false
    }
  });
  assert.equal('client_id' in safe, false);
  assert.equal('mode' in safe.manager_approval_policy, false);
  assert.equal('private_rule' in safe.manager_approval_policy, false);
});

test('paper return pages expose the frozen order and QR requirement without source hashes', () => {
  const sourceComponentId = '00000000-0000-4000-8000-000000000011';
  const safe = safePaperReturnPages({ pages: [
    { page_key: 'HOURS_TIMESHEET', component_kind: 'HOURS_TIMESHEET' },
    {
      page_key: `MILEAGE_FORM:${sourceComponentId}`,
      component_kind: 'MILEAGE_FORM', expense_category: 'MILEAGE',
      source_component_id: sourceComponentId, source_content_sha256: 'a'.repeat(64)
    }
  ] });
  assert.deepEqual(safe, [{
    ordinal: 1, page_key: 'HOURS_TIMESHEET', component_kind: 'HOURS_TIMESHEET',
    expense_category: null, source_component_id: null, qr_required: true
  }, {
    ordinal: 2, page_key: `MILEAGE_FORM:${sourceComponentId}`,
    component_kind: 'MILEAGE_FORM', expense_category: 'MILEAGE',
    source_component_id: sourceComponentId, qr_required: false
  }]);
  assert.equal('source_content_sha256' in safe[1], false);
});

test('paper return V2 keeps frozen evidence digests private while assembly can match them internally', () => {
  const sourceComponentId = '00000000-0000-4000-8000-000000000012';
  const sourceHash = 'b'.repeat(64);
  const manifest = {
    manifest_version: 2,
    qr_contract_version: 'CANDIDATE_PAPER_PAGE_QR_V2',
    pages: [{
      ordinal: 1,
      page_key: `EXPENSE_EVIDENCE:${sourceComponentId}`,
      component_kind: 'EXPENSE_EVIDENCE',
      expense_category: 'ACCOMMODATION',
      source_component_id: sourceComponentId,
      source_content_sha256: sourceHash,
      display_name: 'Accommodation 1',
      category_occurrence: 1,
      page_kind_code: 'E',
      category_code: 'A',
      page_key_sha256_16: 'c'.repeat(16),
      qr_required: true
    }]
  };
  const publicPages = safePaperReturnPages(manifest);
  const internalPages = safePaperReturnPages(manifest, { includeSourceContentSha256: true });
  assert.equal('source_content_sha256' in publicPages[0], false);
  assert.equal(internalPages[0].source_content_sha256, sourceHash);
});

test('paper return page projection fails closed for malformed manifest members', () => {
  assert.throws(
    () => safePaperReturnPages({ pages: [{ page_key: 'HOURS_TIMESHEET' }] }),
    error => error?.code === 'CANDIDATE_PAPER_RETURN_MANIFEST_STALE'
  );
});

test('component upload tickets are encrypted and do not disclose the R2 key', async () => {
  const env = { CANDIDATE_PRIVATE_UPLOAD_TOKEN_SECRET: 'test-only-secret-material' };
  const storageKey = 'candidate-app/test/workflow/source/private-object.pdf';
  const ticket = await uploadTicket(env, {
    authority_kind: 'CANDIDATE_SESSION',
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

test('Candidate upload authority survives a short-lived federated session projection rotation', async () => {
  const accountId = '00000000-0000-4000-8000-000000000611';
  const candidateId = '00000000-0000-4000-8000-000000000612';
  const oldSessionId = '00000000-0000-4000-8000-000000000613';
  const currentSession = {
    id: '00000000-0000-4000-8000-000000000614',
    account_id: accountId,
    selected_candidate_id: candidateId,
    environment: 'TEST', status: 'ACTIVE', rotation: 0,
    expires_at_utc: '2099-01-01T00:00:00.000Z',
    absolute_expires_at_utc: '2099-01-01T00:00:00.000Z'
  };
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'test-only-session-secret-material',
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async url => {
    const target = new URL(String(url));
    if (target.pathname.endsWith('/candidate_app_sessions')) return Response.json([currentSession]);
    throw new Error(`Unexpected TEST request GET ${target.pathname}`);
  };
  try {
    const token = await createAccessToken(env, { ...currentSession, session_id: currentSession.id });
    const owner = await authenticateUploadOwner(new Request(
      'https://private.test/candidate-app/v1/uploads/encrypted', {
        method: 'PUT', headers: { authorization: `Bearer ${token}` }
      }
    ), env, {}, {
      owner: 'candidate', authority_kind: 'CANDIDATE_SESSION',
      owner_id: oldSessionId, candidate_account_id: accountId, candidate_id: candidateId
    });
    assert.equal(owner.session_id, currentSession.id);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('Candidate upload stable binding rejects another Candidate on the same account', async () => {
  const accountId = '00000000-0000-4000-8000-000000000621';
  const currentSession = {
    id: '00000000-0000-4000-8000-000000000622',
    account_id: accountId,
    selected_candidate_id: '00000000-0000-4000-8000-000000000623',
    environment: 'TEST', status: 'ACTIVE', rotation: 0,
    expires_at_utc: '2099-01-01T00:00:00.000Z',
    absolute_expires_at_utc: '2099-01-01T00:00:00.000Z'
  };
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'test-only-session-secret-material',
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async url => {
    const target = new URL(String(url));
    if (target.pathname.endsWith('/candidate_app_sessions')) return Response.json([currentSession]);
    throw new Error(`Unexpected TEST request GET ${target.pathname}`);
  };
  try {
    const token = await createAccessToken(env, { ...currentSession, session_id: currentSession.id });
    await assert.rejects(
      authenticateUploadOwner(new Request(
        'https://private.test/candidate-app/v1/uploads/encrypted', {
          method: 'PUT', headers: { authorization: `Bearer ${token}` }
        }
      ), env, {}, {
        owner: 'candidate', authority_kind: 'CANDIDATE_SESSION',
        owner_id: currentSession.id, candidate_account_id: accountId,
        candidate_id: '00000000-0000-4000-8000-000000000624'
      }),
      error => error?.code === 'CANDIDATE_UPLOAD_TICKET_INVALID'
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('Candidate component preparation reissues an upload ticket from the exact durable replay without another mutation', async () => {
  const session = {
    id: '00000000-0000-4000-8000-000000000401',
    session_id: '00000000-0000-4000-8000-000000000401',
    account_id: '00000000-0000-4000-8000-000000000402',
    selected_candidate_id: '00000000-0000-4000-8000-000000000403',
    environment: 'TEST', status: 'ACTIVE', rotation: 1,
    expires_at_utc: '2099-01-01T00:00:00.000Z',
    absolute_expires_at_utc: '2099-01-02T00:00:00.000Z'
  };
  const workflowId = '00000000-0000-4000-8000-000000000404';
  const componentId = '00000000-0000-4000-8000-000000000405';
  const idempotencyKey = '00000000-0000-4000-8000-000000000406';
  const workflow = {
    id: workflowId, account_id: session.account_id,
    candidate_id: session.selected_candidate_id,
    environment: 'TEST', generation: 2, state: 'WORKER_DRAFT'
  };
  const component = {
    id: componentId, workflow_id: workflowId, workflow_generation: 2,
    component_kind: 'CANDIDATE_SIGNATURE', document_role: 'CANDIDATE_SIGNATURE',
    expense_category: null, paper_return_page_key: null,
    storage_key: 'candidate-app/test/workflow/2/source/candidate-signature-original.png',
    media_type: 'image/png', byte_size: 321, state: 'PENDING',
    upload_idempotency_key: idempotencyKey, approval_request_id: null,
    manager_signature_capture_method: null, expected_source_content_sha256: null
  };
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'test-only-session-secret-material',
    CANDIDATE_PRIVATE_UPLOAD_TOKEN_SECRET: 'test-only-upload-secret-material',
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  const token = await createAccessToken(env, session);
  const originalFetch = globalThis.fetch;
  const reads = [];
  globalThis.fetch = async url => {
    const target = new URL(String(url));
    reads.push(target.pathname);
    if (target.pathname.endsWith('/candidate_app_sessions')) return Response.json([session]);
    if (target.pathname.endsWith('/candidate_submission_components')) return Response.json([component]);
    if (target.pathname.endsWith('/candidate_submission_workflows')) return Response.json([workflow]);
    throw new Error(`Unexpected TEST request GET ${target.pathname}`);
  };
  try {
    const response = await handleCandidateAppRequest(new Request(
      `https://private.test/candidate-app/v1/workflows/${workflowId}/components/prepare`, {
        method: 'POST',
        headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
        body: JSON.stringify({
          generation: 2, component_kind: 'CANDIDATE_SIGNATURE',
          document_role: 'CANDIDATE_SIGNATURE', media_type: 'image/png',
          byte_size: 321, idempotency_key: idempotencyKey
        })
      }
    ), env, {}, {
      routeAudience: 'PRIVATE',
      async rpc() { throw new Error('the durable replay must not invoke the mutation RPC'); }
    });
    assert.equal(response.status, 200);
    const body = await response.json();
    assert.equal(body.idempotent_replay, true);
    assert.equal(body.component_id, componentId);
    assert.equal(Object.hasOwn(body.upload, 'storage_key'), false);
    assert.match(body.upload.url, /^\/candidate-app\/v1\/uploads\//);
    assert.equal(reads.filter(path => path.endsWith('/candidate_submission_components')).length, 1);
    assert.equal(reads.filter(path => path.endsWith('/candidate_submission_workflows')).length, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('new Candidate component preparation uses the narrow authenticated fast-path RPC', async () => {
  const session = {
    id: '00000000-0000-4000-8000-000000000431',
    session_id: '00000000-0000-4000-8000-000000000431',
    account_id: '00000000-0000-4000-8000-000000000432',
    selected_candidate_id: '00000000-0000-4000-8000-000000000433',
    environment: 'TEST', status: 'ACTIVE', rotation: 1,
    expires_at_utc: '2099-01-01T00:00:00.000Z',
    absolute_expires_at_utc: '2099-01-02T00:00:00.000Z'
  };
  const workflowId = '00000000-0000-4000-8000-000000000434';
  const componentId = '00000000-0000-4000-8000-000000000435';
  const idempotencyKey = '00000000-0000-4000-8000-000000000436';
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'test-only-session-secret-material',
    CANDIDATE_PRIVATE_UPLOAD_TOKEN_SECRET: 'test-only-upload-secret-material',
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  const token = await createAccessToken(env, session);
  const calls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async url => {
    const target = new URL(String(url));
    if (target.pathname.endsWith('/candidate_app_sessions')) return Response.json([session]);
    if (target.pathname.endsWith('/candidate_submission_components')) return Response.json([]);
    throw new Error(`Unexpected TEST request GET ${target.pathname}`);
  };
  try {
    const response = await handleCandidateAppRequest(new Request(
      `https://private.test/candidate-app/v1/workflows/${workflowId}/components/prepare`, {
        method: 'POST',
        headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
        body: JSON.stringify({
          generation: 2, component_kind: 'CANDIDATE_SIGNATURE',
          document_role: 'CANDIDATE_SIGNATURE', media_type: 'image/png',
          byte_size: 321, idempotency_key: idempotencyKey
        })
      }
    ), env, {}, {
      routeAudience: 'PRIVATE',
      async rpc(name, args) {
        calls.push({ name, args });
        return {
          ok: true, component_id: componentId, workflow_generation: 2,
          storage_key: 'candidate-app/test/workflow/2/source/candidate-signature.png',
          media_type: 'image/png', byte_size: 321,
          component_kind: 'CANDIDATE_SIGNATURE', document_role: 'CANDIDATE_SIGNATURE',
          expense_category: null, paper_return_page_key: null, state: 'PENDING'
        };
      }
    });
    assert.equal(response.status, 201);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].name, 'candidate_component_prepare_atomic_v1');
    assert.equal(Object.hasOwn(calls[0].args, 'p_action'), false);
    assert.equal(calls[0].args.p_session_id, session.session_id);
    assert.equal(calls[0].args.p_expected_generation, 2);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('changed Candidate hours reuse unchanged immutable expense evidence without uploading its bytes again', async () => {
  const session = {
    id: '00000000-0000-4000-8000-000000000441',
    session_id: '00000000-0000-4000-8000-000000000441',
    account_id: '00000000-0000-4000-8000-000000000442',
    selected_candidate_id: '00000000-0000-4000-8000-000000000443',
    environment: 'TEST', status: 'ACTIVE', rotation: 1,
    expires_at_utc: '2099-01-01T00:00:00.000Z',
    absolute_expires_at_utc: '2099-01-02T00:00:00.000Z'
  };
  const workflowId = '00000000-0000-4000-8000-000000000444';
  const sourceWorkflowId = '00000000-0000-4000-8000-000000000445';
  const sourceComponentId = '00000000-0000-4000-8000-000000000446';
  const componentId = '00000000-0000-4000-8000-000000000447';
  const digest = '8'.repeat(64);
  const contractId = '00000000-0000-4000-8000-000000000448';
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'test-only-session-secret-material',
    CANDIDATE_PRIVATE_UPLOAD_TOKEN_SECRET: 'test-only-upload-secret-material',
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  const token = await createAccessToken(env, session);
  const calls = [];
  const reads = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async url => {
    const target = new URL(String(url));
    reads.push(`${target.pathname}?${target.searchParams}`);
    if (target.pathname.endsWith('/candidate_app_sessions')) return Response.json([session]);
    if (target.pathname.endsWith('/candidate_submission_workflows')) {
      const id = target.searchParams.get('id');
      if (id === `eq.${workflowId}`) return Response.json([{
        id: workflowId, environment: 'TEST', account_id: session.account_id,
        candidate_id: session.selected_candidate_id, contract_id: contractId,
        week_ending_date: '2026-07-26', generation: 1, state: 'WORKER_DRAFT'
      }]);
      if (id === `eq.${sourceWorkflowId}`) return Response.json([{
        id: sourceWorkflowId, environment: 'TEST', account_id: session.account_id,
        candidate_id: session.selected_candidate_id, contract_id: contractId,
        week_ending_date: '2026-07-26', state: 'SUPERSEDED'
      }]);
    }
    if (target.pathname.endsWith('/candidate_submission_components')) {
      if (target.searchParams.get('workflow_id') === `eq.${workflowId}`
          && target.searchParams.get('source_content_sha256')?.startsWith('eq.')) {
        return Response.json([]);
      }
      if (target.searchParams.get('source_content_sha256')?.startsWith('eq.')) return Response.json([{
        id: sourceComponentId, workflow_id: sourceWorkflowId, workflow_generation: 1,
        component_kind: 'EXPENSE_EVIDENCE', document_role: 'SOURCE_EVIDENCE',
        expense_category: 'ACCOMMODATION', media_type: 'image/jpeg', byte_size: 5,
        state: 'SUPERSEDED', immutable_at_utc: '2026-08-31T10:00:00.000Z',
        source_content_sha256: `\\x${digest}`
      }]);
      return Response.json([]);
    }
    throw new Error(`Unexpected TEST request GET ${target.pathname}${target.search}`);
  };
  try {
    const response = await handleCandidateAppRequest(new Request(
      `https://private.test/candidate-app/v1/workflows/${workflowId}/components/prepare`, {
        method: 'POST',
        headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
        body: JSON.stringify({
          generation: 1, component_kind: 'EXPENSE_EVIDENCE',
          document_role: 'SOURCE_EVIDENCE', expense_category: 'ACCOMMODATION',
          media_type: 'image/jpeg', byte_size: 5, source_content_sha256: digest,
          idempotency_key: '00000000-0000-4000-8000-000000000449'
        })
      }
    ), env, {}, {
      routeAudience: 'PRIVATE',
      async rpc(name, args) {
        calls.push({ name, args });
        return {
          ok: true, component_id: componentId, workflow_generation: 1,
          storage_key: 'candidate-app/test/old/source/expense.jpg',
          media_type: 'image/jpeg', byte_size: 5,
          component_kind: 'EXPENSE_EVIDENCE', document_role: 'SOURCE_EVIDENCE',
          expense_category: 'ACCOMMODATION', paper_return_page_key: null, state: 'IMMUTABLE'
        };
      }
    });
    const body = await response.json();
    assert.equal(response.status, 201, `${JSON.stringify(body)} calls=${calls.length} reads=${JSON.stringify(reads)}`);
    assert.equal(body.reused_existing_upload, true);
    assert.equal(Object.hasOwn(body, 'upload'), false);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].name, 'candidate_component_prepare_atomic_v1');
    assert.equal(calls[0].args.p_payload.source_component_id, sourceComponentId);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('interrupted duplicate expense prepare preserves completed components and repairs only its empty placeholder', async () => {
  const session = {
    id: '00000000-0000-4000-8000-000000000451',
    session_id: '00000000-0000-4000-8000-000000000451',
    account_id: '00000000-0000-4000-8000-000000000452',
    selected_candidate_id: '00000000-0000-4000-8000-000000000453',
    environment: 'TEST', status: 'ACTIVE', rotation: 1,
    expires_at_utc: '2099-01-01T00:00:00.000Z',
    absolute_expires_at_utc: '2099-01-02T00:00:00.000Z'
  };
  const workflowId = '00000000-0000-4000-8000-000000000454';
  const sourceWorkflowId = '00000000-0000-4000-8000-000000000455';
  const sourceComponentId = '00000000-0000-4000-8000-000000000456';
  const pendingComponentId = '00000000-0000-4000-8000-000000000457';
  const replacementComponentId = '00000000-0000-4000-8000-000000000458';
  const contractId = '00000000-0000-4000-8000-000000000459';
  const originalPrepareKey = '00000000-0000-4000-8000-000000000460';
  const digest = '9'.repeat(64);
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'test-only-session-secret-material',
    CANDIDATE_PRIVATE_UPLOAD_TOKEN_SECRET: 'test-only-upload-secret-material',
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  const token = await createAccessToken(env, session);
  const calls = [];
  let superseded = false;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async url => {
    const target = new URL(String(url));
    if (target.pathname.endsWith('/candidate_app_sessions')) return Response.json([session]);
    if (target.pathname.endsWith('/candidate_submission_workflows')) {
      const id = target.searchParams.get('id');
      if (id === `eq.${workflowId}`) return Response.json([{
        id: workflowId, environment: 'TEST', account_id: session.account_id,
        candidate_id: session.selected_candidate_id, contract_id: contractId,
        week_ending_date: '2026-07-26', generation: 1, state: 'WORKER_DRAFT'
      }]);
      if (id === `eq.${sourceWorkflowId}`) return Response.json([{
        id: sourceWorkflowId, environment: 'TEST', account_id: session.account_id,
        candidate_id: session.selected_candidate_id, contract_id: contractId,
        week_ending_date: '2026-07-26', state: 'SUPERSEDED'
      }]);
    }
    if (target.pathname.endsWith('/candidate_submission_components')) {
      if (target.searchParams.get('workflow_id') === `eq.${workflowId}`
          && target.searchParams.get('source_content_sha256')?.startsWith('eq.')) {
        return Response.json([]);
      }
      if (target.searchParams.get('source_content_sha256')?.startsWith('eq.')) return Response.json([{
        id: sourceComponentId, workflow_id: sourceWorkflowId, workflow_generation: 1,
        component_kind: 'EXPENSE_EVIDENCE', document_role: 'SOURCE_EVIDENCE',
        expense_category: 'ACCOMMODATION', media_type: 'image/jpeg', byte_size: 5,
        state: 'SUPERSEDED', immutable_at_utc: '2026-08-31T10:00:00.000Z',
        source_content_sha256: `\\x${digest}`
      }]);
      const prepareKey = target.searchParams.get('upload_idempotency_key');
      if (prepareKey === `eq.${originalPrepareKey}`) return Response.json([{
        id: pendingComponentId, workflow_id: workflowId, workflow_generation: 1,
        component_kind: 'EXPENSE_EVIDENCE', document_role: 'SOURCE_EVIDENCE',
        expense_category: 'ACCOMMODATION', paper_return_page_key: null,
        media_type: 'image/jpeg', byte_size: 5,
        state: superseded ? 'SUPERSEDED' : 'PENDING', approval_request_id: null,
        manager_signature_capture_method: null, expected_source_content_sha256: null,
        source_content_sha256: null, source_component_id: null
      }]);
      return Response.json([]);
    }
    throw new Error(`Unexpected TEST request GET ${target.pathname}${target.search}`);
  };
  try {
    const response = await handleCandidateAppRequest(new Request(
      `https://private.test/candidate-app/v1/workflows/${workflowId}/components/prepare`, {
        method: 'POST',
        headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
        body: JSON.stringify({
          generation: 1, component_kind: 'EXPENSE_EVIDENCE',
          document_role: 'SOURCE_EVIDENCE', expense_category: 'ACCOMMODATION',
          media_type: 'image/jpeg', byte_size: 5, source_content_sha256: digest,
          idempotency_key: originalPrepareKey
        })
      }
    ), env, {}, {
      routeAudience: 'PRIVATE',
      async rpc(name, args) {
        calls.push({ name, args });
        if (name === 'candidate_workflow_transition_atomic_v1') {
          assert.equal(args.p_action, 'COMPONENT_SUPERSEDE');
          assert.equal(args.p_payload.component_id, pendingComponentId);
          superseded = true;
          return { ok: true, component_id: pendingComponentId, state: 'SUPERSEDED' };
        }
        assert.equal(name, 'candidate_component_prepare_atomic_v1');
        return {
          ok: true, component_id: replacementComponentId, workflow_generation: 1,
          storage_key: 'candidate-app/test/old/source/expense.jpg',
          media_type: 'image/jpeg', byte_size: 5,
          component_kind: 'EXPENSE_EVIDENCE', document_role: 'SOURCE_EVIDENCE',
          expense_category: 'ACCOMMODATION', paper_return_page_key: null, state: 'IMMUTABLE'
        };
      }
    });
    const body = await response.json();
    assert.equal(response.status, 201, JSON.stringify(body));
    assert.equal(body.reused_existing_upload, true);
    assert.equal(Object.hasOwn(body, 'upload'), false);
    assert.equal(calls.length, 2);
    assert.equal(calls[1].args.p_payload.source_component_id, sourceComponentId);
    assert.match(calls[1].args.p_idempotency_key, /^lineage:[0-9a-f]{64}$/);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('expense prepare reuses the exact carried component in the current generation', async () => {
  const workflowId = '00000000-0000-4000-8000-000000000471';
  const sourceComponentId = '00000000-0000-4000-8000-000000000472';
  const carriedComponentId = '00000000-0000-4000-8000-000000000473';
  const digest = 'a'.repeat(64);
  const expected = {
    media_type: 'image/jpeg', byte_size: 5,
    component_kind: 'EXPENSE_EVIDENCE', document_role: 'SOURCE_EVIDENCE',
    expense_category: 'OTHER', paper_return_page_key: null,
    workflow_generation: 4, source_component_id: sourceComponentId,
    source_content_sha256: digest
  };
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async url => {
    const target = new URL(String(url));
    assert.equal(target.pathname.endsWith('/candidate_submission_components'), true);
    assert.equal(target.searchParams.get('workflow_id'), `eq.${workflowId}`);
    assert.equal(target.searchParams.get('workflow_generation'), 'eq.4');
    assert.equal(target.searchParams.get('source_content_sha256'), `eq.\\x${digest}`);
    return Response.json([{
      id: carriedComponentId, workflow_id: workflowId, workflow_generation: 4,
      component_kind: 'EXPENSE_EVIDENCE', document_role: 'SOURCE_EVIDENCE',
      expense_category: 'OTHER', paper_return_page_key: null,
      storage_key: 'candidate-app/test/carried/expense.jpg',
      media_type: 'image/jpeg', byte_size: 5, state: 'IMMUTABLE',
      source_component_id: sourceComponentId, source_content_sha256: `\\x${digest}`,
      approval_request_id: null, manager_signature_capture_method: null,
      expected_source_content_sha256: null
    }]);
  };
  try {
    const replay = await currentCandidateExpenseComponentReplay(
      { SUPABASE_URL: 'https://test.supabase.invalid', SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder' },
      workflowId, 4, expected, { id: sourceComponentId }
    );
    assert.equal(replay.component_id, carriedComponentId);
    assert.equal(replay.idempotent_replay, true);
    assert.equal(replay.storage_key, 'candidate-app/test/carried/expense.jpg');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('Candidate component fast-path refreshes the PostgREST schema cache after installation', async () => {
  const sql = await readFile(new URL(
    '../supabase/repeatable/27082026_0740_candidate_component_prepare_fast_path_v1.sql',
    import.meta.url
  ), 'utf8');
  assert.match(sql, /grant execute on function public\.candidate_component_prepare_atomic_v1[\s\S]*notify pgrst, 'reload schema';/i);
});

test('finalised Candidate detail reads the immutable signed artifact generation', async () => {
  const sql = await readFile(new URL(
    '../supabase/repeatable/27082026_0858_candidate_finalised_artifact_readiness_v1.sql',
    import.meta.url
  ), 'utf8');
  assert.match(sql, /create or replace function public\.candidate_app_timesheet_detail_v1/i);
  assert.match(sql, /state='FINALISED' then greatest\(w\.generation-1,1\) else w\.generation end/i);
  assert.match(sql, /state='FINALISED' then greatest\(document_workflow\.generation-1,1\) else document_workflow\.generation end/i);
  assert.doesNotMatch(sql, /review_component\.workflow_generation=w\.generation/i);
  assert.doesNotMatch(sql, /final_component\.workflow_generation=w\.generation/i);
  assert.doesNotMatch(sql, /component\.workflow_generation=document_workflow\.generation/i);
  assert.match(sql, /revoke all on function public\.candidate_app_timesheet_detail_v1[\s\S]*from public,anon,authenticated/i);
  assert.match(sql, /grant execute on function public\.candidate_app_timesheet_detail_v1[\s\S]*to service_role/i);
  assert.match(sql, /notify pgrst, 'reload schema';/i);
});

test('Candidate component preparation recovers one exact pending component after an older phone loses its prepare key', async () => {
  const session = {
    id: '00000000-0000-4000-8000-000000000421',
    session_id: '00000000-0000-4000-8000-000000000421',
    account_id: '00000000-0000-4000-8000-000000000422',
    selected_candidate_id: '00000000-0000-4000-8000-000000000423',
    environment: 'TEST', status: 'ACTIVE', rotation: 1,
    expires_at_utc: '2099-01-01T00:00:00.000Z',
    absolute_expires_at_utc: '2099-01-02T00:00:00.000Z'
  };
  const workflowId = '00000000-0000-4000-8000-000000000424';
  const componentId = '00000000-0000-4000-8000-000000000425';
  const requestKey = '00000000-0000-4000-8000-000000000426';
  const component = {
    id: componentId, workflow_id: workflowId, workflow_generation: 3,
    component_kind: 'CANDIDATE_SIGNATURE', document_role: 'CANDIDATE_SIGNATURE',
    expense_category: null, paper_return_page_key: null,
    storage_key: 'candidate-app/test/workflow/3/source/candidate-signature-pending.png',
    media_type: 'image/png', byte_size: 654, state: 'PENDING',
    upload_idempotency_key: '00000000-0000-4000-8000-000000000427',
    approval_request_id: null, manager_signature_capture_method: null,
    expected_source_content_sha256: null, source_content_sha256: null
  };
  const workflow = {
    id: workflowId, account_id: session.account_id,
    candidate_id: session.selected_candidate_id,
    environment: 'TEST', generation: 3, state: 'WORKER_DRAFT'
  };
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'test-only-session-secret-material',
    CANDIDATE_PRIVATE_UPLOAD_TOKEN_SECRET: 'test-only-upload-secret-material',
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  const token = await createAccessToken(env, session);
  const originalFetch = globalThis.fetch;
  let componentReads = 0;
  globalThis.fetch = async url => {
    const target = new URL(String(url));
    if (target.pathname.endsWith('/candidate_app_sessions')) return Response.json([session]);
    if (target.pathname.endsWith('/candidate_submission_components')) {
      componentReads += 1;
      return Response.json(target.searchParams.has('upload_idempotency_key') ? [] : [component]);
    }
    if (target.pathname.endsWith('/candidate_submission_workflows')) return Response.json([workflow]);
    throw new Error(`Unexpected TEST request GET ${target.pathname}`);
  };
  try {
    const response = await handleCandidateAppRequest(new Request(
      `https://private.test/candidate-app/v1/workflows/${workflowId}/components/prepare`, {
        method: 'POST',
        headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
        body: JSON.stringify({
          generation: 3, component_kind: 'CANDIDATE_SIGNATURE',
          document_role: 'CANDIDATE_SIGNATURE', media_type: 'image/png',
          byte_size: 654, idempotency_key: requestKey
        })
      }
    ), env, {}, {
      routeAudience: 'PRIVATE',
      async rpc() { throw new Error('pending prepare recovery must not invoke the mutation RPC'); }
    });
    assert.equal(response.status, 200);
    const body = await response.json();
    assert.equal(body.idempotent_replay, true);
    assert.equal(body.component_id, componentId);
    assert.equal(componentReads, 2);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('Candidate component replay remains bound to the signed-in Candidate and exact immutable upload contract', async () => {
  const workflowId = '00000000-0000-4000-8000-000000000411';
  const idempotencyKey = '00000000-0000-4000-8000-000000000412';
  const component = {
    id: '00000000-0000-4000-8000-000000000413', workflow_id: workflowId,
    workflow_generation: 1, component_kind: 'CANDIDATE_SIGNATURE',
    document_role: 'CANDIDATE_SIGNATURE', expense_category: null,
    paper_return_page_key: null, storage_key: 'candidate-app/test/original.png',
    media_type: 'image/png', byte_size: 100, state: 'PENDING',
    upload_idempotency_key: idempotencyKey, approval_request_id: null,
    manager_signature_capture_method: null, expected_source_content_sha256: null
  };
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async url => {
    const target = new URL(String(url));
    if (target.pathname.endsWith('/candidate_submission_components')) return Response.json([component]);
    if (target.pathname.endsWith('/candidate_submission_workflows')) return Response.json([{
      id: workflowId, account_id: '00000000-0000-4000-8000-000000000414',
      candidate_id: '00000000-0000-4000-8000-000000000415',
      environment: 'TEST', generation: 1, state: 'WORKER_DRAFT'
    }]);
    throw new Error(`Unexpected TEST request GET ${target.pathname}`);
  };
  try {
    await assert.rejects(preparedCandidateComponentReplay(
      { CANDIDATE_APP_ENVIRONMENT: 'TEST', SUPABASE_URL: 'https://test.supabase.invalid', SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder' },
      {
        account_id: '00000000-0000-4000-8000-000000000416',
        selected_candidate_id: '00000000-0000-4000-8000-000000000415'
      },
      workflowId, 1, idempotencyKey, {
        media_type: 'image/png', byte_size: 100, component_kind: 'CANDIDATE_SIGNATURE',
        document_role: 'CANDIDATE_SIGNATURE', expense_category: null,
        paper_return_page_key: null, workflow_generation: 1
      }
    ), error => error?.code === 'CANDIDATE_WORKFLOW_NOT_FOUND');
  } finally {
    globalThis.fetch = originalFetch;
  }
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

test('generic Mileage Claim Form is blank, reusable and agency branded', async () => {
  const bytes = await mileageClaimFormBytes({}, {
    agency_name: 'Configured Agency', logo: null,
    branding_contract_sha256: 'a'.repeat(64)
  });
  const pdf = await PDFDocument.load(bytes);
  assert.equal(pdf.getPageCount(), 1);
  const source = await readFile(new URL('../broker/src/candidate-app-backend.js', import.meta.url), 'utf8');
  for (const label of ['Mileage Claim Form', 'Candidate name', 'Week ending', 'Post Code from', 'Post Code To', 'Number of miles']) {
    assert.equal(source.includes(label), true);
  }
  const formBlock = source.slice(
    source.indexOf('async function mileageClaimFormBytes('),
    source.indexOf('async function paperExpensePageBytes(')
  );
  assert.match(formBlock, /Array\.from\(\{ length: 18 \}\)/);
  assert.doesNotMatch(formBlock, /drawCandidatePaperPageQr|candidatePaperPageQrText/);
  assert.doesNotMatch(formBlock, /page\.drawText\('Manager signature'|page\.drawText\('Date'/);
  assert.doesNotMatch(formBlock, /submittedMileageUnits|mileage_units/);
});

test('Candidate mileage form actions prepare the exact PDF and queue one registered-email attachment', async () => {
  const agencyLogoBytes = new Uint8Array(Buffer.from(
    'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Y9ZP8sAAAAASUVORK5CYII=',
    'base64'
  ));
  const agencyLogoDigest = createHash('sha256').update(agencyLogoBytes).digest('hex');
  const agencyLogoKey = `candidate-app/branding/${agencyLogoDigest}.png`;
  const session = {
    id: '00000000-0000-4000-8000-000000000091',
    session_id: '00000000-0000-4000-8000-000000000091',
    account_id: '00000000-0000-4000-8000-000000000092',
    selected_candidate_id: '00000000-0000-4000-8000-000000000093',
    environment: 'TEST', status: 'ACTIVE', rotation: 1,
    expires_at_utc: '2099-01-01T00:00:00.000Z',
    absolute_expires_at_utc: '2099-01-02T00:00:00.000Z'
  };
  const genericWorkflowId = '00000000-0000-0000-0000-000000000000';
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'test-only-secret-material',
    QR_SIGNING_SECRET: 'test-only-qr-signing-secret',
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder',
    R2: {
      async get(key) {
        assert.equal(key, agencyLogoKey);
        return {
          httpMetadata: { contentType: 'image/png' },
          async arrayBuffer() {
            return agencyLogoBytes.buffer.slice(
              agencyLogoBytes.byteOffset,
              agencyLogoBytes.byteOffset + agencyLogoBytes.byteLength
            );
          }
        };
      },
      async put() { return { etag: 'created' }; },
      async head() { return null; }
    }
  };
  const token = await createAccessToken(env, session);
  const originalFetch = globalThis.fetch;
  const outboxWrites = [];
  globalThis.fetch = async (url, init = {}) => {
    const target = new URL(String(url));
    const method = String(init.method || 'GET').toUpperCase();
    if (target.pathname.endsWith('/candidate_app_sessions')) return Response.json([session]);
    if (target.pathname.endsWith('/candidate_submission_workflows')
        || target.pathname.endsWith('/timesheets_financials')
        || target.pathname.endsWith('/timesheets')) {
      throw new Error('A generic Mileage Form must not read a Timesheet workflow');
    }
    if (target.pathname.endsWith('/settings_defaults')) {
      return Response.json([{
        agency_name: 'Configured Agency',
        agency_logo: null,
        candidate_app_logo_asset_key: agencyLogoKey
      }]);
    }
    if (target.pathname.endsWith('/candidate_app_accounts')) {
      return Response.json([{ id: session.account_id, email_normalized: 'candidate@example.test' }]);
    }
    if (target.pathname.endsWith('/candidates')) {
      throw new Error('Outbox display names are derived by the unified reader from recipient_id');
    }
    if (target.pathname.endsWith('/mail_outbox') && method === 'POST') {
      const body = JSON.parse(String(init.body));
      outboxWrites.push(body);
      return Response.json([{ id: '00000000-0000-4000-8000-000000000095', status: 'QUEUED' }]);
    }
    throw new Error(`Unexpected TEST request ${method} ${target.pathname}`);
  };
  const request = (action, key) => new Request(
    `https://private.test/candidate-app/v1/workflows/${genericWorkflowId}/actions/${action}`,
    {
      method: 'POST',
      headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
      body: JSON.stringify({ generation: 1, idempotency_key: key })
    }
  );
  try {
    const prepared = await handleCandidateAppRequest(
      request('mileage-form-prepare', '00000000-0000-4000-8000-000000000096'),
      env, {}, { routeAudience: 'PRIVATE' }
    );
    const preparedBody = await prepared.json();
    assert.equal(prepared.status, 200, JSON.stringify(preparedBody));
    assert.equal(preparedBody.mileage_form_state, 'PREPARED');
    assert.equal(preparedBody.mileage_form_scope, 'GENERIC');
    assert.equal(preparedBody.workflow_id, genericWorkflowId);
    assert.equal(preparedBody.state, 'GENERIC_DOCUMENT_READY');
    assert.match(preparedBody.mileage_form_sha256, /^[0-9a-f]{64}$/);
    assert.equal(preparedBody.mileage_form_filename, 'Mileage_Claim_Form.pdf');
    for (const forbidden of [
      'mileage_form_qr_proof_contract_version', 'mileage_form_timesheet_id',
      'mileage_form_mileage_units', 'mileage_form_semantic_sha256', 'mileage_form_qr_text'
    ]) assert.equal(Object.hasOwn(preparedBody, forbidden), false);
    const preparedPdf = await PDFDocument.load(Buffer.from(preparedBody.mileage_form_content_base64, 'base64'));
    assert.equal(preparedPdf.getPageCount(), 1);
    assert.equal(outboxWrites.length, 0);

    const emailed = await handleCandidateAppRequest(
      request('mileage-form-email', '00000000-0000-4000-8000-000000000097'),
      env, {}, { routeAudience: 'PRIVATE' }
    );
    assert.equal(emailed.status, 202);
    const emailedBody = await emailed.json();
    assert.equal(emailedBody.mileage_form_state, 'EMAIL_QUEUED');
    assert.equal(emailedBody.mileage_form_scope, 'GENERIC');
    assert.equal(outboxWrites.length, 1);
    assert.equal(outboxWrites[0].to, 'candidate@example.test');
    assert.equal(outboxWrites[0].recipient_id, session.selected_candidate_id);
    assert.equal(outboxWrites[0].context_kind, 'CANDIDATE_ACCOUNT');
    assert.equal(outboxWrites[0].context_id, session.account_id);
    assert.equal(Object.hasOwn(outboxWrites[0], 'recipient_display_name'), false);
    assert.equal(outboxWrites[0].attachments.length, 1);
    assert.equal(outboxWrites[0].attachments[0].content_type, 'application/pdf');
    assert.match(
      outboxWrites[0].attachments[0].r2_key,
      /\/agency\/[0-9a-f]{64}\/generic\/mileage-claim-form-v1\.pdf$/
    );
    assert.equal(outboxWrites[0].payment_scope_json.candidate_mail_authority, 'CANDIDATE_GENERIC_DOCUMENT_V1');
    assert.equal(Object.hasOwn(outboxWrites[0].payment_scope_json, 'candidate_workflow_id'), false);

    const declaredMileage = await handleCandidateAppRequest(new Request(
      `https://private.test/candidate-app/v1/workflows/${genericWorkflowId}/actions/mileage-form-prepare`, {
        method: 'POST',
        headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
        body: JSON.stringify({
          generation: 1,
          idempotency_key: '00000000-0000-4000-8000-000000000099',
          mileage_units: 100
        })
      }
    ), env, {}, { routeAudience: 'PRIVATE' });
    assert.equal(declaredMileage.status, 400);
    assert.equal((await declaredMileage.json()).error_code, 'CANDIDATE_GENERIC_MILEAGE_FORM_MUST_NOT_DECLARE_MILEAGE');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('final mileage submission requires the declared total and every current JPEG evidence object', async () => {
  const workflow = {
    id: '00000000-0000-4000-8000-0000000000a1', generation: 4,
    target_timesheet_id: '00000000-0000-4000-8000-0000000000a2',
    week_ending_date: '2026-08-30'
  };
  const components = [1, 2].map((number) => ({
    id: `00000000-0000-4000-8000-0000000000a${number + 2}`,
    storage_key: `candidate-app/test/${workflow.id}/4/mileage-${number}.jpg`,
    media_type: 'image/jpeg',
    byte_size: 100 + number,
    source_content_sha256: String(number).repeat(64)
  }));
  const env = {
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder',
    R2: {
      async head(key) {
        const component = components.find((item) => item.storage_key === key);
        return component ? {
          size: component.byte_size,
          httpMetadata: { contentType: 'image/jpeg' },
          customMetadata: { sha256: component.source_content_sha256 }
        } : null;
      }
    }
  };
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async url => {
    const target = new URL(String(url));
    if (target.pathname.endsWith('/candidate_submission_components')) return Response.json(components);
    throw new Error(`Unexpected TEST request GET ${target.pathname}`);
  };
  try {
    await assertCandidateMileageEvidenceMatchesSubmission(env, workflow, {
      expense_claim: { mileage_units: 35, mileage_total_confirmed: true }
    });
    await assert.rejects(
      assertCandidateMileageEvidenceMatchesSubmission(env, workflow, {
        expense_claim: { mileage_units: 35, mileage_total_confirmed: false }
      }),
      error => error?.code === 'CANDIDATE_MILEAGE_TOTAL_CONFIRMATION_REQUIRED'
    );
    env.R2.head = async (key) => {
      const component = components.find((item) => item.storage_key === key);
      return {
        size: component.byte_size,
        httpMetadata: { contentType: 'image/jpeg' },
        customMetadata: { sha256: 'f'.repeat(64) }
      };
    };
    await assert.rejects(
      assertCandidateMileageEvidenceMatchesSubmission(env, workflow, {
        expense_claim: { mileage_units: 35, mileage_total_confirmed: true }
      }),
      error => error?.code === 'CANDIDATE_MILEAGE_FORM_EVIDENCE_INVALID'
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('Candidate paper email queues the exact ready pack to the active registered account', async () => {
  const workflow = {
    id: '00000000-0000-4000-8000-0000000000b1',
    account_id: '00000000-0000-4000-8000-0000000000b2',
    candidate_id: '00000000-0000-4000-8000-0000000000b3',
    target_timesheet_id: '00000000-0000-4000-8000-0000000000b4',
    environment: 'TEST', generation: 2, state: 'AWAITING_PAPER_RETURN',
    workflow_kind: 'CONTRACT_COMBINED', scope: 'WEEKLY', route: 'PAPER',
    week_ending_date: '2026-08-30', contract_id: null,
    paper_return_manifest_sha256: 'a'.repeat(64)
  };
  const access = { account_id: workflow.account_id, selected_candidate_id: workflow.candidate_id };
  const context = {
    ready: true, workflow,
    complete: {
      ready: true, key: 'candidate-app/test/ready-pack.pdf', sha256: 'b'.repeat(64),
      byte_size: 123456, page_count: 4
    }
  };
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  const writes = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    const target = new URL(String(url));
    const method = String(init.method || 'GET').toUpperCase();
    if (target.pathname.endsWith('/candidate_app_accounts')) {
      return Response.json([{ id: workflow.account_id, email_normalized: 'candidate@example.test' }]);
    }
    if (target.pathname.endsWith('/timesheets')) {
      return Response.json([{
        timesheet_id: workflow.target_timesheet_id, sheet_scope: 'WEEKLY',
        hospital_norm: 'Test Hospital', ward_norm: 'Test Ward', job_title_norm: 'Nurse', band: '6'
      }]);
    }
    if (target.pathname.endsWith('/timesheets_financials')) return Response.json([]);
    if (target.pathname.endsWith('/candidates')) {
      return Response.json([{ id: workflow.candidate_id, first_name: 'Test', last_name: 'Worker' }]);
    }
    if (target.pathname.endsWith('/settings_defaults')) {
      return Response.json([{ agency_name: 'Configured Agency', agency_logo: null }]);
    }
    if (target.pathname.endsWith('/mail_outbox') && method === 'POST') {
      const body = JSON.parse(String(init.body));
      writes.push(body);
      return Response.json([{
        ...body,
        id: '00000000-0000-4000-8000-0000000000b5',
        status: 'QUEUED'
      }]);
    }
    throw new Error(`Unexpected TEST request ${method} ${target.pathname}`);
  };
  try {
    const result = await queueCandidatePaperPackEmail(env, workflow, access, {
      generation: 2, idempotency_key: '00000000-0000-4000-8000-0000000000b6'
    }, context);
    assert.equal(result.paper_email_state, 'EMAIL_QUEUED');
    assert.equal(result.idempotent_replay, false);
    assert.equal(writes.length, 1);
    assert.equal(writes[0].to, 'candidate@example.test');
    assert.equal(writes[0].recipient_id, workflow.candidate_id);
    assert.equal(Object.hasOwn(writes[0], 'recipient_display_name'), false);
    assert.equal(writes[0].attachments.length, 1);
    assert.deepEqual(writes[0].attachments[0], {
      r2_key: context.complete.key,
      filename: 'Official_Documents_2026-08-30.pdf',
      content_type: 'application/pdf',
      sha256: context.complete.sha256,
      size_bytes: 123456,
      page_count: 4,
      candidate_workflow_id: workflow.id,
      candidate_workflow_generation: 2,
      paper_return_manifest_sha256: workflow.paper_return_manifest_sha256
    });
    assert.equal(writes[0].payment_scope_json.candidate_mail_authority,
      'CANDIDATE_PAPER_PACK_EMAIL_V1');
    assert.equal(writes[0].payment_scope_json.candidate_paper_pack_ready, true);
    assert.equal(writes[0].payment_scope_json.mail_held_until_pdf_rendered, false);
    assert.equal(writes[0].payment_scope_json.mail_hold_reason, null);
    assert.equal(writes[0].payment_scope_json.candidate_complete_pack_media_type,
      'application/pdf');
    assert.equal(writes[0].context_kind, 'timesheets');
    assert.equal(writes[0].context_id, workflow.target_timesheet_id);
    assert.match(writes[0].deterministic_outbox_key,
      /CANDIDATE_PAPER_PACK_EMAIL:.*00000000-0000-4000-8000-0000000000b6$/);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('Candidate paper email replays only an already claimable completed-pack row', async () => {
  const workflow = {
    id: '00000000-0000-4000-8000-0000000000c1',
    account_id: '00000000-0000-4000-8000-0000000000c2',
    candidate_id: '00000000-0000-4000-8000-0000000000c3',
    target_timesheet_id: '00000000-0000-4000-8000-0000000000c4',
    environment: 'TEST', generation: 3, state: 'AWAITING_PAPER_RETURN',
    workflow_kind: 'CONTRACT_COMBINED', scope: 'WEEKLY', route: 'PAPER',
    week_ending_date: '2026-08-30', contract_id: null,
    paper_return_manifest_sha256: 'c'.repeat(64)
  };
  const access = { account_id: workflow.account_id, selected_candidate_id: workflow.candidate_id };
  const context = {
    ready: true, workflow,
    complete: {
      ready: true, key: 'candidate-app/test/replay-pack.pdf', sha256: 'd'.repeat(64),
      byte_size: 654321, page_count: 3
    }
  };
  const idempotencyKey = '00000000-0000-4000-8000-0000000000c6';
  const attachment = {
    r2_key: context.complete.key,
    filename: 'Official_Documents_2026-08-30.pdf',
    content_type: 'application/pdf',
    sha256: context.complete.sha256,
    size_bytes: context.complete.byte_size,
    page_count: context.complete.page_count,
    candidate_workflow_id: workflow.id,
    candidate_workflow_generation: workflow.generation,
    paper_return_manifest_sha256: workflow.paper_return_manifest_sha256
  };
  const durable = {
    id: '00000000-0000-4000-8000-0000000000c5', status: 'QUEUED',
    context_kind: 'timesheets', context_id: workflow.target_timesheet_id,
    deterministic_outbox_key: `CANDIDATE_PAPER_PACK_EMAIL:${workflow.id}:${workflow.generation}:${idempotencyKey}`,
    attachments: [attachment],
    payment_scope_json: {
      candidate_mail_authority: 'CANDIDATE_PAPER_PACK_EMAIL_V1',
      candidate_workflow_id: workflow.id,
      candidate_workflow_generation: workflow.generation,
      paper_return_manifest_sha256: workflow.paper_return_manifest_sha256,
      candidate_paper_pack_ready: true,
      mail_held_until_pdf_rendered: false,
      mail_hold_reason: null,
      candidate_complete_pack_storage_key: context.complete.key,
      candidate_complete_pack_sha256: context.complete.sha256,
      candidate_complete_pack_size_bytes: context.complete.byte_size,
      candidate_complete_pack_page_count: context.complete.page_count,
      candidate_complete_pack_media_type: 'application/pdf'
    }
  };
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  let returnBrokenExisting = false;
  let durableSelect = '';
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    const target = new URL(String(url));
    const method = String(init.method || 'GET').toUpperCase();
    if (target.pathname.endsWith('/candidate_app_accounts')) {
      return Response.json([{ id: workflow.account_id, email_normalized: 'candidate@example.test' }]);
    }
    if (target.pathname.endsWith('/timesheets')) {
      return Response.json([{
        timesheet_id: workflow.target_timesheet_id, sheet_scope: 'WEEKLY',
        hospital_norm: 'Test Hospital', ward_norm: 'Test Ward', job_title_norm: 'Nurse', band: '6'
      }]);
    }
    if (target.pathname.endsWith('/timesheets_financials')) return Response.json([]);
    if (target.pathname.endsWith('/candidates')) {
      return Response.json([{ id: workflow.candidate_id, first_name: 'Test', last_name: 'Worker' }]);
    }
    if (target.pathname.endsWith('/settings_defaults')) {
      return Response.json([{ agency_name: 'Configured Agency', agency_logo: null }]);
    }
    if (target.pathname.endsWith('/mail_outbox') && method === 'POST') return Response.json([]);
    if (target.pathname.endsWith('/mail_outbox') && method === 'GET') {
      durableSelect = target.searchParams.get('select') || '';
      return Response.json([returnBrokenExisting ? {
        ...durable,
        payment_scope_json: {
          ...durable.payment_scope_json,
          candidate_paper_pack_ready: false,
          mail_held_until_pdf_rendered: true
        }
      } : durable]);
    }
    throw new Error(`Unexpected TEST request ${method} ${target.pathname}`);
  };
  try {
    const replay = await queueCandidatePaperPackEmail(env, workflow, access, {
      generation: workflow.generation, idempotency_key: idempotencyKey
    }, context);
    assert.equal(replay.idempotent_replay, true);
    assert.equal(replay.mail_outbox_id, durable.id);
    assert.match(durableSelect, /(?:^|,)context_kind(?:,|$)/);
    assert.match(durableSelect, /(?:^|,)context_id(?:,|$)/);

    durable.payment_scope_json.candidate_mail_authority = 'CANDIDATE_PAPER_V1';
    await assert.rejects(
      () => queueCandidatePaperPackEmail(env, workflow, access, {
        generation: workflow.generation, idempotency_key: idempotencyKey
      }, context),
      error => error?.code === 'CANDIDATE_PAPER_EMAIL_OUTBOX_CONFLICT'
    );
    durable.payment_scope_json.candidate_mail_authority = 'CANDIDATE_PAPER_PACK_EMAIL_V1';

    returnBrokenExisting = true;
    await assert.rejects(
      () => queueCandidatePaperPackEmail(env, workflow, access, {
        generation: workflow.generation, idempotency_key: idempotencyKey
      }, context),
      error => error?.code === 'CANDIDATE_PAPER_EMAIL_OUTBOX_CONFLICT'
    );

    returnBrokenExisting = false;
    durable.context_kind = 'CANDIDATE_WORKFLOW';
    durable.context_id = workflow.id;
    await assert.rejects(
      () => queueCandidatePaperPackEmail(env, workflow, access, {
        generation: workflow.generation, idempotency_key: idempotencyKey
      }, context),
      error => error?.code === 'CANDIDATE_PAPER_EMAIL_OUTBOX_CONFLICT'
    );
  } finally {
    globalThis.fetch = originalFetch;
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
  const firstMileage = await mileageClaimFormBytes(env, { ...branding, logo: null });
  const firstExpense = (await renderExpensePage(env, { review_ordinal: 1, render_input: {} }, { workflow, component }, 'REVIEW')).pdf_bytes;
  await new Promise(resolve => setTimeout(resolve, 1100));
  const secondMileage = await mileageClaimFormBytes(env, { ...branding, logo: null });
  const secondExpense = (await renderExpensePage(env, { review_ordinal: 1, render_input: {} }, { workflow, component }, 'REVIEW')).pdf_bytes;
  const digest = bytes => createHash('sha256').update(bytes).digest('hex');
  assert.equal(digest(firstMileage), digest(secondMileage));
  assert.equal(digest(firstExpense), digest(secondExpense));
});

test('printed Mileage pages use the frozen canonical claim total', () => {
  assert.equal(submittedMileageUnits({
    expense_submission: {
      mileage_units: 0,
      canonical_tsfin_snapshot: {
        mileage_units: 25,
        expenses_pay_ex_vat: 0
      }
    }
  }), 25);
  assert.equal(submittedMileageUnits({
    expense_claim: {
      mileage_units: 12.5,
      mileage_total_confirmed: true
    }
  }), 12.5);
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

test('Candidate app branding uses its dedicated logo pointer and leaves document branding independent', async () => {
  const originalFetch = globalThis.fetch;
  const logoBytes = new Uint8Array([137, 80, 78, 71, 13, 10, 26, 10, 1, 2, 3, 4]);
  const logoDigest = createHash('sha256').update(logoBytes).digest('hex');
  const logoKey = `candidate-app/branding/${logoDigest}.png`;
  let settingsQuery = '';
  const env = {
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder',
    R2: {
      async get(key) {
        assert.equal(key, logoKey);
        return {
          httpMetadata: { contentType: 'image/png' },
          async arrayBuffer() { return logoBytes.buffer.slice(0); }
        };
      }
    }
  };
  globalThis.fetch = async url => {
    settingsQuery = String(url);
    return Response.json([{
      agency_name: 'Configured Agency',
      agency_logo: 'Assets/LEGACY-DOCUMENT-LOGO.png',
      candidate_app_logo_asset_key: logoKey
    }]);
  };
  try {
    const branding = await candidateAppAgencyBranding(env);
    assert.equal(branding.agency_name, 'Configured Agency');
    assert.equal(branding.logo_media_type, 'image/png');
    assert.equal(branding.logo_sha256, logoDigest);
    assert.equal(branding.logo_data_url.startsWith('data:image/png;base64,'), true);
    assert.match(settingsQuery, /select=agency_name,candidate_app_logo_asset_key/);
    assert.equal(settingsQuery.includes('agency_logo'), false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('Candidate mileage documents use the agency app logo rather than CloudTMS document branding', async () => {
  const originalFetch = globalThis.fetch;
  const logoBytes = new Uint8Array([137, 80, 78, 71, 13, 10, 26, 10, 5, 4, 3, 2]);
  const logoDigest = createHash('sha256').update(logoBytes).digest('hex');
  const logoKey = `candidate-app/branding/${logoDigest}.png`;
  let settingsQuery = '';
  const env = {
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder',
    R2: {
      async get(key) {
        assert.equal(key, logoKey);
        return {
          httpMetadata: { contentType: 'image/png' },
          async arrayBuffer() { return logoBytes.buffer.slice(0); }
        };
      }
    }
  };
  globalThis.fetch = async url => {
    settingsQuery = String(url);
    return Response.json([{
      agency_name: 'Arthur Rai Medical Services',
      agency_logo: 'Assets/CLOUDTMS-DOCUMENT-LOGO.png',
      candidate_app_logo_asset_key: logoKey
    }]);
  };
  try {
    const branding = await candidateAppAgencyDocumentBranding(env);
    assert.equal(branding.agency_name, 'Arthur Rai Medical Services');
    assert.equal(branding.logo_key, logoKey);
    assert.equal(branding.logo_sha256, logoDigest);
    assert.equal(branding.logo_media_type, 'image/png');
    assert.deepEqual(branding.logo.bytes, logoBytes);
    assert.match(branding.branding_contract_sha256, /^[0-9a-f]{64}$/);
    assert.match(settingsQuery, /select=agency_name,candidate_app_logo_asset_key/);
    assert.equal(settingsQuery.includes('agency_logo'), false);
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

test('Paper Timesheet page renders unsigned from the exact submitted workflow without electronic hours or signature components', async () => {
  const timesheetId = '00000000-0000-4000-8000-000000000527';
  const workflow = {
    id: '00000000-0000-4000-8000-000000000526',
    generation: 2,
    scope: 'WEEKLY',
    route: 'PAPER',
    candidate_id: '00000000-0000-4000-8000-000000000528',
    target_timesheet_id: timesheetId,
    anchor_timesheet_id: timesheetId,
    contract_id: null,
    week_ending_date: '2026-07-26',
    candidate_signature_component_id: null,
    candidate_signed_at_utc: null,
    immutable_submission_json: {
      official_presentation: {
        branding: noLogoBranding(),
        worker: { first_name: 'Test', surname: 'Worker', job_profile_title: 'Nurse' },
        client: { name: 'Test Client', hospital: 'Test Hospital', site_ward: 'Ward A' }
      },
      hours_submission: {
        actual_schedule_json: [{
          date: '2026-07-20', start: '17:00', end: '18:00',
          break_minutes: 0, no_break_declared: true
        }]
      }
    }
  };
  const timesheet = {
    timesheet_id: timesheetId,
    version: 1,
    sheet_scope: 'WEEKLY'
  };
  const env = {
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder',
    R2: { async get() { throw new Error('unsigned PAPER rendering must not read a signature asset'); } }
  };
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async url => {
    const path = new URL(String(url)).pathname;
    if (path.endsWith('/timesheets_financials')) return Response.json([]);
    if (path.endsWith('/candidates')) {
      return Response.json([{ id: workflow.candidate_id, first_name: 'Test', last_name: 'Worker' }]);
    }
    if (path.endsWith('/candidate_submission_components')) {
      throw new Error('unsigned PAPER rendering must not query a signature component');
    }
    return Response.json([]);
  };
  try {
    const bytes = await candidatePaperTimesheetPageBytes(
      env, workflow, timesheet, [], 'TSQ2.diagnostic-paper-timesheet'
    );
    const rendered = await PDFDocument.load(bytes);
    assert.equal(rendered.getPageCount(), 1);
    assert.ok(bytes.byteLength > 1_000);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('paper pack component lookup accepts the exact current-generation clone under its frozen page identity', () => {
  const originalId = '00000000-0000-4000-8000-000000000501';
  const sourceHash = 'a'.repeat(64);
  const clone = {
    id: '00000000-0000-4000-8000-000000000502',
    source_component_id: originalId,
    workflow_id: '00000000-0000-4000-8000-000000000503',
    workflow_generation: 4,
    state: 'IMMUTABLE',
    component_kind: 'EXPENSE_EVIDENCE',
    source_content_sha256: `\\x${sourceHash}`
  };
  assert.equal(candidatePaperPackComponentForPage([clone], {
    component_kind: 'EXPENSE_EVIDENCE', source_component_id: originalId,
    source_content_sha256: sourceHash
  }), clone);
});

test('paper evidence carry and manifest canonicalise one page per durable source identity', async () => {
  const sql = await readFile(new URL(
    '../supabase/repeatable/30082026_1903_candidate_expense_carrier_anchor_route_v1.sql',
    import.meta.url
  ), 'utf8');
  const canonicalSelectors = sql.match(/select distinct on \([\s\S]{0,360}?coalesce\([\s\S]{0,120}?source_component_id[\s\S]{0,120}?[.]id\)[\s\S]{0,180}?source_content_sha256/gi) || [];
  assert.equal(canonicalSelectors.length >= 4, true, `canonical selectors=${canonicalSelectors.length}`);
  assert.match(sql, /elsif v_action='PAPER_PREPARE'[\s\S]*?from \(\s*select distinct on \([\s\S]*?\) source_component;/i);
});

test('paper pack component lookup canonicalises equivalent carried rows for one frozen page', () => {
  const sourceId = '00000000-0000-4000-8000-000000000511';
  const sourceHash = 'b'.repeat(64);
  const common = {
    source_component_id: sourceId,
    component_kind: 'EXPENSE_EVIDENCE',
    expense_category: 'OTHER',
    document_role: 'EXPENSE_EVIDENCE',
    storage_key: 'candidate/evidence/one.png',
    media_type: 'image/png',
    byte_size: 123,
    source_content_sha256: sourceHash
  };
  const chosen = candidatePaperPackComponentForPage([
    { ...common, id: '00000000-0000-4000-8000-000000000513', component_no: 9 },
    { ...common, id: '00000000-0000-4000-8000-000000000512', component_no: 4 }
  ], {
    component_kind: 'EXPENSE_EVIDENCE', expense_category: 'OTHER',
    source_component_id: sourceId, source_content_sha256: sourceHash
  });
  assert.equal(chosen.id, '00000000-0000-4000-8000-000000000512');
});

test('paper pack component lookup fails closed when duplicate rows disagree on page identity', () => {
  const sourceId = '00000000-0000-4000-8000-000000000521';
  const sourceHash = 'e'.repeat(64);
  assert.throws(
    () => candidatePaperPackComponentForPage([
      {
        id: '00000000-0000-4000-8000-000000000522', source_component_id: sourceId,
        component_kind: 'EXPENSE_EVIDENCE', expense_category: 'OTHER',
        document_role: 'EXPENSE_EVIDENCE', storage_key: 'candidate/evidence/one.png',
        media_type: 'image/png', byte_size: 123, source_content_sha256: sourceHash
      },
      {
        id: '00000000-0000-4000-8000-000000000523', source_component_id: sourceId,
        component_kind: 'EXPENSE_EVIDENCE', expense_category: 'OTHER',
        document_role: 'EXPENSE_EVIDENCE', storage_key: 'candidate/evidence/two.png',
        media_type: 'image/png', byte_size: 123, source_content_sha256: sourceHash
      }
    ], {
      component_kind: 'EXPENSE_EVIDENCE', expense_category: 'OTHER', source_component_id: sourceId,
      source_content_sha256: sourceHash
    }),
    error => error?.code === 'CANDIDATE_PAPER_PACK_COMPONENT_CONFLICT'
  );
});

test('paper pack component lookup ignores same-id rows outside the exact frozen page identity', () => {
  const sourceId = '00000000-0000-4000-8000-000000000514';
  const sourceHash = 'c'.repeat(64);
  const evidence = {
    id: '00000000-0000-4000-8000-000000000515', source_component_id: sourceId,
    component_kind: 'EXPENSE_EVIDENCE', source_content_sha256: sourceHash
  };
  const mileage = {
    id: '00000000-0000-4000-8000-000000000516', source_component_id: sourceId,
    component_kind: 'MILEAGE_FORM', source_content_sha256: sourceHash
  };
  const unrelatedSameKind = {
    id: '00000000-0000-4000-8000-000000000517', source_component_id: sourceId,
    component_kind: 'EXPENSE_EVIDENCE', source_content_sha256: 'd'.repeat(64)
  };
  assert.equal(candidatePaperPackComponentForPage(
    [evidence, mileage, unrelatedSameKind],
    {
      component_kind: 'EXPENSE_EVIDENCE', source_component_id: sourceId,
      source_content_sha256: sourceHash
    }
  ), evidence);
});

test('complete paper pack assembles cloned expense evidence referenced by its durable source identity', async () => {
  const branding = noLogoBranding();
  const basePdf = await PDFDocument.create({ updateMetadata: false });
  basePdf.addPage([200, 200]);
  const baseBytes = new Uint8Array(await basePdf.save());
  const evidencePdf = await PDFDocument.create({ updateMetadata: false });
  evidencePdf.addPage([180, 120]).drawText('Expense evidence', { x: 12, y: 60, size: 10 });
  const evidenceBytes = new Uint8Array(await evidencePdf.save());
  const baseHash = createHash('sha256').update(baseBytes).digest('hex');
  const evidenceHash = createHash('sha256').update(evidenceBytes).digest('hex');
  const originalId = '00000000-0000-4000-8000-000000000521';
  const clone = {
    id: '00000000-0000-4000-8000-000000000522',
    source_component_id: originalId,
    workflow_id: '00000000-0000-4000-8000-000000000523',
    workflow_generation: 2,
    state: 'IMMUTABLE',
    component_kind: 'EXPENSE_EVIDENCE',
    document_role: 'SOURCE_EVIDENCE',
    expense_category: 'ACCOMMODATION',
    review_ordinal: 2,
    storage_key: 'evidence.pdf',
    media_type: 'application/pdf',
    source_content_sha256: evidenceHash
  };
  const unrelatedKindWithSameDurableIdentity = {
    ...clone,
    id: '00000000-0000-4000-8000-000000000525',
    component_kind: 'MILEAGE_FORM',
    expense_category: 'MILEAGE'
  };
  const unrelatedSameKindWithDifferentContent = {
    ...clone,
    id: '00000000-0000-4000-8000-000000000526',
    source_content_sha256: 'e'.repeat(64)
  };
  const workflow = {
    id: clone.workflow_id,
    generation: 2,
    renderer_contract_version: 'CANDIDATE_REVIEW_DOCUMENTS_V1',
    paper_return_manifest_sha256: 'd'.repeat(64),
    paper_return_manifest_json: { pages: [
      { page_key: 'HOURS_TIMESHEET', component_kind: 'HOURS_TIMESHEET' },
      {
        page_key: 'EXPENSE_EVIDENCE:1', component_kind: 'EXPENSE_EVIDENCE',
        expense_category: 'ACCOMMODATION', source_component_id: originalId,
        source_content_sha256: evidenceHash
      }
    ] },
    immutable_submission_json: { official_presentation: { branding } }
  };
  const timesheet = { timesheet_id: '00000000-0000-4000-8000-000000000524' };
  const version = { r2_key: 'base.pdf', sha256: baseHash };
  const objects = new Map();
  const r2Object = (bytes, mediaType) => ({
    httpMetadata: { contentType: mediaType },
    async arrayBuffer() { return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength); }
  });
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    SUPABASE_URL: 'https://test.supabase.invalid', SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder',
    R2: {
      async get(key) {
        if (key === 'base.pdf') return r2Object(baseBytes, 'application/pdf');
        if (key === 'evidence.pdf') return r2Object(evidenceBytes, 'application/pdf');
        return null;
      },
      async put(key, bytes, options) {
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
    const target = new URL(String(url));
    if (target.pathname.endsWith('/candidate_submission_components')) {
      return Response.json([
        clone, unrelatedKindWithSameDurableIdentity, unrelatedSameKindWithDifferentContent
      ]);
    }
    return Response.json([]);
  };
  try {
    const receipt = await assembleCandidatePaperPack(env, workflow, timesheet, version);
    assert.equal(receipt.page_count, 2);
    const stored = objects.get(receipt.key);
    const combined = await PDFDocument.load(stored.bytes);
    assert.equal(combined.getPageCount(), 2);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('paper pack readiness uses the durable receipt and advances only the exact authenticated workflow', async () => {
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
  const readEnd = source.indexOf('async function resumeCandidatePaperPackFromStatus', readStart);
  const readPath = source.slice(readStart, readEnd);
  assert.doesNotMatch(readPath, /assembleCandidatePaperPack|restWrite|immutablePut/);
  assert.match(readPath, /readyPaperPackReceiptFromOutbox/);
  assert.match(readPath, /if \(outbox\)[\s\S]*readyPaperPackReceiptFromOutbox[\s\S]*if \(!complete && version\?\.r2_key\)/);
  assert.match(readPath, /workflows\.length > 1[\s\S]*CANDIDATE_PAPER_WORKFLOW_CONFLICT/);
  const statusStart = source.indexOf('async function handlePaperPackStatus', readEnd);
  const statusEnd = source.indexOf('async function handlePaperPackDownload', statusStart);
  const statusPath = source.slice(statusStart, statusEnd);
  assert.match(statusPath, /workflow_id: context\.workflow\.id/);
  assert.match(statusPath, /generation: Number\(context\.workflow\.generation\)/);
  assert.match(statusPath, /paper_return_manifest_sha256: manifestSha256/);
  assert.match(statusPath, /paper_return_pages: paperReturnPages/);
  assert.match(statusPath, /page_count: paperReturnPages\.length/);
  assert.match(statusPath, /context\.state === 'PREPARING'/);
  assert.match(statusPath, /upper\(context\.timesheet\.document_state\) === 'STALE'/);
  assert.match(statusPath, /restartCandidatePaperSourceDocumentFromStatus\(/);
  assert.match(statusPath, /deferBackground\(ctx, work, 'paper-source-status-requeue'/);
  assert.match(statusPath, /UUID_RE\.test\(documentOperationId\)/);
  assert.match(statusPath, /document_operation_id: documentOperationId/);
  assert.match(statusPath, /current_timesheet_id: context\.id/);
  assert.match(statusPath, /deferBackground\(ctx, work, 'paper-pack-status-nudge'/);
  assert.match(statusPath, /resumeCandidatePaperPackFromStatus\(env, deps, context\)/);
  assert.doesNotMatch(statusPath, /timesheet_qr_send_enqueue_v1|immutablePut/);
});

test('released Paper pack remains authoritative after an expected generic document invalidation', async () => {
  const workflowId = '00000000-0000-4000-8000-000000000034';
  const timesheetId = '00000000-0000-4000-8000-000000000035';
  const manifestHash = 'a'.repeat(64);
  const baseHash = 'b'.repeat(64);
  const brandingHash = 'c'.repeat(64);
  const packHash = 'd'.repeat(64);
  const complete = {
    key: `candidate-app/test/${workflowId}/2/paper-pack/`
      + `${manifestHash}-${baseHash}-${brandingHash}-CANDIDATE_REVIEW_DOCUMENTS_V1.pdf`,
    sha256: packHash,
    byte_size: 654,
    page_count: 3
  };
  const workflow = {
    id: workflowId,
    generation: 2,
    state: 'AWAITING_PAPER_RETURN',
    paper_return_manifest_sha256: manifestHash
  };
  const timesheet = {
    timesheet_id: timesheetId,
    document_state: 'STALE',
    current_document_version_id: null
  };
  const outbox = {
    status: 'SENT',
    payment_scope_json: {
      ...readyPaperScope(workflowId, 2, manifestHash, complete),
      base_document_sha256: baseHash,
      branding_contract_sha256: brandingHash,
      renderer_contract_version: 'CANDIDATE_REVIEW_DOCUMENTS_V1'
    },
    attachments: [readyPaperAttachment(workflowId, 2, manifestHash, complete)]
  };
  const receipt = await readyPaperPackReceiptFromOutbox({
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    R2: {
      async head(key) {
        assert.equal(key, complete.key);
        return {
          size: complete.byte_size,
          customMetadata: {
            purpose: 'candidate-complete-paper-pack',
            workflow_id: workflowId,
            workflow_generation: '2',
            timesheet_id: timesheetId,
            manifest_sha256: manifestHash,
            base_document_sha256: baseHash,
            branding_contract_sha256: brandingHash,
            renderer_contract_version: 'CANDIDATE_REVIEW_DOCUMENTS_V1',
            media_type: 'application/pdf',
            sha256: packHash,
            byte_size: String(complete.byte_size),
            page_count: String(complete.page_count)
          }
        };
      }
    }
  }, workflow, timesheet, outbox);
  assert.equal(receipt.ready, true);
  assert.equal(receipt.key, complete.key);
  assert.equal(receipt.page_count, 3);
});

test('released Paper pack reads the canonical completed-pack identity written by the database', () => {
  const workflowId = '00000000-0000-4000-8000-00000000032a';
  const manifestHash = 'a'.repeat(64);
  const baseHash = 'b'.repeat(64);
  const brandingHash = 'c'.repeat(64);
  const packHash = 'd'.repeat(64);
  const complete = {
    key: `candidate-app/test/${workflowId}/2/paper-pack/`
      + `${manifestHash}-${baseHash}-${brandingHash}-CANDIDATE_REVIEW_DOCUMENTS_V1.pdf`,
    sha256: packHash,
    byte_size: 654,
    page_count: 3
  };
  const workflow = {
    id: workflowId,
    generation: 2,
    state: 'AWAITING_PAPER_RETURN',
    paper_return_manifest_sha256: manifestHash
  };
  const outbox = {
    status: 'SENT',
    payment_scope_json: {
      ...readyPaperScope(workflowId, 2, manifestHash, complete),
      candidate_complete_pack_base_document_sha256: baseHash,
      candidate_complete_pack_branding_contract_sha256: brandingHash,
      candidate_complete_pack_renderer_contract_version: 'CANDIDATE_REVIEW_DOCUMENTS_V1'
    },
    attachments: [readyPaperAttachment(workflowId, 2, manifestHash, complete)]
  };

  const receipt = candidatePaperCompleteReceipt(
    { CANDIDATE_APP_ENVIRONMENT: 'TEST' }, workflow, 2, outbox
  );
  assert.equal(receipt.ready, true);
  assert.equal(receipt.key, complete.key);
  assert.equal(receipt.renderer_contract_version, 'CANDIDATE_REVIEW_DOCUMENTS_V1');
});

test('released Paper pack rejects conflicting canonical and compatibility identities', () => {
  const workflowId = '00000000-0000-4000-8000-00000000032b';
  const manifestHash = 'a'.repeat(64);
  const baseHash = 'b'.repeat(64);
  const brandingHash = 'c'.repeat(64);
  const complete = {
    key: `candidate-app/test/${workflowId}/2/paper-pack/`
      + `${manifestHash}-${baseHash}-${brandingHash}-CANDIDATE_REVIEW_DOCUMENTS_V1.pdf`,
    sha256: 'd'.repeat(64),
    byte_size: 654,
    page_count: 3
  };
  const workflow = {
    id: workflowId,
    generation: 2,
    state: 'AWAITING_PAPER_RETURN',
    paper_return_manifest_sha256: manifestHash
  };
  const outbox = {
    status: 'SENT',
    payment_scope_json: {
      ...readyPaperScope(workflowId, 2, manifestHash, complete),
      candidate_complete_pack_base_document_sha256: baseHash,
      base_document_sha256: 'e'.repeat(64),
      candidate_complete_pack_branding_contract_sha256: brandingHash,
      candidate_complete_pack_renderer_contract_version: 'CANDIDATE_REVIEW_DOCUMENTS_V1'
    },
    attachments: [readyPaperAttachment(workflowId, 2, manifestHash, complete)]
  };

  assert.throws(() => candidatePaperCompleteReceipt(
    { CANDIDATE_APP_ENVIRONMENT: 'TEST' }, workflow, 2, outbox
  ), error => error?.code === 'CANDIDATE_PAPER_PACK_IDENTITY_CONFLICT');
});

test('Paper status polling never requeues after the exact released pack is complete', async () => {
  const sessionId = '00000000-0000-4000-8000-000000000036';
  const accountId = '00000000-0000-4000-8000-000000000037';
  const candidateId = '00000000-0000-4000-8000-000000000038';
  const workflowId = '00000000-0000-4000-8000-000000000039';
  const timesheetId = '00000000-0000-4000-8000-00000000003a';
  const manifestHash = 'a'.repeat(64);
  const baseHash = 'b'.repeat(64);
  const brandingHash = 'c'.repeat(64);
  const packHash = 'd'.repeat(64);
  const complete = {
    key: `candidate-app/test/${workflowId}/2/paper-pack/`
      + `${manifestHash}-${baseHash}-${brandingHash}-CANDIDATE_REVIEW_DOCUMENTS_V1.pdf`,
    sha256: packHash,
    byte_size: 654,
    page_count: 3
  };
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'test-only-secret-material',
    SUPABASE_URL: 'https://test.example.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder',
    R2: {
      async head() {
        return {
          size: complete.byte_size,
          customMetadata: {
            purpose: 'candidate-complete-paper-pack', workflow_id: workflowId,
            workflow_generation: '2', timesheet_id: timesheetId,
            manifest_sha256: manifestHash, base_document_sha256: baseHash,
            branding_contract_sha256: brandingHash,
            renderer_contract_version: 'CANDIDATE_REVIEW_DOCUMENTS_V1',
            media_type: 'application/pdf', sha256: packHash,
            byte_size: String(complete.byte_size), page_count: '3'
          }
        };
      }
    }
  };
  const session = {
    id: sessionId, session_id: sessionId, account_id: accountId,
    selected_candidate_id: candidateId, environment: 'TEST', status: 'ACTIVE', rotation: 1,
    expires_at_utc: '2099-01-01T00:00:00.000Z',
    absolute_expires_at_utc: '2099-01-02T00:00:00.000Z'
  };
  const workflow = {
    id: workflowId, account_id: accountId, candidate_id: candidateId,
    generation: 2, environment: 'TEST', route: 'PAPER', state: 'AWAITING_PAPER_RETURN',
    target_timesheet_id: timesheetId, anchor_timesheet_id: timesheetId,
    paper_return_manifest_sha256: manifestHash,
    paper_return_manifest_json: { pages: [
      { page_key: 'hours', component_kind: 'HOURS_TIMESHEET' },
      { page_key: 'summary', component_kind: 'EXPENSE_SUMMARY' },
      { page_key: 'other', component_kind: 'EXPENSE_EVIDENCE', expense_category: 'OTHER' }
    ] }
  };
  const outbox = {
    id: '00000000-0000-4000-8000-00000000003b', status: 'SENT',
    attempt_lease_token: null, attempt_lease_expires_at_utc: null,
    payment_scope_json: {
      ...readyPaperScope(workflowId, 2, manifestHash, complete),
      base_document_sha256: baseHash,
      branding_contract_sha256: brandingHash,
      renderer_contract_version: 'CANDIDATE_REVIEW_DOCUMENTS_V1'
    },
    attachments: [readyPaperAttachment(workflowId, 2, manifestHash, complete)]
  };
  const token = await createAccessToken(env, session);
  const originalFetch = globalThis.fetch;
  let enqueueCalls = 0;
  globalThis.fetch = async input => {
    const path = new URL(String(input)).pathname;
    if (path.endsWith('/candidate_app_sessions')) return Response.json([session]);
    if (path.endsWith('/candidate_submission_workflows')) return Response.json([workflow]);
    if (path.endsWith('/timesheets')) return Response.json([{
      timesheet_id: timesheetId, version: 8, sheet_scope: 'WEEKLY',
      submission_mode: 'MANUAL', qr_status: 'SENT', qr_token: 'paper-token',
      document_revision: 10, document_state: 'STALE',
      current_document_version_id: null, active_document_operation_id: null
    }]);
    if (path.endsWith('/mail_outbox')) return Response.json([outbox]);
    throw new Error(`unexpected fetch ${path}`);
  };
  try {
    const response = await handleCandidateAppRequest(new Request(
      `https://private.test/candidate-app/v1/timesheets/${timesheetId}/paper-pack/status`,
      { headers: { authorization: `Bearer ${token}` } }
    ), env, {}, {
      routeAudience: 'PRIVATE',
      async rpc(name) {
        assert.equal(name, 'candidate_app_timesheet_detail_v1');
        return { ok: true };
      },
      async enqueueQrPack() { enqueueCalls += 1; }
    });
    assert.equal(response.status, 200);
    const body = await response.json();
    assert.equal(body.paper_pack_state, 'READY');
    assert.equal(body.download_available, true);
    assert.equal(body.page_count, 3);
    assert.equal(body.paper_pack_obtained, false);
    assert.deepEqual(body.paper_pack_delivery_methods, []);
    assert.equal(body.paper_pack_obtained_at_utc, null);
    assert.equal(enqueueCalls, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
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
        candidate_mail_authority: 'CANDIDATE_PAPER_V1',
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
          candidate_mail_authority: 'CANDIDATE_PAPER_V1',
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
    candidate_mail_authority: 'CANDIDATE_PAPER_V1',
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
        candidate_mail_authority: 'CANDIDATE_PAPER_V1',
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
        candidate_mail_authority: 'CANDIDATE_PAPER_V1',
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

test('completed paper pack stays downloadable while its email delivery is leased', async () => {
  const originalFetch = globalThis.fetch;
  const workflowId = '00000000-0000-4000-8000-000000000044';
  const timesheetId = '00000000-0000-4000-8000-000000000047';
  const manifestHash = 'd'.repeat(64);
  const completed = {
    id: '00000000-0000-4000-8000-000000000043',
    status: 'QUEUED',
    payment_scope_json: readyPaperScope(workflowId, 2, manifestHash),
    attachments: [readyPaperAttachment(workflowId, 2, manifestHash)],
    attempt_lease_token: 'email-delivery-in-progress',
    attempt_lease_expires_at_utc: '2026-08-30T12:30:00.000Z'
  };
  globalThis.fetch = async (url, options = {}) => {
    assert.equal(options.method || 'GET', 'GET');
    assert.ok(new URL(url).pathname.endsWith('/mail_outbox'));
    return Response.json([completed]);
  };
  try {
    const result = await requireCandidatePaperOutbox({
      SUPABASE_URL: 'https://test.supabase.invalid',
      SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
    }, {
      id: workflowId,
      generation: 2,
      paper_return_manifest_sha256: manifestHash
    }, { timesheet_id: timesheetId });
    assert.deepEqual(result, completed);
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

test('paper-pack scheduler durably records a classified retryable storage failure', async () => {
  const originalFetch = globalThis.fetch;
  const workflow = {
    id: '00000000-0000-4000-8000-000000000070', generation: 3,
    route: 'PAPER', state: 'AWAITING_PAPER_RETURN',
    target_timesheet_id: '00000000-0000-4000-8000-000000000071',
    paper_return_manifest_sha256: 'a'.repeat(64),
    paper_return_manifest_json: { pages: [{ component_kind: 'HOURS_TIMESHEET' }] },
    renderer_contract_version: 'CANDIDATE_REVIEW_DOCUMENTS_V1',
    immutable_submission_json: {
      official_presentation: { branding: { branding_contract_sha256: 'c'.repeat(64) } }
    }
  };
  const timesheet = {
    timesheet_id: workflow.target_timesheet_id, version: 1, sheet_scope: 'WEEKLY',
    submission_mode: 'MANUAL', qr_status: 'PENDING', document_state: 'READY',
    current_document_version_id: '00000000-0000-4000-8000-000000000072'
  };
  const outboxId = '00000000-0000-4000-8000-000000000073';
  let failureRpc = null;
  globalThis.fetch = async (url) => {
    const parsed = new URL(url);
    if (parsed.pathname.endsWith('/candidate_submission_workflows')) return Response.json([workflow]);
    if (parsed.pathname.endsWith('/timesheets')) return Response.json([timesheet]);
    if (parsed.pathname.endsWith('/invoice_document_versions')) return Response.json([{
      id: timesheet.current_document_version_id, entity_type: 'TIMESHEET',
      entity_id: timesheet.timesheet_id, purpose: 'TIMESHEET', status: 'READY',
      r2_key: 'candidate/base.pdf', sha256: 'b'.repeat(64)
    }]);
    if (parsed.pathname.endsWith('/mail_outbox')) return Response.json([{
      id: outboxId, type: 'TIMESHEET_QR', context_kind: 'timesheets',
      context_id: timesheet.timesheet_id, status: 'QUEUED', attachments: [],
      attempt_lease_token: null, attempt_lease_expires_at_utc: null,
      payment_scope_json: {
        candidate_mail_authority: 'CANDIDATE_PAPER_V1',
        candidate_workflow_id: workflow.id,
        candidate_workflow_generation: workflow.generation,
        paper_return_manifest_sha256: workflow.paper_return_manifest_sha256,
        candidate_paper_pack_ready: false,
        mail_held_until_pdf_rendered: true,
        mail_hold_reason: 'CANDIDATE_PAPER_PACK_PENDING'
      }
    }]);
    throw new Error(`unexpected request ${parsed.pathname}`);
  };
  try {
    const result = await processPendingCandidatePaperPacks({
      CANDIDATE_APP_ENVIRONMENT: 'TEST',
      SUPABASE_URL: 'https://test.supabase.invalid',
      SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder',
      R2: {
        async head() { throw new Error('simulated R2 read outage'); }
      }
    }, {
      async rpc(name, args) {
        if (args.p_action === 'PAPER_PACK_ATTEMPT_CLAIM') {
          return { data: {
            ok: true, paper_pack_attempt_state: 'CLAIMED', claim_acquired_new: true
          } };
        }
        failureRpc = { name, args };
        return { data: { ok: true, paper_pack_state: 'FAILED_RETRYABLE' } };
      }
    }, 1);
    assert.equal(result.results.length, 1);
    assert.equal(result.results[0].ok, false);
    assert.equal(result.results[0].error_code, 'CANDIDATE_PAPER_SOURCE_READ_TRANSIENT');
    assert.equal(result.results[0].failure_recorded, true);
    assert.equal(result.results[0].failure_state, 'FAILED_RETRYABLE');
    assert.equal(failureRpc.name, 'candidate_workflow_transition_atomic_v1');
    assert.equal(failureRpc.args.p_action, 'PAPER_PACK_MARK_FAILURE');
    assert.equal(failureRpc.args.p_payload.mail_outbox_id, outboxId);
    assert.equal(failureRpc.args.p_payload.error_code, 'CANDIDATE_PAPER_SOURCE_READ_TRANSIENT');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('Candidate auth/account mutation routes reject a missing caller idempotency key before work', async () => {
  const cases = [
    ['/candidate-app/v1/auth/challenge/start', { email: 'candidate@example.test', purpose: 'ACTIVATE' }],
    ['/candidate-app/v1/auth/challenge/resend', {
      email: 'candidate@example.test', purpose: 'ACTIVATE',
      challenge_id: '00000000-0000-4000-8000-000000000001'
    }],
    ['/candidate-app/v1/auth/challenge/verify', {
      email: 'candidate@example.test', purpose: 'ACTIVATE', token: 'token'
    }],
    ['/candidate-app/v1/auth/password/complete', {
      challenge_id: '00000000-0000-4000-8000-000000000001', password: 'long-enough-password'
    }],
    ['/candidate-app/v1/auth/login', { email: 'candidate@example.test', password: 'password' }],
    ['/candidate-app/v1/auth/refresh', {
      session_id: '00000000-0000-4000-8000-000000000001', refresh_token: 'refresh'
    }],
    ['/candidate-app/v1/auth/logout', {}],
    ['/candidate-app/v1/account/select-candidate', {
      selected_candidate_id: '00000000-0000-4000-8000-000000000002'
    }],
    ['/candidate-app/v1/account/preferences', { notification_preferences: { email: true } }],
    ['/candidate-app/v1/account/push-token', {
      push_provider: 'WEB_PUSH', push_token_ciphertext_hex: 'aa'.repeat(32), push_key_version: 1
    }],
    ['/candidate-app/v1/account/password', {
      current_password: 'old-password-value', password: 'new-password-value'
    }],
    ['/candidate-app/v1/notifications/00000000-0000-4000-8000-000000000003/read', {}]
  ];
  for (const [path, body] of cases) {
    const response = await handleCandidateAppRequest(new Request(`https://private.test${path}`, {
      method: path.endsWith('/preferences') ? 'PATCH' : 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body)
    }), { CANDIDATE_APP_ENVIRONMENT: 'TEST' }, {}, {
      routeAudience: 'PRIVATE',
      async rpc() { throw new Error('mutation RPC must not run'); }
    });
    assert.equal(response.status, 400, path);
    assert.equal((await response.json()).error_code, 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED', path);
  }
});

test('failed login lost-response replay does not advance the lockout counter twice', async () => {
  const verifier = await derivePasswordVerifier('correct-login-password');
  const originalFetch = globalThis.fetch;
  let receipt = null;
  let failureWrites = 0;
  let accountReads = 0;
  const deps = {
    routeAudience: 'PRIVATE',
    async rpc(name, args) {
      assert.equal(name, 'candidate_auth_account_transition_v1');
      if (args.p_payload.replay_probe_only === true
          && !args.p_payload.idempotency_request_sha256) {
        return receipt
          ? { replay_receipt_found: true, request_version_reserved: true, request_key_version: 1 }
          : { replay_receipt_found: false, request_version_reserved: true, request_key_version: 1 };
      }
      if (args.p_payload.replay_probe_only === true) {
        return { ...receipt, idempotent_replay: true };
      }
      assert.equal(args.p_action, 'LOGIN_SUCCESS');
      assert.equal(args.p_payload.login_failed, true);
      assert.match(args.p_payload.presented_password_digest_hex, /^[0-9a-f]{64}$/);
      assert.match(args.p_payload.expected_password_authority_sha256, /^[0-9a-f]{64}$/);
      failureWrites += 1;
      receipt = { ok: false, error_code: 'CANDIDATE_LOGIN_INVALID', failed_login_recorded: true };
      return receipt;
    }
  };
  globalThis.fetch = async () => {
    accountReads += 1;
    return Response.json([{
      id: '00000000-0000-4000-8000-000000000081', environment: 'TEST', status: 'ACTIVE',
      password_scheme: verifier.scheme, password_scheme_version: verifier.scheme_version,
      password_salt: verifier.salt_hex, password_digest: verifier.digest_hex,
      password_params_json: verifier.params, locked_until_utc: null
    }]);
  };
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'candidate-auth-test-secret',
    SUPABASE_URL: 'https://test.supabase.invalid', SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  const makeRequest = () => new Request('https://private.test/candidate-app/v1/auth/login', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      email: 'candidate@example.test', password: 'wrong-login-password',
      idempotency_key: 'failed-login-lost-response-key'
    })
  });
  try {
    const first = await handleCandidateAppRequest(makeRequest(), env, {}, deps);
    const replay = await handleCandidateAppRequest(makeRequest(), env, {}, deps);
    assert.equal(first.status, 401, JSON.stringify(await first.clone().json()));
    assert.equal(replay.status, 401, JSON.stringify(await replay.clone().json()));
    assert.equal((await first.json()).error_code, 'CANDIDATE_LOGIN_INVALID');
    assert.equal((await replay.json()).error_code, 'CANDIDATE_LOGIN_INVALID');
    assert.equal(failureWrites, 1);
    assert.equal(accountReads, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('unknown-account login owns a durable generic failure receipt and changed email conflicts', async () => {
  const originalFetch = globalThis.fetch;
  let receipt = null;
  let writes = 0;
  let accountReads = 0;
  const deps = {
    routeAudience: 'PRIVATE',
    async rpc(name, args) {
      assert.equal(name, 'candidate_auth_account_transition_v1');
      if (args.p_payload.replay_probe_only === true
          && !args.p_payload.idempotency_request_sha256) {
        return receipt
          ? { replay_receipt_found: true, request_version_reserved: true, request_key_version: 1 }
          : { replay_receipt_found: false, request_version_reserved: true, request_key_version: 1 };
      }
      if (args.p_payload.replay_probe_only === true) {
        if (args.p_payload.idempotency_request_sha256 !== receipt.request_sha256) {
          throw new Error('CANDIDATE_IDEMPOTENCY_CONFLICT');
        }
        return { ...receipt.response, idempotent_replay: true };
      }
      assert.equal(args.p_account_id, null);
      assert.equal(args.p_payload.login_failed, true);
      assert.equal(args.p_payload.presented_password_digest_hex, undefined);
      assert.equal(args.p_payload.expected_password_authority_sha256, undefined);
      writes += 1;
      receipt = {
        request_sha256: args.p_payload.idempotency_request_sha256,
        response: {
          ok: false, error_code: 'CANDIDATE_LOGIN_INVALID', failed_login_recorded: false
        }
      };
      return receipt.response;
    }
  };
  globalThis.fetch = async () => {
    accountReads += 1;
    return Response.json([]);
  };
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'unknown-account-receipt-secret',
    CANDIDATE_AUTH_REPLAY_SECRET_V1: 'unknown-account-request-secret',
    SUPABASE_URL: 'https://test.supabase.invalid', SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  const invoke = email => handleCandidateAppRequest(new Request(
    'https://private.test/candidate-app/v1/auth/login', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        email, password: 'unknown-account-password',
        idempotency_key: 'unknown-account-durable-key'
      })
    }
  ), env, {}, deps);
  try {
    const first = await invoke('unknown-one@example.test');
    const replay = await invoke('unknown-one@example.test');
    const conflict = await invoke('unknown-two@example.test');
    assert.equal(first.status, 401);
    assert.equal(replay.status, 401);
    assert.equal(conflict.status, 409);
    assert.equal((await conflict.json()).error_code, 'CANDIDATE_IDEMPOTENCY_CONFLICT');
    assert.equal(writes, 1);
    assert.equal(accountReads, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('concurrent activation, login and refresh return the database winner refresh token', async () => {
  const originalFetch = globalThis.fetch;
  const verifier = await derivePasswordVerifier('correct-concurrent-password');
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'concurrent-session-signing-secret',
    CANDIDATE_AUTH_REPLAY_SECRET_V1: 'concurrent-auth-replay-secret',
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  globalThis.fetch = async (url) => {
    const path = new URL(url).pathname;
    if (path.endsWith('/candidate_app_accounts')) return Response.json([{
      id: '00000000-0000-4000-8000-000000000181', environment: 'TEST', status: 'ACTIVE',
      password_scheme: verifier.scheme, password_scheme_version: verifier.scheme_version,
      password_salt: verifier.salt_hex, password_digest: verifier.digest_hex,
      password_params_json: verifier.params, locked_until_utc: null
    }]);
    throw new Error(`unexpected REST operation ${path}`);
  };

  async function runRace(action, path, requestBody) {
    let metadataCalls = 0;
    let releaseMetadata;
    const bothMetadataCalls = new Promise(resolve => { releaseMetadata = resolve; });
    let winner = null;
    let winnerRefreshHash = null;
    let mutationCalls = 0;
    const deps = {
      routeAudience: 'PRIVATE',
      async rpc(name, args) {
        assert.equal(name, 'candidate_auth_account_transition_v1');
        assert.equal(args.p_action, action);
        if (args.p_payload.replay_probe_only === true
            && !args.p_payload.idempotency_request_sha256) {
          metadataCalls += 1;
          if (metadataCalls === 2) releaseMetadata();
          await bothMetadataCalls;
          return { replay_receipt_found: false, request_version_reserved: true, request_key_version: 1 };
        }
        mutationCalls += 1;
        if (action === 'LOGIN_SUCCESS') {
          assert.match(args.p_payload.presented_password_digest_hex, /^[0-9a-f]{64}$/);
          assert.match(args.p_payload.expected_password_authority_sha256, /^[0-9a-f]{64}$/);
        }
        if (!winner) {
          const sessionId = action === 'REFRESH_SESSION'
            ? args.p_payload.new_session_id : args.p_session_id;
          winnerRefreshHash = action === 'REFRESH_SESSION'
            ? args.p_payload.new_refresh_token_hash_hex : args.p_payload.refresh_token_hash_hex;
          winner = {
            ok: true, session_id: sessionId,
            rotation: action === 'REFRESH_SESSION' ? 1 : 0,
            issued_at_utc: new Date(Date.now() - 1000).toISOString(),
            expires_at_utc: new Date(Date.now() + 30 * 86_400_000).toISOString(),
            absolute_expires_at_utc: new Date(Date.now() + 90 * 86_400_000).toISOString(),
            selected_candidate_id: null, selection_required: false,
            token_key_version: 1
          };
          return winner;
        }
        return { ...winner, idempotent_replay: true };
      }
    };
    const invoke = () => handleCandidateAppRequest(new Request(`https://private.test${path}`, {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify(requestBody)
    }), env, {}, deps);
    const responses = await Promise.all([invoke(), invoke()]);
    const bodies = await Promise.all(responses.map(response => response.json()));
    assert.deepEqual(responses.map(response => response.status), [200, 200], action);
    assert.equal(metadataCalls, 2, action);
    assert.equal(mutationCalls, 2, action);
    assert.equal(bodies[0].session_id, bodies[1].session_id, action);
    assert.equal(bodies[0].access_token, bodies[1].access_token, action);
    assert.equal(bodies[0].refresh_token, bodies[1].refresh_token, action);
    assert.equal(
      createHash('sha256').update(bodies[0].refresh_token).digest('hex'),
      winnerRefreshHash,
      `${action} must return the token whose hash the winning transaction stored`
    );
  }

  try {
    await runRace('ACTIVATE_PASSWORD', '/candidate-app/v1/auth/password/complete', {
      challenge_id: '00000000-0000-4000-8000-000000000182',
      password: 'correct-concurrent-password', idempotency_key: 'concurrent-activation'
    });
    await runRace('LOGIN_SUCCESS', '/candidate-app/v1/auth/login', {
      email: 'candidate@example.test', password: 'correct-concurrent-password',
      idempotency_key: 'concurrent-login'
    });
    await runRace('REFRESH_SESSION', '/candidate-app/v1/auth/refresh', {
      session_id: '00000000-0000-4000-8000-000000000183',
      refresh_token: 'concurrent-original-refresh', idempotency_key: 'concurrent-refresh'
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('concurrent logout and password change recover the durable result after mutable preconditions move', async () => {
  const originalFetch = globalThis.fetch;
  const sessionId = '00000000-0000-4000-8000-000000000191';
  const accountId = '00000000-0000-4000-8000-000000000192';
  const candidateId = '00000000-0000-4000-8000-000000000193';
  const oldVerifier = await derivePasswordVerifier('old-concurrent-password');
  const newVerifier = await derivePasswordVerifier('new-concurrent-password');
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'precondition-session-secret',
    CANDIDATE_AUTH_REPLAY_SECRET_V1: 'precondition-replay-secret',
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  const accessToken = await createAccessToken(env, { session_id: sessionId, rotation: 0 });

  async function runRace(action, path, body, mode) {
    let metadataCalls = 0;
    let releaseMetadata;
    const metadataBarrier = new Promise(resolve => { releaseMetadata = resolve; });
    let resolveCommitted;
    const committed = new Promise(resolve => { resolveCommitted = resolve; });
    let receipt = null;
    let mutationCalls = 0;
    let sessionReads = 0;
    let accountReads = 0;
    globalThis.fetch = async (url) => {
      const restPath = new URL(url).pathname;
      if (restPath.endsWith('/candidate_app_sessions')) {
        sessionReads += 1;
        if (mode === 'LOGOUT' && sessionReads === 2) {
          await committed;
          return Response.json([]);
        }
        return Response.json([{
          id: sessionId, account_id: accountId, environment: 'TEST',
          selected_candidate_id: candidateId, status: 'ACTIVE', rotation: 0,
          expires_at_utc: '2099-01-01T00:00:00.000Z',
          absolute_expires_at_utc: '2099-01-01T00:00:00.000Z'
        }]);
      }
      if (restPath.endsWith('/candidate_app_accounts')) {
        accountReads += 1;
        if (accountReads === 2) await committed;
        const source = accountReads === 1 ? oldVerifier : newVerifier;
        return Response.json([{
          id: accountId, password_scheme: source.scheme,
          password_scheme_version: source.scheme_version,
          password_salt: source.salt_hex, password_digest: source.digest_hex,
          password_params_json: source.params
        }]);
      }
      throw new Error(`unexpected REST operation ${restPath}`);
    };
    const deps = {
      routeAudience: 'PRIVATE',
      async rpc(name, args) {
        assert.equal(name, 'candidate_auth_account_transition_v1');
        assert.equal(args.p_action, action);
        if (args.p_payload.replay_probe_only === true
            && !args.p_payload.idempotency_request_sha256) {
          metadataCalls += 1;
          if (metadataCalls === 2) releaseMetadata();
          await metadataBarrier;
          return { replay_receipt_found: false, request_version_reserved: true, request_key_version: 1 };
        }
        if (args.p_payload.replay_probe_only === true) {
          assert.equal(args.p_payload.idempotency_request_sha256, receipt.request_sha256);
          return { ...receipt.response, idempotent_replay: true };
        }
        if (receipt) {
          assert.equal(args.p_payload.idempotency_request_sha256, receipt.request_sha256);
          return { ...receipt.response, idempotent_replay: true };
        }
        mutationCalls += 1;
        receipt = {
          request_sha256: args.p_payload.idempotency_request_sha256,
          response: action === 'LOGOUT'
            ? { ok: true, session_id: sessionId, status: 'REVOKED' }
            : { ok: true, account_id: accountId, session_version: 2 }
        };
        resolveCommitted();
        return receipt.response;
      }
    };
    const invoke = () => handleCandidateAppRequest(new Request(`https://private.test${path}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${accessToken}` },
      body: JSON.stringify(body)
    }), env, {}, deps);
    const responses = await Promise.all([invoke(), invoke()]);
    const bodies = await Promise.all(responses.map(response => response.json()));
    assert.deepEqual(responses.map(response => response.status), [200, 200], action);
    assert.equal(metadataCalls, 2, action);
    assert.equal(mutationCalls, 1, action);
    assert.equal(bodies[0].ok, true, action);
    assert.equal(bodies[1].ok, true, action);
  }

  try {
    await runRace('LOGOUT', '/candidate-app/v1/auth/logout', {
      idempotency_key: 'concurrent-logout'
    }, 'LOGOUT');
    await runRace('CHANGE_PASSWORD', '/candidate-app/v1/account/password', {
      current_password: 'old-concurrent-password', password: 'new-concurrent-password',
      idempotency_key: 'concurrent-password-change'
    }, 'CHANGE_PASSWORD');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('selected-candidate precondition recovery returns the same frozen access credential', async () => {
  const originalFetch = globalThis.fetch;
  const sessionId = '00000000-0000-4000-8000-000000000281';
  const candidateId = '00000000-0000-4000-8000-000000000282';
  const issuedAtUtc = new Date(Math.floor((Date.now() - 1000) / 1000) * 1000).toISOString();
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'selected-recovery-access-secret',
    CANDIDATE_AUTH_REPLAY_SECRET_V1: 'selected-recovery-replay-secret',
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  const accessToken = await createAccessToken(env, {
    session_id: sessionId, rotation: 3, issued_at_utc: issuedAtUtc
  });
  let exactReplayReads = 0;
  const deps = {
    routeAudience: 'PRIVATE',
    async rpc(name, args) {
      assert.equal(name, 'candidate_auth_account_transition_v1');
      assert.equal(args.p_action, 'SELECT_TEST_CANDIDATE');
      assert.equal(args.p_payload.replay_probe_only, true);
      if (!args.p_payload.idempotency_request_sha256) {
        return { replay_receipt_found: false, request_version_reserved: true, request_key_version: 1 };
      }
      exactReplayReads += 1;
      return {
        ok: true,
        session_id: sessionId,
        selected_candidate_id: candidateId,
        idempotent_replay: true
      };
    }
  };
  globalThis.fetch = async (url) => {
    assert.match(new URL(url).pathname, /candidate_app_sessions$/);
    return Response.json([]);
  };
  const invoke = () => handleCandidateAppRequest(new Request(
    'https://private.test/candidate-app/v1/account/select-candidate', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        authorization: `Bearer ${accessToken}`
      },
      body: JSON.stringify({
        selected_candidate_id: candidateId,
        idempotency_key: 'selected-candidate-precondition-recovery'
      })
    }
  ), env, {}, deps);
  try {
    const first = await invoke();
    const replay = await invoke();
    assert.equal(first.status, 200);
    assert.equal(replay.status, 200);
    const firstBody = await first.json();
    const replayBody = await replay.json();
    assert.equal(firstBody.selected_candidate_id, candidateId);
    assert.equal(firstBody.access_token, replayBody.access_token);
    assert.equal(firstBody.issued_at_utc, issuedAtUtc);
    assert.equal(replayBody.issued_at_utc, issuedAtUtc);
    assert.equal(exactReplayReads, 2);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('push registration replay hashes semantic token identity rather than randomized ciphertext', async () => {
  const originalFetch = globalThis.fetch;
  const sessionId = '00000000-0000-4000-8000-0000000001a1';
  const accountId = '00000000-0000-4000-8000-0000000001a2';
  const candidateId = '00000000-0000-4000-8000-0000000001a3';
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'push-session-secret',
    CANDIDATE_AUTH_REPLAY_SECRET_V1: 'push-replay-secret',
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  const accessToken = await createAccessToken(env, { session_id: sessionId, rotation: 0 });
  globalThis.fetch = async (url) => {
    const path = new URL(url).pathname;
    if (path.endsWith('/candidate_app_sessions')) return Response.json([{
      id: sessionId, account_id: accountId, environment: 'TEST',
      selected_candidate_id: candidateId, status: 'ACTIVE', rotation: 0,
      expires_at_utc: '2099-01-01T00:00:00.000Z',
      absolute_expires_at_utc: '2099-01-01T00:00:00.000Z'
    }]);
    throw new Error(`unexpected REST operation ${path}`);
  };
  let receipt = null;
  let writes = 0;
  const deps = {
    routeAudience: 'PRIVATE',
    async rpc(name, args) {
      assert.equal(name, 'candidate_auth_account_transition_v1');
      assert.equal(args.p_action, 'REGISTER_PUSH_TOKEN');
      if (args.p_payload.replay_probe_only === true
          && !args.p_payload.idempotency_request_sha256) {
        return receipt
          ? {
              replay_receipt_found: true, request_version_reserved: true, request_key_version: 1,
              push_token_identity_key_version: receipt.push_token_identity_key_version
            }
          : { replay_receipt_found: false, request_version_reserved: true, request_key_version: 1 };
      }
      if (args.p_payload.replay_probe_only === true) {
        if (args.p_payload.idempotency_request_sha256 !== receipt.request_sha256) {
          throw new Error('CANDIDATE_IDEMPOTENCY_CONFLICT');
        }
        return { ...receipt.response, idempotent_replay: true };
      }
      writes += 1;
      receipt = {
        request_sha256: args.p_payload.idempotency_request_sha256,
        push_token_identity_key_version: args.p_payload.push_token_identity_key_version,
        response: { ok: true, session_id: sessionId, push_registered: true }
      };
      return receipt.response;
    }
  };
  const invoke = (ciphertext, identity, identityVersion = 1, proofs = null) => handleCandidateAppRequest(new Request(
    'https://private.test/candidate-app/v1/account/push-token', {
      method: 'POST',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${accessToken}` },
      body: JSON.stringify({
        push_provider: 'FCM', push_token_ciphertext_hex: ciphertext, push_key_version: 1,
        push_token_identity_hmac: identity, push_token_identity_key_version: identityVersion,
        push_token_identity_proofs: proofs || [{ key_version: identityVersion, identity_hmac: identity }],
        idempotency_key: 'push-semantic-replay'
      })
    }
  ), env, {}, deps);
  try {
    const first = await invoke('aa'.repeat(32), '1'.repeat(64));
    const replay = await invoke('bb'.repeat(32), '1'.repeat(64));
    const replayAfterIdentityRotation = await invoke(
      'cc'.repeat(32), '2'.repeat(64), 2,
      [
        { key_version: 1, identity_hmac: '1'.repeat(64) },
        { key_version: 2, identity_hmac: '2'.repeat(64) }
      ]
    );
    const conflict = await invoke('dd'.repeat(32), '3'.repeat(64), 2, [
      { key_version: 1, identity_hmac: '3'.repeat(64) },
      { key_version: 2, identity_hmac: '3'.repeat(64) }
    ]);
    assert.equal(first.status, 200);
    assert.equal(replay.status, 200);
    assert.equal((await replay.json()).push_registered, true);
    assert.equal(replayAfterIdentityRotation.status, 200);
    assert.equal((await replayAfterIdentityRotation.json()).push_registered, true);
    assert.equal(conflict.status, 409);
    assert.equal((await conflict.json()).error_code, 'CANDIDATE_IDEMPOTENCY_CONFLICT');
    assert.equal(writes, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('Candidate OpenAPI requires caller idempotency for every auth and account mutation', async () => {
  const openapi = await readFile(new URL('../docs/candidate-app/CANDIDATE_API_OPENAPI_V1.yaml', import.meta.url), 'utf8');
  for (const path of [
    '/candidate-app/v1/auth/challenge/start', '/candidate-app/v1/auth/challenge/resend',
    '/candidate-app/v1/auth/challenge/verify', '/candidate-app/v1/auth/password/complete',
    '/candidate-app/v1/auth/login', '/candidate-app/v1/auth/logout',
    '/candidate-app/v1/account/select-candidate', '/candidate-app/v1/account/preferences',
    '/candidate-app/v1/account/password',
    '/candidate-app/v1/notifications/{notificationId}/read'
  ]) {
    const start = openapi.indexOf(`  ${path}:`);
    const next = openapi.indexOf('\n  /', start + 4);
    const operation = openapi.slice(start, next < 0 ? undefined : next);
    assert.ok(start >= 0, path);
    assert.match(operation, /requestBodies\/IdempotentJsonBody/, path);
  }
  assert.match(openapi, /RefreshBody:[\s\S]*required: \[refresh_token, session_id, idempotency_key\]/);
  assert.match(openapi, /PushTokenBody:[\s\S]*required: \[push_provider, push_token, idempotency_key\]/);
  assert.match(openapi, /CANDIDATE_CHALLENGE_RESEND_TOO_SOON[\s\S]*Retry-After/);
  assert.match(openapi, /Either throttle consumes its idempotency key/);
  assert.match(openapi, /details:[\s\S]*retry_after_seconds and terminal/);
});

test('notification read acknowledgement has one durable timestamped result and conflicts on changed identity', async () => {
  const originalFetch = globalThis.fetch;
  const sessionId = '00000000-0000-4000-8000-000000000071';
  const accountId = '00000000-0000-4000-8000-000000000072';
  const candidateId = '00000000-0000-4000-8000-000000000073';
  const notificationId = '00000000-0000-4000-8000-000000000074';
  const otherNotificationId = '00000000-0000-4000-8000-000000000075';
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'notification-access-signing-secret',
    CANDIDATE_AUTH_REPLAY_SECRET_V1: 'notification-replay-secret',
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  const accessToken = await createAccessToken(env, { session_id: sessionId, rotation: 0 });
  let receipt = null;
  let writes = 0;
  globalThis.fetch = async (url) => {
    const path = new URL(url).pathname;
    if (path.endsWith('/candidate_app_sessions')) return Response.json([{
      id: sessionId, account_id: accountId, environment: 'TEST',
      selected_candidate_id: candidateId, status: 'ACTIVE', rotation: 0,
      expires_at_utc: '2099-01-01T00:00:00.000Z',
      absolute_expires_at_utc: '2099-01-01T00:00:00.000Z'
    }]);
    throw new Error(`unexpected REST operation ${path}`);
  };
  const deps = {
    routeAudience: 'PRIVATE',
    async rpc(name, args) {
      assert.equal(name, 'candidate_auth_account_transition_v1');
      assert.equal(args.p_action, 'MARK_NOTIFICATION_READ');
      if (args.p_payload.replay_probe_only === true
          && !args.p_payload.idempotency_request_sha256) {
        return receipt
          ? { replay_receipt_found: true, request_version_reserved: true, request_key_version: 1 }
          : { replay_receipt_found: false, request_version_reserved: true, request_key_version: 1 };
      }
      if (args.p_payload.replay_probe_only === true) {
        if (args.p_payload.idempotency_request_sha256 !== receipt.request_sha256) {
          throw new Error('CANDIDATE_IDEMPOTENCY_CONFLICT');
        }
        return { ...receipt.response, idempotent_replay: true };
      }
      writes += 1;
      receipt = {
        request_sha256: args.p_payload.idempotency_request_sha256,
        response: {
          ok: true, notification_id: args.p_payload.notification_id,
          state: 'READ', read_at_utc: '2026-08-12T14:30:00.000Z'
        }
      };
      return receipt.response;
    }
  };
  const invoke = (id) => handleCandidateAppRequest(new Request(
    `https://private.test/candidate-app/v1/notifications/${id}/read`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${accessToken}` },
      body: JSON.stringify({ idempotency_key: 'notification-read-lost-response-key' })
    }
  ), env, {}, deps);
  try {
    const first = await invoke(notificationId);
    const replay = await invoke(notificationId);
    const conflict = await invoke(otherNotificationId);
    assert.equal(first.status, 200);
    assert.equal(replay.status, 200);
    assert.equal((await first.json()).read_at_utc, '2026-08-12T14:30:00.000Z');
    assert.equal((await replay.json()).read_at_utc, '2026-08-12T14:30:00.000Z');
    assert.equal(conflict.status, 409);
    assert.equal((await conflict.json()).error_code, 'CANDIDATE_IDEMPOTENCY_CONFLICT');
    assert.equal(writes, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('refresh lost-response replay returns the same successor token and a new key retains theft detection', async () => {
  const sessionId = '00000000-0000-4000-8000-000000000091';
  let successorId = null;
  const idempotencyKey = 'refresh-lost-response-key';
  let receipt = null;
  let firstRequestSha = null;
  const deps = {
    routeAudience: 'PRIVATE',
    async rpc(name, args) {
      assert.equal(name, 'candidate_auth_account_transition_v1');
      if (args.p_payload.replay_probe_only === true
          && !args.p_payload.idempotency_request_sha256) {
        if (args.p_idempotency_key === idempotencyKey && receipt) {
          return { replay_receipt_found: true, request_version_reserved: true, request_key_version: 1 };
        }
        return { replay_receipt_found: false, request_version_reserved: true, request_key_version: 1 };
      }
      if (args.p_payload.replay_probe_only === true) {
        assert.equal(args.p_payload.idempotency_request_sha256, firstRequestSha);
        return { ...receipt, idempotent_replay: true };
      }
      if (!receipt) {
        firstRequestSha = args.p_payload.idempotency_request_sha256;
        successorId = args.p_payload.new_session_id;
        receipt = {
          ok: true, session_id: successorId, rotation: 1,
          issued_at_utc: '2026-08-12T12:00:00.000Z',
          expires_at_utc: '2026-09-11T12:00:00.000Z',
          absolute_expires_at_utc: '2026-11-10T12:00:00.000Z',
          selected_candidate_id: null, token_key_version: 1
        };
        return receipt;
      }
      return { ok: false, error_code: 'CANDIDATE_REFRESH_TOKEN_REUSE', family_revoked: true };
    }
  };
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'stable-private-session-secret'
  };
  const invoke = (key) => handleCandidateAppRequest(new Request(
    'https://private.test/candidate-app/v1/auth/refresh', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        session_id: sessionId, refresh_token: 'original-refresh-token', idempotency_key: key
      })
    }
  ), env, {}, deps);
  const first = await invoke(idempotencyKey);
  const firstBody = await first.json();
  const replay = await invoke(idempotencyKey);
  const replayBody = await replay.json();
  assert.equal(first.status, 200);
  assert.equal(replay.status, 200);
  assert.equal(replayBody.session_id, successorId);
  assert.equal(replayBody.refresh_token, firstBody.refresh_token);
  assert.equal(replayBody.access_token, firstBody.access_token);
  const theft = await invoke('different-refresh-key');
  assert.equal(theft.status, 401);
  assert.equal((await theft.json()).error_code, 'CANDIDATE_REFRESH_TOKEN_REUSE');
});

test('phone handoff replay freezes key versions and conflicts on changed public session, device, or generation', async () => {
  const originalFetch = globalThis.fetch;
  const sessionId = '00000000-0000-4000-8000-0000000000a1';
  const accountId = '00000000-0000-4000-8000-0000000000a2';
  const candidateId = '00000000-0000-4000-8000-0000000000a3';
  const workflowId = '00000000-0000-4000-8000-0000000000a4';
  const operationKey = 'phone-handoff-replay-key';
  const baseEnv = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'access-signing-secret',
    CANDIDATE_MANAGER_TOKEN_SECRET_V1: 'retained-manager-v1-secret',
    CANDIDATE_MANAGER_TOKEN_SECRET_V2: 'new-manager-v2-secret',
    CANDIDATE_MANAGER_TOKEN_KEY_VERSION: '1',
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  const binding = {
    contract_version: 'CANDIDATE_PUBLIC_PHONE_BINDING_V1',
    public_session_binding_sha256: '11'.repeat(32),
    device_binding_sha256: '22'.repeat(32)
  };
  const accessToken = await createAccessToken(baseEnv, { session_id: sessionId, rotation: 0 });
  globalThis.fetch = async (url) => {
    const path = new URL(url).pathname;
    if (path.endsWith('/candidate_app_sessions')) return Response.json([{
      id: sessionId, account_id: accountId, environment: 'TEST',
      selected_candidate_id: candidateId, status: 'ACTIVE', rotation: 0,
      expires_at_utc: '2099-01-01T00:00:00.000Z',
      absolute_expires_at_utc: '2099-01-01T00:00:00.000Z'
    }]);
    throw new Error(`unexpected REST read ${path}`);
  };
  let durable = null;
  let durableIdentity = null;
  const deps = {
    routeAudience: 'PRIVATE',
    async rpc(name, args) {
      assert.equal(name, 'candidate_workflow_transition_atomic_v1');
      if (args.p_payload.mutation_replay_probe_only === true) {
        if (durable && JSON.stringify({
          generation: args.p_expected_generation,
          binding: args.p_payload.mutation_replay_semantic_payload.public_broker_binding
        }) !== JSON.stringify(durableIdentity)) {
          throw new Error('CANDIDATE_IDEMPOTENCY_CONFLICT');
        }
        return durable ? { ...durable, idempotent_replay: true } : { replay_found: false };
      }
      durableIdentity = {
        generation: args.p_expected_generation,
        binding: args.p_payload.public_broker_binding
      };
      durable = {
        ok: true, workflow_id: workflowId, generation: 1,
        state: 'AWAITING_MANAGER_APPROVAL',
        approval_request_id: '00000000-0000-4000-8000-0000000000a5',
        method: 'PHONE', handoff_token_key_version: args.p_payload.handoff_token_key_version,
        approval_token_hash_hex: args.p_payload.approval_token_hash_hex,
        public_broker_binding: args.p_payload.public_broker_binding,
        broker_handoff_key_version: args.p_payload.broker_handoff_key_version
      };
      return durable;
    }
  };
  const invoke = (env, publicBinding = binding, generation = 1, brokerKeyVersion = 1) => handleCandidateAppRequest(new Request(
    `https://private.test/candidate-app/v1/workflows/${workflowId}/actions/select-phone-approval`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${accessToken}` },
      body: JSON.stringify({
        generation, idempotency_key: operationKey,
        payload: {
          public_broker_binding: publicBinding,
          broker_handoff_key_version: brokerKeyVersion
        }
      })
    }
  ), env, {}, deps);
  try {
    const first = await invoke(baseEnv);
    const firstBody = await first.json();
    const replay = await invoke(
      { ...baseEnv, CANDIDATE_MANAGER_TOKEN_KEY_VERSION: '2' }, binding, 1, 2
    );
    const replayBody = await replay.json();
    assert.equal(first.status, 201);
    assert.equal(replay.status, 201);
    assert.equal(firstBody.handoff_token_key_version, 1);
    assert.equal(replayBody.handoff_token_key_version, 1);
    assert.equal(replayBody.manager_handoff_token, firstBody.manager_handoff_token);
    assert.equal(firstBody.approval_token_hash_hex, undefined);
    assert.equal(replayBody.approval_token_hash_hex, undefined);
    assert.equal(replayBody.broker_handoff_key_version, 1);
    const changedDevice = await invoke(baseEnv, {
      ...binding, device_binding_sha256: '33'.repeat(32)
    });
    const changedSession = await invoke(baseEnv, {
      ...binding, public_session_binding_sha256: '44'.repeat(32)
    });
    const changedGeneration = await invoke(baseEnv, binding, 2);
    assert.equal(changedDevice.status, 409);
    assert.equal(changedSession.status, 409);
    assert.equal(changedGeneration.status, 409);
    assert.equal((await changedDevice.json()).error_code, 'CANDIDATE_IDEMPOTENCY_CONFLICT');
    assert.equal((await changedSession.json()).error_code, 'CANDIDATE_IDEMPOTENCY_CONFLICT');
    assert.equal((await changedGeneration.json()).error_code, 'CANDIDATE_IDEMPOTENCY_CONFLICT');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('finalisation probes its durable key receipt before current approval history', async () => {
  const originalFetch = globalThis.fetch;
  const sessionId = '00000000-0000-4000-8000-0000000000b1';
  const accountId = '00000000-0000-4000-8000-0000000000b2';
  const candidateId = '00000000-0000-4000-8000-0000000000b3';
  const workflowId = '00000000-0000-4000-8000-0000000000b4';
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'finalisation-access-secret',
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder',
    R2: {
      async head() { return null; },
      async put() { return { etag: 'finalisation-test-lease' }; },
      async delete() {}
    }
  };
  const accessToken = await createAccessToken(env, { session_id: sessionId, rotation: 0 });
  let approvalRead = false;
  globalThis.fetch = async (url) => {
    const path = new URL(url).pathname;
    if (path.endsWith('/candidate_app_sessions')) return Response.json([{
      id: sessionId, account_id: accountId, environment: 'TEST',
      selected_candidate_id: candidateId, status: 'ACTIVE', rotation: 0,
      expires_at_utc: '2099-01-01T00:00:00.000Z',
      absolute_expires_at_utc: '2099-01-01T00:00:00.000Z'
    }]);
    if (path.endsWith('/candidate_submission_workflows')) return Response.json([{
      id: workflowId, account_id: accountId, candidate_id: candidateId,
      environment: 'TEST', workflow_kind: 'CONTRACT_HOURS', route: 'PHONE',
      state: 'FINALISED', generation: 3
    }]);
    if (path.endsWith('/candidate_approval_requests')) {
      approvalRead = true;
      return Response.json([]);
    }
    throw new Error(`unexpected REST read ${path}`);
  };
  try {
    const response = await handleCandidateAppRequest(new Request(
      `https://private.test/candidate-app/v1/workflows/${workflowId}/actions/retry-finalisation`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', authorization: `Bearer ${accessToken}` },
        body: JSON.stringify({ generation: 2, idempotency_key: 'finalisation-lost-response-key' })
      }
    ), env, {}, {
      routeAudience: 'PRIVATE',
      async rpc(name, args) {
        assert.equal(name, 'candidate_submission_finalize_atomic_v1');
        assert.equal(args.p_daily_materialisation_json.service_finalisation.replay_key_probe_only, true);
        return {
          ok: true, workflow_id: workflowId, generation: 3,
          state: 'FINALISED', idempotent_replay: true
        };
      }
    });
    assert.equal(response.status, 200);
    assert.equal((await response.json()).state, 'FINALISED');
    assert.equal(approvalRead, false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('a pending source document remains PREPARING after its observation deadline without claiming an attempt', async () => {
  const originalFetch = globalThis.fetch;
  const workflowId = '00000000-0000-4000-8000-0000000000c1';
  const timesheetId = '00000000-0000-4000-8000-0000000000c2';
  const outboxId = '00000000-0000-4000-8000-0000000000c3';
  const workflow = {
    id: workflowId, generation: 1, route: 'PAPER', state: 'AWAITING_PAPER_RETURN',
    target_timesheet_id: timesheetId, anchor_timesheet_id: timesheetId,
    paper_return_manifest_sha256: 'd'.repeat(64)
  };
  globalThis.fetch = async (url) => {
    const path = new URL(url).pathname;
    if (path.endsWith('/candidate_submission_workflows')) return Response.json([workflow]);
    if (path.endsWith('/timesheets')) return Response.json([{
      timesheet_id: timesheetId, document_state: 'PENDING', current_document_version_id: null
    }]);
    if (path.endsWith('/mail_outbox')) return Response.json([{
      id: outboxId, status: 'QUEUED', attachments: [],
      attempt_lease_token: null, attempt_lease_expires_at_utc: null,
      payment_scope_json: {
        candidate_mail_authority: 'CANDIDATE_PAPER_V1',
        candidate_workflow_id: workflowId, candidate_workflow_generation: 1,
        paper_return_manifest_sha256: 'd'.repeat(64),
        candidate_paper_pack_ready: false, candidate_paper_pack_retryable: false,
        candidate_paper_pack_attempt_count: 0,
        candidate_paper_pack_preparation_deadline_at_utc: '2000-01-01T00:00:00.000Z',
        mail_held_until_pdf_rendered: true,
        mail_hold_reason: 'CANDIDATE_PAPER_PACK_PENDING'
      }
    }]);
    throw new Error(`unexpected REST read ${path}`);
  };
  try {
    let rpcCalls = 0;
    const result = await processPendingCandidatePaperPacks({
      CANDIDATE_APP_ENVIRONMENT: 'TEST',
      SUPABASE_URL: 'https://test.supabase.invalid',
      SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
    }, {
      async rpc() { rpcCalls += 1; throw new Error('pending source must not claim'); }
    }, 1);
    assert.equal(result.results[0].execution_state, 'PREPARING');
    assert.equal(result.results[0].error_code, 'CANDIDATE_PAPER_DOCUMENT_PENDING');
    assert.equal(rpcCalls, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('Office PAPER retry recovers an expired executor lease with a new inner attempt key', async () => {
  const originalFetch = globalThis.fetch;
  const workflowId = '00000000-0000-4000-8000-000000000080';
  const timesheetId = '00000000-0000-4000-8000-000000000081';
  const versionId = '00000000-0000-4000-8000-000000000082';
  const outboxId = '00000000-0000-4000-8000-000000000083';
  const operationId = '00000000-0000-4000-8000-000000000084';
  const actorId = '00000000-0000-4000-8000-000000000085';
  const manifestHash = 'a'.repeat(64);
  const workflow = {
    id: workflowId, generation: 1, route: 'PAPER', state: 'AWAITING_PAPER_RETURN',
    target_timesheet_id: timesheetId, anchor_timesheet_id: timesheetId,
    paper_return_manifest_sha256: manifestHash,
    paper_return_manifest_json: {
      pages: [{ page_key: 'hours:1', component_kind: 'HOURS_TIMESHEET' }]
    }
  };
  const outbox = {
    id: outboxId, type: 'TIMESHEET_QR', context_kind: 'timesheets', context_id: timesheetId,
    status: 'QUEUED', attachments: [], attempt_lease_token: null,
    attempt_lease_expires_at_utc: null,
    payment_scope_json: {
      candidate_mail_authority: 'CANDIDATE_PAPER_V1',
      candidate_workflow_id: workflowId,
      candidate_workflow_generation: 1,
      paper_return_manifest_sha256: manifestHash,
      candidate_paper_pack_ready: false,
      mail_held_until_pdf_rendered: true,
      mail_hold_reason: 'CANDIDATE_PAPER_PACK_PENDING',
      candidate_paper_pack_retryable: true,
      candidate_paper_pack_next_retry_at_utc: '2000-01-01T00:00:00.000Z',
      candidate_paper_pack_attempt_count: 3,
      candidate_paper_pack_operation_id: operationId,
      candidate_paper_pack_operation_state: 'CLAIMED',
      candidate_paper_pack_attempt_expires_at_utc: '2000-01-01T00:10:00.000Z'
    }
  };
  globalThis.fetch = async (url) => {
    const path = new URL(url).pathname;
    if (path.endsWith('/candidate_submission_workflows')) return Response.json([workflow]);
    if (path.endsWith('/mail_outbox')) return Response.json([outbox]);
    if (path.endsWith('/timesheets')) return Response.json([{
      timesheet_id: timesheetId, version: 1, sheet_scope: 'WEEKLY', submission_mode: 'MANUAL',
      qr_status: 'PENDING', qr_token: 'paper-token', document_state: 'READY',
      current_document_version_id: versionId, is_current: true, archived_at_utc: null
    }]);
    if (path.endsWith('/invoice_document_versions')) return Response.json([{
      id: versionId, entity_type: 'TIMESHEET', entity_id: timesheetId,
      purpose: 'TIMESHEET', status: 'READY', r2_key: 'candidate/base.pdf', sha256: 'b'.repeat(64)
    }]);
    throw new Error(`unexpected request ${path}`);
  };
  let claimEnvelope = null;
  let failureEnvelope = null;
  const deps = {
    routeAudience: 'OFFICE',
    async requireOfficeUser() { return { id: actorId, role: 'admin' }; },
    async rpc(name, args) {
      assert.equal(name, 'cloudtms_office_candidate_adapter_v1');
      if (args.p_action === 'PAPER_RETRY_REPLAY') return { found: false };
      if (args.p_action === 'WORKFLOW_ACTION_EXECUTE'
          && args.p_payload.workflow_action === 'PAPER_PACK_ATTEMPT_CLAIM') {
        claimEnvelope = args.p_payload;
        return {
          ok: true, paper_pack_attempt_state: 'CLAIMED', paper_pack_attempt_count: 4,
          claim_acquired_new: true, idempotent_replay: false
        };
      }
      if (args.p_action === 'WORKFLOW_ACTION_EXECUTE'
          && args.p_payload.workflow_action === 'PAPER_PACK_MARK_FAILURE') {
        failureEnvelope = args.p_payload;
        const result = {
          ok: false,
          contract_version: 'OFFICE_CANDIDATE_PAPER_RETRY_RESULT_V3',
          idempotency_key: operationId,
          workflow_id: workflowId,
          generation: 1,
          paper_pack_state: 'FAILED_RETRYABLE',
          retryable: true,
          error_code: 'CANDIDATE_PAPER_SOURCE_READ_TRANSIENT',
          next_retry_at_utc: '2099-01-01T00:00:00.000Z'
        };
        return {
          ok: true, paper_pack_state: 'FAILED_RETRYABLE',
          failure_code: 'CANDIDATE_PAPER_SOURCE_READ_TRANSIENT',
          next_retry_at_utc: '2099-01-01T00:00:00.000Z',
          office_paper_retry_receipt: {
            found: true, http_status: 503, result, idempotent_replay: false
          }
        };
      }
      throw new Error(`unexpected RPC ${args.p_action}:${args.p_payload?.workflow_action || ''}`);
    }
  };
  try {
    const response = await handleCandidateAppRequest(new Request(
      `https://office.test/api/candidate-app/workflows/${workflowId}/actions/retry-paper-preparation`, {
        method: 'POST', headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ generation: 1, idempotency_key: operationId })
      }
    ), {
      CANDIDATE_APP_ENVIRONMENT: 'TEST',
      SUPABASE_URL: 'https://test.supabase.invalid',
      SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder',
      R2: { async get() { throw new Error('simulated expired-lease recovery read failure'); } }
    }, {}, deps);
    assert.equal(response.status, 503);
    assert.equal((await response.json()).paper_pack_state, 'FAILED_RETRYABLE');
    assert.equal(claimEnvelope.idempotency_key, `${operationId}:attempt:4`);
    assert.equal(claimEnvelope.payload.paper_pack_operation_id, operationId);
    assert.equal(failureEnvelope.payload.paper_pack_operation_id, operationId);
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
      candidate_mail_authority: 'CANDIDATE_PAPER_V1',
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

test('manager approval authority is matched to the current request before a decisive mutation', async () => {
  const approval = {
    id: '00000000-0000-4000-8000-000000000053',
    request_generation: 2,
    method: 'PHONE'
  };
  await assert.doesNotReject(assertManagerRouteApprovalContext({}, approval, {
    authority_kind: 'MANAGER_PHONE'
  }));
  await assert.rejects(
    assertManagerRouteApprovalContext({}, approval, {
      authority_kind: 'MANAGER_EMAIL'
    }),
    error => error?.code === 'MANAGER_ROUTE_CONTEXT_INVALID'
  );

  const source = await readFile(new URL('../broker/src/candidate-app-backend.js', import.meta.url), 'utf8');
  const start = source.indexOf('async function handleManagerAction');
  const end = source.indexOf('async function handleDocumentStream', start);
  const body = source.slice(start, end);
  const currentApproval = body.indexOf('managerDocumentReadContext');
  const authorityCheck = body.indexOf('assertManagerRouteApprovalContext');
  const decisiveTransition = body.indexOf('const result = await rpcCall');
  assert.ok(currentApproval >= 0);
  assert.ok(authorityCheck > currentApproval);
  assert.ok(decisiveTransition > authorityCheck);
});

test('manager document reads validate the pending token and immutable manifest without a workflow mutation', async () => {
  const workflowId = '00000000-0000-4000-8000-000000000051';
  const componentId = '00000000-0000-4000-8000-000000000052';
  const approvalId = '00000000-0000-4000-8000-000000000053';
  const handoffToken = 'manager-document-test-token';
  const tokenHash = createHash('sha256').update(handoffToken).digest('hex');
  const documentBytes = new Uint8Array([37, 80, 68, 70, 45, 49, 46, 55]);
  const documentHash = createHash('sha256').update(documentBytes).digest('hex');
  const manifestHash = 'a'.repeat(64);
  const requested = [];
  let rpcCalls = 0;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (input) => {
    const url = String(input);
    requested.push(url);
    if (url.includes('/candidate_submission_workflows?')) return Response.json([{
      id: workflowId,
      environment: 'TEST',
      generation: 4,
      review_manifest_sha256: `\\x${manifestHash}`
    }]);
    if (url.includes('/candidate_approval_requests?')) return Response.json([{
      id: approvalId,
      workflow_id: workflowId,
      workflow_generation: 4,
      request_generation: 2,
      method: 'PHONE',
      state: 'PENDING',
      expires_at_utc: '2099-01-01T00:00:00.000Z',
      review_manifest_sha256: `\\x${manifestHash}`,
      required_component_ids: [componentId],
      required_component_manifest_json: []
    }]);
    if (url.includes('/candidate_submission_components?')) return Response.json([{
      id: componentId,
      workflow_id: workflowId,
      review_storage_key: 'candidate-app/test/review.pdf',
      review_content_sha256: `\\x${documentHash}`
    }]);
    throw new Error(`unexpected request ${url}`);
  };
  try {
    const response = await handleCandidateAppRequest(new Request(
      `https://private.test/candidate-manager/v1/workflows/${workflowId}/components/${componentId}/document`,
      { headers: { authorization: `Bearer ${handoffToken}` } }
    ), {
      CANDIDATE_APP_ENVIRONMENT: 'TEST',
      SUPABASE_URL: 'https://test.supabase.invalid',
      SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder',
      R2: {
        async get(key) {
          assert.equal(key, 'candidate-app/test/review.pdf');
          return {
            httpMetadata: { contentType: 'application/pdf' },
            async arrayBuffer() { return documentBytes.buffer.slice(0); }
          };
        }
      }
    }, {}, {
      routeAudience: 'PRIVATE',
      async rpc() { rpcCalls += 1; return {}; }
    });
    assert.equal(response.status, 200);
    assert.deepEqual(new Uint8Array(await response.arrayBuffer()), documentBytes);
    assert.equal(rpcCalls, 0);
    const approvalRequest = requested.find(url => url.includes('/candidate_approval_requests?')) || '';
    assert.match(approvalRequest, new RegExp(`token_hash=eq\\.%5Cx${tokenHash}`, 'i'));
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('candidate submitted-expense document reads return the original evidence rather than its review rendering', () => {
  const sourceHash = 'a'.repeat(64);
  const reviewHash = 'b'.repeat(64);
  for (const componentKind of ['MILEAGE_FORM', 'EXPENSE_EVIDENCE']) {
    assert.deepEqual(documentStreamSource('candidate', {
      component_kind: componentKind,
      storage_key: `candidate-app/test/source-${componentKind}.jpg`,
      source_content_sha256: `\\x${sourceHash}`,
      review_storage_key: `candidate-app/test/review-${componentKind}.pdf`,
      review_content_sha256: `\\x${reviewHash}`
    }), {
      key: `candidate-app/test/source-${componentKind}.jpg`,
      hash: sourceHash
    });
  }
  assert.deepEqual(documentStreamSource('manager', {
    component_kind: 'MILEAGE_FORM',
    storage_key: 'candidate-app/test/source.jpg',
    source_content_sha256: `\\x${sourceHash}`,
    review_storage_key: 'candidate-app/test/review.pdf',
    review_content_sha256: `\\x${reviewHash}`
  }), {
    key: 'candidate-app/test/review.pdf',
    hash: reviewHash
  });
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
