import test from 'node:test';
import assert from 'node:assert/strict';

import {
  createImportReviewDispatcher,
  importReviewContract,
  importReviewSourceEvidenceFromR2,
  normalizeContractQueryEmailOverride
} from '../src/import-review.js';

const ACTOR_ID = '10000000-0000-4000-8000-000000000001';
const IMPORT_ID = '20000000-0000-4000-8000-000000000002';
const OTHER_IMPORT_ID = '20000000-0000-4000-8000-000000000003';
const OPERATION_ID = '30000000-0000-4000-8000-000000000003';
const REQUEST_ID = '40000000-0000-4000-8000-000000000004';
const HR_ROW_ID = '50000000-0000-4000-8000-000000000005';
const TIMESHEET_ID = '60000000-0000-4000-8000-000000000006';
const ACTION_ID = 'a'.repeat(64);
const HASH = 'b'.repeat(64);
const PREVIEW_HASH = 'c'.repeat(64);
const EVIDENCE_HASH = 'd'.repeat(64);

const installedContract = Object.freeze({
  ok: true,
  schema_contract_version: importReviewContract.schema,
  apply_envelope_version: importReviewContract.apply,
  apply_operation_version: importReviewContract.operation,
  correction_operation_version: importReviewContract.correction,
  follow_up_component_version: importReviewContract.followUpComponent,
  incremental_apply_version: importReviewContract.incrementalApply,
  review_ui_contract_version: importReviewContract.reviewUi,
  email_grouping_version: importReviewContract.emailGrouping,
  legacy_contracts_supported: false
});

function readyReview() {
  return {
    ok: true,
    import: {
      id: IMPORT_ID,
      source_route: 'NHSP',
      coverage_fingerprint: HASH
    },
    state: {
      status: 'READY',
      editability: { allowed_commands: ['APPLY'] },
      state_version: 7,
      preview_generation: 3,
      preview_fingerprint: PREVIEW_HASH,
      apply_contract: {
        selected_action_ids: [ACTION_ID],
        reference_invalidation_action_ids: [],
        request_envelope: { schema: importReviewContract.apply },
        request_hash: HASH
      }
    }
  };
}

function jsonRequest(method, path, body) {
  return new Request(`https://test-worker.example${path}`, {
    method,
    headers: body === undefined ? undefined : { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body)
  });
}

async function responseJson(response) {
  return { status: response.status, body: await response.json() };
}

function successRpcValue(name) {
  if (name === 'import_review_contract_version_get_v1') return installedContract;
  if (name === 'import_review_staged_scope_get_v1') return {
    ok: true,
    import_id: IMPORT_ID,
    coverage_start_date: '2026-07-01',
    coverage_end_date: '2026-07-07',
    authority_mode: 'VALIDATION_ONLY',
    authority_summary: { mode: 'VALIDATION_ONLY', source_route: 'HR_DAILY', basis: 'DAILY_EXISTING_TIMESHEET_VALIDATION' },
    scope_clients: [],
    candidate_options: [],
    candidate_has_next: false
  };
  if (name === 'import_review_actions_page_v1') return {
    ok: true,
    items: [{
      action_id: ACTION_ID,
      imported_evidence: { work_date: '2026-07-01', role: 'Band 5' },
      current_evidence: { work_date: '2026-07-01', role: 'RMN', band: 'Band 5' },
      difference_codes: ['START_TIME'], outcome_label: 'Apply reviewed amendment',
      evidence_rows: [{
        imported_evidence: { work_date: '2026-07-01', start: '08:00' },
        current_evidence: { work_date: '2026-07-01', start: '08:30' },
        difference_codes: ['START_TIME']
      }],
      resolution_kind: 'WEEKLY_ASSIGNMENT_CONTRACT',
      resolution_options: [{ option_id: `contract:${IMPORT_ID}`, contract_id: IMPORT_ID, selectable: true }]
    }],
    total_items: 1, page_number: 1, page_size: 25, total_pages: 1
  };
  if (name === 'import_review_get_v1') return readyReview();
  if (name === 'import_review_apply_status_get_v1') {
    return {
      ok: true,
      import_id: IMPORT_ID,
      operation_id: OPERATION_ID,
      outcome: 'COMMITTED_WITH_FOLLOW_UP_PENDING',
      follow_up_status: 'FAILED_RETRYABLE',
      stored_response: { post_commit_email_action_ids: [], affected_timesheet_ids: [] }
    };
  }
  if (name.endsWith('_apply_transactional')) {
    return {
      ok: true,
      import_id: IMPORT_ID,
      review_operation_id: OPERATION_ID,
      post_commit_email_action_ids: [],
      affected_timesheet_ids: []
    };
  }
  return { ok: true, rpc: name, marker: 'happy-path' };
}

test('staged scope preserves the server-owned authority summary for coverage wording', async () => {
  const current = scenario();
  const response = await current.dispatcher(
    jsonRequest('GET', `/api/import-reviews/staged/${IMPORT_ID}/scope?candidate_page=1&candidate_page_size=25`),
    { TEST: true },
    current.ctx
  );
  const captured = await responseJson(response);
  assert.equal(captured.status, 200);
  assert.equal(captured.body.data.authority_mode, 'VALIDATION_ONLY');
  assert.deepEqual(captured.body.data.authority_summary, {
    mode: 'VALIDATION_ONLY', source_route: 'HR_DAILY', basis: 'DAILY_EXISTING_TIMESHEET_VALIDATION'
  });
});

function scenario({ conflictRpc = null, conflictToken = 'IMPORT_REVIEW_VERSION_CONFLICT', contract = installedContract } = {}) {
  const calls = [];
  const followUps = [];
  const waitUntil = [];
  const sbRpc = async (_env, name, args, options) => {
    calls.push({ name, args, options });
    if (name === 'import_review_contract_version_get_v1') return contract;
    if (name === conflictRpc) {
      const error = new Error('database rejected stale request');
      error.status = 409;
      error.json = { message: conflictToken };
      throw error;
    }
    return successRpcValue(name);
  };
  const dispatcher = createImportReviewDispatcher({
    requireUser: async () => ({ id: ACTOR_ID, role: 'admin' }),
    sbRpc,
    runFollowUp: async (_env, details) => { followUps.push(details); }
  });
  const ctx = { waitUntil(promise) { waitUntil.push(Promise.resolve(promise)); } };
  return { dispatcher, calls, followUps, waitUntil, ctx };
}

const endpoints = [
  {
    name: 'contract', method: 'GET', path: '/api/import-review/contract', rpc: null
  },
  {
    name: 'list', method: 'GET', path: '/api/import-reviews?page_size=25', rpc: 'import_review_list_v1'
  },
  {
    name: 'staged scope', method: 'GET', path: `/api/import-reviews/staged/${IMPORT_ID}/scope?candidate_page=1&candidate_page_size=25`, rpc: 'import_review_staged_scope_get_v1'
  },
  {
    name: 'create', method: 'POST', path: '/api/import-reviews', rpc: 'import_review_create_v1',
    body: {
      import_id: IMPORT_ID,
      coverage_mode: 'COMPLETE_ALL',
      coverage_start_date: '2026-07-01',
      coverage_end_date: '2026-07-07',
      scope_clients: [],
      scope_candidates: [],
      expected_source_file_sha256: HASH,
      expected_parser_version: 'CLOUDTMS_IMPORT_REVIEW_PARSER_V1:NHSP',
      operation_key: 'create-import-review-0001'
    }
  },
  {
    name: 'atomic replace', method: 'POST', path: '/api/import-reviews', rpc: 'import_review_replace_v1',
    body: {
      import_id: IMPORT_ID,
      coverage_mode: 'COMPLETE_ALL',
      coverage_start_date: '2026-07-01',
      coverage_end_date: '2026-07-07',
      scope_clients: [],
      scope_candidates: [],
      expected_source_file_sha256: HASH,
      expected_parser_version: 'CLOUDTMS_IMPORT_REVIEW_PARSER_V1:NHSP',
      operation_key: 'replace-import-review-0001',
      supersede_import_id: OTHER_IMPORT_ID,
      expected_supersede_state_version: 7
    }
  },
  {
    name: 'get', method: 'GET', path: `/api/import-reviews/${IMPORT_ID}`, rpc: 'import_review_get_v1'
  },
  {
    name: 'paged actions', method: 'GET', path: `/api/import-reviews/${IMPORT_ID}/actions?page=1&page_size=25&sort_by=CANDIDATE&sort_direction=ASC&view=PENDING`, rpc: 'import_review_actions_page_v1'
  },
  {
    name: 'save selections', method: 'PUT', path: `/api/import-reviews/${IMPORT_ID}/selections`, rpc: 'import_review_save_v1',
    body: {
      expected_state_version: 7,
      expected_preview_generation: 3,
      expected_preview_fingerprint: PREVIEW_HASH,
      action_changes: [{ action_id: ACTION_ID, selected: true }],
      ui_state: { expanded_candidates: [] },
      request_id: REQUEST_ID
    }
  },
  {
    name: 'refresh', method: 'POST', path: `/api/import-reviews/${IMPORT_ID}/refresh`, rpc: 'import_review_refresh_v1',
    body: { expected_state_version: 7, max_actions: 100 }
  },
  {
    name: 'abandon', method: 'POST', path: `/api/import-reviews/${IMPORT_ID}/abandon`, rpc: 'import_review_abandon_v1',
    body: { expected_state_version: 7, reason: 'Operator chose to start again.', confirmed: true }
  },
  {
    name: 'Daily timesheet resolution', method: 'PUT', path: `/api/import-reviews/${IMPORT_ID}/daily-timesheet-resolution`, rpc: 'hr_daily_timesheet_resolution_save_v1',
    body: {
      hr_row_id: HR_ROW_ID,
      timesheet_id: TIMESHEET_ID,
      expected_state_version: 7,
      expected_preview_generation: 3,
      expected_evidence_fingerprint: EVIDENCE_HASH,
      request_id: REQUEST_ID
    }
  },
  {
    name: 'preview', method: 'GET', path: `/api/import-reviews/${IMPORT_ID}/preview?source_route=NHSP&limit=25`, rpc: 'nhsp_weekly_review_preview_v1'
  },
  {
    name: 'apply', method: 'POST', path: `/api/import-reviews/${IMPORT_ID}/apply`, rpc: 'nhsp_weekly_apply_transactional',
    body: { operation_id: OPERATION_ID, expected_state_version: 7, expected_request_hash: HASH }
  },
  {
    name: 'apply status', method: 'GET', path: `/api/import-reviews/${IMPORT_ID}/apply-status?operation_id=${OPERATION_ID}&request_hash=${HASH}`,
    rpc: 'import_review_apply_status_get_v1'
  },
  {
    name: 'failed-before-commit recovery', method: 'POST', path: `/api/import-reviews/${IMPORT_ID}/apply-recover`,
    rpc: 'import_review_apply_failed_before_commit_recover_v1', body: { operation_id: OPERATION_ID, request_hash: HASH }
  },
  {
    name: 'follow-up retry', method: 'POST', path: `/api/import-reviews/${IMPORT_ID}/follow-up/retry`, rpc: 'import_review_apply_status_get_v1',
    body: { operation_id: OPERATION_ID, request_hash: HASH }
  }
];

for (const endpoint of endpoints) {
  test(`${endpoint.name}: happy response matches the Worker contract`, async () => {
    const current = scenario();
    const response = await current.dispatcher(
      jsonRequest(endpoint.method, endpoint.path, endpoint.body),
      { TEST: true },
      current.ctx
    );
    const captured = await responseJson(response);
    assert.equal(captured.body.ok, true);
    assert.equal(captured.body.contract_version, importReviewContract.schema);
    assert.ok(Object.prototype.hasOwnProperty.call(captured.body, 'data'));
    assert.equal(captured.status, ['create', 'atomic replace'].includes(endpoint.name) ? 201 : endpoint.name === 'follow-up retry' ? 202 : 200);
    assert.equal(current.calls[0].name, 'import_review_contract_version_get_v1');
    if (endpoint.rpc) assert.ok(current.calls.some((call) => call.name === endpoint.rpc));
    if (endpoint.name === 'apply') {
      const applyCall = current.calls.find((call) => call.name === 'nhsp_weekly_apply_transactional');
      assert.deepEqual(Object.keys(applyCall.args.p_payload).sort(), [
        'invalidation_action_ids', 'review_contract', 'review_selected_action_ids'
      ]);
      assert.equal(applyCall.args.p_actor_user_id, ACTOR_ID);
      assert.equal(current.waitUntil.length, 1);
    }
    await Promise.all(current.waitUntil);
    if (endpoint.name === 'apply') {
      assert.equal(current.followUps.length, 1);
      assert.equal(current.followUps[0].requestHash, HASH);
    }
  });

  test(`${endpoint.name}: stale/conflict/error response is stable and contains no raw database detail`, async () => {
    const current = endpoint.name === 'contract'
      ? scenario({ contract: { ...installedContract, schema_contract_version: 'WRONG_VERSION' } })
      : scenario({ conflictRpc: endpoint.rpc });
    const response = await current.dispatcher(
      jsonRequest(endpoint.method, endpoint.path, endpoint.body),
      { TEST: true },
      current.ctx
    );
    const captured = await responseJson(response);
    assert.equal(captured.body.ok, false);
    assert.equal(captured.body.contract_version, importReviewContract.schema);
    assert.equal(typeof captured.body.error.code, 'string');
    assert.equal(typeof captured.body.error.message, 'string');
    assert.equal(typeof captured.body.error.category, 'string');
    assert.equal(typeof captured.body.error.retryable, 'boolean');
    assert.equal(JSON.stringify(captured.body).includes('database rejected stale request'), false);
    assert.equal(captured.status, endpoint.name === 'contract' ? 503 : 409);
  });
}

test('paged actions preserve the normalized V3 evidence and server-owned resolution options', async () => {
  const current = scenario();
  const response = await current.dispatcher(
    jsonRequest(
      'GET',
      `/api/import-reviews/${IMPORT_ID}/actions?page=1&page_size=25&sort_by=CANDIDATE&sort_direction=ASC&view=PENDING`
    ),
    { TEST: true },
    current.ctx
  );
  const captured = await responseJson(response);
  assert.equal(captured.status, 200);
  assert.equal(captured.body.contract_version, 'IMPORT_REVIEW_DB_V1');
  const [item] = captured.body.data.items;
  assert.deepEqual(item.imported_evidence, { work_date: '2026-07-01', role: 'Band 5' });
  assert.deepEqual(item.current_evidence, { work_date: '2026-07-01', role: 'RMN', band: 'Band 5' });
  assert.deepEqual(item.difference_codes, ['START_TIME']);
  assert.deepEqual(item.evidence_rows, [{
    imported_evidence: { work_date: '2026-07-01', start: '08:00' },
    current_evidence: { work_date: '2026-07-01', start: '08:30' },
    difference_codes: ['START_TIME']
  }]);
  assert.equal(item.outcome_label, 'Apply reviewed amendment');
  assert.equal(item.resolution_kind, 'WEEKLY_ASSIGNMENT_CONTRACT');
  assert.deepEqual(item.resolution_options, [
    { option_id: `contract:${IMPORT_ID}`, contract_id: IMPORT_ID, selectable: true }
  ]);
});

test('all business routes fail closed before their RPC when the database contract is unavailable', async () => {
  const current = scenario({ contract: null });
  const response = await current.dispatcher(
    jsonRequest('GET', '/api/import-reviews?page_size=25'),
    { TEST: true },
    current.ctx
  );
  const captured = await responseJson(response);
  assert.equal(captured.status, 503);
  assert.equal(captured.body.error.code, 'IMPORT_REVIEW_CONTRACT_MISMATCH');
  assert.deepEqual(current.calls.map((call) => call.name), ['import_review_contract_version_get_v1']);
});

test('the contract gate fails closed when component-aware follow-up support is absent', async () => {
  const { follow_up_component_version: _removed, ...oldContract } = installedContract;
  const current = scenario({ contract: oldContract });
  const response = await current.dispatcher(
    jsonRequest('GET', '/api/import-reviews?page_size=25'),
    { TEST: true },
    current.ctx
  );
  const captured = await responseJson(response);
  assert.equal(captured.status, 503);
  assert.equal(captured.body.error.code, 'IMPORT_REVIEW_CONTRACT_MISMATCH');
  assert.deepEqual(current.calls.map((call) => call.name), ['import_review_contract_version_get_v1']);
});

test('the V6 Worker fails closed against the superseded V5 review UI contract', async () => {
  const oldContract = { ...installedContract, review_ui_contract_version: 'IMPORT_REVIEW_UI_V5' };
  const current = scenario({ contract: oldContract });
  const response = await current.dispatcher(
    jsonRequest('GET', '/api/import-reviews?page_size=25'),
    { TEST: true },
    current.ctx
  );
  const captured = await responseJson(response);
  assert.equal(captured.status, 503);
  assert.equal(captured.body.error.code, 'IMPORT_REVIEW_CONTRACT_MISMATCH');
});

test('final confirmation views remain allowlisted and support deeply paged bounded reviews', async () => {
  const current = scenario();
  const response = await current.dispatcher(
    jsonRequest('GET', `/api/import-reviews/${IMPORT_ID}/actions?page=101&page_size=100&sort_by=CANDIDATE&sort_direction=ASC&view=CONFIRM_NON_STANDARD`),
    { TEST: true },
    current.ctx
  );
  const captured = await responseJson(response);
  assert.equal(captured.status, 200);
  const actionCall = current.calls.find((call) => call.name === 'import_review_actions_page_v1');
  assert.equal(actionCall.args.p_page_number, 101);
  assert.equal(actionCall.args.p_page_size, 100);
  assert.equal(actionCall.args.p_view, 'CONFIRM_NON_STANDARD');
});

test('the Worker fails closed when incremental apply support is absent', async () => {
  const { incremental_apply_version: _removed, ...oldContract } = installedContract;
  const current = scenario({ contract: oldContract });
  const response = await current.dispatcher(
    jsonRequest('GET', '/api/import-reviews?page_size=25'),
    { TEST: true },
    current.ctx
  );
  const captured = await responseJson(response);
  assert.equal(captured.status, 503);
  assert.equal(captured.body.error.code, 'IMPORT_REVIEW_CONTRACT_MISMATCH');
});

test('a BLOCKED review may apply its server-issued ready candidate/client batch', async () => {
  const calls = [];
  const dispatcher = createImportReviewDispatcher({
    requireUser: async () => ({ id: ACTOR_ID, role: 'admin' }),
    sbRpc: async (_env, name, args) => {
      calls.push({ name, args });
      if (name === 'import_review_contract_version_get_v1') return installedContract;
      if (name === 'import_review_get_v1') {
        const review = readyReview();
        review.state.status = 'BLOCKED';
        review.state.editability = { allowed_commands: ['SAVE_SELECTIONS', 'REFRESH', 'ABANDON', 'APPLY'] };
        return review;
      }
      if (name === 'nhsp_weekly_apply_transactional') return { ok: true, partial_application: true };
      return successRpcValue(name);
    },
    runFollowUp: async () => ({ ok: true })
  });
  const response = await dispatcher(jsonRequest('POST', `/api/import-reviews/${IMPORT_ID}/apply`, {
    operation_id: OPERATION_ID, expected_state_version: 7, expected_request_hash: HASH
  }), { TEST: true }, { waitUntil() {} });
  const captured = await responseJson(response);
  assert.equal(captured.status, 200);
  assert.equal(captured.body.data.apply.partial_application, true);
  assert.ok(calls.some((call) => call.name === 'nhsp_weekly_apply_transactional'));
});

test('the former two-step supersede route is retired', async () => {
  const current = scenario();
  const response = await current.dispatcher(
    jsonRequest('POST', `/api/import-reviews/${IMPORT_ID}/supersede`, {
      expected_state_version: 7,
      new_import_id: OTHER_IMPORT_ID
    }),
    { TEST: true },
    current.ctx
  );
  const captured = await responseJson(response);
  assert.equal(captured.status, 410);
  assert.equal(captured.body.error, 'IMPORT_REVIEW_SUPERSEDE_ROUTE_RETIRED');
  assert.equal(current.calls.some((call) => call.name === 'import_review_supersede_v1'), false);
});

test('the contract gate fails closed when recipient-address consolidation is absent', async () => {
  const { email_grouping_version: _email, ...oldContract } = installedContract;
  const current = scenario({ contract: oldContract });
  const response = await current.dispatcher(
    jsonRequest('GET', '/api/import-reviews?page_size=25'),
    { TEST: true },
    current.ctx
  );
  const captured = await responseJson(response);
  assert.equal(captured.status, 503);
  assert.equal(captured.body.error.code, 'IMPORT_REVIEW_CONTRACT_MISMATCH');
});

test('follow-up retry waits while work is pending and never starts a second runner', async () => {
  const current = scenario();
  const original = successRpcValue;
  const dispatcher = createImportReviewDispatcher({
    requireUser: async () => ({ id: ACTOR_ID, role: 'admin' }),
    sbRpc: async (_env, name) => {
      if (name === 'import_review_contract_version_get_v1') return installedContract;
      if (name === 'import_review_apply_status_get_v1') return {
        ok: true,
        outcome: 'COMMITTED_WITH_FOLLOW_UP_PENDING',
        follow_up_status: 'PENDING',
        stored_response: {}
      };
      return original(name);
    },
    runFollowUp: async () => { throw new Error('must not start'); }
  });
  const response = await dispatcher(jsonRequest(
    'POST',
    `/api/import-reviews/${IMPORT_ID}/follow-up/retry`,
    { operation_id: OPERATION_ID, request_hash: HASH }
  ), {}, current.ctx);
  const captured = await responseJson(response);
  assert.equal(captured.status, 409);
  assert.equal(captured.body.error.code, 'IMPORT_REVIEW_FOLLOW_UP_ALREADY_PENDING');
  assert.equal(current.waitUntil.length, 0);
});

test('completed follow-up is a safe no-action response', async () => {
  const current = scenario();
  const dispatcher = createImportReviewDispatcher({
    requireUser: async () => ({ id: ACTOR_ID, role: 'admin' }),
    sbRpc: async (_env, name) => {
      if (name === 'import_review_contract_version_get_v1') return installedContract;
      if (name === 'import_review_apply_status_get_v1') return {
        ok: true,
        outcome: 'COMMITTED_APPLIED',
        follow_up_status: 'COMPLETE',
        stored_response: {}
      };
      throw new Error(`unexpected RPC ${name}`);
    },
    runFollowUp: async () => { throw new Error('must not start'); }
  });
  const response = await dispatcher(jsonRequest(
    'POST',
    `/api/import-reviews/${IMPORT_ID}/follow-up/retry`,
    { operation_id: OPERATION_ID, request_hash: HASH }
  ), {}, current.ctx);
  const captured = await responseJson(response);
  assert.equal(captured.status, 200);
  assert.equal(captured.body.data.follow_up_started, false);
  assert.equal(captured.body.data.no_action_required, true);
  assert.equal(current.waitUntil.length, 0);
});

test('apply maps an active Banking Pay draft to a protected-artifact lock response', async () => {
  const current = scenario({
    conflictRpc: 'nhsp_weekly_apply_transactional',
    conflictToken: 'BLOCKED_ACTIVE_PAY_DRAFT'
  });
  const response = await current.dispatcher(jsonRequest(
    'POST',
    `/api/import-reviews/${IMPORT_ID}/apply`,
    { operation_id: OPERATION_ID, expected_state_version: 7, expected_request_hash: HASH }
  ), { TEST: true }, current.ctx);
  const captured = await responseJson(response);
  assert.equal(captured.status, 423);
  assert.equal(captured.body.error.category, 'PROTECTED_ARTIFACT');
  assert.equal(captured.body.error.action, 'LEAVE_IMPORT_AND_RESOLVE_BANKING_PAY');
});

test('apply maps a locked revalidation race to a reloadable stale conflict', async () => {
  const current = scenario({
    conflictRpc: 'nhsp_weekly_apply_transactional',
    conflictToken: 'IMPORT_REVIEW_APPLY_STALE_OR_NOT_READY'
  });
  const response = await current.dispatcher(jsonRequest(
    'POST',
    `/api/import-reviews/${IMPORT_ID}/apply`,
    { operation_id: OPERATION_ID, expected_state_version: 7, expected_request_hash: HASH }
  ), { TEST: true }, current.ctx);
  const captured = await responseJson(response);
  assert.equal(captured.status, 409);
  assert.equal(captured.body.error.category, 'STALE_CONFLICT');
  assert.equal(captured.body.error.action, 'RELOAD_REVIEW');
});

test('Daily resolution supports an explicit clear without writing browser authority to hr_rows', async () => {
  const current = scenario();
  const response = await current.dispatcher(jsonRequest(
    'PUT',
    `/api/import-reviews/${IMPORT_ID}/daily-timesheet-resolution`,
    {
      hr_row_id: HR_ROW_ID,
      timesheet_id: null,
      expected_state_version: 7,
      expected_preview_generation: 3,
      expected_evidence_fingerprint: EVIDENCE_HASH,
      request_id: REQUEST_ID
    }
  ), { TEST: true }, current.ctx);
  assert.equal(response.status, 200);
  const call = current.calls.find((entry) => entry.name === 'hr_daily_timesheet_resolution_save_v1');
  assert.equal(call.args.p_timesheet_id, null);
});

test('unknown browser authority fields are rejected before any mutation RPC', async () => {
  const current = scenario();
  const response = await current.dispatcher(jsonRequest(
    'POST',
    `/api/import-reviews/${IMPORT_ID}/apply`,
    {
      operation_id: OPERATION_ID,
      expected_state_version: 7,
      expected_request_hash: HASH,
      validation_rows: [{ browser_owned: true }]
    }
  ), { TEST: true }, current.ctx);
  const captured = await responseJson(response);
  assert.equal(captured.status, 400);
  assert.equal(captured.body.error.code, 'IMPORT_REVIEW_INVALID_REQUEST');
  assert.equal(current.calls.some((entry) => entry.name.endsWith('_apply_transactional')), false);
});

test('paging and action limits are enforced before the database call', async () => {
  const current = scenario();
  const response = await current.dispatcher(
    jsonRequest('GET', '/api/import-reviews?page_size=101'),
    { TEST: true },
    current.ctx
  );
  assert.equal(response.status, 400);
  assert.equal(current.calls.some((entry) => entry.name === 'import_review_list_v1'), false);
});

test('apply-status rejects a database request-hash mismatch instead of wrapping it as success', async () => {
  const calls = [];
  const dispatcher = createImportReviewDispatcher({
    requireUser: async () => ({ id: ACTOR_ID, role: 'admin' }),
    sbRpc: async (_env, name, args) => {
      calls.push({ name, args });
      if (name === 'import_review_contract_version_get_v1') return installedContract;
      return { ok: false, status: 'OPERATION_REQUEST_MISMATCH' };
    }
  });
  const response = await dispatcher(jsonRequest(
    'GET',
    `/api/import-reviews/${IMPORT_ID}/apply-status?operation_id=${OPERATION_ID}&request_hash=${HASH}`
  ), {}, {});
  const captured = await responseJson(response);
  assert.equal(captured.status, 409);
  assert.equal(captured.body.error.code, 'IMPORT_REVIEW_OPERATION_REQUEST_MISMATCH');
  assert.equal(calls.at(-1).args.p_request_hash, HASH);
});

test('a committed apply recovered after timeout still starts idempotent post-commit follow-up', async () => {
  const followUps = [];
  const pending = [];
  const dispatcher = createImportReviewDispatcher({
    requireUser: async () => ({ id: ACTOR_ID, role: 'admin' }),
    sbRpc: async (_env, name) => {
      if (name === 'import_review_contract_version_get_v1') return installedContract;
      if (name === 'import_review_get_v1') return readyReview();
      if (name === 'nhsp_weekly_apply_transactional') {
        const error = new Error('RPC timed out');
        error.status = 408;
        throw error;
      }
      if (name === 'import_review_apply_status_get_v1') {
        return {
          ok: true,
          outcome: 'COMMITTED_WITH_FOLLOW_UP_PENDING',
          stored_response: { affected_timesheet_ids: [TIMESHEET_ID], post_commit_email_action_ids: [] }
        };
      }
      throw new Error(`unexpected RPC ${name}`);
    },
    runFollowUp: async (_env, details) => { followUps.push(details); }
  });
  const response = await dispatcher(jsonRequest(
    'POST',
    `/api/import-reviews/${IMPORT_ID}/apply`,
    { operation_id: OPERATION_ID, expected_state_version: 7, expected_request_hash: HASH }
  ), {}, { waitUntil(promise) { pending.push(Promise.resolve(promise)); } });
  const captured = await responseJson(response);
  assert.equal(captured.status, 200);
  assert.equal(captured.body.data.recovered_from_unknown_outcome, true);
  assert.equal(captured.body.data.follow_up_started, true);
  await Promise.all(pending);
  assert.equal(followUps.length, 1);
  assert.equal(followUps[0].requestHash, HASH);
  assert.equal(followUps[0].applyResult.affected_timesheet_ids[0], TIMESHEET_ID);
});

test('the known Supabase plpgsql_check failure is status-checked and retried once only when not started', async () => {
  let applyCalls = 0;
  let statusCalls = 0;
  const followUps = [];
  const dispatcher = createImportReviewDispatcher({
    requireUser: async () => ({ id: ACTOR_ID, role: 'admin' }),
    sbRpc: async (_env, name) => {
      if (name === 'import_review_contract_version_get_v1') return installedContract;
      if (name === 'import_review_get_v1') return readyReview();
      if (name === 'nhsp_weekly_apply_transactional') {
        applyCalls += 1;
        if (applyCalls === 1) {
          const error = new Error('cannot find parent statement on pldbgapi2 call stack');
          error.status = 500;
          error.json = { code: 'XX000', message: error.message };
          throw error;
        }
        return { ok: true, affected_timesheet_ids: [TIMESHEET_ID], post_commit_email_action_ids: [] };
      }
      if (name === 'import_review_apply_status_get_v1') {
        statusCalls += 1;
        return { ok: true, outcome: 'NOT_STARTED' };
      }
      throw new Error(`unexpected RPC ${name}`);
    },
    runFollowUp: async (_env, details) => { followUps.push(details); }
  });

  const response = await dispatcher(jsonRequest(
    'POST',
    `/api/import-reviews/${IMPORT_ID}/apply`,
    { operation_id: OPERATION_ID, expected_state_version: 7, expected_request_hash: HASH }
  ), {}, {});
  const captured = await responseJson(response);
  assert.equal(captured.status, 200);
  assert.equal(captured.body.data.recovered_from_database_interruption, true);
  assert.equal(applyCalls, 2);
  assert.equal(statusCalls, 1);
  assert.equal(followUps.length, 1);
});

test('the known Supabase plpgsql_check failure never retries when status proves the operation committed', async () => {
  let applyCalls = 0;
  let statusCalls = 0;
  const dispatcher = createImportReviewDispatcher({
    requireUser: async () => ({ id: ACTOR_ID, role: 'admin' }),
    sbRpc: async (_env, name) => {
      if (name === 'import_review_contract_version_get_v1') return installedContract;
      if (name === 'import_review_get_v1') return readyReview();
      if (name === 'nhsp_weekly_apply_transactional') {
        applyCalls += 1;
        const error = new Error('cannot find parent statement on pldbgapi2 call stack');
        error.status = 500;
        error.json = { code: 'XX000', message: error.message };
        throw error;
      }
      if (name === 'import_review_apply_status_get_v1') {
        statusCalls += 1;
        return {
          ok: true,
          outcome: 'COMMITTED_APPLIED',
          stored_response: { ok: true, affected_timesheet_ids: [TIMESHEET_ID], post_commit_email_action_ids: [] }
        };
      }
      throw new Error(`unexpected RPC ${name}`);
    }
  });

  const response = await dispatcher(jsonRequest(
    'POST',
    `/api/import-reviews/${IMPORT_ID}/apply`,
    { operation_id: OPERATION_ID, expected_state_version: 7, expected_request_hash: HASH }
  ), {}, {});
  const captured = await responseJson(response);
  assert.equal(captured.status, 200);
  assert.equal(captured.body.data.recovered_from_database_interruption, true);
  assert.equal(applyCalls, 1);
  assert.equal(statusCalls, 1);
});

test('a generic database 500 is not automatically retried as the Supabase platform failure', async () => {
  let applyCalls = 0;
  let statusCalls = 0;
  const dispatcher = createImportReviewDispatcher({
    requireUser: async () => ({ id: ACTOR_ID, role: 'admin' }),
    sbRpc: async (_env, name) => {
      if (name === 'import_review_contract_version_get_v1') return installedContract;
      if (name === 'import_review_get_v1') return readyReview();
      if (name === 'nhsp_weekly_apply_transactional') {
        applyCalls += 1;
        const error = new Error('generic upstream failure');
        error.status = 500;
        throw error;
      }
      if (name === 'import_review_apply_status_get_v1') statusCalls += 1;
      throw new Error(`unexpected RPC ${name}`);
    }
  });

  const response = await dispatcher(jsonRequest(
    'POST',
    `/api/import-reviews/${IMPORT_ID}/apply`,
    { operation_id: OPERATION_ID, expected_state_version: 7, expected_request_hash: HASH }
  ), {}, {});
  const captured = await responseJson(response);
  assert.equal(captured.status, 502);
  assert.equal(applyCalls, 1);
  assert.equal(statusCalls, 0);
});

test('a transient read failure is retried once without retrying any mutation RPC', async () => {
  let getCalls = 0;
  const dispatcher = createImportReviewDispatcher({
    requireUser: async () => ({ id: ACTOR_ID, role: 'admin' }),
    sbRpc: async (_env, name) => {
      if (name === 'import_review_contract_version_get_v1') return installedContract;
      if (name === 'import_review_get_v1') {
        getCalls += 1;
        if (getCalls === 1) {
          const error = new Error('transient upstream failure');
          error.status = 502;
          throw error;
        }
        return readyReview();
      }
      throw new Error(`unexpected RPC ${name}`);
    }
  });

  const response = await dispatcher(jsonRequest('GET', `/api/import-reviews/${IMPORT_ID}`), {}, {});
  const captured = await responseJson(response);
  assert.equal(captured.status, 200);
  assert.equal(captured.body.data.state.status, 'READY');
  assert.equal(getCalls, 2);
});

test('refresh retries once after the exact Supabase plpgsql_check transaction abort', async () => {
  let refreshCalls = 0;
  const dispatcher = createImportReviewDispatcher({
    requireUser: async () => ({ id: ACTOR_ID, role: 'admin' }),
    sbRpc: async (_env, name) => {
      if (name === 'import_review_contract_version_get_v1') return installedContract;
      if (name === 'import_review_refresh_v1') {
        refreshCalls += 1;
        if (refreshCalls === 1) {
          const error = new Error('cannot find parent statement on pldbgapi2 call stack');
          error.status = 500;
          error.json = { code: 'XX000', message: error.message };
          throw error;
        }
        return { ok: true, status: 'READY', state_version: 8 };
      }
      throw new Error(`unexpected RPC ${name}`);
    }
  });

  const response = await dispatcher(jsonRequest(
    'POST',
    `/api/import-reviews/${IMPORT_ID}/refresh`,
    { expected_state_version: 7, max_actions: 500 }
  ), {}, {});
  const captured = await responseJson(response);
  assert.equal(captured.status, 200);
  assert.equal(captured.body.data.state_version, 8);
  assert.equal(refreshCalls, 2);
});

test('contract query-email override is normalized and cannot be enabled without an address', () => {
  assert.deepEqual(normalizeContractQueryEmailOverride({
    send_ts_queries_to_different_email: true,
    ts_queries_alt_email_address: '  Rota.Team@Example.COM '
  }), {
    send_ts_queries_to_different_email: true,
    ts_queries_alt_email_address: 'rota.team@example.com'
  });
  assert.throws(
    () => normalizeContractQueryEmailOverride({ send_ts_queries_to_different_email: true }),
    /required/
  );
  assert.deepEqual(normalizeContractQueryEmailOverride({
    send_ts_queries_to_different_email: false,
    ts_queries_alt_email_address: 'unused@example.com'
  }), {
    send_ts_queries_to_different_email: false,
    ts_queries_alt_email_address: null
  });
});

test('source evidence hashing is deterministic and bounded before reading the object body', async () => {
  const bytes = new TextEncoder().encode('same import evidence');
  let reads = 0;
  const env = {
    FILE_MAX_BYTES: '1024',
    R2: {
      async get() {
        return { size: bytes.byteLength, async arrayBuffer() { reads += 1; return bytes.buffer; } };
      }
    }
  };
  const first = await importReviewSourceEvidenceFromR2(env, 'files/import.xlsx', 'PARSER_V1');
  const second = await importReviewSourceEvidenceFromR2(env, 'files/import.xlsx', 'PARSER_V1');
  assert.equal(first.source_file_sha256, second.source_file_sha256);
  assert.equal(first.source_file_sha256.length, 64);
  assert.equal(first.source_file_size_bytes, bytes.byteLength);
  assert.equal(reads, 2);

  let oversizedRead = false;
  await assert.rejects(() => importReviewSourceEvidenceFromR2({
    FILE_MAX_BYTES: '10',
    R2: {
      async get() {
        return { size: 11, async arrayBuffer() { oversizedRead = true; return new ArrayBuffer(11); } };
      }
    }
  }, 'files/large.xlsx', 'PARSER_V1'), /IMPORT_SOURCE_FILE_TOO_LARGE/);
  assert.equal(oversizedRead, false);
});
