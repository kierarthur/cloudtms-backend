import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  dispatchWeeklySourceRequest,
  WEEKLY_SOURCE_OFFICE_ROUTE_CONTRACT,
} from '../../broker/src/weekly-source/routes.js';

const ACTOR_ID = '81000000-0000-4000-8000-000000000001';
const TIMESHEET_ID = '81000000-0000-4000-8000-000000000002';
const REPOSITORY_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');

const officeDependencies = (overrides = {}) => ({
  requireUser: async () => ({ id: ACTOR_ID, role: 'admin' }),
  rpc: async (name, args) => ({ ok: true, name, args }),
  ...overrides,
});

const jsonRequest = (path, body, method = 'POST') => new Request(`https://test.invalid${path}`, {
  method,
  headers: { 'content-type': 'application/json' },
  ...(method === 'GET' ? {} : { body: JSON.stringify(body) }),
});

test('ignores requests outside the single Weekly Source prefix', async () => {
  const response = await dispatchWeeklySourceRequest(
    jsonRequest('/api/timesheets', null, 'GET'),
    {},
    {},
    officeDependencies(),
  );
  assert.equal(response, null);
});

test('workspace read injects the authenticated Office actor and never trusts a query actor', async () => {
  const calls = [];
  const response = await dispatchWeeklySourceRequest(
    jsonRequest('/api/weekly-source/v1/workspace?actor_user_id=bad&cycle=2026-09-13', null, 'GET'),
    {},
    {},
    officeDependencies({ rpc: async (...args) => { calls.push(args); return { ok: true }; } }),
  );
  assert.equal(response.status, 200);
  assert.equal(calls.length, 1);
  assert.equal(calls[0][0], 'weekly_source_office_workspace_v1');
  assert.deepEqual(calls[0][1], {
    p_request: { actor_user_id: ACTOR_ID, cycle: '2026-09-13' },
  });
});

test('Office notifications are read through their bounded owner with server actor identity', async () => {
  const calls = [];
  const response = await dispatchWeeklySourceRequest(
    jsonRequest('/api/weekly-source/v1/notifications?limit=25&open_only=false', null, 'GET'),
    {},
    {},
    officeDependencies({ rpc: async (...args) => { calls.push(args); return { ok: true, notifications: [] }; } }),
  );
  assert.equal(response.status, 200);
  assert.equal(calls.length, 1);
  assert.equal(calls[0][0], 'weekly_source_office_notifications_list_v1');
  assert.deepEqual(calls[0][1], {
    p_request: { actor_user_id: ACTOR_ID, limit: 25, open_only: false },
  });
});

test('Office notification query refuses unknown fields and cannot accept a browser actor', async () => {
  const response = await dispatchWeeklySourceRequest(
    jsonRequest('/api/weekly-source/v1/notifications?actor_user_id=bad', null, 'GET'),
    {},
    {},
    officeDependencies(),
  );
  assert.equal(response.status, 400);
  assert.equal((await response.json()).error_code, 'WEEKLY_SOURCE_NOTIFICATIONS_QUERY_INVALID');
});

test('upload preview delegates parsing to the server-owned context resolver', async () => {
  const calls = [];
  const response = await dispatchWeeklySourceRequest(
    jsonRequest('/api/weekly-source/v1/uploads/preview', {
      file_key: 'weekly-source/test.xlsx',
      source_group_id: '82000000-0000-4000-8000-000000000001',
      source_cycle_id: '82000000-0000-4000-8000-000000000002',
      profile_id: 'NHSP_FINAL_BACKING_V1',
      parser_options: { profileId: 'NHSP_FINAL_BACKING_V1' },
    }),
    {},
    {},
    officeDependencies({
      loadFileBytes: async () => new Uint8Array([1, 2, 3]),
      previewUpload: async (input) => {
        calls.push(input);
        return {
          parsed: { ok: true, profileId: 'NHSP_FINAL_BACKING_V1' },
          accept_context: { report_scope_id: '82000000-0000-4000-8000-000000000003' },
        };
      },
      recordUploadPreview: async () => assert.fail('legacy preview recorder must not run'),
    }),
  );
  assert.equal(response.status, 200);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].actor.id, ACTOR_ID);
  assert.equal(calls[0].body.file_key, 'weekly-source/test.xlsx');
  assert.equal(calls[0].bytes.byteLength, 3);
  const payload = await response.json();
  assert.equal(payload.preview.profileId, 'NHSP_FINAL_BACKING_V1');
  assert.equal(payload.accept_context.report_scope_id, '82000000-0000-4000-8000-000000000003');
});

test('timesheet presentation accepts only a canonical UUID and injects the Office actor', async () => {
  const calls = [];
  const response = await dispatchWeeklySourceRequest(
    jsonRequest(`/api/weekly-source/v1/timesheets/${TIMESHEET_ID}/presentation`, null, 'GET'),
    {},
    {},
    officeDependencies({ rpc: async (...args) => { calls.push(args); return { ok: true }; } }),
  );
  assert.equal(response.status, 200);
  assert.equal(calls[0][0], 'weekly_source_office_timesheet_presentation_v1');
  assert.deepEqual(calls[0][1], {
    p_request: { actor_user_id: ACTOR_ID, timesheet_id: TIMESHEET_ID },
  });
});

test('finalisation overwrites a caller-supplied actor and uses only the server-owned orchestrator', async () => {
  const calls = [];
  const response = await dispatchWeeklySourceRequest(
    jsonRequest('/api/weekly-source/v1/commands', {
      action: 'FINALISE_WEEK',
      payload: {
        actor_user_id: '82000000-0000-4000-8000-000000000001',
        source_cycle_id: '82000000-0000-4000-8000-000000000002',
      },
    }),
    {},
    {},
    officeDependencies({
      rpc: async () => assert.fail('finalisation must not use a browser-selected direct RPC'),
      orchestrateFinalisation: async (input) => {
        calls.push(input);
        return { ok: true, status: 'FINALISED' };
      },
    }),
  );
  assert.equal(response.status, 200);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].request.actor_user_id, ACTOR_ID);
  assert.equal(calls[0].actor.id, ACTOR_ID);
  assert.deepEqual(await response.json(), { ok: true, status: 'FINALISED' });
});

test('finalised-pay recovery is a separate explicit action and cannot be inferred from finalisation replay', async () => {
  const calls = [];
  const payload = {
    final_revision_id: '82000000-0000-4000-8000-000000000001',
    run_id: '82000000-0000-4000-8000-000000000002',
    task_id: '82000000-0000-4000-8000-000000000003',
    expected_task_version: 4,
    confirm_retry: false,
  };
  const response = await dispatchWeeklySourceRequest(
    jsonRequest('/api/weekly-source/v1/commands', {
      action: 'RECOVER_FINALISED_PAY',
      payload,
    }),
    {},
    {},
    officeDependencies({
      rpc: async () => assert.fail('recovery must not use a browser-selected direct RPC'),
      recoverFinalisedPay: async (input) => {
        calls.push(input);
        return { ok: true, status: 'FINALISED_PAY_RECOVERY_REQUIRED' };
      },
    }),
  );
  assert.equal(response.status, 200);
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].request, { ...payload, actor_user_id: ACTOR_ID });
  assert.equal(calls[0].actor.id, ACTOR_ID);
});

test('group outreach and exact-group acceptance use one server-owned all-or-nothing bulk transaction', async () => {
  for (const action of ['ASK_CANDIDATES', 'SEND_MANAGER_NOW', 'ACCEPT_SYSTEM_HOURS']) {
    const calls = [];
    const response = await dispatchWeeklySourceRequest(
      jsonRequest('/api/weekly-source/v1/commands', {
        action,
        payload: {
          source_cycle_id: '82000000-0000-4000-8000-000000000002',
          projection_publication_id: '82000000-0000-4000-8000-000000000003',
          expected_workspace_version: 'workspace-version-1',
          selection: {
            mode: 'ALL_FILTERED',
            group_keys: [],
            excluded_group_keys: [],
            filters: { status: 'UNRESOLVED' },
            sort_key: 'candidate',
            sort_direction: 'asc',
          },
        },
      }),
      {},
      {},
      officeDependencies({ rpc: async (...args) => { calls.push(args); return { ok: true }; } }),
    );
    assert.equal(response.status, 200);
    assert.equal(calls.length, 1);
    assert.equal(calls[0][0], 'weekly_source_office_bulk_query_action_atomic_v1');
    assert.equal(calls[0][1].p_request.action, action);
    assert.equal(calls[0][1].p_request.actor_user_id, ACTOR_ID);
    assert.equal(calls[0][1].p_request.expected_workspace_version, 'workspace-version-1');
  }
});

test('no-shifts attestation reaches only its guarded service RPC with the Office actor', async () => {
  const calls = [];
  const payload = {
    source_cycle_id: '82000000-0000-4000-8000-000000000002',
    source_group_id: '82000000-0000-4000-8000-000000000003',
    client_id: '82000000-0000-4000-8000-000000000004',
    expected_cycle_version: 4,
    attestation_text: 'I confirm there are no shifts to import for this week.',
  };
  const response = await dispatchWeeklySourceRequest(
    jsonRequest('/api/weekly-source/v1/commands', {
      action: 'NO_SHIFTS_TO_IMPORT',
      payload,
    }),
    {},
    {},
    officeDependencies({ rpc: async (...args) => { calls.push(args); return { ok: true }; } }),
  );
  assert.equal(response.status, 200);
  assert.deepEqual(calls, [[
    'weekly_source_no_shifts_attest_atomic_v1',
    { p_request: { ...payload, actor_user_id: ACTOR_ID } },
    { timeoutMs: 45_000 },
  ]]);
});

test('NHSP rate-warning acceptance sends only opaque warning keys and the server proof', async () => {
  const calls = [];
  const payload = {
    source_cycle_id: '82000000-0000-4000-8000-000000000002',
    projection_publication_id: '82000000-0000-4000-8000-000000000003',
    warning_keys: ['all-zero-source-charge', 'charge-check:82000000-0000-4000-8000-000000000004'],
    selection_proof: 'a'.repeat(64),
  };
  const response = await dispatchWeeklySourceRequest(
    jsonRequest('/api/weekly-source/v1/commands', {
      action: 'ACCEPT_NHSP_SOURCE_CHARGES',
      payload,
    }),
    {},
    {},
    officeDependencies({ rpc: async (...args) => { calls.push(args); return { ok: true }; } }),
  );
  assert.equal(response.status, 200);
  assert.deepEqual(calls, [[
    'weekly_source_charge_accept_atomic_v1',
    { p_request: { ...payload, actor_user_id: ACTOR_ID } },
    { timeoutMs: 45_000 },
  ]]);
});

test('browser commands cannot submit pay, charge, residual, C1 or target facts', async () => {
  for (const payload of [
    { pay_amount: '100.00' },
    { nested: { charge_ex_vat: '200.00' } },
    { c1_components: [] },
    { target_snapshot: {} },
  ]) {
    const response = await dispatchWeeklySourceRequest(
      jsonRequest('/api/weekly-source/v1/commands', { action: 'FINALISE_WEEK', payload }),
      {},
      {},
      officeDependencies(),
    );
    const body = await response.json();
    assert.equal(response.status, 400);
    assert.equal(body.error_code, 'WEEKLY_SOURCE_BROWSER_FINANCIAL_FACT_FORBIDDEN');
  }
});

test('protected-hours actions reach only the server-owned orchestration dependency', async () => {
  const calls = [];
  const response = await dispatchWeeklySourceRequest(
    jsonRequest('/api/weekly-source/v1/commands', {
      action: 'APPROVE_PROTECTED_HOURS',
      payload: {
        source_cycle_id: '82000000-0000-4000-8000-000000000002',
        work_date: '2026-09-01',
        start: '09:00',
        end: '17:00',
        break_minutes: 30,
        reason: 'Candidate confirmed the worked shift.',
        idempotency_key: 'weekly-source-test-key-0001',
      },
    }),
    {},
    { waitUntil() {} },
    officeDependencies({
      rpc: async () => assert.fail('protected actions must not call a browser-selected RPC'),
      orchestrateProtectedAction: async (input) => {
        calls.push(input);
        return { ok: true, outcome: 'PUBLISHED' };
      },
    }),
  );
  assert.equal(response.status, 200);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].action, 'APPROVE_PROTECTED_HOURS');
  assert.equal(calls[0].request.actor_user_id, ACTOR_ID);
  assert.deepEqual(await response.json(), { ok: true, outcome: 'PUBLISHED' });
});

test('a saved unknown C1 outcome is returned as a recoverable conflict', async () => {
  const response = await dispatchWeeklySourceRequest(
    jsonRequest('/api/weekly-source/v1/commands', {
      action: 'ACCEPT_SOURCE_AND_RECONCILE',
      payload: {},
    }),
    {},
    {},
    officeDependencies({
      orchestrateProtectedAction: async () => {
        throw Object.assign(new Error('The saved result must be resolved before continuing.'), {
          code: 'C1_DURABLE_RECOVERY_REQUIRED',
        });
      },
    }),
  );
  assert.equal(response.status, 409);
  assert.equal((await response.json()).error_code, 'C1_DURABLE_RECOVERY_REQUIRED');
});

test('unsupported and unavailable commands fail closed', async () => {
  const unknown = await dispatchWeeklySourceRequest(
    jsonRequest('/api/weekly-source/v1/commands', { action: 'DELETE_EVERYTHING', payload: {} }),
    {},
    {},
    officeDependencies(),
  );
  assert.equal(unknown.status, 400);
  assert.equal((await unknown.json()).error_code, 'WEEKLY_SOURCE_COMMAND_NOT_SUPPORTED');

  const protectedUnavailable = await dispatchWeeklySourceRequest(
    jsonRequest('/api/weekly-source/v1/commands', {
      action: 'RECORD_NOT_WORKED',
      payload: {},
    }),
    {},
    {},
    officeDependencies(),
  );
  assert.equal(protectedUnavailable.status, 503);
  assert.equal((await protectedUnavailable.json()).error_code, 'WEEKLY_SOURCE_PROTECTED_ACTION_UNAVAILABLE');
});

test('the exported route contract is narrow and contains no Banking route', () => {
  assert.equal(WEEKLY_SOURCE_OFFICE_ROUTE_CONTRACT.prefix, '/api/weekly-source/v1');
  assert.equal(WEEKLY_SOURCE_OFFICE_ROUTE_CONTRACT.browserFinancialFactsAccepted, false);
  assert.equal(JSON.stringify(WEEKLY_SOURCE_OFFICE_ROUTE_CONTRACT).includes('/api/banking/'), false);
});

test('the Worker mounts the single route before legacy import review and outside Banking Pay', () => {
  const worker = fs.readFileSync(path.join(REPOSITORY_ROOT, 'broker/src/index.js'), 'utf8');
  assert.match(worker, /import \{ dispatchWeeklySourceRequest \} from '\.\/weekly-source\/routes\.js';/);
  const mount = worker.indexOf('const weeklySourceResponse = await dispatchWeeklySourceRequest');
  const legacy = worker.indexOf('const importReviewResponse = await dispatchImportReviewRequest');
  const banking = worker.indexOf("if (p.startsWith('/api/banking/'))");
  assert.ok(mount > 0 && legacy > mount);
  assert.ok(banking > mount);
  assert.equal(worker.slice(mount, legacy).includes('orchestrateProtectedAction:'), true);
  assert.equal(worker.slice(mount, legacy).includes('calculateWeeklyProtectedSnapshot'), true);
  assert.equal(worker.slice(mount, legacy).includes('createWeeklySourceC1RawRpc'), true);
  assert.equal(worker.slice(mount, legacy).includes('orchestrateFinalisation:'), true);
  assert.equal(worker.slice(mount, legacy).includes('recoverFinalisedPay:'), true);
  assert.equal(worker.slice(mount, legacy).includes('orchestrateWeeklySourceFinalisation'), true);
  assert.equal(worker.slice(mount, legacy).includes('recoverWeeklySourceFinalisationPayProjection'), true);
  assert.equal(worker.slice(mount, legacy).includes('previewUpload:'), true);
  assert.equal(worker.slice(mount, legacy).includes('recordUploadPreview:'), true);
  assert.equal(worker.slice(mount, legacy).includes('acceptUpload:'), true);
  assert.match(worker, /createWeeklySourceUploadPublicationOwner\s*\(/);
  assert.equal(/pay_workbench|pay_batch|pay_execute/i.test(worker.slice(mount, legacy)), false);
  assert.equal(worker.slice(mount, legacy).includes('previewCorrectFinalSource:'), true);
  assert.equal(worker.slice(mount, legacy).includes('applyCorrectFinalSource:'), true);
  assert.equal(worker.slice(mount, legacy).includes('orchestrateWeeklyCorrectFinalPreview'), true);
  assert.equal(worker.slice(mount, legacy).includes('orchestrateWeeklyCorrectFinalApply'), true);
  assert.equal(worker.slice(mount, legacy).includes('stageReplacementSource:'), true);
  assert.equal(worker.slice(mount, legacy).includes('rebuildReplacementProjection:'), true);
  assert.equal(worker.slice(mount, legacy).includes('buildWeeklyCorrectFinalServiceSnapshot'), true);
  const builder = worker.slice(
    worker.indexOf('async function buildWeeklyCorrectFinalServiceSnapshot'),
    worker.indexOf('function officeAuthRefreshRejected'),
  );
  assert.match(builder, /tsfin_load_context_batch/);
  assert.match(builder, /tsfin_load_weekly_context_batch/);
  assert.match(builder, /buildWeeklyScheduleSegmentsSnapshot/);
  assert.match(builder, /write_now:\s*false/);
  assert.match(builder, /ignore_locked_segments_for_preview:\s*true/);
  assert.equal(/pay_workbench|pay_batch|pay_execute|invoice_lines/i.test(builder), false);
});
