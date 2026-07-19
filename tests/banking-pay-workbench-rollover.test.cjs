const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const workerSource = fs.readFileSync(
  path.resolve(__dirname, '../broker/src/index.js'),
  'utf8'
);

const SOURCE_SESSION_ID = '33297ecf-1111-4111-8111-111111111111';
const TARGET_SESSION_ID = '2eb92bd1-2222-4222-8222-222222222222';
const ACTOR_USER_ID = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
const ROOT_JOB_ID = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb';

function asyncFunctionSource(name, nextName) {
  const start = workerSource.indexOf(`async function ${name}`);
  assert.ok(start >= 0, `${name} must exist`);
  const end = workerSource.indexOf(`async function ${nextName}`, start + 1);
  assert.ok(end > start, `${nextName} must follow ${name}`);
  return workerSource.slice(start, end);
}

function functionSourceBetween(startMarker, endMarker) {
  const start = workerSource.indexOf(startMarker);
  assert.ok(start >= 0, `${startMarker} must exist`);
  const end = workerSource.indexOf(endMarker, start + 1);
  assert.ok(end > start, `${endMarker} must follow ${startMarker}`);
  return workerSource.slice(start, end);
}

function loadRolloverFunctions(overrides = {}) {
  const ensureSource = asyncFunctionSource(
    'ensureCurrentBankingPayWorkbenchSession',
    'bankingPayWorkbenchPayDateRolloverTick'
  );
  const rolloverSource = functionSourceBetween(
    'async function bankingPayWorkbenchPayDateRolloverTick',
    'function logBankingPayWorkbenchDiag'
  );
  const context = vm.createContext({
    console,
    setTimeout,
    clearTimeout,
    ...overrides
  });

  new vm.Script(`${ensureSource}\n${rolloverSource}\nthis.__rollover = { ensureCurrentBankingPayWorkbenchSession, bankingPayWorkbenchPayDateRolloverTick };`).runInContext(context);
  return context.__rollover;
}

function stripOpenOnlyMetadata(filters) {
  const copy = JSON.parse(JSON.stringify(filters || {}));
  delete copy.open_options;
  delete copy.options;
  return copy;
}

test('midnight pay-date rollover preserves canonical scope markers and queues clone/rebase', async () => {
  const sourceFilters = {
    scope_is_row_backed: true,
    scope_seed_source: 'pay_preview_build_context.PAGE',
    scope_count_unknown: true,
    canonical_scope_signature_version: 4
  };
  let capturedOpenArgs = null;
  const diagnosticEvents = [];

  const sbFetch = async (_env, url) => {
    if (url.includes('/rest/v1/tms_users?')) {
      return { rows: [{ id: ACTOR_USER_ID, is_active: true }] };
    }
    if (url.includes('/rest/v1/banking_pay_workbench_sessions?')) {
      return {
        rows: [{
          id: SOURCE_SESSION_ID,
          actor_user_id: ACTOR_USER_ID,
          pay_date: '2026-07-17',
          week_ending_cutoff: '9999-12-31',
          filters_json: sourceFilters,
          session_signature: 'previous-pay-date-signature',
          status: 'OPEN',
          discarded_at_utc: null,
          progress_state: 'ERROR',
          progress_json: {},
          updated_at_utc: '2026-07-17T22:59:59.000Z',
          version: 1
        }]
      };
    }
    throw new Error(`Unexpected TEST URL: ${url}`);
  };

  const sbRpc = async (_env, rpcName, args) => {
    if (rpcName === 'pay_workbench_session_open_shared_v2') {
      capturedOpenArgs = JSON.parse(JSON.stringify(args));
      const targetFilters = stripOpenOnlyMetadata(args.p_filters_json);
      const sourceMatchesTarget = JSON.stringify(targetFilters) === JSON.stringify(sourceFilters);

      if (!sourceMatchesTarget) {
        return {
          session_id: TARGET_SESSION_ID,
          action: 'WORKBENCH_SESSION_CREATED',
          created: true,
          root_job_id: ROOT_JOB_ID,
          root_job_type: 'WORKBENCH_SESSION_SCOPE_SEED',
          clone_source_selection_attempted: true,
          clone_source_selection_result: 'SOURCE_SCOPE_MISMATCH',
          clone_rebase_attempted: false,
          clone_rebase_fallback_reason: 'SOURCE_SCOPE_MISMATCH',
          clone_rebase_enabled: true,
          work_queued: true
        };
      }

      return {
        session_id: TARGET_SESSION_ID,
        action: 'WORKBENCH_SESSION_CREATED',
        created: true,
        root_job_id: ROOT_JOB_ID,
        root_job_type: 'WORKBENCH_SESSION_CLONE_REBASE',
        clone_source_selection_attempted: true,
        clone_source_selection_result: 'EXPLICIT_SOURCE_SELECTED',
        clone_from_session_id: SOURCE_SESSION_ID,
        clone_source_explicit: true,
        clone_rebase_attempted: true,
        clone_rebase_enabled: true,
        clone_rebase_queued: true,
        work_queued: true
      };
    }

    if (rpcName === 'pay_workbench_session_get_progress_light') {
      return {
        session_id: TARGET_SESSION_ID,
        progress_state: 'CLONING_PREVIOUS_PAY_DATE',
        session_ready: false,
        work_queued: true,
        still_running: true,
        scope_pending_count: 2,
        job_counts: { queued: 1, running: 0 }
      };
    }

    throw new Error(`Unexpected TEST RPC: ${rpcName}`);
  };

  const { bankingPayWorkbenchPayDateRolloverTick } = loadRolloverFunctions({
    sbFetch,
    sbRpc,
    resolveBankingPayOfficialDateContext: async () => ({
      current_official_pay_date: '2026-07-24',
      next_official_pay_date: '2026-07-31',
      business_date: '2026-07-18',
      configuration_fingerprint: 'test-rollover-fixture'
    }),
    nudgeBankingPayWorkbenchDrain: () => ({
      ok: true,
      scheduled: true,
      reason: 'TEST_CLONE_REBASE_QUEUED'
    }),
    logBankingPayWorkbenchDiag: (_env, eventName, payload) => {
      diagnosticEvents.push({ eventName, payload });
    }
  });

  const result = await bankingPayWorkbenchPayDateRolloverTick(
    { SUPABASE_URL: 'https://test.invalid' },
    {
      now: '2026-07-17T23:00:54.000Z',
      maxSessions: 3,
      scanLimit: 25,
      ctx: { waitUntil() {} }
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.current_pay_date, '2026-07-24');
  assert.equal(result.created_count, 1);
  assert.equal(
    result.clone_rebase_queued_count,
    1,
    JSON.stringify({ clone_rebase: result.clone_rebase, sessions: result.sessions })
  );
  assert.equal(result.scope_seed_fallback_count, 0);
  assert.deepEqual(Object.keys(result.fallback_reason_counts), []);
  assert.equal(result.sessions[0].root_job_type, 'WORKBENCH_SESSION_CLONE_REBASE');
  assert.equal(result.sessions[0].clone_source_selection_result, 'EXPLICIT_SOURCE_SELECTED');
  assert.equal(result.sessions[0].clone_from_session_id, SOURCE_SESSION_ID);

  assert.ok(capturedOpenArgs, 'shared-open RPC must be called');
  assert.equal(capturedOpenArgs.p_pay_date, '2026-07-24');
  assert.equal(capturedOpenArgs.p_filters_json.scope_is_row_backed, true);
  assert.equal(capturedOpenArgs.p_filters_json.scope_seed_source, 'pay_preview_build_context.PAGE');
  assert.equal(capturedOpenArgs.p_filters_json.scope_count_unknown, true);
  assert.equal(capturedOpenArgs.p_filters_json.canonical_scope_signature_version, 4);
  assert.equal(capturedOpenArgs.p_filters_json.open_options.clone_from_session_id, SOURCE_SESSION_ID);
  assert.equal(capturedOpenArgs.p_filters_json.open_options.allow_session_rebase, true);

  assert.ok(
    diagnosticEvents.some((event) => event.eventName === 'WORKBENCH_PAY_DATE_ROLLOVER_TICK_END'),
    'rollover completion must remain observable'
  );
});

test('rollover-only fix leaves payment execution and post-draft paths outside its boundary', () => {
  const ensureBody = asyncFunctionSource(
    'ensureCurrentBankingPayWorkbenchSession',
    'bankingPayWorkbenchPayDateRolloverTick'
  );
  const rolloverBody = functionSourceBetween(
    'async function bankingPayWorkbenchPayDateRolloverTick',
    'function logBankingPayWorkbenchDiag'
  );
  const combined = `${ensureBody}\n${rolloverBody}`;

  assert.match(combined, /policy_x_authority_scope: 'PRE_DRAFT_SHARED_WORKBENCH_OPEN'/);
  assert.doesNotMatch(combined, /pay_batch_execute|pay_batch_settle|provider_submission|remittance_send/);
});
