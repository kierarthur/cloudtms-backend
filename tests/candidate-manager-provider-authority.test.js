import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import { candidateManagerProviderAuthorityCurrent } from '../broker/src/candidate-manager-provider-authority.js';

const workflowId = 'b5500000-0000-4000-8000-000000000001';
const requestId = 'b5500000-0000-4000-8000-000000000002';
const outboxId = 'b5500000-0000-4000-8000-000000000003';
const lease = 'manager-provider-lease';
const now = '2026-08-11T12:00:00.000Z';

function claimedRow(scopePatch = {}) {
  return {
    id: outboxId,
    payment_scope_json: {
      candidate_mail_authority: 'MANAGER_APPROVAL_V1',
      candidate_manager_mail_kind: 'INITIAL',
      candidate_manager_workflow_id: workflowId,
      candidate_manager_workflow_generation: 4,
      candidate_approval_request_id: requestId,
      candidate_approval_request_generation: 2,
      candidate_manager_mail_retired: false,
      ...scopePatch
    }
  };
}

function fetchFixture({ environment = 'TEST', rpcOk = true, rpcResult = null } = {}) {
  const calls = [];
  const fetchImpl = async (url, options = {}) => {
    calls.push({ url: String(url), options });
    if (String(url).includes('/settings_defaults?')) {
      return {
        ok: true,
        async json() { return [{ candidate_app_environment: environment }]; }
      };
    }
    assert.match(String(url), /\/rpc\/candidate_workflow_transition_atomic_v1$/);
    return {
      ok: rpcOk,
      async json() {
        return rpcResult ?? {
          ok: true,
          workflow_id: workflowId,
          generation: 4,
          approval_request_id: requestId,
          approval_request_generation: 2,
          approval_workflow_generation: 4,
          mail_outbox_id: outboxId,
          manager_mail_kind: 'INITIAL',
          provider_submit_permit: true,
          provider_submit_permit_expires_at_utc: '2026-08-11T12:15:00.000Z'
        };
      }
    };
  };
  return { calls, fetchImpl };
}

async function check(options = {}) {
  const fixture = fetchFixture(options.fixture);
  const result = await candidateManagerProviderAuthorityCurrent({
    env: { SUPABASE_URL: 'https://test.example.invalid' },
    claimedRow: options.claimed ?? claimedRow(),
    currentLeaseToken: options.lease ?? lease,
    fetchImpl: fixture.fetchImpl,
    headers: { authorization: 'test-only' },
    nowUtc: () => now
  });
  return { ...fixture, result };
}

test('manager provider handoff obtains one exact database submit permit', async () => {
  const { result, calls } = await check();
  assert.deepEqual(result, { candidate_bound: true, authorised: true, reason: null });
  assert.equal(calls.length, 2);
  assert.equal(calls[0].options.method, 'GET');
  assert.equal(calls[1].options.method, 'POST');
  assert.deepEqual(JSON.parse(calls[1].options.body), {
    p_session_id: null,
    p_environment: 'TEST',
    p_workflow_id: workflowId,
    p_action: 'MANAGER_PROVIDER_SUBMIT_PERMIT',
    p_expected_generation: 4,
    p_payload: {
      service_manager_provider_submit_permit: true,
      mail_outbox_id: outboxId,
      attempt_lease_token: lease,
      approval_request_id: requestId,
      approval_request_generation: 2,
      manager_mail_kind: 'INITIAL'
    },
    p_idempotency_key: null,
    p_now_utc: now
  });
});

test('withdrawal permit remains bound to its terminal request generation', async () => {
  const { result, calls } = await check({
    claimed: claimedRow({ candidate_manager_mail_kind: 'WITHDRAWAL' }),
    fixture: { rpcResult: {
      ok: true,
      workflow_id: workflowId,
      generation: 5,
      approval_request_id: requestId,
      approval_request_generation: 2,
      approval_workflow_generation: 4,
      mail_outbox_id: outboxId,
      manager_mail_kind: 'WITHDRAWAL',
      provider_submit_permit: true
    } }
  });
  assert.equal(result.authorised, true);
  assert.equal(JSON.parse(calls[1].options.body).p_expected_generation, null);
});

test('manager provider handoff rejects stale identity or generation receipts', async () => {
  for (const rpcResult of [
    { ok: true, provider_submit_permit: false },
    { ok: true, provider_submit_permit: true, workflow_id: workflowId,
      approval_request_id: requestId, approval_request_generation: 3,
      approval_workflow_generation: 4, mail_outbox_id: outboxId, manager_mail_kind: 'INITIAL' },
    { ok: true, provider_submit_permit: true, workflow_id: workflowId,
      approval_request_id: requestId, approval_request_generation: 2,
      approval_workflow_generation: 3, mail_outbox_id: outboxId, manager_mail_kind: 'INITIAL' },
    { ok: true, provider_submit_permit: true, workflow_id: workflowId,
      approval_request_id: requestId, approval_request_generation: 2,
      approval_workflow_generation: 4, mail_outbox_id: `${outboxId.slice(0, -1)}9`, manager_mail_kind: 'INITIAL' }
  ]) {
    const { result } = await check({ fixture: { rpcResult } });
    assert.deepEqual(result, {
      candidate_bound: true,
      authorised: false,
      reason: 'CANDIDATE_MANAGER_PROVIDER_SUBMIT_PERMIT_REFUSED'
    });
  }
});

test('manager provider handoff fails before RPC for retired or malformed binding', async () => {
  for (const claimed of [
    claimedRow({ candidate_manager_mail_retired: true }),
    claimedRow({ candidate_approval_request_id: 'bad' }),
    claimedRow({ candidate_approval_request_generation: 0 })
  ]) {
    const { result, calls } = await check({ claimed });
    assert.equal(result.authorised, false);
    assert.equal(result.reason, 'CANDIDATE_MANAGER_PROVIDER_BINDING_INVALID');
    assert.equal(calls.length, 0);
  }
});

test('ordinary non-manager mail bypasses manager provider authority', async () => {
  let called = false;
  const result = await candidateManagerProviderAuthorityCurrent({
    env: { SUPABASE_URL: 'https://test.example.invalid' },
    claimedRow: { id: outboxId, payment_scope_json: {} },
    currentLeaseToken: lease,
    fetchImpl: async () => { called = true; throw new Error('must not fetch'); },
    headers: {}
  });
  assert.deepEqual(result, { candidate_bound: false, authorised: true });
  assert.equal(called, false);
});

test('normal mail handoff composes manager permit before the external provider call', async () => {
  const source = await readFile(new URL('../broker/src/index.js', import.meta.url), 'utf8');
  assert.match(source, /import \{ candidateManagerProviderAuthorityCurrent \} from '\.\/candidate-manager-provider-authority\.js'/);
  const start = source.indexOf('candidatePaperProviderAuthorityCurrent({');
  const end = source.indexOf('postToPowerAutomate', start);
  const handoff = source.slice(start, end);
  assert.match(handoff, /candidateManagerProviderAuthorityCurrent\(\{[\s\S]*claimedRow[\s\S]*currentLeaseToken: attemptLeaseToken/);
  assert.ok(start >= 0 && end > start, 'Candidate provider permits must run before the provider request');
});
