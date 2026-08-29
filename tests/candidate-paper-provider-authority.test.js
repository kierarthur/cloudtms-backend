import assert from 'node:assert/strict';
import test from 'node:test';

import { candidatePaperProviderAuthorityCurrent } from '../broker/src/candidate-paper-provider-authority.js';

const workflowId = 'b5400000-0000-4000-8000-000000000001';
const outboxId = 'b5400000-0000-4000-8000-000000000002';
const manifest = 'a'.repeat(64);
const lease = 'provider-lease';
const now = '2026-08-11T12:00:00.000Z';

function claimedRow(scopePatch = {}) {
  return {
    id: outboxId,
    payment_scope_json: {
      candidate_mail_authority: 'CANDIDATE_PAPER_V1',
      candidate_workflow_id: workflowId,
      candidate_workflow_generation: 3,
      paper_return_manifest_sha256: manifest,
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
          generation: 3,
          mail_outbox_id: outboxId,
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
  const result = await candidatePaperProviderAuthorityCurrent({
    env: { SUPABASE_URL: 'https://test.example.invalid' },
    claimedRow: options.claimed ?? claimedRow(),
    currentLeaseToken: options.lease ?? lease,
    fetchImpl: fixture.fetchImpl,
    headers: { authorization: 'test-only' },
    nowUtc: () => now
  });
  return { ...fixture, result };
}

test('provider handoff obtains one atomic database submit permit', async () => {
  const { result, calls } = await check();
  assert.deepEqual(result, { candidate_bound: true, authorised: true, reason: null });
  assert.equal(calls.length, 2);
  assert.equal(calls[0].options.method, 'GET');
  assert.match(calls[0].url, /settings_defaults/);
  assert.equal(calls[1].options.method, 'POST');
  const body = JSON.parse(calls[1].options.body);
  assert.deepEqual(body, {
    p_session_id: null,
    p_environment: 'TEST',
    p_workflow_id: workflowId,
    p_action: 'PAPER_PROVIDER_SUBMIT_PERMIT',
    p_expected_generation: 3,
    p_payload: {
      service_paper_provider_submit_permit: true,
      mail_outbox_id: outboxId,
      attempt_lease_token: lease,
      paper_return_manifest_sha256: manifest
    },
    p_idempotency_key: null,
    p_now_utc: now
  });
});

test('provider handoff fails closed when the atomic permit is refused or stale', async () => {
  for (const fixture of [
    { rpcOk: false, rpcResult: { message: 'stale' } },
    { rpcResult: { ok: true, provider_submit_permit: false } },
    { rpcResult: { ok: true, provider_submit_permit: true, workflow_id: workflowId, generation: 2, mail_outbox_id: outboxId } },
    { rpcResult: { ok: true, provider_submit_permit: true, workflow_id: workflowId, generation: 3, mail_outbox_id: 'b5400000-0000-4000-8000-000000000099' } }
  ]) {
    const { result } = await check({ fixture });
    assert.equal(result.candidate_bound, true);
    assert.equal(result.authorised, false);
    assert.equal(result.reason, 'CANDIDATE_PAPER_PROVIDER_SUBMIT_PERMIT_REFUSED');
  }
});

test('provider handoff rejects a permit receipt for another workflow', async () => {
  const { result } = await check({
    fixture: {
      rpcResult: {
        ok: true,
        provider_submit_permit: true,
        workflow_id: 'b5400000-0000-4000-8000-000000000099',
        generation: 3,
        mail_outbox_id: outboxId
      }
    }
  });
  assert.deepEqual(result, {
    candidate_bound: true,
    authorised: false,
    reason: 'CANDIDATE_PAPER_PROVIDER_SUBMIT_PERMIT_REFUSED'
  });
});

test('provider handoff fails before the permit RPC for invalid binding or environment', async () => {
  const missingLease = await check({ lease: '' });
  assert.equal(missingLease.result.authorised, false);
  assert.equal(missingLease.calls.length, 0);

  const badManifest = await check({
    claimed: claimedRow({ paper_return_manifest_sha256: 'bad' })
  });
  assert.equal(badManifest.result.authorised, false);
  assert.equal(badManifest.calls.length, 0);

  const missingAuthority = await check({
    claimed: claimedRow({ candidate_mail_authority: null })
  });
  assert.equal(missingAuthority.result.authorised, false);
  assert.equal(missingAuthority.calls.length, 0);

  const badEnvironment = await check({ fixture: { environment: 'UNKNOWN' } });
  assert.equal(badEnvironment.result.authorised, false);
  assert.equal(badEnvironment.calls.length, 1);
});

test('ordinary non-Candidate mail bypasses Candidate provider authority', async () => {
  let called = false;
  const result = await candidatePaperProviderAuthorityCurrent({
    env: { SUPABASE_URL: 'https://test.example.invalid' },
    claimedRow: { id: outboxId, payment_scope_json: {} },
    currentLeaseToken: lease,
    fetchImpl: async () => { called = true; throw new Error('must not fetch'); },
    headers: {}
  });
  assert.deepEqual(result, { candidate_bound: false, authorised: true });
  assert.equal(called, false);
});
