import assert from 'node:assert/strict';
import test from 'node:test';

import { candidatePaperProviderAuthorityCurrent } from '../broker/src/candidate-paper-provider-authority.js';

const workflowId = 'b5400000-0000-4000-8000-000000000001';
const outboxId = 'b5400000-0000-4000-8000-000000000002';
const timesheetId = 'b5400000-0000-4000-8000-000000000003';
const manifest = 'a'.repeat(64);
const lease = 'provider-lease';

function claimedRow(scopePatch = {}) {
  return {
    id: outboxId,
    payment_scope_json: {
      candidate_workflow_id: workflowId,
      candidate_workflow_generation: 1,
      paper_return_manifest_sha256: manifest,
      ...scopePatch
    }
  };
}

function mailRow(patch = {}) {
  return {
    id: outboxId,
    type: 'TIMESHEET_QR',
    status: 'QUEUED',
    sent_at: null,
    attempt_lease_token: lease,
    context_kind: 'timesheets',
    context_id: timesheetId,
    attachments: [{ r2_key: 'candidate-app/test/pack.pdf' }],
    payment_scope_json: {
      candidate_workflow_id: workflowId,
      candidate_workflow_generation: 1,
      paper_return_manifest_sha256: manifest,
      candidate_paper_generation_retired: false,
      candidate_paper_pack_ready: true,
      mail_held_until_pdf_rendered: false
    },
    ...patch
  };
}

function workflowRow(patch = {}) {
  return {
    id: workflowId,
    route: 'PAPER',
    state: 'AWAITING_PAPER_RETURN',
    generation: 1,
    target_timesheet_id: timesheetId,
    anchor_timesheet_id: timesheetId,
    paper_return_manifest_sha256: `\\x${manifest}`,
    ...patch
  };
}

function fetchFixture(mail, workflow) {
  const calls = [];
  const fetchImpl = async url => {
    calls.push(String(url));
    const body = String(url).includes('/mail_outbox?') ? [mail] : [workflow];
    return {
      ok: true,
      async json() { return body; }
    };
  };
  return { calls, fetchImpl };
}

async function check({ claimed = claimedRow(), mail = mailRow(), workflow = workflowRow() } = {}) {
  const fixture = fetchFixture(mail, workflow);
  const result = await candidatePaperProviderAuthorityCurrent({
    env: { SUPABASE_URL: 'https://test.example.invalid' },
    claimedRow: claimed,
    currentLeaseToken: lease,
    fetchImpl: fixture.fetchImpl,
    headers: { authorization: 'test-only' }
  });
  return { ...fixture, result };
}

test('provider recheck accepts only the exact current awaiting PAPER generation', async () => {
  const { result, calls } = await check();
  assert.deepEqual(result, { candidate_bound: true, authorised: true, reason: null });
  assert.equal(calls.length, 2);
});

test('provider recheck refuses RECEIVED and REJECTED workflows after claim', async () => {
  for (const state of ['RECEIVED', 'REJECTED']) {
    const { result } = await check({ workflow: workflowRow({ state }) });
    assert.equal(result.candidate_bound, true);
    assert.equal(result.authorised, false);
    assert.equal(result.reason, 'CANDIDATE_PAPER_PROVIDER_WORKFLOW_STALE');
  }
});

test('provider recheck refuses a retired generation or a changed provider lease', async () => {
  const retired = await check({
    mail: mailRow({
      payment_scope_json: {
        ...mailRow().payment_scope_json,
        candidate_paper_generation_retired: true
      }
    })
  });
  assert.equal(retired.result.authorised, false);
  assert.equal(retired.result.reason, 'CANDIDATE_PAPER_PROVIDER_MAIL_STALE');

  const changedLease = await check({ mail: mailRow({ attempt_lease_token: 'other-lease' }) });
  assert.equal(changedLease.result.authorised, false);
  assert.equal(changedLease.result.reason, 'CANDIDATE_PAPER_PROVIDER_MAIL_STALE');
});

test('provider recheck refuses generation, manifest and source identity drift', async () => {
  for (const workflow of [
    workflowRow({ generation: 2 }),
    workflowRow({ paper_return_manifest_sha256: `\\x${'b'.repeat(64)}` }),
    workflowRow({ target_timesheet_id: 'b5400000-0000-4000-8000-000000000004' })
  ]) {
    const { result } = await check({ workflow });
    assert.equal(result.authorised, false);
    assert.equal(result.reason, 'CANDIDATE_PAPER_PROVIDER_WORKFLOW_STALE');
  }
});

test('ordinary non-Candidate mail bypasses Candidate revalidation', async () => {
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
