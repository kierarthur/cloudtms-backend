import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

import {
  candidateDailySpecialistInternals,
  createCandidateDailySpecialist
} from '../broker/src/candidate-daily-specialist.js';

const correlationId = '01K2ABCDEF0123456789ABCDEF';
const candidateContext = {
  policy: 'CANDIDATE_SURFACE',
  environment: 'TEST',
  candidate_id: '00000000-0000-4000-8000-000000000101'
};
const effectKey = 'a'.repeat(64);
const receiptId = '00000000-0000-4000-8000-000000000201';

function environment(overrides = {}) {
  return {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_DAILY_SPECIALIST_GOOGLE_ENABLED: 'TRUE',
    CANDIDATE_DAILY_SPECIALIST_GOOGLE_URL: 'https://script.google.test/macros/s/specialist/exec',
    CANDIDATE_DAILY_SPECIALIST_GOOGLE_KEY_ID: 'test-key-v1',
    CANDIDATE_DAILY_SPECIALIST_GOOGLE_SECRET: 'test-only-secret-material-at-least-32-bytes',
    ...overrides
  };
}

function effectRequest(operationId = 'sendCandidateDailyRunningLate') {
  return {
    operation_id: operationId,
    input: {
      emergency_shift_token: 'b'.repeat(64),
      running_late_option_token: 'c'.repeat(64)
    },
    idempotency_key: 'candidate-daily-effect-key-0001',
    correlation_id: correlationId,
    candidate_context: candidateContext
  };
}

test('DAILY specialist reads Past Shifts and emergency contacts only from the agency database', async () => {
  const calls = [];
  const originalFetch = globalThis.fetch;
  let networkCalls = 0;
  globalThis.fetch = async () => { networkCalls += 1; throw new Error('Google must not be called'); };
  try {
    const specialist = createCandidateDailySpecialist(environment(), async (name, args) => {
      calls.push({ name, args });
      return { eligible: false, grace_minutes_after_start: 600, shifts: [] };
    });
    const result = await specialist({
      operation_id: 'getCandidateDailyEmergencyWindow', input: {}, correlation_id: correlationId,
      candidate_context: candidateContext
    });
    assert.deepEqual(result.result, { eligible: false, grace_minutes_after_start: 600, shifts: [] });
    assert.equal(networkCalls, 0);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].name, 'candidate_daily_specialist_read_v1');
    assert.equal(calls[0].args.p_operation, 'EMERGENCY_WINDOW');
    assert.deepEqual(calls[0].args.p_internal_context, candidateContext);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('DAILY retained candidate messages resolve identity in the agency database before the signed Google read', async () => {
  const order = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (_url, options) => {
    order.push('GOOGLE');
    const body = JSON.parse(options.body);
    assert.equal(body.operation, 'CONTENT_READ');
    assert.deepEqual(body.payload, {
      kind: 'candidate-message', platform: 'ANDROID',
      candidate: { display_name: 'Test Candidate', callable_mobile: '07123456789' }
    });
    return new Response(JSON.stringify({
      ok: true, request_id: correlationId, provider_reference: null,
      result: {
        kind: 'candidate-message', title: 'Welcome (Android)', html: '<p>Welcome</p>',
        message_token: 'message-token-1234567890', message_kind: 'WELCOME', acknowledgement_mode: 'ALL',
        appInfo: { version: 'test', buildTs: '2026-08-23' }
      }
    }), { status: 200, headers: { 'content-type': 'application/json' } });
  };
  try {
    const specialist = createCandidateDailySpecialist(environment(), async (name, args) => {
      order.push(name);
      assert.equal(name, 'candidate_daily_specialist_read_v1');
      assert.equal(args.p_operation, 'MESSAGE_CONTEXT');
      assert.deepEqual(args.p_input, {});
      return { candidate: { display_name: 'Test Candidate', callable_mobile: '07123456789' } };
    });
    const output = await specialist({
      operation_id: 'getCandidateDailyContent',
      input: { kind: 'candidate-message', platform: 'ANDROID' },
      correlation_id: correlationId,
      candidate_context: candidateContext
    });
    assert.deepEqual(order, ['candidate_daily_specialist_read_v1', 'GOOGLE']);
    assert.equal(output.result.message_token, 'message-token-1234567890');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('DAILY hospital and accommodation directories come directly from the agency database', async () => {
  const calls = [];
  const originalFetch = globalThis.fetch;
  let networkCalls = 0;
  globalThis.fetch = async () => {
    networkCalls += 1;
    throw new Error('Google must not be called for Office-owned directories');
  };
  try {
    const specialist = createCandidateDailySpecialist(environment(), async (name, args) => {
      calls.push({ name, args });
      return {
        kind: args.p_kind,
        title: args.p_kind === 'hospital-addresses'
          ? 'Hospital addresses' : 'Accommodation contacts',
        schema_version: 1,
        entries: [],
        appInfo: { version: 'CLOUDTMS_DIRECTORY_V1', buildTs: '2026-08-30T03:20:00.000Z' }
      };
    });
    for (const kind of ['hospital-addresses', 'accommodation-contacts']) {
      const output = await specialist({
        operation_id: 'getCandidateDailyContent', input: { kind, platform: 'ANDROID' },
        correlation_id: correlationId, candidate_context: candidateContext
      });
      assert.equal(output.result.kind, kind);
    }
    assert.equal(networkCalls, 0);
    assert.equal(calls.length, 2);
    assert.ok(calls.every(call => call.name === 'candidate_daily_information_candidate_v1'));
    assert.ok(calls.every(call => call.args.p_internal_context === candidateContext));
    assert.deepEqual(calls.map(call => call.args.p_kind), [
      'hospital-addresses', 'accommodation-contacts'
    ]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('DAILY external effect claims one durable database receipt before the narrow Google call', async () => {
  const order = [];
  const rpcCalls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (_url, options) => {
    order.push('GOOGLE');
    const body = JSON.parse(options.body);
    assert.equal(body.schema_version, 'CLOUDTMS_CANDIDATE_SPECIALIST_V1');
    assert.equal(body.operation, 'RUNNING_LATE_SEND');
    assert.equal(body.effect_key, effectKey);
    assert.equal(body.payload.effect_key, effectKey);
    assert.equal(body.environment, 'TEST');
    assert.equal(body.request_id, correlationId);
    assert.match(body.signature, /^[A-Za-z0-9_-]{43}$/);
    assert.ok(Date.parse(body.expires_at) - Date.parse(body.issued_at) <= 60_000);
    return new Response(JSON.stringify({
      ok: true, request_id: correlationId, provider_reference: 'provider-reference-not-public',
      result: { accepted: true }
    }), { status: 200, headers: { 'content-type': 'application/json' } });
  };
  try {
    const specialist = createCandidateDailySpecialist(environment(), async (name, args) => {
      order.push(name);
      rpcCalls.push({ name, args });
      if (name === 'candidate_daily_effect_claim_candidate_v1') return {
        state: 'CLAIMED', effect_receipt_id: receiptId, effect_key: effectKey,
        lease_token: 'lease-token-000000000000000000000000',
        effect_payload: {
          effect_key: effectKey, operation: 'RUNNING_LATE_SEND', candidate: {}, input: {},
          running_late: {}, shift: {}, current_contacts: [], previous_contacts: [], dna_subject: null
        }
      };
      assert.equal(name, 'candidate_daily_effect_complete_candidate_v1');
      assert.equal(args.p_outcome, 'COMPLETED');
      assert.match(args.p_provider_reference_hash, /^[a-f0-9]{64}$/);
      assert.equal(args.p_provider_reference_hash.includes('provider-reference-not-public'), false);
      return {
        effect_key: effectKey, operation: 'RUNNING_LATE_SEND', status: 'COMPLETED',
        created_at: '2026-08-23T10:00:00.000Z', updated_at: '2026-08-23T10:00:01.000Z',
        safe_message: args.p_safe_message
      };
    });
    const result = await specialist(effectRequest());
    assert.deepEqual(order, [
      'candidate_daily_effect_claim_candidate_v1',
      'GOOGLE',
      'candidate_daily_effect_complete_candidate_v1'
    ]);
    assert.equal(result.result.status, 'COMPLETED');
    assert.equal(rpcCalls[0].args.p_idempotency_key, 'candidate-daily-effect-key-0001');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('uncertain Google delivery becomes UNKNOWN and is never blindly retried', async () => {
  let networkCalls = 0;
  const outcomes = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => { networkCalls += 1; throw new Error('connection lost after submit'); };
  try {
    const specialist = createCandidateDailySpecialist(environment(), async (name, args) => {
      if (name === 'candidate_daily_effect_claim_candidate_v1') return {
        state: 'CLAIMED', effect_receipt_id: receiptId, effect_key: effectKey,
        lease_token: 'lease-token-000000000000000000000000', effect_payload: {
          effect_key: effectKey, operation: 'RUNNING_LATE_SEND', candidate: {}, input: {},
          running_late: {}, shift: {}, current_contacts: [], previous_contacts: [], dna_subject: null
        }
      };
      outcomes.push(args.p_outcome);
      return {
        effect_key: effectKey, operation: 'RUNNING_LATE_SEND', status: args.p_outcome,
        created_at: '2026-08-23T10:00:00.000Z', updated_at: '2026-08-23T10:00:01.000Z',
        safe_message: args.p_safe_message
      };
    });
    const result = await specialist(effectRequest());
    assert.equal(networkCalls, 1);
    assert.deepEqual(outcomes, ['UNKNOWN']);
    assert.equal(result.result.status, 'UNKNOWN');
    assert.match(result.result.safe_message, /Do not submit it again/);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('specialist canonical signing and source preserve the no-generic-tunnel, no-fallback boundary', () => {
  assert.equal(candidateDailySpecialistInternals.stableJson({ z: 1, a: { y: 2, x: 3 } }),
    '{"a":{"x":3,"y":2},"z":1}');
  const source = readFileSync(new URL('../broker/src/candidate-daily-specialist.js', import.meta.url), 'utf8');
  const sql = readFileSync(new URL('../supabase/repeatable/23082026_1820_candidate_daily_specialist_v1.sql',
    import.meta.url), 'utf8');
  assert.doesNotMatch(source, /CLOUDTMS_PRIVATE|default binding|google\.script\.run/i);
  assert.match(source, /candidate_daily_effect_claim_candidate_v1[\s\S]*callGoogleSpecialist[\s\S]*candidate_daily_effect_complete_candidate_v1/);
  assert.match(sql, /private\.candidate_daily_external_effect_receipts/);
  assert.match(sql, /DNA'[\s\S]*v_groups->'current'/);
  assert.match(sql, /'current_contacts'[\s\S]*'previous_contacts'/);
  assert.doesNotMatch(sql, /https?:\/\/|urlfetch|script\.google|CANDIDATE_DAILY_SPECIALIST_GOOGLE_/i);
});
