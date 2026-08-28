import assert from 'node:assert/strict';
import test from 'node:test';

import {
  findCandidateDailyRoute,
  rebuildCandidateDailySuccessBody
} from '../broker/src/candidate-daily-contract-v1.js';
import {
  candidateDailyPhase1bInternals,
  composeCandidateBootstrapPhase1b,
  handleCandidateDailyPhase1bRequest
} from '../broker/src/candidate-daily-phase1b.js';
import { candidateBrokerInternals } from '../candidate-broker/src/candidate-broker.js';

const correlationId = '01K2ABCDEF0123456789ABCDEF';
const candidateId = '00000000-0000-4000-8000-000000000101';
const generationId = '00000000-0000-4000-8000-000000000102';
const sessionId = '00000000-0000-4000-8000-000000000103';
const commandId = '00000000-0000-4000-8000-000000000104';
const access = {
  session_id: sessionId,
  selected_candidate_id: candidateId,
  environment: 'TEST'
};

function freshness(overrides = {}) {
  return {
    generation_version: 1,
    generation_published_at: '2026-08-17T00:00:00.000Z',
    generation_age_seconds: 1,
    canonical_version: 0,
    accepted_canonical_cursor: 0,
    required_visible_cursor: 0,
    projection_oldest_pending_seconds: 0,
    generation_max_age_seconds: 120,
    projection_warning_seconds: 30,
    ready: true,
    reasons: [],
    overlay_proof_cursor: 0,
    effective_visible_cursor: 0,
    delivered_visible_cursor: 0,
    ...overrides
  };
}

function dailyTiles(overrides = {}) {
  const start = new Date('2026-08-17T00:00:00.000Z');
  const tiles = Array.from({ length: 14 }, (_, index) => {
    const value = new Date(start);
    value.setUTCDate(value.getUTCDate() + index);
    const date = value.toISOString().slice(0, 10);
    return {
      date,
      display_day: value.toLocaleDateString('en-GB', { weekday: 'short', timeZone: 'UTC' }),
      display_date: value.toLocaleDateString('en-GB', { day: '2-digit', month: '2-digit', timeZone: 'UTC' }),
      booked: false,
      system_blocked: false,
      editable: true,
      status: 'PENDING',
      availability: 'PENDING'
    };
  });
  return {
    candidate_id: candidateId,
    window_start: '2026-08-17',
    window_end: '2026-08-30',
    generation_id: generationId,
    generation_version: 1,
    availability_version: 0,
    freshness: freshness(),
    cohorts: [],
    tiles,
    ...overrides
  };
}

function request(path, options = {}) {
  return new Request(`https://private.test${path}`, {
    ...options,
    headers: {
      'x-correlation-id': correlationId,
      ...(options.headers || {})
    }
  });
}

test('R8 bootstrap preserves the database-owned Daily capability and fails unreadable authority closed', () => {
  assert.deepEqual(composeCandidateBootstrapPhase1b({
    ok: true,
    capabilities: { daily_availability: { enabled: false, unavailable_reason: 'GLOBAL_DISABLED' } }
  }).capabilities.daily_availability, { enabled: false, unavailable_reason: 'GLOBAL_DISABLED' });
  assert.deepEqual(composeCandidateBootstrapPhase1b({ ok: true }).capabilities.daily_availability,
    { enabled: false, unavailable_reason: 'AUTHORITY_UNREADABLE' });
});

test('R8 tiles dispatch returns exactly fourteen database-owned tiles through the strict envelope', async () => {
  const calls = [];
  const response = await handleCandidateDailyPhase1bRequest(request('/candidate-app/v1/daily/tiles?days=14'),
    access, {}, { async rpc(name, args) { calls.push([name, args]); return dailyTiles(); } });
  assert.equal(response.status, 200);
  assert.equal(response.headers.get('x-idempotent-replay'), 'false');
  const body = await response.json();
  assert.equal(body.result.tiles.length, 14);
  assert.equal(calls.length, 1);
  assert.equal(calls[0][0], 'candidate_daily_tiles_get_v1');
  assert.deepEqual(calls[0][1].p_internal_context,
    { policy: 'CANDIDATE_SURFACE', environment: 'TEST', candidate_id: candidateId });
});

test('Rota booked shift timestamps survive both private and public response boundaries', async () => {
  const source = dailyTiles();
  Object.assign(source.tiles[0], {
    booked: true, editable: false, status: 'BOOKED',
    shift_starts_at: '2026-08-17T18:30:00+00:00',
    shift_ends_at: '2026-08-18T07:00:00+00:00'
  });
  Object.assign(source.tiles[1], { shift_starts_at: null, shift_ends_at: null });
  const privateResponse = await handleCandidateDailyPhase1bRequest(
    request('/candidate-app/v1/daily/tiles?days=14'), access, {},
    { async rpc() { return source; } }
  );
  assert.equal(privateResponse.status, 200);
  const route = findCandidateDailyRoute('GET', '/candidate-app/v1/daily/tiles');
  const publicResponse = await candidateBrokerInternals.publicSafeDailyResponse(privateResponse, correlationId, route);
  assert.equal(publicResponse.status, 200);
  const result = (await publicResponse.json()).result;
  assert.deepEqual(result.tiles, source.tiles);
  assert.equal(result.tiles.length, 14);
});

test('Rota time-field validation rejects malformed values and still rejects undeclared fields', () => {
  const route = findCandidateDailyRoute('GET', '/candidate-app/v1/daily/tiles');
  const envelope = (result) => rebuildCandidateDailySuccessBody(route, 200,
    { ok: true, correlation_id: correlationId, result }, correlationId);
  for (const field of ['shift_starts_at', 'shift_ends_at']) {
    for (const value of ['', '0900', '2026-08-17', '2026-08-17T09:00:00', 'not-a-date', 123, {}, true]) {
      const source = dailyTiles();
      source.tiles[0][field] = value;
      assert.equal(envelope(source), null, `${field}: ${JSON.stringify(value)}`);
    }
    for (const value of ['2026-08-17T09:00:00Z', '2026-08-17T09:00:00.123Z', '2026-08-17T09:00:00+01:00']) {
      const source = dailyTiles();
      source.tiles[0][field] = value;
      assert.ok(envelope(source), `${field}: ${value}`);
      source.tiles[0].private_source_hmac = 'must-not-leak';
      assert.equal(envelope(source), null);
    }
  }
});

test('R8 availability command freezes one caller key and emits replay only from the durable owner', async () => {
  const body = JSON.stringify({
    expected_availability_version: 0,
    changes: [{ date: '2026-08-17', availability: 'LONG_DAY' }]
  });
  const deps = {
    async rpc(name, args) {
      assert.equal(name, 'candidate_daily_availability_apply_atomic_v1');
      assert.equal(args.p_idempotency_key, 'candidate-daily-command-key-0001');
      return { command_id: commandId, availability_version: 1, changed_dates: ['2026-08-17'], _idempotent_replay: true };
    }
  };
  const response = await handleCandidateDailyPhase1bRequest(request('/candidate-app/v1/daily/availability', {
    method: 'PATCH',
    headers: { 'content-type': 'application/json', 'idempotency-key': 'candidate-daily-command-key-0001' },
    body
  }), access, {}, deps);
  assert.equal(response.status, 200);
  assert.equal(response.headers.get('x-idempotent-replay'), 'true');
  assert.deepEqual((await response.json()).result,
    { command_id: commandId, availability_version: 1, changed_dates: ['2026-08-17'] });
});

test('R8 Candidate commands preserve stable conflict details and the operation-specific refresh contract', async () => {
  const source = JSON.stringify({
    expected_availability_version: 4,
    changes: [{ date: '2026-08-17', availability: 'NIGHT' }]
  });
  for (const [error, status, retry] of [
    [{ error_code: 'AVAILABILITY_VERSION_CONFLICT', current_availability_version: 5 }, 409, 'REFRESH'],
    [{ error_code: 'GENERATION_INCOMPLETE' }, 422, 'REFRESH']
  ]) {
    const response = await handleCandidateDailyPhase1bRequest(request('/candidate-app/v1/daily/availability', {
      method: 'PATCH',
      headers: { 'content-type': 'application/json', 'idempotency-key': 'candidate-daily-command-key-0002' },
      body: source
    }), access, {}, { async rpc() { return error; } });
    const payload = await response.json();
    assert.equal(response.status, status);
    assert.equal(payload.retry_class, retry);
    if (error.current_availability_version) {
      assert.deepEqual(payload.details, { kind: 'CONFLICT', current_availability_version: 5 });
    }
  }
});

test('R8 strict success authority rejects malformed cohorts, freshness, and action locators', () => {
  const route = findCandidateDailyRoute('GET', '/candidate-app/v1/daily/tiles');
  const envelope = (result) => rebuildCandidateDailySuccessBody(route, 200,
    { ok: true, correlation_id: correlationId, result }, correlationId);
  assert.ok(envelope(dailyTiles()));
  assert.equal(envelope(dailyTiles({ cohorts: [{ display_name: 'A', role: 'B', private_mobile: 'forbidden' }] })), null);
  assert.equal(envelope(dailyTiles({ freshness: freshness({ reasons: ['UNAPPROVED_REASON'], ready: false }) })), null);
  const withBadTarget = dailyTiles();
  withBadTarget.tiles[0] = {
    ...withBadTarget.tiles[0],
    action_target: {
      target_kind: 'TIMESHEET_DETAIL',
      timesheet_id: candidateId,
      workflow_id: null,
      row_signature: 'A'.repeat(32)
    }
  };
  assert.equal(envelope(withBadTarget), null);
  withBadTarget.tiles[0].action_target.row_signature = 'a'.repeat(32);
  assert.ok(envelope(withBadTarget));
});

test('R8 specialist seam passes only typed facts and reconstructs a closed success envelope', async () => {
  let observed;
  const response = await handleCandidateDailyPhase1bRequest(request(
    '/candidate-app/v1/daily/running-late/preview', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        emergency_shift_token: 'emergency_token_123456',
        running_late_option_token: 'late_option_token_123456'
      })
    }
  ), access, {}, {
    async candidateDailySpecialist(input) {
      observed = input;
      return {
        result: { arrival_at: '2026-08-17T09:30:00.000Z', preview_text: 'Expected arrival 09:30.' }
      };
    }
  });
  assert.equal(response.status, 200);
  assert.deepEqual(observed.input, {
    emergency_shift_token: 'emergency_token_123456',
    running_late_option_token: 'late_option_token_123456'
  });
  assert.equal(observed.operation_id, 'previewCandidateDailyRunningLate');
  assert.equal(Object.hasOwn(observed, 'request'), false);
  assert.equal((await response.json()).result.preview_text, 'Expected arrival 09:30.');
});

test('R8 retained candidate-message read requires one closed platform and returns an acknowledgement token', async () => {
  let observed;
  const response = await handleCandidateDailyPhase1bRequest(request(
    '/candidate-app/v1/daily/content/candidate-message?platform=ANDROID'
  ), access, {}, {
    async candidateDailySpecialist(input) {
      observed = input;
      return { result: {
        kind: 'candidate-message', title: 'Welcome (Android)', html: '<p>Welcome</p>',
        message_token: 'message-token-1234567890', message_kind: 'WELCOME', acknowledgement_mode: 'ALL',
        appInfo: { version: 'test', buildTs: '2026-08-23' }
      } };
    }
  });
  assert.equal(response.status, 200);
  assert.deepEqual(observed.input, { kind: 'candidate-message', platform: 'ANDROID' });
  assert.equal((await response.json()).result.acknowledgement_mode, 'ALL');

  for (const path of [
    '/candidate-app/v1/daily/content/candidate-message',
    '/candidate-app/v1/daily/content/candidate-message?platform=ANDROID&platform=WEB',
    '/candidate-app/v1/daily/content/candidate-message?platform=UNKNOWN'
  ]) {
    const invalid = await handleCandidateDailyPhase1bRequest(request(path), access, {}, {
      async candidateDailySpecialist() { throw new Error('must not run'); }
    });
    assert.equal(invalid.status, 400);
  }
});

test('R8 specialist seam rejects unapproved request keys and malformed provider responses', async () => {
  const invalid = await handleCandidateDailyPhase1bRequest(request('/candidate-app/v1/daily/running-late/options', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ emergency_shift_token: 'emergency_token_123456', candidate_id: candidateId })
  }), access, {}, { async candidateDailySpecialist() { throw new Error('must not run'); } });
  assert.equal(invalid.status, 400);
  assert.equal((await invalid.json()).error_code, 'VALIDATION_FAILED');

  const malformed = await handleCandidateDailyPhase1bRequest(request('/candidate-app/v1/daily/emergency-window'),
    access, {}, { async candidateDailySpecialist() { return { result: { eligible: true, shifts: [] } }; } });
  assert.equal(malformed.status, 503);
  assert.equal((await malformed.json()).error_code, 'CANDIDATE_DAILY_NOT_READY');
});

test('R8 Candidate external-effect responses remain exact, replayable and candidate-bound', async () => {
  const effect = {
    effect_key: 'effect_key_1234567890',
    operation: 'CANNOT_ATTEND',
    status: 'COMPLETED',
    created_at: '2026-08-17T01:00:00.000Z',
    updated_at: '2026-08-17T01:00:01.000Z',
    safe_message: 'Emergency sent.'
  };
  const response = await handleCandidateDailyPhase1bRequest(request('/candidate-app/v1/daily/emergencies', {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'idempotency-key': 'candidate-daily-effect-key-0001' },
    body: JSON.stringify({
      type: 'CANNOT_ATTEND', emergency_shift_token: 'emergency_token_123456', reason_text: 'Unable to attend.'
    })
  }), access, {}, { async candidateDailySpecialist() { return { result: effect, idempotent_replay: true }; } });
  assert.equal(response.status, 200);
  assert.equal(response.headers.get('x-idempotent-replay'), 'true');
  assert.deepEqual((await response.json()).result, effect);
});

test('R8 public success boundary rebuilds approved fields and rejects leakage', async () => {
  const route = findCandidateDailyRoute('GET', '/candidate-app/v1/daily/tiles');
  const privateResponse = new Response(JSON.stringify({
    ok: true, correlation_id: correlationId, result: dailyTiles()
  }), { status: 200, headers: { 'content-type': 'application/json', 'x-correlation-id': correlationId,
    'x-idempotent-replay': 'false' } });
  const safe = await candidateBrokerInternals.publicSafeDailyResponse(privateResponse, correlationId, route);
  assert.equal(safe.status, 200);
  assert.equal((await safe.json()).result.tiles.length, 14);

  const leaked = new Response(JSON.stringify({
    ok: true, correlation_id: correlationId, result: dailyTiles({ internal_secret: 'forbidden' })
  }), { status: 200, headers: { 'content-type': 'application/json', 'x-correlation-id': correlationId } });
  const rejected = await candidateBrokerInternals.publicSafeDailyResponse(leaked, correlationId, route);
  assert.equal(rejected.status, 503);
  assert.equal((await rejected.json()).error_code, 'CANDIDATE_DAILY_NOT_READY');
});

test('R8 signed-system operation mappings and strict body validators cover every Phase 2 RPC seam', () => {
  assert.equal(Object.keys(candidateDailyPhase1bInternals.RPC_BY_OPERATION).length, 13);
  assert.equal(candidateDailyPhase1bInternals.validateSystemBody('googleAvailabilityEffectStatus',
    { effect_key: 'effect_key_1234567890' }), true);
  assert.equal(candidateDailyPhase1bInternals.validateSystemBody('googleAvailabilityEffectStatus',
    { effect_key: 'effect_key_1234567890', candidate_id: candidateId }), false);
  assert.equal(candidateDailyPhase1bInternals.validateSystemBody('googleAvailabilityCompleteProjection', {
    batch_request_id: commandId,
    items: [{
      outbox_id: generationId,
      lease_token: 'lease_token_123456789',
      outcome: 'DELIVERED'
    }]
  }), false);
});
