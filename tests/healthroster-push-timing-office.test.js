import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { createPushTimingOfficeHandler, validatePushTimingPolicy } from '../broker/src/weekly-source/push-timing-office.mjs';

const ACTOR = 'f7160000-0000-4000-8000-000000000001';
const AGENCY = 'f7160000-0000-4000-8000-000000000019';
const REQUEST = 'f7160000-0000-4000-8000-000000000002';
const REVISION = '9007199254740993';
const policy = () => ({ version: 'PUSH_TIMING_POLICY_V1', enabled: true, timezone: 'Europe/London',
  normal_quiet_start: '00:00', normal_quiet_end: '07:00',
  day_rules: Object.fromEntries(['monday','tuesday','wednesday','thursday','friday','saturday','sunday'].map(day => [day, { mode: 'INHERIT' }])),
  bank_holiday_rule: { mode: 'INHERIT' } });
const save = () => ({ version: 'PUSH_TIMING_SAVE_V1', request_id: REQUEST, expected_revision: REVISION, policy: policy() });
const state = saving => ({ ok: true, version: saving ? 'PUSH_TIMING_SAVED_V1' : 'PUSH_TIMING_STATE_V1',
  ...(saving ? { request_id: REQUEST } : { can_edit: true }), revision: REVISION, policy: policy(),
  policy_sha256: 'a'.repeat(64), updated_at_utc: '2026-10-15T23:32:00.123456Z' });
function fixture({ result, error, user = { id: ACTOR }, deployment = { agency_id: AGENCY, environment: 'TEST' } } = {}) {
  const calls = [];
  const handler = createPushTimingOfficeHandler({
    requireAdmin: async () => { if (user instanceof Error) throw user; return user; },
    deploymentContext: () => { if (deployment instanceof Error) throw deployment; return deployment; },
    rpc: async (...args) => {
      calls.push(args);
      if (error) throw error;
      return result === undefined ? state(args[1] === 'candidate_push_timing_save_v1') : result;
    }
  });
  const run = (body, method = body === undefined ? 'GET' : 'PUT', search = '') => handler({}, new Request(
    'https://agency.invalid/api/mytms/push-timing' + search,
    { method, ...(body === undefined ? {} : { body: typeof body === 'string' || body instanceof Uint8Array ? body : JSON.stringify(body) }) }
  ));
  return { calls, run, handler };
}

test('actual QT GET uses authenticated admin and trusted agency, preserving string revision', async () => {
  const f = fixture(); const response = await f.run();
  assert.equal(response.status, 200); assert.deepEqual(await response.json(), state(false));
  assert.deepEqual(f.calls[0].slice(1), ['candidate_push_timing_get_v1', { p_request: {
    version: 'PUSH_TIMING_GET_V1', actor_user_id: ACTOR, agency_id: AGENCY, environment: 'TEST'
  } }, { timeoutMs: 20_000 }]);
  assert.equal(response.headers.get('cache-control'), 'no-store');
});
test('actual QT SAVE carries the exact immutable request and >2^53 revision without rounding', async () => {
  const f = fixture(); const body = save();
  assert.equal((await f.run(body)).status, 200);
  assert.deepEqual(f.calls[0][2], { p_request: { ...body, actor_user_id: ACTOR, agency_id: AGENCY, environment: 'TEST' } });
  assert.equal(f.calls[0][2].p_request.expected_revision, REVISION);
  assert.deepEqual(body, save());
});
test('single JSONB row envelope remains the same closed saved receipt', async () => {
  const f = fixture({ result: [state(true)] });
  assert.deepEqual(await (await f.run(save())).json(), state(true));
});

const invalidBodies = [
  ['actor', b => ({ ...b, actor_user_id: ACTOR })], ['agency', b => ({ ...b, agency_id: AGENCY })],
  ['environment', b => ({ ...b, environment: 'TEST' })], ['capability', b => ({ ...b, capability: true })],
  ['number revision', b => ({ ...b, expected_revision: 1 })], ['leading zero', b => ({ ...b, expected_revision: '01' })],
  ['signed revision', b => ({ ...b, expected_revision: '+1' })], ['overflow', b => ({ ...b, expected_revision: '9223372036854775808' })],
  ['UUID array', b => ({ ...b, request_id: [REQUEST] })], ['uppercase UUID', b => ({ ...b, request_id: REQUEST.toUpperCase() })],
  ['unknown policy', b => ({ ...b, policy: { ...b.policy, candidate_id: ACTOR } })],
  ['array time', b => ({ ...b, policy: { ...b.policy, normal_quiet_start: ['00:00'] } })],
  ['null time', b => ({ ...b, policy: { ...b.policy, normal_quiet_start: null } })],
  ['inherited extra field', b => { b.policy.day_rules.monday.start = '08:00'; return b; }],
  ['wrapped override window', b => { b.policy.day_rules.monday = { mode: 'ALLOW_WINDOW', start: '22:00', end: '07:00' }; return b; }],
  ['no weekday', b => { for (const day of Object.keys(b.policy.day_rules)) b.policy.day_rules[day] = { mode: 'NO_PUSH_DAY' }; return b; }]
];
for (const [name, change] of invalidBodies) test('QT rejects ' + name + ' before RPC', async () => {
  const f = fixture(); const response = await f.run(change(save()));
  assert.equal(response.status, 400); assert.deepEqual(await response.json(), { ok: false, error_code: 'PUSH_TIMING_REQUEST_INVALID' });
  assert.equal(f.calls.length, 0);
});
for (const [name, text] of [
  ['duplicate root', '{"version":"PUSH_TIMING_SAVE_V1","version":"PUSH_TIMING_SAVE_V1"}'],
  ['escaped duplicate', '{"version":"PUSH_TIMING_SAVE_V1","\\u0076ersion":"PUSH_TIMING_SAVE_V1"}'],
  ['nested duplicate', '{"policy":{"mode":"INHERIT","mode":"NO_PUSH_DAY"}}'],
  ['malformed JSON', '{'], ['array JSON', '[]'], ['null JSON', 'null'],
  ['oversized body', ' '.repeat(16_385)], ['invalid UTF8', new Uint8Array([0xc0, 0xaf])]
]) test('QT rejects ' + name + ' body before RPC', async () => {
  const f = fixture(); assert.equal((await f.run(text)).status, 400); assert.equal(f.calls.length, 0);
});
test('GET rejects all browser deployment/query options', async () => {
  const f = fixture(); assert.equal((await f.run(undefined, 'GET', '?agency_id=' + AGENCY)).status, 400); assert.equal(f.calls.length, 0);
});
for (const [name, options, status] of [
  ['missing actor', { user: null }, 403], ['missing deployment', { deployment: new Error('internal config') }, 503],
  ['malformed trusted deployment', { deployment: { agency_id: [AGENCY], environment: 'TEST' } }, 503],
  ['permission boundary', { user: Object.assign(new Error('sensitive'), { status: 401 }) }, 401]
]) test('QT fails closed for ' + name, async () => {
  const f = fixture(options); const response = await f.run();
  assert.equal(response.status, status); assert.equal(f.calls.length, 0); assert.doesNotMatch(await response.text(), /internal config|sensitive/);
});
for (const [name, result] of [
  ['numeric SQL revision', { ...state(false), revision: 1 }], ['foreign fields', { ...state(false), agency_id: AGENCY }],
  ['permission flag', { ...state(false), can_edit: false }], ['array hash', { ...state(false), policy_sha256: ['a'.repeat(64)] }],
  ['bad timestamp', { ...state(false), updated_at_utc: 'invalid' }], ['two rows', [state(false), state(false)]]
]) test('QT refuses malformed native reply: ' + name, async () => {
  const f = fixture({ result }); const response = await f.run();
  assert.equal(response.status, 503); assert.deepEqual(await response.json(), { ok: false, error_code: 'PUSH_TIMING_DEPENDENCY_UNAVAILABLE' });
});
test('lost save/conflict preserves exact error and performs no automatic retry', async () => {
  const f = fixture({ error: { fn: 'candidate_push_timing_save_v1', json: { message: 'PUSH_TIMING_REVISION_CONFLICT', details: 'private row' } } });
  const response = await f.run(save()); assert.equal(response.status, 409);
  assert.deepEqual(await response.json(), { ok: false, error_code: 'PUSH_TIMING_REVISION_CONFLICT' }); assert.equal(f.calls.length, 1);
});
test('schema cache/dependency failures never report a successful unchanged save', async () => {
  const f = fixture({ error: { fn: 'candidate_push_timing_save_v1', json: { code: 'PGRST202', message: 'sensitive function diagnostic' } } });
  const response = await f.run(save()); assert.equal(response.status, 503);
  assert.deepEqual(await response.json(), { ok: false, error_code: 'PUSH_TIMING_DEPENDENCY_UNAVAILABLE' }); assert.equal(f.calls.length, 1);
});
test('a different RPC cannot inject a known business refusal', async () => {
  const f = fixture({ error: { fn: 'other_rpc', json: { message: 'PUSH_TIMING_REVISION_CONFLICT' } } });
  assert.equal((await f.run()).status, 503);
});
test('valid wraparound normal quiet period and disabled equal endpoints retain exact policy', () => {
  const p = policy(); p.normal_quiet_start = '22:00'; assert.equal(validatePushTimingPolicy(p), p);
  p.enabled = false; p.normal_quiet_end = '22:00'; assert.equal(validatePushTimingPolicy(p), p);
});
test('mounted route uses the existing MyTMS admin and actual deployment resolver without changing control-plane payload', () => {
  const index = fs.readFileSync(new URL('../broker/src/index.js', import.meta.url), 'utf8');
  assert.match(index, /createPushTimingOfficeHandler\(\{ requireAdmin: requireMyTmsOfficeAdmin, rpc: sbRpc, deploymentContext: weeklySourceDeploymentContext \}\)/);
  assert.equal(index.split("p === '/api/mytms/push-timing'").length - 1, 1);
  assert.match(index, /p === '\/api\/mytms\/push-timing'\) return withCORS\(env, req, await handlePushTimingOffice\(env, req\)\)/);
  assert.match(index, /p === '\/api\/mytms\/settings'\).*handleMyTmsOfficeSettings\(env, req, 'SET'\)/);
});
