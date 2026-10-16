import assert from 'node:assert/strict';
import test from 'node:test';
import { contactReminderRefusal } from '../../broker/src/weekly-source/contact-reminder-refusal.mjs';
import { dispatchWeeklySourceRequest } from '../../broker/src/weekly-source/routes.js';

const ID = '81000000-0000-4000-8000-000000000001';
const CLIENT = '81000000-0000-4000-8000-000000000002';
const detail = () => ({ version: 'CONTACT_PAUSE_REFUSAL_V1', candidate_id: ID,
  client_id: CLIENT, audience: 'WORKER', next_allowed_utc: '2026-10-18T23:00:00.000000Z',
  next_allowed_local: '2026-10-19 00:00 Europe/London' });
const nativeError = (value = detail()) => Object.assign(new Error('native refusal'), {
  fn: 'weekly_source_candidate_reminder_atomic_v1', status: 400,
  json: { code: '55000', message: 'CONTACT_PAUSED', details: JSON.stringify(value) },
});

test('pause wording uses the server expiry correctly across London DST boundaries', () => {
  for (const [utc, local, expected] of [
    ['2026-10-18T23:00:00.000000Z', '2026-10-19 00:00 Europe/London', '19/10/2026 00:00'],
    ['2026-10-26T00:00:00.000000Z', '2026-10-26 00:00 Europe/London', '26/10/2026 00:00'],
    ['2026-03-29T23:00:00.000000Z', '2026-03-30 00:00 Europe/London', '30/03/2026 00:00'],
  ]) {
    assert.deepEqual(contactReminderRefusal(nativeError({ ...detail(), next_allowed_utc: utc,
      next_allowed_local: local })), {
      ok: false, error_code: 'CONTACT_PAUSED', message: `Contact paused until ${expected} (UK)`,
    });
  }
});

test('foreign, uncertain and malformed errors cannot assert a contact pause', () => {
  const mutations = [
    error => { delete error.fn; },
    error => { error.fn = 'weekly_source_query_sync_atomic_v1'; },
    error => { error.status = 200; },
    error => { error.status = '400'; },
    error => { error.json.code = '40001'; },
    error => { error.json.message = 'OTHER_CONTACT_PAUSED'; },
    error => { error.json.details = '<private raw detail>'; },
    error => { error.json.details = detail(); },
  ];
  for (const mutate of mutations) {
    const error = nativeError(); mutate(error);
    assert.equal(contactReminderRefusal(error), null);
  }
  for (const value of [
    { ...detail(), extra: true }, { ...detail(), version: 'OTHER' },
    { ...detail(), candidate_id: [ID] }, { ...detail(), client_id: CLIENT.toUpperCase() + 'A' },
    { ...detail(), audience: 'BOTH' },
    { ...detail(), next_allowed_utc: '2026-02-30T00:00:00.000000Z' },
    { ...detail(), next_allowed_utc: '2026-10-18T23:00:00.000Z' },
    { ...detail(), next_allowed_local: '2026-10-18 23:00 Europe/London' },
    { ...detail(), next_allowed_local: '<private raw detail>' },
  ]) assert.equal(contactReminderRefusal(nativeError(value)), null);
});

async function command(action, rpc) {
  return dispatchWeeklySourceRequest(new Request('https://test.invalid/api/weekly-source/v1/commands', {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({
      action, payload: { candidate_generation_id: ID, projection_publication_id: CLIENT },
    }),
  }), {}, {}, { requireUser: async () => ({ id: ID, role: 'admin', is_active: true }), rpc });
}

test('the actual reminder HTTP boundary returns the finite refusal after one failed RPC', async () => {
  let calls = 0;
  const response = await command('REMIND_CANDIDATE', async (name, args) => {
    calls += 1;
    assert.equal(name, 'weekly_source_candidate_reminder_atomic_v1');
    assert.equal(args.p_request.actor_user_id, ID);
    throw nativeError();
  });
  assert.equal(calls, 1);
  assert.equal(response.status, 409);
  assert.deepEqual(await response.json(), { ok: false, error_code: 'CONTACT_PAUSED',
    message: 'Contact paused until 19/10/2026 00:00 (UK)' });
  assert.equal(response.headers.get('cache-control'), 'no-store');
});

test('successful reminders and unrelated/uncertain failures retain existing response semantics', async () => {
  const receipt = { ok: true, message_intent_id: ID, next_manual_reminder_at_utc: '2026-10-17T10:00:00Z' };
  assert.deepEqual(await (await command('REMIND_CANDIDATE', async () => receipt)).json(), receipt);
  for (const [action, error] of [
    ['QUERY_SYNC', nativeError()],
    ['REMIND_CANDIDATE', Object.assign(new Error('network interrupted'), { status: 503 })],
    ['REMIND_CANDIDATE', nativeError({ ...detail(), next_allowed_local: 'wrong' })],
  ]) {
    const response = await command(action, async () => { throw error; });
    const value = await response.json();
    assert.notEqual(value.error_code, 'CONTACT_PAUSED');
    assert.doesNotMatch(value.message, /Contact paused until/);
  }
});
