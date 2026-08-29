import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import vm from 'node:vm';

import {
  findCandidateDailyRoute,
  rebuildCandidateDailySuccessBody
} from '../broker/src/candidate-daily-contract-v1.js';

const helperPath = path.join(process.cwd(), 'docs', 'candidate-app', 'phase3-apps-script',
  'master-rota', 'CloudTMSCandidateBridge.gs');
const source = fs.readFileSync(helperPath, 'utf8');
const correlationId = `0${'A'.repeat(25)}`;
const route = findCandidateDailyRoute('POST',
  '/candidate-system/v1/google-availability/rota-generations');
const receiptId = '00000000-0000-4000-8000-000000000901';

function utilities() {
  let uuid = 0;
  const bytes = (value) => Buffer.isBuffer(value) ? value
    : Buffer.from(Array.isArray(value) ? value.map((item) => item < 0 ? item + 256 : item) : String(value),
      Array.isArray(value) ? undefined : 'utf8');
  return {
    DigestAlgorithm: { SHA_256: 'SHA_256' },
    newBlob(value) {
      const body = bytes(value);
      return { getBytes: () => [...body].map((item) => item > 127 ? item - 256 : item) };
    },
    computeDigest(_algorithm, value) {
      return [...crypto.createHash('sha256').update(bytes(value)).digest()]
        .map((item) => item > 127 ? item - 256 : item);
    },
    computeHmacSha256Signature(value, key) {
      return [...crypto.createHmac('sha256', bytes(key)).update(bytes(value)).digest()]
        .map((item) => item > 127 ? item - 256 : item);
    },
    base64EncodeWebSafe(value) { return bytes(value).toString('base64url'); },
    getUuid() {
      uuid += 1;
      return `00000000-0000-4000-8000-${String(uuid).padStart(12, '0')}`;
    },
    formatDate(value, _timezone, format) {
      if (format !== 'yyyy-MM-dd') throw new Error('unsupported format');
      return new Date(value).toISOString().slice(0, 10);
    }
  };
}

function runtime() {
  const values = new Map([
    ['CLOUDTMS_CANDIDATE_BRIDGE_ENABLED', 'true'],
    ['CLOUDTMS_CANDIDATE_BASE_URL', 'https://test.invalid'],
    ['CLOUDTMS_CANDIDATE_ENVIRONMENT', 'TEST'],
    ['CLOUDTMS_CANDIDATE_GOOGLE_HMAC_KEY_ID', 'test-key'],
    ['CLOUDTMS_CANDIDATE_GOOGLE_HMAC_SECRET', 'transport-secret'],
    ['CLOUDTMS_CANDIDATE_SOURCE_HMAC_SECRET', 'source-secret']
  ]);
  const logs = [];
  const sandbox = vm.createContext({
    Utilities: utilities(),
    PropertiesService: {
      getScriptProperties() {
        return {
          getProperty: (key) => values.get(key) ?? null,
          getProperties: () => Object.fromEntries(values),
          setProperty: (key, value) => values.set(key, String(value)),
          deleteProperty: (key) => values.delete(key)
        };
      }
    },
    LockService: { getScriptLock: () => ({ waitLock() {}, releaseLock() {} }) },
    UrlFetchApp: { fetch() { throw new Error('unexpected network'); } },
    console: { log(value) { logs.push(JSON.parse(value)); } },
    Date, JSON, Math, Object, Array, String, Number, Boolean, Set, Map
  });
  new vm.Script(source, { filename: helperPath }).runInContext(sandbox);
  return { sandbox, values, logs };
}

function committed(index, status = 'COMMITTED') {
  return {
    index,
    status,
    generation_id: `00000000-0000-4000-8000-${String(700 + index).padStart(12, '0')}`,
    generation_version: index + 1
  };
}

function rejected(index, errorCode) {
  return { index, status: 'REJECTED', ...(errorCode ? { error_code: errorCode } : {}) };
}

function workerEnvelope(outcomes) {
  const json = rebuildCandidateDailySuccessBody(route, 200, {
    ok: true,
    correlation_id: correlationId,
    result: { batch_receipt_id: receiptId, outcomes }
  }, correlationId);
  assert.ok(json, 'fixture must pass the current Worker success-envelope builder');
  return { http_code: 200, uncertain: false, json };
}

function rawEnvelope(result) {
  return { http_code: 200, uncertain: false,
    json: { ok: true, correlation_id: correlationId, result } };
}

function item(index) {
  return {
    candidate_global_key: `CID1-ABCDEFGHJKMNPQRS${index}`,
    candidate_source_hmac: crypto.createHash('sha256').update(`candidate-${index}`).digest('hex'),
    source_hmac_key_version: 1,
    source_event_id: `master-rota.r14-${index}`,
    source_revision: `phase3.${'a'.repeat(64)}`,
    source_hash: 'b'.repeat(64),
    window_start: '2026-08-17',
    days: [{ date: '2026-08-17', booked: false, system_blocked: false,
      source_row_hash: 'c'.repeat(64), note: 'r14' }],
    source_event_time: '2026-08-17T12:00:00.000Z',
    item_key: `rota.r14.${String(index).padStart(8, '0')}.fixed`
  };
}

test('R14 current Worker success envelope with every item COMMITTED is success', () => {
  const { sandbox } = runtime();
  assert.equal(sandbox.ctmsP3_masterContractDisposition_(
    workerEnvelope([committed(0), committed(1)]), 2), 'SUCCESS');
});

test('R14 current Worker success envelope with every item REPLAYED is success', () => {
  const { sandbox } = runtime();
  assert.equal(sandbox.ctmsP3_masterContractDisposition_(
    workerEnvelope([committed(0, 'REPLAYED'), committed(1, 'REPLAYED')]), 2), 'SUCCESS');
});

for (const errorCode of [
  'SOURCE_EVENT_CONFLICT',
  'GENERATION_INCOMPLETE',
  'IDENTITY_LINK_MISSING',
  'IDENTITY_LINK_AMBIGUOUS',
  'IDENTITY_LINK_CONFLICT',
  'CANDIDATE_DAILY_NOT_READY'
]) {
  test(`R14 ${errorCode} item rejection is terminal and never mirror completion`, () => {
    const { sandbox, values, logs } = runtime();
    sandbox.ctmsP3_masterPersistEvent_([item(0), item(1)]);
    sandbox.ctmsP3_masterSignedPost_ = () => workerEnvelope([
      committed(0), rejected(1, errorCode)
    ]);
    const result = sandbox.ctmsP3_masterRecoverPending_();
    assert.equal(result.terminal_rejection, true);
    assert.equal(values.has('CTMS_P3_ROTA_PENDING_INDEX'), false);
    assert.equal(logs.some((entry) => entry.event === 'ROTA_GENERATION_MIRROR_COMPLETE'), false);
    const terminal = logs.find((entry) => entry.event === 'ROTA_GENERATION_TERMINAL_REJECTION');
    assert.equal(terminal.error_code, 'GENERATION_ITEM_REJECTED');
    assert.deepEqual(JSON.parse(JSON.stringify(terminal.rejection_items)), [
      { index: 1, error_code: errorCode }
    ]);
  });
}

test('R14 missing outcome index preserves the frozen operation', () => {
  const { sandbox } = runtime();
  const result = rawEnvelope({ batch_receipt_id: receiptId,
    outcomes: [{ status: 'COMMITTED' }, committed(1)] });
  assert.equal(sandbox.ctmsP3_masterContractDisposition_(result, 2), 'PRESERVE');
});

test('R14 duplicate outcome index preserves the frozen operation', () => {
  const { sandbox } = runtime();
  assert.equal(sandbox.ctmsP3_masterContractDisposition_(
    workerEnvelope([committed(0), committed(0)]), 2), 'PRESERVE');
});

for (const [name, outcomes, expectedCount] of [
  ['smaller', [committed(0)], 2],
  ['larger', [committed(0), committed(1)], 1]
]) {
  test(`R14 ${name} outcome count preserves the frozen operation`, () => {
    const { sandbox } = runtime();
    assert.equal(sandbox.ctmsP3_masterContractDisposition_(
      workerEnvelope(outcomes), expectedCount), 'PRESERVE');
  });
}

test('R14 malformed receipt, unknown status, and unsafe rejection code preserve', () => {
  const { sandbox } = runtime();
  const fixtures = [
    rawEnvelope({ batch_receipt_id: 'not-a-uuid', outcomes: [committed(0)] }),
    rawEnvelope({ batch_receipt_id: receiptId, outcomes: [{ index: 0, status: 'UNKNOWN' }] }),
    workerEnvelope([rejected(0, undefined)]),
    workerEnvelope([rejected(0, 'FUTURE_UNCLASSIFIED_REJECTION')])
  ];
  fixtures.forEach((fixture) => {
    assert.equal(sandbox.ctmsP3_masterContractDisposition_(fixture, 1), 'PRESERVE');
  });
});

test('R14 first batch commit followed by second-batch rejection never logs overall completion', () => {
  const { sandbox, values, logs } = runtime();
  sandbox.ctmsP3_masterPersistEvent_(Array.from({ length: 51 }, (_, index) => item(index)));
  let calls = 0;
  sandbox.ctmsP3_masterSignedPost_ = (_path, body) => {
    calls += 1;
    if (calls === 1) return workerEnvelope(body.items.map((_, index) => committed(index)));
    return workerEnvelope([rejected(0, 'IDENTITY_LINK_MISSING')]);
  };
  const result = sandbox.ctmsP3_masterRecoverPending_();
  assert.equal(result.terminal_rejection, true);
  assert.equal(calls, 2);
  assert.equal(values.has('CTMS_P3_ROTA_PENDING_INDEX'), false);
  assert.equal(logs.some((entry) => entry.event === 'ROTA_GENERATION_MIRROR_COMPLETE'), false);
  assert.equal(logs.filter((entry) => entry.event === 'ROTA_GENERATION_TERMINAL_REJECTION').length, 1);
});

test('R14 exact current Worker envelope is the fixture authority, not a top-level error approximation', () => {
  const envelope = workerEnvelope([committed(0), rejected(1, 'SOURCE_EVENT_CONFLICT')]);
  assert.equal(envelope.http_code, 200);
  assert.equal(envelope.json.ok, true);
  assert.equal(envelope.json.result.outcomes[1].status, 'REJECTED');
  assert.equal(Object.hasOwn(envelope.json, 'error_code'), false);
});
