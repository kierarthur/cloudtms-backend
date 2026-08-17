import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import vm from 'node:vm';

const repositoryHelper = path.join(process.cwd(), 'docs', 'candidate-app', 'phase3-apps-script',
  'master-rota', 'CloudTMSCandidateBridge.gs');
const packagedHelper = path.join(process.cwd(), 'source', 'master-rota', 'CloudTMSCandidateBridge.gs');
const helper = fs.existsSync(repositoryHelper) ? repositoryHelper : packagedHelper;
const source = fs.readFileSync(helper, 'utf8');

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

function store(initial = {}) {
  const values = new Map(Object.entries(initial).map(([key, value]) => [key, String(value)]));
  const deleted = [];
  const written = [];
  return {
    values,
    deleted,
    written,
    service: {
      getScriptProperties() {
        return {
          getProperty: (key) => values.get(key) ?? null,
          getProperties: () => Object.fromEntries(values),
          setProperty(key, value) { values.set(key, String(value)); written.push(key); },
          deleteProperty(key) { values.delete(key); deleted.push(key); }
        };
      }
    }
  };
}

function context(initial = {}) {
  const properties = store({
    CLOUDTMS_CANDIDATE_BRIDGE_ENABLED: 'true',
    CLOUDTMS_CANDIDATE_BASE_URL: 'https://test.invalid',
    CLOUDTMS_CANDIDATE_ENVIRONMENT: 'TEST',
    CLOUDTMS_CANDIDATE_GOOGLE_HMAC_KEY_ID: 'test-key',
    CLOUDTMS_CANDIDATE_GOOGLE_HMAC_SECRET: 'transport-secret',
    CLOUDTMS_CANDIDATE_SOURCE_HMAC_SECRET: 'source-secret',
    ...initial
  });
  const logs = [];
  const sandbox = vm.createContext({
    Utilities: utilities(),
    PropertiesService: properties.service,
    LockService: { getScriptLock: () => ({ waitLock() {}, releaseLock() {} }) },
    UrlFetchApp: { fetch() { throw new Error('unexpected network'); } },
    console: { log(value) { logs.push(JSON.parse(value)); } },
    Date, JSON, Math, Object, Array, String, Number, Boolean, Set, Map
  });
  new vm.Script(source, { filename: helper }).runInContext(sandbox);
  return { sandbox, properties, logs };
}

function item(index, payloadBytes = 32) {
  return {
    candidate_source_hmac: crypto.createHash('sha256').update(`candidate-${index}`).digest('hex'),
    source_event_id: `master-rota.test-${index}`,
    source_revision: `phase3.${'a'.repeat(64)}`,
    source_hash: 'b'.repeat(64),
    window_start: '2026-08-17',
    days: [{ date: '2026-08-17', booked: false, system_blocked: false,
      source_row_hash: 'c'.repeat(64), note: 'x'.repeat(payloadBytes) }],
    source_event_time: '2026-08-17T12:00:00.000Z',
    item_key: `rota.${String(index).padStart(8, '0')}.fixed`
  };
}

function success() {
  return { http_code: 200, json: { ok: true, result: {} }, uncertain: false };
}

function statusInProgress() {
  return { http_code: 409,
    json: { ok: false, error_code: 'BATCH_IN_PROGRESS', retry_class: 'STATUS_CHECK' },
    uncertain: false };
}

test('R13 deliberately has no candidate-specific or cohort allowlist in the runtime source', () => {
  assert.doesNotMatch(source, /SOURCE_HMAC_ALLOWLIST|ALLOWED_CANDIDATE|KIER/i);
  assert.match(source, /ctmsP3_masterBuildGenerationItems_/);
});

test('four ordinary items are stored in property values below the 7 KB safety ceiling', () => {
  const { sandbox, properties } = context();
  const index = sandbox.ctmsP3_masterPersistEvent_(Array.from({ length: 4 }, (_, i) => item(i, 2400)));
  assert.ok(index.manifest_keys.length >= 1);
  for (const [key, value] of properties.values) {
    if (key.startsWith('CTMS_P3_ROTA_')) assert.ok(Buffer.byteLength(value) <= 7000, key);
  }
});

test('fifty items reassemble byte-for-byte with one frozen batch identity', () => {
  const { sandbox } = context();
  const items = Array.from({ length: 50 }, (_, i) => item(i, 1600));
  const index = sandbox.ctmsP3_masterPersistEvent_(items);
  assert.equal(index.manifest_keys.length, 1);
  const state = sandbox.ctmsP3_masterStateFromManifest_(index.manifest_keys[0]);
  assert.equal(state.body.items.length, 50);
  assert.deepEqual(JSON.parse(JSON.stringify(state.body.items)), items);
  assert.equal(state.manifest.body_sha256,
    crypto.createHash('sha256').update(state.body_text).digest('hex'));
});

test('store capacity is checked before any bridge POST or durable event is admitted', () => {
  const { sandbox, properties } = context({ LEGACY_LARGE_VALUE: 'z'.repeat(479500) });
  assert.throws(() => sandbox.ctmsP3_masterPersistEvent_([item(1, 3000)]),
    /CTMS_ROTA_PROPERTY_STORE_CAPACITY/);
  assert.equal(properties.values.has('CTMS_P3_ROTA_PENDING_INDEX'), false);
  assert.equal([...properties.values.keys()].some((key) => key.startsWith('CTMS_P3_ROTA_BODY_')), false);
});

test('oversized collections are partitioned below both item and route byte limits', () => {
  const { sandbox } = context();
  const index = sandbox.ctmsP3_masterPersistEvent_([item(1, 130000), item(2, 130000)]);
  assert.equal(index.manifest_keys.length, 2);
  index.manifest_keys.forEach((key) => {
    const state = sandbox.ctmsP3_masterStateFromManifest_(key);
    assert.ok(state.manifest.body_bytes <= 245760);
    assert.ok(state.body.items.length <= 50);
  });
});

test('a single item above the route safety limit fails before state or POST', () => {
  const { sandbox, properties } = context();
  assert.throws(() => sandbox.ctmsP3_masterPersistEvent_([item(1, 250000)]),
    /CTMS_ROTA_ITEM_EXCEEDS_ROUTE_LIMIT/);
  assert.equal(properties.values.has('CTMS_P3_ROTA_PENDING_INDEX'), false);
});

test('BATCH_IN_PROGRESS is retained and never classified as completion', () => {
  const { sandbox, properties, logs } = context();
  sandbox.ctmsP3_masterPersistEvent_([item(1)]);
  sandbox.ctmsP3_masterSignedPost_ = () => statusInProgress();
  const result = sandbox.ctmsP3_masterRecoverPending_();
  assert.equal(result.resolved, false);
  assert.equal(properties.values.has('CTMS_P3_ROTA_PENDING_INDEX'), true);
  assert.equal(logs.some((entry) => entry.event === 'ROTA_GENERATION_MIRROR_COMPLETE'), false);
});

test('unknown malformed 409 is retained fail closed', () => {
  const { sandbox, properties } = context();
  sandbox.ctmsP3_masterPersistEvent_([item(1)]);
  sandbox.ctmsP3_masterSignedPost_ = () => ({ http_code: 409, json: { message: 'wait' }, uncertain: false });
  const result = sandbox.ctmsP3_masterRecoverPending_();
  assert.equal(result.resolved, false);
  assert.equal(properties.values.has('CTMS_P3_ROTA_PENDING_INDEX'), true);
});

for (const uncertain of [
  { http_code: 429, json: null, uncertain: true },
  { http_code: 500, json: null, uncertain: true },
  { http_code: 503, json: null, uncertain: true },
  { http_code: -1, json: null, uncertain: true }
]) {
  test(`uncertain ${uncertain.http_code} retains the exact frozen operation`, () => {
    const { sandbox, properties } = context();
    sandbox.ctmsP3_masterPersistEvent_([item(1)]);
    sandbox.ctmsP3_masterSignedPost_ = () => uncertain;
    assert.equal(sandbox.ctmsP3_masterRecoverPending_().resolved, false);
    assert.equal(properties.values.has('CTMS_P3_ROTA_PENDING_INDEX'), true);
  });
}

test('uncertainty and a later event reuse the exact body, batch, key and correlation even after eight days', () => {
  const { sandbox } = context();
  sandbox.ctmsP3_masterPersistEvent_([item(1, 100)]);
  const calls = [];
  sandbox.ctmsP3_masterSignedPost_ = (_path, body, key, correlation) => {
    calls.push({ body: JSON.stringify(body), key, correlation });
    return calls.length === 1 ? statusInProgress() : success();
  };
  assert.equal(sandbox.ctmsP3_masterRecoverPending_().resolved, false);
  const originalNow = sandbox.Date.now;
  sandbox.Date.now = () => originalNow() + 8 * 24 * 60 * 60 * 1000;
  assert.equal(sandbox.ctmsP3_masterRecoverPending_().resolved, true);
  assert.deepEqual(calls[1], calls[0]);
});

test('two-batch event does not complete until every frozen batch succeeds', () => {
  const { sandbox, properties, logs } = context();
  sandbox.ctmsP3_masterPersistEvent_(Array.from({ length: 51 }, (_, i) => item(i)));
  const responses = [success(), statusInProgress(), success()];
  sandbox.ctmsP3_masterSignedPost_ = () => responses.shift();
  assert.equal(sandbox.ctmsP3_masterRecoverPending_().resolved, false);
  const pending = JSON.parse(properties.values.get('CTMS_P3_ROTA_PENDING_INDEX'));
  assert.equal(pending.manifest_keys.length, 1);
  assert.equal(logs.some((entry) => entry.event === 'ROTA_GENERATION_MIRROR_COMPLETE'), false);
  assert.equal(sandbox.ctmsP3_masterRecoverPending_().resolved, true);
  assert.equal(properties.values.has('CTMS_P3_ROTA_PENDING_INDEX'), false);
  assert.equal(logs.filter((entry) => entry.event === 'ROTA_GENERATION_MIRROR_COMPLETE').length, 1);
});

for (const terminal of [
  { http_code: 409, json: { ok: false, error_code: 'SOURCE_EVENT_CONFLICT', retry_class: 'DO_NOT_RETRY' }, uncertain: false },
  { http_code: 422, json: { ok: false, error_code: 'GENERATION_INCOMPLETE', retry_class: 'DO_NOT_RETRY' }, uncertain: false }
]) {
  test(`${terminal.json.error_code} is an explicit terminal rejection, not success`, () => {
    const { sandbox, properties, logs } = context();
    sandbox.ctmsP3_masterPersistEvent_(Array.from({ length: 51 }, (_, i) => item(i)));
    sandbox.ctmsP3_masterSignedPost_ = () => terminal;
    const result = sandbox.ctmsP3_masterRecoverPending_();
    assert.equal(result.terminal_rejection, true);
    assert.equal(properties.values.has('CTMS_P3_ROTA_PENDING_INDEX'), false);
    assert.equal(logs.some((entry) => entry.event === 'ROTA_GENERATION_TERMINAL_REJECTION'), true);
    assert.equal(logs.some((entry) => entry.event === 'ROTA_GENERATION_MIRROR_COMPLETE'), false);
  });
}

test('corrupt chunk fails closed and cannot be replaced with a newer event', () => {
  const { sandbox, properties } = context();
  const index = sandbox.ctmsP3_masterPersistEvent_([item(1, 9000)]);
  const manifest = JSON.parse(properties.values.get(index.manifest_keys[0]));
  properties.values.set(manifest.body_prefix + '1', 'corrupt');
  let network = 0;
  sandbox.ctmsP3_masterSignedPost_ = () => { network += 1; return success(); };
  assert.throws(() => sandbox.ctmsP3_masterRecoverPending_(), /CTMS_PENDING_BODY_CORRUPT/);
  assert.equal(network, 0);
  assert.equal(properties.values.has('CTMS_P3_ROTA_PENDING_INDEX'), true);
  assert.throws(() => sandbox.ctmsP3_masterPersistEvent_([item(2)]),
    /CTMS_PENDING_EVENT_ALREADY_EXISTS/);
});

test('a later accepted legacy event recovers pending state and does not build a replacement event', () => {
  const { sandbox } = context();
  sandbox.ctmsP3_masterPersistEvent_([item(1)]);
  let builds = 0;
  let posts = 0;
  sandbox.ctmsP3_masterBuildGenerationItems_ = () => { builds += 1; return [item(2)]; };
  sandbox.ctmsP3_masterSignedPost_ = () => { posts += 1; return success(); };
  sandbox.ctmsP3_masterMirrorLegacyEvent_('AVAILABILITY_UPDATE_END', { runId: 'new-run' }, { httpCode: 200 });
  assert.equal(posts, 1);
  assert.equal(builds, 0);
});

test('orphaned body chunks are cleaned only when no pending index owns them', () => {
  const { sandbox, properties } = context({
    CTMS_P3_ROTA_BODY_orphan_1: 'orphan',
    CTMS_P3_ROTA_MANIFEST_orphan: '{bad json',
    LEGACY_PROPERTY: 'preserve'
  });
  sandbox.ctmsP3_masterCleanupOrphans_();
  assert.equal(properties.values.has('CTMS_P3_ROTA_BODY_orphan_1'), false);
  assert.equal(properties.values.has('CTMS_P3_ROTA_MANIFEST_orphan'), false);
  assert.equal(properties.values.get('LEGACY_PROPERTY'), 'preserve');
});
